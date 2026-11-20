package config

import (
	"encoding/base64"
	"errors"
	"flag"
	"io"
	"log/slog"
	"net"
	"os"
	"strconv"
	"strings"
	"time"
)

// Config 保存当前阶段需要的运行设置。
type Config struct {
	HTTPAddr        string        // HTTP 服务监听地址
	LogLevel        slog.Level    // 日志最低输出级别
	ShutdownTimeout time.Duration // 服务关闭时的最长等待时间
	MySQLAddr       string        // MySQL 的 TCP 地址
	MySQLDatabase   string        // 应用数据库名称
	MySQLUser       string        // 应用数据库账号
	MySQLPassword   string        // 仅从环境变量读取，不写入日志
	JWTSecret       []byte        // Base64 环境变量解码后的签名密钥，不写入日志
}

// Default 返回未指定配置时使用的默认值。
func Default() Config {
	return Config{
		HTTPAddr:        "127.0.0.1:8080",
		LogLevel:        slog.LevelInfo,
		ShutdownTimeout: 30 * time.Second,
		MySQLAddr:       "127.0.0.1:3306",
		MySQLDatabase:   "livestream",
		MySQLUser:       "livestream",
	}
}

// Usage 是 -h 显示的帮助文字，实际默认值由 Default 提供。
const Usage = `用法：api [参数]
  -addr host:port          HTTP 监听地址，默认 127.0.0.1:8080
  -log-level level         日志级别：debug、info、warn、error，默认 info
  -shutdown-timeout value  关闭等待时间，需要为正数并带单位，默认 30s

环境变量：LIVESTREAM_HTTP_ADDR、LIVESTREAM_LOG_LEVEL、
LIVESTREAM_SHUTDOWN_TIMEOUT。
优先级：命令行参数 > 环境变量 > 默认值。
程序不会自动读取 .env 文件。

MySQL 配置仅使用环境变量：
  LIVESTREAM_MYSQL_ADDR      默认 127.0.0.1:3306
  LIVESTREAM_MYSQL_DATABASE  默认 livestream
  LIVESTREAM_MYSQL_USER      默认 livestream
  LIVESTREAM_MYSQL_PASSWORD  必填，无默认密码

JWT 配置仅使用环境变量：
  LIVESTREAM_JWT_SECRET      必填，至少 32 随机字节的标准 Base64 编码
`

// Load 按顺序合并配置，再检查最终值；此函数不启动 HTTP 服务。
func Load(args []string) (Config, error) {
	// 1. 先准备默认值。级别和时长暂存为字符串，合并完成后再转换。
	defaults := Default()
	httpAddr := defaults.HTTPAddr
	logLevelText := defaults.LogLevel.String()
	shutdownTimeoutText := defaults.ShutdownTimeout.String()

	// 2. 用已设置的环境变量覆盖默认值，显式空值也留给后续校验。
	envAddr, exists := os.LookupEnv("LIVESTREAM_HTTP_ADDR")
	if exists {
		httpAddr = envAddr
	}

	envLevel, exists := os.LookupEnv("LIVESTREAM_LOG_LEVEL")
	if exists {
		logLevelText = envLevel
	}

	envTimeout, exists := os.LookupEnv("LIVESTREAM_SHUTDOWN_TIMEOUT")
	if exists {
		shutdownTimeoutText = envTimeout
	}

	// 3. 将命令行参数绑定到变量，Parse 会用用户传入的值覆盖它们。
	flags := flag.NewFlagSet("api", flag.ContinueOnError)
	// 由入口统一显示帮助和错误，避免解析器回显原始参数。
	flags.SetOutput(io.Discard)
	flags.StringVar(&httpAddr, "addr", httpAddr, "HTTP 监听地址")
	flags.StringVar(&logLevelText, "log-level", logLevelText, "日志级别")
	flags.StringVar(&shutdownTimeoutText, "shutdown-timeout", shutdownTimeoutText, "关闭等待时间")

	err := flags.Parse(args)
	if errors.Is(err, flag.ErrHelp) {
		return Config{}, flag.ErrHelp
	}
	if err != nil {
		return Config{}, errors.New("命令行参数无效，请使用 -h 查看支持的参数")
	}
	if flags.NArg() != 0 {
		return Config{}, errors.New("不支持未命名的参数，请使用 -h 查看帮助")
	}

	// 4. 只校验最终值，因此正确的命令行参数可以覆盖无效的环境变量。
	host, port, err := net.SplitHostPort(httpAddr)
	if err != nil {
		return Config{}, errors.New("LIVESTREAM_HTTP_ADDR / -addr 必须采用 host:port 格式")
	}
	if strings.ContainsAny(host, "/\\?#@ \t\r\n") {
		return Config{}, errors.New("LIVESTREAM_HTTP_ADDR / -addr 的主机部分包含非法字符")
	}
	portNumber, err := strconv.ParseUint(port, 10, 16)
	if err != nil || portNumber == 0 {
		return Config{}, errors.New("LIVESTREAM_HTTP_ADDR / -addr 的端口必须是 1 到 65535 的数字")
	}

	var logLevel slog.Level
	switch strings.ToLower(logLevelText) {
	case "debug":
		logLevel = slog.LevelDebug
	case "info":
		logLevel = slog.LevelInfo
	case "warn":
		logLevel = slog.LevelWarn
	case "error":
		logLevel = slog.LevelError
	default:
		return Config{}, errors.New("LIVESTREAM_LOG_LEVEL / -log-level 必须是 debug、info、warn 或 error")
	}

	shutdownTimeout, err := time.ParseDuration(shutdownTimeoutText)
	if err != nil || shutdownTimeout <= 0 {
		return Config{}, errors.New("LIVESTREAM_SHUTDOWN_TIMEOUT / -shutdown-timeout 必须是带单位的正数，例如 30s")
	}

	// 5. API 和迁移命令共用数据库配置；迁移命令无需读取 HTTP 参数。
	cfg, err := LoadMySQL()
	if err != nil {
		return Config{}, err
	}
	cfg.HTTPAddr = httpAddr
	cfg.LogLevel = logLevel
	cfg.ShutdownTimeout = shutdownTimeout
	secretText := os.Getenv("LIVESTREAM_JWT_SECRET")
	secret, err := base64.StdEncoding.Strict().DecodeString(secretText)
	if err != nil || len(secret) < 32 {
		return Config{}, errors.New("LIVESTREAM_JWT_SECRET 必须是至少 32 随机字节的标准 Base64 编码")
	}
	cfg.JWTSecret = secret
	return cfg, nil
}

// LoadMySQL 只读取和校验数据库配置，供 API 与独立迁移命令复用。
func LoadMySQL() (Config, error) {
	cfg := Default()
	value, exists := os.LookupEnv("LIVESTREAM_MYSQL_ADDR")
	if exists {
		cfg.MySQLAddr = value
	}
	value, exists = os.LookupEnv("LIVESTREAM_MYSQL_DATABASE")
	if exists {
		cfg.MySQLDatabase = value
	}
	value, exists = os.LookupEnv("LIVESTREAM_MYSQL_USER")
	if exists {
		cfg.MySQLUser = value
	}
	// 密码不放在命令行参数中，避免进入命令历史和进程参数。
	cfg.MySQLPassword = os.Getenv("LIVESTREAM_MYSQL_PASSWORD")

	mysqlHost, mysqlPort, err := net.SplitHostPort(cfg.MySQLAddr)
	if err != nil || mysqlHost == "" || strings.ContainsAny(mysqlHost, "/\\?#@ \t\r\n") {
		return Config{}, errors.New("LIVESTREAM_MYSQL_ADDR 必须包含有效主机和端口，例如 127.0.0.1:3306")
	}
	mysqlPortNumber, err := strconv.ParseUint(mysqlPort, 10, 16)
	if err != nil || mysqlPortNumber == 0 {
		return Config{}, errors.New("LIVESTREAM_MYSQL_ADDR 的端口必须是 1 到 65535 的数字")
	}
	if strings.TrimSpace(cfg.MySQLDatabase) == "" || strings.TrimSpace(cfg.MySQLUser) == "" {
		return Config{}, errors.New("LIVESTREAM_MYSQL_DATABASE 和 LIVESTREAM_MYSQL_USER 不能为空")
	}
	if cfg.MySQLPassword == "" {
		return Config{}, errors.New("请通过 LIVESTREAM_MYSQL_PASSWORD 设置数据库密码")
	}

	return cfg, nil
}
