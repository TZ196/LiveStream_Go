package config

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/google/uuid"
)

// Likes 保存点赞链路的连接设置。
type Likes struct {
	Enabled              bool
	RedisAddr            string
	Brokers              []string
	ProjectionEpoch      string
	WSOrigin             string
	RealtimeConsumers    int
	PersistenceConsumers int
}

// LoadLikes 从环境变量加载点赞链路配置；未启用时不连接中间件。
func LoadLikes() (Likes, error) {
	if os.Getenv("LIVESTREAM_ENABLE_LIKES") != "true" {
		return Likes{}, nil
	}
	cfg := Likes{Enabled: true, RedisAddr: os.Getenv("LIVESTREAM_REDIS_ADDR"), ProjectionEpoch: os.Getenv("LIVESTREAM_PROJECTION_EPOCH"), WSOrigin: os.Getenv("LIVESTREAM_WS_ORIGIN")}
	for _, broker := range strings.Split(os.Getenv("LIVESTREAM_KAFKA_BROKERS"), ",") {
		broker = strings.TrimSpace(broker)
		if broker != "" {
			cfg.Brokers = append(cfg.Brokers, broker)
		}
	}
	if cfg.RedisAddr == "" || len(cfg.Brokers) == 0 {
		return Likes{}, errors.New("启用点赞须指定 Redis 和 Kafka 地址")
	}
	id, err := uuid.Parse(cfg.ProjectionEpoch)
	if err != nil || id.String() != cfg.ProjectionEpoch {
		return Likes{}, errors.New("启用点赞须指定稳定的小写 UUID 投影代次")
	}
	origin, err := url.Parse(cfg.WSOrigin)
	if err != nil || (origin.Scheme != "http" && origin.Scheme != "https") || origin.Host == "" || origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" || origin.User != nil {
		return Likes{}, errors.New("WebSocket 来源必须是完整的 HTTP 或 HTTPS Origin")
	}
	cfg.RealtimeConsumers, err = consumerCount("LIVESTREAM_REALTIME_CONSUMERS")
	if err != nil {
		return Likes{}, err
	}
	cfg.PersistenceConsumers, err = consumerCount("LIVESTREAM_PERSISTENCE_CONSUMERS")
	if err != nil {
		return Likes{}, err
	}
	return cfg, nil
}

func consumerCount(key string) (int, error) {
	value, configured := os.LookupEnv(key)
	if !configured {
		return 4, nil
	}
	count, err := strconv.Atoi(value)
	if err != nil || count < 1 || count > 16 {
		return 0, fmt.Errorf("%s 须为 1 到 16 之间的整数", key)
	}
	return count, nil
}
