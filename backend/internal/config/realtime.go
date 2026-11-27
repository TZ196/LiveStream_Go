package config

import (
	"errors"
	"os"
	"strconv"
)

// LoadWSConnectionLimit 返回单 API 实际连接预算。默认值是开发保护阈值。
func LoadWSConnectionLimit() (int, error) {
	raw, configured := os.LookupEnv("LIVESTREAM_WS_MAX_CONNECTIONS")
	if !configured {
		return 256, nil
	}
	count, err := strconv.Atoi(raw)
	if err != nil || count < 1 || count > 10000 {
		return 0, errors.New("LIVESTREAM_WS_MAX_CONNECTIONS 须为 1 到 10000 之间的整数")
	}
	return count, nil
}
