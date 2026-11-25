package config

import (
	"errors"
	"net/url"
	"os"
	"strings"
)

type Danmaku struct {
	Enabled              bool
	RedisAddr            string
	Brokers              []string
	WordsPath            string
	WSOrigin             string
	RealtimeConsumers    int
	PersistenceConsumers int
}

// LoadDanmaku 与点赞分别启用、配置和启动，不借用点赞的可用状态。
func LoadDanmaku() (Danmaku, error) {
	if os.Getenv("LIVESTREAM_ENABLE_DANMAKU") != "true" {
		return Danmaku{}, nil
	}
	cfg := Danmaku{Enabled: true, RedisAddr: os.Getenv("LIVESTREAM_REDIS_ADDR"), WordsPath: os.Getenv("LIVESTREAM_MODERATION_WORDS"), WSOrigin: os.Getenv("LIVESTREAM_WS_ORIGIN")}
	if cfg.WordsPath == "" {
		cfg.WordsPath = "configs/moderation_words.tsv"
	}
	for _, value := range strings.Split(os.Getenv("LIVESTREAM_KAFKA_BROKERS"), ",") {
		if strings.TrimSpace(value) != "" {
			cfg.Brokers = append(cfg.Brokers, strings.TrimSpace(value))
		}
	}
	origin, err := url.Parse(cfg.WSOrigin)
	if err != nil || (origin.Scheme != "http" && origin.Scheme != "https") || origin.Host == "" || origin.Path != "" || origin.RawQuery != "" || origin.Fragment != "" || origin.User != nil {
		return Danmaku{}, errors.New("弹幕 WebSocket 来源须为有效 HTTP Origin")
	}
	if cfg.RedisAddr == "" || len(cfg.Brokers) == 0 {
		return Danmaku{}, errors.New("启用弹幕须配置 Redis 和 Kafka 地址")
	}
	cfg.RealtimeConsumers, err = consumerCount("LIVESTREAM_DANMAKU_REALTIME_CONSUMERS")
	if err != nil {
		return Danmaku{}, err
	}
	cfg.PersistenceConsumers, err = consumerCount("LIVESTREAM_DANMAKU_PERSISTENCE_CONSUMERS")
	return cfg, err
}
