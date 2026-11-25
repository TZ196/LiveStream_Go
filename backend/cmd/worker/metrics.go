package main

import (
	"context"
	"log/slog"
	"os"
	"time"

	"github.com/TZ196/LiveStream_Go/backend/internal/danmaku"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/common/adaptor"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// startWorkerMetrics 只开放本地运维指标；不另起业务进程或共享 API 端口。
func startWorkerMetrics(metrics *danmaku.Metrics) func() {
	address := os.Getenv("LIVESTREAM_WORKER_METRICS_ADDR")
	if address == "" {
		address = "127.0.0.1:9091"
	}
	hertz := server.New(server.WithHostPorts(address), server.WithTransport(standard.NewTransporter), server.WithReadTimeout(3*time.Second), server.WithWriteTimeout(3*time.Second))
	hertz.GET("/metrics", adaptor.HertzHandler(promhttp.HandlerFor(metrics.Registry, promhttp.HandlerOpts{})))
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := hertz.Run(); err != nil {
			slog.Warn("worker_metrics_stopped", "error", err)
		}
	}()
	return func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := hertz.Shutdown(ctx); err != nil {
			slog.Warn("worker_metrics_shutdown", "error", err)
		}
		select {
		case <-done:
		case <-ctx.Done():
		}
	}
}
