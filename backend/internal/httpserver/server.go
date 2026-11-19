package httpserver

import (
	"context"
	"time"

	"github.com/cloudwego/hertz/pkg/app"
	"github.com/cloudwego/hertz/pkg/app/server"
	"github.com/cloudwego/hertz/pkg/network/standard"
	"github.com/cloudwego/hertz/pkg/protocol/consts"
)

// New assembles HTTP routes without starting a listener.
func New(addr string) *server.Hertz {
	h := server.New(
		server.WithHostPorts(addr),
		// The standard transport returns listener errors to the startup caller.
		server.WithTransport(standard.NewTransporter),
		server.WithHandleMethodNotAllowed(true),
		server.WithExitWaitTime(30*time.Second),
	)
	h.GET("/healthz", health)
	return h
}

type healthResponse struct {
	Status string `json:"status"`
}

// Liveness checks only this process, not the availability of future dependencies.
func health(_ context.Context, c *app.RequestContext) {
	c.Header("Cache-Control", "no-store")
	c.JSON(consts.StatusOK, healthResponse{Status: "ok"})
}
