package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/TZ196/LiveStream_Go/backend/internal/httpserver"
)

func main() {
	if err := run(); err != nil {
		log.Printf("API server stopped with an error: %v", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", "127.0.0.1:8080", "HTTP listen address")
	flag.Parse()

	h := httpserver.New(*addr)
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	defer signal.Stop(signals)

	// Spin manages shutdown; preserve startup errors so the process exits nonzero.
	var serveErr error
	h.SetCustomSignalWaiter(func(errs chan error) error {
		select {
		case serveErr = <-errs:
			return serveErr
		case <-signals:
			return nil
		}
	})
	h.Spin()
	return serveErr
}
