package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"workflowmonolith/internal/engine"
)

func main() { os.Exit(run()) }
func run() int {
	if len(os.Args) > 1 && os.Args[1] == "health" {
		address := os.Getenv("WF_HEALTH_URL")
		if address == "" {
			address = "http://localhost:8000/health/ready"
		}
		client := http.Client{Timeout: 2 * time.Second}
		r, e := client.Get(address)
		if e != nil {
			return 1
		}
		r.Body.Close()
		if r.StatusCode != 200 {
			return 1
		}
		return 0
	}
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	cfg, err := engine.ConfigFromEnv()
	if err != nil {
		slog.Error("configuration", "error", err)
		return 1
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	app, err := engine.Start(ctx, cfg)
	if err != nil {
		slog.Error("startup_failed", "error", err)
		return 1
	}
	defer app.Close()
	server := &http.Server{Addr: cfg.Address, Handler: engine.NewHandler(app), ReadHeaderTimeout: 3 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: 15 * time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 16 << 10}
	fail := make(chan error, 1)
	go func() { fail <- server.ListenAndServe() }()
	slog.Info("engine_ready", "address", cfg.Address, "handlers", cfg.Concurrency)
	code := 0
	select {
	case <-ctx.Done():
	case err := <-app.Fatal():
		slog.Error("engine_stopped", "error", err)
		code = 1
	case err := <-fail:
		if err != http.ErrServerClosed {
			slog.Error("http_failed", "error", err)
			code = 1
		}
	}
	shutdown, done := context.WithTimeout(context.Background(), 10*time.Second)
	defer done()
	if err = server.Shutdown(shutdown); err != nil {
		fmt.Fprintln(os.Stderr, err)
	}
	return code
}
