// Command backend serves the dashboard's API: JSON endpoints for control
// actions and a Server-Sent-Events stream of live state.
//
// It serves no HTML. The dashboard is a separate static frontend, so the two
// deploy and scale independently.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"go.temporal.io/sdk/client"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/api"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/traffic"
)

func main() {
	logger := config.Logger()
	if err := run(logger); err != nil {
		logger.Error("backend failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg := config.TemporalFromEnv()
	addr := ":" + config.Env("PORT", "8080")
	pollInterval := config.EnvDuration("POLL_INTERVAL", time.Second)

	logger.Info("starting backend",
		"addr", addr,
		"address", cfg.Address,
		"namespace", cfg.Namespace,
		"deployment", cfg.DeploymentName,
		"pollInterval", pollInterval,
	)

	c, err := client.Dial(cfg.ClientOptions(logger))
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer c.Close()

	server := api.New(api.Options{
		Client:        c,
		Deployment:    deploy.New(c, cfg.DeploymentName, cfg.Namespace, logger),
		Reader:        metrics.NewReader(c, cfg.Namespace, cfg.DeploymentName),
		Logger:        logger,
		PollInterval:  pollInterval,
		AllowedOrigin: config.Env("ALLOWED_ORIGIN", "*"),
		// Optional: without it the sync match gauge reports itself
		// unavailable rather than failing anything.
		MetricsURL: os.Getenv("TEMPORAL_METRICS_URL"),
		// Temporal Cloud's metrics endpoint needs its own key, from a service
		// account with the Metrics-Read-Only role. A self-hosted server's
		// metrics port needs nothing.
		MetricsAPIKey:    os.Getenv("TEMPORAL_METRICS_API_KEY"),
		MetricsNamespace: cfg.Namespace,
		// Traffic stops itself if left untouched. This matters most when the
		// workers are serverless: orders left running overnight keep invoking
		// Lambdas, and each invocation bills for its whole window.
		TrafficMaxRun: config.EnvDuration("TRAFFIC_MAX_RUN", traffic.DefaultMaxRun),
		// How many live orders the rail seats at once.
		OrderSample: config.EnvInt("LIVE_ORDER_SAMPLE", 0),
	})

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// Give the deployment a Current version if it has none, so the first order
	// has somewhere to go. Runs in the background: the API should come up even
	// if no workers are running yet.
	go server.EnsureCurrentVersion(ctx)

	server.Start(ctx, pollInterval)

	httpServer := &http.Server{
		Addr:    addr,
		Handler: server.Routes(),
		// No write timeout: the SSE stream is meant to stay open indefinitely.
		ReadHeaderTimeout: 10 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := httpServer.Shutdown(shutdownCtx); err != nil {
			logger.Warn("shutdown was not clean", "err", err)
		}
	}()

	logger.Info("backend listening", "addr", addr)
	if err := httpServer.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		return fmt.Errorf("serve: %w", err)
	}

	logger.Info("backend stopped")
	return nil
}
