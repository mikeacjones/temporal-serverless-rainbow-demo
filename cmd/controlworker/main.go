// Command controlworker runs the demo's control plane: the rollout coordinator
// and the traffic generator.
//
// It is deliberately NOT part of the versioned Worker Deployment. A Workflow
// that changes which version takes traffic cannot itself be pinned to one of
// those versions, or a rollout could strand its own coordinator.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/rollout"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/traffic"
)

func main() {
	logger := config.Logger()
	if err := run(logger); err != nil {
		logger.Error("control worker failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg := config.TemporalFromEnv()

	logger.Info("starting control worker",
		"address", cfg.Address,
		"namespace", cfg.Namespace,
		"deployment", cfg.DeploymentName,
		"taskQueue", rollout.TaskQueue,
	)

	c, err := client.Dial(cfg.ClientOptions(logger))
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer c.Close()

	deployment := deploy.New(c, cfg.DeploymentName, cfg.Namespace, logger)
	reader := metrics.NewReader(c, cfg.Namespace, cfg.DeploymentName)

	w := worker.New(c, rollout.TaskQueue, worker.Options{
		// The traffic generator's batch Activities are I/O bound, so allow
		// plenty of them: this is where a spike's sharpness comes from.
		MaxConcurrentActivityExecutionSize: config.EnvInt("CONTROL_MAX_CONCURRENT_ACTIVITIES", 100),
	})

	rollout.NewActivities(c, deployment, reader, logger).Register(w)
	traffic.NewActivities(c, deployment,
		config.EnvInt("TRAFFIC_CONCURRENCY", traffic.DefaultConcurrency), logger).Register(w)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := w.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}
	defer w.Stop()

	<-ctx.Done()
	logger.Info("control worker stopped")
	return nil
}
