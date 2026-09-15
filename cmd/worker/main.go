// Command worker runs one version of the customer-order worker.
//
// A worker process serves exactly one pipeline version, chosen by the
// ORDER_VERSION environment variable. In this demo every version runs at the
// same time — five processes locally, or five Lambda aliases in AWS — which is
// what makes it possible to roll out from any version to any other version,
// including backwards.
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
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

func main() {
	logger := config.Logger()
	if err := run(logger); err != nil {
		logger.Error("order worker failed", "err", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	cfg := config.TemporalFromEnv()

	version, ok := orders.ParseVersion(config.Env("ORDER_VERSION", string(orders.V1)))
	if !ok {
		return fmt.Errorf("invalid ORDER_VERSION %q", os.Getenv("ORDER_VERSION"))
	}
	profile, ok := orders.ParseProfile(config.Env("ORDER_PROFILE", string(orders.ProfileDemo)))
	if !ok {
		return fmt.Errorf("invalid ORDER_PROFILE %q", os.Getenv("ORDER_PROFILE"))
	}

	// The Build ID is what Temporal actually routes on. Defaulting it to the
	// version label keeps local runs readable; in Kubernetes or AWS it is set
	// explicitly to something immutable (a pod-template hash, a Lambda alias).
	buildID := config.Env("TEMPORAL_WORKER_BUILD_ID", string(version))

	logger = logger.With("version", version, "buildId", buildID)
	logger.Info("starting order worker",
		"address", cfg.Address,
		"namespace", cfg.Namespace,
		"deployment", cfg.DeploymentName,
		"taskQueue", orders.TaskQueue,
		"profile", profile,
	)

	c, err := client.Dial(cfg.ClientOptions(logger))
	if err != nil {
		return fmt.Errorf("connect to Temporal: %w", err)
	}
	defer c.Close()

	w := worker.New(c, orders.TaskQueue, worker.Options{
		// Versioning is the whole point: this worker joins the deployment as
		// one version, and Temporal decides which orders reach it.
		DeploymentOptions: worker.DeploymentOptions{
			UseVersioning: true,
			Version: worker.WorkerDeploymentVersion{
				DeploymentName: cfg.DeploymentName,
				BuildID:        buildID,
			},
		},
		// Deliberately modest defaults: a worker that saturates is what makes
		// backlog, and therefore scale-out, visible.
		MaxConcurrentActivityExecutionSize:     config.EnvInt("WORKER_MAX_CONCURRENT_ACTIVITIES", 20),
		MaxConcurrentWorkflowTaskExecutionSize: config.EnvInt("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", 20),

		// Send Activity tasks through the task queue instead of handing them
		// straight to the worker that just completed the Workflow task.
		//
		// Eager dispatch is faster, but it bypasses the queue entirely — so
		// the add and dispatch rates stay at zero and the backlog reads as
		// "nothing is happening" however much load is applied. Those are the
		// numbers this demo exists to show.
		DisableEagerActivities: true,
	})

	orders.Register(w, version, profile)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	if err := w.Start(); err != nil {
		return fmt.Errorf("start worker: %w", err)
	}
	defer w.Stop()

	// Tell the world which version this Build ID is, so nothing downstream has
	// to parse Build IDs. Best-effort: a version is not registered until its
	// first poll, so this retries in the background.
	go deploy.PublishLabel(ctx, c, cfg.DeploymentName, buildID, string(version), logger)

	<-ctx.Done()
	logger.Info("order worker stopped")
	return nil
}
