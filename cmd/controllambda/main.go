// Command controllambda runs the demo's control plane as a Temporal
// serverless worker on AWS Lambda: the rollout coordinator, the traffic
// generator, and the Standalone Activities that queue a burst of orders.
//
// It is the same code cmd/controlworker runs on Kubernetes, hosted
// differently, and it is registered as its own Worker Deployment —
// deliberately not part of the one it manages. A Workflow that decides which
// order version takes traffic must not be routed by that decision: joining
// rainbow-orders would mean promoting a candidate could migrate the
// coordinator mid-rollout, and a broken candidate could take down the very
// thing meant to detect it and roll back.
package main

import (
	"context"
	"log"
	"os"

	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/contrib/aws/lambdaworker"
	"go.temporal.io/sdk/worker"
	"go.temporal.io/sdk/workflow"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/awssecret"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/deploy"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/metrics"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/rollout"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/traffic"
)

// DefaultControlDeployment is the control plane's own Worker Deployment.
//
// Separate from the order deployment on purpose; see the package comment.
const DefaultControlDeployment = "rainbow-control"

func main() {
	logger := config.Logger()

	// Fetch the Temporal credential before the worker tries to connect.
	if err := awssecret.ResolveTemporalAPIKey(context.Background()); err != nil {
		log.Fatalf("cannot resolve the Temporal API key: %v", err)
	}

	cfg := config.TemporalFromEnv()

	deploymentVersion := worker.WorkerDeploymentVersion{
		DeploymentName: config.Env("CONTROL_DEPLOYMENT_NAME", DefaultControlDeployment),
		BuildID:        config.Env("CONTROL_BUILD_ID", "control"),
	}

	logger.Info("starting the control plane on Lambda",
		"namespace", cfg.Namespace,
		"taskQueue", rollout.TaskQueue,
		"controlDeployment", deploymentVersion.DeploymentName,
		"buildId", deploymentVersion.BuildID,
		"managing", cfg.DeploymentName,
	)

	lambdaworker.RunWorker(deploymentVersion, func(opts *lambdaworker.Options) error {
		opts.TaskQueue = rollout.TaskQueue

		opts.WorkerOptions.DeploymentOptions = worker.DeploymentOptions{
			UseVersioning: true,
			Version:       deploymentVersion,
			// Auto-upgrade, not Pinned. The generator runs for the whole
			// session and the coordinator can outlive a deploy, and neither
			// has any reason to stay on old control-plane code — pinning them
			// would mean a new control version never picked up the Workflows
			// already running.
			DefaultVersioningBehavior: workflow.VersioningBehaviorAutoUpgrade,
		}

		// The control Activities need a client of their own.
		//
		// lambdaworker dials the worker's client after this callback returns
		// and does not hand it back, so this dials a second one from the same
		// pre-populated options — which is what keeps both connecting
		// identically without re-deriving any configuration here.
		c, err := client.Dial(opts.ClientOptions)
		if err != nil {
			return err
		}

		deployment := deploy.New(c, cfg.DeploymentName, cfg.Namespace, logger)
		reader := metrics.NewReader(c, cfg.Namespace, cfg.DeploymentName)

		rollout.NewActivities(c, deployment, reader, logger).Register(opts)
		traffic.NewActivities(c, deployment,
			config.EnvInt("TRAFFIC_CONCURRENCY", traffic.DefaultConcurrency), logger).Register(opts)

		// Order starts are network round trips, and a burst batch is hundreds
		// of them, so the slots have to be generous or a burst is paced by the
		// worker rather than by the server.
		tuneControlWorker(&opts.WorkerOptions)
		return nil
	})

	_ = os.Stdout.Sync()
}
