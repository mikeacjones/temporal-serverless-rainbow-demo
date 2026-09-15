// Command lambdaworker runs one version of the order worker inside AWS Lambda,
// as a Temporal serverless worker.
//
// This is the same order pipeline as cmd/worker, hosted differently: Temporal
// Cloud watches the task queue and invokes this function when there is work,
// so worker capacity follows demand instead of being provisioned for the peak.
// One Lambda function (or alias) per version, each registered as its own
// Worker Deployment Version, is what makes rollouts between arbitrary versions
// work the same way here as they do locally.
package main

import (
	"context"
	"log"
	"os"

	"go.temporal.io/sdk/contrib/aws/lambdaworker"
	"go.temporal.io/sdk/worker"

	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/config"
	"github.com/temporal-sa/temporal-serverless-rainbow-demo/internal/orders"
)

func main() {
	version, ok := orders.ParseVersion(config.Env("ORDER_VERSION", string(orders.V1)))
	if !ok {
		log.Fatalf("invalid ORDER_VERSION %q", os.Getenv("ORDER_VERSION"))
	}
	profile, ok := orders.ParseProfile(config.Env("ORDER_PROFILE", string(orders.ProfileDemo)))
	if !ok {
		log.Fatalf("invalid ORDER_PROFILE %q", os.Getenv("ORDER_PROFILE"))
	}

	// Fetch the Temporal credential before the worker tries to connect.
	if err := resolveAPIKey(context.Background()); err != nil {
		log.Fatalf("cannot resolve the Temporal API key: %v", err)
	}

	deploymentName := config.Env("TEMPORAL_DEPLOYMENT_NAME", "rainbow-orders")
	// The Build ID identifies this version to Temporal. Defaulting it to the
	// version label keeps the Temporal Cloud UI readable; set it explicitly to
	// something immutable (an image digest, a Lambda alias) if a version's code
	// can change under a fixed label.
	buildID := config.Env("TEMPORAL_WORKER_BUILD_ID", string(version))

	deploymentVersion := worker.WorkerDeploymentVersion{
		DeploymentName: deploymentName,
		BuildID:        buildID,
	}

	// Connection settings (address, namespace, API key, TLS) are loaded from
	// the environment by the lambdaworker package itself, so there is no
	// client wiring here.
	lambdaworker.RunWorker(deploymentVersion, func(opts *lambdaworker.Options) error {
		opts.TaskQueue = orders.TaskQueue

		opts.WorkerOptions.DeploymentOptions = worker.DeploymentOptions{
			UseVersioning: true,
			Version:       deploymentVersion,
		}

		// Send Activity tasks through the task queue rather than handing them
		// straight to the worker that just completed the Workflow task.
		//
		// Eager dispatch would keep work inside one invocation, so the queue's
		// add and dispatch rates would stay at zero — and those rates are what
		// Temporal scales this function on, as well as what the dashboard
		// shows. A demo about elastic scale must not hide its own signal.
		opts.WorkerOptions.DisableEagerActivities = true

		opts.WorkerOptions.MaxConcurrentActivityExecutionSize =
			config.EnvInt("WORKER_MAX_CONCURRENT_ACTIVITIES", 5)
		opts.WorkerOptions.MaxConcurrentWorkflowTaskExecutionSize =
			config.EnvInt("WORKER_MAX_CONCURRENT_WORKFLOW_TASKS", 5)

		orders.Register(opts, version, profile)
		return nil
	})
}
