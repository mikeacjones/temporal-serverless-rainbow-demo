#!/usr/bin/env bash
# Create or update one Lambda function per order version, and publish a
# version of each.
#
# Each function becomes its own Worker Deployment Version in Temporal, which is
# what lets a rollout move traffic between any two of them. They all share one
# binary and differ only in ORDER_VERSION and the Build ID it maps to.
#
# Temporal must be pointed at a *qualified* (published) Lambda version rather
# than $LATEST: a published version is immutable, so a given Build ID always
# maps to exactly one snapshot of code and configuration. That is the same
# reason a Kubernetes deployment would pin an image digest.
#
# Required:
#   EXECUTION_ROLE      IAM role ARN the functions run as
#   TEMPORAL_ADDRESS    <namespace>.<account>.tmprl.cloud:7233
#   TEMPORAL_NAMESPACE  <namespace>.<account>
# One of:
#   TEMPORAL_API_KEY_SECRET_ARN  Secrets Manager ARN to read the key from
#   TEMPORAL_API_KEY             the key itself (visible in the console)
set -euo pipefail

cd "$(dirname "$0")/../.."

: "${EXECUTION_ROLE:?set EXECUTION_ROLE to the IAM role ARN the functions run as}"
: "${TEMPORAL_ADDRESS:?set TEMPORAL_ADDRESS to your Temporal Cloud endpoint}"
: "${TEMPORAL_NAMESPACE:?set TEMPORAL_NAMESPACE to your namespace}"

if [[ -z "${TEMPORAL_API_KEY:-}" && -z "${TEMPORAL_API_KEY_SECRET_ARN:-}" ]]; then
  echo "set TEMPORAL_API_KEY_SECRET_ARN (preferred) or TEMPORAL_API_KEY" >&2
  exit 1
fi

PREFIX=${PREFIX:-rainbow-orders}
DEPLOYMENT_NAME=${TEMPORAL_DEPLOYMENT_NAME:-rainbow-orders}
REGION=${AWS_REGION:-ca-central-1}
ARCH=${ARCH:-arm64}
ORDER_PROFILE=${ORDER_PROFILE:-demo}
VERSIONS=${VERSIONS:-"v1 v2 v3 v4 v5"}
MEMORY=${MEMORY:-512}

# How many Activities one worker runs at once.
#
# The default in the Lambda entrypoint is deliberately small, which turns out
# to be wrong for a burst: with five slots per worker, draining a 5,000-order
# spike needs hundreds of concurrent invocations, and asking AWS for hundreds
# of invocations is how you get throttled. Twenty slots does the same work with
# roughly a fifth of the workers. The steps only sleep, so they are not
# competing for CPU.
WORKER_ACTIVITIES=${WORKER_ACTIVITIES:-20}

# Reserved concurrency is NOT set by default, on purpose.
#
# It looks like a safety guardrail and behaves like a throttle: reserving 100
# per function capped a burst at 695 orders/min and produced 1,495 Lambda
# throttles against 189 successful invocations, because Temporal kept asking
# to scale out and AWS kept refusing. Removing it took the same burst to
# 4,400 orders/min.
#
# The account's own concurrency limit is the real guardrail, and only one
# version takes traffic at a time, so the five functions sharing one pool is
# the right shape. Set this only if you must protect other workloads in the
# same region.
RESERVED_CONCURRENCY=${RESERVED_CONCURRENCY:-}
# A Lambda worker polls until its invocation deadline and then exits — it does
# not exit early when idle. So this timeout is exactly how long an idle worker
# keeps billing, and how long "scaled to zero" takes to become true after a
# burst ends. 600s made that a ten-minute wait, which undercuts the whole
# point on stage.
#
# The floor is the longest activity StartToClose (30s) plus the worker stop
# timeout (7s), and the docs recommend at least a minute; 120s clears both
# with room for a deliberately slowed 45s step.
TIMEOUT=${TIMEOUT:-120}
OUT=${OUT:-dist/lambda/arns.txt}

profile_arg=()
[[ -n "${AWS_PROFILE:-}" ]] && profile_arg=(--profile "$AWS_PROFILE")

ZIP=dist/lambda/worker.zip
[[ -f $ZIP ]] || ARCH="$ARCH" ./deploy/aws/build.sh

lambda_arch=$([[ $ARCH == arm64 ]] && echo arm64 || echo x86_64)

: > "$OUT"

for version in $VERSIONS; do
  name="$PREFIX-$version"
  echo
  echo "=== $name ==="

  # The Build ID is deliberately the version label. The dashboard reads a
  # version's friendly name from Worker Deployment Version metadata and falls
  # back to the Build ID, so keeping them equal means the labels read correctly
  # without the Lambda needing to publish metadata of its own.
  env_vars="TEMPORAL_ADDRESS=$TEMPORAL_ADDRESS"
  env_vars+=",TEMPORAL_NAMESPACE=$TEMPORAL_NAMESPACE"
  env_vars+=",TEMPORAL_DEPLOYMENT_NAME=$DEPLOYMENT_NAME"
  env_vars+=",TEMPORAL_WORKER_BUILD_ID=$version"
  env_vars+=",ORDER_VERSION=$version"
  env_vars+=",ORDER_PROFILE=$ORDER_PROFILE"
  env_vars+=",WORKER_MAX_CONCURRENT_ACTIVITIES=$WORKER_ACTIVITIES"
  [[ -n "${TEMPORAL_API_KEY_SECRET_ARN:-}" ]] && env_vars+=",TEMPORAL_API_KEY_SECRET_ARN=$TEMPORAL_API_KEY_SECRET_ARN"
  [[ -n "${TEMPORAL_API_KEY:-}" ]] && env_vars+=",TEMPORAL_API_KEY=$TEMPORAL_API_KEY"

  if aws lambda get-function --function-name "$name" --region "$REGION" "${profile_arg[@]}" >/dev/null 2>&1; then
    echo "  updating code"
    aws lambda update-function-code \
      --function-name "$name" --zip-file "fileb://$ZIP" \
      --region "$REGION" "${profile_arg[@]}" >/dev/null
    aws lambda wait function-updated --function-name "$name" --region "$REGION" "${profile_arg[@]}"

    echo "  updating configuration"
    aws lambda update-function-configuration \
      --function-name "$name" \
      --environment "Variables={$env_vars}" \
      --memory-size "$MEMORY" --timeout "$TIMEOUT" \
      --region "$REGION" "${profile_arg[@]}" >/dev/null
    aws lambda wait function-updated --function-name "$name" --region "$REGION" "${profile_arg[@]}"
  else
    echo "  creating"
    aws lambda create-function \
      --function-name "$name" \
      --runtime provided.al2023 \
      --architectures "$lambda_arch" \
      --handler bootstrap \
      --role "$EXECUTION_ROLE" \
      --zip-file "fileb://$ZIP" \
      --timeout "$TIMEOUT" \
      --memory-size "$MEMORY" \
      --environment "Variables={$env_vars}" \
      --region "$REGION" "${profile_arg[@]}" >/dev/null
    aws lambda wait function-active-v2 --function-name "$name" --region "$REGION" "${profile_arg[@]}"
  fi

  # Publish an immutable snapshot for Temporal to invoke.
  qualified=$(aws lambda publish-version \
    --function-name "$name" \
    --description "$version ($ORDER_PROFILE profile)" \
    --region "$REGION" "${profile_arg[@]}" \
    --query 'FunctionArn' --output text)

  if [[ -n "$RESERVED_CONCURRENCY" ]]; then
    # Reserve on the function, not the published version: the limit is shared
    # across all versions of a function.
    aws lambda put-function-concurrency \
      --function-name "$name" \
      --reserved-concurrent-executions "$RESERVED_CONCURRENCY" \
      --region "$REGION" "${profile_arg[@]}" >/dev/null
    echo "  published $qualified (capped at $RESERVED_CONCURRENCY concurrent)"
  else
    echo "  published $qualified"
  fi
  echo "$version $qualified" >> "$OUT"
done

echo
echo "Published ARNs written to $OUT:"
cat "$OUT"
