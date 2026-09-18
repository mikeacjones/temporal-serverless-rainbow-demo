#!/usr/bin/env bash
# Deploy the control plane as a Temporal serverless worker on AWS Lambda.
#
# The control plane gets its own Worker Deployment, separate from the order
# deployment it manages. That separation is the point: a Workflow that decides
# which order version takes traffic must not be routed by that decision.
# Inside rainbow-orders, promoting a candidate could migrate the coordinator
# mid-rollout, and a broken candidate could take down the thing meant to
# detect it and roll back.
#
# Unlike the order workers there is one function, not one per version — the
# control plane has no versions to serve.
#
# Required:
#   EXECUTION_ROLE      IAM role ARN the function runs as
#   TEMPORAL_ADDRESS    <namespace>.<account>.tmprl.cloud:7233
#   TEMPORAL_NAMESPACE  <namespace>.<account>
#   TEMPORAL_API_KEY    for registering the Worker Deployment Version
# One of:
#   TEMPORAL_API_KEY_SECRET_ARN  Secrets Manager ARN the function reads at start
#   TEMPORAL_API_KEY             the key itself
set -euo pipefail

cd "$(dirname "$0")/../.."

: "${EXECUTION_ROLE:?set EXECUTION_ROLE to the IAM role ARN the function runs as}"
: "${TEMPORAL_ADDRESS:?set TEMPORAL_ADDRESS to your Temporal Cloud endpoint}"
: "${TEMPORAL_NAMESPACE:?set TEMPORAL_NAMESPACE to your namespace}"
: "${TEMPORAL_API_KEY:?set TEMPORAL_API_KEY so the deployment version can be registered}"

NAME=${CONTROL_FUNCTION:-rainbow-control}
CONTROL_DEPLOYMENT=${CONTROL_DEPLOYMENT_NAME:-rainbow-control}
BUILD_ID=${CONTROL_BUILD_ID:-control}
DEPLOYMENT_NAME=${TEMPORAL_DEPLOYMENT_NAME:-rainbow-orders}
REGION=${AWS_REGION:-ca-central-1}
ARCH=${ARCH:-arm64}
MEMORY=${CONTROL_MEMORY:-1024}
ZIP=${ZIP:-dist/lambda/control.zip}

# Longer than the order workers' 120s, because control Activities wait on
# things: a canary probe watches a whole order finish, and a burst batch is
# hundreds of starts. Both have to fit inside one invocation.
TIMEOUT=${CONTROL_TIMEOUT:-300}

INVOKE_ROLE_STACK=${INVOKE_ROLE_STACK:-rainbow-serverless-invoke-role}
INVOKE_ROLE_NAME=${INVOKE_ROLE_NAME:-rainbow-serverless-temporal-invoke}

[[ -f "$ZIP" ]] || { echo "missing $ZIP — run ./deploy/aws/build.sh first" >&2; exit 1; }

lambda_arch=$([[ "$ARCH" == "arm64" ]] && echo arm64 || echo x86_64)
account=$(aws sts get-caller-identity --query Account --output text --region "$REGION")

echo "=== $NAME ==="

env_vars="TEMPORAL_ADDRESS=$TEMPORAL_ADDRESS"
env_vars+=",TEMPORAL_NAMESPACE=$TEMPORAL_NAMESPACE"
env_vars+=",TEMPORAL_DEPLOYMENT_NAME=$DEPLOYMENT_NAME"
env_vars+=",CONTROL_DEPLOYMENT_NAME=$CONTROL_DEPLOYMENT"
env_vars+=",CONTROL_BUILD_ID=$BUILD_ID"
[[ -n "${TEMPORAL_API_KEY_SECRET_ARN:-}" ]] && env_vars+=",TEMPORAL_API_KEY_SECRET_ARN=$TEMPORAL_API_KEY_SECRET_ARN"

if aws lambda get-function --function-name "$NAME" --region "$REGION" >/dev/null 2>&1; then
  echo "  updating code"
  aws lambda update-function-code --function-name "$NAME" \
    --zip-file "fileb://$ZIP" --region "$REGION" >/dev/null
  aws lambda wait function-updated --function-name "$NAME" --region "$REGION"

  echo "  updating configuration"
  aws lambda update-function-configuration --function-name "$NAME" \
    --environment "Variables={$env_vars}" \
    --memory-size "$MEMORY" --timeout "$TIMEOUT" \
    --region "$REGION" >/dev/null
  aws lambda wait function-updated --function-name "$NAME" --region "$REGION"
else
  echo "  creating"
  aws lambda create-function --function-name "$NAME" \
    --runtime provided.al2023 --architectures "$lambda_arch" \
    --handler bootstrap --role "$EXECUTION_ROLE" \
    --zip-file "fileb://$ZIP" --timeout "$TIMEOUT" --memory-size "$MEMORY" \
    --environment "Variables={$env_vars}" \
    --region "$REGION" >/dev/null
  aws lambda wait function-active-v2 --function-name "$NAME" --region "$REGION"
fi

qualified=$(aws lambda publish-version --function-name "$NAME" \
  --description "control plane" --region "$REGION" \
  --query 'FunctionArn' --output text)
echo "  published $qualified"

# Temporal can only invoke what its role allows, and that policy names the
# order functions explicitly. Add this one, keeping the external ID the stack
# already holds.
echo "  allowing Temporal to invoke it"
arns="arn:aws:lambda:$REGION:$account:function:$NAME:*"
for v in ${VERSIONS:-v1 v2 v3 v4 v5}; do
  arns+=",arn:aws:lambda:$REGION:$account:function:${PREFIX:-rainbow-orders}-$v:*"
done
if out=$(aws cloudformation update-stack --stack-name "$INVOKE_ROLE_STACK" \
    --template-body "file://deploy/aws/cfn/invoke-role.yaml" \
    --capabilities CAPABILITY_NAMED_IAM --region "$REGION" \
    --parameters \
      "ParameterKey=RoleName,UsePreviousValue=true" \
      "ParameterKey=AssumeRoleExternalId,UsePreviousValue=true" \
      "ParameterKey=LambdaFunctionARNs,ParameterValue=\"$arns\"" 2>&1); then
  aws cloudformation wait stack-update-complete --stack-name "$INVOKE_ROLE_STACK" --region "$REGION"
  echo "    policy updated"
elif grep -qi "No updates are to be performed" <<<"$out"; then
  echo "    policy already covers it"
else
  echo "    $out" >&2
  exit 1
fi

# Register the control plane as its own Worker Deployment Version.
invoke_role=$(aws iam get-role --role-name "$INVOKE_ROLE_NAME" --query 'Role.Arn' --output text)
external_id=$(aws iam get-role --role-name "$INVOKE_ROLE_NAME" \
  --query 'Role.AssumeRolePolicyDocument' --output json | python3 -c "
import json,sys
for s in json.load(sys.stdin).get('Statement', []):
    v = (s.get('Condition', {}).get('StringEquals', {}) or {}).get('sts:ExternalId')
    if v:
        print(v[0] if isinstance(v, list) else v)
        break")

t=(--address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY" --tls)

# A Worker Deployment is normally created lazily, the first time a Worker
# polls with a version. A serverless Worker cannot do that: Temporal will not
# invoke it until a version is registered, and a version cannot be registered
# without the deployment. So it has to be pre-defined. Already existing is the
# normal case on redeploys and is not an error.
echo "  ensuring the $CONTROL_DEPLOYMENT deployment exists"
if out=$(temporal worker deployment create "${t[@]}" --name "$CONTROL_DEPLOYMENT" 2>&1); then
  echo "    created"
elif grep -qi "already exists" <<<"$out"; then
  echo "    already there"
else
  echo "    $out" >&2
  exit 1
fi

echo "  registering $CONTROL_DEPLOYMENT:$BUILD_ID"
if temporal worker deployment describe-version "${t[@]}" \
     --deployment-name "$CONTROL_DEPLOYMENT" --build-id "$BUILD_ID" >/dev/null 2>&1; then
  temporal worker deployment update-version-compute-config "${t[@]}" \
    --deployment-name "$CONTROL_DEPLOYMENT" --build-id "$BUILD_ID" \
    --aws-lambda-function-arn "$qualified" \
    --aws-lambda-assume-role-arn "$invoke_role" \
    --aws-lambda-assume-role-external-id "$external_id" | tail -1
else
  temporal worker deployment create-version "${t[@]}" \
    --deployment-name "$CONTROL_DEPLOYMENT" --build-id "$BUILD_ID" \
    --aws-lambda-function-arn "$qualified" \
    --aws-lambda-assume-role-arn "$invoke_role" \
    --aws-lambda-assume-role-external-id "$external_id" | tail -1
fi

# Temporal only invokes the Current version, so a control deployment with no
# Current version is a control plane that never runs.
echo "  making it current"
temporal worker deployment set-current-version "${t[@]}" \
  --deployment-name "$CONTROL_DEPLOYMENT" --build-id "$BUILD_ID" --yes 2>&1 | tail -1

echo
echo "Control plane deployed. It serves the ${CONTROL_DEPLOYMENT} deployment and"
echo "manages ${DEPLOYMENT_NAME}; the two are deliberately separate."
