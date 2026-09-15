#!/usr/bin/env bash
# Remove the serverless side of the demo.
#
# Worth doing between demos. Temporal keeps a warm pool of Lambda workers on
# whichever version is Current — so that live orders never pay a cold start —
# which means the deployment does not fall to zero cost while it is registered.
# Roughly 18 warm workers at 512MB is about $13/day. Tearing down is cheap
# because deploy.sh rebuilds it in a couple of minutes.
#
# By default this leaves the Temporal Cloud namespace and the API key alone, so
# only the AWS side goes away. Pass --all to remove the Worker Deployment
# Versions too.
set -euo pipefail

cd "$(dirname "$0")/../.."

REGION=${AWS_REGION:-ca-central-1}
PREFIX=${PREFIX:-rainbow-orders}
VERSIONS=${VERSIONS:-"v1 v2 v3 v4 v5"}
profile_arg=()
[[ -n "${AWS_PROFILE:-}" ]] && profile_arg=(--profile "$AWS_PROFILE")

remove_versions=false
[[ "${1:-}" == "--all" ]] && remove_versions=true

if $remove_versions; then
  : "${TEMPORAL_ADDRESS:?set TEMPORAL_ADDRESS to remove Worker Deployment Versions}"
  : "${TEMPORAL_NAMESPACE:?set TEMPORAL_NAMESPACE to remove Worker Deployment Versions}"
  : "${TEMPORAL_API_KEY:?set TEMPORAL_API_KEY to remove Worker Deployment Versions}"

  echo "Removing Worker Deployment Versions…"
  # A Current or Ramping version cannot be deleted, so stand the routing down
  # to unversioned first.
  temporal worker deployment set-current-version \
    --address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY" \
    --deployment-name "$PREFIX" --unversioned --yes 2>/dev/null \
    || echo "  (could not unset the current version; delete-version may refuse)"

  for version in $VERSIONS; do
    printf '  %s: ' "$version"
    temporal worker deployment delete-version \
      --address "$TEMPORAL_ADDRESS" --namespace "$TEMPORAL_NAMESPACE" --api-key "$TEMPORAL_API_KEY" \
      --deployment-name "$PREFIX" --build-id "$version" --yes 2>&1 | tail -1
  done
fi

echo "Deleting Lambda functions…"
for version in $VERSIONS; do
  name="$PREFIX-$version"
  if aws lambda delete-function --function-name "$name" --region "$REGION" "${profile_arg[@]}" 2>/dev/null; then
    echo "  deleted $name"
  else
    echo "  $name already gone"
  fi
done

echo "Deleting CloudFormation stacks…"
for stack in rainbow-serverless-invoke-role rainbow-serverless-execution-role; do
  if aws cloudformation delete-stack --stack-name "$stack" --region "$REGION" "${profile_arg[@]}" 2>/dev/null; then
    echo "  deleting $stack"
    aws cloudformation wait stack-delete-complete --stack-name "$stack" --region "$REGION" "${profile_arg[@]}" 2>/dev/null || true
  fi
done

cat <<'NOTE'

Left in place on purpose:
  - the Secrets Manager secret holding the Temporal API key
  - the Temporal Cloud namespace and service account
  - the sa-demo Kubernetes deployment

Remove those separately if you are finished with the demo entirely:
  kubectl delete -k deploy/k8s/sa-demo
  aws secretsmanager delete-secret --secret-id temporal/michaelj-rainbow-serverless/api-key --region ca-central-1
  tcld namespace delete -n <namespace>
NOTE
