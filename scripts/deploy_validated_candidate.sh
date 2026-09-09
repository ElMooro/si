#!/usr/bin/env bash
#
# Publish and promote a Lambda candidate without exposing an unvalidated
# revision to a stable alias or EventBridge Scheduler.
#
# Usage:
#   deploy_validated_candidate.sh FUNCTION REGION TMP_DIR CONFIG EXPECTED_SCHEMA
#
# The Lambda must implement the dependency-free validation contract returned by
# {"mode":"validate_only"}:
#   statusCode=200 and a JSON body containing ok=true, validation_only=true,
#   the expected schema_version, a non-empty status, and artifact_size_bytes.
set -euo pipefail

if [ "$#" -ne 5 ]; then
  echo "usage: $0 FUNCTION REGION TMP_DIR CONFIG EXPECTED_SCHEMA" >&2
  exit 2
fi

fn="$1"
region="$2"
tmp="$3"
config="$4"
expected_schema="$5"
alias_name="live"

mkdir -p "$tmp"

# Some engines use existing classic rules or schedules managed outside config.
# protect_lambda_alias.py already pinned those targets before staging $LATEST.
if jq -e '.eventbridge_scheduler' "$config" >/dev/null; then
  jq -e '.eventbridge_scheduler | (.schedule_name|type=="string" and length>0) and (.cron|type=="string" and length>0) and (.role_arn|type=="string" and length>0)' "$config" >/dev/null
fi

validation_event="$tmp/${fn}-validation-event.json"
validation_payload="$tmp/${fn}-validation-payload.json"
validation_meta="$tmp/${fn}-validation-meta.json"
printf '%s\n' '{"mode":"validate_only"}' > "$validation_event"

# One configuration read keeps RevisionId, CodeSha256, and FunctionArn from the
# same $LATEST snapshot. publish-version rejects the request if either pin has
# changed, rather than publishing a revision other than the one CI inspected.
candidate_info=$(aws lambda get-function-configuration \
  --function-name "$fn" \
  --region "$region" \
  --output json)
candidate_revision=$(jq -er '.RevisionId | select(type == "string" and length > 0)' <<<"$candidate_info")
candidate_sha=$(jq -er '.CodeSha256 | select(type == "string" and length > 0)' <<<"$candidate_info")
function_arn=$(jq -er '.FunctionArn | select(type == "string" and length > 0)' <<<"$candidate_info")

# Pin to the artifact built by this exact workflow, not merely whatever happens
# to be in $LATEST after a concurrent configuration/code mutation.
[ -f "$tmp/deploy.zip" ] || { echo "::error::Candidate zip missing"; exit 1; }
expected_sha=$(openssl dgst -sha256 -binary "$tmp/deploy.zip" | openssl base64 -A)
[ "$candidate_sha" = "$expected_sha" ] || { echo "::error::Candidate code hash differs from the reviewed zip"; exit 1; }
validation_timeout=$(jq -r '(.timeout // 900) + 60' "$config")

candidate_version=$(aws lambda publish-version \
  --function-name "$fn" \
  --region "$region" \
  --revision-id "$candidate_revision" \
  --code-sha256 "$candidate_sha" \
  --query 'Version' --output text)
case "$candidate_version" in
  ''|*[!0-9]*|0)
    echo "::error::$fn publish-version returned a non-numbered version: $candidate_version"
    exit 1
    ;;
esac

echo "Invoking pinned $fn candidate version $candidate_version in read-only validation mode"
aws lambda invoke \
  --function-name "$fn" \
  --qualifier "$candidate_version" \
  --region "$region" \
  --cli-read-timeout "$validation_timeout" \
  --cli-binary-format raw-in-base64-out \
  --payload "fileb://$validation_event" \
  "$validation_payload" > "$validation_meta"

if jq -e '.FunctionError != null' "$validation_meta" > /dev/null 2>&1; then
  echo "::error::$fn candidate returned FunctionError; live alias and schedule are unchanged"
  echo "Validation response withheld; inspect private runner logs for source errors"
  exit 1
fi

if ! jq -e --arg schema "$expected_schema" '
  .statusCode == 200
  and (
    (.body | fromjson? // {}) as $body
    | $body.ok == true
      and $body.validation_only == true
      and $body.schema_version == $schema
      and ($body.status | type == "string" and length > 0)
      and ($body.artifact_size_bytes | type == "number" and . >= 0)
  )
' "$validation_payload" > /dev/null; then
  echo "::error::$fn candidate returned an invalid validation envelope; live alias and schedule are unchanged"
  echo "Validation response withheld; inspect private runner logs for source errors"
  exit 1
fi

# Capture both the old version and alias revision immediately before promotion.
# Revision pins prevent this workflow from overwriting a concurrent alias edit.
alias_existed=0
previous_version=""
previous_alias_revision=""
alias_state="$tmp/${fn}-previous-alias.json"
alias_error="$tmp/${fn}-previous-alias.error"
if aws lambda get-alias \
  --function-name "$fn" \
  --name "$alias_name" \
  --region "$region" \
  --output json > "$alias_state" 2> "$alias_error"; then
  alias_existed=1
  previous_version=$(jq -er '.FunctionVersion' "$alias_state")
  previous_alias_revision=$(jq -er '.RevisionId' "$alias_state")
elif grep -q 'ResourceNotFoundException' "$alias_error"; then
  rm -f "$alias_error"
else
  echo "::error::Could not read $fn:$alias_name; promotion aborted"
  cat "$alias_error"
  exit 1
fi

promoted=0
promoted_alias_revision=""
rollback_alias() {
  failure_status=$?
  [ "$failure_status" -ne 0 ] || failure_status=1
  trap - ERR
  set +e

  if [ "$promoted" -eq 1 ]; then
    restore_failed=0
    if [ "$alias_existed" -eq 1 ]; then
      aws lambda update-alias \
        --function-name "$fn" \
        --name "$alias_name" \
        --function-version "$previous_version" \
        --revision-id "$promoted_alias_revision" \
        --region "$region" \
        --output text > /dev/null || restore_failed=1
    else
      aws lambda delete-alias \
        --function-name "$fn" \
        --name "$alias_name" \
        --revision-id "$promoted_alias_revision" \
        --region "$region" > /dev/null || restore_failed=1
    fi

    if [ "$restore_failed" -ne 0 ]; then
      echo "::error::$fn deployment failed and its previous live alias could not be restored"
    else
      echo "::error::$fn deployment failed; its previous live alias was restored"
    fi
  fi
  exit "$failure_status"
}
trap rollback_alias ERR

if [ "$alias_existed" -eq 1 ]; then
  promoted_alias_revision=$(aws lambda update-alias \
    --function-name "$fn" \
    --name "$alias_name" \
    --function-version "$candidate_version" \
    --revision-id "$previous_alias_revision" \
    --region "$region" \
    --query 'RevisionId' --output text)
else
  promoted_alias_revision=$(aws lambda create-alias \
    --function-name "$fn" \
    --name "$alias_name" \
    --function-version "$candidate_version" \
    --description "Last schema-validated $fn release" \
    --region "$region" \
    --query 'RevisionId' --output text)
fi
promoted=1

# Scheduler mutation deliberately occurs only after the numbered candidate has
# passed validation and the stable alias has moved. The schedule never targets
# $LATEST or a transient numbered version.
if jq -e '.eventbridge_scheduler' "$config" >/dev/null; then
sched_name=$(jq -er '.eventbridge_scheduler.schedule_name' "$config")
sched_cron=$(jq -er '.eventbridge_scheduler.cron' "$config")
sched_tz=$(jq -r '.eventbridge_scheduler.timezone // "UTC"' "$config")
sched_role=$(jq -er '.eventbridge_scheduler.role_arn' "$config")
sched_desc=$(jq -r '.eventbridge_scheduler.description // "Scheduled run"' "$config")
target_json=$(jq -n \
  --arg arn "${function_arn}:${alias_name}" \
  --arg role "$sched_role" \
  '{Arn:$arn,RoleArn:$role,Input:"{}",RetryPolicy:{MaximumRetryAttempts:2,MaximumEventAgeInSeconds:3600}}')

schedule_state="$tmp/${fn}-schedule.json"
schedule_error="$tmp/${fn}-schedule.error"
if aws scheduler get-schedule \
  --name "$sched_name" \
  --region "$region" \
  --output json > "$schedule_state" 2> "$schedule_error"; then
  aws scheduler update-schedule \
    --name "$sched_name" \
    --schedule-expression "$sched_cron" \
    --schedule-expression-timezone "$sched_tz" \
    --flexible-time-window '{"Mode":"OFF"}' \
    --state ENABLED \
    --description "$sched_desc" \
    --target "$target_json" \
    --region "$region" --output text > /dev/null
elif grep -q 'ResourceNotFoundException' "$schedule_error"; then
  aws scheduler create-schedule \
    --name "$sched_name" \
    --schedule-expression "$sched_cron" \
    --schedule-expression-timezone "$sched_tz" \
    --flexible-time-window '{"Mode":"OFF"}' \
    --state ENABLED \
    --description "$sched_desc" \
    --target "$target_json" \
    --region "$region" --output text > /dev/null
else
  echo "::error::Could not read EventBridge Scheduler schedule $sched_name"
  cat "$schedule_error"
  false
fi

fi
trap - ERR
proof_dir="${GITHUB_WORKSPACE:-$tmp}/release-evidence"
mkdir -p "$proof_dir"
jq -n --arg function "$fn" --arg version "$candidate_version" --arg code_sha256 "$candidate_sha" --arg schema "$expected_schema" --arg commit "${GITHUB_SHA:-local-test}" '{function:$function,version:$version,code_sha256:$code_sha256,validation_schema:$schema,commit_sha:$commit,validation_only:true,alias:"live"}' > "$proof_dir/$fn.json"
echo "  ✅ $fn live alias promoted to validated version $candidate_version"

