#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)
work=$(mktemp -d)
trap 'rm -rf "$work"' EXIT
mkdir -p "$work/bin" "$work/output"
: > "$work/output/deploy.zip"

cat > "$work/bin/aws" <<'MOCK'
#!/usr/bin/env bash
set -euo pipefail
printf '%s' "$1 $2" >> "$MOCK_AWS_LOG"
printf ' %q' "${@:3}" >> "$MOCK_AWS_LOG"
printf '\n' >> "$MOCK_AWS_LOG"

service="$1"
operation="$2"
shift 2
case "$service/$operation" in
  lambda/get-function-configuration)
    printf '%s\n' '{"State":"Active","LastUpdateStatus":"Successful","RevisionId":"candidate-revision","CodeSha256":"47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=","FunctionArn":"arn:aws:lambda:us-east-1:123456789012:function:justhodl-khalid-risk"}'
    ;;
  lambda/publish-version)
    printf '%s\n' '42'
    ;;
  lambda/invoke)
    output="${@: -1}"
    if [ "${MOCK_VALIDATION_OK:-1}" = "1" ]; then
      printf '%s\n' '{"statusCode":200,"body":"{\"ok\":true,\"validation_only\":true,\"schema_version\":\"1.0.0\",\"status\":\"OK\",\"artifact_size_bytes\":123}"}' > "$output"
    else
      printf '%s\n' '{"statusCode":200,"body":"{\"ok\":false,\"validation_only\":true,\"schema_version\":\"1.0.0\",\"status\":\"OK\",\"artifact_size_bytes\":123}"}' > "$output"
    fi
    if [ "${MOCK_DIRECT:-0}" = 1 ]; then
      jq '.body | fromjson' "$output" > "$output.direct"
      mv "$output.direct" "$output"
    fi
    printf '{"StatusCode":200,"ExecutedVersion":"%s"}\n' "${MOCK_EXECUTED_VERSION:-42}"
    ;;
  lambda/get-alias)
    if [ "${MOCK_ALIAS_EXISTS:-1}" = "1" ]; then
      printf '%s\n' '{"FunctionVersion":"7","RevisionId":"old-alias-revision","RoutingConfig":{"AdditionalVersionWeights":{"8":0.2}}}'
    else
      echo 'ResourceNotFoundException: alias absent' >&2
      exit 254
    fi
    ;;
  lambda/update-alias)
    printf '%s\n' 'promoted-alias-revision'
    ;;
  lambda/create-alias)
    printf '%s\n' 'promoted-alias-revision'
    ;;
  lambda/delete-alias)
    ;;
  scheduler/get-schedule)
    printf '%s\n' '{"Name":"justhodl-khalid-risk-hourly","GroupName":"default","State":"DISABLED","ScheduleExpression":"rate(1 hour)","FlexibleTimeWindow":{"Mode":"FLEXIBLE","MaximumWindowInMinutes":5},"StartDate":"2026-01-01T00:00:00Z","Target":{"Arn":"arn:aws:lambda:us-east-1:123456789012:function:justhodl-khalid-risk:live","RoleArn":"original-role","Input":"{\"mode\":\"research\"}","DeadLetterConfig":{"Arn":"original-dlq"},"RetryPolicy":{"MaximumRetryAttempts":9,"MaximumEventAgeInSeconds":7200}}}'
    ;;
  scheduler/update-schedule)
    test "$1" = --cli-input-json
    jq -e '.State == "DISABLED" and .Target.Input == "{\"mode\":\"research\"}" and .Target.DeadLetterConfig.Arn == "original-dlq" and .Target.RetryPolicy.MaximumRetryAttempts == 9 and .FlexibleTimeWindow.MaximumWindowInMinutes == 5 and .StartDate == "2026-01-01T00:00:00Z" and (.Target.Arn|endswith(":live"))' "${2#file://}" >/dev/null
    if [ "${MOCK_SCHEDULE_FAIL:-0}" = "1" ]; then
      echo 'simulated schedule failure' >&2
      exit 71
    fi
    ;;
  *)
    echo "unexpected mock AWS call: $service/$operation" >&2
    exit 90
    ;;
esac
MOCK
chmod +x "$work/bin/aws"

cat > "$work/config.json" <<'JSON'
{
  "eventbridge_scheduler": {
    "schedule_name": "justhodl-khalid-risk-hourly",
    "cron": "cron(20 * * * ? *)",
    "timezone": "UTC",
    "role_arn": "arn:aws:iam::123456789012:role/scheduler",
    "description": "test schedule"
  }
}
JSON

run_candidate() {
  PATH="$work/bin:$PATH" \
  MOCK_AWS_LOG="$work/aws.log" \
  MOCK_DIRECT="${MOCK_DIRECT:-0}" \
  MOCK_EXECUTED_VERSION="${MOCK_EXECUTED_VERSION:-42}" \
  MOCK_VALIDATION_OK="${MOCK_VALIDATION_OK:-1}" \
  MOCK_ALIAS_EXISTS="${MOCK_ALIAS_EXISTS:-1}" \
  MOCK_SCHEDULE_FAIL="${MOCK_SCHEDULE_FAIL:-0}" \
    bash "$root/scripts/deploy_validated_candidate.sh" \
      justhodl-khalid-risk us-east-1 "$work/output" "${MOCK_CONFIG:-$work/config.json}" 1.0.0
}

# Success: the exact pins and numbered qualifier are used, and Scheduler sees
# only the stable live alias after validation and promotion.
: > "$work/aws.log"
run_candidate > "$work/success.out"
publish_line=$(grep -n '^lambda publish-version ' "$work/aws.log" | cut -d: -f1)
invoke_line=$(grep -n '^lambda invoke ' "$work/aws.log" | cut -d: -f1)
promote_line=$(grep -n '^lambda update-alias ' "$work/aws.log" | head -1 | cut -d: -f1)
schedule_line=$(grep -n '^scheduler update-schedule ' "$work/aws.log" | cut -d: -f1)
test "$publish_line" -lt "$invoke_line"
test "$invoke_line" -lt "$promote_line"
test "$promote_line" -lt "$schedule_line"
grep -q -- '--revision-id candidate-revision' "$work/aws.log"
grep -q -- '--code-sha256 47DEQpj8HBSa+/TImW+5JCeuQeRkm5NMpJWZG3hSuFU=' "$work/aws.log"
grep -q -- '--qualifier 42' "$work/aws.log"
grep -q -- '--routing-config' "$work/aws.log"

# Failed validation must not move the alias or touch Scheduler.
: > "$work/aws.log"
if MOCK_VALIDATION_OK=0 run_candidate > "$work/invalid.out" 2>&1; then
  echo "invalid candidate unexpectedly succeeded" >&2
  exit 1
fi
! grep -q '^lambda update-alias ' "$work/aws.log"
! grep -q '^lambda create-alias ' "$work/aws.log"
! grep -q '^scheduler .*schedule ' "$work/aws.log"

# A post-promotion Scheduler failure restores the prior alias version.
: > "$work/aws.log"
if MOCK_SCHEDULE_FAIL=1 run_candidate > "$work/rollback.out" 2>&1; then
  echo "schedule failure unexpectedly succeeded" >&2
  exit 1
fi
test "$(grep -c '^lambda update-alias ' "$work/aws.log")" -eq 2
grep -q -- '--function-version 7' "$work/aws.log"
grep -q -- '--revision-id promoted-alias-revision' "$work/aws.log"

# DeleteAlias has no CAS parameter: retain the already-validated new alias on failure.
: > "$work/aws.log"
if MOCK_ALIAS_EXISTS=0 MOCK_SCHEDULE_FAIL=1 run_candidate > "$work/delete.out" 2>&1; then
  echo "new-alias schedule failure unexpectedly succeeded" >&2
  exit 1
fi
grep -q '^lambda create-alias ' "$work/aws.log"
! grep -q '^lambda delete-alias ' "$work/aws.log"
grep -q 'retained for reconciliation' "$work/delete.out"

# Wrong zip bytes abort before publishing or invoking anything.
: > "$work/aws.log"
printf 'changed zip' > "$work/output/deploy.zip"
if run_candidate > "$work/hash.out" 2>&1; then exit 1; fi
! grep -q '^lambda publish-version ' "$work/aws.log"
! grep -q '^lambda invoke ' "$work/aws.log"
: > "$work/output/deploy.zip"

# Direct metadata is the production contract of Katlin/Sizer/Risk Gate.
: > "$work/aws.log"
MOCK_DIRECT=1 run_candidate > "$work/direct.out"
grep -q '^lambda update-alias ' "$work/aws.log"

# A successful-looking response from a different executed version is invalid.
: > "$work/aws.log"
if MOCK_EXECUTED_VERSION=41 run_candidate > "$work/version.out" 2>&1; then exit 1; fi
! grep -q '^lambda update-alias ' "$work/aws.log"

# No optional config and no Scheduler are both supported without guessed changes.
: > "$work/aws.log"
MOCK_CONFIG="$work/missing-config.json" run_candidate > "$work/no-config.out"
! grep -q '^scheduler ' "$work/aws.log"
printf '{}\n' > "$work/empty-config.json"
: > "$work/aws.log"
MOCK_CONFIG="$work/empty-config.json" run_candidate > "$work/no-schedule.out"
! grep -q '^scheduler ' "$work/aws.log"

echo "Validated candidate shell tests passed: 9"
