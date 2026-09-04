#!/usr/bin/env bash
set -euo pipefail

fn="$1"
region="$2"
tmp="$3"
dir="$4"

printf '{"mode":"validate_only"}' > "$tmp/khalid-validation-event.json"
echo "Invoking Khalid candidate in read-only validation mode"
aws lambda invoke \
  --function-name "$fn" \
  --region "$region" \
  --cli-read-timeout 310 \
  --payload "fileb://$tmp/khalid-validation-event.json" \
  "$tmp/khalid-invoke.json" > "$tmp/khalid-invoke-meta.json"

if [ "$(jq -r '.FunctionError // empty' "$tmp/khalid-invoke-meta.json" 2>/dev/null)" != "" ]; then
  echo "::error::Khalid candidate returned FunctionError; live alias and schedule are unchanged"
  cat "$tmp/khalid-invoke.json"
  exit 1
fi

jq -r '.body' "$tmp/khalid-invoke.json" \
  | jq -e 'select(.validation_only == true) | .artifact' \
  > "$tmp/khalid.json"
python3 "$dir/tests/validate_artifact.py" "$tmp/khalid.json"

candidate_version=$(aws lambda publish-version \
  --function-name "$fn" \
  --region "$region" \
  --query 'Version' --output text)

previous_version=""
alias_existed=0
if previous_version=$(aws lambda get-alias \
  --function-name "$fn" --name live --region "$region" \
  --query 'FunctionVersion' --output text 2>/dev/null); then
  alias_existed=1
fi

backup_dir="$tmp/khalid-production-backup"
mkdir -p "$backup_dir"
production_keys=(
  "data/khalid.json"
  "data/khalid-candidates.json"
  "data/history/khalid.json"
)
for key in "${production_keys[@]}"; do
  backup_file="$backup_dir/${key//\//__}"
  backup_error="$backup_file.error"
  if aws s3 cp \
    "s3://justhodl-dashboard-live/$key" \
    "$backup_file" \
    --region "$region" > /dev/null 2>"$backup_error"; then
    rm -f "$backup_error"
  elif grep -Eq '(^|[^0-9])404([^0-9]|$)|NoSuchKey|Not Found' "$backup_error"; then
    rm -f "$backup_error"
    touch "$backup_file.missing"
  else
    echo "::error::Could not safely back up s3://justhodl-dashboard-live/$key; promotion aborted"
    cat "$backup_error"
    exit 1
  fi
done

promoted=0
rollback_alias() {
  failure_status=$?
  if [ "$failure_status" -eq 0 ]; then
    failure_status=1
  fi
  trap - ERR
  set +e
  if [ "$promoted" -ne 1 ]; then
    exit "$failure_status"
  fi
  restore_failed=0
  if [ "$alias_existed" -eq 1 ]; then
    aws lambda update-alias \
      --function-name "$fn" --name live \
      --function-version "$previous_version" \
      --region "$region" --output text > /dev/null || restore_failed=1
  else
    aws lambda delete-alias \
      --function-name "$fn" --name live --region "$region" || restore_failed=1
  fi
  for key in "${production_keys[@]}"; do
    backup_file="$backup_dir/${key//\//__}"
    if [ -f "$backup_file" ]; then
      aws s3 cp \
        "$backup_file" \
        "s3://justhodl-dashboard-live/$key" \
        --region "$region" > /dev/null || restore_failed=1
    else
      aws s3 rm \
        "s3://justhodl-dashboard-live/$key" \
        --region "$region" > /dev/null || restore_failed=1
    fi
  done
  if [ "$restore_failed" -ne 0 ]; then
    echo "::error::Khalid deployment failed and at least one rollback operation also failed"
  else
    echo "::error::Khalid live alias and authoritative artifacts rolled back after post-promotion failure"
  fi
  exit "$failure_status"
}
trap rollback_alias ERR

if [ "$alias_existed" -eq 1 ]; then
  aws lambda update-alias \
    --function-name "$fn" --name live \
    --function-version "$candidate_version" \
    --region "$region" --output text > /dev/null
else
  aws lambda create-alias \
    --function-name "$fn" --name live \
    --function-version "$candidate_version" \
    --description "Last schema-validated Khalid release" \
    --region "$region" --output text > /dev/null
fi
promoted=1

echo "Invoking promoted Khalid alias and validating the authoritative artifact"
aws lambda invoke \
  --function-name "${fn}:live" \
  --region "$region" \
  --cli-read-timeout 310 \
  "$tmp/khalid-live-invoke.json" > "$tmp/khalid-live-invoke-meta.json"

if [ "$(jq -r '.FunctionError // empty' "$tmp/khalid-live-invoke-meta.json")" != "" ]; then
  echo "::error::Promoted Khalid alias returned FunctionError"
  cat "$tmp/khalid-live-invoke.json"
  false
fi

aws s3 cp \
  "s3://justhodl-dashboard-live/data/khalid.json" \
  "$tmp/khalid-live.json" \
  --region "$region" > /dev/null
python3 "$dir/tests/validate_artifact.py" "$tmp/khalid-live.json"
trap - ERR
echo "  ✅ Khalid live alias promoted to version $candidate_version"
