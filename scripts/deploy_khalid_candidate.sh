#!/usr/bin/env bash
set -euo pipefail

fn="$1"
region="$2"
tmp="$3"
dir="$4"

echo "Invoking Khalid candidate and validating its published artifact"
aws lambda invoke \
  --function-name "$fn" \
  --region "$region" \
  --cli-read-timeout 310 \
  "$tmp/khalid-invoke.json" > "$tmp/khalid-invoke-meta.json"

if [ "$(jq -r '.FunctionError // empty' "$tmp/khalid-invoke-meta.json" 2>/dev/null)" != "" ]; then
  echo "::error::Khalid candidate returned FunctionError; live alias and schedule are unchanged"
  cat "$tmp/khalid-invoke.json"
  exit 1
fi

aws s3 cp \
  "s3://justhodl-dashboard-live/data/khalid.json" \
  "$tmp/khalid.json" \
  --region "$region" > /dev/null
python3 "$dir/tests/validate_artifact.py" "$tmp/khalid.json"

candidate_version=$(aws lambda publish-version \
  --function-name "$fn" \
  --region "$region" \
  --query 'Version' --output text)

if aws lambda get-alias --function-name "$fn" --name live --region "$region" >/dev/null 2>&1; then
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
echo "  ✅ Khalid live alias promoted to version $candidate_version"
