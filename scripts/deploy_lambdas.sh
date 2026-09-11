#!/usr/bin/env bash
# The reviewed deployment transaction lives outside YAML to avoid GitHub expression size limits.
: "${DEPLOY_TARGETS:?deployment target list required}"
: "${DEPLOY_AWS_REGION:?AWS region required}"
set -e
# Do not put the subshell in an OR/if condition: Bash would disable
# errexit throughout its body and continue after failed validation.
declare -a failed_lambdas
python3 scripts/validate_lambda_configs.py $DEPLOY_TARGETS
DEPLOY_TARGETS=$(python3 scripts/release_order.py order $DEPLOY_TARGETS)
for fn in $DEPLOY_TARGETS; do
  caller_phase=0
  if python3 scripts/release_order.py is-caller "$fn"; then caller_phase=1; fi
  set +e
  (
    set -e
    dir="aws/lambdas/$fn"
    if [ ! -d "$dir/source" ]; then
      if [ "$caller_phase" -eq 1 ]; then
        echo "::error::Required caller source missing for $fn; producer staging blocked"
        exit 1
      fi
      echo "::warning::$dir/source not found — skipping"
      exit 0
    fi
    echo "──── Deploying $fn ────"
    tmp=$(mktemp -d)
    config_file="$dir/config.json"
    if [ -f "$config_file" ]; then
      python3 scripts/normalize_lambda_config.py "$config_file" > "$tmp/config.json"
      config_file="$tmp/config.json"
      jq -c --arg function "$fn" 'select(.release_schedule_note != null) | {phase:"schedule_configuration",function:$function} + .release_schedule_note' "$config_file"
    fi

  # Read config (function_name + create-time config like runtime/timeout/memory/env)
  fn_runtime="python3.12"
  fn_timeout="300"
  fn_memory="512"
  fn_ephemeral=""
  fn_desc="JustHodl.AI Lambda"
  fn_env_args=""
  cfg_env_json="{}"
  architecture_create_args=()
  architecture_update_args=()
  if [ -f "$config_file" ]; then
    cfg_name=$(jq -r '.function_name // empty' "$config_file")
    [ -n "$cfg_name" ] && fn="$cfg_name"
    fn_runtime=$(jq -r '.runtime // "python3.12"' "$config_file")
    fn_timeout=$(jq -r '.timeout // 300' "$config_file")
    fn_memory=$(jq -r '.memory // 512' "$config_file")
    fn_ephemeral=$(jq -r '.ephemeral_storage // empty' "$config_file")
    fn_desc=$(jq -r '.description // "JustHodl.AI Lambda"' "$config_file")
    cfg_env_json=$(python3 scripts/lambda_config_environment.py "$config_file" "$DEPLOY_AWS_REGION")
    create_architecture=$(python3 scripts/lambda_architecture.py "$config_file" create)
    update_architecture=$(python3 scripts/lambda_architecture.py "$config_file" update)
    if [ -n "$create_architecture" ]; then architecture_create_args=(--architectures "$create_architecture"); fi
    if [ -n "$update_architecture" ]; then architecture_update_args=(--architectures "$update_architecture"); fi
  fi

  staging="$tmp/stage"
  mkdir -p "$staging"

  if [ -d aws/shared ]; then
    find aws/shared -maxdepth 1 -name '*.py' -type f -exec cp {} "$staging/" \;
  fi

  cp -rT "$dir/source" "$staging"

  (cd "$staging" && zip -qr "$tmp/deploy.zip" .)
  echo "Built $(du -h $tmp/deploy.zip | cut -f1) zip for $fn (with shared/ bundle)"

  if [ -d aws/shared ]; then
    zip_names=$(python3 -c "import zipfile,sys;print('\n'.join(zipfile.ZipFile(sys.argv[1]).namelist()))" "$tmp/deploy.zip")
    vmissing=""
    for sp in aws/shared/*.py; do
      [ -e "$sp" ] || continue
      vmod=$(basename "$sp" .py)
      if grep -rqE "(from|import)[[:space:]]+${vmod}([^a-zA-Z0-9_]|\$)" "$dir/source" 2>/dev/null; then
        echo "$zip_names" | grep -qx "${vmod}.py" || vmissing="$vmissing ${vmod}.py"
      fi
    done
    if [ -n "$vmissing" ]; then
      echo "::error::$fn imports shared module(s) missing from its zip:$vmissing — bundling regression, failing build for $fn"
      exit 1
    fi
  fi

  candidate_managed=0
  candidate_schema=""
  if [ "$fn" = "justhodl-engine-fusion" ] || [ "$fn" = "justhodl-khalid-risk" ]; then
    schema_file="$dir/source/output_schema.json"
    [ "$fn" = "justhodl-engine-fusion" ] && schema_file="$dir/source/fusion-schema.v1.json"
    candidate_schema=$(jq -er '.schema_version' "$schema_file")
    candidate_managed=1
  elif [ -f "$config_file" ] && jq -e '.release_validation.schema_version' "$config_file" >/dev/null; then
    candidate_schema=$(jq -er '.release_validation.schema_version' "$config_file")
    candidate_managed=1
  fi

  # Check if Lambda exists. If not, create it with defaults / config.json overrides.
  if aws lambda get-function --function-name "$fn" --region "$DEPLOY_AWS_REGION" >/dev/null 2>&1; then
    echo "Updating existing Lambda $fn"
    # Freeze all existing scheduled targets on the previous numbered release.
    code_revision_args=()
    if [ "$candidate_managed" -eq 1 ]; then
      python3 scripts/protect_lambda_alias.py "$fn" "$DEPLOY_AWS_REGION" > "$tmp/production-protection.json"
      code_revision_args=(--revision-id "$(jq -er '.revision_id' "$tmp/production-protection.json")")
    fi
    aws lambda update-function-code \
      --function-name "$fn" \
      "${code_revision_args[@]}" \
      "${architecture_update_args[@]}" \
      --zip-file "fileb://$tmp/deploy.zip" \
      --region "$DEPLOY_AWS_REGION" \
      --query 'LastModified' --output text

    aws lambda wait function-updated \
      --function-name "$fn" \
      --region "$DEPLOY_AWS_REGION"
    # ── Deploy lane v2 (2026-09-11): a green step is not proof. Prove the live
    # CodeSha256 equals the zip built from this checkout, then publish a receipt
    # to S3 (data/ops/releases/<fn>.json) that any lane can verify over HTTPS.
    local_code_sha=$(openssl dgst -sha256 -binary "$tmp/deploy.zip" | base64 -w0)
    live_code_sha=$(aws lambda get-function-configuration \
      --function-name "$fn" --region "$DEPLOY_AWS_REGION" \
      --query 'CodeSha256' --output text)
    if [ "$local_code_sha" != "$live_code_sha" ]; then
      echo "::error::$fn: live CodeSha256 $live_code_sha != built zip $local_code_sha -- AWS is NOT running this commit"
      exit 1
    fi
    echo "  ✅ CodeSha256 verified ($live_code_sha)"
    DEPLOY_COMMIT="${DEPLOY_COMMIT:-$(git rev-parse HEAD)}" DEPLOY_RUN_ID="${DEPLOY_RUN_ID:-}" \
    DEPLOY_WORKFLOW="${DEPLOY_WORKFLOW:-deploy-lambdas.yml}" DEPLOY_ACTOR="${DEPLOY_ACTOR:-}" \
      python3 scripts/release_receipt.py "$fn" "$tmp/deploy.zip" "$dir/source" "$live_code_sha" \
      || echo "::warning::$fn: release receipt not published"

    # Apply config overrides if present (env vars, timeout, memory may have changed)
    if [ -f "$config_file" ]; then
      # MERGE env: the function's CURRENT env is the base, config.json
      # env overrides on top. This preserves ops-patched secrets
      # (FMP_KEY, TELEGRAM_*, etc.) across redeploys — config.json need
      # only declare non-secret vars like MAX_WORKERS.
      #
      # Hardened (2026-07-07 incident): a FAILED env read must never be
      # treated as "empty env" — that turned the merge into a replace
      # and nuked justhodl-equity-research down to its cfg-inherited
      # vars. On read failure we now SKIP the config update entirely
      # (code is already deployed; env/timeout can wait) and surface a
      # warning instead of silently destroying the secrets bundle.
      env_read_ok=1
      existing_env=$(aws lambda get-function-configuration \
        --function-name "$fn" --region "$DEPLOY_AWS_REGION" \
        --query 'Environment.Variables' --output json 2>/dev/null) || env_read_ok=0
      if [ "$env_read_ok" = "0" ]; then
        echo "::error::$fn: env read failed; refusing promotion with unapplied configuration"
        exit 1
      else
      if [ -z "$existing_env" ] || [ "$existing_env" = "null" ]; then
        existing_env="{}"
      fi
      merged_env=$(jq -n --argjson a "$existing_env" --argjson b "$cfg_env_json" '$a * $b')
      # Skip --environment when empty (AWS rejects an empty Variables map).
      env_arg=""
      if [ "$merged_env" != "{}" ] && [ -n "$merged_env" ]; then
        echo "{\"Variables\": $merged_env}" > "$tmp/env.json"
        env_arg="--environment file://$tmp/env.json"
      fi
      ephemeral_arg=""
      if [ -n "$fn_ephemeral" ]; then
        ephemeral_arg="--ephemeral-storage Size=$fn_ephemeral"
      fi
      # Minimal validation-only config files must not reset runtime settings.
      config_args=()
      # Runtime upgrades require an explicit opt-in; imported legacy metadata alone
      # must not downgrade a runtime that operations already upgraded.
      # Runtime is historically a create-time field. Upgrades require an explicit
      # reviewed flag so old imported metadata cannot downgrade other functions.
      if jq -e '.update_runtime == true' "$config_file" >/dev/null; then
        config_args+=(--runtime "$fn_runtime")
      fi
      if jq -e 'has("timeout")' "$config_file" >/dev/null; then config_args+=(--timeout "$fn_timeout"); fi
      if jq -e 'has("memory")' "$config_file" >/dev/null; then config_args+=(--memory-size "$fn_memory"); fi
      if jq -e 'has("description")' "$config_file" >/dev/null; then config_args+=(--description "$fn_desc"); fi
      python3 scripts/secret_lambda_config.py update-function-configuration "$fn" \
        "${config_args[@]}" \
        --region "$DEPLOY_AWS_REGION" \
        $env_arg \
        $ephemeral_arg \
        --output text > /dev/null
      aws lambda wait function-updated \
        --function-name "$fn" \
        --region "$DEPLOY_AWS_REGION"
      fi
    fi
  else
    echo "Lambda $fn does not exist — creating"
    # Only pass --environment when there are real vars; AWS rejects an
    # empty Variables map on create, which silently failed no-env engines.
    env_arg=""
    if [ "$cfg_env_json" != "{}" ] && [ -n "$cfg_env_json" ]; then
      echo "{\"Variables\": $cfg_env_json}" > "$tmp/env.json"
      env_arg="--environment file://$tmp/env.json"
    fi
    ephemeral_arg=""
    if [ -n "$fn_ephemeral" ]; then
      ephemeral_arg="--ephemeral-storage Size=$fn_ephemeral"
    fi
    python3 scripts/secret_lambda_config.py create-function "$fn" \
      "${architecture_create_args[@]}" \
      --runtime "$fn_runtime" \
      --role "arn:aws:iam::857687956942:role/lambda-execution-role" \
      --handler "lambda_function.lambda_handler" \
      --zip-file "fileb://$tmp/deploy.zip" \
      --timeout "$fn_timeout" \
      --memory-size "$fn_memory" \
      --description "$fn_desc" \
      --region "$DEPLOY_AWS_REGION" \
      $env_arg \
      $ephemeral_arg \
      --tracing-config Mode=Active \
      --dead-letter-config "TargetArn=arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default" \
      --output text > /dev/null

    aws lambda wait function-active-v2 \
      --function-name "$fn" \
      --region "$DEPLOY_AWS_REGION"
    # Verify creation actually took — surface loudly instead of false success.
    if ! aws lambda get-function --function-name "$fn" --region "$DEPLOY_AWS_REGION" >/dev/null 2>&1; then
      echo "::error::create-function for $fn reported done but the function is still missing"
      exit 1
    fi
    echo "✅ Created new Lambda $fn (with X-Ray + DLQ from creation)"

    # ── Deploy lane v2 (2026-09-11): a green step is not proof. Prove the live
    # CodeSha256 equals the zip built from this checkout, then publish a receipt
    # to S3 (data/ops/releases/<fn>.json) that any lane can verify over HTTPS.
    local_code_sha=$(openssl dgst -sha256 -binary "$tmp/deploy.zip" | base64 -w0)
    live_code_sha=$(aws lambda get-function-configuration \
      --function-name "$fn" --region "$DEPLOY_AWS_REGION" \
      --query 'CodeSha256' --output text)
    if [ "$local_code_sha" != "$live_code_sha" ]; then
      echo "::error::$fn: live CodeSha256 $live_code_sha != built zip $local_code_sha -- AWS is NOT running this commit"
      exit 1
    fi
    echo "  ✅ CodeSha256 verified ($live_code_sha)"
    DEPLOY_COMMIT="${DEPLOY_COMMIT:-$(git rev-parse HEAD)}" DEPLOY_RUN_ID="${DEPLOY_RUN_ID:-}" \
    DEPLOY_WORKFLOW="${DEPLOY_WORKFLOW:-deploy-lambdas.yml}" DEPLOY_ACTOR="${DEPLOY_ACTOR:-}" \
      python3 scripts/release_receipt.py "$fn" "$tmp/deploy.zip" "$dir/source" "$live_code_sha" \
      || echo "::warning::$fn: release receipt not published"
  fi

  # ── Defense-in-depth: ensure X-Ray + DLQ on existing Lambdas too ──
  # No-ops on Lambdas that already have these; covers any drift.
  aws lambda update-function-configuration \
    --function-name "$fn" \
    --tracing-config Mode=Active \
    --dead-letter-config "TargetArn=arn:aws:sqs:us-east-1:857687956942:justhodl-dlq-default" \
    --region "$DEPLOY_AWS_REGION" \
    --output text > /dev/null 2>&1 || true
  aws lambda wait function-updated \
    --function-name "$fn" \
    --region "$DEPLOY_AWS_REGION" 2>/dev/null || true

  # Every opted-in engine uses exactly one pinned, validated promotion.
  if [ "$candidate_managed" -eq 1 ]; then
    bash scripts/deploy_validated_candidate.sh "$fn" "$DEPLOY_AWS_REGION" "$tmp" "$config_file" "$candidate_schema"
  fi

  # ── EventBridge schedule (if config.json has .schedule) ──
  if [ -f "$config_file" ] && jq -e '.schedule' "$config_file" >/dev/null 2>&1; then
    rule_name=$(jq -r '.schedule.rule_name' "$config_file")
    cron_expr=$(jq -r '.schedule.cron' "$config_file")
    rule_desc=$(jq -r '.schedule.description // "Scheduled run"' "$config_file")
    region="$DEPLOY_AWS_REGION"
    acc="857687956942"

    echo "Setting up EventBridge rule $rule_name → $cron_expr"
    # PutRule and PutTargets replace omitted options. Preserve existing
    # disabled state, event pattern, role, target input, retry and DLQ.
    if ! aws events describe-rule --name "$rule_name" --region "$region" --output json > "$tmp/classic-rule.json" 2> "$tmp/classic-rule.error"; then
      if grep -q ResourceNotFoundException "$tmp/classic-rule.error"; then
        printf '{}\n' > "$tmp/classic-rule.json"
      else
        echo "::error::Could not read classic rule; response withheld"
        exit 1
      fi
    fi
    jq --arg name "$rule_name" --arg cron "$cron_expr" --arg desc "$rule_desc" '
      {Name:$name,ScheduleExpression:$cron,State:(.State // "ENABLED"),Description:$desc}
      + (if .EventPattern then {EventPattern:.EventPattern} else {} end)
      + (if .RoleArn then {RoleArn:.RoleArn} else {} end)
    ' "$tmp/classic-rule.json" > "$tmp/classic-rule-update.json"
    aws events put-rule --cli-input-json "file://$tmp/classic-rule-update.json" \
      --region "$region" --output text > /dev/null

    target_fn_arn="arn:aws:lambda:${region}:${acc}:function:${fn}"
    qualifier_arg=""
    if jq -e '.release_validation.schema_version' "$config_file" >/dev/null; then
      target_fn_arn="${target_fn_arn}:live"
      qualifier_arg="--qualifier live"
    fi
    # Grant invoke permission to the same qualified target.
    aws lambda add-permission \
      --function-name "$fn" \
      $qualifier_arg \
      --statement-id "EventBridge-${rule_name}" \
      --action "lambda:InvokeFunction" \
      --principal "events.amazonaws.com" \
      --source-arn "arn:aws:events:${region}:${acc}:rule/${rule_name}" \
      --region "$region" 2>/dev/null || echo "  permission already exists"

    # Preserve every matching target and leave unrelated targets alone.
    aws events list-targets-by-rule --rule "$rule_name" --region "$region" --output json > "$tmp/classic-targets.json"
    jq --arg arn "$target_fn_arn" --arg base "arn:aws:lambda:${region}:${acc}:function:${fn}" --arg id "audit-${fn}" '
      [.Targets[] | select(.Arn == $base or .Arn == ($base + ":$LATEST") or .Arn == ($base + ":live")) | .Arn=$arn] as $matched
      | if ($matched|length)>0 then $matched
        elif any(.Targets[]; .Id == $id) then error("Configured target ID belongs to another function")
        else [{Id:$id,Arn:$arn}] end
    ' "$tmp/classic-targets.json" > "$tmp/classic-target-update.json"
    aws events put-targets --rule "$rule_name" --targets "file://$tmp/classic-target-update.json" \
      --region "$region" --output json > "$tmp/classic-target-result.json"
    jq -e '.FailedEntryCount == 0' "$tmp/classic-target-result.json" >/dev/null
    echo "  ✅ Schedule attached"
  fi

  # ── EventBridge Scheduler (if config.json has .eventbridge_scheduler) ──
  # Go-forward scheduling path. The classic 300-rule EventBridge cap
  # is saturated, so new engines schedule via EventBridge Scheduler
  # (1M-schedule quota). Purely additive — Lambdas using the classic
  # .schedule block above are unaffected. See ops 821.
  # Candidate helper already applied the complete preserved Scheduler payload.
  if [ "$candidate_managed" -eq 0 ] && [ -f "$config_file" ] && jq -e '.eventbridge_scheduler' "$config_file" >/dev/null 2>&1; then
    python3 scripts/apply_direct_scheduler.py "$config_file" \
      "arn:aws:lambda:${DEPLOY_AWS_REGION}:857687956942:function:${fn}" "$DEPLOY_AWS_REGION"
  fi

  # ── Function URL (if config.json has .function_url.enabled=true) ──
  if [ -f "$config_file" ] && jq -e '.function_url.enabled' "$config_file" >/dev/null 2>&1; then
    region="$DEPLOY_AWS_REGION"
    cors_origins=$(jq -r '.function_url.cors_origins // ["*"] | join(",")' "$config_file")
    echo "Setting up Function URL for $fn (CORS: $cors_origins)"

    # Check if URL exists, create or use existing
    existing_url=$(aws lambda get-function-url-config \
      --function-name "$fn" \
      --region "$region" \
      --query 'FunctionUrl' --output text 2>/dev/null || echo "")
    if [ -z "$existing_url" ] || [ "$existing_url" = "None" ]; then
      # Build CORS JSON file (CLI shorthand for list values is brittle)
      jq -n --argjson origins "$(jq '.function_url.cors_origins // ["*"]' "$config_file")" '{AllowOrigins: $origins, AllowMethods: ["GET","OPTIONS"], AllowHeaders: ["content-type"], MaxAge: 86400}' > /tmp/cors.json
      fn_url=$(aws lambda create-function-url-config \
        --function-name "$fn" \
        --auth-type NONE \
        --cors file:///tmp/cors.json \
        --region "$region" \
        --query 'FunctionUrl' --output text)
      echo "  ✅ Created Function URL: $fn_url"
    else
      fn_url="$existing_url"
      echo "  ℹ️  Existing Function URL: $fn_url"
    fi

    # Allow public invoke (idempotent)
    aws lambda add-permission \
      --function-name "$fn" \
      --statement-id "FunctionURLAllowPublicAccess" \
      --action "lambda:InvokeFunctionUrl" \
      --principal "*" \
      --function-url-auth-type NONE \
      --region "$region" 2>/dev/null || echo "  permission already exists"

    # Write the URL into the summary (the visible deploy log)
    echo "FUNCTION_URL_${fn}=$fn_url" >> $GITHUB_OUTPUT
    echo "  - **$fn URL**: \`$fn_url\`" >> $GITHUB_STEP_SUMMARY

    # Also write to a tracked file so we can read it from anywhere
    echo "$fn_url" > "$dir/.function-url"
    git add "$dir/.function-url" 2>/dev/null || true
    echo "FN_URL_PATCHED_FILE=$dir/.function-url" >> $GITHUB_ENV

    # If config says to patch a file with this URL, do it
    patch_target=$(jq -r '.function_url.patch_file // empty' "$config_file")
    if [ -n "$patch_target" ] && [ -f "$patch_target" ]; then
      placeholder=$(jq -r '.function_url.placeholder // "__FUNCTION_URL_PLACEHOLDER__"' "$config_file")
      if grep -q "$placeholder" "$patch_target"; then
        sed -i "s|$placeholder|$fn_url|g" "$patch_target"
        echo "  ✅ Patched $patch_target with Function URL"
        # Stage the change so the post-step commits it back
        echo "FN_URL_PATCHED_FILE=$patch_target" >> $GITHUB_ENV
      fi
    fi
  fi

  if [ "$caller_phase" -eq 1 ]; then
    python3 scripts/release_order.py verify-caller "$fn" "$DEPLOY_AWS_REGION" "$tmp/deploy.zip"
  fi
  echo "✅ $fn deployed"
  rm -rf "$tmp"
  )
  deploy_status=$?
  set -e
  if [ "$deploy_status" -ne 0 ]; then
    echo "::error::Deploy failed for $fn"
    failed_lambdas+=("$fn")
    if [ "$caller_phase" -eq 1 ]; then
      echo "::error::Required alias-aware caller failed; producer staging blocked"
      exit 1
    fi
  fi
done

if [ "${#failed_lambdas[@]}" -gt 0 ]; then
  echo "::error::Lambdas that failed to deploy: ${failed_lambdas[*]}"
  exit 1
fi
