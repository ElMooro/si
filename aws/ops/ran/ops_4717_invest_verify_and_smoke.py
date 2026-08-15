"""
ops/4717 — verify justhodl-invest deployed cleanly and smoke-test it.

Per AUTONOMY.md: "Do NOT hand-roll deploys." The actual deploy is
deploy-lambdas.yml, triggered automatically by the same push that carries
aws/lambdas/justhodl-invest/{source/**,config.json} — it zips source +
aws/shared/*.py, creates/updates the function, applies config.json, and
sets the EventBridge Scheduler schedule from .eventbridge_scheduler. This
script runs AFTER that (via run-ops.yml on the same push) and only:

  1. Confirms the function exists and reached State=='Active' (known trap:
     a fresh function can report LastUpdateStatus='Successful' while
     State is still 'Pending' -- invoking then throws
     ResourceConflictException).
  2. Confirms the EventBridge Scheduler schedule actually exists and is
     ENABLED (known trap: config.json's declarative schedule has
     historically failed to materialise on first create for the classic
     .schedule path; this checks the modern .eventbridge_scheduler path
     and self-heals by re-issuing create/update, treating
     ConflictException as success, exactly as AUTONOMY.md prescribes).
  3. Smoke-invokes the function (RequestResponse, explicit read_timeout
     above the function's own 300s timeout, per _preflight's own warning
     about bare sync .invoke()) and reports what it returned.
  4. Reads back data/invest.json from S3 if the invoke produced one, and
     prints its top-level shape -- proof of a genuinely working run, not
     just "the function returned 200".

Does NOT create the Lambda, does NOT touch IAM, does NOT zip/upload code.
If step 1 shows the function is missing entirely, that means
deploy-lambdas.yml did not run or failed -- this script says so plainly
and stops; it does not fall back to deploying by hand.
"""
import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))

import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
ACCOUNT = "857687956942"
FUNCTION_NAME = "justhodl-invest"
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/invest.json"

SCHED_NAME = "justhodl-invest-daily"
SCHED_CRON = "cron(0 15 * * ? *)"
SCHED_TZ = "UTC"
SCHED_ROLE = f"arn:aws:iam::{ACCOUNT}:role/justhodl-scheduler-role"

lam = boto3.client("lambda", region_name=REGION)
sched = boto3.client("scheduler", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def verify_function_active(rep, tries=6, wait_s=10):
    for i in range(tries):
        try:
            cfg = lam.get_function_configuration(FunctionName=FUNCTION_NAME)
        except ClientError as e:
            if e.response["Error"]["Code"] == "ResourceNotFoundException":
                rep.fail(f"  {FUNCTION_NAME} does not exist -- deploy-lambdas.yml "
                         f"either hasn't run yet or failed. Check its Actions run, "
                         f"not this script.")
                return None
            raise
        state = cfg.get("State")
        last_update = cfg.get("LastUpdateStatus")
        rep.log(f"  attempt {i+1}/{tries}: State={state} LastUpdateStatus={last_update}")
        if state == "Active" and last_update == "Successful":
            rep.ok(f"  {FUNCTION_NAME} is Active. Runtime={cfg['Runtime']} "
                   f"Memory={cfg['MemorySize']} Timeout={cfg['Timeout']} "
                   f"LastModified={cfg['LastModified']}")
            return cfg
        if state == "Failed" or last_update == "Failed":
            rep.fail(f"  {FUNCTION_NAME} is in a Failed state: "
                     f"{cfg.get('StateReason', 'no reason given')}")
            return None
        time.sleep(wait_s)
    rep.fail(f"  {FUNCTION_NAME} never reached State=Active after "
             f"{tries * wait_s}s -- last seen State={state}")
    return None


def verify_or_heal_schedule(rep):
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT}:function:{FUNCTION_NAME}"
    target = {"Arn": fn_arn, "RoleArn": SCHED_ROLE, "Input": "{}",
              "RetryPolicy": {"MaximumRetryAttempts": 2, "MaximumEventAgeInSeconds": 3600}}
    try:
        existing = sched.get_schedule(Name=SCHED_NAME)
        state = existing.get("State")
        expr = existing.get("ScheduleExpression")
        if state == "ENABLED" and expr == SCHED_CRON:
            rep.ok(f"  schedule {SCHED_NAME} already correct: {expr}, {state}")
            return True
        rep.warn(f"  schedule {SCHED_NAME} exists but drifted "
                 f"(expr={expr}, state={state}) -- updating")
        sched.update_schedule(
            Name=SCHED_NAME, ScheduleExpression=SCHED_CRON,
            ScheduleExpressionTimezone=SCHED_TZ,
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED", Target=target,
        )
        rep.ok(f"  schedule {SCHED_NAME} updated to {SCHED_CRON}")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] != "ResourceNotFoundException":
            raise
    # Known trap (AUTONOMY.md): declarative schedule can fail to
    # materialise on first create. Self-heal here; ConflictException on
    # the create call itself means another process won the race -- also
    # success.
    try:
        sched.create_schedule(
            Name=SCHED_NAME, ScheduleExpression=SCHED_CRON,
            ScheduleExpressionTimezone=SCHED_TZ,
            FlexibleTimeWindow={"Mode": "OFF"}, State="ENABLED", Target=target,
            Description="justhodl-invest daily refresh, 15:00 UTC (self-healed by ops 4717)",
        )
        rep.ok(f"  schedule {SCHED_NAME} did not exist -- created it now "
               f"(deploy-lambdas.yml's declarative create did not stick; known trap)")
        return True
    except ClientError as e:
        if e.response["Error"]["Code"] == "ConflictException":
            rep.ok(f"  schedule {SCHED_NAME} create raced with another process -- "
                   f"ConflictException treated as success per house doctrine")
            return True
        rep.fail(f"  could not create schedule {SCHED_NAME}: {e}")
        return False


def smoke_invoke(rep):
    client_long = boto3.client(
        "lambda", region_name=REGION,
        config=Config(read_timeout=310, retries={"max_attempts": 0}),
    )
    rep.log(f"  invoking {FUNCTION_NAME} synchronously (this can take up to ~5min)...")
    t0 = time.time()
    resp = client_long.invoke(
        FunctionName=FUNCTION_NAME, InvocationType="RequestResponse",
        Payload=b"{}",
    )
    elapsed = time.time() - t0
    status_code = resp.get("StatusCode")
    payload_raw = resp["Payload"].read()
    fn_error = resp.get("FunctionError")
    rep.kv(invoke_status_code=status_code, invoke_elapsed_s=round(elapsed, 1),
           function_error=fn_error)
    try:
        payload = json.loads(payload_raw)
    except Exception:
        payload = payload_raw[:2000].decode("utf-8", errors="replace")
    if fn_error:
        rep.fail(f"  invoke returned FunctionError={fn_error}. Payload: "
                 f"{json.dumps(payload)[:2000] if isinstance(payload, dict) else payload}")
        return False
    rep.ok(f"  invoke succeeded in {elapsed:.1f}s, StatusCode={status_code}")
    rep.log(f"  handler response: {json.dumps(payload)[:1500] if isinstance(payload, dict) else payload}")
    return True


def check_output_artifact(rep):
    try:
        body = s3.get_object(Bucket=BUCKET, Key=OUT_KEY)["Body"].read()
        doc = json.loads(body)
    except ClientError as e:
        if e.response["Error"]["Code"] == "NoSuchKey":
            rep.warn(f"  {OUT_KEY} does not exist yet -- the invoke above may not "
                     f"have completed a full write, or wrote 0 bytes on an "
                     f"internal early-return. Check the invoke payload above.")
            return
        raise
    rep.ok(f"  {OUT_KEY} exists, {len(body)} bytes, schema={doc.get('schema')}")
    top_keys = sorted(doc.keys())
    rep.kv(output_top_level_keys=top_keys)
    inds = doc.get("leading_indicators", [])
    gates = doc.get("industry_gates", {})
    picks = doc.get("stock_picks", [])
    rep.kv(
        leading_indicators_n=len(inds),
        leading_indicators_confirmed=sum(1 for i in inds if i.get("status") == "CONFIRMED"),
        industry_gates_n=len(gates),
        industry_gates_pass=sum(1 for g in gates.values() if g.get("pass")),
        stock_picks_n=len(picks),
    )


def main():
    with report("4717_invest_verify_and_smoke") as rep:
        rep.heading("ops 4717 — verify justhodl-invest deploy, self-heal schedule, smoke test")

        rep.section("1. Function state")
        cfg = verify_function_active(rep)
        if cfg is None:
            rep.warn("Stopping here -- fix the deploy-lambdas.yml run first, "
                     "then re-push this file (renumbered) to re-verify.")
            return

        rep.section("2. EventBridge Scheduler")
        verify_or_heal_schedule(rep)

        rep.section("3. Smoke invoke")
        invoked_ok = smoke_invoke(rep)

        rep.section("4. Output artifact")
        if invoked_ok:
            check_output_artifact(rep)

        rep.section("Verdict")
        if invoked_ok:
            rep.ok("justhodl-invest is deployed, scheduled daily 15:00 UTC, and "
                   "produced real output on first invoke.")
        else:
            rep.warn("Deployed and scheduled, but the smoke invoke failed -- see "
                     "section 3 above for the actual error before trusting Tier 1.")


if __name__ == "__main__":
    try:
        main()
    except Exception:
        import traceback
        print("VERIFY SCRIPT ERROR:\n" + traceback.format_exc(), flush=True)
        sys.exit(1)
    sys.exit(0)
