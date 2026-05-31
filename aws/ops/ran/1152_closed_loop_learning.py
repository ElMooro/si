"""ops 1152 — Closed-loop learning rollout.

Wires the new front-run system into the existing signal-logger →
outcome-checker → calibrator learning pipeline that's already been running
for months on regime-engine signals. Closes the gap where front-run sniffers,
convergence fingerprints, and sustained targets weren't being graded.

Deploys:
  1. Patched signal-logger Lambda (Phase 11 additions) — now reads the
     front-run briefs + alert state files and writes predictions to DynamoDB
     justhodl-signals table with proper schema (horizon, target_price,
     baseline_price, regime, confidence) for outcome-checker to grade.

  2. Patched router with new generate_frontrun_skill_check() function +
     frontrun-skill-aggregator context. Scans DynamoDB for our signal_types,
     computes per-engine hit rate / profit factor / regime breakdown /
     confidence calibration, writes data/_skill/frontrun-skill-index.json.

  3. EventBridge rule: justhodl-frontrun-skill-daily
     Schedule: cron(30 10 * * ? *) — 10:30 UTC daily (runs ~1.5h after
     morning digest so outcome-checker has had time to grade overnight)

  4. Front-end /skill.html cockpit — reads the skill index, renders 5
     sections (headline KPIs / per-engine cards / regime breakdown /
     calibration / recent graded predictions).

This op:
  - Redeploys signal-logger with Phase 11 front-run prediction logging
  - Redeploys router with skill_aggregator dispatch
  - Uploads 38-context registry
  - Creates EB rule for daily skill aggregation
  - Fires signal-logger once to log the FIRST batch of front-run predictions
  - Fires skill aggregator once to populate the index (will mostly be
    pending grades on day 1 — that's expected)

After this rolls out:
  - Every 6h signal-logger picks up the latest sniffer briefs and logs
    predictions to DynamoDB (alongside the 26+ existing engine signals)
  - Daily 10:30 UTC the skill aggregator refreshes data/_skill/frontrun-skill-index.json
  - Daily outcome-checker grades whichever predictions have elapsed
  - After 3-30 days of data, the /skill.html cockpit shows the system's
    actual track record per engine
  - Phase 2 (next session): weekly calibrator proposes prompt/threshold
    tweaks based on this accuracy data — the actual self-improvement loop
"""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
BUCKET = "justhodl-dashboard-live"
REGISTRY_KEY = "config/ai-brief-contexts.json"
ACCOUNT_ID = "857687956942"
ROUTER_FN = "justhodl-ai-brief-router"
LOGGER_FN = "justhodl-signal-logger"
EB_RULE_NAME = "justhodl-frontrun-skill-daily"
EB_SCHEDULE = "cron(30 10 * * ? *)"  # 10:30 UTC daily
EB_DESCRIPTION = "Daily front-run skill aggregator — closes the learning loop"

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)
eb = boto3.client("events", region_name=REGION, config=_cfg)


def zip_src(d):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as z:
        for root, _, files in os.walk(d):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root: continue
                fp = os.path.join(root, f)
                z.write(fp, os.path.relpath(fp, d))
    return buf.getvalue()


def wait_active(fn, t=300):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=fn)
            if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") in ("Successful", None)):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(3)
    return False


def update_code_with_retries(fn, zipped, max_tries=8):
    last_err = None
    for attempt in range(1, max_tries + 1):
        wait_active(fn)
        try:
            lam.update_function_code(FunctionName=fn, ZipFile=zipped, Publish=False)
            return {"attempt": attempt, "status": "OK"}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            last_err = str(e)[:200]
            if code == "ResourceConflictException":
                time.sleep(10 + attempt * 2)
                continue
            raise
    return {"attempt": max_tries, "status": "EXHAUSTED", "err": last_err}


def setup_eb_schedule():
    eb.put_rule(Name=EB_RULE_NAME, ScheduleExpression=EB_SCHEDULE,
                 State="ENABLED", Description=EB_DESCRIPTION)
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{ROUTER_FN}"
    eb.put_targets(Rule=EB_RULE_NAME, Targets=[{
        "Id": "1", "Arn": fn_arn,
        "Input": json.dumps({"contexts": ["frontrun-skill-aggregator"]}),
    }])
    stmt_id = f"eb-invoke-{EB_RULE_NAME}"
    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{EB_RULE_NAME}"
    try:
        lam.add_permission(FunctionName=ROUTER_FN, StatementId=stmt_id,
                            Action="lambda:InvokeFunction",
                            Principal="events.amazonaws.com",
                            SourceArn=rule_arn)
        return {"rule": "created", "permission": "added", "rule_arn": rule_arn}
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            return {"rule": "updated", "permission": "already_existed", "rule_arn": rule_arn}
        return {"rule": "updated", "permission_err": str(e)[:200], "rule_arn": rule_arn}


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Upload 38-context registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        body = open(registry_path).read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        rpt["registry"] = {
            "n_contexts": len(json.loads(body).get("contexts") or {}),
            "skill_aggregator_present":
                "frontrun-skill-aggregator" in json.loads(body).get("contexts", {}),
        }

        # 2) Redeploy signal-logger with Phase 11 additions
        logger_src = os.path.join(REPO_ROOT, "aws/lambdas", LOGGER_FN, "source")
        rpt["redeploy_signal_logger"] = update_code_with_retries(
            LOGGER_FN, zip_src(logger_src))
        wait_active(LOGGER_FN)

        # 3) Redeploy router with skill_aggregator dispatch
        router_src = os.path.join(REPO_ROOT, "aws/lambdas", ROUTER_FN, "source")
        rpt["redeploy_router"] = update_code_with_retries(
            ROUTER_FN, zip_src(router_src))
        wait_active(ROUTER_FN)

        # 4) Create EventBridge daily schedule
        rpt["eb_schedule"] = setup_eb_schedule()

        # 5) Fire signal-logger to log the FIRST batch of front-run predictions
        print("[1152] firing signal-logger (Phase 11 front-run predictions) …")
        time.sleep(2)
        inv1 = lam.invoke(FunctionName=LOGGER_FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"action": "1152_phase11_test"}).encode(),
                          LogType="Tail")
        body_resp = json.loads(inv1["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        rpt["signal_logger_invoke"] = {
            "fn_err": inv1.get("FunctionError"),
            "logged": body_resp.get("logged") if isinstance(body_resp, dict) else None,
        }
        # Extract just the FRONTRUN-related log lines
        logs1 = base64.b64decode(inv1.get("LogResult","")).decode("utf-8","replace")[-3000:]
        front_lines = [L for L in logs1.split("\n")
                        if any(k in L for k in ["frontrun_sniffer_setup",
                                                "macro_frontrun_sniffer_setup",
                                                "convergence_fingerprint",
                                                "sustained_target_equity",
                                                "FRONTRUN-PREDICTIONS"])]
        rpt["frontrun_log_lines"] = "\n".join(front_lines[:20])
        time.sleep(3)

        # 6) Fire skill aggregator synchronously
        print("[1152] firing frontrun-skill-aggregator …")
        wait_active(ROUTER_FN)
        inv2 = lam.invoke(FunctionName=ROUTER_FN, InvocationType="RequestResponse",
                          Payload=json.dumps({"contexts": ["frontrun-skill-aggregator"]}).encode(),
                          LogType="Tail")
        body_resp2 = json.loads(inv2["Payload"].read() or b"{}")
        if isinstance(body_resp2, dict) and "body" in body_resp2:
            try: body_resp2 = json.loads(body_resp2["body"])
            except Exception: pass
        rpt["skill_aggregator_invoke"] = {
            "fn_err": inv2.get("FunctionError"),
            "n_ok": body_resp2.get("n_ok") if isinstance(body_resp2, dict) else None,
            "duration_s": body_resp2.get("duration_s") if isinstance(body_resp2, dict) else None,
            "results": body_resp2.get("results") if isinstance(body_resp2, dict) else None,
        }
        time.sleep(2)

        # 7) Read back skill index
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/_skill/frontrun-skill-index.json")
            idx = json.loads(obj["Body"].read())
            rpt["skill_index"] = {
                "updated_at":     idx.get("updated_at"),
                "lookback_days":  idx.get("lookback_days"),
                "n_total":        idx.get("n_total_predictions"),
                "n_scored":       idx.get("n_scored"),
                "n_pending":      idx.get("n_pending"),
                "scored_pct":     idx.get("scored_pct"),
                "engines_tracked": list((idx.get("by_engine") or {}).keys()),
                "engine_summary": {k: {"n_total": v.get("n_total"),
                                        "n_scored": v.get("n_scored"),
                                        "hit_rate": v.get("hit_rate"),
                                        "n_pending_grade": v.get("n_pending_grade")}
                                   for k, v in (idx.get("by_engine") or {}).items()},
            }
        except ClientError as e:
            rpt["skill_index"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 8) Verify EB rule
        try:
            desc = eb.describe_rule(Name=EB_RULE_NAME)
            tgt = eb.list_targets_by_rule(Rule=EB_RULE_NAME)
            rpt["eb_verify"] = {
                "state":         desc.get("State"),
                "schedule":      desc.get("ScheduleExpression"),
                "n_targets":     len(tgt.get("Targets") or []),
                "target_input":  (tgt.get("Targets") or [{}])[0].get("Input"),
            }
        except Exception as e:
            rpt["eb_verify"] = f"ERR: {str(e)[:200]}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1152.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "traceback"},
                     indent=2, default=str)[:5500])


if __name__ == "__main__":
    main()
