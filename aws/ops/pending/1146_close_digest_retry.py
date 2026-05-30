"""ops 1146 — Retry of ops 1145 (close digest rollout).

Previous attempt hit ResourceConflictException on UpdateFunctionCode because
a scheduled 4h cycle held the router update lock. This op adds a retry loop
around the deploy step that waits for the lock to clear.

Same end state as 1145:
  - Registry uploaded (36 contexts)
  - Router redeployed (session-aware digest)
  - EventBridge rule justhodl-alerts-digest-close-daily (cron(0 21 * * ? *))
  - Test close digest fired
  - Verification reads
"""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
BUCKET = "justhodl-dashboard-live"
REGISTRY_KEY = "config/ai-brief-contexts.json"
ACCOUNT_ID = "857687956942"
EB_RULE_NAME = "justhodl-alerts-digest-close-daily"
EB_SCHEDULE = "cron(0 21 * * ? *)"
EB_DESCRIPTION = "Daily closing front-run alerts digest at 21:00 UTC (alerts-digest-close)"

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


def wait_active(t=300):
    """Wait for State=Active AND LastUpdateStatus in (Successful, None)."""
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") in ("Successful", None)):
                return True, c
            if c.get("LastUpdateStatus") == "Failed":
                return False, c
        except ClientError: pass
        time.sleep(3)
    return False, None


def update_code_with_retries(zipped, max_tries=8):
    """Retry UpdateFunctionCode through ResourceConflictException
    until the in-progress update releases."""
    last_err = None
    for attempt in range(1, max_tries + 1):
        wait_active()
        try:
            lam.update_function_code(FunctionName=FN, ZipFile=zipped, Publish=False)
            return {"attempt": attempt, "status": "OK"}
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            last_err = str(e)[:200]
            if code == "ResourceConflictException":
                # Active update still running, sleep and retry
                time.sleep(10 + attempt * 2)
                continue
            raise
    return {"attempt": max_tries, "status": "EXHAUSTED", "err": last_err}


def setup_eb_schedule():
    eb.put_rule(Name=EB_RULE_NAME, ScheduleExpression=EB_SCHEDULE,
                 State="ENABLED", Description=EB_DESCRIPTION)
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{FN}"
    eb.put_targets(Rule=EB_RULE_NAME, Targets=[{
        "Id": "1", "Arn": fn_arn,
        "Input": json.dumps({"contexts": ["alerts-digest-close"]}),
    }])
    stmt_id = f"eb-invoke-{EB_RULE_NAME}"
    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{EB_RULE_NAME}"
    try:
        lam.add_permission(FunctionName=FN, StatementId=stmt_id,
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
        # 1) Upload registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        body = open(registry_path).read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        rpt["registry"] = {
            "n_contexts": len(registry.get("contexts") or {}),
            "digests_present": [k for k,v in (registry.get("contexts") or {}).items()
                                  if v.get("brief_type") == "digest"],
        }

        # 2) Redeploy router with retries
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        zipped = zip_src(src_dir)
        rpt["redeploy"] = update_code_with_retries(zipped)
        wait_active()

        # 3) EventBridge rule
        rpt["eb_schedule"] = setup_eb_schedule()

        # 4) Force-fire test close digest
        print("[1146] firing test close digest …")
        time.sleep(3)
        # Wait again in case the redeploy just settled
        wait_active()
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"contexts": ["alerts-digest-close"]}).encode(),
                         LogType="Tail")
        body_resp = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        rpt["close_invoke"] = {
            "fn_err": inv.get("FunctionError"),
            "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
            "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
            "results": body_resp.get("results") if isinstance(body_resp, dict) else None,
        }
        rpt["close_log"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1500:]

        # 5) Read close audit
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=f"data/_alerts/digest-{today}-close.json")
            ca = json.loads(obj["Body"].read())
            rpt["close_audit"] = {
                "date":            ca.get("date"),
                "session":         ca.get("session"),
                "telegram_ok":     ca.get("telegram_ok"),
                "message_chars":   ca.get("message_chars"),
                "message_preview": (ca.get("message_preview") or "")[:1500],
            }
        except ClientError as e:
            rpt["close_audit"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 6) Verify index has both sessions today
        try:
            obj2 = s3.get_object(Bucket=BUCKET, Key="data/_alerts/digests-index.json")
            idx = json.loads(obj2["Body"].read())
            entries = idx.get("entries") or []
            today_entries = [e for e in entries if e.get("date") == today]
            rpt["index_state"] = {
                "n_entries_total":    len(entries),
                "n_entries_today":    len(today_entries),
                "today_sessions":     sorted([e.get("session") or "open" for e in today_entries]),
                "earliest_date":      idx.get("earliest_date"),
                "latest_date":        idx.get("latest_date"),
                "activity_breakdown": idx.get("activity_breakdown"),
            }
        except ClientError as e:
            rpt["index_state"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 7) EB rule live state
        try:
            desc = eb.describe_rule(Name=EB_RULE_NAME)
            tgt = eb.list_targets_by_rule(Rule=EB_RULE_NAME)
            rpt["eb_verify"] = {
                "state": desc.get("State"),
                "schedule": desc.get("ScheduleExpression"),
                "n_targets": len(tgt.get("Targets") or []),
                "target_input": (tgt.get("Targets") or [{}])[0].get("Input"),
            }
        except Exception as e:
            rpt["eb_verify"] = f"ERR: {str(e)[:200]}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1146.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items()
                       if k not in ("close_log", "traceback")},
                     indent=2, default=str)[:4800])


if __name__ == "__main__":
    main()
