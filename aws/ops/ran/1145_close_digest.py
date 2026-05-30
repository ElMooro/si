"""ops 1145 — Closing digest rollout.

Symmetric counterpart to the morning digest (ops 1143). Adds a second
daily digest at 21:00 UTC (~5pm EDT, post US-close in summer, ~4pm EST
in winter) that captures the US-session debrief.

Architecture:
  - New context: alerts-digest-close (brief_type: digest, session_flavor: close)
  - Router function generate_alerts_digest() now reads cfg.session_flavor
    and reframes the message title + events-window label accordingly:
      'open'  → '📅 DAILY OPEN DIGEST · 09:00 UTC · pre-market'
      'close' → '🔔 DAILY CLOSE DIGEST · 21:00 UTC · US session debrief'
  - Audit file paths:
      session=open  → data/_alerts/digest-{YYYY-MM-DD}.json       (back-compat)
      session=close → data/_alerts/digest-{YYYY-MM-DD}-close.json (new)
  - Index entries now carry a session field. Dedup key changed from
    (date) → (date, session) so both sessions can coexist per day.
    Index cap raised 60 → 120 (60 days × 2 sessions).
  - Sort: newest date first, within same date 'close' before 'open'.

This op:
  1. Uploads patched 36-context registry (adds alerts-digest-close)
  2. Redeploys router with session-aware digest handler
  3. Creates EventBridge rule justhodl-alerts-digest-close-daily
     with schedule cron(0 21 * * ? *) targeting the router with
     {"contexts": ["alerts-digest-close"]}
  4. Adds events.amazonaws.com → lambda:InvokeFunction permission (idempotent)
  5. Force-fires a test close digest synchronously NOW
  6. Reads back data/_alerts/digest-{date}-close.json and digests-index.json
     to verify both files have the expected shape with two sessions today.
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
EB_SCHEDULE = "cron(0 21 * * ? *)"  # 21:00 UTC every day
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


def wait_active(t=180):
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if c.get("State") == "Active" and c.get("LastUpdateStatus") in ("Successful", None):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(2)
    return False


def setup_eb_schedule():
    """Create or update the EventBridge daily 21:00 UTC rule."""
    eb.put_rule(
        Name=EB_RULE_NAME,
        ScheduleExpression=EB_SCHEDULE,
        State="ENABLED",
        Description=EB_DESCRIPTION,
    )
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{FN}"
    eb.put_targets(
        Rule=EB_RULE_NAME,
        Targets=[{
            "Id": "1",
            "Arn": fn_arn,
            "Input": json.dumps({"contexts": ["alerts-digest-close"]}),
        }],
    )
    stmt_id = f"eb-invoke-{EB_RULE_NAME}"
    rule_arn = f"arn:aws:events:{REGION}:{ACCOUNT_ID}:rule/{EB_RULE_NAME}"
    try:
        lam.add_permission(
            FunctionName=FN,
            StatementId=stmt_id,
            Action="lambda:InvokeFunction",
            Principal="events.amazonaws.com",
            SourceArn=rule_arn,
        )
        return {"rule": "created", "permission": "added", "rule_arn": rule_arn}
    except ClientError as e:
        if e.response["Error"]["Code"] == "ResourceConflictException":
            return {"rule": "updated", "permission": "already_existed", "rule_arn": rule_arn}
        return {"rule": "updated", "permission_err": str(e)[:200], "rule_arn": rule_arn}


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Upload patched 36-context registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        rpt["registry"] = {
            "n_contexts": len(registry.get("contexts") or {}),
            "digests_present": [k for k,v in (registry.get("contexts") or {}).items()
                                  if (v.get("brief_type") == "digest")],
        }

        # 2) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 3) Create/update EventBridge close-digest schedule
        rpt["eb_schedule"] = setup_eb_schedule()

        # 4) Force-fire a test CLOSE digest synchronously
        print("[1145] firing test close digest …")
        time.sleep(2)
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
        }
        rpt["close_log"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1500:]

        # 5) Read back the close audit file
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=f"data/_alerts/digest-{today}-close.json")
            ca = json.loads(obj["Body"].read())
            rpt["close_audit"] = {
                "date":          ca.get("date"),
                "session":       ca.get("session"),
                "telegram_ok":   ca.get("telegram_ok"),
                "message_chars": ca.get("message_chars"),
                "message_preview": (ca.get("message_preview") or "")[:1400],
            }
        except ClientError as e:
            rpt["close_audit"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 6) Read the index to confirm both sessions for today are present
        try:
            obj2 = s3.get_object(Bucket=BUCKET, Key="data/_alerts/digests-index.json")
            idx = json.loads(obj2["Body"].read())
            entries = idx.get("entries") or []
            today_entries = [e for e in entries if e.get("date") == today]
            rpt["index_state"] = {
                "n_entries_total":   len(entries),
                "n_entries_today":   len(today_entries),
                "today_sessions":    sorted([e.get("session") or "open" for e in today_entries]),
                "earliest_date":     idx.get("earliest_date"),
                "latest_date":       idx.get("latest_date"),
                "activity_breakdown": idx.get("activity_breakdown"),
            }
        except ClientError as e:
            rpt["index_state"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 7) Verify the EB rule is live
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
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1145.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items()
                       if k not in ("close_log", "traceback")},
                     indent=2, default=str)[:4800])


if __name__ == "__main__":
    main()
