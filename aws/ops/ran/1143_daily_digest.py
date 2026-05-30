"""ops 1143 — Daily alerts digest rollout.

Wires the new `alerts-digest` context (brief_type: digest) into:
  1. Router code — generate_alerts_digest() reads 6 data sources, builds the
     Markdown digest, sends via Telegram, writes an audit log to S3 at
       data/_alerts/digest-{YYYY-MM-DD}.json
       data/_alerts/digest-latest.json
  2. EventBridge schedule — daily at 09:00 UTC invokes the router with
     {"contexts": ["alerts-digest"]}
  3. Test fire — synchronously invoke once now to verify Telegram delivery
     and the audit log shape.

The digest message includes (regardless of whether alerts fired):
  • Current state of both sniffers (score, regime, last-alert-age, today's count)
  • Both convergence fingerprints — equity 4-cardinal + macro 3-pillar
  • Last 24h merged event list (or "tape was quiet ✓")
  • 7-day score trajectory per sniffer (mean, range, regime breakdown)
  • Most-targeted assets/instruments this week
  • Deeplink to /alerts.html

Goal: morning heartbeat ping. Even on quiet days you confirm "system healthy,
nothing screaming, here's where we are this week." On busy days it's the
debrief.
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
EB_RULE_NAME = "justhodl-alerts-digest-daily"
EB_SCHEDULE = "cron(0 9 * * ? *)"  # 09:00 UTC every day
EB_DESCRIPTION = "Daily front-run alerts digest at 09:00 UTC (alerts-digest context)"

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
    """Create or update the EventBridge daily 9am UTC rule."""
    # 1) Create/update the rule itself
    eb.put_rule(
        Name=EB_RULE_NAME,
        ScheduleExpression=EB_SCHEDULE,
        State="ENABLED",
        Description=EB_DESCRIPTION,
    )

    # 2) Attach the router Lambda as the target
    fn_arn = f"arn:aws:lambda:{REGION}:{ACCOUNT_ID}:function:{FN}"
    eb.put_targets(
        Rule=EB_RULE_NAME,
        Targets=[{
            "Id": "1",
            "Arn": fn_arn,
            "Input": json.dumps({"contexts": ["alerts-digest"]}),
        }],
    )

    # 3) Permission for EventBridge → Lambda (idempotent — catch AlreadyExists)
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
        # 1) Upload patched 35-context registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        with open(registry_path) as fh:
            body = fh.read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        rpt["registry"] = {
            "n_contexts": len(registry.get("contexts") or {}),
            "digest_present": "alerts-digest" in (registry.get("contexts") or {}),
        }

        # 2) Redeploy router with the new digest handler
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 3) Create / update EventBridge daily-9am-UTC schedule
        rpt["eb_schedule"] = setup_eb_schedule()

        # 4) Force-trigger a test digest right now to verify end-to-end
        print("[1143] firing test digest …")
        time.sleep(2)
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"contexts": ["alerts-digest"]}).encode(),
                         LogType="Tail")
        body_resp = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        rpt["test_invoke"] = {
            "fn_err": inv.get("FunctionError"),
            "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
            "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
            "results": body_resp.get("results") if isinstance(body_resp, dict) else None,
        }
        rpt["test_invoke_log"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1800:]

        # 5) Read back the digest audit log from S3 to verify
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/_alerts/digest-latest.json")
            audit = json.loads(obj["Body"].read())
            rpt["digest_audit"] = {
                "date": audit.get("date"),
                "telegram_ok": audit.get("telegram_ok"),
                "message_chars": audit.get("message_chars"),
                "equity_score": audit.get("equity_score"),
                "equity_regime": audit.get("equity_regime"),
                "macro_score": audit.get("macro_score"),
                "macro_regime": audit.get("macro_regime"),
                "n_equity_alerts_today": audit.get("n_equity_alerts_today"),
                "n_macro_alerts_today": audit.get("n_macro_alerts_today"),
                "message_preview": (audit.get("message_preview") or "")[:1200],
            }
        except ClientError as e:
            rpt["digest_audit"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 6) Confirm the EB rule is live
        try:
            desc = eb.describe_rule(Name=EB_RULE_NAME)
            tgt = eb.list_targets_by_rule(Rule=EB_RULE_NAME)
            rpt["eb_verify"] = {
                "state": desc.get("State"),
                "schedule": desc.get("ScheduleExpression"),
                "arn": desc.get("Arn"),
                "n_targets": len(tgt.get("Targets") or []),
                "target_input": (tgt.get("Targets") or [{}])[0].get("Input"),
            }
        except Exception as e:
            rpt["eb_verify"] = f"ERR: {str(e)[:200]}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1143.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items()
                       if k not in ("test_invoke_log", "traceback")},
                     indent=2, default=str)[:4800])


if __name__ == "__main__":
    main()
