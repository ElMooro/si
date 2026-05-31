"""ops 1150 — System uptime monitor rollout.

Deploys:
  1. New registry context: system-uptime-monitor (brief_type: uptime)
     monitored_briefs: 7 critical artifacts with per-brief max_age_hours
  2. Router: generate_uptime_check() reads age of each artifact, classifies
     FRESH/WARNING/STALE/MISSING, fires Telegram on FRESH→STALE transitions
     and ALL_CLEAR recoveries
  3. EventBridge rule: justhodl-uptime-monitor-hourly
     Schedule: cron(0 * * * ? *) → 24 invocations/day (~$0.001/month cost)
     Input: {"contexts": ["system-uptime-monitor"]}
  4. Test fire: invoke synchronously, verify uptime-status.json shape

What gets monitored:
  data/frontrun-sniffer.json         max 8h   (4h cadence × 2 grace)
  data/macro-frontrun-sniffer.json   max 8h
  data/_alerts/digest-latest.json    max 26h  (12h cadence × 2 + 2h slop)
  data/_alerts/digests-index.json    max 26h
  data/_alerts/targets-index.json    max 26h
  data/master-cio-synthesis.json     max 30h  (daily)
  data/portfolio-brief.json          max 4h   (hourly cadence × 4 grace)

Alert rules:
  STALE_NEW       any brief transitions FRESH→STALE  → priority alert
  STALE_REMINDER  still stale & >4h since last STALE → reminder ping
  ALL_CLEAR       all back to FRESH after stale     → recovery ping
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
EB_RULE_NAME = "justhodl-uptime-monitor-hourly"
EB_SCHEDULE = "cron(0 * * * ? *)"  # Top of every hour
EB_DESCRIPTION = "Hourly system uptime heartbeat — checks staleness of critical briefs"

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
    end = time.time() + t
    while time.time() < end:
        try:
            c = lam.get_function_configuration(FunctionName=FN)
            if (c.get("State") == "Active"
                and c.get("LastUpdateStatus") in ("Successful", None)):
                return True
            if c.get("LastUpdateStatus") == "Failed": return False
        except ClientError: pass
        time.sleep(3)
    return False


def update_code_with_retries(zipped, max_tries=8):
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
        "Input": json.dumps({"contexts": ["system-uptime-monitor"]}),
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
            "uptime_present": "system-uptime-monitor" in (registry.get("contexts") or {}),
        }

        # 2) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        zipped = zip_src(src_dir)
        rpt["redeploy"] = update_code_with_retries(zipped)
        wait_active()

        # 3) Create EventBridge hourly schedule
        rpt["eb_schedule"] = setup_eb_schedule()

        # 4) Fire test uptime check synchronously
        print("[1150] firing test uptime check …")
        time.sleep(2)
        wait_active()
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"contexts": ["system-uptime-monitor"]}).encode(),
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
        rpt["test_log"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1800:]

        # 5) Read uptime-status to verify shape
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/_alerts/uptime-status.json")
            doc = json.loads(obj["Body"].read())
            rpt["uptime_status"] = {
                "checked_at":      doc.get("checked_at"),
                "n_monitored":     doc.get("n_monitored"),
                "n_fresh":         doc.get("n_fresh"),
                "n_warning":       doc.get("n_warning"),
                "n_stale":         doc.get("n_stale"),
                "n_missing":       doc.get("n_missing"),
                "overall_status":  doc.get("overall_status"),
                "briefs":          [
                    {"label": b.get("label"), "status": b.get("status"),
                     "age_hours": b.get("age_hours"),
                     "max_age_hours": b.get("max_age_hours")}
                    for b in (doc.get("briefs") or [])
                ],
            }
        except ClientError as e:
            rpt["uptime_status"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 6) Verify EB rule live state
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
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1150.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items()
                       if k not in ("test_log", "traceback")},
                     indent=2, default=str)[:5000])


if __name__ == "__main__":
    main()
