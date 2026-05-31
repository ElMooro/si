"""ops 1151 — Fix uptime monitor brief paths.

Initial paths in ops 1150 used my guesses for CIO synthesis and portfolio
brief. The test fire revealed they're MISSING because the actual S3 paths are:
  CIO synthesis: data/desk-consensus.json    (context: desk-consensus)
  Portfolio:     data/portfolio-manager-brief.json  (context: portfolio-manager-brief)

Also raised portfolio threshold 4h → 30h to match the actual cadence
(it's daily/business-hours, not hourly).

This op:
  1. Uploads patched registry with fixed paths
  2. Fires uptime check (no Lambda redeploy needed — registry is hot-loaded)
  3. Verifies all 7 briefs are now FRESH or have an explainable status
  4. ALL_CLEAR alert fires automatically since previous run was STALE
"""
import io, json, os, time, traceback, base64
from datetime import datetime, timezone
import boto3
from botocore.exceptions import ClientError
from botocore.config import Config

REGION = "us-east-1"
REPO_ROOT = os.environ.get("REPO_ROOT", os.getcwd())
FN = "justhodl-ai-brief-router"
BUCKET = "justhodl-dashboard-live"
REGISTRY_KEY = "config/ai-brief-contexts.json"

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)


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


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Upload patched registry
        registry_path = os.path.join(REPO_ROOT, "config/ai-brief-contexts.json")
        body = open(registry_path).read()
        s3.put_object(Bucket=BUCKET, Key=REGISTRY_KEY,
                       Body=body.encode("utf-8"), ContentType="application/json")
        registry = json.loads(body)
        rpt["registry"] = {
            "n_contexts": len(registry.get("contexts") or {}),
            "monitored_briefs": [b.get("key") for b in
                (registry.get("contexts") or {}).get("system-uptime-monitor", {}).get("monitored_briefs", [])],
        }

        # 2) Fire uptime check
        wait_active()
        time.sleep(2)
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"contexts": ["system-uptime-monitor"]}).encode(),
                         LogType="Tail")
        body_resp = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        rpt["invoke"] = {
            "fn_err": inv.get("FunctionError"),
            "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
            "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
            "results": body_resp.get("results") if isinstance(body_resp, dict) else None,
        }
        rpt["log_tail"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1500:]

        # 3) Read uptime-status to verify shape
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
                     "max_age_hours": b.get("max_age_hours"),
                     "generated_at": b.get("generated_at")}
                    for b in (doc.get("briefs") or [])
                ],
            }
        except ClientError as e:
            rpt["uptime_status"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1151.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items()
                       if k not in ("log_tail", "traceback")},
                     indent=2, default=str)[:4500])


if __name__ == "__main__":
    main()
