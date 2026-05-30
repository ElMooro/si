"""ops 1144 — Digest archive rollout.

Adds:
  1. Router code: generate_alerts_digest() now ALSO maintains a rolling
     index file at data/_alerts/digests-index.json
       - Every successful digest run upserts today's entry (de-duped by date)
       - Capped at 60 entries (~2 months of daily digests)
       - Each entry has activity_level classification:
           EXTREME = regime EXTREME on either sniffer or score ≥ 70
           ACTIVE  = alerts fired or score ≥ 45
           QUIET   = nothing fired, scores below 45
       - Index also stores activity_breakdown {extreme, active, quiet}
  2. Front-end /digest-archive.html reads the index via Cloudflare proxy
     and renders one card per past digest with expand-to-see-message
     interaction.

This op:
  1. Redeploys router (already has the digest handler + new index logic)
  2. Invokes the digest synchronously to:
     - Send today's digest (replaces today's earlier entry if already sent)
     - Force index file creation
  3. Reads data/_alerts/digests-index.json from S3 to verify shape
  4. No new EventBridge schedule needed — the existing daily 9am UTC rule
     from ops 1143 already calls this same handler, which now maintains
     the index automatically.
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

_cfg = Config(connect_timeout=10, read_timeout=300, retries={"max_attempts": 2})
lam = boto3.client("lambda", region_name=REGION, config=_cfg)
s3 = boto3.client("s3", region_name=REGION)


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


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 2) Synchronously fire the digest to populate the index
        print("[1144] firing digest to populate the archive index …")
        inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                         Payload=json.dumps({"contexts": ["alerts-digest"]}).encode(),
                         LogType="Tail")
        body_resp = json.loads(inv["Payload"].read() or b"{}")
        if isinstance(body_resp, dict) and "body" in body_resp:
            try: body_resp = json.loads(body_resp["body"])
            except Exception: pass
        rpt["digest_invoke"] = {
            "fn_err": inv.get("FunctionError"),
            "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
            "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
        }
        rpt["digest_log"] = base64.b64decode(inv.get("LogResult","")).decode("utf-8","replace")[-1800:]

        # 3) Read the index file from S3 to verify it was created
        time.sleep(2)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/_alerts/digests-index.json")
            idx = json.loads(obj["Body"].read())
            rpt["index_file"] = {
                "version":            idx.get("version"),
                "updated_at":         idx.get("updated_at"),
                "n_entries":          idx.get("n_entries"),
                "earliest_date":      idx.get("earliest_date"),
                "latest_date":        idx.get("latest_date"),
                "activity_breakdown": idx.get("activity_breakdown"),
                "first_entry":        (idx.get("entries") or [{}])[0],
            }
        except ClientError as e:
            rpt["index_file"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 4) Read latest digest audit to confirm it's still being written
        try:
            obj2 = s3.get_object(Bucket=BUCKET, Key="data/_alerts/digest-latest.json")
            latest = json.loads(obj2["Body"].read())
            rpt["latest_audit"] = {
                "date":            latest.get("date"),
                "telegram_ok":     latest.get("telegram_ok"),
                "message_chars":   latest.get("message_chars"),
            }
        except ClientError as e:
            rpt["latest_audit"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1144.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k not in ("digest_log", "traceback")},
                     indent=2, default=str)[:3500])


if __name__ == "__main__":
    main()
