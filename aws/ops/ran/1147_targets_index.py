"""ops 1147 — Sustained targets leaderboard rollout.

Deploys the router's _update_targets_index() helper, which builds a 30-day
rolling targets leaderboard at data/_alerts/targets-index.json from each
digest run's 'most_targeted_assets' / 'most_targeted_instruments' snapshots.

Each daily digest (09:00 open + 21:00 close) contributes one observation
per top-targeted name. The index tracks:
  - n_digest_appearances (how many digests flagged this name)
  - max_n_times_in_window (peak n_times across all observed 7-day windows)
  - first_seen, last_seen
  - session_breakdown (open vs close)
  - is_recent (last_seen within last 7 days)

Auto-prunes entries with last_seen older than 30 days.
Capped at 50 per list (equity + macro).

This op:
  1. Redeploys router (with concurrency-safe retries from ops 1146)
  2. Fires both digests synchronously so the index gets populated from
     both today's open AND today's close cycles (2 observations per name)
  3. Reads back targets-index.json to verify structure
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


def fire_digest(ctx_name):
    """Wait for active state, then invoke digest synchronously."""
    wait_active()
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"contexts": [ctx_name]}).encode(),
                     LogType="Tail")
    body_resp = json.loads(inv["Payload"].read() or b"{}")
    if isinstance(body_resp, dict) and "body" in body_resp:
        try: body_resp = json.loads(body_resp["body"])
        except Exception: pass
    return {
        "fn_err": inv.get("FunctionError"),
        "n_ok": body_resp.get("n_ok") if isinstance(body_resp, dict) else None,
        "duration_s": body_resp.get("duration_s") if isinstance(body_resp, dict) else None,
        "results": body_resp.get("results") if isinstance(body_resp, dict) else None,
    }


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Redeploy router with targets-index helper
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        zipped = zip_src(src_dir)
        rpt["redeploy"] = update_code_with_retries(zipped)
        wait_active()

        # 2) Fire both digests synchronously to populate the targets index
        #    with observations from both sessions today
        print("[1147] firing OPEN digest …")
        rpt["fire_open"] = fire_digest("alerts-digest")
        time.sleep(2)
        wait_active()

        print("[1147] firing CLOSE digest …")
        rpt["fire_close"] = fire_digest("alerts-digest-close")
        time.sleep(2)

        # 3) Read back the targets index
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/_alerts/targets-index.json")
            tx = json.loads(obj["Body"].read())
            rpt["targets_index"] = {
                "version":           tx.get("version"),
                "updated_at":        tx.get("updated_at"),
                "lookback_days":     tx.get("lookback_days"),
                "n_equity_targets":  tx.get("n_equity_targets"),
                "n_macro_targets":   tx.get("n_macro_targets"),
                "n_equity_recent":   tx.get("n_equity_recent"),
                "n_macro_recent":    tx.get("n_macro_recent"),
                "equity_top5":       (tx.get("equity_targets") or [])[:5],
                "macro_top5":        (tx.get("macro_targets")  or [])[:5],
            }
        except ClientError as e:
            rpt["targets_index"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 4) Confirm digests-index has 2 entries for today (both sessions)
        try:
            obj2 = s3.get_object(Bucket=BUCKET, Key="data/_alerts/digests-index.json")
            di = json.loads(obj2["Body"].read())
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            today_entries = [e for e in (di.get("entries") or []) if e.get("date") == today]
            rpt["digests_index_today"] = {
                "n_today":  len(today_entries),
                "sessions": sorted([e.get("session") or "open" for e in today_entries]),
            }
        except ClientError as e:
            rpt["digests_index_today"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1147.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "traceback"},
                     indent=2, default=str)[:4800])


if __name__ == "__main__":
    main()
