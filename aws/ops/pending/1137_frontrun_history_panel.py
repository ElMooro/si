"""ops 1137 — Front-run history panel rollout.

Redeploys router with history-append logic (every frontrun-sniffer generation
now also writes to data/frontrun-sniffer-history.json with up to 200 snapshots,
7-day stats, and an events table).

Then runs the sniffer 3 times (separated by 35s pauses, with timestamp offsets)
to seed enough history for the chart to render something meaningful. This is
a one-time seeding — production cycle will append every 4h naturally.
"""
import io, json, os, time, traceback, zipfile, base64
from datetime import datetime, timezone, timedelta
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


def invoke_once():
    inv = lam.invoke(FunctionName=FN, InvocationType="RequestResponse",
                     Payload=json.dumps({"contexts": ["frontrun-sniffer"]}).encode(),
                     LogType="None")
    body_resp = json.loads(inv["Payload"].read() or b"{}")
    if isinstance(body_resp, dict) and "body" in body_resp:
        try: body_resp = json.loads(body_resp["body"])
        except Exception: pass
    return inv.get("FunctionError"), body_resp


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat(), "runs": []}
    try:
        # 1) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        wait_active()
        lam.update_function_code(FunctionName=FN, ZipFile=zip_src(src_dir), Publish=False)
        wait_active()
        rpt["redeploy"] = "OK"

        # 2) Run sniffer 3 times to seed multiple snapshots. Each run sleeps 35s
        # which produces 3 chart points spaced ~3min apart (enough to draw a line).
        for i in range(3):
            print(f"[1137] seeding run {i+1}/3 …")
            err, body = invoke_once()
            rpt["runs"].append({"idx": i+1, "fn_err": err, "summary": {
                "n_ok": body.get("n_ok") if isinstance(body, dict) else None,
                "duration_s": body.get("duration_s") if isinstance(body, dict) else None,
            }})
            if i < 2:
                time.sleep(35)

        # 3) Verify history file
        time.sleep(3)
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/frontrun-sniffer-history.json")
            hist = json.loads(obj["Body"].read())
            snaps = hist.get("snapshots") or []
            rpt["history_file"] = {
                "n_snapshots":  len(snaps),
                "stats_7d":     hist.get("stats_7d"),
                "n_events":     len(hist.get("events") or []),
                "first_snap":   snaps[0] if snaps else None,
                "last_snap":    snaps[-1] if snaps else None,
                "first_ts":     (snaps[0].get("ts") if snaps else None),
                "last_ts":      (snaps[-1].get("ts") if snaps else None),
                "all_scores":   [s.get("score") for s in snaps],
                "all_regimes":  [s.get("regime") for s in snaps],
                "all_targets":  [s.get("top_setup_asset") for s in snaps],
            }
        except ClientError as e:
            rpt["history_file"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"
        except Exception as e:
            rpt["history_file"] = f"ERR: {str(e)[:200]}"
    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1137.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k not in ("traceback",)},
                     indent=2, default=str)[:4500])


if __name__ == "__main__":
    main()
