"""ops 1148 — Wire sustained-patterns block into the digest message.

Reorders generate_alerts_digest() so the targets-index is updated BEFORE
the message is formatted, then passes the fresh index into _format_digest
so the Telegram message includes a new section:

  📈 Sustained patterns (30d rolling index):
    🎯 Equity:  PEP×4●, AMD×3●, AMZN×2●
    🏛 Macro:   TLT×3●
    (● = active in last 7d, ○ = cooling off)

Threshold: only names with n_digest_appearances >= 2 shown. Limits to top 3
per sniffer to keep the message focused. Skips the section entirely when
no name has 2+ observations (early days, day 1 first run).

Also extends the footer:
  → https://justhodl.ai/targets.html · https://justhodl.ai/alerts.html
(was just /alerts.html)

This op:
  1. Redeploys router with the reordered handler + new format block
  2. Fires OPEN digest (will now include the sustained-patterns section
     since today already has 2-appearance counts from earlier digest runs)
  3. Fires CLOSE digest
  4. Reads back digest-latest.json to confirm the new format is live
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
    }


def main():
    rpt = {"started": datetime.now(timezone.utc).isoformat()}
    try:
        # 1) Redeploy router
        src_dir = os.path.join(REPO_ROOT, "aws/lambdas", FN, "source")
        zipped = zip_src(src_dir)
        rpt["redeploy"] = update_code_with_retries(zipped)
        wait_active()

        # 2) Fire OPEN digest with new format
        print("[1148] firing OPEN digest with sustained-patterns block …")
        rpt["fire_open"] = fire_digest("alerts-digest")
        time.sleep(2)
        wait_active()

        # 3) Fire CLOSE digest with new format
        print("[1148] firing CLOSE digest with sustained-patterns block …")
        rpt["fire_close"] = fire_digest("alerts-digest-close")
        time.sleep(2)

        # 4) Read back digest-latest to verify new format
        try:
            obj = s3.get_object(Bucket=BUCKET, Key="data/_alerts/digest-latest.json")
            la = json.loads(obj["Body"].read())
            rpt["latest_digest"] = {
                "date":          la.get("date"),
                "session":       la.get("session"),
                "telegram_ok":   la.get("telegram_ok"),
                "message_chars": la.get("message_chars"),
                "has_sustained_section":
                    "Sustained patterns" in (la.get("message_preview") or ""),
                "message_preview": (la.get("message_preview") or "")[:1500],
            }
        except ClientError as e:
            rpt["latest_digest"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

        # 5) Read targets index to show what was used
        try:
            obj2 = s3.get_object(Bucket=BUCKET, Key="data/_alerts/targets-index.json")
            tx = json.loads(obj2["Body"].read())
            rpt["targets_index_state"] = {
                "n_equity_targets":  tx.get("n_equity_targets"),
                "n_macro_targets":   tx.get("n_macro_targets"),
                "n_equity_recent":   tx.get("n_equity_recent"),
                "n_macro_recent":    tx.get("n_macro_recent"),
                "equity_top3":       (tx.get("equity_targets") or [])[:3],
                "macro_top3":        (tx.get("macro_targets")  or [])[:3],
            }
        except ClientError as e:
            rpt["targets_index_state"] = f"NOT_WRITTEN: {e.response['Error']['Code']}"

    except Exception as e:
        rpt["fatal_err"] = str(e)[:500]
        rpt["traceback"] = traceback.format_exc()[-1500:]

    rpt["finished"] = datetime.now(timezone.utc).isoformat()
    p = os.path.join(REPO_ROOT, "aws/ops/reports/1148.json")
    os.makedirs(os.path.dirname(p), exist_ok=True)
    json.dump(rpt, open(p, "w"), indent=2, default=str)
    print(json.dumps({k: v for k, v in rpt.items() if k != "traceback"},
                     indent=2, default=str)[:4800])


if __name__ == "__main__":
    main()
