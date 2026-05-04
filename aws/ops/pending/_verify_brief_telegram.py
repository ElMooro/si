"""Force redeploy ai-brief + invoke + verify Telegram digest sent."""
import io
import json
import os
import time
import zipfile
import boto3
from ops_report import report

REGION = "us-east-1"
SOURCE_DIR = "aws/lambdas/justhodl-ai-brief/source"

lam = boto3.client("lambda", region_name=REGION)
s3 = boto3.client("s3", region_name=REGION)


def make_zip(source_dir):
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        for root, dirs, files in os.walk(source_dir):
            for f in files:
                if f.endswith(".pyc") or "__pycache__" in root:
                    continue
                full = os.path.join(root, f)
                rel = os.path.relpath(full, source_dir)
                zf.write(full, rel)
    buf.seek(0)
    return buf.read()


def main():
    with report("verify_brief_telegram") as r:
        # Wait
        r.heading("0) Wait for any in-progress update")
        for attempt in range(20):
            cfg = lam.get_function(FunctionName="justhodl-ai-brief")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.log(f"  ready, mod={cfg.get('LastModified')}")
                break
            time.sleep(3)

        # Force redeploy
        r.heading("1) Force redeploy")
        zb = make_zip(SOURCE_DIR)
        r.log(f"  zip size: {len(zb):,}b")
        try:
            lam.update_function_code(FunctionName="justhodl-ai-brief", ZipFile=zb)
            r.log("  ✓ update_function_code accepted")
        except Exception as e:
            r.log(f"  ✗ {e}")
            return

        for attempt in range(25):
            cfg = lam.get_function(FunctionName="justhodl-ai-brief")["Configuration"]
            if cfg["State"] == "Active" and cfg.get("LastUpdateStatus") in (None, "Successful"):
                r.ok(f"  ✓ deployed, mod={cfg.get('LastModified')}")
                break
            time.sleep(2)

        # Verify deployed source has telegram code
        r.heading("2) Inspect deployed source for Telegram digest code")
        try:
            cresp = lam.get_function(FunctionName="justhodl-ai-brief")
            url = cresp["Code"]["Location"]
            import urllib.request
            with urllib.request.urlopen(url, timeout=20) as resp:
                z = zipfile.ZipFile(io.BytesIO(resp.read()))
            for n in z.namelist():
                if n.endswith("lambda_function.py"):
                    src = z.read(n).decode("utf-8", errors="replace")
                    checks = [
                        ("build_telegram_digest", "def build_telegram_digest(" in src),
                        ("send_telegram", "def send_telegram(" in src),
                        ("get_telegram_chat_id", "def get_telegram_chat_id(" in src),
                        ("digest send block", "telegram digest sent:" in src),
                        ("SKIP_TELEGRAM env var", "SKIP_TELEGRAM" in src),
                    ]
                    for label, ok in checks:
                        r.log(f"  {'✓' if ok else '✗'} {label}")
                    break
        except Exception as e:
            r.log(f"  ✗ {e}")

        # Invoke
        r.heading("3) Invoke ai-brief end-to-end (will send Telegram)")
        t0 = time.time()
        resp = lam.invoke(FunctionName="justhodl-ai-brief", InvocationType="RequestResponse")
        body = resp["Payload"].read().decode()
        r.log(f"  status: {resp['StatusCode']}, duration: {time.time()-t0:.1f}s")
        try:
            outer = json.loads(body)
            inner = json.loads(outer.get("body", "{}"))
            r.log(f"  brief_chars: {inner.get('brief_chars')}")
        except Exception as e:
            r.log(f"  parse: {e}")

        # Pull CloudWatch logs to verify Telegram send happened
        r.heading("4) Check CloudWatch logs for Telegram send line")
        try:
            logs = boto3.client("logs", region_name=REGION)
            log_group = "/aws/lambda/justhodl-ai-brief"
            streams = logs.describe_log_streams(
                logGroupName=log_group, orderBy="LastEventTime", descending=True, limit=2
            )
            for st in streams.get("logStreams", []):
                ev = logs.get_log_events(
                    logGroupName=log_group, logStreamName=st["logStreamName"],
                    startFromHead=False, limit=200,
                )
                relevant = [e for e in ev.get("events", []) if "telegram" in (e.get("message", "") or "").lower()]
                if relevant:
                    r.log(f"  stream: {st['logStreamName']}")
                    for e in relevant[-10:]:
                        r.log(f"    {e['message'].strip()[:300]}")
                    break
        except Exception as e:
            r.log(f"  ✗ logs: {e}")

        # Pull the latest brief snapshot to see what would have been sent
        r.heading("5) Reconstruct what the Telegram digest looked like")
        try:
            obj = s3.get_object(Bucket="justhodl-dashboard-live", Key="data/ai-brief.json")
            d = json.loads(obj["Body"].read())
            snap = d.get("snapshot") or {}
            cal_v2 = snap.get("calibration_v2") or {}
            pp = snap.get("paper_portfolio") or {}
            sp = pp.get("signal_portfolio") or {}
            ml = pp.get("macro_loop2") or {}
            ed = snap.get("eurodollar_stress") or {}
            intel = snap.get("intelligence") or {}

            r.log(f"  Snapshot inputs available:")
            r.log(f"    cal_v2.highest_weight: {cal_v2.get('highest_weight')}")
            r.log(f"    cal_v2.weighted_mean_accuracy: {cal_v2.get('weighted_mean_accuracy')}")
            r.log(f"    cal_v2.top_weighted_signals[:3]: {[t.get('sig') for t in (cal_v2.get('top_weighted_signals') or [])[:3]]}")
            r.log(f"    paper.signal_portfolio.n_open: {sp.get('n_open')}")
            r.log(f"    paper.macro_loop2.system_alpha_pct: {ml.get('system_alpha_pct')}")
            r.log(f"    eurodollar.score: {ed.get('score') or ed.get('composite_score')}")
            r.log(f"    intel.phase: {intel.get('phase')}")
        except Exception as e:
            r.log(f"  ✗ {e}")


if __name__ == "__main__":
    main()
