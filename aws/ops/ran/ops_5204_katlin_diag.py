"""ops_5204 -- KATLIN diagnostics: what did justhodl-katlin do on its first invocations?
Pulls the CloudWatch log tail (last 4h) for /aws/lambda/justhodl-katlin, prints every [katlin] progress line, every
REPORT/timeout/traceback line, the S3 state of the lanes it banks (crypto bars, 4h bars) and the artifact keys, so the
next fix is evidence-driven. Diagnostic only: always exits 0 unless the log group itself is unreadable."""
import json
import sys
import time
from pathlib import Path

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

CFG = Config(retries={"max_attempts": 3, "mode": "adaptive"}, read_timeout=60)
logs = boto3.client("logs", region_name="us-east-1", config=CFG)
s3 = boto3.client("s3", region_name="us-east-1", config=CFG)
lam = boto3.client("lambda", region_name="us-east-1", config=CFG)
BUCKET = "justhodl-dashboard-live"
FN = "justhodl-katlin"

with report("ops_5204_katlin_diag") as R:
    R.heading("ops 5204 -- KATLIN diagnostics")
    cfg = lam.get_function_configuration(FunctionName=FN)
    env = cfg.get("Environment", {}).get("Variables", {})
    R.log("   fn %s state=%s mem=%s timeout=%s env=%s codesize=%s" % (FN, cfg.get("State"), cfg.get("MemorySize"), cfg.get("Timeout"), sorted(env.keys()), cfg.get("CodeSize")))
    since = int((time.time() - 4 * 3600) * 1000)
    events = []
    token = None
    try:
        while True:
            kw = {"logGroupName": "/aws/lambda/" + FN, "startTime": since, "limit": 10000}
            if token:
                kw["nextToken"] = token
            r = logs.filter_log_events(**kw)
            events.extend(r.get("events") or [])
            token = r.get("nextToken")
            if not token or len(events) > 40000:
                break
    except Exception as e:
        R.fail("   log group unreadable: %s" % str(e)[:200])
        sys.exit(1)
    R.log("   %d log events in the last 4h" % len(events))
    R.section("every invocation: START / REPORT / errors")
    keep = []
    for e in events:
        m = e.get("message", "")
        if m.startswith(("START", "END", "REPORT")) or "Task timed out" in m or "Traceback" in m or "Error" in m or "error" in m[:40] or "[katlin]" in m or "MemoryError" in m or "Runtime." in m:
            keep.append((e["timestamp"], m.rstrip()))
    R.log("   %d relevant lines" % len(keep))
    # print [katlin] lines and errors verbatim (cap 260 lines, keep the tail of tracebacks)
    shown = 0
    last_ts = None
    for ts, m in keep:
        if shown >= 260:
            R.log("   ... (truncated)")
            break
        stamp = time.strftime("%H:%M:%S", time.gmtime(ts / 1000))
        if m.startswith("START"):
            R.log("   ---- %s %s" % (stamp, m[:80]))
        else:
            R.log("   %s %s" % (stamp, m[:400].replace("\n", " | ")))
        shown += 1
        last_ts = ts
    # tracebacks: pull the full multi-line message of the last error event
    errs = [e for e in events if "Traceback" in e.get("message", "") or "[ERROR]" in e.get("message", "")]
    if errs:
        R.section("last traceback (verbatim)")
        for line in errs[-1]["message"].splitlines()[-40:]:
            R.log("   " + line[:300])
    R.section("S3 state of Katlin's lanes")
    for prefix in ("data/warm/katlin/crypto-bars/", "data/warm/katlin/intraday-4h/", "data/katlin/history/", "data/katlin"):
        try:
            r = s3.list_objects_v2(Bucket=BUCKET, Prefix=prefix, MaxKeys=1000)
            objs = r.get("Contents") or []
            R.log("   %-34s %4d objects, %.1f MB, newest %s" % (prefix, len(objs), sum(o["Size"] for o in objs) / 1e6, max((o["LastModified"] for o in objs), default=None)))
        except Exception as e:
            R.log("   %s: %s" % (prefix, str(e)[:100]))
    try:
        head = s3.head_object(Bucket=BUCKET, Key="data/katlin.json")
        R.log("   data/katlin.json exists: %s bytes, %s" % (head.get("ContentLength"), head.get("LastModified")))
    except Exception as e:
        R.log("   data/katlin.json: %s" % str(e)[:80])
    R.ok("   diagnostics captured")
    sys.exit(0)
