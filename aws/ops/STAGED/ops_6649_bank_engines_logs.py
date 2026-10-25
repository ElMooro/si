"""ops 6649 - read-only: CloudWatch REPORT/ERROR lines + S3 state for the new bank engines.

justhodl-fmp-walker's first run (ops 6646 kick, 16:26:54 UTC) wrote no state within 16 min - a Lambda timeout (hang)
or a crash before the first state write.  This op reads the last 3 h of /aws/lambda/<fn> for fmp-walker, fmp-bank,
polygon-bank and provider-bank: REPORT lines (duration, max memory, "Task timed out"), Traceback/ERROR lines (first 40
each), the invocation count, and each bank's state.json / manifest.json heads.  Nothing is changed.  Keys never printed.
"""
from datetime import datetime, timedelta, timezone
from pathlib import Path
import json
import re
import sys
import time

import boto3
from botocore.config import Config

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
from ops_report import report  # noqa: E402

REGION = "us-east-1"
LIVE = "justhodl-dashboard-live"
CFG = Config(retries={"max_attempts": 8, "mode": "standard"}, connect_timeout=10, read_timeout=120)
ENGINES = {
    "justhodl-fmp-walker": "data/warm/fmp/",
    "justhodl-fmp-bank": "data/warm/fmp-bank/",
    "justhodl-polygon-bank": "data/warm/polygon-bank/",
    "justhodl-provider-bank": "data/warm/provider-bank/",
}
SECRET_RE = re.compile(r"(apikey|apiKey|api_key|token|c=|key=)[=:]?[A-Za-z0-9_\-]{12,}", re.I)


def redact(s):
    return SECRET_RE.sub(lambda m: m.group(0)[:8] + "<redacted>", str(s))


def jget(s3, key):
    try:
        return json.loads(s3.get_object(Bucket=LIVE, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return {}


def main():
    logs = boto3.client("logs", region_name=REGION, config=CFG)
    s3 = boto3.client("s3", region_name=REGION, config=CFG)
    lam = boto3.client("lambda", region_name=REGION, config=CFG)
    since = int((datetime.now(timezone.utc) - timedelta(hours=3)).timestamp() * 1000)
    with report("ops_6649_bank_engines_logs") as r:
        r.heading("ops 6649 - bank engines: CloudWatch REPORT/ERROR lines + S3 state (read-only)")
        for fn, root in ENGINES.items():
            r.section(fn)
            try:
                cf = lam.get_function_configuration(FunctionName=fn)
                r.log("config: memory %s timeout %s ephemeral %s modified %s state %s" % (cf.get("MemorySize"), cf.get("Timeout"), (cf.get("EphemeralStorage") or {}).get("Size"), (cf.get("LastModified") or "")[:19], cf.get("State")))
                try:
                    cc = lam.get_function_concurrency(FunctionName=fn)
                    r.log("reserved concurrency: %s" % cc.get("ReservedConcurrentExecutions"))
                except Exception as e:  # noqa: BLE001
                    r.log("reserved concurrency: ? (%s)" % type(e).__name__)
            except Exception as e:  # noqa: BLE001
                r.warn("%s: %s" % (fn, type(e).__name__))
                continue
            group = "/aws/lambda/" + fn
            reports, errors, n_events = [], [], 0
            try:
                kwargs = {"logGroupName": group, "startTime": since, "limit": 10000}
                for _ in range(20):
                    resp = logs.filter_log_events(**kwargs)
                    for ev in resp.get("events", []):
                        n_events += 1
                        msg = ev.get("message") or ""
                        if msg.startswith("REPORT"):
                            reports.append((ev["timestamp"], msg.strip()))
                        elif "Task timed out" in msg or "Traceback" in msg or "[ERROR]" in msg or "Error" in msg[:40] or "MemoryError" in msg:
                            errors.append((ev["timestamp"], msg.strip()[:600]))
                    if resp.get("nextToken"):
                        kwargs["nextToken"] = resp["nextToken"]
                    else:
                        break
            except Exception as e:  # noqa: BLE001
                r.warn("logs %s: %s %s" % (group, type(e).__name__, redact(e)[:200]))
            r.log("events in last 3 h: %d, REPORT lines: %d, error-ish lines: %d" % (n_events, len(reports), len(errors)))
            for ts, msg in reports[-12:]:
                r.log("%s %s" % (datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%H:%M:%S"), redact(msg)[:300]))
            for ts, msg in errors[:40]:
                r.log("%s ERR %s" % (datetime.fromtimestamp(ts / 1000, timezone.utc).strftime("%H:%M:%S"), redact(msg)[:600]))
            st = jget(s3, root + "_state/state.json")
            man = jget(s3, root + "manifest.json")
            r.log("state: runs %s phase %s key_status %s lease_until %s updated_at %s" % (st.get("runs"), st.get("phase"), st.get("key_status"), st.get("lease_until"), st.get("updated_at")))
            lr = st.get("last_run") or {}
            r.log("last_run: calls %s retries %s puts %s unchanged %s seconds %s http %s steps %s" % (lr.get("calls"), lr.get("retries"), lr.get("puts"), lr.get("unchanged"), lr.get("seconds"), json.dumps(lr.get("http"))[:500], json.dumps(lr.get("steps"))[:600]))
            r.log("manifest: objects %s MB %.1f datasets %s/%s phase %s refused %s" % (man.get("objects"), (man.get("bytes") or 0) / 1e6, man.get("datasets"), man.get("datasets_declared"), man.get("phase"), json.dumps(man.get("endpoints_refused") or man.get("endpoints_not_served"))[:800]))
            if man.get("universe_counts"):
                r.log("universes: %s" % json.dumps(man.get("universe_counts"))[:600])
        r.ok("ops 6649 complete (read-only)")


if __name__ == "__main__":
    main()
    sys.exit(0)
