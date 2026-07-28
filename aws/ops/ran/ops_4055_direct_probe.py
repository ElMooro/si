"""ops_4055 — DIRECT: is the harvest visible server-side RIGHT NOW?"""
import json
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("4055_direct_probe") as rep:
        rep.heading("ops 4055 — server-side truth, this minute")
        now = datetime.now(timezone.utc)

        rep.section("A. the file")
        try:
            sr = json.loads(s3.get_object(Bucket=BUCKET,
                                          Key="data/tv-sources.json")["Body"].read())
            m = sr.get("sources") or {}
            rep.ok(f"  BORN — {len(m)} sources, generated {sr.get('generated_at')}")
            rep.kv(diag=json.dumps(sr.get("last_harvest_diag"))[:280])
            for k, v in list(m.items())[:8]:
                rep.log(f"    {k}: {str(v.get('source'))[:50]}")
        except Exception:
            rep.log("  ABSENT still")

        rep.section("B. ingest logs — last 30 min, everything")
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/justhodl-tv-notes-ingest",
            startTime=int((now - timedelta(minutes=30)).timestamp() * 1000),
            limit=100)
        events = ev.get("events") or []
        starts = sum(1 for e in events if "START" in e.get("message", ""))
        rep.kv(invocations_30min=starts, log_lines=len(events))
        for e in events[-30:]:
            ts = datetime.fromtimestamp(e["timestamp"] / 1000,
                                        tz=timezone.utc).strftime("%H:%M:%S")
            msg = e["message"].strip()[:130]
            if any(x in msg for x in ("START", "sources", "watchlists",
                                      "notes", "fail", "Error", "error",
                                      "Traceback", "REPORT")):
                rep.log(f"  [{ts}] {msg}")
        rep.ok("PROBE DONE")


if __name__ == "__main__":
    main()
