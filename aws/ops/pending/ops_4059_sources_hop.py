"""ops_4059 — corner the sources hop: filtered ingest logs + watchlist ts."""
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
    with report("4059_sources_hop") as rep:
        rep.heading("ops 4059 — which hop drops the sources?")
        now = datetime.now(timezone.utc)

        rep.section("A. watchlists timestamp — did the 23:48/00:03 syncs "
                    "complete THROUGH the watchlists phase?")
        h = s3.head_object(Bucket=BUCKET, Key="data/tv-watchlists.json")
        rep.kv(watchlists_modified=h["LastModified"].isoformat(),
               age_min=round((now - h["LastModified"]).total_seconds() / 60, 1))

        rep.section("B. ingest logs, notes-flood filtered, last 75 min")
        ev = logs.filter_log_events(
            logGroupName="/aws/lambda/justhodl-tv-notes-ingest",
            startTime=int((now - timedelta(minutes=75)).timestamp() * 1000),
            filterPattern='?sources ?watchlists ?Traceback ?ERROR ?fail',
            limit=100)
        events = ev.get("events") or []
        rep.kv(matching_lines=len(events))
        for e in events[-40:]:
            ts = datetime.fromtimestamp(e["timestamp"] / 1000,
                                        tz=timezone.utc).strftime("%H:%M:%S")
            rep.log(f"  [{ts}] {e['message'].strip()[:150]}")

        rep.section("C. raw invocation count last 75 min")
        ev2 = logs.filter_log_events(
            logGroupName="/aws/lambda/justhodl-tv-notes-ingest",
            startTime=int((now - timedelta(minutes=75)).timestamp() * 1000),
            filterPattern='"START RequestId"', limit=200)
        rep.kv(invocations=len(ev2.get("events") or []))
        rep.ok("PROBE DONE — the hop is named above")


if __name__ == "__main__":
    main()
