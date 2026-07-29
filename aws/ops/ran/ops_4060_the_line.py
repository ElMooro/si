"""ops_4060 — THE line: dedicated (zero-notes) requests only + failures."""
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
    with report("4060_the_line") as rep:
        rep.heading("ops 4060 — dedicated sources/watchlists requests only")
        now = datetime.now(timezone.utc)
        try:
            h = s3.head_object(Bucket=BUCKET, Key="data/tv-sources.json")
            rep.ok(f"  tv-sources.json EXISTS — {h['ContentLength']}B, "
                   f"{h['LastModified'].isoformat()}")
        except Exception:
            rep.log("  tv-sources.json absent at probe time")
        for pat, label in (('"\\"received\\": 0"', "zero-notes requests"),
                           ('"save failed"', "handler failures"),
                           ('"Traceback"', "crashes")):
            ev = logs.filter_log_events(
                logGroupName="/aws/lambda/justhodl-tv-notes-ingest",
                startTime=int((now - timedelta(minutes=100)).timestamp() * 1000),
                filterPattern=pat, limit=40)
            events = ev.get("events") or []
            rep.section(f"{label} — {len(events)} lines")
            for e in events[-16:]:
                ts = datetime.fromtimestamp(e["timestamp"] / 1000,
                                            tz=timezone.utc).strftime("%H:%M:%S")
                rep.log(f"  [{ts}] {e['message'].strip()[:210]}")
        rep.ok("PROBE DONE — the sources hop is on this page")


if __name__ == "__main__":
    main()
