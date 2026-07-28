"""ops_4027 — LIVE verification: is Khalid's harvest actually talking to us?"""
import json
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
logs = boto3.client("logs", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def head(key):
    try:
        h = s3.head_object(Bucket=BUCKET, Key=key)
        return h["ContentLength"], h["LastModified"].isoformat()
    except Exception:
        return None, None


def main():
    with report("4027_harvest_live_check") as rep:
        rep.heading("ops 4027 — live harvest check")
        now = datetime.now(timezone.utc)

        rep.section("A. ingest lambda — heard from the browser recently?")
        try:
            start = int((now - timedelta(minutes=45)).timestamp() * 1000)
            ev = logs.filter_log_events(
                logGroupName="/aws/lambda/justhodl-tv-notes-ingest",
                startTime=start, limit=50)
            events = ev.get("events") or []
            rep.kv(events_last_45min=len(events))
            starts = [e for e in events if "START RequestId" in e.get("message", "")]
            rep.kv(invocations=len(starts))
            for e in events[-10:]:
                ts = datetime.fromtimestamp(e["timestamp"] / 1000,
                                            tz=timezone.utc).strftime("%H:%M:%S")
                rep.log(f"  [{ts}] {e['message'].strip()[:120]}")
        except Exception as e2:
            rep.log(f"  logs: {type(e2).__name__}: {str(e2)[:100]}")

        rep.section("B. artifacts — anything moved?")
        for key in ("data/tv-sources.json", "data/tv-watchlists.json",
                    "data/brain.json"):
            n, ts = head(key)
            fresh = ""
            if ts:
                age = (now - datetime.fromisoformat(ts)).total_seconds() / 60
                fresh = f"({age:.0f} min ago)" if age < 120 else f"({age/1440:.1f} d ago)"
            rep.log(f"  {key}: {n or 'ABSENT'} bytes, modified {ts} {fresh}")
        try:
            srcs = json.loads(s3.get_object(Bucket=BUCKET,
                                            Key="data/tv-sources.json")["Body"].read())
            m = srcs.get("sources") or {}
            rep.kv(sources_n=len(m))
            for k, v in list(m.items())[:8]:
                rep.log(f"    {k}: {str(v.get('source'))[:50]}")
        except Exception:
            rep.log("  tv-sources.json not born yet (expected until first "
                    "upload lands — auto-upload fires at harvest FINISH)")

        rep.ok("CHECK DONE — verdict is in the evidence above")


if __name__ == "__main__":
    main()
