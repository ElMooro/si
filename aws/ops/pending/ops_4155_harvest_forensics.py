"""ops_4155 — harvest pipeline forensics: which stage died overnight?"""
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
ING = "justhodl-tv-notes-ingest"


def main():
    with report("4155_harvest_forensics") as rep:
        rep.heading("ops 4155 — harvest pipeline forensics")

        rep.section("A. S3 truth — every data/tv* object, LastModified")
        now = datetime.now(timezone.utc)
        resp = s3.list_objects_v2(Bucket=BUCKET, Prefix="data/tv")
        for o in sorted(resp.get("Contents", []),
                        key=lambda x: x["LastModified"], reverse=True):
            age_h = (now - o["LastModified"]).total_seconds() / 3600
            rep.log(f"  {o['Key']:34s} {o['Size']:>9,}B  "
                    f"age={age_h:6.1f}h")

        d = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tv-sources.json")["Body"].read())
        rows = (d.get("sources") or d.get("symbols")
                or d.get("by_symbol") or d)
        n = len(rows) if hasattr(rows, "__len__") else -1
        rep.kv(sources_entries=n, delta_vs_2019=n - 2019,
               doc_generated_at=d.get("generated_at")
               or d.get("updated_at"))

        rep.section("B. ingest lambda — overnight log truth")
        try:
            ev = logs.filter_log_events(
                logGroupName=f"/aws/lambda/{ING}",
                startTime=int((now - timedelta(hours=14))
                              .timestamp() * 1000),
                limit=250)
            events = ev.get("events") or []
            rep.kv(events_14h=len(events))
            if events:
                first = datetime.fromtimestamp(
                    events[0]["timestamp"] / 1000, tz=timezone.utc)
                last = datetime.fromtimestamp(
                    events[-1]["timestamp"] / 1000, tz=timezone.utc)
                rep.kv(first_event=str(first)[:16],
                       last_event=str(last)[:16])
                kinds = {}
                for e in events:
                    m = e["message"].strip()[:60]
                    key = m.split()[0][:22] if m else "?"
                    kinds[key] = kinds.get(key, 0) + 1
                rep.log("  message kinds: " + json.dumps(
                    dict(sorted(kinds.items(),
                                key=lambda x: -x[1])[:10]))[:400])
                for e in events[-8:]:
                    ts = datetime.fromtimestamp(
                        e["timestamp"] / 1000,
                        tz=timezone.utc).strftime("%H:%M")
                    rep.log(f"  [{ts}] {e['message'].strip()[:130]}")
            else:
                rep.log("  ZERO ingest events in 14h — nothing "
                        "reached the server")
        except Exception as e2:
            rep.log(f"  logs EXC {type(e2).__name__}: {str(e2)[:120]}")
        rep.ok(f"FORENSICS — sources {n}, see stage ages above")


if __name__ == "__main__":
    main()
