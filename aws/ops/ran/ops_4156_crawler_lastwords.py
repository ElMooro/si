"""ops_4156 — the crawler's last words + freshest source timestamps."""
import json
import sys
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("4156_crawler_lastwords") as rep:
        rep.heading("ops 4156 — crawler last words")
        c = s3.get_object(Bucket=BUCKET,
                          Key="data/tv-crawler-status.json")["Body"].read()
        rep.log("  STATUS VERBATIM: " + c.decode("utf-8", "ignore")[:340])
        d = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tv-sources.json")["Body"].read())
        rows = (d.get("sources") or d.get("symbols")
                or d.get("by_symbol") or d)
        items = list(rows.items()) if isinstance(rows, dict) else [
            (r.get("symbol"), r) for r in rows]
        rep.kv(entries=len(items),
               doc_ts=d.get("generated_at") or d.get("updated_at"))
        with_ts = []
        for sym, r in items:
            if isinstance(r, dict):
                ts = (r.get("harvested_at") or r.get("ts")
                      or r.get("updated_at") or r.get("t"))
                if ts:
                    with_ts.append((str(ts), sym))
        with_ts.sort(reverse=True)
        rep.kv(entries_with_ts=len(with_ts))
        for ts, sym in with_ts[:6]:
            rep.log(f"  newest: {ts[:19]}  {sym}")
        if with_ts:
            rep.log(f"  oldest: {with_ts[-1][0][:19]}  {with_ts[-1][1]}")
        rep.ok(f"LAST WORDS READ — {len(items)} entries")


if __name__ == "__main__":
    main()
