"""ops_4224 — tonight's harvest tally: CQ-sourced LIVE rows, SSR state,
board totals after the bus+vault fires."""
import json
import sys
from collections import Counter
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

s3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"


def main():
    with report("4224_harvest_tally") as rep:
        rep.heading("ops 4224 — harvest tally")
        v = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/tradingview.json")["Body"].read())
        st = Counter()
        cq_rows = []
        for r in v.get("symbols") or []:
            st[r.get("status")] += 1
            if r.get("status") == "LIVE" and "cryptoquant" in str(
                    r.get("source") or ""):
                cq_rows.append([r.get("symbol"),
                                r.get("value")])
        rep.kv(**{k: n for k, n in st.most_common(4)})
        rep.kv(cq_live_rows=len(cq_rows))
        rep.log("  cq-sourced: " + json.dumps(cq_rows[:12])[:400])
        cd = json.loads(s3.get_object(
            Bucket=BUCKET, Key="data/cq-feed.json")["Body"].read())
        ssr = ((cd.get("metrics") or {}).get(
            "btc_market-indicator_stablecoin-supply-ratio") or {}
        ).get("fields") or {}
        rep.kv(feed_metrics=cd.get("n_metrics"),
               ssr=ssr.get("stablecoin_supply_ratio"))
        bus = json.loads(s3.get_object(
            Bucket=BUCKET,
            Key="data/indicator-bus.json")["Body"].read())
        rep.kv(bus_n=bus.get("n"))
        rep.ok(f"TALLY — LIVE={st.get('LIVE')} cq_rows={len(cq_rows)}")


if __name__ == "__main__":
    main()
