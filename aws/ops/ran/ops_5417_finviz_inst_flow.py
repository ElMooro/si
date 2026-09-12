"""ops_5417 -- institutional footprint from Finviz universe.

No new Lambda. Ranks inst_trans_pct / inst_own_pct already in
data/finviz-universe.json. Writes data/finviz-inst-flow.json now
so we do not wait for the 14 UTC signals run.
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
REGION = "us-east-1"
UNI = "data/finviz-universe.json"


def main():
    with report("ops_5417_finviz_inst_flow") as R:
        R.heading("ops 5417 -- Finviz institutional footprint")
        s3 = boto3.client("s3", region_name=REGION)
        raw = json.loads(s3.get_object(Bucket=B, Key=UNI)["Body"].read())
        by = raw.get("by_ticker") or raw.get("tickers") or {}
        if isinstance(by, list):
            tmp = {}
            for r in by:
                tk = (r.get("ticker") or r.get("Ticker") or "").upper()
                if tk:
                    tmp[tk] = r
            by = tmp
        R.ok("universe n=%s generated=%s" % (len(by), raw.get("generated_at")))
        rows = []
        for tk, u in by.items():
            if not isinstance(u, dict):
                continue
            it = u.get("inst_trans_pct")
            if it is None:
                continue
            rows.append({
                "ticker": tk,
                "company": u.get("company"),
                "sector": u.get("sector"),
                "price": u.get("price") or u.get("prev_close"),
                "market_cap": u.get("market_cap"),
                "inst_own_pct": u.get("inst_own_pct"),
                "inst_trans_pct": it,
                "insider_own_pct": u.get("insider_own_pct"),
                "insider_trans_pct": u.get("insider_trans_pct"),
                "short_float_pct": u.get("short_float_pct"),
            })
        buy = sorted([r for r in rows if (r["inst_trans_pct"] or 0) > 0],
                     key=lambda r: -r["inst_trans_pct"])[:100]
        sell = sorted([r for r in rows if (r["inst_trans_pct"] or 0) < 0],
                      key=lambda r: r["inst_trans_pct"])[:100]
        both = [r for r in buy if (r.get("insider_trans_pct") or 0) > 0][:40]
        high_own_buy = [r for r in buy if (r.get("inst_own_pct") or 0) >= 50][:40]
        payload = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5417",
            "from": UNI,
            "src_generated_at": raw.get("generated_at"),
            "n_with_inst_trans": len(rows),
            "n_accumulating": len(buy),
            "n_distributing": len(sell),
            "status": "LIVE" if rows else "DATA_HOLD",
            "accumulating": buy,
            "distributing": sell,
            "inst_and_insider_buying": both,
            "high_own_still_buying": high_own_buy,
            "note": "inst_trans_pct is Finviz 3-month institutional ownership change",
        }
        s3.put_object(
            Bucket=B,
            Key="data/finviz-inst-flow.json",
            Body=json.dumps(payload, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/finviz-inst-flow.json")["Body"].read())
        if back.get("schema_version") != 1:
            R.fail("read-back")
            sys.exit(1)
        top = [(r["ticker"], r["inst_trans_pct"]) for r in buy[:8]]
        bot = [(r["ticker"], r["inst_trans_pct"]) for r in sell[:8]]
        R.ok("GREEN -- n=%s buy=%s sell=%s top=%s dump=%s" % (
            len(rows), len(buy), len(sell), top, bot))


if __name__ == "__main__":
    main()
