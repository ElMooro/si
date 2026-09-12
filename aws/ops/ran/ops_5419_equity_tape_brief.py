"""ops_5419 -- equity tape brief.

Joins warehouse Finviz inst-flow, Polygon snapshot, ETF Global flows
into data/equity-tape-brief.json. Downstream engines/pages read this
key. Does not invoke other Lambdas and does not call Massive/Finviz HTTP.
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


def _load(s3, key):
    try:
        obj = s3.get_object(Bucket=B, Key=key)
        return json.loads(obj["Body"].read()), obj["LastModified"].isoformat()
    except Exception as e:
        return {"_error": str(e)[:160]}, None


def main():
    with report("ops_5419_equity_tape_brief") as R:
        R.heading("ops 5419 -- equity tape brief")
        s3 = boto3.client("s3", region_name="us-east-1")
        flow, flow_lm = _load(s3, "data/finviz-inst-flow.json")
        snap, snap_lm = _load(s3, "data/polygon-snapshot.json")
        etf, etf_lm = _load(s3, "data/etf-global-flows.json")
        sig, sig_lm = _load(s3, "data/finviz-signals.json")
        news, news_lm = _load(s3, "data/polygon-news.json")

        acc = (flow.get("accumulating") or [])[:15]
        dist = (flow.get("distributing") or [])[:15]
        ticks = snap.get("tickers") or []
        px = {t.get("ticker"): t for t in ticks if t.get("ticker")}

        etf_last = {}
        by = (etf.get("by_ticker") or {})
        for tk, pack in by.items():
            rows = pack.get("results") or []
            if rows:
                etf_last[tk] = {
                    "effective_date": rows[0].get("effective_date"),
                    "fund_flow": rows[0].get("fund_flow"),
                    "nav": rows[0].get("nav"),
                }

        confluence = (sig.get("confluence") or {}) if isinstance(sig, dict) else {}
        brief = {
            "generated_at": datetime.now(timezone.utc).isoformat(),
            "schema_version": 1,
            "source": "ops_5419",
            "status": "LIVE",
            "inputs": {
                "finviz_inst_flow": flow_lm,
                "polygon_snapshot": snap_lm,
                "etf_global_flows": etf_lm,
                "finviz_signals": sig_lm,
                "polygon_news": news_lm,
            },
            "spot": {k: {"last": v.get("day_c"), "chg": v.get("todaysChangePerc")} for k, v in px.items()},
            "etf_flow_last": etf_last,
            "inst_accum_top": [{"ticker": r.get("ticker"), "inst_trans_pct": r.get("inst_trans_pct"), "inst_own_pct": r.get("inst_own_pct")} for r in acc],
            "inst_dist_top": [{"ticker": r.get("ticker"), "inst_trans_pct": r.get("inst_trans_pct")} for r in dist],
            "confluence_keys": list(confluence.keys())[:12],
            "confluence_n": {k: len(confluence.get(k) or []) for k in list(confluence.keys())[:12]},
            "read_me": "Engines that need equity tape: s3.get_object data/equity-tape-brief.json. Do not call Massive or Finviz from those engines.",
        }
        s3.put_object(
            Bucket=B,
            Key="data/equity-tape-brief.json",
            Body=json.dumps(brief, default=str).encode("utf-8"),
            ContentType="application/json",
        )
        back = json.loads(s3.get_object(Bucket=B, Key="data/equity-tape-brief.json")["Body"].read())
        R.ok("GREEN -- brief LIVE spot=%s etf=%s accum=%s" % (
            len(back.get("spot") or {}),
            len(back.get("etf_flow_last") or {}),
            len(back.get("inst_accum_top") or {}),
        ))


if __name__ == "__main__":
    main()
