"""
ops_3881 — follow-up to ops 3880: does catalyst-calendar.json retain PAST
events (semis earnings/news in the last 2-4 weeks specifically), and read
rotation-dashboard's layer1_regime with the correct key (3880 guessed wrong).
Also checks the leadership->rotation angle rebalance-radar's docstring
mentions (crypto/other legs absorbing what semis lost) with real numbers.
WRITES NO CODE.
"""
import json
import sys
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
SEMI_TICKERS = {"NVDA","AMD","AVGO","TSM","MU","QCOM","TXN","INTC","LRCX","KLAC",
                "AMAT","ARM","ASML","MRVL","ON","SMCI","SMH","SOXX"}


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def main():
    with report("3881_semi_investigation_followup") as rep:
        rep.heading("ops 3881 — follow-up: past catalyst events + rotation-dashboard layer1_regime")
        failures = []

        rep.section("1. catalyst-calendar — does it retain PAST events, any semi-specific in last 30d")
        try:
            cc, cc_lm = get("data/catalyst-calendar.json")
            events = cc.get("events") or []
            rep.kv(as_of=cc.get("as_of"), n_events=len(events), window_days=cc.get("window_days"))
            days_to_vals = sorted(set(e.get("days_to") for e in events if e.get("days_to") is not None))
            rep.log(f"  days_to range across all events: min={min(days_to_vals)} max={max(days_to_vals)}")
            past = [e for e in events if (e.get("days_to") or 0) < 0]
            rep.log(f"  events with days_to < 0 (past): {len(past)}")
            semi_events = [e for e in events if e.get("ticker") in SEMI_TICKERS]
            rep.log(f"  semi-ticker events anywhere in the 60d window: {len(semi_events)}")
            for e in sorted(semi_events, key=lambda x: x.get("days_to", 999))[:20]:
                rep.log(f"    {e.get('date')} days_to={e.get('days_to')} {e.get('ticker')} "
                        f"{e.get('type')} impact={e.get('impact')} — {e.get('title')}")
            recent_or_upcoming = [e for e in events if -21 <= (e.get("days_to") or 999) <= 3]
            rep.log(f"  ALL events (any ticker) with days_to in [-21, 3] (i.e. spanning the past 3 weeks "
                    f"through Monday): {len(recent_or_upcoming)}")
            for e in sorted(recent_or_upcoming, key=lambda x: x.get("days_to", 999))[:15]:
                rep.log(f"    {e.get('date')} days_to={e.get('days_to')} type={e.get('type')} "
                        f"impact={e.get('impact')} — {e.get('title')}")
            hi7 = cc.get("high_impact_next_7d")
            rep.log(f"  high_impact_next_7d field: {json.dumps(hi7, default=str)[:1000]}")
        except Exception as e:
            rep.fail(f"  catalyst-calendar read/parse failed: {str(e)[:200]}")
            failures.append("catalyst-calendar")

        rep.section("2. rotation-dashboard — layer1_regime with the correct key")
        try:
            rd, rd_lm = get("data/rotation-dashboard.json")
            l1 = rd.get("layer1_regime")
            rep.log(f"  layer1_regime: {json.dumps(l1, default=str)[:1200]}")
            thesis = rd.get("thesis")
            rep.log(f"  thesis: {json.dumps(thesis, default=str)[:600]}")
            over = rd.get("overweight"); avoid = rd.get("avoid")
            rep.log(f"  overweight: {json.dumps(over, default=str)[:500]}")
            rep.log(f"  avoid: {json.dumps(avoid, default=str)[:500]}")
        except Exception as e:
            rep.fail(f"  rotation-dashboard read/parse failed: {str(e)[:200]}")
            failures.append("rotation-dashboard")

        rep.section("3. rebalance-radar qtd_proxies — real 'where did the money go' numbers")
        try:
            rr, rr_lm = get("data/rebalance-radar.json")
            qtd = rr.get("qtd_proxies")
            rep.log(f"  qtd_proxies (QTD = quarter-to-date leadership complex read): "
                    f"{json.dumps(qtd, default=str)[:2000]}")
            wf = rr.get("window_forensics")
            rep.log(f"  window_forensics: {json.dumps(wf, default=str)[:1500]}")
        except Exception as e:
            rep.fail(f"  rebalance-radar re-read failed: {str(e)[:200]}")
            failures.append("rebalance-radar-detail")

        rep.section("4. verdict")
        rep.kv(failures=str(failures))
        if len(failures) >= 3:
            rep.fail(f"ALL THREE follow-up reads failed: {failures}")
            sys.exit(1)
        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
