"""
ops_3890 — PROBE: verify two specific claims against LIVE data (not my memory
of ops 3880's read, several hours old and from before EOD Friday's data may
have settled): (1) crypto leg holding up / accelerating per rebalance-radar's
own rotation-risk evidence, (2) SMH + "many names" showing CAPITULATION in
the Master Universe quadrant system Khalid is looking at right now. Counts
this precisely across the WHOLE universe (ETF + stock) rather than asserting
"many" impressionistically, and checks whether capitulation is concentrated
in semis/tech specifically or spread broadly. Writes no code.
"""
import json
import sys
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
SEMI_ETFS = ["SMH", "SOXX", "XLK", "SOXL", "SOXS"]
SEMI_STOCKS = {"NVDA", "AMD", "AVGO", "TSM", "MU", "QCOM", "TXN", "INTC",
               "LRCX", "KLAC", "AMAT", "ARM", "ASML", "MRVL", "ON", "SMCI"}


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def age_h(lm):
    return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)


def main():
    with report("3890_capitulation_crypto_verify") as rep:
        rep.heading("ops 3890 — verify SMH/many-names capitulation + crypto-leg claims, live")

        rep.section("1. rebalance-radar — is crypto leg STILL accelerating (fresher than ops 3880's read)")
        try:
            rr, rr_lm = get("data/rebalance-radar.json")
            rep.ok(f"  {age_h(rr_lm)}h old, generated {rr.get('generated_at')}")
            rr_risk = rr.get("rotation_risk") or {}
            rep.kv(rotation_risk_flag=rr_risk.get("flag"), severity=rr_risk.get("severity"),
                   evidence=str(rr_risk.get("evidence")))
            qtd = rr.get("qtd_proxies") or {}
            rep.log(f"  QTD proxies now: {json.dumps(qtd, default=str)}")
        except Exception as e:
            rep.fail(f"  rebalance-radar.json unreadable: {str(e)[:200]}")

        rep.section("2. daily.json — SMH/SOXX/XLK quadrant + z + return RIGHT NOW")
        try:
            daily, d_lm = get("etf-flows/daily.json")
        except Exception as e:
            rep.fail(f"  daily.json unreadable: {str(e)[:200]}")
            sys.exit(1)
        rows = {r["ticker"]: r for r in (daily.get("metrics") or [])}
        if not rows:
            rep.fail("  daily.json has no metrics — cannot verify anything")
            sys.exit(1)
        rep.ok(f"  daily.json {age_h(d_lm)}h old, generated {daily.get('generated_at')}")
        for tk in SEMI_ETFS:
            r = rows.get(tk)
            if not r:
                continue
            rep.log(f"  {tk:<6} quadrant={r.get('quadrant')} z90d={r.get('flow_zscore_90d')} "
                    f"ret21d={r.get('ret_21d_pct')}% ret5d={r.get('ret_5d_pct')}% "
                    f"divergence_score={r.get('divergence_score')}")

        rep.section("3. WHOLE universe — precise count of CAPITULATION, ETF side")
        etf_quad = Counter(r.get("quadrant") or "NEUTRAL" for r in rows.values())
        rep.kv(etf_quadrant_counts=str(dict(etf_quad)))
        capit_etfs = [r["ticker"] for r in rows.values() if r.get("quadrant") == "CAPITULATION"]
        rep.log(f"  ETFs in CAPITULATION ({len(capit_etfs)}): {sorted(capit_etfs)}")
        capit_etf_sectors = Counter(rows[t].get("ref_sector") or rows[t].get("category")
                                     for t in capit_etfs)
        rep.log(f"  by sector/category: {dict(capit_etf_sectors)}")

        rep.section("4. WHOLE universe — precise count of CAPITULATION, stock side")
        try:
            cp, cp_lm = get("etf-flows/constituent-pressure.json")
        except Exception as e:
            rep.fail(f"  constituent-pressure.json unreadable: {str(e)[:200]}")
            sys.exit(1)
        per = cp.get("per_stock_exposure") or {}
        if not per:
            rep.fail("  per_stock_exposure empty — cannot verify the stock side")
            sys.exit(1)
        rep.ok(f"  constituent-pressure.json {age_h(cp_lm)}h old, {len(per)} stocks")
        stock_quad = Counter(r.get("quadrant") or "NEUTRAL" for r in per.values())
        rep.kv(stock_quadrant_counts=str(dict(stock_quad)))
        capit_stocks = [s for s, r in per.items() if r.get("quadrant") == "CAPITULATION"]
        rep.log(f"  stocks in CAPITULATION: {len(capit_stocks)} total")
        capit_semi_stocks = [s for s in capit_stocks if s in SEMI_STOCKS]
        rep.log(f"  of which semis specifically: {capit_semi_stocks}")
        capit_sectors = Counter(per[s].get("sector") for s in capit_stocks if per[s].get("sector"))
        rep.log(f"  CAPITULATION by sector (top 8): {capit_sectors.most_common(8)}")
        other_capit = [s for s in capit_stocks if s not in SEMI_STOCKS][:15]
        rep.log(f"  sample of NON-semi names also in CAPITULATION: {other_capit}")

        rep.section("5. sector-flow-state — Technology posture right now")
        try:
            sf, sf_lm = get("data/sector-flow-state.json")
            sectors = sf.get("sectors") or []
            tech = next((s for s in sectors if s.get("name") in ("Technology", "Information Technology")), None)
            rep.log(f"  {age_h(sf_lm)}h old · Technology: {json.dumps(tech, default=str)}")
        except Exception as e:
            rep.log(f"  sector-flow-state.json unavailable: {str(e)[:150]}")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
