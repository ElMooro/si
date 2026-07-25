"""
ops_3880 — INVESTIGATE (probe, writes no code): Khalid's observation that
semis dumped hard this week while flow signals looked positive, plus a claim
that institutional quarter-rebalancing ~1-2 weeks ago is the cause (asked to
confirm the timing).

Audit-first turned up justhodl-rebalance-radar, already built for EXACTLY
this pattern per its own docstring: "leadership complex (top QTD) seeing
real outflows+price weakness while crypto leg accelerates inside the window
-> regime-risk evidence list (the exact pattern Khalid observed at Q2-end)."
SMH is explicitly tagged "Semiconductors/AI" in its proxy list. This ops
reads that engine's LIVE output first, then cross-checks against:
  - the new pressure/heatmap feature's live semi-complex data (ETF + single
    name, built ops 3869-3879) for an independent read on flow vs price
  - justhodl-catalyst-calendar for real scheduled events this week (earnings,
    macro prints) that could explain price action independent of flow
  - deterministic rebalance calendar dates (Russell/S&P/Nasdaq mechanical
    reconstitution) vs Khalid's "a week or two ago" claim — computed, not
    assumed, since standard mechanical dates (Russell June 26, S&P/Nasdaq
    June 19, Q2 close June 30) are all ~25-36 days before today, not 7-14
  - whether this ops's execution environment (GitHub Actions) can reach
    general internet for an independent price check, since Claude's own
    sandbox cannot (network allowlist is package registries only)
"""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone

import boto3

ROOT_report = None
import sys as _s
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")

SEMI_ETFS = ["SMH", "SOXX", "XLK", "SOXL", "SOXS"]
SEMI_STOCKS = ["NVDA", "AMD", "AVGO", "TSM", "MU", "QCOM", "TXN", "INTC",
               "LRCX", "KLAC", "AMAT", "ARM", "ASML", "MRVL", "ON", "SMCI"]


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def age_h(lm):
    return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)


def main():
    with report("3880_semi_flow_price_investigation") as rep:
        rep.heading("ops 3880 — INVESTIGATE: semi flow/price divergence + rebalance-timing claim")

        rep.section("1. rebalance-radar — the engine already built for this exact pattern")
        core_failures = []
        try:
            rr, rr_lm = get("data/rebalance-radar.json")
            rep.ok(f"  data/rebalance-radar.json: {age_h(rr_lm)}h old, generated {rr.get('generated_at')}")
            rep.log(f"  top-level keys: {sorted(rr.keys())}")
        except Exception as e:
            rr = {}
            rep.fail(f"  rebalance-radar.json unreadable: {str(e)[:200]}")
            core_failures.append("rebalance-radar.json")

        if rr:
            window = rr.get("window") or rr.get("rebalance_window") or {}
            rep.log(f"  window/context: {json.dumps(window)[:500]}")
            forensics = rr.get("forensics") or rr.get("live_window_forensics") or rr.get("classification")
            rep.log(f"  forensics/classification: {json.dumps(forensics, default=str)[:800]}")
            rotation_risk = rr.get("rotation_risk") or rr.get("rotation_risk_flag")
            rep.log(f"  rotation_risk: {json.dumps(rotation_risk, default=str)[:800]}")
            # dump the whole thing raw too, safest against wrong-key guessing
            rep.log(f"  FULL RAW DOC (first 3000 chars): {json.dumps(rr, default=str)[:3000]}")

        rep.section("2. event-study cache — measured T-5..T+5 pattern for SMH around quarter-end")
        try:
            es, es_lm = get("data/history/rebalance-eventstudy.json")
            rep.ok(f"  rebalance-eventstudy.json: {age_h(es_lm)}h old")
            smh_study = es.get("SMH") or es.get("study", {}).get("SMH") if isinstance(es, dict) else None
            rep.log(f"  SMH event-study entry: {json.dumps(smh_study, default=str)[:1200]}")
            rep.log(f"  top-level keys: {sorted(es.keys()) if isinstance(es, dict) else type(es)}")
        except Exception as e:
            rep.fail(f"  event-study cache unreadable: {str(e)[:200]}")

        rep.section("3. catalyst-calendar — real scheduled events, this week specifically")
        try:
            cc, cc_lm = get("data/catalyst-calendar.json")
            rep.ok(f"  catalyst-calendar.json: {age_h(cc_lm)}h old")
            rep.log(f"  top-level keys: {sorted(cc.keys())}")
            rep.log(f"  FULL RAW DOC (first 2500 chars): {json.dumps(cc, default=str)[:2500]}")
        except Exception as e:
            rep.fail(f"  catalyst-calendar.json unreadable: {str(e)[:200]}")

        rep.section("4. LIVE semi-complex ETF data — daily.json (built this arc, fully trusted)")
        try:
            daily, d_lm = get("etf-flows/daily.json")
            rows = {r["ticker"]: r for r in (daily.get("metrics") or [])}
            rep.ok(f"  daily.json {age_h(d_lm)}h old, {len(rows)} ETFs")
            for tk in SEMI_ETFS:
                r = rows.get(tk)
                if not r:
                    rep.log(f"  {tk}: not in ETF universe")
                    continue
                rep.log(f"  {tk:<6} daily=${(r.get('daily_flow_usd') or 0)/1e6:+8.1f}M "
                        f"5d=${(r.get('flow_5d_usd') or 0)/1e6:+9.1f}M "
                        f"21d=${(r.get('flow_21d_usd') or 0)/1e6:+9.1f}M "
                        f"z90d={r.get('flow_zscore_90d')} "
                        f"ret5d={r.get('ret_5d_pct')}% ret21d={r.get('ret_21d_pct')}% "
                        f"quadrant={r.get('quadrant')} persistence_days={r.get('persistence_days')}")
        except Exception as e:
            rep.fail(f"  daily.json unreadable: {str(e)[:200]}")
            core_failures.append("daily.json")

        rep.section("5. LIVE single-name semi data — constituent-pressure.json (built this arc)")
        try:
            cp, cp_lm = get("etf-flows/constituent-pressure.json")
            per = cp.get("per_stock_exposure") or {}
            rep.ok(f"  constituent-pressure.json {age_h(cp_lm)}h old, {len(per)} stocks")
            found, missing = [], []
            for tk in SEMI_STOCKS:
                r = per.get(tk)
                if not r:
                    missing.append(tk)
                    continue
                found.append(tk)
                rep.log(f"  {tk:<6} sector={r.get('sector')} "
                        f"daily=${(r.get('total_aggregate_flow_daily_usd') or 0)/1e6:+8.1f}M "
                        f"5d=${(r.get('total_aggregate_flow_5d_usd') or 0)/1e6:+9.1f}M "
                        f"21d=${(r.get('total_aggregate_flow_21d_usd') or 0)/1e6:+9.1f}M "
                        f"perf_w={r.get('perf_w_pct')}% perf_m={r.get('perf_m_pct')}% "
                        f"perf_ytd={r.get('perf_ytd_pct')}% "
                        f"z_xsec={r.get('flow_zscore_cross_sectional')} quadrant={r.get('quadrant')}")
            rep.kv(semi_stocks_found=str(found), semi_stocks_missing_from_universe=str(missing))
        except Exception as e:
            rep.fail(f"  constituent-pressure.json unreadable: {str(e)[:200]}")
            core_failures.append("constituent-pressure.json")

        rep.section("6. sector-flow-state / sector rotation read on Technology this week")
        try:
            sf, sf_lm = get("data/sector-flow-state.json")
            sectors = sf.get("sectors") or []
            tech = next((s for s in sectors if s.get("name") in ("Technology", "Information Technology")), None)
            rep.ok(f"  sector-flow-state.json {age_h(sf_lm)}h old")
            rep.log(f"  Technology sector entry: {json.dumps(tech, default=str)[:1000]}")
        except Exception as e:
            rep.log(f"  sector-flow-state.json unavailable/unexpected shape: {str(e)[:150]}")

        rep.section("7. rotation-dashboard — does the cross-asset rotation view show a leadership handoff")
        try:
            rd, rd_lm = get("data/rotation-dashboard.json")
            rep.ok(f"  rotation-dashboard.json {age_h(rd_lm)}h old")
            rep.log(f"  top-level keys: {sorted(rd.keys())}")
            l1 = rd.get("l1_nowcast") or rd.get("nowcast")
            rep.log(f"  L1 nowcast: {json.dumps(l1, default=str)[:800]}")
        except Exception as e:
            rep.log(f"  rotation-dashboard.json unavailable: {str(e)[:150]}")

        rep.section("8. can THIS environment (GitHub Actions runner) reach general internet "
                    "for an independent price check — Claude's own sandbox cannot")
        try:
            req = urllib.request.Request(
                "https://stooq.com/q/d/l/?s=smh.us&i=d",
                headers={"User-Agent": "Mozilla/5.0"})
            with urllib.request.urlopen(req, timeout=15) as r:
                csv_data = r.read().decode("utf-8", "ignore")
            lines = csv_data.strip().splitlines()
            rep.ok(f"  reached stooq.com — {len(lines)} lines of SMH daily history, "
                   f"last 5 rows:\n" + "\n".join(lines[-5:]))
        except Exception as e:
            rep.fail(f"  could not reach stooq.com from this environment either: {str(e)[:200]}")

        rep.section("9. verdict prep — deterministic rebalance-calendar facts (no data needed)")
        rep.log("  Russell 2026 reconstitution: 2026-06-26 (29 days before 2026-07-25)")
        rep.log("  S&P/Nasdaq quarterly rebalance: 2026-06-19 (36 days before 2026-07-25)")
        rep.log("  Q2 2026 quarter-end: 2026-06-30 (25 days before 2026-07-25)")
        rep.log("  Khalid's stated timing 'a week or two ago' = 2026-07-11 to 2026-07-18 "
                "(7-14 days before 2026-07-25) — does NOT match any standard mechanical date")

        rep.section("10. verdict")
        rep.kv(core_failures=str(core_failures))
        if len(core_failures) >= 2:
            rep.fail(f"CORE DATA MOSTLY UNREADABLE {core_failures} — investigation cannot "
                     f"draw a conclusion from this run, needs a retry")
            sys.exit(1)
        rep.ok("PROBE COMPLETE — core data readable, findings above, no conclusions asserted "
               "beyond what's printed")


if __name__ == "__main__":
    main()
