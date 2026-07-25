"""
ops_3894 — PROBE (writes no code): does the system's history show signals
that genuinely LED price action, not just coincided with it same-day?

Real, dated archives confirmed to exist on repo audit:
  etf-flows/history/{date}.json          — full daily.json snapshot, dated
  data/archive/convergence-radar/{date}.json — per-ticker multi-engine flag count, dated

Plan: pull SMH/SOXX/XLK's snapshot from ~10-14 calendar days ago (before
today's confirmed capitulation), compare the flow/z-score/quadrant THEN
against the price move that's ALREADY HAPPENED since (known now, not a
forecast) — a genuine backward-looking check of lead time, not same-day
coincidence. Same idea for convergence-radar: what did it flag ~1-2 weeks
ago, and did those specific tickers subsequently move.

Every comparison here uses the platform's own FMP-sourced feeds as the price
source — this session has no working independent internet path (confirmed:
sandbox network is registry-only, and the one external test this session,
stooq.com via the ops runner, hit an anti-bot wall) — stated explicitly so
this isn't mistaken for a third-party cross-check.
"""
import json
import sys
from datetime import datetime, timezone, timedelta
from pathlib import Path

import boto3

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
from ops_report import report  # noqa: E402

BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")
TRACK_ETFS = ["SMH", "SOXX", "XLK", "XLE", "XLV", "XLF"]


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def try_get(key):
    try:
        return get(key)
    except Exception:
        return None, None


def main():
    with report("3894_lead_time_check") as rep:
        rep.heading("ops 3894 — did any signal genuinely LEAD today's known price action")

        rep.section("1. locate real dated history snapshots for etf-flows, last ~21 calendar days")
        today = datetime.now(timezone.utc).date()
        found_dates = []
        for days_back in range(1, 22):
            d = (today - timedelta(days=days_back)).isoformat()
            doc, lm = try_get(f"etf-flows/history/{d}.json")
            if doc:
                found_dates.append(d)
        rep.kv(n_history_snapshots_found=len(found_dates), dates=str(found_dates))
        if len(found_dates) < 3:
            rep.fail(f"  fewer than 3 historical snapshots found in the last 21 days — "
                     f"cannot do a genuine lead-time check without real dated history")
            sys.exit(1)

        rep.section("2. SMH/SOXX/XLK — flow/z/quadrant on the OLDEST available date vs TODAY")
        oldest = min(found_dates)
        newest_doc, _ = try_get(f"etf-flows/history/{max(found_dates)}.json")
        oldest_doc, _ = try_get(f"etf-flows/history/{oldest}.json")
        old_rows = {r["ticker"]: r for r in (oldest_doc.get("metrics") or [])} if oldest_doc else {}
        new_rows = {r["ticker"]: r for r in (newest_doc.get("metrics") or [])} if newest_doc else {}
        for tk in TRACK_ETFS:
            o, n = old_rows.get(tk), new_rows.get(tk)
            if not o or not n:
                rep.log(f"  {tk}: missing from oldest or newest snapshot")
                continue
            rep.log(f"  {tk:<6} [{oldest}] z={o.get('flow_zscore_90d')} quadrant={o.get('quadrant')} "
                     f"persistence={o.get('persistence_days')} ret21d_then={o.get('ret_21d_pct')}%  "
                     f"->  [{max(found_dates)}] z={n.get('flow_zscore_90d')} quadrant={n.get('quadrant')} "
                     f"ret21d_now={n.get('ret_21d_pct')}%")

        rep.section("3. convergence-radar — real dated archive, last 14 days, what got flagged")
        cr_dates_found = []
        cr_tickers_over_time = {}
        for days_back in range(1, 15):
            d = (today - timedelta(days=days_back)).isoformat()
            doc, lm = try_get(f"data/archive/convergence-radar/{d}.json")
            if doc:
                cr_dates_found.append(d)
                board = doc.get("board") or doc.get("tickers") or []
                if isinstance(board, dict):
                    board = list(board.values())
                for row in board[:50]:
                    tk = row.get("ticker") if isinstance(row, dict) else None
                    n_eng = row.get("n_engines") if isinstance(row, dict) else None
                    if tk:
                        cr_tickers_over_time.setdefault(tk, []).append((d, n_eng))
        rep.kv(n_convergence_radar_archive_days=len(cr_dates_found))
        if cr_dates_found:
            rep.log(f"  archive dates found: {sorted(cr_dates_found)}")
            # tickers that appeared on MULTIPLE distinct archive days (a real, sustained flag)
            sustained = {tk: hist for tk, hist in cr_tickers_over_time.items() if len(hist) >= 2}
            rep.log(f"  tickers flagged on 2+ distinct days ({len(sustained)}): "
                    f"{list(sustained.keys())[:20]}")
            for tk, hist in list(sustained.items())[:10]:
                rep.log(f"    {tk}: {hist}")
        else:
            rep.fail("  no convergence-radar archive days found in the last 14 days — "
                     "either the engine isn't running, or the archive key pattern differs "
                     "from what its own source declares")

        rep.section("4. cross-check: are any convergence-radar-flagged tickers ALSO showing "
                    "notable price moves in the LIVE constituent-pressure/daily data right now")
        try:
            cp, _ = get("etf-flows/constituent-pressure.json")
            per = cp.get("per_stock_exposure") or {}
        except Exception as e:
            per = {}
            rep.fail(f"  constituent-pressure.json unreadable: {str(e)[:150]}")
        checked = 0
        for tk in list(cr_tickers_over_time.keys())[:30]:
            rec = per.get(tk)
            if rec:
                checked += 1
                rep.log(f"    {tk}: perf_m={rec.get('perf_m_pct')}% quadrant={rec.get('quadrant')} "
                        f"flagged_on={[d for d, _ in cr_tickers_over_time[tk]]}")
        rep.kv(n_flagged_tickers_cross_checked_against_live_price=checked)

        rep.ok("PROBE COMPLETE — see logs above for concrete lead/lag evidence")


if __name__ == "__main__":
    main()
