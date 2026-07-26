"""
ops_3898 — PROBE: ops 3897 read 6 real, populated feeds and got "NOT FOUND"
for TSLA/ORCL on every single one — because its find_ticker() guessed generic
container names (tickers/stocks/data/results) instead of using the REAL ones,
which that same probe's own "top-level keys" log already revealed:
  earnings-whisper:      all_setups, top_setups
  eps-revision-velocity: all_qualifying
  sec-filings-intel:     all_tickers, events_by_signal, highlights
  capital-return:        cannibals
  hiring-velocity:       top_50
  talent-migration:      departures, appointments, recent_moves
Redoing the exact same search against the exact same feeds with the correct
keys this time — no new engines, just reading the ones already fetched
properly. Writes no code.
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


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read()), o["LastModified"]


def age_h(lm):
    return round((datetime.now(timezone.utc) - lm).total_seconds() / 3600, 1)


def scan_for_ticker(obj, ticker, path="", hits=None):
    """Recursively walk any structure looking for dicts that mention this
    ticker by 'ticker' or 'symbol' key — no guessing which container name
    is right, just find it wherever it actually is."""
    if hits is None:
        hits = []
    if isinstance(obj, dict):
        if obj.get("ticker") == ticker or obj.get("symbol") == ticker:
            hits.append((path, obj))
        for k, v in obj.items():
            scan_for_ticker(v, ticker, f"{path}.{k}", hits)
    elif isinstance(obj, list):
        for i, v in enumerate(obj[:500]):  # cap to avoid pathological scans
            scan_for_ticker(v, ticker, f"{path}[{i}]", hits)
    return hits


def main():
    with report("3898_tsla_orcl_correct_containers") as rep:
        rep.heading("ops 3898 — same feeds, real container names this time")

        rep.section("1. earnings-whisper — all_setups + top_setups, search for TSLA")
        try:
            ew, ew_lm = get("data/earnings-whisper.json")
            hits = scan_for_ticker(ew.get("all_setups"), "TSLA") + scan_for_ticker(ew.get("top_setups"), "TSLA")
            rep.kv(earnings_whisper_age_h=age_h(ew_lm), tsla_hits=len(hits))
            for path, row in hits[:3]:
                rep.log(f"    {path}: {json.dumps(row, default=str)[:500]}")
            if not hits:
                rep.log(f"  n_upcoming={ew.get('n_upcoming')} tier_counts={ew.get('tier_counts')} "
                        f"— TSLA genuinely absent from this feed's setups")
        except Exception as e:
            rep.fail(f"  unreadable: {str(e)[:200]}")

        rep.section("2. eps-revision-velocity — all_qualifying, search for TSLA and ORCL")
        try:
            erv, erv_lm = get("data/eps-revision-velocity.json")
            aq = erv.get("all_qualifying")
            rep.kv(eps_revision_age_h=age_h(erv_lm), n_qualifying=len(aq) if isinstance(aq, list) else None,
                   summary=json.dumps(erv.get("summary"), default=str)[:400])
            for tk in ("TSLA", "ORCL"):
                hits = scan_for_ticker(aq, tk)
                rep.log(f"  {tk}: {len(hits)} hits")
                for path, row in hits[:3]:
                    rep.log(f"    {path}: {json.dumps(row, default=str)[:500]}")
        except Exception as e:
            rep.fail(f"  unreadable: {str(e)[:200]}")

        rep.section("3. sec-filings-intel — all_tickers + events_by_signal + highlights, search ORCL")
        try:
            sfi, sfi_lm = get("data/sec-filings-intel.json")
            rep.kv(sec_filings_age_h=age_h(sfi_lm), n_events_total=sfi.get("n_events_total"),
                   n_tickers_with_signals=sfi.get("n_tickers_with_signals"),
                   signal_definitions=json.dumps(sfi.get("signal_definitions"), default=str)[:400])
            hits = (scan_for_ticker(sfi.get("all_tickers"), "ORCL") +
                    scan_for_ticker(sfi.get("events_by_signal"), "ORCL") +
                    scan_for_ticker(sfi.get("highlights"), "ORCL"))
            rep.log(f"  ORCL hits: {len(hits)}")
            for path, row in hits[:8]:
                rep.log(f"    {path}: {json.dumps(row, default=str)[:500]}")
            # also raw-text scan highlights in case it's descriptive strings, not ticker-keyed dicts
            hl = json.dumps(sfi.get("highlights"), default=str)
            if "ORCL" in hl and not hits:
                idx = hl.find("ORCL")
                rep.log(f"  ORCL mentioned in highlights text: ...{hl[max(0,idx-100):idx+300]}...")
        except Exception as e:
            rep.fail(f"  unreadable: {str(e)[:200]}")

        rep.section("4. capital-return — cannibals list, is ORCL in it (aggressive buybacks)")
        try:
            cr, cr_lm = get("data/capital-return.json")
            cannibals = cr.get("cannibals") or []
            rep.kv(capital_return_age_h=age_h(cr_lm), n_cannibals=len(cannibals) if isinstance(cannibals, list) else None,
                   headline=cr.get("headline"))
            hits = scan_for_ticker(cannibals, "ORCL")
            rep.log(f"  ORCL in cannibals list: {len(hits)} hits")
            for path, row in hits[:3]:
                rep.log(f"    {json.dumps(row, default=str)[:600]}")
        except Exception as e:
            rep.fail(f"  unreadable: {str(e)[:200]}")

        rep.section("5. hiring-velocity top_50 + talent-migration departures/appointments — ORCL")
        try:
            hv, hv_lm = get("data/hiring-velocity.json")
            hits = scan_for_ticker(hv.get("top_50"), "ORCL")
            rep.kv(hiring_velocity_age_h=age_h(hv_lm))
            rep.log(f"  ORCL in hiring top_50: {len(hits)} hits: "
                    f"{json.dumps(hits[0][1], default=str)[:400] if hits else 'not present'}")
        except Exception as e:
            rep.fail(f"  hiring-velocity unreadable: {str(e)[:200]}")
        try:
            tm, tm_lm = get("data/talent-migration.json")
            dep_hits = scan_for_ticker(tm.get("departures"), "ORCL")
            app_hits = scan_for_ticker(tm.get("appointments"), "ORCL")
            recent_hits = scan_for_ticker(tm.get("recent_moves"), "ORCL")
            rep.kv(talent_migration_age_h=age_h(tm_lm), n_total=tm.get("n_total"),
                   n_departures=tm.get("n_departures"), n_appointments=tm.get("n_appointments"))
            rep.log(f"  ORCL departures: {len(dep_hits)}, appointments: {len(app_hits)}, "
                    f"recent_moves: {len(recent_hits)}")
            for path, row in (dep_hits + app_hits + recent_hits)[:5]:
                rep.log(f"    {path}: {json.dumps(row, default=str)[:400]}")
        except Exception as e:
            rep.fail(f"  talent-migration unreadable: {str(e)[:200]}")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
