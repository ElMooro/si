"""
ops_3896 — PROBE (writes no code): TSLA and ORCL both had real, large declines
this month (confirmed independently by Khalid, and matching this session's
own data: TSLA perf_m=-16.64%, ORCL perf_m=-27.0%). ops 3895 found both
sustained-flagged in convergence-radar's archive but only extracted raw
n_engines, not direction — an honest gap flagged last message. This closes
it: full directional time-series (bearish_engines, bullish_engines,
directional_score, exclude_from_longs, tier, pump_likelihood) for BOTH
tickers across every available archive snapshot in the relevant window, plus
a check of earnings-tracker's recent_results_30d for a concrete catalyst.
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
PREFIX = "data/archive/convergence-radar/"
TARGETS = ["TSLA", "ORCL"]


def get(key):
    o = s3.get_object(Bucket=BUCKET, Key=key)
    return json.loads(o["Body"].read())


def main():
    with report("3896_tsla_orcl_directional_check") as rep:
        rep.heading("ops 3896 — TSLA/ORCL full directional time-series + earnings catalyst check")

        rep.section("1. list every archive key (real S3 listing, not a guessed date format)")
        try:
            paginator = s3.get_paginator("list_objects_v2")
            keys = []
            for page in paginator.paginate(Bucket=BUCKET, Prefix=PREFIX):
                for o in page.get("Contents", []) or []:
                    keys.append(o["Key"])
        except Exception as e:
            rep.fail(f"  list failed: {str(e)[:200]}")
            sys.exit(1)
        keys.sort()
        if len(keys) < 10:
            rep.fail(f"  only {len(keys)} archive keys found — cannot build a real time-series")
            sys.exit(1)
        rep.kv(n_total_archive_files=len(keys), earliest=keys[0], latest=keys[-1])

        rep.section("2. dense sample across June 20 - July 25 specifically (the decline window)")
        # filenames are data/archive/convergence-radar/YYYYMMDD_HHMM.json
        dense = [k for k in keys if k[len(PREFIX):len(PREFIX)+8] >= "20260620"]
        # thin to roughly one per day (first seen each date) to keep this readable
        seen_dates = set()
        daily_sample = []
        for k in dense:
            d = k[len(PREFIX):len(PREFIX)+8]
            if d not in seen_dates:
                seen_dates.add(d)
                daily_sample.append(k)
        rep.kv(n_dense_snapshots=len(dense), n_daily_thinned=len(daily_sample))

        rep.section("3. full directional record for TSLA and ORCL, every day, June 20 - July 25")
        series = {t: [] for t in TARGETS}
        for k in daily_sample:
            try:
                doc = get(k)
            except Exception:
                continue
            board = doc.get("board") or doc.get("tickers") or []
            if isinstance(board, dict):
                board = list(board.values())
            by_ticker = {row.get("ticker"): row for row in board if isinstance(row, dict)}
            date = k[len(PREFIX):len(PREFIX)+8]
            for t in TARGETS:
                row = by_ticker.get(t)
                if row:
                    series[t].append({
                        "date": date,
                        "n_engines": row.get("n_engines"),
                        "n_bullish": row.get("n_bullish_eng"),
                        "n_bearish": row.get("n_bearish_eng"),
                        "directional_score": row.get("directional_score"),
                        "convergence_score": row.get("convergence_score"),
                        "exclude_from_longs": row.get("exclude_from_longs"),
                        "pump_likelihood": row.get("pump_likelihood"),
                        "tier": row.get("tier"),
                        "is_accelerating": row.get("is_accelerating"),
                    })

        for t in TARGETS:
            rep.log(f"  === {t} — day-by-day, {len(series[t])} snapshots found ===")
            for row in series[t]:
                rep.log(f"    {row['date']}: n_eng={row['n_engines']} bull={row['n_bullish']} "
                        f"bear={row['n_bearish']} dir_score={row['directional_score']} "
                        f"conv={row['convergence_score']} exclude_from_longs={row['exclude_from_longs']} "
                        f"tier={row['tier']} accelerating={row['is_accelerating']}")

        rep.section("4. did the bearish count meaningfully RISE before/during the decline, for either name")
        for t in TARGETS:
            vals = [(r["date"], r["n_bearish"]) for r in series[t] if r["n_bearish"] is not None]
            if len(vals) >= 2:
                first, last = vals[0], vals[-1]
                rep.kv(**{f"{t}_bearish_first": f"{first[0]}={first[1]}",
                          f"{t}_bearish_last": f"{last[0]}={last[1]}"})
            else:
                rep.log(f"  {t}: not enough bearish-count data points to trend")

        rep.section("5. earnings-tracker — was there a real, dated earnings catalyst for either name")
        try:
            et = get("data/earnings-tracker.json")
            recent = et.get("recent_results_30d") or []
            for t in TARGETS:
                rec = next((r for r in recent if r.get("ticker") == t), None)
                if rec:
                    rep.log(f"  {t} EARNINGS FOUND: {json.dumps(rec, default=str)}")
                else:
                    rep.log(f"  {t}: no entry in recent_results_30d (no earnings report in the last 30d, "
                            f"or it aged out — decline is NOT earnings-driven per this feed)")
        except Exception as e:
            rep.fail(f"  earnings-tracker.json unreadable: {str(e)[:200]}")

        rep.ok("PROBE COMPLETE")


if __name__ == "__main__":
    main()
