"""justhodl-theme-cascade-backtest

Empirically validates the theme cascade thesis: do recent pumpers cluster
in hot themes? If yes → the cascade has predictive value. If no → reconsider.

METHOD:
  1. Read momentum-leaders.json (60 stocks with perf_5d, perf_20d, perf_60d)
  2. Read theme-rotation.json (current hot themes with momentum_score / RS rank)
  3. Read stock-exposure-lookup.json (ticker → ETFs that hold it)
  4. For each pumper (5d perf >= +5%), find its hottest ETF theme
  5. Compute aggregate stats:
     - % of pumpers in TOP-10 hot themes
     - % of pumpers in TOP-20 hot themes
     - Mean / median momentum_score of pumpers' themes
     - Mean / median RS rank of pumpers' themes
  6. Compare against NON-pumpers (perf_5d <= 0) as control group
  7. Report a validation_rate (higher = better theme thesis)

ALSO IDENTIFIES "LAGGARDS IN HOT THEMES" — stocks not yet pumping but in
the same themes as recent pumpers. These are the SECOND-WAVE candidates.

OUTPUT: data/theme-cascade-backtest.json
"""
import json
import time
from datetime import datetime, timezone
from typing import Optional

import boto3

S3_BUCKET = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1")


def _read_json(key: str) -> Optional[dict]:
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"[read] {key}: {e}")
        return None


def get_etfs_for_ticker(ticker: str, exposure_lookup: dict,
                        breadth_details: dict) -> list:
    """Return list of ETF tickers that hold this stock."""
    etfs = []
    info = exposure_lookup.get(ticker) if isinstance(exposure_lookup, dict) else None
    if info:
        for e in (info.get("top_etfs") or []):
            if isinstance(e, dict) and e.get("etf"):
                etfs.append(e["etf"])
    # Also check breadth_details
    for etf, info in (breadth_details or {}).items():
        for c in (info.get("constituents_perf") or []):
            if c.get("symbol") == ticker and etf not in etfs:
                etfs.append(etf)
    return etfs


def get_best_theme_for_ticker(ticker: str, etfs_for_ticker: list,
                               theme_index: dict) -> Optional[dict]:
    """Return the theme entry with HIGHEST momentum_score that holds this ticker."""
    candidates = [theme_index[e] for e in etfs_for_ticker if e in theme_index]
    if not candidates:
        return None
    return max(candidates, key=lambda x: x.get("momentum_score") or 0)


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[backtest] starting at {datetime.now(timezone.utc).isoformat()}")

    # Load data sources
    momentum = _read_json("data/momentum-leaders.json") or {}
    theme_rotation = _read_json("data/theme-rotation.json") or {}
    exposure_lookup = _read_json("etf-flows/stock-exposure-lookup.json") or {}

    leaders = momentum.get("leaders") or []
    print(f"[backtest] loaded {len(leaders)} momentum leaders")

    # Build theme index keyed by ETF
    all_themes = theme_rotation.get("all_themes") or []
    theme_index = {t.get("ticker"): t for t in all_themes if t.get("ticker")}
    print(f"[backtest] {len(theme_index)} ETF themes indexed")

    # Determine RS rank tiers
    # Sort themes by RS rank ascending (lower = better)
    sorted_by_rs = sorted(
        [t for t in all_themes if t.get("rs_rank_20d") is not None],
        key=lambda x: x.get("rs_rank_20d") or 999,
    )
    top_10_etfs = {t.get("ticker") for t in sorted_by_rs[:10]}
    top_20_etfs = {t.get("ticker") for t in sorted_by_rs[:20]}
    top_30_etfs = {t.get("ticker") for t in sorted_by_rs[:30]}

    breadth_details = theme_rotation.get("breadth_details") or {}

    # ── Categorize stocks by perf ──
    pumpers_5d = []       # perf_5d_pct >= +5%
    big_pumpers_5d = []   # perf_5d_pct >= +10%
    flat_or_down = []     # perf_5d_pct <= 0
    laggards_hot = []     # perf_5d_pct < 0 BUT in hot theme (second-wave candidates)

    for stock in leaders:
        t = stock.get("ticker")
        if not t:
            continue
        perf_5d = stock.get("perf_5d_pct")
        perf_20d = stock.get("perf_20d_pct")
        if perf_5d is None:
            continue

        # Find this stock's hottest theme
        etfs = get_etfs_for_ticker(t, exposure_lookup, breadth_details)
        best_theme = get_best_theme_for_ticker(t, etfs, theme_index)

        analysis = {
            "ticker": t,
            "perf_5d_pct": perf_5d,
            "perf_20d_pct": perf_20d,
            "n_etfs_holding": len(etfs),
            "hot_etf": best_theme.get("ticker") if best_theme else None,
            "hot_etf_name": best_theme.get("name") if best_theme else None,
            "theme_category": best_theme.get("category") if best_theme else None,
            "theme_momentum": best_theme.get("momentum_score") if best_theme else None,
            "theme_rs_rank": best_theme.get("rs_rank_20d") if best_theme else None,
            "theme_rs_accel": best_theme.get("rs_acceleration") if best_theme else None,
            "in_top_10": best_theme and best_theme.get("ticker") in top_10_etfs,
            "in_top_20": best_theme and best_theme.get("ticker") in top_20_etfs,
            "in_top_30": best_theme and best_theme.get("ticker") in top_30_etfs,
        }

        if perf_5d >= 10:
            big_pumpers_5d.append(analysis)
        if perf_5d >= 5:
            pumpers_5d.append(analysis)
        elif perf_5d <= 0:
            flat_or_down.append(analysis)
            if analysis["in_top_10"]:
                laggards_hot.append(analysis)

    # ── Compute validation stats ──
    def stats(group, label):
        if not group:
            return {"n": 0}
        with_theme = [g for g in group if g.get("hot_etf")]
        if not with_theme:
            return {"n": len(group), "n_with_theme": 0}
        in_top_10 = sum(1 for g in with_theme if g["in_top_10"])
        in_top_20 = sum(1 for g in with_theme if g["in_top_20"])
        in_top_30 = sum(1 for g in with_theme if g["in_top_30"])
        moms = [g["theme_momentum"] for g in with_theme if g.get("theme_momentum") is not None]
        rs_ranks = [g["theme_rs_rank"] for g in with_theme if g.get("theme_rs_rank") is not None]
        accels = [g["theme_rs_accel"] for g in with_theme if g.get("theme_rs_accel") is not None]
        return {
            "n": len(group),
            "n_with_theme": len(with_theme),
            "pct_in_top_10": round(100 * in_top_10 / len(with_theme), 1),
            "pct_in_top_20": round(100 * in_top_20 / len(with_theme), 1),
            "pct_in_top_30": round(100 * in_top_30 / len(with_theme), 1),
            "mean_theme_momentum": round(sum(moms) / len(moms), 1) if moms else None,
            "median_theme_momentum": sorted(moms)[len(moms)//2] if moms else None,
            "mean_theme_rs_rank": round(sum(rs_ranks) / len(rs_ranks), 1) if rs_ranks else None,
            "median_theme_rs_rank": sorted(rs_ranks)[len(rs_ranks)//2] if rs_ranks else None,
            "mean_theme_acceleration": round(sum(accels) / len(accels), 1) if accels else None,
            "label": label,
        }

    big_pumpers_stats = stats(big_pumpers_5d, "Big pumpers 5d >=+10%")
    pumpers_stats = stats(pumpers_5d, "Pumpers 5d >=+5%")
    control_stats = stats(flat_or_down, "Flat/down 5d <=0% (control)")
    laggards_hot_stats = stats(laggards_hot, "Laggards in hot themes")

    # ── Calculate validation_rate ──
    # If pumpers cluster in hot themes vs control, the thesis is validated
    if pumpers_stats.get("pct_in_top_10") and control_stats.get("pct_in_top_10"):
        lift_top_10 = pumpers_stats["pct_in_top_10"] - control_stats["pct_in_top_10"]
        lift_top_20 = pumpers_stats["pct_in_top_20"] - control_stats["pct_in_top_20"]
    else:
        lift_top_10 = None
        lift_top_20 = None

    # Theme momentum lift
    if pumpers_stats.get("mean_theme_momentum") and control_stats.get("mean_theme_momentum"):
        mom_lift = pumpers_stats["mean_theme_momentum"] - control_stats["mean_theme_momentum"]
    else:
        mom_lift = None

    # ── Print results ──
    print(f"\n[backtest] RESULTS:")
    print(f"  Big pumpers (+10% 5d):  n={big_pumpers_stats['n']}  pct_in_top_10={big_pumpers_stats.get('pct_in_top_10')}%")
    print(f"  Pumpers (+5% 5d):       n={pumpers_stats['n']}  pct_in_top_10={pumpers_stats.get('pct_in_top_10')}%")
    print(f"  Control (flat/down):    n={control_stats['n']}  pct_in_top_10={control_stats.get('pct_in_top_10')}%")
    print(f"  Lift top-10: +{lift_top_10}pp  Lift top-20: +{lift_top_20}pp")
    print(f"  Theme momentum lift: +{mom_lift}")
    print(f"  Laggards in hot themes (second-wave candidates): {laggards_hot_stats['n']}")

    elapsed = round(time.time() - t0, 1)

    # ── Output ──
    output = {
        "schema_version": "1.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": elapsed,
        "n_momentum_leaders": len(leaders),
        "n_etf_themes": len(theme_index),
        "top_10_etfs": list(top_10_etfs),
        "top_20_etfs": list(top_20_etfs),
        "big_pumpers_5d_stats": big_pumpers_stats,
        "pumpers_5d_stats": pumpers_stats,
        "control_stats": control_stats,
        "laggards_hot_stats": laggards_hot_stats,
        "lift_metrics": {
            "pct_in_top_10_lift_pp": lift_top_10,
            "pct_in_top_20_lift_pp": lift_top_20,
            "mean_theme_momentum_lift": mom_lift,
            "interpretation": (
                "Strong validation: lift > 30pp" if (lift_top_10 or 0) > 30 else
                "Validated: lift > 15pp" if (lift_top_10 or 0) > 15 else
                "Mild validation: lift 5-15pp" if (lift_top_10 or 0) > 5 else
                "Weak / no validation: lift < 5pp"
            ),
        },
        "big_pumpers_detail": sorted(big_pumpers_5d, key=lambda x: -(x.get("perf_5d_pct") or 0)),
        "pumpers_detail": sorted(pumpers_5d, key=lambda x: -(x.get("perf_5d_pct") or 0)),
        "laggards_hot_detail": sorted(laggards_hot,
                                       key=lambda x: -(x.get("theme_momentum") or 0))[:25],
    }

    s3.put_object(Bucket=S3_BUCKET, Key="data/theme-cascade-backtest.json",
                  Body=json.dumps(output, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")

    return {
        "statusCode": 200,
        "headers": {"Content-Type": "application/json",
                     "Access-Control-Allow-Origin": "*"},
        "body": json.dumps({
            "ok": True, "elapsed_s": elapsed,
            "n_big_pumpers": big_pumpers_stats["n"],
            "n_pumpers": pumpers_stats["n"],
            "n_control": control_stats["n"],
            "pumper_pct_in_top_10": pumpers_stats.get("pct_in_top_10"),
            "control_pct_in_top_10": control_stats.get("pct_in_top_10"),
            "lift_top_10_pp": lift_top_10,
            "interpretation": output["lift_metrics"]["interpretation"],
            "n_laggards_hot": laggards_hot_stats["n"],
            "top_5_big_pumpers": [
                {"ticker": p["ticker"], "perf_5d_pct": p["perf_5d_pct"],
                 "hot_etf": p["hot_etf"], "theme_rs_rank": p["theme_rs_rank"],
                 "theme_momentum": p["theme_momentum"]}
                for p in output["big_pumpers_detail"][:5]
            ],
            "top_5_laggards_hot": [
                {"ticker": l["ticker"], "perf_5d_pct": l["perf_5d_pct"],
                 "hot_etf": l["hot_etf"], "theme_momentum": l["theme_momentum"]}
                for l in output["laggards_hot_detail"][:5]
            ],
        }),
    }
