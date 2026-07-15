"""justhodl-analyst-actions — analyst rating & price-target signal board.

Sourced from FMP /stable (entitled key), replacing the retired Benzinga-
via-Massive feeds (Massive stopped serving Benzinga to this account —
403 NOT_AUTHORIZED on all keys, ops 3311-3316):
  - grades-latest-news       : upgrades/downgrades (rating transitions)
  - price-target-latest-news : price-target raises/cuts

Builds per-action lists + a per-ticker net analyst score (importance-
weighted). Rating transitions and PT revisions are among the most robust
public-data equity signals. Bullish corroborated names are logged to the
signal-harvester under eng:analyst-actions (MEASURE-BEFORE-TRUST — graded
vs SPY before any decision engine consumes them).

Guidance raise/cut is not exposed on FMP /stable, so guidance lists render
empty (schema-tolerated); PT deltas are carried on the ratings records.
"""
import json
import time
from collections import defaultdict
from datetime import datetime, timezone

import boto3

from fmp_analyst import fetch_ratings, fetch_guidance, fetch_analyst_insights

S3 = boto3.client("s3", region_name="us-east-1")
BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/analyst-actions.json"

# net-score weights (importance-scaled). Guidance is strongest, then rating
# transitions, then PT moves.
W_UPGRADE = 2.0
W_DOWNGRADE = -2.0
W_PT_RAISE = 1.0
W_PT_CUT = -1.0
W_GUID_RAISE = 3.0
W_GUID_CUT = -3.0


def _impw(imp):
    try:
        return max(0.4, min(1.2, 0.4 + 0.2 * float(imp)))  # imp1->0.6, imp5->1.2
    except (TypeError, ValueError):
        return 0.6


def lambda_handler(event=None, context=None):
    t0 = time.time()
    ratings = fetch_ratings(days_back=7, min_importance=1) or []
    guidance = fetch_guidance(days_back=21, min_importance=1) or []
    insights = fetch_analyst_insights(days_back=7) or []
    print(f"[analyst] ratings={len(ratings)} guidance={len(guidance)} insights={len(insights)}")

    upgrades = [r for r in ratings if r["rating_dir"] == "UPGRADE"]
    downgrades = [r for r in ratings if r["rating_dir"] == "DOWNGRADE"]
    pt_raises = sorted([r for r in ratings if r["pt_dir"] == "RAISE" and r.get("pt_pct")],
                       key=lambda r: r["pt_pct"], reverse=True)
    pt_cuts = sorted([r for r in ratings if r["pt_dir"] == "CUT" and r.get("pt_pct")],
                     key=lambda r: r["pt_pct"])
    guid_raises = [g for g in guidance if g["overall_dir"] == "RAISE"]
    guid_cuts = [g for g in guidance if g["overall_dir"] == "CUT"]

    # per-ticker rollup
    roll = defaultdict(lambda: {"ticker": None, "company": None, "net_score": 0.0,
                                "signals": [], "n_up": 0, "n_down": 0,
                                "n_pt_raise": 0, "n_pt_cut": 0,
                                "n_guid_raise": 0, "n_guid_cut": 0})

    def add(tk, company, delta, label):
        a = roll[tk]
        a["ticker"] = tk
        a["company"] = a["company"] or company
        a["net_score"] += delta
        if label not in a["signals"]:
            a["signals"].append(label)

    for r in ratings:
        tk = r.get("ticker")
        if not tk:
            continue
        w = _impw(r.get("importance"))
        if r["rating_dir"] == "UPGRADE":
            add(tk, r.get("company"), W_UPGRADE * w, f"UPGRADE {r.get('previous_rating')}\u2192{r.get('rating')} ({r.get('firm')})")
            roll[tk]["n_up"] += 1
        elif r["rating_dir"] == "DOWNGRADE":
            add(tk, r.get("company"), W_DOWNGRADE * w, f"DOWNGRADE {r.get('previous_rating')}\u2192{r.get('rating')} ({r.get('firm')})")
            roll[tk]["n_down"] += 1
        if r["pt_dir"] == "RAISE":
            pct = r.get("pt_pct") or 0
            bonus = min(1.0, abs(pct) / 20.0)  # big PT raises count more
            add(tk, r.get("company"), (W_PT_RAISE + bonus) * w, f"PT RAISE {r.get('pt_prev')}\u2192{r.get('pt')} ({r.get('firm')})")
            roll[tk]["n_pt_raise"] += 1
        elif r["pt_dir"] == "CUT":
            pct = r.get("pt_pct") or 0
            bonus = min(1.0, abs(pct) / 20.0)
            add(tk, r.get("company"), (W_PT_CUT - bonus) * w, f"PT CUT {r.get('pt_prev')}\u2192{r.get('pt')} ({r.get('firm')})")
            roll[tk]["n_pt_cut"] += 1

    for g in guidance:
        tk = g.get("ticker")
        if not tk:
            continue
        w = _impw(g.get("importance"))
        if g["overall_dir"] == "RAISE":
            add(tk, g.get("company"), W_GUID_RAISE * w, f"GUIDANCE RAISED ({g.get('fiscal_period')}{g.get('fiscal_year')})")
            roll[tk]["n_guid_raise"] += 1
        elif g["overall_dir"] == "CUT":
            add(tk, g.get("company"), W_GUID_CUT * w, f"GUIDANCE CUT ({g.get('fiscal_period')}{g.get('fiscal_year')})")
            roll[tk]["n_guid_cut"] += 1

    rollup = []
    for a in roll.values():
        a["net_score"] = round(a["net_score"], 2)
        a["n_distinct"] = sum(1 for k in ("n_up", "n_down", "n_pt_raise", "n_pt_cut",
                                          "n_guid_raise", "n_guid_cut") if a[k] > 0)
        a["bull_types"] = sum(1 for k in ("n_up", "n_pt_raise", "n_guid_raise") if a[k] > 0)
        rollup.append(a)
    rollup.sort(key=lambda a: a["net_score"], reverse=True)
    most_bullish = [a for a in rollup if a["net_score"] > 0][:30]
    most_bearish = [a for a in rollup if a["net_score"] < 0][-30:][::-1]

    # harvestable picks: net bullish, corroborated (>=2 distinct bullish signal
    # types OR a guidance raise OR an upgrade+PT-raise combo), importance implied
    top_picks = []
    for a in rollup:
        if a["net_score"] <= 0:
            continue
        corroborated = (a["bull_types"] >= 2 or a["n_guid_raise"] > 0
                        or (a["n_up"] > 0 and a["n_pt_raise"] > 0))
        if corroborated:
            top_picks.append({"ticker": a["ticker"], "score": a["net_score"],
                              "signals": a["signals"][:4], "company": a["company"]})
        if len(top_picks) >= 20:
            break

    out = {
        "engine": "justhodl-analyst-actions",
        "version": "2.0.0",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Analyst rating transitions, price-target revisions, and company "
                  "guidance changes — three of the most robust public-data equity "
                  "signals. Net analyst score is importance-weighted; guidance "
                  "raises count most, then upgrades, then PT moves.",
        "counts": {"ratings_7d": len(ratings), "guidance_21d": len(guidance),
                   "insights_7d": len(insights), "upgrades": len(upgrades),
                   "downgrades": len(downgrades), "pt_raises": len(pt_raises),
                   "pt_cuts": len(pt_cuts), "guidance_raises": len(guid_raises),
                   "guidance_cuts": len(guid_cuts)},
        "upgrades": upgrades[:40],
        "downgrades": downgrades[:40],
        "pt_raises": pt_raises[:40],
        "pt_cuts": pt_cuts[:30],
        "guidance_raises": guid_raises[:30],
        "guidance_cuts": guid_cuts[:30],
        "most_bullish": most_bullish,
        "most_bearish": most_bearish,
        "top_picks": top_picks,
        "data_source": "FMP grades-latest-news + price-target-latest-news (/stable)",
        "caveats": [
            "Rating direction uses a rank map; firm-specific scales may vary.",
            "Guidance raise/cut compares current vs previous guidance midpoints.",
            "Picks are logged to the harvester and graded vs SPY before any "
            "decision engine trusts them (measure-before-trust).",
        ],
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY,
                  Body=json.dumps(out, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=900")
    return {"statusCode": 200, "body": json.dumps({
        "upgrades": len(upgrades), "downgrades": len(downgrades),
        "pt_raises": len(pt_raises), "guidance_raises": len(guid_raises),
        "guidance_cuts": len(guid_cuts), "n_picks": len(top_picks),
        "top_pick": top_picks[0]["ticker"] if top_picks else None,
        "elapsed_s": out["elapsed_s"]})}
