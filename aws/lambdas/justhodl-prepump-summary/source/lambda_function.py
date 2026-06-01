"""
justhodl-prepump-summary
═══════════════════════════
Builds data/pump-radar-summary.json — a slim, hero-card-sized recomposition
of brief + positioning + catalysts + clusters + early.

WHY THIS EXISTS
═══════════════
The Pre-Pump Radar hero card on index.html needs about 10 fields. Without
this file, the page had to download all 5 upstream sources (137.5 KB
total — 83.8 KB of which is pump-positioning.json with full trade
frameworks, stop/TP levels, and enriched candidate data the hero card
never displays).

This Lambda extracts only what the hero card needs and writes one slim
file (~3 KB raw, ~700 bytes gzipped). The hero card fetches THIS instead
of all 5 sources, cutting payload by ~98%.

WHAT IT WRITES
══════════════
{
  generated_at, freshness_seconds_brief, freshness_seconds_positioning,
  conviction, temperature: {label, score},
  executive_summary,                  # truncated to 200 chars
  top_picks: [                        # up to 3 names, hero-card ready
    {ticker, catalyst_grade, catalyst_type, position_pct, pump_confirmed,
     thesis_1liner}                   # 1-liner only when brief has it
  ],
  catalysts: {n_a, n_b, n_c, n_d, flagged_tickers},
  basket:    {n_positions, n_pump_confirmed, total_exposure},
  clusters:  [{id, quality, type, members, action}],  # top 2
  suggested_additions: [{ticker, grade}],            # up to 3
  early:     {n_confirmed_today, n_fresh, n_aging, actionable}
}

INPUTS
══════
data/pump-radar-brief.json       — conviction, temperature, exec summary
data/pump-positioning.json       — aggressive basket, top positions
data/catalysts.json              — A-grade counts, flagged
data/catalyst-clusters.json      — clusters, suggested_additions
data/velocity-acceleration.json  — early detection counts

SCHEDULE
════════
cron(12,42 * * * ? *) — twice per hour at :12 and :42.
Positioning runs at :10 hourly. Running at :12 catches it fresh. :42
catches any mid-hour refreshes (e.g. manual invokes).
"""
import gzip
import json
import os
import sys
import time
from datetime import datetime, timezone
from typing import Optional

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/pump-radar-summary.json"

INPUTS = {
    "brief":        "data/pump-radar-brief.json",
    "positioning":  "data/pump-positioning.json",
    "catalysts":    "data/catalysts.json",
    "clusters":     "data/catalyst-clusters.json",
    "early":        "data/velocity-acceleration.json",
}

s3 = boto3.client("s3", region_name="us-east-1")


def load_s3_json(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = json.loads(obj["Body"].read())
        # Capture last-modified for freshness reporting
        body["_last_modified"] = obj["LastModified"].isoformat() if obj.get("LastModified") else None
        return body
    except Exception as e:
        print(f"[load] {key}: {str(e)[:120]}")
        return None


def put_gzipped(key: str, payload: dict, max_age: int = 300) -> int:
    """Write JSON to S3 with Content-Encoding: gzip. Returns bytes written."""
    body = json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8")
    gzipped = gzip.compress(body, compresslevel=6)
    s3.put_object(
        Bucket=S3_BUCKET, Key=key, Body=gzipped,
        ContentType="application/json",
        ContentEncoding="gzip",
        CacheControl=f"public, max-age={max_age}",
    )
    return len(gzipped)


def freshness_seconds(iso_str: Optional[str]) -> Optional[int]:
    if not iso_str:
        return None
    try:
        ts = datetime.fromisoformat(iso_str.replace("Z", "+00:00"))
        return int((datetime.now(timezone.utc) - ts).total_seconds())
    except Exception:
        return None


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[summary] start {datetime.now(timezone.utc).isoformat()}")

    raw = {name: load_s3_json(key) for name, key in INPUTS.items()}
    n_loaded = sum(1 for v in raw.values() if v)
    print(f"[summary] loaded {n_loaded}/{len(INPUTS)} inputs")

    brief = raw.get("brief") or {}
    pos = raw.get("positioning") or {}
    cat = raw.get("catalysts") or {}
    clus = raw.get("clusters") or {}
    early = raw.get("early") or {}

    agg = pos.get("aggressive_basket") or {}
    positions = agg.get("positions") or []
    by_grade = cat.get("by_grade") or {}
    flagged = cat.get("flagged") or []
    clusters_in = clus.get("clusters") or []
    bas = clus.get("basket_action_summary") or {}

    # ── Build top_picks: prefer brief's top_3_long_ideas (richer), fall back to basket
    top_picks = []
    ideas = brief.get("top_3_long_ideas") or []
    if ideas:
        for idea in ideas[:3]:
            ticker = idea.get("ticker")
            # Find position size from basket
            pos_match = next((p for p in positions if p.get("ticker") == ticker), {})
            # Try to extract position_pct from sized_position string if not in basket
            size_pct = pos_match.get("position_pct")
            if size_pct is None:
                sp = idea.get("sized_position") or ""
                import re
                m = re.search(r"(\d+(?:\.\d+)?)\s*%", sp)
                if m:
                    try: size_pct = float(m.group(1))
                    except Exception: pass
            top_picks.append({
                "ticker":          ticker,
                "catalyst_grade":  idea.get("catalyst_grade") or pos_match.get("catalyst_grade"),
                "catalyst_type":   pos_match.get("catalyst_type"),
                "position_pct":    size_pct,
                "pump_confirmed":  pos_match.get("pump_confirmed", False),
                "thesis_1liner":   (idea.get("thesis_1liner") or "")[:160],
                "conviction":      idea.get("conviction"),
            })
    else:
        # Fallback: take top 3 basket positions
        for p in positions[:3]:
            top_picks.append({
                "ticker":         p.get("ticker"),
                "catalyst_grade": p.get("catalyst_grade"),
                "catalyst_type":  p.get("catalyst_type"),
                "position_pct":   p.get("position_pct"),
                "pump_confirmed": p.get("pump_confirmed", False),
            })

    # ── Cluster summaries (top 2)
    cluster_summaries = []
    for c in clusters_in[:2]:
        rec = c.get("recommendation") or {}
        cluster_summaries.append({
            "id":      c.get("cluster_id"),
            "type":    c.get("cluster_type"),
            "quality": c.get("quality_grade"),
            "scope":   c.get("scope"),
            "members": (c.get("members") or [])[:4],
            "leader":  rec.get("leader"),
            "action":  rec.get("action"),
        })

    # ── Suggested additions (compacted)
    suggested = []
    for sa in (bas.get("suggested_additions") or [])[:3]:
        suggested.append({
            "ticker": sa.get("ticker"),
            "grade":  sa.get("quality_grade"),
            "why":    (sa.get("rationale") or "")[:140],
        })

    # ── Build summary
    market_temp = brief.get("market_temperature") or {}
    exec_summary = (brief.get("executive_summary") or "")[:240]

    summary = {
        "schema_version": "1.0",
        "generated_at":   datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":    None,  # filled below
        "sources_freshness": {
            "brief_seconds":        freshness_seconds(brief.get("generated_at")),
            "positioning_seconds":  freshness_seconds(pos.get("generated_at")),
            "catalysts_seconds":    freshness_seconds(cat.get("generated_at")),
            "clusters_seconds":     freshness_seconds(clus.get("generated_at")),
            "early_seconds":        freshness_seconds(early.get("generated_at")),
        },
        "sources_loaded":   {k: bool(v) for k, v in raw.items()},

        "conviction":       brief.get("conviction_grade") or "—",
        "temperature": {
            "label": brief.get("market_temperature_label") or market_temp.get("rank"),
            "score": market_temp.get("score"),
        },
        "executive_summary": exec_summary,

        "top_picks":         top_picks,

        "catalysts": {
            "n_a_grade":      len(by_grade.get("A") or []),
            "n_b_grade":      len(by_grade.get("B") or []),
            "n_c_grade":      len(by_grade.get("C") or []),
            "n_d_grade":      len(by_grade.get("D") or []),
            "n_classified":   cat.get("n_classified"),
            "flagged":        flagged[:6],
        },

        "basket": {
            "n_positions":       agg.get("n_positions"),
            "n_pump_confirmed":  sum(1 for p in positions if p.get("pump_confirmed")),
            "total_exposure":    agg.get("total_exposure"),
        },

        "clusters": cluster_summaries,
        "suggested_additions": suggested,

        "early": {
            "trading_date":      early.get("trading_date"),
            "n_confirmed_today": early.get("n_confirmed_today", 0),
            "n_fresh":           early.get("n_fresh", 0),
            "n_aging":           early.get("n_aging", 0),
            "n_actionable":      early.get("n_actionable", 0),
            "actionable":        (early.get("actionable_tickers") or [])[:6],
        },
    }

    summary["elapsed_sec"] = round(time.time() - t0, 2)

    # Write gzipped — readers must support gzip (browsers do automatically)
    n_bytes = put_gzipped(OUTPUT_KEY, summary, max_age=300)

    # Also write a non-gzipped fallback to a sibling key in case some
    # downstream tool can't handle gzip. Cheap to do (~3 KB).
    try:
        plain = json.dumps(summary, default=str, indent=2).encode("utf-8")
        s3.put_object(
            Bucket=S3_BUCKET, Key="data/pump-radar-summary.plain.json", Body=plain,
            ContentType="application/json", CacheControl="public, max-age=300",
        )
    except Exception as e:
        print(f"[plain-fallback] {e}")

    print(f"[summary] wrote {n_bytes} bytes gzipped in {summary['elapsed_sec']}s "
          f"(top_picks={len(top_picks)}, clusters={len(cluster_summaries)})")
    return {"statusCode": 200, "body": json.dumps({
        "status":         "ok",
        "elapsed_sec":    summary["elapsed_sec"],
        "bytes_gzipped":  n_bytes,
        "n_top_picks":    len(top_picks),
        "n_clusters":     len(cluster_summaries),
        "n_suggested":    len(suggested),
        "conviction":     summary["conviction"],
    })}
