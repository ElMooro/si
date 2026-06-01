"""
justhodl-catalyst-clusters
══════════════════════════
Detects time-windowed clusters of catalysts in the aggressive basket and
recommends action: BOOST, RE_RANK, TRIM, or HEDGE.

THE INSIGHT (your insight)
══════════════════════════
When 3+ basket names share a catalyst window (e.g. semi earnings 6/2-6/4,
FOMC week, OPEC meeting), the tape moves them together. That can be:
  - GREAT (cluster leader expected to beat-and-raise → boost sizes), OR
  - HIDDEN LEVERAGE (3 positions with one shared driver = one bet at 3x).

The naive "boost or don't boost" version misses the nuance. The
institutional version DETECTS the cluster and grades its QUALITY to inform
a recommendation:

  STRONG cluster   → boost (concentrate weight in the leader)
  MIXED cluster    → re_rank (cut weak members, grow strong leader)
  BAD cluster      → trim (the catalyst won't lift everyone)
  MACRO cluster    → hedge_or_trim (FOMC/CPI moves everything; pre-trim risk)

CLUSTER TYPES
═════════════
TEMPORAL_EARNINGS — 3+ names with earnings in a 5-day window
                     (e.g. PANW 6/2, AVGO 6/3, CRWD 6/3 = semi cluster)
THEMATIC_PRODUCT  — 3+ names sharing same product/macro catalyst
                     (e.g. WDC, STX, SNDK all riding HDD/storage cycle)
MACRO_EVENT       — FOMC, CPI, OPEC, NFP, election dates
                     (these affect ALL names — different category)
SECTOR_ROTATION   — names clustering in one sector with sympathetic flow

CLUSTER QUALITY GRADING (A → D, computed)
═════════════════════════════════════════
Composite of:
  - Avg catalyst_grade of members (A,A,B = high; B,D,D = low)
  - Spread of momentum scores (tight = coordinated; wide = mixed quality)
  - Theme membership concordance (all in same theme = stronger)
  - Number of A-grade members
  - Macro regime adverse/supportive

  A → boost (concentrate, take leader to 20-25%)
  B → re_rank (boost leader to 18%, cut laggards to 4%)
  C → re_rank or hold (cautious)
  D → trim or hedge (cluster is hidden leverage with no edge)

RECOMMENDED ACTION (per cluster)
═════════════════════════════════
{
  "action":       "BOOST" | "RE_RANK" | "TRIM" | "HEDGE",
  "leader":       "WDC",          # strongest member (highest catalyst+momentum)
  "leader_new_size": 18.0,         # what to size the leader at (if BOOST/RE_RANK)
  "laggard_new_sizes": {           # per-laggard recommendations
    "MU":  4.0,
    "SNDK": 0
  },
  "rationale":    "...",
  "hedge_suggest": "SOXX puts 1m" # if HEDGE recommended
}

INPUTS
══════
data/catalysts.json          (per-ticker catalyst records, just built)
data/pump-positioning.json   (aggressive basket positions)
data/themes.json             (theme membership)
data/earnings-tracker.json   (calendar overlay)
data/ai-website-synthesis.json (macro regime context)

OUTPUT
══════
data/catalyst-clusters.json
{
  "schema_version": "1.0",
  "generated_at":   "...",
  "n_clusters":     3,
  "clusters":       [...],
  "global_recommendations": [...],  # consolidated actions
  "basket_action_summary": {
    "boost":   [ticker, ...],
    "trim":    [ticker, ...],
    "exclude": [ticker, ...],
    "hedge":   [...]
  }
}

SCHEDULE
════════
cron(15 14 * * ? *) — daily 14:15 UTC (after catalyst-classifier at 14:00)
"""
import json
import os
import sys
import time
from collections import Counter
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional, Set, Tuple

import boto3

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

S3_BUCKET     = "justhodl-dashboard-live"
OUTPUT_KEY    = "data/catalyst-clusters.json"
INPUT_KEYS = {
    "catalysts":    "data/catalysts.json",
    "positioning":  "data/pump-positioning.json",
    "themes":       "data/themes.json",
    "earnings_cal": "data/earnings-tracker.json",
    "macro":        "data/ai-website-synthesis.json",
    "momentum":     "data/momentum-leaders.json",
}

# Cluster detection thresholds
MIN_MEMBERS_FOR_CLUSTER = 3
EARNINGS_WINDOW_DAYS    = 5      # tickers within 5 days = "earnings cluster"
MACRO_EVENT_WINDOW_DAYS = 7      # FOMC/CPI within 7 days = macro cluster

GRADE_WEIGHTS = {"A": 1.0, "B": 0.75, "C": 0.5, "D": 0.2}

s3 = boto3.client("s3", region_name="us-east-1")


def load_s3_json(key: str) -> Optional[dict]:
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read())
    except Exception as e:
        print(f"[load] {key}: {str(e)[:120]}")
        return None


# ═════════════════════════════════════════════════════════════════════
# Cluster detection
# ═════════════════════════════════════════════════════════════════════

def detect_temporal_earnings_clusters(catalysts: List[dict], basket_tickers: Set[str]) -> List[dict]:
    """Names with EARNINGS_BEAT catalyst within EARNINGS_WINDOW_DAYS of each other.

    Cluster scope expanded to ALL classified catalysts (not just basket members)
    so we surface CLUSTERS-TO-CONSIDER as well as basket-internal clusters. Each
    cluster is tagged BASKET_LOCAL / MIXED / ADJACENT based on member composition.
    """
    clusters = []
    # ALL dated EARNINGS_BEAT catalysts (basket and adjacent)
    dated = []
    for c in catalysts:
        if c.get("catalyst_type") != "EARNINGS_BEAT":
            continue
        if not c.get("catalyst_date"):
            continue
        try:
            d = datetime.strptime(c["catalyst_date"][:10], "%Y-%m-%d").date()
            dated.append({**c, "_date": d})
        except Exception:
            continue

    if len(dated) < MIN_MEMBERS_FOR_CLUSTER:
        return []

    dated.sort(key=lambda x: x["_date"])
    used = set()
    for i, anchor in enumerate(dated):
        if anchor["ticker"] in used:
            continue
        members = [anchor]
        for j in range(i + 1, len(dated)):
            if dated[j]["ticker"] in used:
                continue
            if (dated[j]["_date"] - anchor["_date"]).days <= EARNINGS_WINDOW_DAYS:
                members.append(dated[j])
        if len(members) >= MIN_MEMBERS_FOR_CLUSTER:
            for m in members: used.add(m["ticker"])
            min_date = min(m["_date"] for m in members)
            max_date = max(m["_date"] for m in members)
            in_basket = [m["ticker"] for m in members if m["ticker"] in basket_tickers]
            adjacent  = [m["ticker"] for m in members if m["ticker"] not in basket_tickers]
            scope = ("BASKET_LOCAL" if len(adjacent) == 0
                      else ("ADJACENT" if len(in_basket) == 0 else "MIXED"))
            clusters.append({
                "cluster_id":   f"earnings_{min_date.isoformat()}",
                "cluster_type": "TEMPORAL_EARNINGS",
                "scope":        scope,
                "date_window":  f"{min_date.isoformat()} to {max_date.isoformat()}",
                "start_date":   min_date.isoformat(),
                "end_date":     max_date.isoformat(),
                "members":      [m["ticker"] for m in members],
                "in_basket":    in_basket,
                "adjacent":     adjacent,
                "member_records": [{k: v for k, v in m.items() if not k.startswith("_")} for m in members],
            })
    return clusters


def detect_thematic_clusters(catalysts: List[dict], basket_tickers: Set[str],
                                themes_doc: Optional[dict]) -> List[dict]:
    """Group ALL classified catalysts sharing catalyst_type + theme.
    Tag clusters as BASKET_LOCAL / MIXED / ADJACENT based on composition.
    """
    if not themes_doc:
        return []

    ticker_to_theme = themes_doc.get("ticker_to_theme", {}) or {}
    theme_meta_map = themes_doc.get("themes", {}) or {}

    buckets: Dict[Tuple[str, str], List[dict]] = {}
    for c in catalysts:
        ct = c.get("catalyst_type")
        theme = ticker_to_theme.get(c["ticker"])
        if not ct or not theme:
            continue
        if ct not in ("PRODUCT_LAUNCH", "MACRO_TAILWIND", "GUIDANCE_RAISE", "SYMPATHY_MOVE"):
            continue
        buckets.setdefault((ct, theme), []).append(c)

    clusters = []
    for (ct, theme), members in buckets.items():
        if len(members) < MIN_MEMBERS_FOR_CLUSTER:
            continue
        theme_label = (theme_meta_map.get(theme) or {}).get("label") or theme
        in_basket = [m["ticker"] for m in members if m["ticker"] in basket_tickers]
        adjacent  = [m["ticker"] for m in members if m["ticker"] not in basket_tickers]
        scope = ("BASKET_LOCAL" if len(adjacent) == 0
                  else ("ADJACENT" if len(in_basket) == 0 else "MIXED"))
        clusters.append({
            "cluster_id":     f"thematic_{theme}_{ct}",
            "cluster_type":   "THEMATIC_" + ct,
            "scope":          scope,
            "theme":          theme,
            "theme_label":    theme_label,
            "shared_catalyst_type": ct,
            "members":        [m["ticker"] for m in members],
            "in_basket":      in_basket,
            "adjacent":       adjacent,
            "member_records": members,
        })
    return clusters


# ═════════════════════════════════════════════════════════════════════
# Cluster quality grading + leader identification
# ═════════════════════════════════════════════════════════════════════

def grade_cluster(cluster: dict, momentum_map: Dict[str, float]) -> dict:
    """Compute cluster quality grade A→D + identify leader."""
    members = cluster["member_records"]

    # Avg catalyst grade weight
    grade_scores = [GRADE_WEIGHTS.get(m.get("catalyst_grade", "D"), 0.2) for m in members]
    avg_grade_score = sum(grade_scores) / len(grade_scores)

    # Count A-grade members
    n_a_grade = sum(1 for m in members if m.get("catalyst_grade") == "A")
    n_d_grade = sum(1 for m in members if m.get("catalyst_grade") == "D")

    # Momentum spread (tighter = more coordinated)
    mom_scores = [momentum_map.get(m["ticker"], 50) for m in members]
    mom_spread = max(mom_scores) - min(mom_scores) if mom_scores else 0
    spread_score = max(0, 1 - mom_spread / 50)  # 0 spread = 1, 50+ spread = 0

    # Quality composite (0-1)
    quality = 0.50 * avg_grade_score + 0.20 * spread_score + 0.30 * (n_a_grade / len(members))

    if quality >= 0.75:
        quality_grade = "A"
    elif quality >= 0.55:
        quality_grade = "B"
    elif quality >= 0.35:
        quality_grade = "C"
    else:
        quality_grade = "D"

    # Identify leader — highest catalyst_grade (A>B>C>D) then highest momentum
    def leader_key(m):
        g = GRADE_WEIGHTS.get(m.get("catalyst_grade", "D"), 0.2)
        mom = momentum_map.get(m["ticker"], 0)
        return (-g, -mom)
    sorted_members = sorted(members, key=leader_key)
    leader = sorted_members[0]["ticker"]

    return {
        "quality":        round(quality, 3),
        "quality_grade":  quality_grade,
        "avg_grade_score": round(avg_grade_score, 3),
        "n_a_grade":      n_a_grade,
        "n_d_grade":      n_d_grade,
        "momentum_spread": round(mom_spread, 1),
        "leader":         leader,
        "ranked_members": [
            {"ticker": m["ticker"],
             "catalyst_grade": m.get("catalyst_grade"),
             "momentum": momentum_map.get(m["ticker"], 0)}
            for m in sorted_members
        ],
    }


# ═════════════════════════════════════════════════════════════════════
# Action recommendation
# ═════════════════════════════════════════════════════════════════════

def recommend_action(cluster: dict, current_sizes: Dict[str, float],
                       macro_regime: str) -> dict:
    """Per-cluster action: BOOST, RE_RANK, TRIM, HEDGE, or CONSIDER_ADD."""
    q_grade = cluster["quality_grade"]
    leader = cluster["leader"]
    members = cluster["members"]
    n_d_grade = cluster["n_d_grade"]
    cluster_type = cluster["cluster_type"]
    scope = cluster.get("scope", "BASKET_LOCAL")
    macro_adverse = macro_regime in ("DEFENSIVE", "EXTREME", "RISK_OFF")

    leader_current = current_sizes.get(leader, 0)

    # ADJACENT clusters — members aren't in the basket, suggest ADD
    if scope == "ADJACENT":
        if q_grade in ("A", "B"):
            action = "CONSIDER_ADD"
            rationale = (f"{cluster_type} {q_grade}-grade cluster outside the basket. "
                          f"Leader {leader} would be a strong addition — consider opening a position. "
                          f"Cluster will move together; missing the leader means missing the trade.")
        else:
            action = "MONITOR"
            rationale = f"Adjacent cluster but quality grade {q_grade} — watch only, don't add."
        return {
            "action":             action,
            "scope":              scope,
            "leader":             leader,
            "leader_current":     0,
            "leader_new_size":    None,  # not in basket, no resize
            "suggested_entry":    leader if action == "CONSIDER_ADD" else None,
            "laggard_new_sizes":  {},
            "rationale":          rationale,
            "hedge_suggest":      None,
            "macro_adverse":      macro_adverse,
        }

    # Default action mapping for BASKET_LOCAL and MIXED clusters
    action = "RE_RANK"
    leader_new_size = leader_current
    laggard_sizes: Dict[str, float] = {}
    hedge_suggest = None
    rationale_parts = []

    if cluster_type.startswith("MACRO_"):
        action = "HEDGE_OR_TRIM"
        hedge_suggest = "Consider trimming risk pre-event or buying protective puts on cluster proxy ETF"
        for t in members:
            if t in current_sizes:
                laggard_sizes[t] = round(current_sizes.get(t, 0) * 0.7, 2)
        rationale_parts.append("Macro event cluster — pre-event vol risk; tape moves all together.")

    elif q_grade == "A":
        action = "BOOST"
        leader_new_size = min(25, round(leader_current * 1.6, 2))
        for t in members:
            if t == leader or t not in current_sizes: continue
            cur = current_sizes.get(t, 0)
            laggard_sizes[t] = round(cur * 0.7, 2)
        rationale_parts.append("Strong cluster — leader has A-grade catalyst, members coordinated.")
        rationale_parts.append(f"Concentrate weight in {leader} to ~{leader_new_size}%, trim laggards by 30%.")

    elif q_grade == "B":
        action = "RE_RANK"
        leader_new_size = min(20, round(leader_current * 1.3, 2))
        for t in members:
            if t == leader or t not in current_sizes: continue
            cur = current_sizes.get(t, 0)
            laggard_sizes[t] = round(cur * 0.5, 2)
        rationale_parts.append("Mixed cluster — leader is solid B+ but laggards are weaker.")
        rationale_parts.append(f"Boost {leader} to ~{leader_new_size}%, halve laggards.")

    elif q_grade == "C":
        action = "RE_RANK"
        leader_new_size = leader_current  # hold
        for t in members:
            if t == leader or t not in current_sizes: continue
            laggard_sizes[t] = round(current_sizes.get(t, 0) * 0.5, 2)
        rationale_parts.append("Cautious cluster — laggards lack edge. Hold leader, cut laggards 50%.")

    else:  # D
        action = "TRIM"
        leader_new_size = round(leader_current * 0.5, 2)
        for t in members:
            if t == leader or t not in current_sizes: continue
            laggard_sizes[t] = 0  # exclude entirely
        rationale_parts.append("Bad cluster — predominantly D-grade catalysts.")
        rationale_parts.append("Hidden leverage with no edge. Trim leader 50%, drop laggards.")

    if macro_adverse:
        rationale_parts.append(f"Macro regime {macro_regime} — additional 30% downscale.")
        leader_new_size = round(leader_new_size * 0.7, 2)
        laggard_sizes = {t: round(s * 0.7, 2) for t, s in laggard_sizes.items()}

    return {
        "action":             action,
        "scope":              scope,
        "leader":             leader,
        "leader_current":     leader_current,
        "leader_new_size":    leader_new_size,
        "laggard_new_sizes":  laggard_sizes,
        "rationale":          " ".join(rationale_parts),
        "hedge_suggest":      hedge_suggest,
        "macro_adverse":      macro_adverse,
    }


# ═════════════════════════════════════════════════════════════════════
# Lambda handler
# ═════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    t0 = time.time()
    print(f"[clusters] start {datetime.now(timezone.utc).isoformat()}")

    # Load inputs
    raw = {name: load_s3_json(key) for name, key in INPUT_KEYS.items()}

    catalysts_doc = raw.get("catalysts")
    if not catalysts_doc:
        return _write_error("No catalysts.json — run catalyst-classifier first")
    catalysts = catalysts_doc.get("catalysts") or []
    if not catalysts:
        return _write_error("No catalyst records found")

    positioning = raw.get("positioning") or {}
    agg = positioning.get("aggressive_basket") or {}
    positions = agg.get("positions") or []
    if not positions:
        return _write_error("No aggressive basket positions")

    basket_tickers: Set[str] = {p["ticker"] for p in positions}
    current_sizes: Dict[str, float] = {p["ticker"]: p.get("position_pct", 0) for p in positions}
    print(f"[clusters] basket: {len(basket_tickers)} tickers")

    momentum_doc = raw.get("momentum") or {}
    momentum_map: Dict[str, float] = {}
    for m in (momentum_doc.get("all_scored") or momentum_doc.get("leaders") or []):
        if m.get("ticker"):
            momentum_map[m["ticker"]] = m.get("momentum_score", 50)

    macro_regime = ((raw.get("macro") or {}).get("synthesis") or {}).get("global_posture", "NEUTRAL")
    print(f"[clusters] macro_regime: {macro_regime}")

    # Detect clusters
    temporal_clusters = detect_temporal_earnings_clusters(catalysts, basket_tickers)
    thematic_clusters = detect_thematic_clusters(catalysts, basket_tickers, raw.get("themes"))
    print(f"[clusters] temporal_earnings: {len(temporal_clusters)}, thematic: {len(thematic_clusters)}")

    all_clusters = temporal_clusters + thematic_clusters

    # Grade + recommend action per cluster
    for c in all_clusters:
        grade = grade_cluster(c, momentum_map)
        c.update(grade)
        c["recommendation"] = recommend_action(c, current_sizes, macro_regime)

    # Sort by quality
    all_clusters.sort(key=lambda c: (-GRADE_WEIGHTS.get(c["quality_grade"], 0),
                                       -len(c["members"])))

    # Build global recommendations rollup
    boost: List[str] = []
    trim: List[str] = []
    exclude: List[str] = []
    suggested_additions: List[dict] = []   # NEW — names not in basket the system says to add
    hedges: List[dict] = []

    proposed_new_sizes: Dict[str, float] = dict(current_sizes)

    for c in all_clusters:
        rec = c["recommendation"]
        leader = rec["leader"]

        # Handle ADJACENT-scope clusters: suggest adding the leader
        if rec.get("action") == "CONSIDER_ADD":
            suggested_additions.append({
                "ticker":       leader,
                "cluster_id":   c["cluster_id"],
                "quality_grade": c["quality_grade"],
                "rationale":    rec["rationale"][:200],
                "cluster_members": c["members"],
            })
            continue

        # In-basket / mixed: actually resize
        leader_new = rec.get("leader_new_size", current_sizes.get(leader, 0))
        if leader_new is None:
            continue
        if leader_new > current_sizes.get(leader, 0):
            boost.append(leader)
            proposed_new_sizes[leader] = max(proposed_new_sizes.get(leader, 0), leader_new)
        elif leader_new < current_sizes.get(leader, 0):
            trim.append(leader)
            proposed_new_sizes[leader] = min(proposed_new_sizes.get(leader, 100), leader_new)
        for t, sz in rec.get("laggard_new_sizes", {}).items():
            if sz == 0:
                exclude.append(t)
                proposed_new_sizes[t] = 0
            elif sz < current_sizes.get(t, 0):
                trim.append(t)
                proposed_new_sizes[t] = min(proposed_new_sizes.get(t, 100), sz)
        if rec.get("hedge_suggest"):
            hedges.append({"cluster_id": c["cluster_id"], "suggestion": rec["hedge_suggest"]})

    output = {
        "schema_version":  "1.0",
        "generated_at":    datetime.now(timezone.utc).isoformat(),
        "elapsed_sec":     round(time.time() - t0, 2),
        "macro_regime":    macro_regime,
        "n_clusters":      len(all_clusters),
        "n_temporal":      len(temporal_clusters),
        "n_thematic":      len(thematic_clusters),
        "clusters":        all_clusters,
        "basket_action_summary": {
            "boost":              sorted(set(boost)),
            "trim":               sorted(set(trim)),
            "exclude":            sorted(set(exclude)),
            "suggested_additions": suggested_additions,
            "hedges":             hedges,
        },
        "current_sizes":      current_sizes,
        "proposed_new_sizes": proposed_new_sizes,
        "size_deltas": {t: round(proposed_new_sizes.get(t, 0) - current_sizes.get(t, 0), 2)
                         for t in current_sizes.keys()},
        "config": {
            "min_members_for_cluster":  MIN_MEMBERS_FOR_CLUSTER,
            "earnings_window_days":     EARNINGS_WINDOW_DAYS,
            "macro_event_window_days":  MACRO_EVENT_WINDOW_DAYS,
            "grade_weights":            GRADE_WEIGHTS,
        },
    }

    body = json.dumps(output, indent=2, default=str)
    s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY, Body=body,
                    ContentType="application/json", CacheControl="max-age=900")

    summary = {
        "status":        "ok",
        "elapsed_sec":   output["elapsed_sec"],
        "n_clusters":    len(all_clusters),
        "n_temporal":    len(temporal_clusters),
        "n_thematic":    len(thematic_clusters),
        "boost":         output["basket_action_summary"]["boost"],
        "trim":          output["basket_action_summary"]["trim"],
        "exclude":       output["basket_action_summary"]["exclude"],
        "cluster_grades": [(c["cluster_id"], c["quality_grade"], c["recommendation"]["action"])
                            for c in all_clusters],
    }
    print(f"[clusters] done: {summary}")
    return {"statusCode": 200, "body": json.dumps(summary)}


def _write_error(message: str, **extras) -> dict:
    payload = {"schema_version": "1.0", "generated_at": datetime.now(timezone.utc).isoformat(),
                "status": "error", "error": message, **extras}
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
                        Body=json.dumps(payload, default=str, indent=2),
                        ContentType="application/json", CacheControl="max-age=300")
    except Exception: pass
    print(f"[clusters] ERROR: {message}")
    return {"statusCode": 500, "body": json.dumps({"status": "error", "error": message})}
