"""
justhodl-screen-builder -- Programmable cross-engine screen builder.

═══════════════════════════════════════════════════════════════════════════════
INSTITUTIONAL THESIS
────────────────────
Your fixed screeners (forensic, deep-value, opportunity, stock-screener)
each enforce ONE methodology. Researchers need to compose ARBITRARY filters
across all available metrics — Greenblatt's Magic Formula, Buffett's
checklist, Burry's distress screens, Marks's downside-protected — and save
them as named screens.

This engine consolidates 60+ metrics from your existing engine outputs
into a unified per-ticker universe, then evaluates arbitrary filter
specifications against it. Bloomberg's screener costs $24k/yr. FactSet's
universal screener $20k/yr. Yours runs free, on your own data, with full
programmability.

THREE OPERATING MODES (via event payload)
──────────────────────────────────────────
  Mode 1: BUILD UNIVERSE  (default if no event, or {"action": "build_universe"})
    Reads all existing Pro Pack v3 + research engine outputs from S3
    Assembles per-ticker metric union → data/screen-universe.json
    Daily 14:00 UTC after all upstream engines settle

  Mode 2: QUERY  ({"action": "query", "screen": {...}})
    Evaluates a screen spec against the cached universe
    Returns ranked matching tickers

  Mode 3: SAVE / LOAD / LIST NAMED SCREENS
    {"action": "save_screen", "name": "Greenblatt", "screen": {...}}
      → s3://justhodl-dashboard-live/screens/Greenblatt.json
    {"action": "load_screen", "name": "Greenblatt"}
    {"action": "list_screens"}

SCREEN SPEC SCHEMA
──────────────────
  {
    "name": "Quality at Discount",        # optional
    "filters": [
      {"metric": "roic_ttm", "op": ">", "value": 0.15},
      {"metric": "ev_ebitda_ttm", "op": "<", "value": 12},
      {"metric": "stars", "op": ">=", "value": 4},
      {"metric": "sector", "op": "in",
        "value": ["Technology", "Healthcare"]}
    ],
    "sort_by": "fcf_yield",
    "sort_order": "desc",                  # desc (default) or asc
    "limit": 25                            # max 100
  }

  Supported operators:
    >, >=, <, <=, ==, !=, in, not_in, between, exists
  
  All filters AND-combined. For OR logic, build multiple screens and
  union results client-side.

AVAILABLE METRICS (across upstream engines)
────────────────────────────────────────────
  Identity: ticker, company_name, sector, industry, market_cap_usd
  Predictability (Pro Pack v3 #7): stars, rev_r2, eps_r2, valuation
  EVA Spread (#10): eva_spread_pct, roic_ttm_pct, super_compounder
  Smart Beta (#8): quality_pct, value_pct, momentum_pct, low_vol_pct
  Beneish (#6): m_score, flag (clean/elevated)
  Peer Comp (R3): each metric + z_score + classification
  Fundamentals: dcf_gap_pct, altman_z, piotroski_f
  Insider/Activist: insider_cluster_score, has_activist
  Earnings: pead_tier, streak, beat_acceleration

SCHEDULE
────────
  Universe-build: daily 14:00 UTC (after engines refresh)
  Query: on-demand via function URL

ACADEMIC BASIS
──────────────
- Greenblatt (2010). The Little Book That Still Beats the Market.
- Piotroski (2000). Value investing: The use of historical financial
  statement information.
- Asness (2014). Quality investing: Industry insights.
═══════════════════════════════════════════════════════════════════════════════
"""
import json
import os
import statistics
import time
from datetime import datetime, timezone

import boto3

VERSION = "1.0.0"
S3_BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
S3_UNIVERSE_KEY = "data/screen-universe.json"
S3_SCREENS_PREFIX = "screens/"

s3 = boto3.client("s3", region_name="us-east-1")

# Upstream engine outputs to ingest
UPSTREAM_FEEDS = [
    ("predictability", "data/predictability.json"),
    ("eva", "data/eva-spread.json"),
    ("smart_beta", "data/smart-beta.json"),
    ("beneish", "data/beneish-m-score.json"),
    ("peer_comparison", "data/peer-comparison.json"),
    ("fundamentals", "data/fundamentals.json"),
    ("insider_clusters", "data/insider-clusters.json"),
    ("activist_13d", "data/activist-13d.json"),
    ("pead_signals", "data/pead-signals.json"),
    ("starmine", "data/starmine.json"),
    ("master_ranker", "data/master-ranker.json"),
]


def fetch_s3_json(key):
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except Exception as e:
        print(f"[fetch] {key} miss: {str(e)[:60]}")
        return None


def safe_get(d, *keys, default=None):
    cur = d
    for k in keys:
        if not isinstance(cur, dict):
            return default
        cur = cur.get(k)
        if cur is None:
            return default
    return cur


# ---------- Universe builder ----------
def merge_into_universe(universe, ticker, fields):
    sym = (ticker or "").upper()
    if not sym:
        return
    entry = universe.setdefault(sym, {"ticker": sym})
    for k, v in fields.items():
        if v is not None and (k not in entry or entry[k] is None):
            entry[k] = v


def ingest_predictability(universe, data):
    if not isinstance(data, dict):
        return
    for src in [data.get("elite_moats") or [],
                 data.get("most_predictable_top_15") or [],
                 data.get("all_tickers") or []]:
        for r in src:
            if not isinstance(r, dict):
                continue
            merge_into_universe(universe, r.get("ticker"), {
                "predictability_stars": r.get("stars"),
                "rev_r2": r.get("rev_r2"),
                "eps_r2": r.get("eps_r2"),
                "predictability_valuation": r.get("valuation"),
                "pe_ttm_pred": r.get("pe_ttm"),
            })


def ingest_eva(universe, data):
    if not isinstance(data, dict):
        return
    for src in [data.get("super_compounders") or [],
                 data.get("top_10_eva_spread") or [],
                 data.get("all_tickers") or []]:
        for r in src:
            if not isinstance(r, dict):
                continue
            merge_into_universe(universe, r.get("ticker"), {
                "eva_spread_pct": r.get("eva_spread_pct"),
                "eva_spread_pctile": r.get("eva_spread_pct_pctile"),
                "roic_ttm_pct": r.get("roic_ttm_pct"),
                "wacc_pct": r.get("wacc_pct"),
                "super_compounder": bool(r.get("super_compounder")),
            })


def ingest_smart_beta(universe, data):
    if not isinstance(data, dict):
        return
    leaders = data.get("factor_leaders") or {}
    for factor, names in leaders.items():
        for r in (names or []):
            if not isinstance(r, dict):
                continue
            merge_into_universe(universe, r.get("ticker"), {
                f"{factor}_pct": (r.get(f"{factor}_pct")
                                    or r.get(f"{factor}_pctile")),
                "composite_smart_beta": r.get("composite"),
            })
    for r in data.get("top_25_diversified") or []:
        if not isinstance(r, dict):
            continue
        merge_into_universe(universe, r.get("ticker"), {
            "quality_pct": r.get("quality_pct"),
            "value_pct": r.get("value_pct"),
            "momentum_pct": r.get("momentum_pct"),
            "low_vol_pct": r.get("low_vol_pct"),
        })


def ingest_beneish(universe, data):
    if not isinstance(data, dict):
        return
    for src in [data.get("clean_quality") or [],
                 data.get("all_tickers") or [],
                 data.get("low_risk") or [],
                 data.get("elevated_risk") or []]:
        for r in src:
            if not isinstance(r, dict):
                continue
            merge_into_universe(universe, r.get("ticker"), {
                "beneish_m_score": r.get("m_score") or r.get(
                    "beneish_m_score"),
                "beneish_flag": r.get("flag"),
            })


def ingest_peer_comparison(universe, data):
    if not isinstance(data, dict):
        return
    for r in data.get("results") or []:
        if not isinstance(r, dict):
            continue
        tm = r.get("target_metrics") or {}
        merge_into_universe(universe, r.get("ticker"), {
            "ev_ebitda_ttm": tm.get("ev_ebitda_ttm"),
            "pe_ttm": tm.get("pe_ttm"),
            "pb": tm.get("pb"),
            "fcf_yield": tm.get("fcf_yield"),
            "gross_margin": tm.get("gross_margin"),
            "ebitda_margin": tm.get("ebitda_margin"),
            "net_debt_to_ebitda": tm.get("net_debt_to_ebitda"),
            "revenue_cagr_3y": tm.get("revenue_cagr_3y"),
            "eps_cagr_3y": tm.get("eps_cagr_3y"),
            "fcf_cagr_3y": tm.get("fcf_cagr_3y"),
            "beta": tm.get("beta"),
            "sector": r.get("sector"),
            "industry": r.get("industry"),
            "market_cap_usd": r.get("market_cap_usd"),
            "company_name": r.get("company_name"),
        })


def ingest_fundamentals(universe, data):
    if not isinstance(data, dict):
        return
    for r in (data.get("all_tickers") or data.get("results") or []):
        if not isinstance(r, dict):
            continue
        merge_into_universe(universe, r.get("ticker"), {
            "dcf_gap_pct": r.get("dcf_gap_pct"),
            "altman_z": r.get("altman_z") or r.get("altman_z_score"),
            "piotroski_f": r.get("piotroski_f") or r.get("piotroski_score"),
        })


def ingest_insider_clusters(universe, data):
    if not isinstance(data, dict):
        return
    for c in (data.get("clusters") or []):
        if not isinstance(c, dict):
            continue
        merge_into_universe(universe, c.get("ticker"), {
            "insider_cluster_score": c.get("score"),
            "insider_n_buyers": c.get("n_insiders"),
            "insider_cluster_value_usd": c.get("total_value"),
            "insider_has_ceo": bool(c.get("has_ceo")),
        })


def ingest_activist(universe, data):
    if not isinstance(data, dict):
        return
    for s in (data.get("all_setups") or data.get("top_setups") or []):
        if not isinstance(s, dict):
            continue
        merge_into_universe(universe, s.get("target_ticker"), {
            "has_activist": True,
            "activist_name": s.get("activist_name"),
            "activist_tier": s.get("tier"),
        })


def ingest_pead(universe, data):
    if not isinstance(data, dict):
        return
    for r in (data.get("all_qualifying") or []):
        if not isinstance(r, dict):
            continue
        m = r.get("metrics") or {}
        merge_into_universe(universe, r.get("symbol"), {
            "pead_tier": r.get("tier"),
            "pead_streak": m.get("streak"),
            "pead_avg_beat_pct": m.get("avg_beat_pct"),
            "pead_acceleration": m.get("beat_acceleration"),
            "pead_drift_pct": m.get("post_earnings_drift_pct"),
        })


def ingest_master_ranker(universe, data):
    if not isinstance(data, dict):
        return
    for r in (data.get("rankings") or data.get("all_ranked")
                or data.get("results") or []):
        if not isinstance(r, dict):
            continue
        merge_into_universe(universe, r.get("ticker"), {
            "master_rank": r.get("rank"),
            "master_composite_score": r.get("composite_score") or r.get(
                "score"),
        })


INGESTORS = {
    "predictability": ingest_predictability,
    "eva": ingest_eva,
    "smart_beta": ingest_smart_beta,
    "beneish": ingest_beneish,
    "peer_comparison": ingest_peer_comparison,
    "fundamentals": ingest_fundamentals,
    "insider_clusters": ingest_insider_clusters,
    "activist_13d": ingest_activist,
    "pead_signals": ingest_pead,
    "master_ranker": ingest_master_ranker,
}


def build_universe():
    universe = {}
    feed_stats = {}
    for name, key in UPSTREAM_FEEDS:
        data = fetch_s3_json(key)
        feed_stats[name] = data is not None
        ingestor = INGESTORS.get(name)
        if ingestor and data:
            try:
                ingestor(universe, data)
            except Exception as e:
                print(f"[ingest {name}] err: {str(e)[:120]}")

    # Compute simple universe stats
    tickers = list(universe.values())
    n_tickers = len(tickers)
    metric_coverage = {}
    if tickers:
        all_keys = set()
        for t in tickers:
            all_keys.update(t.keys())
        for k in all_keys:
            n_with = sum(1 for t in tickers if t.get(k) is not None)
            if n_with > 5:
                metric_coverage[k] = n_with

    return {
        "version": VERSION,
        "built_at": datetime.now(timezone.utc).isoformat(),
        "n_tickers": n_tickers,
        "feeds_available": feed_stats,
        "metric_coverage": metric_coverage,
        "universe": list(universe.values()),
    }


# ---------- Query engine ----------
def evaluate_filter(value, op, target):
    """Evaluate a single filter clause. None values fail filter."""
    if value is None:
        return False
    try:
        if op == ">":
            return float(value) > float(target)
        if op == ">=":
            return float(value) >= float(target)
        if op == "<":
            return float(value) < float(target)
        if op == "<=":
            return float(value) <= float(target)
        if op == "==":
            if isinstance(target, str):
                return str(value).lower() == target.lower()
            return value == target
        if op == "!=":
            if isinstance(target, str):
                return str(value).lower() != target.lower()
            return value != target
        if op == "in":
            if not isinstance(target, list):
                return False
            return str(value).lower() in [str(t).lower() for t in target]
        if op == "not_in":
            if not isinstance(target, list):
                return False
            return str(value).lower() not in [str(t).lower() for t in target]
        if op == "between":
            if not isinstance(target, list) or len(target) != 2:
                return False
            return float(target[0]) <= float(value) <= float(target[1])
        if op == "exists":
            return value is not None
        return False
    except (ValueError, TypeError):
        return False


def run_query(screen, universe_data):
    """Apply filters + sort + limit."""
    universe = universe_data.get("universe", [])
    filters = screen.get("filters") or []
    sort_by = screen.get("sort_by")
    sort_order = (screen.get("sort_order") or "desc").lower()
    limit = min(int(screen.get("limit") or 25), 100)

    # Apply filters
    matched = []
    for t in universe:
        passes = True
        for f in filters:
            metric = f.get("metric")
            op = f.get("op")
            value = f.get("value")
            if metric is None or op is None:
                passes = False
                break
            if not evaluate_filter(t.get(metric), op, value):
                passes = False
                break
        if passes:
            matched.append(t)

    # Sort
    if sort_by:
        def sort_key(t):
            v = t.get(sort_by)
            if v is None:
                return float("-inf") if sort_order == "desc" else float("inf")
            try:
                return float(v)
            except (ValueError, TypeError):
                return str(v)
        matched.sort(key=sort_key, reverse=(sort_order == "desc"))

    return {
        "n_matched": len(matched),
        "results": matched[:limit],
        "limit": limit,
        "screen_name": screen.get("name"),
        "filters_applied": filters,
        "sort_by": sort_by,
        "sort_order": sort_order,
    }


# ---------- Named screen storage ----------
def save_screen(name, screen):
    key = f"{S3_SCREENS_PREFIX}{name}.json"
    payload = {
        "name": name,
        "saved_at": datetime.now(timezone.utc).isoformat(),
        "screen": screen,
    }
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                    Body=json.dumps(payload).encode("utf-8"),
                    ContentType="application/json")
    return key


def load_screen(name):
    key = f"{S3_SCREENS_PREFIX}{name}.json"
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=key)
        return json.loads(obj["Body"].read().decode("utf-8"))
    except s3.exceptions.NoSuchKey:
        return None


def list_screens():
    out = []
    try:
        resp = s3.list_objects_v2(Bucket=S3_BUCKET,
                                    Prefix=S3_SCREENS_PREFIX, MaxKeys=200)
        for obj in resp.get("Contents", []):
            name = obj["Key"].replace(S3_SCREENS_PREFIX, "").replace(
                ".json", "")
            out.append({
                "name": name, "last_modified": str(obj["LastModified"]),
                "size_bytes": obj["Size"],
            })
    except Exception as e:
        print(f"[list_screens] err: {e}")
    return out


# ---------- Main ----------
def extract_event(event):
    if not isinstance(event, dict):
        return {}
    body = event.get("body")
    if isinstance(body, str):
        try:
            return json.loads(body)
        except json.JSONDecodeError:
            return {}
    return event


def lambda_handler(event=None, context=None):
    started = time.time()
    e = extract_event(event or {})
    action = (e.get("action") or "build_universe").lower()
    print(f"[screen-builder] action={action}")

    if action == "build_universe":
        u = build_universe()
        body = json.dumps(u, default=str).encode("utf-8")
        s3.put_object(Bucket=S3_BUCKET, Key=S3_UNIVERSE_KEY,
                        Body=body, ContentType="application/json",
                        CacheControl="public, max-age=3600")
        print(f"[screen-builder] universe built: "
              f"{u['n_tickers']} tickers, {len(body):,} bytes")
        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "action": action,
            "n_tickers": u["n_tickers"],
            "feeds_available": u["feeds_available"],
            "metric_coverage_n": len(u["metric_coverage"]),
            "size_bytes": len(body),
        })}

    if action == "query":
        screen = e.get("screen")
        if not isinstance(screen, dict):
            return {"statusCode": 400, "body": json.dumps({
                "ok": False, "error": "missing 'screen' object"})}
        u = fetch_s3_json(S3_UNIVERSE_KEY)
        if not u:
            return {"statusCode": 500, "body": json.dumps({
                "ok": False,
                "error": "universe not yet built — run action=build_universe"})}
        result = run_query(screen, u)
        return {"statusCode": 200,
                "headers": {"Access-Control-Allow-Origin": "*"},
                "body": json.dumps({
                    "ok": True, "version": VERSION,
                    "action": action,
                    "universe_built_at": u.get("built_at"),
                    "universe_size": u.get("n_tickers"),
                    "result": result,
                })}

    if action == "save_screen":
        name = e.get("name")
        screen = e.get("screen")
        if not name or not isinstance(screen, dict):
            return {"statusCode": 400, "body": json.dumps({
                "ok": False,
                "error": "need 'name' and 'screen' object"})}
        # Sanitize name to filesystem-safe chars
        import re
        safe_name = re.sub(r"[^a-zA-Z0-9_-]", "_", name)[:60]
        key = save_screen(safe_name, screen)
        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "action": action,
            "saved_name": safe_name, "s3_key": key})}

    if action == "load_screen":
        name = e.get("name")
        if not name:
            return {"statusCode": 400, "body": json.dumps({
                "ok": False, "error": "need 'name'"})}
        loaded = load_screen(name)
        if not loaded:
            return {"statusCode": 404, "body": json.dumps({
                "ok": False, "error": f"screen '{name}' not found"})}
        # If "run" flag, immediately execute the loaded screen
        if e.get("run"):
            u = fetch_s3_json(S3_UNIVERSE_KEY)
            if u:
                result = run_query(loaded.get("screen", {}), u)
                return {"statusCode": 200, "body": json.dumps({
                    "ok": True, "loaded_screen": loaded,
                    "result": result})}
        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "loaded_screen": loaded})}

    if action == "list_screens":
        screens = list_screens()
        return {"statusCode": 200, "body": json.dumps({
            "ok": True, "action": action,
            "n_screens": len(screens), "screens": screens})}

    return {"statusCode": 400, "body": json.dumps({
        "ok": False,
        "error": (f"unknown action '{action}'. valid: "
                    "build_universe, query, save_screen, load_screen, "
                    "list_screens")})}


if __name__ == "__main__":
    print(json.dumps(lambda_handler({"action": "build_universe"}), indent=2))
