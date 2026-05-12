"""
justhodl-portfolio-snapshot — Roadmap #9 portfolio enrichment

═══════════════════════════════════════════════════════════════════════
SNAPSHOTS YOUR ENTIRE PORTFOLIO + WATCHLIST EVERY HOUR
─────────────────────────────────────────────────────
Reads positions/watchlist from DDB, joins with the entire JustHodl
intelligence stack (alpha-score, confluence, regime-picks, sentiment),
fetches latest Polygon prices, computes P&L, writes a unified sidecar.

Pipeline:
  1. Scan DDB for POSITION + WATCHLIST items
  2. Auto-sync watchlist: pull current TIER S/A stocks from alpha-score
     - Replace AUTO_TIER_S / AUTO_TIER_A entries with current top picks
     - Leave MANUAL watchlist entries untouched
  3. For each symbol (deduplicated):
       - Fetch latest Polygon price (per-symbol parallel)
       - Join with alpha-score row (alpha, tier, components, signals, flags)
       - Join with confluence row (confluence_tier, components_firing)
       - Join with regime row (regime_adj, regime_adj_score)
  4. For POSITIONS: compute P&L (qty × (current - cost) and %)
  5. Write portfolio/snapshot.json

Schedule: every 30 min during market hours · every hour off-hours
Cost: ~$0 (Polygon free tier, no Claude calls)
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
from decimal import Decimal

import boto3

S3_BUCKET = "justhodl-dashboard-live"
SNAPSHOT_KEY = "portfolio/snapshot.json"
ALPHA_KEY = "screener/alpha-score.json"
CONFLUENCE_KEY = "signals/confluence.json"
REGIME_KEY = "signals/regime-picks.json"
SENTIMENT_KEY = "sentiment/data.json"

TABLE_NAME = "justhodl-portfolio"
POLY_KEY = os.environ.get("POLY_KEY", "")

# How many TIER S + TIER A stocks to auto-sync into watchlist
AUTO_WATCH_TIER_S_LIMIT = 10
AUTO_WATCH_TIER_A_LIMIT = 15

s3 = boto3.client("s3", region_name="us-east-1")
ddb_res = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb_res.Table(TABLE_NAME)


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _scrub_decimals(obj):
    """Recursively convert Decimal → float for JSON output."""
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, dict): return {k: _scrub_decimals(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_scrub_decimals(x) for x in obj]
    return obj


def load_s3_json(key, default=None):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"  [s3:{key}] {str(e)[:120]}")
        return default


def query_pk(pk):
    """Query DDB for all items with given pk."""
    items = []
    last_key = None
    while True:
        kwargs = {"KeyConditionExpression": "pk = :pk",
                    "ExpressionAttributeValues": {":pk": pk}}
        if last_key: kwargs["ExclusiveStartKey"] = last_key
        resp = table.query(**kwargs)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key: break
    return [_scrub_decimals(i) for i in items]


def fetch_polygon_latest(symbol):
    """Get latest daily close from Polygon. Returns dict or None."""
    if not POLY_KEY: return None
    # Use the previous-close endpoint as fallback if last open is in the future
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLY_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-PS/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        results = data.get("results") or []
        if results:
            row = results[0]
            return {
                "price": row.get("c"), "open": row.get("o"),
                "high": row.get("h"), "low": row.get("l"),
                "volume": row.get("v"),
                "as_of_unix_ms": row.get("t"),
            }
    except Exception as e:
        print(f"  [poly:{symbol}] {str(e)[:80]}")
    return None


def batch_fetch_prices(symbols, max_workers=10):
    """Parallel price fetch."""
    if not symbols: return {}
    out = {}
    with ThreadPoolExecutor(max_workers=max_workers) as ex:
        futures = {ex.submit(fetch_polygon_latest, s): s for s in symbols}
        for f in as_completed(futures):
            sym = futures[f]
            try: out[sym] = f.result()
            except Exception: out[sym] = None
    return out


# ═══════════════════════════════════════════════════════════════════════
# AUTO-WATCHLIST SYNC
# ═══════════════════════════════════════════════════════════════════════

def sync_auto_watchlist(alpha_data):
    """Update WATCHLIST entries based on current alpha-score TIER S/A.
    Strategy:
      1. Query existing AUTO_* watchlist entries
      2. Compute new desired set from alpha-score TIER S/A
      3. Add missing, remove stale, leave MANUAL untouched
    Returns dict of changes."""
    desired_s = set()
    desired_a = set()
    for s in (alpha_data.get("stocks") or []):
        if s.get("tier") == "S" and len(desired_s) < AUTO_WATCH_TIER_S_LIMIT:
            desired_s.add(s["symbol"])
        elif s.get("tier") == "A" and len(desired_a) < AUTO_WATCH_TIER_A_LIMIT:
            desired_a.add(s["symbol"])

    existing = query_pk("WATCHLIST")
    existing_auto_s = {i["symbol"] for i in existing if i.get("source") == "AUTO_TIER_S"}
    existing_auto_a = {i["symbol"] for i in existing if i.get("source") == "AUTO_TIER_A"}
    existing_manual = {i["symbol"] for i in existing if i.get("source") == "MANUAL"}

    changes = {"added_S": [], "added_A": [], "removed_S": [], "removed_A": []}

    # Remove stale AUTO entries
    with table.batch_writer() as batch:
        for sym in existing_auto_s - desired_s:
            if sym in existing_manual: continue  # don't kill manual
            batch.delete_item(Key={"pk": "WATCHLIST", "sk": sym})
            changes["removed_S"].append(sym)
        for sym in existing_auto_a - desired_a:
            if sym in existing_manual: continue
            batch.delete_item(Key={"pk": "WATCHLIST", "sk": sym})
            changes["removed_A"].append(sym)

    # Add new AUTO entries (only if not already manual)
    now_iso = datetime.now(timezone.utc).isoformat()
    with table.batch_writer() as batch:
        for sym in desired_s - existing_auto_s - existing_manual:
            batch.put_item(Item={
                "pk": "WATCHLIST", "sk": sym, "symbol": sym,
                "source": "AUTO_TIER_S", "added_at": now_iso,
            })
            changes["added_S"].append(sym)
        for sym in desired_a - existing_auto_a - existing_manual:
            batch.put_item(Item={
                "pk": "WATCHLIST", "sk": sym, "symbol": sym,
                "source": "AUTO_TIER_A", "added_at": now_iso,
            })
            changes["added_A"].append(sym)

    return changes


# ═══════════════════════════════════════════════════════════════════════
# ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════

def index_by_symbol(rows, sym_key="symbol"):
    return {r[sym_key]: r for r in rows if sym_key in r}


def enrich_symbol(sym, price_data, alpha_idx, confluence_s_idx, confluence_a_idx,
                    confluence_b_idx, regime_picks_idx, sentiment_idx):
    """Build a unified enriched record for one symbol."""
    rec = {"symbol": sym}
    p = price_data.get(sym) or {}
    rec["current_price"] = p.get("price")
    rec["price_open"] = p.get("open")
    rec["price_high"] = p.get("high")
    rec["price_low"] = p.get("low")
    rec["volume"] = p.get("volume")

    # Alpha-score
    alpha_row = alpha_idx.get(sym)
    if alpha_row:
        rec["alpha_score"] = alpha_row.get("alpha_score")
        rec["tier"] = alpha_row.get("tier")
        rec["rank"] = alpha_row.get("rank")
        rec["name"] = alpha_row.get("name")
        rec["sector"] = alpha_row.get("sector")
        rec["components"] = alpha_row.get("components")
        rec["top_signals"] = (alpha_row.get("top_signals") or [])[:3]
        rec["risk_flags"] = (alpha_row.get("risk_flags") or [])[:3]

    # Confluence (which tier of confluence?)
    if sym in confluence_s_idx:
        rec["confluence_tier"] = "S"
        rec["confluence_count"] = confluence_s_idx[sym].get("confluence_count")
        rec["components_firing"] = confluence_s_idx[sym].get("components_firing")
    elif sym in confluence_a_idx:
        rec["confluence_tier"] = "A"
        rec["confluence_count"] = confluence_a_idx[sym].get("confluence_count")
    elif sym in confluence_b_idx:
        rec["confluence_tier"] = "B"
        rec["confluence_count"] = confluence_b_idx[sym].get("confluence_count")
    else:
        rec["confluence_tier"] = None

    # Regime fit
    regime_row = regime_picks_idx.get(sym)
    if regime_row:
        rec["regime_adj"] = regime_row.get("regime_adj")
        rec["regime_adj_score"] = regime_row.get("regime_adj_score")

    # News sentiment
    sent_row = sentiment_idx.get(sym)
    if sent_row:
        rec["sentiment_signal"] = sent_row.get("sentimentSignal")
        rec["sentiment_score"] = sent_row.get("sentimentScore")
        rec["sentiment_reason"] = (sent_row.get("sentimentReason") or "")[:140]

    return rec


# ═══════════════════════════════════════════════════════════════════════
# MAIN
# ═══════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== PORTFOLIO SNAPSHOT · {datetime.now(timezone.utc).isoformat()} ===")

    # 1. Load all sidecars
    alpha = load_s3_json(ALPHA_KEY, {})
    confluence = load_s3_json(CONFLUENCE_KEY, {})
    regime = load_s3_json(REGIME_KEY, {})
    sentiment_data = load_s3_json(SENTIMENT_KEY, {})

    alpha_idx = index_by_symbol(alpha.get("stocks") or [])
    confluence_s_idx = index_by_symbol(confluence.get("tier_s_confluence") or [])
    confluence_a_idx = index_by_symbol(confluence.get("tier_a_confluence") or [])
    confluence_b_idx = index_by_symbol(confluence.get("tier_b_confluence") or [])
    regime_picks_idx = index_by_symbol(regime.get("regime_picks") or [])
    sentiment_idx = index_by_symbol(sentiment_data.get("sentiment") or [])

    print(f"  loaded: alpha={len(alpha_idx)} conf_S={len(confluence_s_idx)} "
          f"conf_A={len(confluence_a_idx)} regime={len(regime_picks_idx)} "
          f"sentiment={len(sentiment_idx)}")

    # 2. Auto-sync watchlist
    sync_changes = sync_auto_watchlist(alpha)
    print(f"  watchlist sync: +{len(sync_changes['added_S'])} S "
          f"+{len(sync_changes['added_A'])} A "
          f"-{len(sync_changes['removed_S'])+len(sync_changes['removed_A'])} stale")

    # 3. Re-query after sync
    positions = query_pk("POSITION")
    watchlist = query_pk("WATCHLIST")
    print(f"  positions={len(positions)} watchlist={len(watchlist)}")

    # 4. Build unique symbol set + fetch prices in parallel
    all_symbols = set()
    for p in positions: all_symbols.add(p["symbol"])
    for w in watchlist: all_symbols.add(w["symbol"])
    price_data = batch_fetch_prices(list(all_symbols))
    n_priced = sum(1 for v in price_data.values() if v)
    print(f"  prices fetched: {n_priced}/{len(all_symbols)}")

    # 5. Enrich each symbol
    enriched_by_sym = {}
    for sym in all_symbols:
        enriched_by_sym[sym] = enrich_symbol(
            sym, price_data, alpha_idx,
            confluence_s_idx, confluence_a_idx, confluence_b_idx,
            regime_picks_idx, sentiment_idx)

    # 6. Build POSITIONS list with P&L
    position_records = []
    total_value = 0.0
    total_cost = 0.0
    sector_value = {}
    for p in positions:
        sym = p["symbol"]
        e = enriched_by_sym.get(sym, {"symbol": sym})
        qty = float(p.get("qty") or 0)
        cost_per = float(p.get("cost_basis_per_share") or 0)
        cost_total = float(p.get("cost_basis_total") or qty * cost_per)
        cur_price = e.get("current_price") or cost_per  # fallback to cost if no price
        market_value = qty * cur_price
        pnl_dollars = market_value - cost_total
        pnl_pct = (pnl_dollars / abs(cost_total)) * 100 if cost_total else None
        stop = float(p["stop_loss"]) if p.get("stop_loss") is not None else None
        stop_distance_pct = ((cur_price - stop) / stop) * 100 if (stop and cur_price) else None
        stop_hit = (stop and cur_price and cur_price <= stop) if p.get("position_type") == "LONG" else \
                   (stop and cur_price and cur_price >= stop)

        rec = {
            **e,
            "qty": qty,
            "cost_basis_per_share": cost_per,
            "cost_basis_total": cost_total,
            "market_value": round(market_value, 2),
            "pnl_dollars": round(pnl_dollars, 2),
            "pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
            "position_type": p.get("position_type", "LONG"),
            "stop_loss": stop,
            "stop_distance_pct": round(stop_distance_pct, 2) if stop_distance_pct is not None else None,
            "stop_hit": bool(stop_hit),
            "target_weight_pct": float(p["target_weight_pct"]) if p.get("target_weight_pct") is not None else None,
            "added_at": p.get("added_at"),
            "notes": p.get("notes"),
        }
        position_records.append(rec)
        total_value += market_value
        total_cost += cost_total
        sec = e.get("sector") or p.get("sector") or "Unknown"
        sector_value[sec] = sector_value.get(sec, 0.0) + market_value

    # Compute current weights
    for rec in position_records:
        rec["current_weight_pct"] = round((rec["market_value"] / total_value) * 100, 2) if total_value else None
        # Weight drift from target
        if rec.get("target_weight_pct") is not None and rec.get("current_weight_pct") is not None:
            rec["weight_drift_pct"] = round(rec["current_weight_pct"] - rec["target_weight_pct"], 2)

    # 7. Build WATCHLIST list
    watchlist_records = []
    for w in watchlist:
        sym = w["symbol"]
        e = enriched_by_sym.get(sym, {"symbol": sym})
        watchlist_records.append({
            **e,
            "source": w.get("source", "MANUAL"),
            "added_at": w.get("added_at"),
            "notes": w.get("notes"),
        })
    # Sort watchlist: S confluence first, then by alpha
    watchlist_records.sort(key=lambda r: (
        0 if r.get("confluence_tier") == "S" else 1 if r.get("confluence_tier") == "A" else 2,
        -(r.get("alpha_score") or 0),
    ))

    # 8. Build sector concentration
    sector_concentration = []
    for sec, val in sorted(sector_value.items(), key=lambda x: -x[1]):
        sector_concentration.append({
            "sector": sec,
            "value": round(val, 2),
            "weight_pct": round((val / total_value) * 100, 2) if total_value else None,
        })

    total_pnl = total_value - total_cost
    total_pnl_pct = (total_pnl / abs(total_cost)) * 100 if total_cost else None

    # 9. Stops hit summary
    stops_hit = [{"symbol": r["symbol"], "stop_loss": r["stop_loss"],
                    "current_price": r["current_price"]}
                   for r in position_records if r["stop_hit"]]

    elapsed = time.time() - started

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "elapsed_seconds": round(elapsed, 2),

        # Portfolio summary
        "portfolio_summary": {
            "n_positions": len(position_records),
            "total_market_value": round(total_value, 2),
            "total_cost_basis": round(total_cost, 2),
            "total_pnl_dollars": round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl_pct, 2) if total_pnl_pct is not None else None,
            "stops_hit_count": len(stops_hit),
            "stops_hit": stops_hit,
        },

        # Positions + watchlist
        "positions": position_records,
        "watchlist": watchlist_records,
        "sector_concentration": sector_concentration,

        # Sync info
        "watchlist_sync": sync_changes,

        # Counts
        "counts": {
            "positions": len(position_records),
            "watchlist": len(watchlist_records),
            "auto_watch_S": sum(1 for w in watchlist if w.get("source") == "AUTO_TIER_S"),
            "auto_watch_A": sum(1 for w in watchlist if w.get("source") == "AUTO_TIER_A"),
            "manual_watch": sum(1 for w in watchlist if w.get("source") == "MANUAL"),
        },
    }

    s3.put_object(Bucket=S3_BUCKET, Key=SNAPSHOT_KEY,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=1800")

    print(f"  ✓ snapshot written · {elapsed:.2f}s")

    return {"statusCode": 200, "body": json.dumps({
        "success": True,
        "n_positions": len(position_records),
        "n_watchlist": len(watchlist_records),
        "total_market_value": round(total_value, 2),
        "total_pnl_dollars": round(total_pnl, 2),
        "stops_hit_count": len(stops_hit),
        "elapsed_seconds": round(elapsed, 2),
    })}
