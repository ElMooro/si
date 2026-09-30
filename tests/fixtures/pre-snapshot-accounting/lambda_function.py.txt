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
from private_artifact import publish_private, private_http_denied
import math
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
ddb_client = boto3.client("dynamodb", region_name="us-east-1")
ddb_res = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb_res.Table(TABLE_NAME)


import hashlib
import json
import math
import re
import uuid
from datetime import datetime, timezone

SOURCE_KEY = "screener/alpha-score.json"
META_KEY = {"pk": "SYNC_META", "sk": "AUTO_WATCHLIST_V1"}
MAX_ALPHA_AGE_H = 3
MAX_SCREEN_AGE_H = 120
LIMITS = {"S": AUTO_WATCH_TIER_S_LIMIT, "A": AUTO_WATCH_TIER_A_LIMIT}
AUTO = {"AUTO_TIER_S", "AUTO_TIER_A"}


def stamp(value):
    if not isinstance(value, str) or len(value) > 64:
        raise ValueError("SOURCE_CLOCK_INVALID")
    try:
        out = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if out.tzinfo is None or out.utcoffset() is None:
            raise ValueError()
        return out.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise ValueError("SOURCE_CLOCK_INVALID") from None


def source_frame(data, now):
    if not isinstance(data, dict):
        raise ValueError("SOURCE_NOT_OBJECT")
    rows = data.get("stocks")
    if not isinstance(rows, list) or not rows or len(rows) > 20000:
        raise ValueError("SOURCE_UNIVERSE_UNAVAILABLE")
    if type(data.get("count")) is not int or data["count"] != len(rows):
        raise ValueError("SOURCE_COUNT_MISMATCH")
    if not isinstance(data.get("model_version"), str) or not data["model_version"].strip():
        raise ValueError("SOURCE_VERSION_UNAVAILABLE")
    generated = stamp(data.get("generated_at"))
    inputs = data.get("inputs")
    screen = stamp(inputs.get("screener_generated_at") if isinstance(inputs, dict) else None)
    for clock, maximum in ((generated, MAX_ALPHA_AGE_H), (screen, MAX_SCREEN_AGE_H)):
        if not -300 <= (now - clock).total_seconds() <= maximum * 3600:
            raise ValueError("SOURCE_CLOCK_OUTSIDE_POLICY")
    if (screen - generated).total_seconds() > 300:
        raise ValueError("SOURCE_CLOCK_ORDER_INVALID")
    if "quality" in data:
        quality = data["quality"]
        if not isinstance(quality, dict) or quality.get("status") != "fresh":
            raise ValueError("SOURCE_EXPLICIT_QUALITY_INELIGIBLE")
    desired, seen, counts, scored = {}, set(), {t: 0 for t in "SABCD"}, 0
    previous_score = math.inf
    unscored_started = False
    for row in rows:
        if not isinstance(row, dict):
            raise ValueError("SOURCE_ROW_INVALID")
        symbol, score, tier = row.get("symbol"), row.get("alpha_score"), row.get("tier")
        if not isinstance(symbol, str) or not re.fullmatch(r"[A-Z0-9][A-Z0-9.\-]{0,31}", symbol) or symbol in seen:
            raise ValueError("SOURCE_IDENTITY_INVALID_OR_DUPLICATE")
        seen.add(symbol)
        if score is None:
            if tier != "—" or row.get("rank") is not None:
                raise ValueError("SOURCE_UNSCORED_ROW_INVALID")
            unscored_started = True
            continue
        if type(score) not in (int, float) or not 0 <= score <= 100 or not math.isfinite(score):
            raise ValueError("SOURCE_SCORE_INVALID")
        expected = "S" if score >= 90 else "A" if score >= 80 else "B" if score >= 70 else "C" if score >= 50 else "D"
        scored += 1
        if tier != expected or type(row.get("rank")) is not int or row["rank"] != scored or score > previous_score or unscored_started:
            raise ValueError("SOURCE_RANK_OR_TIER_INCONSISTENT")
        previous_score = score
        counts[tier] += 1
        if tier in LIMITS and counts[tier] <= LIMITS[tier]:
            desired[symbol] = "AUTO_TIER_" + tier
    if type(data.get("scored_count")) is not int or data["scored_count"] != scored or not scored:
        raise ValueError("SOURCE_SCORED_COUNT_INVALID")
    distribution = data.get("tier_distribution")
    if not isinstance(distribution, dict) or set(distribution) != set(counts) or any(type(distribution[t]) is not int or distribution[t] != counts[t] for t in counts):
        raise ValueError("SOURCE_TIER_COUNTS_MISMATCH")
    try:
        raw = json.dumps(data, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode("utf-8")
    except (TypeError, ValueError, UnicodeError):
        raise ValueError("SOURCE_NOT_FINITE_JSON") from None
    return desired, {"source_key": SOURCE_KEY, "source_generated_at": generated.isoformat(),
                     "screener_generated_at": screen.isoformat(), "source_sha256": hashlib.sha256(raw).hexdigest(),
                     "source_count": len(rows), "source_model_version": data["model_version"],
                     "age_policy_hours": {"alpha": MAX_ALPHA_AGE_H, "screener": MAX_SCREEN_AGE_H},
                     "investment_qualified": False}


def observed_condition(item):
    """Compare all observed fields and absence of standard owner-edit fields."""
    names, values, terms = {}, {}, []
    fields = set(item) | {"source", "symbol", "added_at", "notes", "source_generated_at", "sync_version"}
    for index, field in enumerate(sorted(fields)):
        alias, placeholder = "#f" + str(index), ":v" + str(index)
        names[alias] = field
        if field in item:
            values[placeholder] = item[field]
            terms.append(alias + " = " + placeholder)
        else:
            terms.append("attribute_not_exists(" + alias + ")")
    return {"ConditionExpression": " AND ".join(terms), "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}


def plan(existing, meta, desired, provenance, now, table_name, version):
    if meta is not None:
        if not isinstance(meta, dict) or meta.get("pk") != META_KEY["pk"] or meta.get("sk") != META_KEY["sk"] or not isinstance(meta.get("version"), str) or not re.fullmatch(r"[0-9a-f-]{36}", meta["version"]) or not isinstance(meta.get("source_sha256"), str) or not re.fullmatch(r"[0-9a-f]{64}", meta["source_sha256"]):
            raise ValueError("SYNC_CHECKPOINT_INVALID")
        prior, incoming = stamp(meta.get("source_generated_at")), stamp(provenance["source_generated_at"])
        if incoming < prior:
            raise ValueError("SOURCE_REVISION_OLDER_THAN_CHECKPOINT")
        if incoming == prior:
            if meta["source_sha256"] != provenance["source_sha256"]:
                raise ValueError("SOURCE_REVISION_CONTENT_CONFLICT")
            return [], {"status": "UNCHANGED", "reason_codes": [], "added_S": [], "added_A": [], "removed_S": [], "removed_A": [], "provenance": provenance}
    indexed = {}
    for row in existing:
        if not isinstance(row, dict) or row.get("pk") != "WATCHLIST" or not isinstance(row.get("symbol"), str) or row.get("sk") != row["symbol"] or row["symbol"] in indexed:
            raise ValueError("EXISTING_WATCHLIST_IDENTITY_INVALID")
        indexed[row["symbol"]] = row
    changes = {"added_S": [], "added_A": [], "removed_S": [], "removed_A": []}
    operations = []
    for symbol in sorted(set(indexed) | set(desired)):
        old, new_source = indexed.get(symbol), desired.get(symbol)
        if old is not None and old.get("source") not in AUTO:
            continue  # Manual and unrecognized owners are both protected.
        key = {"pk": "WATCHLIST", "sk": symbol}
        if old is not None and new_source is None:
            operations.append({"Delete": {"TableName": table_name, "Key": key, **observed_condition(old)}})
            changes["removed_" + old["source"][-1]].append(symbol)
        elif old is None and new_source is not None:
            operations.append({"Put": {"TableName": table_name, "Item": {**key, "symbol": symbol, "source": new_source, "added_at": now.isoformat(), "source_generated_at": provenance["source_generated_at"], "sync_version": version}, "ConditionExpression": "attribute_not_exists(pk)"}})
            changes["added_" + new_source[-1]].append(symbol)
        elif old is not None:
            condition = observed_condition(old)
            condition["ExpressionAttributeNames"].update({"#new_source": "source", "#new_clock": "source_generated_at", "#new_version": "sync_version"})
            condition["ExpressionAttributeValues"].update({":new_source": new_source, ":new_clock": provenance["source_generated_at"], ":new_version": version})
            operations.append({"Update": {"TableName": table_name, "Key": key, "UpdateExpression": "SET #new_source = :new_source, #new_clock = :new_clock, #new_version = :new_version", **condition}})
            if old["source"] != new_source:
                changes["removed_" + old["source"][-1]].append(symbol)
                changes["added_" + new_source[-1]].append(symbol)
    checkpoint = {**(meta or {}), **META_KEY, "version": version, "source_generated_at": provenance["source_generated_at"], "source_sha256": provenance["source_sha256"], "updated_at": now.isoformat()}
    guard = {"ConditionExpression": "attribute_not_exists(pk)"} if meta is None else {"ConditionExpression": "#v = :v AND #c = :c AND #h = :h", "ExpressionAttributeNames": {"#v": "version", "#c": "source_generated_at", "#h": "source_sha256"}, "ExpressionAttributeValues": {":v": meta["version"], ":c": meta["source_generated_at"], ":h": meta["source_sha256"]}}
    operations.insert(0, {"Put": {"TableName": table_name, "Item": checkpoint, **guard}})
    if len(operations) > 100:
        raise ValueError("SYNC_TRANSACTION_TOO_LARGE")
    return operations, {**changes, "status": "APPLIED", "reason_codes": [], "provenance": provenance, "sync_version": version, "transaction_operations": len(operations)}


def sync_watchlist(table, client, data, now=None):
    now = now or datetime.now(timezone.utc)
    empty = {"added_S": [], "added_A": [], "removed_S": [], "removed_A": []}
    try:
        desired, provenance = source_frame(data, now)
    except ValueError as error:
        return {**empty, "status": "SKIPPED_INVALID_SOURCE", "reason_codes": [str(error)], "write_attempted": False}
    try:
        response = table.get_item(Key=META_KEY, ConsistentRead=True)
        if not isinstance(response, dict):
            raise ValueError("SYNC_CHECKPOINT_READ_INVALID")
        meta = response.get("Item")
        existing, last, seen_pages = [], None, set()
        while True:
            args = {"KeyConditionExpression": "pk = :pk", "ExpressionAttributeValues": {":pk": "WATCHLIST"}, "ConsistentRead": True}
            if last is not None:
                args["ExclusiveStartKey"] = last
            response = table.query(**args)
            if not isinstance(response, dict) or not isinstance(response.get("Items"), list):
                raise ValueError("WATCHLIST_READ_INCOMPLETE")
            existing.extend(response["Items"])
            if len(existing) > 20000:
                raise ValueError("WATCHLIST_READ_BOUND_EXCEEDED")
            last = response.get("LastEvaluatedKey")
            if not last:
                break
            marker = json.dumps(last, sort_keys=True, default=str)
            if marker in seen_pages:
                raise ValueError("WATCHLIST_PAGINATION_REPEATED")
            seen_pages.add(marker)
        version = str(uuid.uuid4())
        operations, result = plan(existing, meta, desired, provenance, now, table.name, version)
        if not operations:
            return {**result, "write_attempted": False}
        from boto3.dynamodb.types import TypeSerializer
        serializer = TypeSerializer()
        wire = []
        for operation in operations:
            kind, body = next(iter(operation.items()))
            out = dict(body)
            for field in ("Item", "Key", "ExpressionAttributeValues"):
                if field in out:
                    out[field] = {k: serializer.serialize(v) for k, v in out[field].items()}
            wire.append({kind: out})
        if len(json.dumps(wire, default=str).encode("utf-8")) > 3_500_000:
            raise ValueError("SYNC_TRANSACTION_BYTE_BOUND")
    except ValueError as error:
        return {**empty, "status": "SKIPPED_INVALID_STATE", "reason_codes": [str(error)], "provenance": provenance, "write_attempted": False}
    except Exception:
        return {**empty, "status": "SKIPPED_READ_OR_PREPARE_ERROR", "reason_codes": ["SYNC_READ_OR_PREPARE_FAILED"], "provenance": provenance, "write_attempted": False}
    try:
        acknowledgement = client.transact_write_items(TransactItems=wire, ClientRequestToken=version)
        metadata = acknowledgement.get("ResponseMetadata") if isinstance(acknowledgement, dict) else None
        if not isinstance(metadata, dict) or type(metadata.get("HTTPStatusCode")) is not int or metadata["HTTPStatusCode"] != 200:
            raise ValueError("Transaction acknowledgement unavailable")
    except Exception as error:
        code = getattr(error, "response", {}).get("Error", {}).get("Code")
        conflict = code in {"TransactionCanceledException", "ConditionalCheckFailedException"}
        return {**empty, "status": "CONFLICT" if conflict else "WRITE_UNCONFIRMED", "reason_codes": ["SYNC_TRANSACTION_REJECTED" if conflict else "SYNC_ACKNOWLEDGEMENT_UNAVAILABLE"], "provenance": provenance, "write_attempted": True}
    return {**result, "write_attempted": True}


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
    body = None
    try:
        response = s3.get_object(Bucket=S3_BUCKET, Key=key)
        body = response["Body"]
        expected = response.get("ContentLength")
        if type(expected) is not int or not 0 <= expected <= 32 * 1024 * 1024:
            raise ValueError("invalid source byte length")
        chunks, size, deadline = [], 0, time.monotonic() + 20
        while True:
            if time.monotonic() > deadline:
                raise ValueError("source acceptance deadline exceeded")
            chunk = body.read(min(65536, 32 * 1024 * 1024 + 1 - size))
            if not isinstance(chunk, bytes):
                raise ValueError("source stream type invalid")
            if time.monotonic() > deadline:
                raise ValueError("source acceptance deadline exceeded")
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            if size > 32 * 1024 * 1024:
                raise ValueError("source byte bound exceeded")
        if size != expected:
            raise ValueError("incomplete source body")
        def pairs(rows):
            out = {}
            for name, value in rows:
                if name in out:
                    raise ValueError("duplicate source member")
                out[name] = value
            return out
        def invalid_constant(value):
            raise ValueError("nonfinite source number")
        out = json.loads(b"".join(chunks).decode("utf-8"), object_pairs_hook=pairs, parse_constant=invalid_constant)
        # Reject numeric overflow, lone surrogates and non-object sidecars.
        json.dumps(out, allow_nan=False, ensure_ascii=False).encode("utf-8")
        if not isinstance(out, dict):
            raise ValueError("source sidecar is not an object")
        return out
    except Exception:
        print(f"  [s3:{key}] source unavailable or invalid")
        return default
    finally:
        if body is not None:
            body.close()


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
    """Preserve existing rows on invalid input; atomically protect owner edits."""
    return {"schema_version": "watchlist-sync.v1", **sync_watchlist(table, ddb_client, alpha_data)}


# ═══════════════════════════════════════════════════════════════════════
# ENRICHMENT
# ═══════════════════════════════════════════════════════════════════════

def index_by_symbol(rows, sym_key="symbol"):
    return {r[sym_key]: r for r in rows if isinstance(r, dict) and isinstance(r.get(sym_key), str)} if isinstance(rows, list) else {}


def enrich_symbol(sym, price_data, alpha_idx, confluence_s_idx, confluence_a_idx,
                    confluence_b_idx, regime_picks_idx, sentiment_idx):
    """Build a unified enriched record for one symbol."""
    rec = {"symbol": sym}
    p = price_data.get(sym) or {}
    rec["current_price"] = p.get("price")
    # audit 2026-09-08 INST-08: carry the mark's provenance with the price
    rec["price_asof_unix_ms"] = p.get("as_of_unix_ms")
    rec["price_provider"] = "polygon:prev_close" if p.get("price") is not None else None
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
    denied = private_http_denied(event)
    if denied:
        return denied
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
    validation_only = isinstance(event, dict) and event.get("mode") == "validate_only"
    sync_changes = ({"added_S":[],"added_A":[],"removed_S":[],"removed_A":[],"validation_skipped_sync":True}
                    if validation_only else sync_auto_watchlist(alpha))
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
    unpriced_cost = 0.0        # signed, for the P&L scope arithmetic
    unpriced_cost_abs = 0.0    # absolute, for display
    unpriced = []
    sector_value = {}
    STALE_MARK_H = 120.0   # a prev-close older than ~5 days is not a valuation-grade mark
    for p in positions:
        sym = p["symbol"]
        e = enriched_by_sym.get(sym, {"symbol": sym})
        qty = float(p.get("qty") or 0)
        cost_per = float(p.get("cost_basis_per_share") or 0)
        # audit 2026-09-08 INST-07/08: the basis is always qty x unit cost (a stale stored total is never trusted),
        # the side follows the SIGN of the quantity, and a missing/stale price NEVER becomes a market price.
        cost_total = qty * cost_per
        side = "LONG" if qty >= 0 else "SHORT"
        cur_price = e.get("current_price")
        try:
            cur_price = float(cur_price)
            if not math.isfinite(cur_price) or cur_price <= 0:
                cur_price = None
        except (TypeError, ValueError):
            cur_price = None
        e["current_price"] = cur_price
        mark_age_h = None
        if e.get("price_asof_unix_ms"):
            try:
                mark_age_h = round((datetime.now(timezone.utc).timestamp() - float(e["price_asof_unix_ms"]) / 1000.0) / 3600.0, 1)
            except Exception:
                mark_age_h = None
        priced = (cur_price is not None and mark_age_h is not None
                  and math.isfinite(mark_age_h) and -0.0833 <= mark_age_h <= STALE_MARK_H)
        valuation_status = ("PRICED" if priced else "UNPRICED" if cur_price is None else
                            "STALE_MARK" if mark_age_h is not None and mark_age_h > STALE_MARK_H else "INVALID_MARK")
        if priced:
            market_value = qty * cur_price
            pnl_dollars = market_value - cost_total
            pnl_pct = (pnl_dollars / abs(cost_total)) * 100 if cost_total else None
        else:
            market_value = None
            pnl_dollars = None
            pnl_pct = None
            unpriced.append(sym)
            unpriced_cost += cost_total
            unpriced_cost_abs += abs(cost_total)
        stop = float(p["stop_loss"]) if p.get("stop_loss") is not None else None
        stop_distance_pct = ((cur_price - stop) / stop) * 100 if (stop and priced) else None
        stop_hit = None
        if stop and priced:
            stop_hit = (cur_price <= stop) if side == "LONG" else (cur_price >= stop)

        rec = {
            **e,
            "qty": qty,
            "cost_basis_per_share": cost_per,
            "cost_basis_total": round(cost_total, 2),
            "market_value": round(market_value, 2) if market_value is not None else None,
            "pnl_dollars": round(pnl_dollars, 2) if pnl_dollars is not None else None,
            "pnl_pct": round(pnl_pct, 2) if pnl_pct is not None else None,
            "position_type": side,
            "valuation_status": valuation_status,
            "mark_age_h": mark_age_h,
            "stop_loss": stop,
            "stop_distance_pct": round(stop_distance_pct, 2) if stop_distance_pct is not None else None,
            "stop_hit": stop_hit,          # None = not evaluable (no valuation-grade mark), never False by default
            "target_weight_pct": float(p["target_weight_pct"]) if p.get("target_weight_pct") is not None else None,
            "added_at": p.get("added_at"),
            "notes": p.get("notes"),
        }
        position_records.append(rec)
        if market_value is not None:
            total_value += market_value
        total_cost += cost_total
        sec = e.get("sector") or p.get("sector") or "Unknown"
        if market_value is not None:
            sector_value[sec] = sector_value.get(sec, 0.0) + market_value

    # Compute current weights
    for rec in position_records:
        rec["current_weight_pct"] = round((rec["market_value"] / total_value) * 100, 2) if (total_value and rec.get("market_value") is not None) else None
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
            "source": w.get("source"),
            "auto_source_generated_at": w.get("source_generated_at"),
            "auto_sync_version": w.get("sync_version"),
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

    # P&L is computed on the PRICED sleeve only: unpriced positions are excluded from BOTH sides
    # (otherwise their cost would read as a loss against a zero mark -- audit 2026-09-08 INST-08)
    priced_cost = total_cost - unpriced_cost
    total_pnl = total_value - priced_cost
    total_pnl_pct = (total_pnl / abs(priced_cost)) * 100 if priced_cost else None

    # 9. Stops hit summary
    stops_hit = [{"symbol": r["symbol"], "stop_loss": r["stop_loss"],
                    "current_price": r["current_price"]}
                   for r in position_records if r["stop_hit"]]

    elapsed = time.time() - started

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "audit_version": "2026-09-30.1",
        "generated_at_unix": int(time.time()),
        "elapsed_seconds": round(elapsed, 2),

        # Portfolio summary
        "portfolio_summary": {
            "n_positions": len(position_records),
            "total_market_value": round(total_value, 2),
            "total_market_value_scope": "PRICED positions only" if unpriced else "all positions priced",
            "total_cost_basis": round(total_cost, 2),
            "total_pnl_dollars": round(total_pnl, 2),
            "total_pnl_pct": round(total_pnl_pct, 2) if total_pnl_pct is not None else None,
            "pnl_scope": "PRICED positions only; unpriced exposure excluded (audit 2026-09-08 INST-08)",
            "unpriced_positions": unpriced,
            "unpriced_cost_basis": round(unpriced_cost_abs, 2),
            "stops_hit_count": len(stops_hit),
            "stops_hit": stops_hit,
            "stops_not_evaluable": [r["symbol"] for r in position_records if r.get("stop_loss") is not None and r.get("stop_hit") is None],
        },

        # A marked holdings sum is not account equity: no cash/liability/order ledger exists here.
        # Consumers must not size from this incomplete book.
        "capital_book": {
            "schema_version": "1.0", "status": "BLOCKED", "allows_new_entries": False,
            "reason_codes": ["MISSING_RECONCILED_CAPITAL_LEDGER"],
            "as_of": datetime.now(timezone.utc).isoformat(), "book_id": None, "account_id": None,
            "currency": None, "equity_nav": None, "cash": None, "liabilities": None,
            "reconciled_at": None, "reserved_order_exposure": None, "nav_history": [], "open_orders": [],
            "gross_exposure": round(sum(abs(r["market_value"]) for r in position_records if r["market_value"] is not None), 2),
            "net_exposure": round(total_value, 2),
            "exposure_scope": "PRICED_POSITIONS_ONLY" if unpriced else "ALL_POSITIONS",
            "positions": position_records, "unpriced_positions": unpriced,
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

    if validation_only:
        return {"ok": True, "validation_only": True, "schema_version": "audit-accounting-1.0",
                "status": payload["capital_book"]["status"], "artifact_size_bytes": len(json.dumps(payload).encode())}
    publish_private("portfolio-snapshot", payload)
    s3.put_object(Bucket=S3_BUCKET, Key=SNAPSHOT_KEY,
        Body=json.dumps(payload, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="private, no-store")

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
