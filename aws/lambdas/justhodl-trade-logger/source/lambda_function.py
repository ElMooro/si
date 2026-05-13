"""
justhodl-trade-logger — Roadmap #16 Trade Journal (logger half)

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
Right now the system is amnesiac — it doesn't remember its own past calls.
This Lambda runs hourly to scan all signal sidecars for new TIER S/A,
confluence, regime, and debate calls and persists them in the
justhodl-trades DDB table with entry price + context.

A separate Lambda (justhodl-trade-evaluator) checks those calls at
1d/7d/30d/90d/180d intervals to fill in outcomes.

After 90 days you have a real performance ledger by strategy type.

═══════════════════════════════════════════════════════════════════════
STRATEGIES TRACKED
──────────────────
  TIER_S_CONFLUENCE   — Confluence detector says TIER S
  TIER_A_CONFLUENCE   — Confluence detector says TIER A
  TIER_S_ALPHA        — Alpha score >= 90 (raw composite)
  TIER_A_ALPHA        — Alpha score >= 80 (raw composite)
  REGIME_PICK         — Top in current regime
  DEBATE_STRONG_BUY   — 4 or 5 of 5 personas say BUY
  DEBATE_BUY          — 3 of 5 personas say BUY
  OPTIONS_TIER_A      — Bullish options flow >= 65

DEDUPE: pk=CALL, sk={call_date}#{symbol}#{strategy} — same call same day
is idempotent. Within a single day, only the first occurrence is logged
per (symbol, strategy) pair.

═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.request
from datetime import datetime, timezone
from decimal import Decimal

import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
ALPHA_KEY = "screener/alpha-score.json"
CONFLUENCE_KEY = "signals/confluence.json"
REGIME_KEY = "signals/regime-picks.json"
DEBATE_KEY = "data/debate.json"
OPTIONS_FLOW_KEY = "data/options-flow.json"
ANOMALIES_KEY = "signals/anomalies.json"
SCREENER_KEY = "screener/data.json"

DDB_TABLE = "justhodl-trades"

POLY_KEY = os.environ.get("POLY_KEY", "")

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table(DDB_TABLE)


# ═══════════════════════════════════════════════════════════════════════
# HELPERS
# ═══════════════════════════════════════════════════════════════════════

def load_s3_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)["Body"].read())
    except Exception as e:
        print(f"  load {key} err: {str(e)[:120]}")
        return None


def _dec(v):
    if v is None: return None
    return Decimal(str(v))


def fetch_current_price(symbol):
    """Latest close from Polygon prev-day endpoint."""
    if not POLY_KEY: return None
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLY_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-TLogger/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        if data.get("results"):
            return float(data["results"][0]["c"])
    except Exception: pass
    return None


def call_already_logged(symbol, strategy, call_date):
    """Idempotency check: did we already log this (symbol, strategy) today?"""
    try:
        resp = table.query(
            KeyConditionExpression="pk = :p AND begins_with(sk, :prefix)",
            ExpressionAttributeValues={
                ":p": "CALL",
                ":prefix": f"{call_date}#{symbol}#{strategy}",
            },
            Limit=1,
        )
        return len(resp.get("Items") or []) > 0
    except Exception as e:
        print(f"  dedupe check err: {e}")
        return False


def write_call(record):
    """Idempotent put_item — uses ConditionExpression to fail silently on dup."""
    try:
        table.put_item(
            Item=record,
            ConditionExpression="attribute_not_exists(pk) AND attribute_not_exists(sk)",
        )
        return True
    except Exception as e:
        # ConditionalCheckFailedException is expected on dupes
        if "ConditionalCheckFailedException" in str(type(e).__name__) or "ConditionalCheck" in str(e):
            return False
        print(f"  put err for {record.get('sk')}: {str(e)[:200]}")
        return False


def make_record(symbol, strategy, alpha_row, screener_row, signals_firing,
                 rationale, regime, macro_stress, current_price,
                 extra_fields=None):
    """Build a normalized DDB record."""
    now = datetime.now(timezone.utc)
    call_date = now.date().isoformat()
    timestamp = now.isoformat()
    sk = f"{call_date}#{symbol}#{strategy}"

    rec = {
        "pk": "CALL",
        "sk": sk,
        "call_date": call_date,
        "call_timestamp": timestamp,
        "symbol": symbol,
        "strategy": strategy,
        "alpha_score": _dec(alpha_row.get("alpha_score") if alpha_row else None),
        "tier": (alpha_row or {}).get("tier"),
        "entry_price": _dec(current_price),
        "current_price_at_call": _dec(current_price),
        "signals_firing": (signals_firing or [])[:8],
        "rationale": (rationale or "")[:500],
        "regime_at_call": regime or "UNKNOWN",
        "macro_stress_at_call": _dec(macro_stress),
        "outcome_status": "OPEN",
        "evaluated": False,
    }
    if screener_row:
        rec["name"] = screener_row.get("name")
        rec["sector"] = screener_row.get("sector")
    if alpha_row:
        comps = alpha_row.get("components") or {}
        rec["components_snapshot"] = {k: _dec(v) for k, v in comps.items() if v is not None}
    if extra_fields:
        for k, v in extra_fields.items():
            if v is None: continue
            rec[k] = _dec(v) if isinstance(v, (int, float)) else v
    return rec


# ═══════════════════════════════════════════════════════════════════════
# MAIN HANDLER
# ═══════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== TRADE LOGGER v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # Load all signal sources
    alpha = load_s3_json(ALPHA_KEY) or {}
    confluence = load_s3_json(CONFLUENCE_KEY) or {}
    regime = load_s3_json(REGIME_KEY) or {}
    debate = load_s3_json(DEBATE_KEY) or {}
    options_flow = load_s3_json(OPTIONS_FLOW_KEY) or {}
    anomalies = load_s3_json(ANOMALIES_KEY) or {}
    screener = load_s3_json(SCREENER_KEY) or {}

    alpha_stocks = alpha.get("stocks") or []
    alpha_by_sym = {s["symbol"]: s for s in alpha_stocks if s.get("symbol")}
    screener_by_sym = {s["symbol"]: s for s in (screener.get("stocks") or [])
                        if s.get("symbol")}

    # Macro context (recorded with every call)
    mss = anomalies.get("macro_stress_score")
    regime_label = anomalies.get("stress_interpretation", "").split("—")[0].strip().upper() if anomalies.get("stress_interpretation") else "UNKNOWN"

    today = datetime.now(timezone.utc).date().isoformat()

    # Collect (symbol, strategy, alpha_row, signals, rationale) tuples
    calls_to_make = []

    # ─── TIER_S_CONFLUENCE + TIER_A_CONFLUENCE ───
    for row in (confluence.get("rankings") or []):
        sym = row.get("symbol")
        tier = row.get("confluence_tier")
        if tier not in ("S", "A"): continue
        strategy = f"TIER_{tier}_CONFLUENCE"
        if call_already_logged(sym, strategy, today): continue
        components_firing = row.get("components_firing") or []
        rationale = (f"Confluence Tier {tier}: {len(components_firing)} signals firing — "
                      f"{', '.join(components_firing[:5])}")
        calls_to_make.append((sym, strategy, alpha_by_sym.get(sym, {}),
                              components_firing[:8], rationale, None))

    # ─── TIER_S_ALPHA (raw alpha >= 90) + TIER_A_ALPHA (>= 80) ───
    for s in alpha_stocks:
        sym = s.get("symbol")
        if not sym: continue
        alpha_score = s.get("alpha_score") or 0
        tier = s.get("tier")
        if tier == "S":
            strategy = "TIER_S_ALPHA"
        elif tier == "A":
            strategy = "TIER_A_ALPHA"
        else: continue
        if call_already_logged(sym, strategy, today): continue
        sigs = s.get("top_signals") or []
        rationale = f"Alpha {alpha_score} ({tier}): " + " · ".join(sigs[:3])
        calls_to_make.append((sym, strategy, s, sigs[:8], rationale, None))

    # ─── REGIME_PICK (top regime-adjusted) ───
    regime_picks = regime.get("regime_picks") or []
    # Top 5 by regime_adj_score
    top_regime = sorted(regime_picks, key=lambda r: -(r.get("regime_adj_score") or 0))[:5]
    for r in top_regime:
        sym = r.get("symbol")
        if not sym: continue
        strategy = "REGIME_PICK"
        if call_already_logged(sym, strategy, today): continue
        regime_label_for_call = r.get("regime") or regime_label or "UNKNOWN"
        rationale = (f"Top {regime_label_for_call} regime pick · adj score {r.get('regime_adj_score')}")
        sigs = [f"regime: {regime_label_for_call}"]
        calls_to_make.append((sym, strategy, alpha_by_sym.get(sym, {}), sigs, rationale,
                                {"regime_adj_score": r.get("regime_adj_score")}))

    # ─── DEBATE_STRONG_BUY / DEBATE_BUY ───
    for d in (debate.get("debates") or []):
        sym = d.get("symbol")
        if not sym: continue
        cv = (d.get("synthesis") or {}).get("consensus_verdict")
        if cv == "STRONG_BUY": strategy = "DEBATE_STRONG_BUY"
        elif cv == "BUY":       strategy = "DEBATE_BUY"
        else: continue
        if call_already_logged(sym, strategy, today): continue
        bear = (d.get("synthesis") or {}).get("bear_case", "")[:300]
        avg_conv = (d.get("synthesis") or {}).get("avg_conviction")
        rationale = f"5-persona debate: {cv} · avg conviction {avg_conv} · bear: {bear[:120]}"
        calls_to_make.append((sym, strategy, alpha_by_sym.get(sym, {}),
                              d.get("verdict_lines", [])[:5], rationale,
                              {"debate_avg_conviction": avg_conv,
                                "bear_case_at_call": bear,
                                "downside_target_pct": (d.get("synthesis") or {}).get("downside_target_pct")}))

    # ─── OPTIONS_TIER_A (bullish flow score >= 65) ───
    for r in (options_flow.get("all_qualifying") or []):
        sym = r.get("symbol")
        tier = r.get("tier")
        if tier != "TIER_A_BULLISH_FLOW": continue
        if call_already_logged(sym, "OPTIONS_TIER_A", today): continue
        flags = r.get("flags") or []
        score = r.get("score")
        rationale = f"Options flow TIER A · score {score} · {', '.join(flags[:3])}"
        calls_to_make.append((sym, "OPTIONS_TIER_A", alpha_by_sym.get(sym, {}),
                                flags, rationale,
                                {"options_flow_score_at_call": score}))

    # Deduplicate across our local batch (same symbol/strategy)
    seen = set()
    unique_calls = []
    for c in calls_to_make:
        key = (c[0], c[1])
        if key in seen: continue
        seen.add(key)
        unique_calls.append(c)

    print(f"  candidate calls: {len(unique_calls)} (after batch dedup)")

    # ─── Get current prices in batch ───
    # Use latest close from screener if available, otherwise fetch from Polygon
    n_logged = 0
    n_dup = 0
    n_price_failed = 0
    n_errors = 0

    for sym, strategy, alpha_row, sigs, rationale, extras in unique_calls:
        screener_row = screener_by_sym.get(sym)
        price = None
        if screener_row:
            price = screener_row.get("price") or screener_row.get("currentPrice")
        if not price:
            price = fetch_current_price(sym)
        if not price:
            n_price_failed += 1
            continue

        record = make_record(sym, strategy, alpha_row, screener_row, sigs,
                              rationale, regime_label, mss, price, extras)
        if write_call(record):
            n_logged += 1
        else:
            n_dup += 1

    elapsed = round(time.time() - started, 2)
    print(f"  logged={n_logged}, dup={n_dup}, price_fail={n_price_failed}, "
          f"elapsed={elapsed}s")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "candidates": len(unique_calls),
        "logged": n_logged, "duplicates": n_dup,
        "price_failed": n_price_failed,
        "elapsed_seconds": elapsed,
    })}
