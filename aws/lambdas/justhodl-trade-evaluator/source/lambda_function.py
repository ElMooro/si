"""
justhodl-trade-evaluator — Roadmap #16 Trade Journal (evaluator half)

═══════════════════════════════════════════════════════════════════════
WHY THIS EXISTS
───────────────
Trade-logger writes calls into DDB with entry price. This Lambda runs
daily after market close to check every open call against current price
and fill in the outcome at 1d/7d/30d/90d/180d intervals.

After 180 days every call has a complete return history. After 90 days
we have enough samples for win-rate stats per strategy.

═══════════════════════════════════════════════════════════════════════
EVALUATION CADENCE
──────────────────
For each open call, compute the return at each checkpoint that has
'matured' since the call:

  call_date + 1 day        → outcome_1d
  call_date + 7 days       → outcome_7d
  call_date + 30 days      → outcome_30d
  call_date + 90 days      → outcome_90d
  call_date + 180 days     → outcome_180d

When the 180-day checkpoint is reached, mark outcome_status = CLOSED.

═══════════════════════════════════════════════════════════════════════
JOURNAL SIDECAR
───────────────
Writes data/trade-journal.json with:
  - aggregate strategy stats (last 90 days)
    win rate, avg return, best/worst, total calls
  - best 10 calls (last 30 days)
  - worst 10 calls (last 30 days)
  - full ledger (last 90 days)

The /trades/ page reads this directly.

═══════════════════════════════════════════════════════════════════════
"""
import json
import os
import time
import urllib.request
import urllib.parse
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
from collections import defaultdict

import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
JOURNAL_KEY = "data/trade-journal.json"
DDB_TABLE = "justhodl-trades"

POLY_KEY = os.environ.get("POLY_KEY", "")

# Checkpoint days for outcome evaluation
CHECKPOINTS = [1, 7, 30, 90, 180]
JOURNAL_LOOKBACK_DAYS = 90

s3 = boto3.client("s3", region_name="us-east-1")
ddb = boto3.resource("dynamodb", region_name="us-east-1")
table = ddb.Table(DDB_TABLE)


# ═══════════════════════════════════════════════════════════════════════
# POLYGON HELPERS
# ═══════════════════════════════════════════════════════════════════════

def fetch_historical_close(symbol, target_date_str):
    """Get close on/just after target_date. Polygon /v2/aggs/.../range/.
    Returns float or None."""
    if not POLY_KEY: return None
    # Pull a 7-day window starting at target_date to handle weekends/holidays
    end_str = (datetime.fromisoformat(target_date_str).date() + timedelta(days=10)).isoformat()
    url = (f"https://api.polygon.io/v2/aggs/ticker/{symbol}/range/1/day/"
           f"{target_date_str}/{end_str}?adjusted=true&sort=asc&limit=20&apiKey={POLY_KEY}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Eval/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        bars = data.get("results") or []
        if bars: return float(bars[0]["c"])
    except Exception as e:
        print(f"  fetch {symbol}@{target_date_str} err: {str(e)[:80]}")
    return None


def fetch_current_close(symbol):
    if not POLY_KEY: return None
    url = f"https://api.polygon.io/v2/aggs/ticker/{symbol}/prev?adjusted=true&apiKey={POLY_KEY}"
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "JustHodl-Eval/1.0"})
        with urllib.request.urlopen(req, timeout=8) as r:
            data = json.loads(r.read().decode("utf-8"))
        if data.get("results"): return float(data["results"][0]["c"])
    except Exception: pass
    return None


# ═══════════════════════════════════════════════════════════════════════
# DDB HELPERS
# ═══════════════════════════════════════════════════════════════════════

def _decimal_to_float(obj):
    if isinstance(obj, Decimal): return float(obj)
    if isinstance(obj, dict): return {k: _decimal_to_float(v) for k, v in obj.items()}
    if isinstance(obj, list): return [_decimal_to_float(v) for v in obj]
    return obj


def _dec(v):
    if v is None: return None
    return Decimal(str(round(v, 4)))


def scan_all_calls(lookback_days=180):
    """Scan all CALL records from last N days. Returns list of dicts."""
    cutoff = (datetime.now(timezone.utc).date() - timedelta(days=lookback_days)).isoformat()
    items = []
    last = None
    while True:
        kwargs = {
            "KeyConditionExpression": "pk = :p AND sk >= :s",
            "ExpressionAttributeValues": {":p": "CALL", ":s": cutoff},
        }
        if last: kwargs["ExclusiveStartKey"] = last
        resp = table.query(**kwargs)
        items.extend(resp.get("Items") or [])
        last = resp.get("LastEvaluatedKey")
        if not last: break
    return [_decimal_to_float(i) for i in items]


def update_call_outcome(pk, sk, updates):
    """Update outcome fields on a CALL record."""
    if not updates: return
    expr_parts = []
    names = {}
    values = {}
    for i, (k, v) in enumerate(updates.items()):
        names[f"#k{i}"] = k
        values[f":v{i}"] = _dec(v) if isinstance(v, (int, float)) and not isinstance(v, bool) else v
        expr_parts.append(f"#k{i} = :v{i}")
    try:
        table.update_item(
            Key={"pk": pk, "sk": sk},
            UpdateExpression="SET " + ", ".join(expr_parts),
            ExpressionAttributeNames=names,
            ExpressionAttributeValues=values,
        )
        return True
    except Exception as e:
        print(f"  update err {sk}: {str(e)[:200]}")
        return False


# ═══════════════════════════════════════════════════════════════════════
# OUTCOME EVALUATION
# ═══════════════════════════════════════════════════════════════════════

def days_since(call_date_str):
    try:
        cd = datetime.fromisoformat(call_date_str).date() if "T" in call_date_str else date.fromisoformat(call_date_str)
    except Exception:
        return 0
    return (datetime.now(timezone.utc).date() - cd).days


def evaluate_call(call):
    """For one call record, compute any newly-matured checkpoints + return updates dict."""
    sym = call.get("symbol")
    entry = call.get("entry_price")
    call_date_str = (call.get("call_date") or "")[:10]
    if not sym or entry is None or not call_date_str:
        return {"_unevaluable": True}, {}

    age_days = days_since(call_date_str)
    updates = {}
    new_outcomes = {}  # for journal aggregation

    call_date = date.fromisoformat(call_date_str)

    for cp in CHECKPOINTS:
        field = f"outcome_{cp}d"
        if call.get(field):
            # Already evaluated this checkpoint
            new_outcomes[cp] = call[field]
            continue
        if age_days < cp: continue  # not yet matured

        # Need close at call_date + cp days
        target = (call_date + timedelta(days=cp)).isoformat()
        close = fetch_historical_close(sym, target)
        if close is None: continue  # leave for next run
        ret_pct = (close / entry - 1) * 100
        outcome = {
            "as_of": target,
            "close_price": round(close, 2),
            "return_pct": round(ret_pct, 2),
        }
        updates[field] = outcome
        new_outcomes[cp] = outcome

    # Determine final status:
    # CLOSED if 180d evaluated, OPEN otherwise. Tag HIT_TARGET / HIT_STOP /
    # STILL_OPEN based on price action vs target_price / stop_loss
    if age_days >= 180 and "outcome_180d" in updates:
        updates["outcome_status"] = "CLOSED"
    elif age_days >= 30 and "outcome_30d" in updates:
        # check if hit target or stop
        ret_30d = updates["outcome_30d"]["return_pct"]
        tgt = call.get("target_price")
        stp = call.get("stop_loss")
        if tgt and entry and updates["outcome_30d"]["close_price"] >= tgt:
            updates["outcome_status"] = "HIT_TARGET"
        elif stp and updates["outcome_30d"]["close_price"] <= stp:
            updates["outcome_status"] = "HIT_STOP"

    updates["evaluated"] = True
    updates["last_evaluated"] = datetime.now(timezone.utc).isoformat()

    return updates, new_outcomes


# ═══════════════════════════════════════════════════════════════════════
# JOURNAL AGGREGATION
# ═══════════════════════════════════════════════════════════════════════

def build_journal_sidecar(all_calls):
    """Aggregate stats for /trades/ dashboard."""
    today = datetime.now(timezone.utc).date()

    # Per-strategy aggregates over last 90 days
    by_strategy = defaultdict(lambda: {"n_calls": 0, "with_30d": 0, "wins_30d": 0,
                                          "returns_30d": [], "with_90d": 0,
                                          "wins_90d": 0, "returns_90d": []})

    for call in all_calls:
        strat = call.get("strategy")
        if not strat: continue
        by_strategy[strat]["n_calls"] += 1

        o30 = call.get("outcome_30d")
        if o30 and o30.get("return_pct") is not None:
            r = o30["return_pct"]
            by_strategy[strat]["with_30d"] += 1
            by_strategy[strat]["returns_30d"].append(r)
            if r > 0: by_strategy[strat]["wins_30d"] += 1

        o90 = call.get("outcome_90d")
        if o90 and o90.get("return_pct") is not None:
            r = o90["return_pct"]
            by_strategy[strat]["with_90d"] += 1
            by_strategy[strat]["returns_90d"].append(r)
            if r > 0: by_strategy[strat]["wins_90d"] += 1

    strategy_stats = []
    for strat, s in by_strategy.items():
        avg_30d = sum(s["returns_30d"]) / len(s["returns_30d"]) if s["returns_30d"] else None
        avg_90d = sum(s["returns_90d"]) / len(s["returns_90d"]) if s["returns_90d"] else None
        win_30d = (s["wins_30d"] / s["with_30d"] * 100) if s["with_30d"] else None
        win_90d = (s["wins_90d"] / s["with_90d"] * 100) if s["with_90d"] else None
        strategy_stats.append({
            "strategy": strat,
            "total_calls_90d": s["n_calls"],
            "evaluated_30d": s["with_30d"],
            "win_rate_30d_pct": round(win_30d, 1) if win_30d is not None else None,
            "avg_return_30d_pct": round(avg_30d, 2) if avg_30d is not None else None,
            "evaluated_90d": s["with_90d"],
            "win_rate_90d_pct": round(win_90d, 1) if win_90d is not None else None,
            "avg_return_90d_pct": round(avg_90d, 2) if avg_90d is not None else None,
        })
    strategy_stats.sort(key=lambda s: -(s.get("avg_return_90d_pct") or s.get("avg_return_30d_pct") or -999))

    # Best/worst calls — based on 30d return where available, otherwise most recent
    eval_30d_recent = [c for c in all_calls
                        if c.get("outcome_30d") and isinstance(c["outcome_30d"].get("return_pct"), (int, float))
                        and days_since(c.get("call_date", "")) <= 60]
    eval_30d_recent.sort(key=lambda c: -c["outcome_30d"]["return_pct"])
    best = eval_30d_recent[:10]
    worst = eval_30d_recent[-10:]
    worst.reverse()  # show worst first

    # Compact ledger for /trades/ page
    ledger = []
    for c in sorted(all_calls, key=lambda x: x.get("call_timestamp", ""), reverse=True)[:200]:
        ledger.append({
            "call_date": c.get("call_date"),
            "call_timestamp": c.get("call_timestamp"),
            "symbol": c.get("symbol"),
            "name": c.get("name"),
            "sector": c.get("sector"),
            "strategy": c.get("strategy"),
            "alpha_score": c.get("alpha_score"),
            "tier": c.get("tier"),
            "entry_price": c.get("entry_price"),
            "regime_at_call": c.get("regime_at_call"),
            "macro_stress_at_call": c.get("macro_stress_at_call"),
            "outcome_1d": c.get("outcome_1d"),
            "outcome_7d": c.get("outcome_7d"),
            "outcome_30d": c.get("outcome_30d"),
            "outcome_90d": c.get("outcome_90d"),
            "outcome_180d": c.get("outcome_180d"),
            "outcome_status": c.get("outcome_status"),
            "rationale": (c.get("rationale") or "")[:200],
            "signals_firing": c.get("signals_firing"),
        })

    total_calls = len(all_calls)
    total_evaluated_30d = sum(s["with_30d"] for s in by_strategy.values())
    total_wins_30d = sum(s["wins_30d"] for s in by_strategy.values())
    total_returns_30d = [r for s in by_strategy.values() for r in s["returns_30d"]]

    summary = {
        "total_calls_90d": total_calls,
        "total_evaluated_30d": total_evaluated_30d,
        "overall_win_rate_30d_pct": (round(total_wins_30d / total_evaluated_30d * 100, 1)
                                       if total_evaluated_30d else None),
        "overall_avg_return_30d_pct": (round(sum(total_returns_30d) / len(total_returns_30d), 2)
                                          if total_returns_30d else None),
        "n_strategies_tracked": len(by_strategy),
        "n_open": sum(1 for c in all_calls if c.get("outcome_status") in (None, "OPEN")),
        "n_closed_or_hit": sum(1 for c in all_calls
                                if c.get("outcome_status") in ("CLOSED", "HIT_TARGET", "HIT_STOP")),
    }

    return {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "lookback_days": JOURNAL_LOOKBACK_DAYS,
        "summary": summary,
        "strategies": strategy_stats,
        "best_calls_60d": best,
        "worst_calls_60d": worst,
        "ledger": ledger,
    }


# ═══════════════════════════════════════════════════════════════════════
# MAIN HANDLER
# ═══════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== TRADE EVALUATOR v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # 1. Scan all open + recently-closed calls (180d lookback)
    all_calls = scan_all_calls(lookback_days=200)
    print(f"  scanned {len(all_calls)} calls")

    # 2. Evaluate each (skip already-fully-evaluated 180d calls)
    n_updated = 0
    n_no_change = 0
    n_unevaluable = 0
    for call in all_calls:
        if call.get("outcome_status") == "CLOSED" and call.get("outcome_180d"):
            n_no_change += 1
            continue
        updates, new_outcomes = evaluate_call(call)
        if updates.get("_unevaluable"):
            n_unevaluable += 1; continue
        # Only push the update if it has new checkpoint data
        meaningful = any(k.startswith("outcome_") for k in updates if k != "evaluated")
        if not meaningful:
            n_no_change += 1; continue
        if update_call_outcome(call["pk"], call["sk"], updates):
            n_updated += 1
            # Merge into local copy for journal aggregation
            for k, v in updates.items():
                if k.startswith("outcome_") and v: call[k] = v

    print(f"  updated={n_updated}  unchanged={n_no_change}  unevaluable={n_unevaluable}")

    # 3. Build journal sidecar
    journal = build_journal_sidecar(all_calls)

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=JOURNAL_KEY,
            Body=json.dumps(journal, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json",
            CacheControl="public, max-age=1800")
        print(f"  ✓ trade-journal.json written ({len(journal['ledger'])} ledger entries, "
              f"{len(journal['strategies'])} strategies)")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": str(e)})}

    elapsed = round(time.time() - started, 2)
    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_calls_scanned": len(all_calls),
        "n_updated": n_updated,
        "n_strategies_tracked": len(journal["strategies"]),
        "overall_win_rate_30d_pct": journal["summary"].get("overall_win_rate_30d_pct"),
        "overall_avg_return_30d_pct": journal["summary"].get("overall_avg_return_30d_pct"),
        "elapsed_seconds": elapsed,
    })}
