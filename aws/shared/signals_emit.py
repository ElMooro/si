"""aws/shared/signals_emit.py — the ONE correct way to log a gradeable signal (ops 3379).

Fleet audit found ~40 direct emitters writing schema-v2 rows the outcome-
checker cannot score: no check_timestamps (its window loop no-ops) and/or a
LITERAL string in measure_against ("ticker", "ticker_vs_benchmark",
"ticker_vs_acwx"…) which the checker then tries to PRICE. The harvester is
the proven-correct template; this module is that template, shared.

Contract (mirrors justhodl-signal-harvester exactly):
  measure_against = the actual SYMBOL to price
  check_windows   = ["5","21",…]  AND  check_timestamps = {"day_5": iso,…}
  baseline_price REQUIRED (unscoreable otherwise) — yprice() included
  dedupe via ConditionExpression on signal_id = f"{type}#{TICKER}#{date}"
"""

import json
import re
import urllib.request
from datetime import datetime, timedelta, timezone
from decimal import Decimal

_UA = {"User-Agent": "Mozilla/5.0 (JustHodl-fleet)"}


def yprice(sym):
    """Latest close, Yahoo v8 keyless. None on any failure."""
    try:
        u = (f"https://query1.finance.yahoo.com/v8/finance/chart/{sym}"
             "?range=5d&interval=1d")
        with urllib.request.urlopen(urllib.request.Request(u, headers=_UA), timeout=12) as r:
            j = json.loads(r.read())
        res = j["chart"]["result"][0]
        m = res.get("meta") or {}
        p = m.get("regularMarketPrice")
        if p:
            return float(p)
        cl = [c for c in res["indicators"]["quote"][0]["close"] if c]
        return float(cl[-1]) if cl else None
    except Exception:
        return None


def _f2d(x):
    if isinstance(x, float):
        return Decimal(str(round(x, 6)))
    if isinstance(x, dict):
        return {k: _f2d(v) for k, v in x.items()}
    if isinstance(x, list):
        return [_f2d(v) for v in x]
    return x


def log_signal(table, signal_type, ticker, direction, windows, baseline_price,
               confidence=0.55, rationale="", metadata=None, benchmark=None,
               signal_value=""):
    """Write one harvester-contract row. Returns True if written, False on
    dedupe or bad inputs. `table` = boto3 dynamodb Table resource."""
    if not (ticker and re.fullmatch(r"[A-Z0-9.\-\^=]{1,10}", ticker)):
        return False
    if not baseline_price or baseline_price <= 0:
        return False
    now = datetime.now(timezone.utc)
    windows = [int(w) for w in windows]
    item = {
        "signal_id": f"{signal_type}#{ticker}#{now.date().isoformat()}",
        "signal_type": signal_type,
        "signal_value": str(signal_value)[:40],
        "predicted_direction": direction,
        "confidence": _f2d(max(0.05, min(0.95, float(confidence)))),
        "measure_against": ticker,
        "baseline_price": _f2d(float(baseline_price)),
        "baseline_benchmark_price": None,
        "benchmark": benchmark,
        "check_windows": [str(d) for d in windows],
        "check_timestamps": {f"day_{d}": (now + timedelta(days=d)).isoformat()
                             for d in windows},
        "outcomes": {}, "accuracy_scores": {},
        "logged_at": now.isoformat(), "logged_epoch": int(now.timestamp()),
        "status": "pending", "schema_version": "2",
        "horizon_days_primary": max(windows),
        "ttl": int((now + timedelta(days=365)).timestamp()),
        "rationale": str(rationale)[:300],
        "metadata": _f2d(metadata or {}),
    }
    try:
        table.put_item(Item=item,
                       ConditionExpression="attribute_not_exists(signal_id)")
        return True
    except Exception:
        return False
