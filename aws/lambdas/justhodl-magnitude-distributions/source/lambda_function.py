"""justhodl-magnitude-distributions — per signal-stack realized return distributions.

THE INSTITUTIONAL ANSWER TO "HOW MUCH WILL IT PUMP?"
─────────────────────────────────────────────────────
No serious desk publishes point estimates ("+18%"). They publish DISTRIBUTIONS:
  "when these specific signals stack together, historical median return over
   30 days is +12%, 25th percentile +3%, 75th percentile +24%, n=42 occurrences."

That is what this engine produces — every overnight, from the platform's own
realized-outcome ledger (DDB justhodl-signals).

PIPELINE POSITION
─────────────────
  signal-logger (continuous)    → writes signals with supporting_signals[] stack
  outcome-checker (weekly)      → fills outcomes{} with realized return_pct
  magnitude-distributions       → reads checked signals, groups by stack
                                   signature, computes distributions
  alpha-compass / conviction    → joins these distributions onto LIVE setups
                                   so the user sees "n=42, median +12%, P25 +3%"

METHOD
──────
1. Scan DDB justhodl-signals for status='checked' (outcomes are resolved).
2. Build a stack signature for each signal:
     sig = sha1(sorted_signal_types_including_self)
   The signature is deterministic — same signals fire the same hash even if
   recorded in different orders. Lone-signal cases included.
3. Extract realized return at the PRIMARY horizon (horizon_days_primary).
4. Group by (stack_sig, horizon). Compute:
     n, mean, median, std, P10/P25/P50/P75/P90, win_rate, max, min
5. Skip stacks with n < MIN_N (default 6 — Wilson-floor on small samples).
6. Emit:
     data/magnitude-distributions.json
       { "stacks": [ {sig, signals[], horizon, n, mean, median, p25, p75, ...} ],
         "by_signal": { "ema_breakout": [stack_sigs containing it], ... },
         "generated_at": ... }

WHY THIS NEVER REPLACES POINT-ESTIMATE LIARS
─────────────────────────────────────────────
A model that says "+18%" with no uncertainty is wrong even when it is right —
the user trades on a number they have no business trusting at that precision.
A distribution lets the desk size risk properly:
  • P25 informs stop-loss bounds (if 25% of historical outcomes were worse
    than -3%, your stop has to accept that).
  • P75-P25 is realized IQR — the natural volatility envelope for this setup.
  • n separates "real edge" from "small-sample folklore".

SELF-IMPROVING BY CONSTRUCTION
──────────────────────────────
Every new signal logged grows the population. Distributions update nightly.
As signal-scorecard deprecates dead signals, their stacks naturally drop
out (n shrinks). New signals appearing in stacks get evidence-based
distributions as soon as ≥MIN_N outcomes accumulate.

SCHEDULE
────────
cron(30 7 * * ? *) — daily 07:30 UTC (post outcome-checker + scorecard).
"""

import hashlib
import json
import os
import statistics
from collections import defaultdict
from datetime import datetime, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
SIGNALS_TABLE = "justhodl-signals"

OUTPUT_KEY = "data/magnitude-distributions.json"
MIN_N = 6                  # minimum sample size to publish a stack
MAX_STACKS_OUTPUT = 5000   # safety cap


def _to_float(v, default=None):
    if v is None:
        return default
    try:
        return float(v)
    except (TypeError, ValueError):
        return default


def _decimal_default(o):
    if isinstance(o, Decimal):
        return float(o)
    if isinstance(o, (datetime,)):
        return o.isoformat()
    raise TypeError(f"unencodeable {type(o)}")


def _to_int(v, default=None):
    if v is None:
        return default
    try:
        return int(v)
    except (TypeError, ValueError):
        try:
            return int(float(v))
        except (TypeError, ValueError):
            return default


def _signal_type_from_id(signal_id: str) -> str:
    """Fallback: extract signal_type prefix from a signal_id when the
    explicit signal_type field is empty.

    Real DDB items have ids like 'deepvalue_ELV_1778537171' where the
    first token is the signal class. Older schema v1 items often have
    signal_type='' but a populated signal_id.
    """
    if not signal_id:
        return ""
    s = str(signal_id)
    parts = s.split("_", 2)
    return parts[0] if parts else ""


def stack_signature(signal_type: str, supporting: list, signal_id: str = "") -> tuple:
    """Deterministic stack identifier with fallback to signal_id-derived type."""
    members = set()
    if signal_type:
        members.add(str(signal_type).strip().lower())
    elif signal_id:
        derived = _signal_type_from_id(signal_id)
        if derived:
            members.add(derived.lower())
    for s in supporting or []:
        if s:
            members.add(str(s).strip().lower())
    return tuple(sorted(members))


def stack_hash(sig_tuple: tuple) -> str:
    """Short stable hash of the stack signature, for keying in client UIs."""
    h = hashlib.sha1("||".join(sig_tuple).encode("utf-8")).hexdigest()
    return h[:12]


def scan_resolved_signals(table) -> list:
    """Full scan of signals table for outcomes that have been resolved.

    Real DDB status values observed: 'complete', 'partial', 'pending',
    'unscoreable', plus older items with status=None. Status='checked' was
    initially expected but doesn't actually exist. We accept 'complete'
    and 'partial' (both have at least some resolved outcome horizons).
    """
    items = []
    last_key = None
    while True:
        kw = {
            "FilterExpression": (
                Attr("status").eq("complete")
                | Attr("status").eq("partial")
                | Attr("status").eq("checked")
            )
        }
        if last_key:
            kw["ExclusiveStartKey"] = last_key
        resp = table.scan(**kw)
        items.extend(resp.get("Items", []))
        last_key = resp.get("LastEvaluatedKey")
        if not last_key:
            break
        if len(items) > 100_000:
            print(f"[magdist] scan cap hit at {len(items)} items")
            break
    return items


def extract_realized_return(item: dict) -> tuple:
    """Pull the realized return at the PRIMARY horizon for this signal.

    DDB stores horizon_days_primary as a STRING ('180' not 180), and
    check_windows entries are strings too. We coerce defensively.
    Returns (return_pct, horizon_days, predicted_direction) or
    (None, None, None) if outcome not usable.
    """
    horizon = _to_int(item.get("horizon_days_primary"))
    if horizon is None:
        windows = item.get("check_windows") or []
        if not windows:
            return None, None, None
        horizons = [_to_int(w) for w in windows if _to_int(w) is not None]
        if not horizons:
            return None, None, None
        horizon = max(horizons)

    outcomes = item.get("outcomes") or {}
    if not isinstance(outcomes, dict):
        return None, None, None
    # Outcomes may be keyed as str or int
    o = (outcomes.get(str(horizon))
         or outcomes.get(horizon)
         or outcomes.get(f"{horizon}d"))
    if not isinstance(o, dict):
        # Some schemas key outcomes by the FULL date — fall back to any
        # outcome value whose dict contains a return_pct.
        for v in outcomes.values():
            if isinstance(v, dict) and v.get("return_pct") is not None:
                o = v
                break
        else:
            return None, None, None
    ret = _to_float(o.get("return_pct"))
    if ret is None:
        return None, None, None
    return ret, horizon, item.get("predicted_direction")


def compute_distribution(returns: list) -> dict:
    """Compute the institutional return-distribution summary."""
    if not returns:
        return {}
    sorted_r = sorted(returns)
    n = len(sorted_r)

    def pct(p):
        # linear interpolation between order statistics
        if n == 1:
            return sorted_r[0]
        k = (n - 1) * p
        f = int(k)
        c = min(f + 1, n - 1)
        if f == c:
            return sorted_r[f]
        return sorted_r[f] + (sorted_r[c] - sorted_r[f]) * (k - f)

    mean = statistics.fmean(sorted_r)
    std = statistics.pstdev(sorted_r) if n > 1 else 0.0
    median = statistics.median(sorted_r)
    p10, p25, p75, p90 = pct(0.10), pct(0.25), pct(0.75), pct(0.90)
    win_rate = sum(1 for r in sorted_r if r > 0) / n
    return {
        "n": n,
        "mean":    round(mean, 3),
        "median":  round(median, 3),
        "std":     round(std, 3),
        "min":     round(sorted_r[0], 3),
        "max":     round(sorted_r[-1], 3),
        "p10":     round(p10, 3),
        "p25":     round(p25, 3),
        "p75":     round(p75, 3),
        "p90":     round(p90, 3),
        "iqr":     round(p75 - p25, 3),
        "win_rate": round(win_rate, 3),
    }


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)

    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    s3 = boto3.client("s3", region_name=REGION)
    table = dynamodb.Table(SIGNALS_TABLE)

    items = scan_resolved_signals(table)
    print(f"[magdist] scanned {len(items)} resolved-status signals")

    # Bucket by (stack_sig, horizon)
    by_stack_horizon = defaultdict(list)
    by_signal_to_stacks = defaultdict(set)
    n_usable = 0

    for it in items:
        ret, horizon, direction = extract_realized_return(it)
        if ret is None or horizon is None:
            continue
        sig_tuple = stack_signature(
            it.get("signal_type"),
            it.get("supporting_signals") or [],
            signal_id=it.get("signal_id", ""),
        )
        if not sig_tuple:
            continue
        key = (sig_tuple, horizon)
        by_stack_horizon[key].append(ret)
        for s in sig_tuple:
            by_signal_to_stacks[s].add(stack_hash(sig_tuple))
        n_usable += 1

    # Build output records
    stacks = []
    for (sig_tuple, horizon), returns in by_stack_horizon.items():
        if len(returns) < MIN_N:
            continue
        dist = compute_distribution(returns)
        stacks.append({
            "stack_hash": stack_hash(sig_tuple),
            "signals":    list(sig_tuple),
            "horizon_days": horizon,
            **dist,
        })

    # Sort by sample size desc (most robust first), tie-break median desc
    stacks.sort(key=lambda r: (-r["n"], -r["median"]))
    stacks = stacks[:MAX_STACKS_OUTPUT]

    by_signal = {s: sorted(list(hashes)) for s, hashes in by_signal_to_stacks.items()}

    output = {
        "schema_version": "1.0",
        "method": "realized_return_distribution_per_signal_stack",
        "generated_at": started.isoformat(),
        "min_n": MIN_N,
        "totals": {
            "checked_signals_scanned": len(items),
            "usable_outcomes":          n_usable,
            "unique_stacks":            len(by_stack_horizon),
            "published_stacks":         len(stacks),
            "unique_signal_types":      len(by_signal_to_stacks),
        },
        "stacks": stacks,
        "by_signal": by_signal,
    }

    s3.put_object(
        Bucket=BUCKET, Key=OUTPUT_KEY,
        Body=json.dumps(output, default=_decimal_default, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
        CacheControl="public, max-age=600",
    )

    print(f"[magdist] published {len(stacks)} stacks from {n_usable} outcomes "
          f"(scanned {len(items)}, unique signal types {len(by_signal_to_stacks)})")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "stacks": len(stacks),
            "outcomes": n_usable,
        }),
    }


# Local alias matching JustHodl's deploy convention
lambda_handler = handler

# deploy-retrigger 1
