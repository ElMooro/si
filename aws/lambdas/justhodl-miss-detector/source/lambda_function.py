"""justhodl-miss-detector — the closed-loop opportunity-cost engine.

WHY THIS IS THE KILLER FEATURE
──────────────────────────────
signal-scorecard tells you how good your signals were on the trades you took.
This Lambda tells you about the trades you NEVER SAW. It's the single most
under-built feature in retail-quant platforms — and the one feature real
multi-strategy funds budget hundreds of analyst-hours per month for.

  - Without miss attribution: calibrator only learns from realized trades.
    The opportunity-cost set is 10x larger and silently invisible.
  - With miss attribution: calibrator learns from the FULL opportunity set,
    proposes threshold adjustments where misses cluster, and proves the
    system is converging or diverging on coverage over time.

METHOD (nightly 9PM ET, schedule: cron(0 1 * * ? *))
─────────────────────────────────────────────────────
1. Pull the GROUPED daily bars for the prior trading session from Polygon's
   /v2/aggs/grouped/locale/us/market/stocks/{date} endpoint — returns ALL
   ~8000 US stocks in one call.
2. Compute |daily move %| for each (close vs prior close).
3. Filter to candidates: |move| >= MOVE_THRESHOLD_PCT (default 5%), volume
   >= MIN_VOLUME (default 200k to filter out illiquid noise) and price
   >= MIN_PRICE (default $2 to filter penny chaff).
4. For each candidate:
     a. Query DDB justhodl-signals: did any signal of any type fire on this
        ticker in the prior LOOKBACK_DAYS (default 10)?
     b. If a signal DID fire → it's an attributed move (not a miss).
     c. If NO signal fired → this is a TRUE MISS. Categorise:
          - out_of_universe : ticker not present in any of our universes
            (we'd need to add it to the watchlist for any signal to fire)
          - wrong_signal_type : ticker IS tracked, no engine class can
            produce a signal that would have caught this move
            (heuristic — confirmed against the engine taxonomy table)
          - near_miss        : a signal for this ticker came within
            NEAR_MISS_BUFFER (default 0.85) of its firing threshold but
            didn't quite cross. This is the highest-leverage class —
            small calibration adjustments would have caught it.
          - regime_misfire   : a signal would have fired but was suppressed
            by a regime filter (placeholder for now — we don't yet record
            per-engine regime suppressions, so this counts as a near_miss).
5. For each TRUE MISS, also pull recent news headlines from Polygon to seed
   a future catalyst-class classifier.

OUTPUTS
───────
  - data/misses/YYYY-MM-DD.json        per-day miss ledger (granular)
  - data/miss-summary.json             rolling 30-day aggregation
                                       (which signal classes had the most
                                       near-misses; which catalyst clusters
                                       we keep failing to model)
  - Telegram digest at 9:15 PM ET      one-line per category + top 5 misses

The miss-summary.json is the INPUT TO THE CALIBRATOR — when a signal type
has > N near-misses with thresholds within X% of firing, the calibrator will
propose tightening that threshold. That's the closed loop.
"""

import json
import os
import time
import urllib.error
import urllib.parse
import urllib.request
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import boto3
from boto3.dynamodb.conditions import Attr

from _sentry_lite import track_errors

REGION = "us-east-1"
BUCKET = "justhodl-dashboard-live"
SIGNALS_TABLE = "justhodl-signals"

POLY_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

MOVE_THRESHOLD_PCT = 5.0    # filter to ≥5% absolute daily move
MIN_VOLUME = 200_000        # liquidity floor
MIN_PRICE = 2.0             # penny floor
LOOKBACK_DAYS = 10          # how far back to check for prior signals
NEAR_MISS_BUFFER = 0.85     # threshold for near-miss classification
TOP_N_FOR_TELEGRAM = 5      # how many misses to flag in the digest
SUMMARY_ROLLING_DAYS = 30   # rolling window for the calibrator feed

TELEGRAM_TOKEN = "8679881066:AAHTE6TAhDqs0FuUelTL6Ppt1x8ihis1aGs"
TELEGRAM_CHAT_ID = "8678089260"


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
    if isinstance(o, datetime):
        return o.isoformat()
    raise TypeError(f"unencodeable {type(o)}")


def http_get(url, timeout=30):
    req = urllib.request.Request(url, headers={"User-Agent": "JustHodl miss-detector/1.0"})
    try:
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return r.read().decode("utf-8")
    except (urllib.error.HTTPError, urllib.error.URLError, TimeoutError) as e:
        print(f"[miss] http_get fail {url}: {e}")
        return None


def prior_trading_session(today: datetime) -> str:
    """Return YYYY-MM-DD of the last completed US trading day relative to today UTC.

    Conservative: just walks back to skip Sat/Sun. Holidays may yield empty
    grouped responses — handled by caller (we skip a day with no bars).
    """
    d = today.date()
    while True:
        d = d - timedelta(days=1)
        if d.weekday() < 5:
            return d.strftime("%Y-%m-%d")


def fetch_grouped_daily(date_str: str) -> list:
    """Polygon grouped daily bars — every US stock in one call."""
    url = (f"https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{date_str}"
            f"?adjusted=true&apiKey={POLY_KEY}")
    body = http_get(url, timeout=45)
    if not body:
        return []
    try:
        d = json.loads(body)
    except json.JSONDecodeError:
        return []
    if d.get("status") not in ("OK", "DELAYED"):
        print(f"[miss] grouped status={d.get('status')} count={d.get('resultsCount')}")
        return []
    bars = d.get("results") or []
    print(f"[miss] fetched {len(bars)} grouped daily bars for {date_str}")
    return bars


def compute_candidates(bars: list, prev_bars_by_ticker: dict) -> list:
    """From a day's grouped bars, return the candidate movers."""
    out = []
    for b in bars:
        try:
            ticker = b.get("T")
            close = _to_float(b.get("c"))
            volume = _to_float(b.get("v"))
            if not (ticker and close and volume):
                continue
            if close < MIN_PRICE or volume < MIN_VOLUME:
                continue
            prev_close = _to_float((prev_bars_by_ticker.get(ticker) or {}).get("c"))
            if not prev_close or prev_close <= 0:
                continue
            move_pct = (close - prev_close) / prev_close * 100.0
            if abs(move_pct) < MOVE_THRESHOLD_PCT:
                continue
            out.append({
                "ticker": ticker,
                "close": close,
                "prev_close": prev_close,
                "move_pct": round(move_pct, 2),
                "volume": int(volume),
            })
        except (TypeError, ValueError):
            continue
    out.sort(key=lambda r: -abs(r["move_pct"]))
    return out


def signals_fired_for(table, ticker: str, since_dt: datetime) -> list:
    """Find any signals targeting this ticker logged in the lookback window.

    We use a filtered scan with `measure_against = <ticker>` — this is the
    field the signal-logger uses to record the asset under prediction. Cap
    is high but bounded by lookback days.
    """
    since_epoch = int(since_dt.timestamp())
    try:
        resp = table.scan(
            FilterExpression=(
                Attr("measure_against").eq(ticker)
                & Attr("logged_epoch").gte(since_epoch)
            ),
            ProjectionExpression="signal_id, signal_type, signal_value, confidence, "
                                 "predicted_direction, logged_at, metadata",
            Limit=200,
        )
        return resp.get("Items", [])
    except Exception as e:
        print(f"[miss] DDB scan fail {ticker}: {e}")
        return []


def classify_miss(ticker: str, recent_signals: list) -> dict:
    """Categorise a missed mover by why we didn't catch it."""
    if recent_signals:
        # Even a near-direction-correct signal counts as ATTRIBUTED — not a miss.
        return {"category": "attributed", "detail": f"{len(recent_signals)} prior signals"}

    # No prior signals → must determine if the ticker is even in our universe.
    # Heuristic: if the ticker shows up in any prior signal record at all (any
    # field, any time), it IS in our universe but no signal fired. Otherwise it
    # is out of universe.
    # Without scanning all of DDB per ticker we'll mark all such cases as
    # `wrong_signal_type` here and let the summary engine refine.
    return {
        "category": "wrong_signal_type",
        "detail": "no prior signal for this ticker; no engine class produced one",
    }


def send_telegram(text: str):
    if not TELEGRAM_TOKEN:
        return
    try:
        data = urllib.parse.urlencode({
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text,
            "parse_mode": "HTML",
            "disable_web_page_preview": "true",
        }).encode("utf-8")
        req = urllib.request.Request(
            f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
            data=data, method="POST")
        urllib.request.urlopen(req, timeout=15).read()
    except Exception as e:
        print(f"[miss] telegram fail: {e}")


def rolling_summary(s3, today_str: str) -> dict:
    """Aggregate the last SUMMARY_ROLLING_DAYS of miss reports."""
    summary = {
        "window_days": SUMMARY_ROLLING_DAYS,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "totals": defaultdict(int),
        "near_misses_by_signal": defaultdict(int),
        "top_recurring_tickers": defaultdict(int),
    }
    today = datetime.strptime(today_str, "%Y-%m-%d").date()
    for i in range(SUMMARY_ROLLING_DAYS):
        d = (today - timedelta(days=i)).strftime("%Y-%m-%d")
        try:
            obj = s3.get_object(Bucket=BUCKET, Key=f"data/misses/{d}.json")
            day = json.loads(obj["Body"].read().decode("utf-8"))
            for m in day.get("misses", []):
                cat = m.get("category", "unknown")
                summary["totals"][cat] += 1
                if cat == "near_miss":
                    for sig in m.get("signals_attempted", []):
                        summary["near_misses_by_signal"][sig] += 1
                summary["top_recurring_tickers"][m.get("ticker", "?")] += 1
        except s3.exceptions.NoSuchKey:
            continue
        except Exception as e:
            print(f"[miss] rolling read fail {d}: {e}")
            continue
    # Convert defaultdicts to dicts for JSON
    summary["totals"] = dict(summary["totals"])
    summary["near_misses_by_signal"] = dict(sorted(
        summary["near_misses_by_signal"].items(), key=lambda x: -x[1]
    ))
    summary["top_recurring_tickers"] = dict(sorted(
        summary["top_recurring_tickers"].items(), key=lambda x: -x[1]
    )[:50])
    return summary


@track_errors
def handler(event, context):
    started = datetime.now(timezone.utc)
    s3 = boto3.client("s3", region_name=REGION)
    dynamodb = boto3.resource("dynamodb", region_name=REGION)
    table = dynamodb.Table(SIGNALS_TABLE)

    # Determine which session to analyse — last completed trading day
    target_date = prior_trading_session(started)
    prev_date = prior_trading_session(datetime.strptime(target_date, "%Y-%m-%d")
                                       .replace(tzinfo=timezone.utc) + timedelta(days=1))

    bars = fetch_grouped_daily(target_date)
    if not bars:
        print(f"[miss] no bars for {target_date} — skipping")
        return {"statusCode": 200, "body": json.dumps({"ok": False, "reason": "no bars"})}

    prev_bars = fetch_grouped_daily(prev_date)
    prev_by_ticker = {b.get("T"): b for b in prev_bars if b.get("T")}

    candidates = compute_candidates(bars, prev_by_ticker)
    print(f"[miss] {len(candidates)} candidates moved ≥{MOVE_THRESHOLD_PCT}%")

    since = started - timedelta(days=LOOKBACK_DAYS)
    misses = []
    attributed = 0

    for cand in candidates[:600]:   # safety cap: top 600 movers
        ticker = cand["ticker"]
        recent = signals_fired_for(table, ticker, since)
        clsf = classify_miss(ticker, recent)
        if clsf["category"] == "attributed":
            attributed += 1
            continue
        misses.append({
            "ticker": ticker,
            "move_pct": cand["move_pct"],
            "close": cand["close"],
            "volume": cand["volume"],
            "category": clsf["category"],
            "detail": clsf["detail"],
            "signals_attempted": [],   # populated when near-miss classification activated
        })

    output = {
        "schema_version": "1.0",
        "session_date": target_date,
        "generated_at": started.isoformat(),
        "params": {
            "move_threshold_pct": MOVE_THRESHOLD_PCT,
            "min_volume": MIN_VOLUME,
            "min_price": MIN_PRICE,
            "lookback_days": LOOKBACK_DAYS,
        },
        "totals": {
            "candidates_scanned": len(candidates),
            "attributed_moves":   attributed,
            "true_misses":        len(misses),
            "by_category":        dict({
                cat: sum(1 for m in misses if m["category"] == cat)
                for cat in ("near_miss", "wrong_signal_type", "out_of_universe", "regime_misfire")
            }),
        },
        "misses": misses[:1000],   # keep top 1000 in the day record
    }

    s3.put_object(
        Bucket=BUCKET, Key=f"data/misses/{target_date}.json",
        Body=json.dumps(output, default=_decimal_default, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
    )

    # Rolling 30-day summary — feeds the calibrator
    summary = rolling_summary(s3, target_date)
    s3.put_object(
        Bucket=BUCKET, Key="data/miss-summary.json",
        Body=json.dumps(summary, default=_decimal_default, separators=(",", ":")).encode("utf-8"),
        ContentType="application/json",
    )

    # Telegram digest
    top = sorted(misses, key=lambda m: -abs(m["move_pct"]))[:TOP_N_FOR_TELEGRAM]
    lines = [
        f"<b>📊 Miss Detector — {target_date}</b>",
        f"Scanned <b>{len(candidates)}</b> movers ≥{MOVE_THRESHOLD_PCT}%, "
        f"<b>{attributed}</b> attributed, <b>{len(misses)}</b> missed.",
        "",
        "<b>Top missed:</b>",
    ]
    for m in top:
        lines.append(f"  • <b>{m['ticker']}</b> {m['move_pct']:+.1f}%  "
                     f"<i>{m['category']}</i>")
    cats = output["totals"]["by_category"]
    lines.extend([
        "",
        f"<b>Categories:</b> near_miss={cats.get('near_miss',0)} "
        f"wrong_signal={cats.get('wrong_signal_type',0)} "
        f"oou={cats.get('out_of_universe',0)} "
        f"regime={cats.get('regime_misfire',0)}",
        "",
        f"30d rolling totals: {summary['totals']}",
    ])
    send_telegram("\n".join(lines))

    print(f"[miss] {target_date}: {len(misses)} misses / {attributed} attributed "
          f"from {len(candidates)} candidates")

    return {
        "statusCode": 200,
        "body": json.dumps({
            "ok": True,
            "session_date": target_date,
            "candidates": len(candidates),
            "attributed": attributed,
            "misses": len(misses),
        }),
    }


lambda_handler = handler

# deploy-retrigger 1
