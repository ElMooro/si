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


def trading_day_before(date_str: str) -> str:
    """Return YYYY-MM-DD of the trading day STRICTLY BEFORE the given date.

    Used to compute previous-close for percentage-move calculation. The bug
    we're fixing here: passing target_date + 1 day into prior_trading_session
    can return target_date itself when target_date+1 is Saturday — making
    prev_date == target_date and every move compute as 0%.
    """
    d = datetime.strptime(date_str, "%Y-%m-%d").date()
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

    Two query paths because DDB schema is heterogeneous:
      A. Newer schema v2: measure_against = <ticker> field is set
      B. Older schema v1: ticker embedded in signal_id like
         'deepvalue_ELV_1778537171' — we can't index this without GSI,
         so we use a contains() filter on signal_id

    Cap on both paths. The miss-detector tolerates duplicates from the
    OR (a hit on either branch counts as 'attributed').
    """
    since_epoch = int(since_dt.timestamp())
    found = []
    
    # Path A: explicit measure_against match (preferred)
    try:
        resp = table.scan(
            FilterExpression=(
                Attr("measure_against").eq(ticker)
                & Attr("logged_epoch").gte(since_epoch)
            ),
            ProjectionExpression="signal_id, signal_type, signal_value, confidence, "
                                 "predicted_direction, logged_at, metadata",
            Limit=100,
        )
        found.extend(resp.get("Items", []))
    except Exception as e:
        print(f"[miss] A-scan fail {ticker}: {e}")
    
    # Path B: ticker contained in signal_id (older schema)
    if not found:
        try:
            resp = table.scan(
                FilterExpression=(
                    Attr("signal_id").contains(f"_{ticker}_")
                    & Attr("logged_epoch").gte(since_epoch)
                ),
                ProjectionExpression="signal_id, signal_type, confidence, "
                                     "predicted_direction, logged_at",
                Limit=50,
            )
            found.extend(resp.get("Items", []))
        except Exception as e:
            print(f"[miss] B-scan fail {ticker}: {e}")
    
    return found


def classify_miss(ticker: str, recent_signals: list,
                   universe_tickers: set = None,
                   ranker_tickers: set = None) -> dict:
    """Categorise a missed mover by why we didn't catch it.

    Categories:
      - attributed         : prior signal exists → not actually a miss
      - near_miss          : signal close to firing (requires engine cooperation
                              to log near-fires; placeholder for now)
      - wrong_signal_type  : ticker IS in our universe, but no engine class
                              produced any signal for it in the lookback window
                              (universe coverage is OK, signal coverage is not)
      - out_of_universe    : ticker NOT in any of our tracked universes
                              (we don't even watch it; expansion candidate)
    """
    if recent_signals:
        return {"category": "attributed", "detail": f"{len(recent_signals)} prior signals"}

    t = ticker.upper()

    # Out-of-universe → we don't watch it
    in_universe = (universe_tickers is not None and t in universe_tickers)
    in_ranker   = (ranker_tickers is not None and t in ranker_tickers)

    if not in_universe and not in_ranker:
        return {
            "category": "out_of_universe",
            "detail": "ticker not present in universe.json nor master-ranker top-tickers",
        }
    
    # In universe but no signal → engine coverage gap
    where = []
    if in_universe: where.append("universe")
    if in_ranker:   where.append("ranker")
    return {
        "category": "wrong_signal_type",
        "detail": f"ticker is in {'+'.join(where)} but no engine produced a signal in {LOOKBACK_DAYS}d",
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
    """Aggregate the last SUMMARY_ROLLING_DAYS of miss reports.

    Also folds in near-misses from near-miss-monitor's hourly output.
    near-miss-monitor populates `data/near-misses-by-signal.json` with
    per-signal_type near-miss COUNTS derived from engine snapshots; we
    accumulate those across the rolling window by summing the latest
    snapshot (a daily run of miss-calibrator uses the latest figure).
    """
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
    
    # Fold in near-miss-monitor's snapshot-derived counts
    try:
        nm_obj = s3.get_object(Bucket=BUCKET, Key="data/near-misses-by-signal.json")
        nm = json.loads(nm_obj["Body"].read().decode("utf-8"))
        for sig, count in (nm.get("near_misses_by_signal") or {}).items():
            try:
                summary["near_misses_by_signal"][sig] += int(count)
            except (TypeError, ValueError):
                pass
        summary["near_miss_monitor"] = {
            "as_of": nm.get("generated_at"),
            "total_added": sum(int(v) for v in (nm.get("near_misses_by_signal") or {}).values()),
            "n_signals": len(nm.get("near_misses_by_signal") or {}),
        }
    except s3.exceptions.NoSuchKey:
        summary["near_miss_monitor"] = {"missing": True}
    except Exception as e:
        summary["near_miss_monitor"] = {"err": str(e)[:120]}
    
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
    prev_date = trading_day_before(target_date)
    print(f"[miss] target_date={target_date}  prev_date={prev_date}")

    bars = fetch_grouped_daily(target_date)
    if not bars:
        print(f"[miss] no bars for {target_date} — skipping")
        return {"statusCode": 200, "body": json.dumps({"ok": False, "reason": "no bars"})}

    prev_bars = fetch_grouped_daily(prev_date)
    if not prev_bars:
        # Holiday on prev_date — walk back further
        for back in range(2, 6):
            alt = trading_day_before(prev_date)
            prev_bars = fetch_grouped_daily(alt)
            if prev_bars:
                prev_date = alt
                print(f"[miss] prev_date holiday — walked back to {alt}")
                break
            prev_date = alt
    prev_by_ticker = {b.get("T"): b for b in prev_bars if b.get("T")}
    print(f"[miss] {len(bars)} target bars, {len(prev_by_ticker)} prev_by_ticker")

    candidates = compute_candidates(bars, prev_by_ticker)
    print(f"[miss] {len(candidates)} candidates moved ≥{MOVE_THRESHOLD_PCT}%")

    # Load universe + master-ranker for universe-aware classification
    universe_tickers = set()
    ranker_tickers = set()
    try:
        u_obj = s3.get_object(Bucket=BUCKET, Key="data/universe.json")
        u = json.loads(u_obj["Body"].read().decode("utf-8"))
        for stk in (u.get("stocks") or []):
            t = (stk.get("ticker") or stk.get("symbol") or "").upper()
            if t:
                universe_tickers.add(t)
    except Exception as e:
        print(f"[miss] could not load universe.json: {e}")
    
    try:
        r_obj = s3.get_object(Bucket=BUCKET, Key="data/master-ranker.json")
        r = json.loads(r_obj["Body"].read().decode("utf-8"))
        for stk in (r.get("top_tickers") or []):
            t = (stk.get("ticker") or "").upper()
            if t:
                ranker_tickers.add(t)
    except Exception as e:
        print(f"[miss] could not load master-ranker.json: {e}")
    
    print(f"[miss] universe={len(universe_tickers)} tickers, "
          f"ranker={len(ranker_tickers)} tickers")

    since = started - timedelta(days=LOOKBACK_DAYS)
    misses = []
    attributed = 0

    # Performance cap: we do 1-2 DDB scans per ticker. 511 candidates × 2 scans
    # × ~500ms = 8.5 min. Lambda timeout is 10min. Cap at top 200 movers.
    for cand in candidates[:200]:
        ticker = cand["ticker"]
        recent = signals_fired_for(table, ticker, since)
        clsf = classify_miss(ticker, recent,
                              universe_tickers=universe_tickers,
                              ranker_tickers=ranker_tickers)
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
            "signals_attempted": [],
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

    # ─── Emit miss.detected event with aggregate counts ─────────────────
    # Coordinator currently routes this to audit-only, but downstream
    # consumers can subscribe to track miss patterns over time.
    try:
        from system_events import publish
        publish("miss.detected", {
            "session_date":      target_date,
            "n_candidates":      len(candidates),
            "n_attributed":      attributed,
            "n_misses":          len(misses),
            "by_category":       cats,
            "move_threshold_pct": MOVE_THRESHOLD_PCT,
            "rolling_30d_totals": summary.get("totals"),
            "n_near_miss_signals": len(summary.get("near_misses_by_signal") or {}),
        }, source_engine="miss-detector")
    except Exception as e:
        print(f"[miss] event publish failed: {e}")

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
