"""
justhodl-dix — Squeezemetrics DIX & GEX historical engine

WHAT IS DIX?
============
DIX (Dark Index) = % of S&P 500 volume that's BUYING in off-exchange dark pools.
Dark pools are where institutions execute large block trades without moving
the public tape. When dark-pool volume is heavily skewed to buying, it
signals quiet institutional accumulation — historically a bullish setup.

WHAT IS GEX (here)?
===================
Squeezemetrics' GEX = aggregate dealer gamma exposure (SPY/SPX options).
Positive GEX = dealers stabilize (mean-revert).
Negative GEX = dealers amplify (trend).
This is the macro-level companion to our per-symbol justhodl-dealer-gex Lambda.

DATA SOURCE
===========
  https://squeezemetrics.com/monitor/static/DIX.csv
  Format: date,price,dix,gex (CSV, ~217 KB, 15+ years of history)
  Daily, free, no auth, refreshed ~5PM ET

METRICS COMPUTED
================
  Current values + 5d/20d/60d moving averages
  Z-score of DIX vs 60d window
  Percentile rank vs 252d (1y) window
  Day count of sustained bullish DIX (>47% over last 5d)
  Day count of sustained bearish DIX (<40% over last 5d)
  GEX regime (positive/negative gamma) + 20d trend
  Forward returns by DIX bucket (backtest from 2011)

REGIME CLASSIFICATION
=====================
  DIX > 47%  → STRONG_ACCUMULATION (90th-pct historical)
  DIX 45-47  → ACCUMULATION
  DIX 42-45  → NEUTRAL
  DIX 40-42  → CAUTION
  DIX < 40   → DISTRIBUTION (10th-pct, historically bearish setup)

  Combined with GEX:
    DIX > 45 + GEX positive  → BULLISH_STABILIZED
    DIX > 45 + GEX negative  → BULLISH_VOLATILE
    DIX < 42 + GEX negative  → BEARISH_GAMMA_RISK
    DIX < 42 + GEX positive  → DISTRIBUTION_CAPPED

INTEGRATION
===========
  Sidecar: data/dix.json
  Page: /dix/
  ai-chat: always-on [DIX] context
  morning-intel: dix_regime, dix_5d, dix_z, gex_macro fields
"""
import io, json, os, time, urllib.request, csv as _csv
from datetime import datetime, timezone
import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/dix.json"
HISTORY_KEY = "data/dix-history.json"  # full series for charting

DIX_CSV_URL = "https://squeezemetrics.com/monitor/static/DIX.csv"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")
HTTP_TIMEOUT = 25

# Regime thresholds (% units)
DIX_STRONG_ACC = 47.0
DIX_ACC = 45.0
DIX_NEUTRAL_HI = 45.0
DIX_NEUTRAL_LO = 42.0
DIX_CAUTION = 42.0
DIX_DIST = 40.0

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# PURE-PYTHON STATS
# ═══════════════════════════════════════════════════════════════════════════

def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs):
    if len(xs) < 2: return 0.0
    m = _mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


def _percentile_rank(xs, value):
    """Return percentile rank of `value` in sorted xs (0.0 to 1.0)."""
    if not xs: return None
    sorted_xs = sorted(xs)
    below = sum(1 for x in sorted_xs if x < value)
    return below / len(sorted_xs)


# ═══════════════════════════════════════════════════════════════════════════
# DATA FETCH
# ═══════════════════════════════════════════════════════════════════════════

def fetch_dix_csv():
    """Returns list of {'date','price','dix','gex'} dicts, sorted ascending."""
    req = urllib.request.Request(DIX_CSV_URL, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120 Safari/537.36",
        "Accept": "text/csv",
    })
    with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
        text = r.read().decode("utf-8")
    reader = _csv.DictReader(io.StringIO(text))
    rows = []
    for row in reader:
        try:
            rows.append({
                "date": row["date"].strip(),
                "price": float(row["price"]),
                "dix": float(row["dix"]),  # 0.0-1.0
                "gex": float(row["gex"]),
            })
        except Exception:
            continue
    # Already chronological in source; ensure ascending
    rows.sort(key=lambda r: r["date"])
    return rows


# ═══════════════════════════════════════════════════════════════════════════
# REGIME LOGIC
# ═══════════════════════════════════════════════════════════════════════════

def classify_dix_regime(dix_pct):
    if dix_pct >= DIX_STRONG_ACC: return "STRONG_ACCUMULATION"
    if dix_pct >= DIX_ACC: return "ACCUMULATION"
    if dix_pct >= DIX_NEUTRAL_LO: return "NEUTRAL"
    if dix_pct >= DIX_DIST: return "CAUTION"
    return "DISTRIBUTION"


def combined_regime(dix_pct, gex):
    """Cross-product DIX regime with GEX sign."""
    gex_pos = gex > 0
    if dix_pct >= DIX_ACC and gex_pos:
        return "BULLISH_STABILIZED", "Institutions accumulating into positive gamma — low vol melt-up risk"
    if dix_pct >= DIX_ACC and not gex_pos:
        return "BULLISH_VOLATILE", "Accumulation against negative gamma — bullish but expect chop"
    if dix_pct < DIX_NEUTRAL_LO and not gex_pos:
        return "BEARISH_GAMMA_RISK", "Distribution + negative gamma — gap-down risk elevated"
    if dix_pct < DIX_NEUTRAL_LO and gex_pos:
        return "DISTRIBUTION_CAPPED", "Distribution but dealers stabilize — slow grind lower"
    return "NEUTRAL", "Mixed signals — wait for confirmation"


# ═══════════════════════════════════════════════════════════════════════════
# FORWARD RETURN BACKTEST
# ═══════════════════════════════════════════════════════════════════════════

def forward_returns_by_dix_bucket(rows, horizon_days):
    """Aggregate forward N-day return by DIX bucket (historical edge analysis)."""
    buckets = {
        "<40": {"n": 0, "rets": []},
        "40-42": {"n": 0, "rets": []},
        "42-45": {"n": 0, "rets": []},
        "45-47": {"n": 0, "rets": []},
        ">=47": {"n": 0, "rets": []},
    }
    for i, r in enumerate(rows):
        if i + horizon_days >= len(rows): break
        d_pct = r["dix"] * 100
        fwd = rows[i + horizon_days]["price"]
        ret = (fwd / r["price"] - 1) * 100
        if d_pct < 40: k = "<40"
        elif d_pct < 42: k = "40-42"
        elif d_pct < 45: k = "42-45"
        elif d_pct < 47: k = "45-47"
        else: k = ">=47"
        buckets[k]["n"] += 1
        buckets[k]["rets"].append(ret)
    out = {}
    for k, v in buckets.items():
        if v["n"] == 0:
            out[k] = {"n": 0}
            continue
        rets = v["rets"]
        avg = _mean(rets)
        win = sum(1 for r in rets if r > 0) / len(rets) * 100
        out[k] = {"n": v["n"], "avg_return_pct": round(avg, 2),
                  "win_rate_pct": round(win, 1),
                  "median": round(sorted(rets)[len(rets)//2], 2)}
    return out


# ═══════════════════════════════════════════════════════════════════════════
# TELEGRAM
# ═══════════════════════════════════════════════════════════════════════════

def get_chat_id():
    if TELEGRAM_CHAT_ID: return TELEGRAM_CHAT_ID
    try:
        return ssm.get_parameter(Name="/justhodl/telegram/chat_id",
                                  WithDecryption=True)["Parameter"]["Value"]
    except Exception: return None


def send_telegram(text):
    if not TELEGRAM_TOKEN: return False
    chat = get_chat_id()
    if not chat: return False
    try:
        url = f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage"
        body = json.dumps({"chat_id": chat, "text": text[:4096],
                            "parse_mode": "Markdown",
                            "disable_web_page_preview": True}).encode()
        req = urllib.request.Request(url, data=body,
            headers={"Content-Type": "application/json"})
        urllib.request.urlopen(req, timeout=10)
        return True
    except Exception as e:
        print(f"  telegram err: {str(e)[:80]}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# PRIOR-STATE COMPARE
# ═══════════════════════════════════════════════════════════════════════════

def load_prior_regime():
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)
        return json.loads(obj["Body"].read()).get("combined_regime")
    except Exception: return None


# ═══════════════════════════════════════════════════════════════════════════
# HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== DIX v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")
    prior_regime = load_prior_regime()
    rows = fetch_dix_csv()
    print(f"  fetched {len(rows)} rows ({rows[0]['date']} → {rows[-1]['date']})")

    if len(rows) < 60:
        return {"statusCode": 500,
                 "body": json.dumps({"err": "insufficient history",
                                       "n_rows": len(rows)})}

    latest = rows[-1]
    dix_pct = latest["dix"] * 100
    gex = latest["gex"]
    gex_b = gex / 1e9

    # Moving averages
    dix_pct_series = [r["dix"] * 100 for r in rows]
    gex_series = [r["gex"] for r in rows]
    price_series = [r["price"] for r in rows]
    dates = [r["date"] for r in rows]

    dix_5d = _mean(dix_pct_series[-5:])
    dix_20d = _mean(dix_pct_series[-20:])
    dix_60d = _mean(dix_pct_series[-60:])
    dix_252d = _mean(dix_pct_series[-252:])

    gex_5d = _mean(gex_series[-5:])
    gex_20d = _mean(gex_series[-20:])
    gex_60d = _mean(gex_series[-60:])

    # 60-day z-score
    sd_60 = _stdev(dix_pct_series[-60:])
    z_60 = (dix_pct - _mean(dix_pct_series[-60:])) / sd_60 if sd_60 > 0 else 0

    # 1y percentile rank
    pct_1y = _percentile_rank(dix_pct_series[-252:], dix_pct)
    pct_5y = _percentile_rank(dix_pct_series[-252*5:], dix_pct) if len(dix_pct_series) >= 252*5 else None
    pct_all = _percentile_rank(dix_pct_series, dix_pct)

    # Sustained signals
    last_5_above_47 = sum(1 for x in dix_pct_series[-5:] if x >= 47)
    last_5_below_40 = sum(1 for x in dix_pct_series[-5:] if x < 40)
    last_20_above_47 = sum(1 for x in dix_pct_series[-20:] if x >= 47)
    last_20_below_40 = sum(1 for x in dix_pct_series[-20:] if x < 40)

    dix_regime = classify_dix_regime(dix_pct)
    gex_regime = "POSITIVE" if gex > 0 else "NEGATIVE"
    comb_regime, comb_signal = combined_regime(dix_pct, gex)

    # Forward-return backtest by bucket (5/10/20/60 day horizons)
    backtest = {}
    for h in (5, 10, 20, 60):
        backtest[f"forward_{h}d"] = forward_returns_by_dix_bucket(rows, h)

    # Day-over-day changes for trends
    dod_dix = dix_pct - (dix_pct_series[-2] if len(dix_pct_series) >= 2 else dix_pct)
    dod_gex = gex - (gex_series[-2] if len(gex_series) >= 2 else gex)
    dod_price_pct = (latest["price"] / rows[-2]["price"] - 1) * 100 if len(rows) >= 2 else 0

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "source": "squeezemetrics.com/monitor/static/DIX.csv",
        "data_date": latest["date"],
        "elapsed_seconds": round(time.time() - started, 2),
        "current": {
            "date": latest["date"],
            "price": round(latest["price"], 2),
            "dix": round(latest["dix"], 5),
            "dix_pct": round(dix_pct, 2),
            "gex": round(gex, 0),
            "gex_billions": round(gex_b, 3),
        },
        "moving_averages": {
            "dix_5d_pct": round(dix_5d, 2),
            "dix_20d_pct": round(dix_20d, 2),
            "dix_60d_pct": round(dix_60d, 2),
            "dix_252d_pct": round(dix_252d, 2),
            "gex_5d": round(gex_5d, 0),
            "gex_20d": round(gex_20d, 0),
            "gex_60d": round(gex_60d, 0),
        },
        "statistics": {
            "dix_z_score_60d": round(z_60, 2),
            "dix_percentile_1y": round(pct_1y * 100, 1) if pct_1y is not None else None,
            "dix_percentile_5y": round(pct_5y * 100, 1) if pct_5y is not None else None,
            "dix_percentile_all_time": round(pct_all * 100, 1) if pct_all is not None else None,
        },
        "sustained_signals": {
            "n_last_5d_above_47": last_5_above_47,
            "n_last_20d_above_47": last_20_above_47,
            "n_last_5d_below_40": last_5_below_40,
            "n_last_20d_below_40": last_20_below_40,
        },
        "day_over_day": {
            "dix_change_pp": round(dod_dix, 2),
            "gex_change": round(dod_gex, 0),
            "price_change_pct": round(dod_price_pct, 2),
        },
        "dix_regime": dix_regime,
        "gex_regime": gex_regime,
        "combined_regime": comb_regime,
        "combined_signal": comb_signal,
        "regime_changed_from_prior": (prior_regime != comb_regime) if prior_regime else False,
        "thresholds": {
            "strong_accumulation": DIX_STRONG_ACC, "accumulation": DIX_ACC,
            "neutral_low": DIX_NEUTRAL_LO, "distribution": DIX_DIST,
        },
        "backtest_forward_returns_by_dix_bucket": backtest,
        "n_history_days": len(rows),
        "history_first_date": rows[0]["date"],
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ dix.json written ({round(len(json.dumps(payload))/1024,1)} KB)")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # Full history for charting (separate sidecar — 217KB+)
    try:
        chart_data = {
            "generated_at": payload["generated_at"],
            "n_days": len(rows),
            "first_date": rows[0]["date"], "last_date": rows[-1]["date"],
            "dates": dates,
            "price": [round(p, 2) for p in price_series],
            "dix_pct": [round(d * 100, 3) for d in [r["dix"] for r in rows]],
            "gex_billions": [round(g / 1e9, 3) for g in gex_series],
        }
        s3.put_object(Bucket=S3_BUCKET, Key=HISTORY_KEY,
            Body=json.dumps(chart_data, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=3600")
        print(f"  ✓ dix-history.json written")
    except Exception as e:
        print(f"  history put err: {str(e)[:120]}")

    # Telegram on regime change OR extreme readings
    alert_sent = False
    if (prior_regime and prior_regime != comb_regime) or comb_regime in (
        "BEARISH_GAMMA_RISK", "BULLISH_STABILIZED"
    ):
        lines = [f"🌑 *DIX Regime · {datetime.now(timezone.utc).strftime('%b %d')}*\n",
                  f"⚡ {comb_regime}",
                  f"_{comb_signal}_\n",
                  f"📊 DIX: *{dix_pct:.1f}%* (z={z_60:.2f}, {pct_1y*100 if pct_1y else 0:.0f}th pct 1y)",
                  f"⚖️  GEX: *{gex_b:+.2f}B* ({gex_regime})",
                  f"📈 SPX: ${latest['price']:.0f}",
                  f"\n_DIX 5d/20d: {dix_5d:.1f}% / {dix_20d:.1f}%_"]
        if prior_regime and prior_regime != comb_regime:
            lines.insert(2, f"_(was {prior_regime})_")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print(f"  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "data_date": latest["date"],
        "dix_pct": round(dix_pct, 2),
        "gex_billions": round(gex_b, 3),
        "dix_regime": dix_regime, "combined_regime": comb_regime,
        "regime_changed": prior_regime != comb_regime if prior_regime else False,
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
