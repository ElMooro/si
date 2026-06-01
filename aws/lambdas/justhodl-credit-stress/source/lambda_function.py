"""
justhodl-credit-stress — Credit Spread Engine (BUILD 8/15)

WHY THIS EXISTS
===============
Credit spreads are the most predictive single signal for risk-asset stress.
Bloomberg's credit terminal costs ~$24k/yr. This builds the primitive from
FRED's free ICE BofA OAS series.

When IG and HY both widen sharply, equities follow with a 2-8 week lag.
The current readout is the institutional baseline for "is credit pricing
in the same risk equity is pricing in?"

SERIES (12)
===========
US Corporate IG:
  BAMLC0A0CM      ICE BofA US Corporate IG OAS (master)
  BAMLC0A1CAAA    AAA OAS (highest grade)
  BAMLC0A2CAA     AA OAS
  BAMLC0A3CA      A OAS
  BAMLC0A4CBBB    BBB OAS (lowest IG — most cycle-sensitive)
US High Yield:
  BAMLH0A0HYM2    HY Master OAS
  BAMLH0A1HYBB    BB OAS (mid-tier junk)
  BAMLH0A2HYB     B OAS
  BAMLH0A3HYC     CCC OAS (deep distress signal)
Emerging Markets:
  BAMLEMCBPIOAS   EM Corporate IG OAS
  BAMLEMHBHYCRPIOAS EM Corporate HY OAS
Cross-check:
  T10Y2Y          10y-2y curve (recession leading indicator)

METRICS
=======
Per series:
  Current bps
  60d/252d moving averages
  60d z-score
  1y, 5y, all-time percentile rank
  Day-over-day change
Composite:
  BBB-AAA spread (quality dispersion within IG)
  CCC-BB spread (quality dispersion within HY)
  HY-IG spread (the canonical risk-on/off macro spread)
  EM HY vs US HY relative (EM premium)

REGIMES
=======
Per HY:
  <300 bps  BENIGN_RISK_ON
  300-450   NEUTRAL
  450-600   STRESSED_WIDENING
  600-800   STRESS_ELEVATED
  >800      CRISIS

Composite considering quality dispersion and trend:
  BENIGN, MELTUP_PRONE
  CONSTRUCTIVE
  NEUTRAL_WATCHING
  STRESS_BUILDING
  STRESS_ACUTE
  CRISIS_REGIME

OUTPUT
======
data/credit-stress.json — current bps, regime, z-scores, history meta
data/credit-stress-history.json — full series for charting

SCHEDULE
========
cron(0 20 ? * MON-FRI *) — daily 20:00 UTC = 4PM ET (after FRED publishes)
"""
import io, json, os, time, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/credit-stress.json"
HISTORY_KEY = "data/credit-stress-history.json"

FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 20
MAX_PARALLEL = 6

SERIES = {
    # US IG
    "BAMLC0A0CM": {"name": "US IG Master", "tier": "ig", "rating": "Master"},
    "BAMLC0A1CAAA": {"name": "AAA", "tier": "ig", "rating": "AAA"},
    "BAMLC0A2CAA": {"name": "AA", "tier": "ig", "rating": "AA"},
    "BAMLC0A3CA": {"name": "A", "tier": "ig", "rating": "A"},
    "BAMLC0A4CBBB": {"name": "BBB", "tier": "ig", "rating": "BBB"},
    # US HY
    "BAMLH0A0HYM2": {"name": "US HY Master", "tier": "hy", "rating": "Master"},
    "BAMLH0A1HYBB": {"name": "BB", "tier": "hy", "rating": "BB"},
    "BAMLH0A2HYB": {"name": "B", "tier": "hy", "rating": "B"},
    "BAMLH0A3HYC": {"name": "CCC", "tier": "hy", "rating": "CCC"},
    # EM
    "BAMLEMCBPIOAS": {"name": "EM IG", "tier": "em", "rating": "IG"},
    "BAMLEMHBHYCRPIOAS": {"name": "EM HY", "tier": "em", "rating": "HY"},
    # Cross-check
    "T10Y2Y": {"name": "10y-2y Curve", "tier": "rates", "rating": "—"},
}

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
    if not xs: return None
    sorted_xs = sorted(xs)
    below = sum(1 for x in sorted_xs if x < value)
    return below / len(sorted_xs)


# ═══════════════════════════════════════════════════════════════════════════
# FRED FETCH
# ═══════════════════════════════════════════════════════════════════════════

def fetch_fred_series(series_id, n_years=10):
    """Returns sorted list of {date, value} dicts. Skips missing."""
    end = datetime.now(timezone.utc).date()
    start = end.replace(year=end.year - n_years)
    url = (f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
            f"&observation_start={start.isoformat()}"
            f"&observation_end={end.isoformat()}")
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
        with urllib.request.urlopen(req, timeout=HTTP_TIMEOUT) as r:
            data = json.loads(r.read().decode("utf-8"))
        obs = data.get("observations", [])
        out = []
        for o in obs:
            try:
                v = float(o.get("value", "."))
                out.append({"date": o["date"], "value": v})
            except (ValueError, TypeError):
                continue
        out.sort(key=lambda x: x["date"])
        return out
    except Exception as e:
        print(f"  fred {series_id} err: {str(e)[:80]}")
        return []


# ═══════════════════════════════════════════════════════════════════════════
# PER-SERIES METRICS
# ═══════════════════════════════════════════════════════════════════════════

def compute_series_metrics(series_id, rows):
    if not rows or len(rows) < 60:
        return {"err": f"insufficient data ({len(rows)} rows)"}

    values = [r["value"] for r in rows]
    latest = rows[-1]
    prev = rows[-2] if len(rows) >= 2 else None

    ma_60 = _mean(values[-60:])
    ma_252 = _mean(values[-252:]) if len(values) >= 252 else None
    sd_60 = _stdev(values[-60:])
    z_60 = (latest["value"] - ma_60) / sd_60 if sd_60 > 0 else 0

    pct_1y = _percentile_rank(values[-252:], latest["value"]) if len(values) >= 252 else None
    pct_5y = _percentile_rank(values[-252*5:], latest["value"]) if len(values) >= 252*5 else None
    pct_all = _percentile_rank(values, latest["value"])

    dod = latest["value"] - (prev["value"] if prev else latest["value"])

    return {
        "current": round(latest["value"], 2),
        "date": latest["date"],
        "dod_change_bps": round(dod * 100, 1),  # bps move
        "ma_60d": round(ma_60, 2),
        "ma_252d": round(ma_252, 2) if ma_252 is not None else None,
        "z_score_60d": round(z_60, 2),
        "pct_1y": round(pct_1y * 100, 1) if pct_1y is not None else None,
        "pct_5y": round(pct_5y * 100, 1) if pct_5y is not None else None,
        "pct_all_time": round(pct_all * 100, 1),
        "n_history_days": len(rows),
    }


# ═══════════════════════════════════════════════════════════════════════════
# REGIME LOGIC
# ═══════════════════════════════════════════════════════════════════════════

def classify_hy_regime(hy_master_pct):
    """Tier-1 HY regime by absolute level (in percent, not bps)."""
    if hy_master_pct < 3.0: return "BENIGN_RISK_ON"
    if hy_master_pct < 4.5: return "NEUTRAL"
    if hy_master_pct < 6.0: return "STRESSED_WIDENING"
    if hy_master_pct < 8.0: return "STRESS_ELEVATED"
    return "CRISIS"


def classify_ig_regime(ig_master_pct):
    if ig_master_pct < 1.0: return "TIGHT"
    if ig_master_pct < 1.5: return "NEUTRAL"
    if ig_master_pct < 2.0: return "WIDENING"
    if ig_master_pct < 3.0: return "STRESSED"
    return "CRISIS"


def composite_regime(metrics, derived):
    hy_pct = (metrics.get("BAMLH0A0HYM2") or {}).get("current")
    ig_pct = (metrics.get("BAMLC0A0CM") or {}).get("current")
    bbb_aaa = derived.get("bbb_minus_aaa")
    ccc_bb = derived.get("ccc_minus_bb")
    hy_z = (metrics.get("BAMLH0A0HYM2") or {}).get("z_score_60d") or 0
    hy_dod = (metrics.get("BAMLH0A0HYM2") or {}).get("dod_change_bps") or 0
    curve = (metrics.get("T10Y2Y") or {}).get("current")

    if hy_pct is None or ig_pct is None:
        return "UNKNOWN", "Missing key series — cannot classify"

    # Tier 1: extreme cases
    if hy_pct >= 8.0:
        return "CRISIS_REGIME", f"HY OAS {hy_pct:.2f}% in crisis territory (>8%)"
    if hy_pct >= 6.0:
        return "STRESS_ACUTE", f"HY OAS {hy_pct:.2f}% acute stress; equities historically follow with 2-4 week lag"
    if hy_pct >= 4.5 and hy_z >= 1.5:
        return "STRESS_BUILDING", f"HY OAS {hy_pct:.2f}% z-score +{hy_z:.1f} — credit accelerating wider"

    # Tier 2: benign or constructive
    if hy_pct < 3.0 and hy_z < -1.0:
        return "MELTUP_PRONE", f"HY OAS {hy_pct:.2f}% (very tight) and compressing — credit complacency, melt-up risk"
    if hy_pct < 3.5 and (bbb_aaa is None or bbb_aaa < 1.0):
        return "BENIGN", f"HY OAS {hy_pct:.2f}% benign · quality dispersion tight · risk-on confirmed"
    if hy_pct < 4.5:
        return "CONSTRUCTIVE", f"HY OAS {hy_pct:.2f}% in healthy range · credit supportive of equities"

    # Curve as secondary
    if curve is not None and curve < 0:
        return "NEUTRAL_WATCHING", f"HY OAS {hy_pct:.2f}% neutral but 2y-10y inverted ({curve:.2f}%) — recession signal active"

    return "NEUTRAL_WATCHING", f"HY OAS {hy_pct:.2f}% mid-range · monitor for trend continuation"


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
        print(f"  tg err: {str(e)[:80]}")
        return False


# ═══════════════════════════════════════════════════════════════════════════
# HANDLER
# ═══════════════════════════════════════════════════════════════════════════

def lambda_handler(event, context):
    started = time.time()
    print(f"=== credit-stress v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    # Load prior state for regime change detection
    try:
        prior_payload = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)["Body"].read())
        prior_regime = prior_payload.get("composite_regime")
    except Exception:
        prior_regime = None

    # ─── Parallel FRED fetch ───
    raw_series = {}
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futures = {ex.submit(fetch_fred_series, sid): sid for sid in SERIES}
        for f in as_completed(futures):
            sid = futures[f]
            raw_series[sid] = f.result()

    # ─── Per-series metrics ───
    metrics = {}
    for sid, rows in raw_series.items():
        m = compute_series_metrics(sid, rows)
        m["meta"] = SERIES[sid]
        metrics[sid] = m
        if not m.get("err"):
            print(f"  ✓ {sid:25s} {SERIES[sid]['name']:18s} "
                  f"{m['current']:.2f}% (z={m.get('z_score_60d'):+.2f} · pct1y={m.get('pct_1y')})")

    # ─── Derived spreads ───
    def get_curr(sid):
        m = metrics.get(sid) or {}
        return m.get("current")

    bbb = get_curr("BAMLC0A4CBBB")
    aaa = get_curr("BAMLC0A1CAAA")
    bbb_minus_aaa = round(bbb - aaa, 2) if bbb is not None and aaa is not None else None
    ccc = get_curr("BAMLH0A3HYC")
    bb = get_curr("BAMLH0A1HYBB")
    ccc_minus_bb = round(ccc - bb, 2) if ccc is not None and bb is not None else None
    hy = get_curr("BAMLH0A0HYM2")
    ig = get_curr("BAMLC0A0CM")
    hy_minus_ig = round(hy - ig, 2) if hy is not None and ig is not None else None
    em_hy = get_curr("BAMLEMHBHYCRPIOAS")
    em_hy_minus_us_hy = round(em_hy - hy, 2) if em_hy is not None and hy is not None else None

    derived = {
        "bbb_minus_aaa": bbb_minus_aaa,  # quality dispersion within IG
        "ccc_minus_bb": ccc_minus_bb,    # quality dispersion within HY
        "hy_minus_ig": hy_minus_ig,      # canonical credit risk premium
        "em_hy_minus_us_hy": em_hy_minus_us_hy,  # EM-DM spread
    }

    # ─── Regimes ───
    hy_regime = classify_hy_regime(hy) if hy is not None else "UNKNOWN"
    ig_regime = classify_ig_regime(ig) if ig is not None else "UNKNOWN"
    comp_regime, comp_signal = composite_regime(metrics, derived)
    regime_changed = (prior_regime != comp_regime) if prior_regime else False

    # ─── Build payload ───
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "elapsed_seconds": round(time.time() - started, 2),
        "data_date": (metrics.get("BAMLH0A0HYM2") or {}).get("date"),
        "current_bps": {  # convenient flat current values
            sid: m.get("current") for sid, m in metrics.items()
        },
        "metrics": metrics,
        "derived_spreads": derived,
        "regimes": {
            "hy_regime": hy_regime,
            "ig_regime": ig_regime,
            "composite_regime": comp_regime,
            "composite_signal": comp_signal,
        },
        "composite_regime": comp_regime,
        "composite_signal": comp_signal,
        "regime_changed_from_prior": regime_changed,
        "thresholds": {
            "hy_benign": 3.0, "hy_stressed": 4.5, "hy_acute": 6.0, "hy_crisis": 8.0,
            "ig_tight": 1.0, "ig_widening": 1.5, "ig_stressed": 2.0, "ig_crisis": 3.0,
        },
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ credit-stress.json written")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # ─── History sidecar for charting ───
    try:
        # Trim to last 5 years for chart performance (~1260 days)
        first_idx = max(0, len(raw_series.get("BAMLH0A0HYM2", [])) - 1260)
        ref_dates = [r["date"] for r in raw_series.get("BAMLH0A0HYM2", [])[first_idx:]]
        chart = {
            "generated_at": payload["generated_at"],
            "n_days": len(ref_dates),
            "first_date": ref_dates[0] if ref_dates else None,
            "last_date": ref_dates[-1] if ref_dates else None,
            "dates": ref_dates,
            "series": {},
        }
        for sid in SERIES:
            rows = raw_series.get(sid, [])
            if not rows: continue
            # Align to reference dates
            d2v = {r["date"]: r["value"] for r in rows}
            chart["series"][sid] = [round(d2v[d], 3) if d in d2v else None for d in ref_dates]
        s3.put_object(Bucket=S3_BUCKET, Key=HISTORY_KEY,
            Body=json.dumps(chart, separators=(",", ":")).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=3600")
        print(f"  ✓ credit-stress-history.json written ({len(ref_dates)}d)")
    except Exception as e:
        print(f"  history put err: {str(e)[:120]}")

    # ─── Telegram on regime change or extreme readings ───
    alert_sent = False
    if regime_changed or comp_regime in ("CRISIS_REGIME", "STRESS_ACUTE", "MELTUP_PRONE"):
        lines = [f"💳 *Credit Stress · {datetime.now(timezone.utc).strftime('%b %d')}*\n",
                  f"⚡ {comp_regime}",
                  f"_{comp_signal}_\n",
                  f"📊 HY OAS: *{hy:.2f}%* (z={(metrics.get('BAMLH0A0HYM2') or {}).get('z_score_60d',0):+.2f})",
                  f"📊 IG OAS: *{ig:.2f}%*",
                  f"⚖️  HY-IG: {hy_minus_ig:.2f}% · BBB-AAA: {bbb_minus_aaa:.2f}%",
                  f"💎 CCC-BB: {ccc_minus_bb:.2f}% (deep HY dispersion)"]
        if prior_regime and prior_regime != comp_regime:
            lines.insert(2, f"_(was {prior_regime})_")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "data_date": payload["data_date"],
        "hy_oas_pct": hy, "ig_oas_pct": ig,
        "hy_regime": hy_regime, "ig_regime": ig_regime,
        "composite_regime": comp_regime,
        "regime_changed": regime_changed,
        "alert_sent": alert_sent,
        "n_series_loaded": sum(1 for m in metrics.values() if not m.get("err")),
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
