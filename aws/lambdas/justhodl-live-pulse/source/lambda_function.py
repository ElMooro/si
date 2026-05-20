"""
justhodl-live-pulse -- the fast intraday stress layer.

The batch engines (GSI, dollar radar, vol radar, signal board) update
on 3-6 hour cadences. Between batches, a lot can happen -- FOMC, CPI,
geopolitical shocks, flash dislocations. The Live Pulse is the
intraday layer: every 15 minutes during US market hours it polls
live quotes for the four canonical stress proxies and computes a
fast 0-100 LIVE PULSE.

Components and weights:
  - SPY intraday move + absolute drawdown from prior close   (35%)
  - VIX level + intraday delta                                (30%)
  - DXY intraday move                                         (15%)
  - MOVE bond volatility                                      (20%)

The pulse is calibrated so:
  - 0-30   "BENIGN" intraday tape
  - 30-55  "WATCHFUL"
  - 55-75  "PRESSURE" (worth checking the desk)
  - 75-100 "EVENT" (something is happening NOW)

DRIFT vs BATCH:
The engine compares the current pulse against the morning's batch
GSI. If pulse > GSI + 15, intraday risk has built up materially since
the morning read; if pulse < GSI - 15, intraday risk has receded.
This is the institutional question: 'has the tape changed since I
last looked?'

Outputs:
  - data/live-pulse.json  -- current snapshot, 60s S3 cache
  - data/live-pulse-history.json -- rolling 24h ring (~96 obs at 15m)
  - Telegram tripwire on EVENT level (75+) or +15 drift vs morning GSI

Runs every 15 minutes during US market hours (cron */15 13-21 UTC).
"""
import json
import os
import time
import urllib.request
import urllib.error
from datetime import datetime, timezone, timedelta

import boto3

S3_BUCKET = "justhodl-dashboard-live"
OUT_KEY = "data/live-pulse.json"
HIST_KEY = "data/live-pulse-history.json"
GSI_KEY = "data/global-stress.json"
SIGBOARD_KEY = "data/signal-board.json"

FMP_KEY = os.environ.get("FMP_API_KEY",
                         "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FMP_BASE = "https://financialmodelingprep.com/stable"
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HISTORY_BARS = 96   # 24h of 15-min snapshots
EVENT_THRESHOLD = 75
DRIFT_THRESHOLD = 15
ALERT_COOLDOWN_MIN = 30   # don't fire the same alert again within 30 min

# Component weights (must sum to 1.0)
W_SPY = 0.35
W_VIX = 0.30
W_DXY = 0.15
W_MOVE = 0.20

s3 = boto3.client("s3")


# ============== FMP quote fetcher =======================================
def fmp_quote(symbol):
    """Fetch a live FMP /stable/quote. Returns dict with price,
    changesPercentage, dayLow, dayHigh, previousClose, volume."""
    url = "%s/quote?symbol=%s&apikey=%s" % (FMP_BASE, symbol, FMP_KEY)
    try:
        req = urllib.request.Request(url, headers={"User-Agent": "jh-live"})
        with urllib.request.urlopen(req, timeout=10) as r:
            data = json.loads(r.read())
            if isinstance(data, list) and data:
                q = data[0]
                return {
                    "symbol": symbol,
                    "price": q.get("price"),
                    "change_pct": q.get("changesPercentage"),
                    "day_low": q.get("dayLow"),
                    "day_high": q.get("dayHigh"),
                    "prev_close": q.get("previousClose"),
                    "volume": q.get("volume"),
                    "ts": datetime.now(timezone.utc).isoformat(),
                }
    except Exception as e:
        print("fmp_quote %s fail: %s" % (symbol, e))
    return None


# ============== I/O helpers =============================================
def read_json(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET,
                                        Key=key)["Body"].read())
    except Exception:
        return {}


def write_json(key, payload, cache_seconds=60):
    s3.put_object(Bucket=S3_BUCKET, Key=key,
                  Body=json.dumps(payload,
                                  default=str).encode("utf-8"),
                  ContentType="application/json",
                  CacheControl="public, max-age=%d" % cache_seconds)


def send_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        return
    try:
        url = "https://api.telegram.org/bot%s/sendMessage" % TELEGRAM_TOKEN
        data = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                           "parse_mode": "HTML"}).encode("utf-8")
        urllib.request.urlopen(urllib.request.Request(
            url, data=data, headers={"Content-Type": "application/json"}),
            timeout=8).read()
    except Exception as e:
        print("telegram fail: %s" % e)


def clamp(x, lo, hi):
    return max(lo, min(hi, x))


# ============== component scorers =======================================
def score_spy(q):
    """SPY: bigger drop = more stress. The score combines:
       - intraday change % (a 1.5% drop -> ~80)
       - position within day range (close at the lows -> bonus)"""
    if not q or q.get("price") is None or q.get("prev_close") is None:
        return None, {}
    price = q["price"]
    prev = q["prev_close"]
    ret_pct = (price - prev) / prev * 100.0 if prev else 0.0
    # negative returns drive stress; clamp to -3..+3% range
    ret_score = clamp(-ret_pct / 0.025, -100, 100)
    # position in range: at day low = max stress; at day high = min
    lo, hi = q.get("day_low"), q.get("day_high")
    range_score = 0
    if lo and hi and hi > lo:
        pos = (price - lo) / (hi - lo)   # 0 at lows, 1 at highs
        range_score = clamp((1 - pos) * 60, 0, 60)
    score = clamp(50 + ret_score * 0.7 + range_score * 0.3, 0, 100)
    return score, {"ret_pct": round(ret_pct, 3),
                   "price": price, "prev_close": prev,
                   "day_low": lo, "day_high": hi}


def score_vix(q):
    """VIX: the absolute level is the dominant input. 12-15 calm,
    20-25 elevated, 30+ stressed, 40+ acute. Intraday delta adds."""
    if not q or q.get("price") is None:
        return None, {}
    vix = q["price"]
    prev = q.get("prev_close") or vix
    delta = vix - prev
    # absolute-level score
    if vix < 12:
        level_score = 5
    elif vix < 15:
        level_score = 20
    elif vix < 20:
        level_score = 40
    elif vix < 25:
        level_score = 60
    elif vix < 30:
        level_score = 75
    elif vix < 40:
        level_score = 88
    else:
        level_score = 98
    delta_score = clamp(delta * 4, -20, 25)  # ~+25 for VIX +6 in a session
    score = clamp(level_score + delta_score, 0, 100)
    return score, {"vix": vix, "delta": round(delta, 2),
                   "level_score": level_score,
                   "delta_score": round(delta_score, 1)}


def score_dxy(q):
    """DXY: dollar pumps coincide with risk-off (Brent Johnson). A
    big intraday move EITHER direction is informative -- both bullish-
    USD (risk off / EM stress) and bearish-USD (forced unwind / Fed
    intervention narrative) can mean dislocation."""
    if not q or q.get("price") is None or q.get("prev_close") is None:
        return None, {}
    dxy = q["price"]
    prev = q["prev_close"]
    ret_pct = (dxy - prev) / prev * 100.0
    # absolute-value of move drives the score (both directions add stress)
    score = clamp(40 + abs(ret_pct) * 30, 0, 100)
    return score, {"dxy": dxy, "ret_pct": round(ret_pct, 3)}


def score_move(q):
    """MOVE: bond volatility. 80 calm, 100 elevated, 130+ stressed,
    180+ crisis."""
    if not q or q.get("price") is None:
        return None, {}
    move = q["price"]
    if move < 70:
        score = 10
    elif move < 90:
        score = 30
    elif move < 110:
        score = 50
    elif move < 130:
        score = 65
    elif move < 160:
        score = 80
    else:
        score = 95
    return score, {"move": move}


# ============== handler =================================================
def lambda_handler(event, context):
    t0 = time.time()
    now = datetime.now(timezone.utc)

    # ---- 1. live quotes -----------------------------------------------
    spy_q = fmp_quote("SPY")
    vix_q = fmp_quote("VIX")     # FMP carries ^VIX as VIX in /stable
    dxy_q = fmp_quote("DXY")     # ICE Dollar Index
    move_q = fmp_quote("MOVE")   # ICE BofA MOVE

    # ---- 2. component scores ------------------------------------------
    spy_score, spy_det = score_spy(spy_q)
    vix_score, vix_det = score_vix(vix_q)
    dxy_score, dxy_det = score_dxy(dxy_q)
    move_score, move_det = score_move(move_q)

    components = [
        ("spy", spy_score, W_SPY, spy_det),
        ("vix", vix_score, W_VIX, vix_det),
        ("dxy", dxy_score, W_DXY, dxy_det),
        ("move", move_score, W_MOVE, move_det),
    ]
    available = [(n, s, w, d) for n, s, w, d in components if s is not None]

    # weighted average over available components
    pulse = None
    if available:
        wsum = sum(w for _, _, w, _ in available)
        pulse = round(
            sum(s * w for _, s, w, _ in available) / wsum) if wsum else None

    # ---- 3. comparison vs morning batch -------------------------------
    gsi_doc = read_json(GSI_KEY)
    morning_gsi = gsi_doc.get("global_stress_index")
    drift = None
    if pulse is not None and isinstance(morning_gsi, (int, float)):
        drift = pulse - morning_gsi

    # ---- 4. level + posture -------------------------------------------
    if pulse is None:
        level, posture = "UNAVAILABLE", "UNAVAILABLE"
    elif pulse < 30:
        level, posture = "BENIGN", "intraday tape benign"
    elif pulse < 55:
        level, posture = "WATCHFUL", "modest intraday pressure"
    elif pulse < 75:
        level, posture = "PRESSURE", "active intraday pressure"
    else:
        level, posture = "EVENT", "intraday EVENT pulse"

    # ---- 5. assemble payload ------------------------------------------
    snapshot = {
        "as_of": now.isoformat(),
        "pulse": pulse,
        "level": level,
        "posture": posture,
        "drift_vs_morning_gsi": drift,
        "morning_gsi": morning_gsi,
        "components": [
            {"name": n, "score": s, "weight": w, "detail": d}
            for n, s, w, d in components
        ],
        "components_available": [n for n, s, _, _ in components if s is not None],
        "components_missing": [n for n, s, _, _ in components if s is None],
        "duration_s": round(time.time() - t0, 1),
        "methodology": (
            "Weighted average of four live components: SPY (35%%), VIX "
            "(30%%), MOVE (20%%), DXY (15%%). Each component is scored "
            "to 0-100 from its intraday move + level. The pulse is "
            "compared to the morning batch GSI to surface intraday "
            "DRIFT -- positive drift = intraday pressure built since "
            "the morning read; negative = pressure receded. Posted "
            "every 15 min during US market hours."),
    }

    # ---- 6. history ring -----------------------------------------------
    hist = read_json(HIST_KEY)
    rows = hist.get("snapshots") or []
    rows.append({"ts": now.isoformat(), "pulse": pulse, "level": level,
                 "drift": drift,
                 "components": {n: s for n, s, _, _ in components}})
    rows = rows[-HISTORY_BARS:]
    write_json(HIST_KEY, {"snapshots": rows, "cap": HISTORY_BARS},
               cache_seconds=120)

    # short trail for the dashboard inline
    snapshot["history_trail"] = rows[-32:]

    write_json(OUT_KEY, snapshot, cache_seconds=60)

    # ---- 7. tripwire ---------------------------------------------------
    prev = hist.get("last_alert") or {}
    prev_ts = prev.get("ts")
    fire_event = (pulse is not None and pulse >= EVENT_THRESHOLD)
    fire_drift = (drift is not None and abs(drift) >= DRIFT_THRESHOLD
                  and pulse is not None and pulse >= 50)
    if fire_event or fire_drift:
        skip = False
        if prev_ts:
            try:
                t_prev = datetime.fromisoformat(prev_ts.replace("Z", "+00:00"))
                if now - t_prev < timedelta(minutes=ALERT_COOLDOWN_MIN):
                    skip = True
            except Exception:
                pass
        if not skip:
            reasons = []
            if fire_event:
                reasons.append("pulse <b>%d</b> = EVENT" % pulse)
            if fire_drift:
                reasons.append(
                    "drift vs morning GSI <b>%+d</b>" % drift)
            send_telegram(
                "\u26A1 <b>Live Pulse</b> -- %s. %s. %s." % (
                    level, ", ".join(reasons),
                    "SPY %.2f%%, VIX %.1f, DXY %.2f%%, MOVE %s" % (
                        (spy_det.get("ret_pct") if spy_det else 0) or 0,
                        (vix_det.get("vix") if vix_det else 0) or 0,
                        (dxy_det.get("ret_pct") if dxy_det else 0) or 0,
                        (move_det.get("move") if move_det else "n/a"))))
            # remember the alert so we cool down
            write_json(HIST_KEY, {"snapshots": rows,
                                  "cap": HISTORY_BARS,
                                  "last_alert": {"ts": now.isoformat(),
                                                 "pulse": pulse,
                                                 "drift": drift,
                                                 "level": level}},
                       cache_seconds=120)

    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "pulse": pulse, "level": level,
        "drift": drift, "morning_gsi": morning_gsi,
        "components_available": [n for n, s, _, _ in components if s is not None],
        "elapsed_s": round(time.time() - t0, 1)})}
