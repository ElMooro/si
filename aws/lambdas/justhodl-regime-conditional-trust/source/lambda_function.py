"""
justhodl-regime-conditional-trust — WHICH ENGINES WORK IN WHICH REGIME
═══════════════════════════════════════════════════════════════════════════════════════════════
An engine's average edge is a blend across regimes that can hide the truth: a crisis engine may be
brilliant in BROAD RISK-OFF and useless in a melt-up; a breadth engine the reverse. This engine learns
that conditioning from REAL graded outcomes. It reconstructs the historical dispersion regime
(the Risk Map's exact classifier, applied to price history week-by-week — no look-ahead, no new data),
buckets every engine's net-of-cost excess-vs-SPY by the regime that was live the day it fired, and
computes each engine's conditional win-rate (Wilson lower-bound) per regime. It then reads the CURRENT
dispersion regime from the live Risk Map and emits a per-engine regime factor — so the proven-alpha
book and the Strategist (via engine-trust) lean on the engines actually suited to the tape we are in.

Output: data/regime-conditional-trust.json. Consumed by justhodl-engine-trust (the fleet trust gate).
"""
import json
import math
import os
import time
import urllib.request
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone, timedelta

import boto3

REGION = "us-east-1"
BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
POLY = os.environ.get("POLYGON_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
S3 = boto3.client("s3", REGION)
DDB = boto3.resource("dynamodb", REGION)
OUTCOMES_TABLE = "justhodl-outcomes"
OUT_KEY = "data/regime-conditional-trust.json"
COST_RT_PCT = 0.30
MIN_REGIME_N = 6
UP = {"UP", "LONG", "BULLISH", "BUY", "POSITIVE", "BULL"}
DOWN = {"DOWN", "SHORT", "BEARISH", "SELL", "NEGATIVE", "BEAR"}

# same universe + classifier as the Risk Map (so reconstructed history matches live labels)
EQUITY = ["SPY", "RSP", "IWM", "MDY", "MAGS", "IWF", "IWD", "XLK", "XLC", "XLY", "XLF", "XLI",
          "XLE", "XLB", "XLV", "XLP", "XLU", "XLRE", "MTUM", "QUAL", "USMV", "VLUE"]
CRYPTO = ["BTCUSD", "ETHUSD", "SOLUSD"]
ALLTK = EQUITY + CRYPTO


def num(x):
    try:
        return float(x)
    except Exception:
        return None


def get_field(o, key):
    if key in o:
        return o[key]
    for sub in ("outcome", "prices", "data"):
        d = o.get(sub)
        if isinstance(d, dict) and key in d:
            return d[key]
    return None


def dpart(s):
    if not s:
        return None
    return str(s)[:10]


def clamp(v, lo, hi):
    return max(lo, min(hi, v))


def mean(x):
    return sum(x) / len(x) if x else 0.0


def wilson_lb(wins, n, z=1.96):
    if n == 0:
        return None
    p = wins / n
    return (p + z * z / (2 * n) - z * math.sqrt((p * (1 - p) + z * z / (4 * n)) / n)) / (1 + z * z / n)


def fetch(t):
    pre = "X:" if t in CRYPTO else ""
    frm = (datetime.now(timezone.utc) - timedelta(days=900)).strftime("%Y-%m-%d")
    to = datetime.now(timezone.utc).strftime("%Y-%m-%d")
    url = f"https://api.polygon.io/v2/aggs/ticker/{pre}{t}/range/1/day/{frm}/{to}?adjusted=true&sort=asc&limit=1000&apiKey={POLY}"
    try:
        r = json.loads(urllib.request.urlopen(urllib.request.Request(url, headers={"User-Agent": "jh"}), timeout=25).read())
        return t, {x["t"]: x["c"] for x in r.get("results", []) if x.get("c")}
    except Exception:
        return t, {}


def score_at(c, i):
    if i < 130:
        return None
    last = c[i]
    r21 = last / c[i - 21] - 1
    r63 = last / c[i - 63] - 1
    r126 = last / c[i - 126] - 1
    ma50 = mean(c[i - 49:i + 1])
    ma200 = mean(c[i - 199:i + 1]) if i >= 199 else mean(c[:i + 1])
    pts = (1 if last > ma50 else 0) + (1 if last > ma200 else 0) + (1 if ma50 > ma200 else 0)
    trend = (pts - 1.5) / 1.5
    mom = 0.45 * r63 + 0.30 * r126 + 0.25 * r21
    return clamp(mom * 100 * 2.2 + trend * 25, -100, 100)


def classify(eq_avg, crypto_avg, breadth, conc):
    if eq_avg > 8 and crypto_avg < -15:
        return "BIFURCATED"
    if eq_avg > 8 and breadth >= 62 and conc < 4:
        return "BROAD RISK-ON"
    if eq_avg > 8 and (breadth < 45 or conc >= 5):
        return "NARROW / CONCENTRATED"
    if eq_avg < -8:
        return "BROAD RISK-OFF"
    return "MIXED / ROTATIONAL"


def reconstruct_regimes(series, dates):
    """date -> dispersion regime, computed weekly from trailing windows (no look-ahead)."""
    arr = {t: [series[t][d] for d in dates] for t in ALLTK if series.get(t)}
    have = [t for t in ALLTK if t in arr and len(arr[t]) == len(dates)]
    eq = [t for t in EQUITY if t in have]
    cr = [t for t in CRYPTO if t in have]
    out = {}
    last_label = None
    for i in range(len(dates)):
        if i < 200 or i % 5 != 0:        # weekly anchors after 200d warmup
            if last_label:
                out[dates[i]] = last_label
            continue
        eq_scores = [s for s in (score_at(arr[t], i) for t in eq) if s is not None]
        cr_scores = [s for s in (score_at(arr[t], i) for t in cr) if s is not None]
        if not eq_scores:
            continue
        eq_avg = mean(eq_scores)
        crypto_avg = mean(cr_scores) if cr_scores else 0.0
        breadth = 100 * sum(1 for s in eq_scores if s > 0) / len(eq_scores)
        spy_a, rsp_a = arr.get("SPY"), arr.get("RSP")
        conc = ((spy_a[i] / spy_a[i - 63] - 1) - (rsp_a[i] / rsp_a[i - 63] - 1)) * 100 if (spy_a and rsp_a and i >= 63) else 0.0
        last_label = classify(eq_avg, crypto_avg, breadth, round(conc, 1))
        out[dates[i]] = last_label
    return out


def lambda_handler(event=None, context=None):
    t0 = time.time()
    with ThreadPoolExecutor(max_workers=18) as ex:
        series = dict(ex.map(fetch, ALLTK))
    if not series.get("SPY"):
        return {"statusCode": 500, "body": "no SPY"}
    common = set(series["SPY"].keys())
    for t in ALLTK:
        if series.get(t):
            common &= set(series[t].keys())
    dates = sorted(common)
    regimes_ts = reconstruct_regimes(series, dates)
    regimes_iso = {datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d"): lab
                   for ms, lab in regimes_ts.items()}
    reg_dates = sorted(regimes_iso)

    def regime_on(date_str):
        best = None
        for d in reg_dates:
            if d <= date_str:
                best = regimes_iso[d]
            else:
                break
        return best

    # ── scan outcomes ──
    table = DDB.Table(OUTCOMES_TABLE)
    rows, kw, scanned = [], {}, 0
    while True:
        r = table.scan(**kw)
        rows.extend(r.get("Items", []))
        scanned += len(r.get("Items", []))
        if "LastEvaluatedKey" not in r or scanned > 80000:
            break
        kw["ExclusiveStartKey"] = r["LastEvaluatedKey"]

    # iso-date SPY map for excess
    spy_iso = _iso_close(series["SPY"])

    def spy_on(date_str):
        # find SPY close on/just after date_str
        if date_str in spy_iso:
            return spy_iso[date_str]
        for d in sorted(spy_iso):
            if d >= date_str:
                return spy_iso[d]
        return None

    buckets = defaultdict(lambda: defaultdict(lambda: {"wins": 0, "n": 0, "sum_excess": 0.0}))
    overall = defaultdict(lambda: {"wins": 0, "n": 0, "sum_excess": 0.0})
    for o in rows:
        eng = o.get("signal_type")
        if not eng:
            continue
        pdr = str(o.get("predicted_dir") or "").strip().upper()
        dm = 1.0 if pdr in UP else (-1.0 if pdr in DOWN else 0.0)
        if dm == 0.0:
            continue
        p_sig, p_chk = num(get_field(o, "price_at_signal")), num(get_field(o, "price_at_check"))
        d_sig, d_chk = dpart(o.get("logged_at")), dpart(o.get("checked_at"))
        if not (p_sig and p_chk and p_sig > 0 and d_sig and d_chk):
            continue
        asset_ret = (p_chk / p_sig - 1) * 100
        ss, sc = spy_on(d_sig), spy_on(d_chk)
        spy_ret = (sc / ss - 1) * 100 if ss and sc and ss > 0 else 0.0
        net_excess = dm * (asset_ret - spy_ret) - COST_RT_PCT
        reg = regime_on(d_sig)
        win = 1 if net_excess > 0 else 0
        if reg:
            b = buckets[eng][reg]
            b["wins"] += win; b["n"] += 1; b["sum_excess"] += net_excess
        ov = overall[eng]
        ov["wins"] += win; ov["n"] += 1; ov["sum_excess"] += net_excess

    # current regime from the live Risk Map
    try:
        rmap = json.loads(S3.get_object(Bucket=BUCKET, Key="data/regime-map.json")["Body"].read())
        current = (rmap.get("regime") or {}).get("label")
    except Exception:
        current = None

    engines_out = {}
    for eng, ov in overall.items():
        if ov["n"] < 8:
            continue
        ov_lb = wilson_lb(ov["wins"], ov["n"])
        by = {}
        for reg, b in buckets[eng].items():
            if b["n"] >= 3:
                by[reg] = {"n": b["n"], "win_rate": round(b["wins"] / b["n"], 3),
                           "wilson_lb": round(wilson_lb(b["wins"], b["n"]), 3),
                           "mean_excess_pct": round(b["sum_excess"] / b["n"], 3)}
        # current-regime factor (same shape engine-trust uses)
        factor, status = 1.0, "NO_REGIME_DATA"
        cur = by.get(current) if current else None
        if cur and cur["n"] >= MIN_REGIME_N and ov_lb is not None:
            delta = cur["wilson_lb"] - ov_lb
            if delta <= -0.10:
                factor, status = 0.55, "MUCH WORSE IN REGIME"
            elif delta <= -0.04:
                factor, status = 0.80, "WORSE IN REGIME"
            elif delta >= 0.10:
                factor, status = 1.20, "MUCH BETTER IN REGIME"
            elif delta >= 0.04:
                factor, status = 1.08, "BETTER IN REGIME"
            else:
                factor, status = 1.0, "NEUTRAL IN REGIME"
        engines_out[eng] = {"overall": {"n": ov["n"], "win_rate": round(ov["wins"] / ov["n"], 3),
                                        "wilson_lb": round(ov_lb, 3) if ov_lb else None,
                                        "mean_excess_pct": round(ov["sum_excess"] / ov["n"], 3)},
                            "by_regime": by, "current_regime_factor": factor, "current_regime_status": status}

    # ranking: best & worst suited to the CURRENT regime
    suited = sorted([(e, v) for e, v in engines_out.items() if v["current_regime_factor"] != 1.0 or current in v["by_regime"]],
                    key=lambda kv: -(kv[1]["by_regime"].get(current, {}).get("mean_excess_pct", -99) if current else -99))
    best = [{"engine": e, "regime_mean_excess_pct": v["by_regime"].get(current, {}).get("mean_excess_pct"),
             "n": v["by_regime"].get(current, {}).get("n"), "factor": v["current_regime_factor"]}
            for e, v in suited[:12] if current in v["by_regime"]]
    worst = [{"engine": e, "regime_mean_excess_pct": v["by_regime"].get(current, {}).get("mean_excess_pct"),
              "n": v["by_regime"].get(current, {}).get("n"), "factor": v["current_regime_factor"]}
             for e, v in reversed(suited[-12:]) if current in v["by_regime"]]

    payload = {
        "engine": "justhodl-regime-conditional-trust", "version": "1.0.0", "ok": True,
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "thesis": "Learns each engine's conditional edge per dispersion regime from real outcomes; the live "
                  "Risk Map selects the regime, and engine-trust leans on the engines suited to it.",
        "current_regime": current, "n_engines": len(engines_out),
        "regime_distribution": _regime_hist(regimes_ts),
        "best_suited_to_current_regime": best, "worst_suited_to_current_regime": worst,
        "engines": engines_out,
        "method": "Risk-Map classifier reconstructed weekly over price history; per-engine net-of-cost "
                  "excess-vs-SPY bucketed by regime at signal date; Wilson-LB win-rate; factor vs overall.",
        "note": "Consumed by justhodl-engine-trust as the regime factor in effective_trust. Buckets with n<%d "
                "stay neutral (factor 1.0) — measure-before-trust." % MIN_REGIME_N,
        "elapsed_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, default=str).encode(),
                  ContentType="application/json", CacheControl="public, max-age=3600")
    print(f"[regime-cond-trust] engines={len(engines_out)} current={current} "
          f"best={[b['engine'] for b in best[:3]]} worst={[w['engine'] for w in worst[:3]]} {payload['elapsed_s']}s")
    return {"statusCode": 200, "body": json.dumps({"ok": True, "current_regime": current, "n_engines": len(engines_out),
            "n_best": len(best), "n_worst": len(worst)})}


# ── small date helpers (Polygon ms epoch -> iso) ──
def _iso_close(ms_map):
    out = {}
    for ms, c in ms_map.items():
        out[datetime.fromtimestamp(ms / 1000, timezone.utc).strftime("%Y-%m-%d")] = c
    return out


def _regime_hist(regimes_ts):
    h = defaultdict(int)
    for lab in regimes_ts.values():
        h[lab] += 1
    return dict(h)
