"""justhodl-leading-markets — the macro turning-point engine (v2).

Certain markets reliably lead global macro tops and bottoms. A global-macro
desk does NOT treat them as one flat list — it BUCKETS them by what each
canary front-runs, because each type leads a different macro force. And it
watches RELATIVE STRENGTH vs the global benchmark, not just absolute trend:
leaders underperforming while indices are still up is the divergence that
precedes tops.

THE 5 CANARY BUCKETS (19 markets via liquid US-listed ETFs)
═══════════════════════════════════════════════════════════
  RISK_APPETITE   FM, EEM, EWH
      the marginal risk capital — sentiment & liquidity; tops/bottoms first
  TRADE_CYCLE     EWT, EWY, EWG, EWN, EWS, VNM
      the exporters — global trade & the semiconductor cycle
      (Taiwan/Korea = tech, Germany/Netherlands = industry, Singapore/Vietnam
       = trade & supply-chain)
  COMMODITY_CYCLE ECH, EPU, EWA, COPX
      Chile/Peru/Australia + copper miners — China demand & the industrial
      cycle (Chile #1 copper, Peru copper+metals, Australia iron ore)
  CREDIT_STRESS   EWI, EWP
      Italy & Spain — European sovereign / financial-plumbing stress
  EUROPE_CORE     EWQ, EFNL, EWD, VGK
      France, Finland, Sweden, Europe-broad

Each market's ETF is classified EXPANSION / SLOWING / RECOVERY / CONTRACTION
from price trend + momentum, AND tagged outperforming/inline/lagging vs ACWI.
Each bucket gets its own diagnosis — so the turning-point signal tells you
WHICH macro force is flashing (trade cycle vs China demand vs credit vs
risk appetite).

Emits risk_score (crisis-composite) and expansion_breadth_pct +
turning_point_signal (Khalid Index). OUTPUT: data/leading-markets.json
Schedule: daily.
"""
import json, os, time
from datetime import datetime, timezone
from urllib import request, error
import boto3

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/leading-markets.json"
S3_HISTORY_KEY = "data/leading-markets-history.json"
HISTORY_MAX = 365

FMP_KEY = os.environ.get("FMP_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")

BENCHMARK = "ACWI"  # MSCI All-Country World — the relative-strength yardstick

# canary universe — ETF, market name, bucket
UNIVERSE = [
    ("FM",   "Frontier Mkts", "risk_appetite"),
    ("EEM",  "Emerging Mkts", "risk_appetite"),
    ("EWH",  "Hong Kong",     "risk_appetite"),
    ("EWT",  "Taiwan",        "trade_cycle"),
    ("EWY",  "South Korea",   "trade_cycle"),
    ("EWG",  "Germany",       "trade_cycle"),
    ("EWN",  "Netherlands",   "trade_cycle"),
    ("EWS",  "Singapore",     "trade_cycle"),
    ("VNM",  "Vietnam",       "trade_cycle"),
    ("ECH",  "Chile",         "commodity_cycle"),
    ("EPU",  "Peru",          "commodity_cycle"),
    ("EWA",  "Australia",     "commodity_cycle"),
    ("COPX", "Copper Miners", "commodity_cycle"),
    ("EWI",  "Italy",         "credit_stress"),
    ("EWP",  "Spain",         "credit_stress"),
    ("EWQ",  "France",        "europe_core"),
    ("EFNL", "Finland",       "europe_core"),
    ("EWD",  "Sweden",        "europe_core"),
    ("VGK",  "Europe",        "europe_core"),
]

BUCKET_LABEL = {
    "risk_appetite":   "Risk-appetite canaries",
    "trade_cycle":     "Trade & tech-cycle canaries",
    "commodity_cycle": "Commodity-cycle canaries",
    "credit_stress":   "Credit-stress canaries (Europe periphery)",
    "europe_core":     "Core Europe",
}
BUCKET_FLASH_READ = {
    "risk_appetite":   "risk capital is retreating — a sentiment / liquidity warning",
    "trade_cycle":     "global trade and the tech cycle are rolling over",
    "commodity_cycle": "China demand and the global industrial cycle are weakening",
    "credit_stress":   "European sovereign / credit stress is building",
    "europe_core":     "core Europe is contracting",
}

REGIME_HEALTH = {"EXPANSION": 100, "RECOVERY": 65, "SLOWING": 35, "CONTRACTION": 5}


def _get_json(url, timeout=15, retries=3):
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-Leading/2.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError):
            time.sleep(0.5 * (i + 1))
    return None


def fetch_history(sym, days=300):
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
           f"?symbol={sym}&apikey={FMP_KEY}")
    d = _get_json(url)
    rows = []
    if isinstance(d, dict) and "historical" in d:
        rows = d["historical"]
    elif isinstance(d, list):
        rows = d
    out = []
    for r in rows[:days + 5]:
        try:
            out.append({"date": r.get("date"),
                        "close": float(r.get("close") or r.get("adjClose") or 0)})
        except Exception:
            pass
    out.sort(key=lambda x: x["date"])
    return [h["close"] for h in out if h["close"] > 0]


def maybe_telegram(msg):
    if not TELEGRAM_TOKEN or not TELEGRAM_CHAT_ID:
        print(f"[tg] no creds: {msg[:80]}")
        return
    try:
        body = json.dumps({"chat_id": TELEGRAM_CHAT_ID, "text": msg,
                            "parse_mode": "HTML", "disable_web_page_preview": True}).encode("utf-8")
        req = request.Request(f"https://api.telegram.org/bot{TELEGRAM_TOKEN}/sendMessage",
                               data=body, headers={"Content-Type": "application/json"})
        request.urlopen(req, timeout=10).read()
    except Exception as e:
        print(f"[tg] err: {e}")


def sma(closes, n):
    return sum(closes[-n:]) / n if len(closes) >= n else None


def ret(closes, n):
    if len(closes) <= n:
        return None
    prior = closes[-n - 1]
    return (closes[-1] - prior) / prior * 100 if prior else None


def classify(closes):
    """One market -> EXPANSION / SLOWING / RECOVERY / CONTRACTION + metrics."""
    if len(closes) < 130:
        return None
    px = closes[-1]
    sma200 = sma(closes, 200) or sma(closes, len(closes) - 1)
    sma50 = sma(closes, 50)
    above_200 = sma200 is not None and px > sma200
    r1, r3, r6 = ret(closes, 21), ret(closes, 63), ret(closes, 126)
    hi = max(closes[-252:]) if len(closes) >= 252 else max(closes)
    from_high = (px - hi) / hi * 100 if hi else None
    sma50_prior = sma(closes[:-10], 50) if len(closes) > 60 else None
    sma50_rising = (sma50 is not None and sma50_prior is not None and sma50 > sma50_prior)
    m1, m3, m6 = (r1 or 0), (r3 or 0), (r6 or 0)

    if above_200 and m3 > 0 and m6 > 0:
        regime = "EXPANSION"
    elif above_200 and (m3 < 0 or not sma50_rising):
        regime = "SLOWING"
    elif (not above_200) and m1 > 0 and m3 > 0 and sma50_rising:
        regime = "RECOVERY"
    elif not above_200:
        regime = "CONTRACTION"
    else:
        regime = "EXPANSION"

    return {"price": round(px, 2), "above_200dma": above_200,
            "ret_1m_pct": round(r1, 2) if r1 is not None else None,
            "ret_3m_pct": round(r3, 2) if r3 is not None else None,
            "ret_6m_pct": round(r6, 2) if r6 is not None else None,
            "from_52w_high_pct": round(from_high, 1) if from_high is not None else None,
            "sma50_rising": sma50_rising, "regime": regime}


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[leading-markets] v2 starting {datetime.now(timezone.utc).isoformat()}")
    if not FMP_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FMP_KEY not set"})}

    # benchmark for relative strength
    bench = fetch_history(BENCHMARK)
    bench_r3 = ret(bench, 63) if bench else None
    bench_r1 = ret(bench, 21) if bench else None

    markets, failed = [], []
    for sym, name, bucket in UNIVERSE:
        closes = fetch_history(sym)
        cls = classify(closes)
        if cls is None:
            failed.append(sym)
            print(f"[leading] {sym} ({name}): insufficient data ({len(closes)})")
            continue
        rs3 = None
        if cls["ret_3m_pct"] is not None and bench_r3 is not None:
            rs3 = cls["ret_3m_pct"] - bench_r3
        rs_state = (None if rs3 is None else
                    "outperforming" if rs3 > 3 else
                    "lagging" if rs3 < -3 else "inline")
        cls.update({"etf": sym, "market": name, "bucket": bucket, "role": bucket,
                    "rs_3m_pct": round(rs3, 2) if rs3 is not None else None,
                    "rs_state": rs_state})
        markets.append(cls)
        print(f"[leading] {sym} ({name}/{bucket}): {cls['regime']} "
              f"3m={cls['ret_3m_pct']} rs={cls['rs_3m_pct']}")

    if not markets:
        return {"statusCode": 500, "body": json.dumps({"error": "no data", "failed": failed})}

    n = len(markets)
    counts = {"EXPANSION": 0, "SLOWING": 0, "RECOVERY": 0, "CONTRACTION": 0}
    for m in markets:
        counts[m["regime"]] += 1

    leading_score = sum(REGIME_HEALTH.get(m["regime"], 50) for m in markets) / n
    risk_score = round(100 - leading_score, 1)
    expansion_breadth_pct = round(counts["EXPANSION"] / n * 100, 1)
    contraction_pct = round(counts["CONTRACTION"] / n * 100, 1)
    improving_pct = round((counts["EXPANSION"] + counts["RECOVERY"]) / n * 100, 1)
    deteriorating_pct = round((counts["SLOWING"] + counts["CONTRACTION"]) / n * 100, 1)

    # ── per-bucket diagnosis ──
    buckets = {}
    flashing = []
    for bk in BUCKET_LABEL:
        bms = [m for m in markets if m["bucket"] == bk]
        if not bms:
            continue
        health = sum(REGIME_HEALTH.get(m["regime"], 50) for m in bms) / len(bms)
        bcounts = {}
        for m in bms:
            bcounts[m["regime"]] = bcounts.get(m["regime"], 0) + 1
        dominant = max(bcounts, key=bcounts.get)
        n_lagging = sum(1 for m in bms if m["rs_state"] == "lagging")
        is_flashing = health < 45
        if is_flashing:
            flashing.append(bk)
        buckets[bk] = {
            "label": BUCKET_LABEL[bk],
            "health": round(health, 1),
            "dominant_regime": dominant,
            "regime_counts": bcounts,
            "n_lagging_vs_acwi": n_lagging,
            "flashing": is_flashing,
            "members": [m["etf"] for m in bms],
        }

    # ── turning-point signal ──
    if counts["EXPANSION"] >= n * 0.6:
        signal = "EXPANSION_CONFIRMED"
        base = ("Most canary markets are expanding — broad global risk-on. The "
                "leaders confirm the uptrend.")
    elif counts["CONTRACTION"] >= n * 0.6:
        signal = "BROAD_CONTRACTION"
        base = ("Most canary markets are contracting — global risk-off is "
                "entrenched. Wait for RECOVERY readings before turning constructive.")
    elif counts["RECOVERY"] >= n * 0.3 and counts["RECOVERY"] >= counts["SLOWING"]:
        signal = "BOTTOM_SIGNAL"
        base = ("The canary markets are turning up off their lows — an early "
                "bottom signal. Leaders bottom before the broad market.")
    elif counts["SLOWING"] >= n * 0.35 and counts["SLOWING"] >= counts["RECOVERY"]:
        signal = "TOP_WARNING"
        base = ("The canary markets are rolling over while still above trend — "
                "an early top warning. Leaders top before the broad market.")
    else:
        signal = "MIXED"
        base = "The canary markets are mixed — no unified macro lead right now."

    if flashing:
        fl = "; ".join(BUCKET_FLASH_READ[b] for b in flashing)
        signal_read = f"{base} Flashing now: {fl}."
    else:
        signal_read = base

    # ── risk appetite: high-beta (risk_appetite bucket) vs core Europe ──
    def avg(bk, field="ret_3m_pct"):
        v = [m[field] for m in markets if m["bucket"] == bk and m.get(field) is not None]
        return sum(v) / len(v) if v else None
    hi_beta, core = avg("risk_appetite"), avg("europe_core")
    risk_appetite = None
    if hi_beta is not None and core is not None:
        sp = hi_beta - core
        risk_appetite = ("RISK_SEEKING" if sp > 3 else
                         "RISK_AVERSE" if sp < -3 else "NEUTRAL")

    hist = {"snapshots": []}
    try:
        hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)["Body"].read())
    except Exception:
        pass
    prior_signal = hist["snapshots"][-1]["signal"] if hist.get("snapshots") else None

    out = {
        "schema_version": "2.0",
        "method": "leading_markets_v2_bucketed",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "fmp_failed": failed,
        "n_markets": n,
        "benchmark": {"etf": BENCHMARK,
                      "ret_3m_pct": round(bench_r3, 2) if bench_r3 is not None else None,
                      "ret_1m_pct": round(bench_r1, 2) if bench_r1 is not None else None},
        "turning_point_signal": signal,
        "signal_read": signal_read,
        "flashing_buckets": flashing,
        "leading_score": round(leading_score, 1),
        "risk_score": risk_score,
        "expansion_breadth_pct": expansion_breadth_pct,
        "contraction_pct": contraction_pct,
        "improving_pct": improving_pct,
        "deteriorating_pct": deteriorating_pct,
        "regime_counts": counts,
        "risk_appetite": risk_appetite,
        "buckets": buckets,
        "markets": sorted(markets, key=lambda m: REGIME_HEALTH.get(m["regime"], 50)),
        "interpretation": (
            "19 canary markets in 5 buckets (risk-appetite, trade-cycle, "
            "commodity-cycle, credit-stress, core Europe). Each is classified "
            "EXPANSION/SLOWING/RECOVERY/CONTRACTION from price trend + momentum "
            "and tagged for relative strength vs ACWI. A flashing bucket names "
            "which macro force is leading the turn."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    hist["snapshots"].append({"ts": out["generated_at"], "signal": signal,
                               "leading_score": round(leading_score, 1),
                               "risk_score": risk_score,
                               "expansion_breadth_pct": expansion_breadth_pct,
                               "flashing_buckets": flashing})
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    if prior_signal and prior_signal != signal and signal in ("TOP_WARNING", "BOTTOM_SIGNAL", "BROAD_CONTRACTION"):
        maybe_telegram(
            f"[leading-markets] <b>CANARY SIGNAL: {signal.replace('_',' ')}</b>\n"
            f"{prior_signal} → {signal}\n"
            f"expanding {counts['EXPANSION']}/{n} · slowing {counts['SLOWING']} · "
            f"contracting {counts['CONTRACTION']}\n"
            f"flashing buckets: {', '.join(flashing) if flashing else 'none'}\n{signal_read}")

    print(f"[leading-markets] done {out['elapsed_s']}s signal={signal} "
          f"score={leading_score:.1f} flashing={flashing} failed={failed}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "turning_point_signal": signal,
        "leading_score": round(leading_score, 1), "risk_score": risk_score,
        "expansion_breadth_pct": expansion_breadth_pct,
        "flashing_buckets": flashing, "n_markets": n, "fmp_failed": failed})}
