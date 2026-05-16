"""justhodl-leading-markets — the macro turning-point engine.

Certain markets reliably lead global macro tops and bottoms. They turn
BEFORE the broad market because of what they are:

  Taiwan (EWT)        the global tech / semiconductor cycle canary (TSMC)
  Hong Kong (EWH)     China gateway + offshore liquidity proxy
  Emerging Mkts (EEM) the marginal risk capital — tops and bottoms first
  Frontier Mkts (FM)  the highest-beta risk appetite gauge
  Italy (EWI)         European sovereign-stress canary (periphery)
  Spain (EWP)         European periphery — credit/banking sensitivity
  Netherlands (EWN)   trade-intensive open economy — turns early (ASML, Rotterdam)
  Finland (EFNL)      small open economy — high cyclical sensitivity
  France (EWQ)        core Europe
  Europe (VGK)        the broad European tape

The equity market of each IS a leading indicator — markets discount the
economy ~6 months out. This engine tracks each via its liquid US-listed
ETF, classifies it EXPANSION / SLOWING / RECOVERY / CONTRACTION from price
trend + momentum, then composites a global turning-point read.

TURNING-POINT SIGNAL:
  EXPANSION_CONFIRMED  most canaries expanding — broad risk-on
  TOP_WARNING          canaries rolling over (SLOWING) — leaders topping
  BOTTOM_SIGNAL        canaries turning up (RECOVERY) — leaders bottoming
  BROAD_CONTRACTION    most canaries contracting — risk-off entrenched
  MIXED                no clear lead

Outputs a risk_score (0-100, high = leaders contracting) consumed by
crisis-composite, and expansion_breadth_pct consumed by the Khalid Index.

OUTPUT: data/leading-markets.json   Schedule: daily.
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

# canary universe — ETF, market name, role group
UNIVERSE = [
    ("EWT",  "Taiwan",        "asia_tech"),
    ("EWH",  "Hong Kong",     "asia_china"),
    ("EEM",  "Emerging Mkts", "emerging"),
    ("FM",   "Frontier Mkts", "frontier"),
    ("EWI",  "Italy",         "europe_periphery"),
    ("EWP",  "Spain",         "europe_periphery"),
    ("EWN",  "Netherlands",   "europe_core"),
    ("EFNL", "Finland",       "europe_core"),
    ("EWQ",  "France",        "europe_core"),
    ("VGK",  "Europe",        "europe_broad"),
]


def _get_json(url, timeout=15, retries=3):
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-Leading/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError):
            time.sleep(0.5 * (i + 1))
    return None


def fetch_history(sym, days=300):
    """FMP /stable EOD history -> list of closes, oldest-first."""
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
    return out


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
    if len(closes) < n:
        return None
    return sum(closes[-n:]) / n


def ret(closes, n):
    if len(closes) <= n:
        return None
    prior = closes[-n - 1]
    if prior:
        return (closes[-1] - prior) / prior * 100
    return None


def classify(closes):
    """Classify one market into EXPANSION / SLOWING / RECOVERY / CONTRACTION."""
    if len(closes) < 130:
        return None
    px = closes[-1]
    sma200 = sma(closes, 200) or sma(closes, len(closes) - 1)
    sma50 = sma(closes, 50)
    above_200 = sma200 is not None and px > sma200
    r1, r3, r6 = ret(closes, 21), ret(closes, 63), ret(closes, 126)
    hi_252 = max(closes[-252:]) if len(closes) >= 252 else max(closes)
    lo_252 = min(closes[-252:]) if len(closes) >= 252 else min(closes)
    from_high = (px - hi_252) / hi_252 * 100 if hi_252 else None
    # 50dma slope (last ~10d)
    sma50_prior = sma(closes[:-10], 50) if len(closes) > 60 else None
    sma50_rising = (sma50 is not None and sma50_prior is not None and sma50 > sma50_prior)

    mom3 = r3 if r3 is not None else 0
    mom6 = r6 if r6 is not None else 0
    mom1 = r1 if r1 is not None else 0

    if above_200 and mom3 > 0 and mom6 > 0:
        regime = "EXPANSION"
    elif above_200 and (mom3 < 0 or not sma50_rising):
        regime = "SLOWING"
    elif (not above_200) and mom1 > 0 and mom3 > 0 and sma50_rising:
        regime = "RECOVERY"
    elif not above_200:
        regime = "CONTRACTION"
    else:
        regime = "EXPANSION"

    return {
        "price": round(px, 2),
        "above_200dma": above_200,
        "ret_1m_pct": round(r1, 2) if r1 is not None else None,
        "ret_3m_pct": round(r3, 2) if r3 is not None else None,
        "ret_6m_pct": round(r6, 2) if r6 is not None else None,
        "from_52w_high_pct": round(from_high, 1) if from_high is not None else None,
        "sma50_rising": sma50_rising,
        "regime": regime,
    }


# regime -> 0-100 health contribution (high = expansion)
REGIME_HEALTH = {"EXPANSION": 100, "RECOVERY": 65, "SLOWING": 35, "CONTRACTION": 5}


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[leading-markets] starting {datetime.now(timezone.utc).isoformat()}")
    if not FMP_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FMP_KEY not set"})}

    markets = []
    failed = []
    for sym, name, role in UNIVERSE:
        hist = fetch_history(sym)
        closes = [h["close"] for h in hist if h["close"] > 0]
        cls = classify(closes)
        if cls is None:
            failed.append(sym)
            print(f"[leading] {sym} ({name}): insufficient data ({len(closes)})")
            continue
        cls.update({"etf": sym, "market": name, "role": role})
        markets.append(cls)
        print(f"[leading] {sym} ({name}): {cls['regime']} 3m={cls['ret_3m_pct']}")

    if not markets:
        return {"statusCode": 500, "body": json.dumps({"error": "no market data", "failed": failed})}

    n = len(markets)
    counts = {"EXPANSION": 0, "SLOWING": 0, "RECOVERY": 0, "CONTRACTION": 0}
    for m in markets:
        counts[m["regime"]] = counts.get(m["regime"], 0) + 1

    leading_score = sum(REGIME_HEALTH.get(m["regime"], 50) for m in markets) / n
    risk_score = round(100 - leading_score, 1)          # high = leaders contracting
    expansion_breadth_pct = round(counts["EXPANSION"] / n * 100, 1)
    contraction_pct = round(counts["CONTRACTION"] / n * 100, 1)
    improving_pct = round((counts["EXPANSION"] + counts["RECOVERY"]) / n * 100, 1)
    deteriorating_pct = round((counts["SLOWING"] + counts["CONTRACTION"]) / n * 100, 1)

    # turning-point signal
    if counts["EXPANSION"] >= n * 0.6:
        signal = "EXPANSION_CONFIRMED"
        read = ("Most canary markets are expanding — broad global risk-on. The "
                "leaders confirm the uptrend; tops are not made here.")
    elif counts["CONTRACTION"] >= n * 0.6:
        signal = "BROAD_CONTRACTION"
        read = ("Most canary markets are contracting — global risk-off is entrenched. "
                "Wait for RECOVERY readings to appear before turning constructive.")
    elif counts["RECOVERY"] >= n * 0.35 and counts["RECOVERY"] >= counts["SLOWING"]:
        signal = "BOTTOM_SIGNAL"
        read = ("The canary markets are turning up off their lows — historically an "
                "early bottom signal. Leaders bottom before the broad market does.")
    elif counts["SLOWING"] >= n * 0.4 and counts["SLOWING"] >= counts["RECOVERY"]:
        signal = "TOP_WARNING"
        read = ("The canary markets are rolling over while still above trend — "
                "historically an early top warning. Leaders top before the broad "
                "market does. Tighten risk.")
    else:
        signal = "MIXED"
        read = ("The canary markets are mixed — no unified lead on a macro turning "
                "point right now.")

    # relative strength: high-beta risk capital (EM + Frontier) vs core Europe
    def avg_ret(role_set, field="ret_3m_pct"):
        vals = [m[field] for m in markets if m["role"] in role_set and m[field] is not None]
        return sum(vals) / len(vals) if vals else None
    em_front = avg_ret({"emerging", "frontier"})
    core = avg_ret({"europe_core", "europe_broad"})
    risk_appetite = None
    if em_front is not None and core is not None:
        spread = em_front - core
        risk_appetite = ("RISK_SEEKING" if spread > 3 else
                          "RISK_AVERSE" if spread < -3 else "NEUTRAL")

    hist_doc = {"snapshots": []}
    try:
        hist_doc = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)["Body"].read())
    except Exception:
        pass
    prior_signal = hist_doc["snapshots"][-1]["signal"] if hist_doc.get("snapshots") else None

    out = {
        "schema_version": "1.0",
        "method": "leading_markets_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "fmp_failed": failed,
        "n_markets": n,
        "turning_point_signal": signal,
        "signal_read": read,
        "leading_score": round(leading_score, 1),
        "risk_score": risk_score,
        "expansion_breadth_pct": expansion_breadth_pct,
        "contraction_pct": contraction_pct,
        "improving_pct": improving_pct,
        "deteriorating_pct": deteriorating_pct,
        "regime_counts": counts,
        "risk_appetite": risk_appetite,
        "markets": sorted(markets, key=lambda m: REGIME_HEALTH.get(m["regime"], 50)),
        "interpretation": (
            "Each canary market's equity ETF is classified EXPANSION/SLOWING/"
            "RECOVERY/CONTRACTION from price trend and momentum. These markets "
            "lead global macro turning points — Taiwan the tech cycle, the "
            "European periphery sovereign stress, EM/Frontier risk appetite."
        ),
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    hist_doc["snapshots"].append({"ts": out["generated_at"], "signal": signal,
                                   "leading_score": round(leading_score, 1),
                                   "risk_score": risk_score,
                                   "expansion_breadth_pct": expansion_breadth_pct})
    hist_doc["snapshots"] = hist_doc["snapshots"][-HISTORY_MAX:]
    hist_doc["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist_doc, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    if prior_signal and prior_signal != signal and signal in ("TOP_WARNING", "BOTTOM_SIGNAL", "BROAD_CONTRACTION"):
        maybe_telegram(
            f"[leading-markets] <b>CANARY SIGNAL: {signal.replace('_',' ')}</b>\n"
            f"{prior_signal} → {signal}\n"
            f"expanding {counts['EXPANSION']}/{n} · slowing {counts['SLOWING']} · "
            f"recovery {counts['RECOVERY']} · contracting {counts['CONTRACTION']}\n{read}")

    print(f"[leading-markets] done {out['elapsed_s']}s signal={signal} "
          f"leading_score={leading_score:.1f} failed={failed}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "turning_point_signal": signal,
        "leading_score": round(leading_score, 1), "risk_score": risk_score,
        "expansion_breadth_pct": expansion_breadth_pct, "fmp_failed": failed})}
