"""justhodl-china-liquidity — China liquidity + credit-impulse engine.

China is the world's #2 economy and the marginal driver of global growth and
commodity demand. Your Global Liquidity Index covers the Fed, ECB and BOJ but
not China — this fills that gap.

The single most useful China signal is the CREDIT IMPULSE: the change in the
flow of new credit. It leads Chinese activity, global manufacturing PMIs and
commodity prices by roughly 6-12 months. The textbook measure uses Total
Social Financing, which is not on FRED; this engine uses the best free proxy
— the ACCELERATION of broad/narrow money growth (the 2nd derivative), which
moves with the same signal — and is explicit that it is a proxy.

MEASURES (all FRED — free, defensive: uses whatever series resolve):
  • China M1 / M2 year-over-year growth        (money supply)
  • Money-growth ACCELERATION                  (credit-impulse proxy)
  • China interbank rate                       (liquidity tightness)
  • USD/CNY                                    (currency pressure / capital flow)
  • Copper price YoY + copper/gold ratio       (Dr. Copper — real China demand)

REGIME: EASING / NEUTRAL / TIGHTENING — with the read on what it has
historically meant for commodities, EM and global cyclicals ~2-3 quarters out.

OUTPUT: data/china-liquidity.json   Schedule: daily.
"""
import json, os, time
from datetime import datetime, timezone
from urllib import request, error
import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1073)

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/china-liquidity.json"
S3_HISTORY_KEY = "data/china-liquidity-history.json"
HISTORY_MAX = 260

FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")

# Candidate FRED series — tried in order; first that resolves wins.
SERIES = {
    "m1": ["MANMM101CNM189S", "MANMM101CNQ189S"],
    "m2": ["MYAGM2CNM189N", "MABMM301CNM189S", "MABMM301CNQ189S"],
    "interbank": ["IR3TIB01CNM156N", "IR3TIB01CNM156S"],
    "usdcny": ["DEXCHUS"],
    "copper": ["PCOPPUSDM"],
    "gold": ["IQ12260", "GOLDAMGBD228NLBM"],
}


def _get_json(url, timeout=15, retries=3):
    last = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-China/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last = e
            time.sleep(0.5 * (i + 1))
    return None


def fred(series_id, limit=400):
    if not FRED_KEY:
        return []
    url = (f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}"
           f"&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit={limit}")
    j = _get_json(url)
    if not j:
        return []
    out = []
    for o in j.get("observations", []):
        v = o.get("value")
        if v in (None, ".", ""):
            continue
        try:
            out.append({"date": o.get("date"), "value": float(v)})
        except Exception:
            pass
    return out  # newest-first


def fred_first(candidates):
    """Return (series_id, observations) for the first candidate that resolves."""
    for sid in candidates:
        obs = fred(sid)
        if obs:
            return sid, obs
    return None, []


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


def yoy(obs, periods=12):
    """Year-over-year % from a monthly newest-first series."""
    if len(obs) < periods + 1:
        return None
    cur, prior = obs[0]["value"], obs[periods]["value"]
    if prior and prior != 0:
        return (cur - prior) / abs(prior) * 100
    return None


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[china-liquidity] starting {datetime.now(timezone.utc).isoformat()}")
    if not FRED_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FRED_API_KEY not set"})}

    resolved = {}
    raw = {}
    for key, cands in SERIES.items():
        sid, obs = fred_first(cands)
        resolved[key] = sid
        raw[key] = obs
        print(f"[china] {key}: {sid} ({len(obs)} obs)")

    failed = [k for k, v in raw.items() if not v]

    # ── money growth + credit-impulse proxy ──
    m1 = raw.get("m1", [])
    m2 = raw.get("m2", [])
    m1_yoy = yoy(m1)
    m2_yoy = yoy(m2)
    # prior-year YoY for acceleration
    m1_yoy_prior = yoy(m1[12:]) if len(m1) > 24 else None
    m2_yoy_prior = yoy(m2[12:]) if len(m2) > 24 else None
    # credit-impulse proxy = acceleration of money growth (pp change in YoY)
    impulse_m1 = (m1_yoy - m1_yoy_prior) if (m1_yoy is not None and m1_yoy_prior is not None) else None
    impulse_m2 = (m2_yoy - m2_yoy_prior) if (m2_yoy is not None and m2_yoy_prior is not None) else None
    impulse = None
    parts = [x for x in (impulse_m1, impulse_m2) if x is not None]
    if parts:
        impulse = sum(parts) / len(parts)

    # ── interbank rate (tightness) ──
    ib = raw.get("interbank", [])
    ib_latest = ib[0]["value"] if ib else None
    ib_3m_ago = ib[3]["value"] if len(ib) > 3 else None
    ib_trend = (ib_latest - ib_3m_ago) if (ib_latest is not None and ib_3m_ago is not None) else None

    # ── USD/CNY pressure ──
    cny = raw.get("usdcny", [])
    cny_latest = cny[0]["value"] if cny else None
    cny_3m_ago = cny[63]["value"] if len(cny) > 63 else None
    cny_chg_3m = ((cny_latest - cny_3m_ago) / cny_3m_ago * 100) if (cny_latest and cny_3m_ago) else None

    # ── Dr. Copper ──
    cop = raw.get("copper", [])
    gold = raw.get("gold", [])
    copper_yoy = yoy(cop)
    copper_gold = None
    if cop and gold and gold[0]["value"]:
        copper_gold = cop[0]["value"] / gold[0]["value"]

    # ── regime classification ──
    # primary signal: credit-impulse proxy + money growth level
    score = 0
    if impulse is not None:
        score += 2 if impulse > 1.5 else (-2 if impulse < -1.5 else 0)
    if m2_yoy is not None:
        score += 1 if m2_yoy > 9 else (-1 if m2_yoy < 6 else 0)
    if ib_trend is not None:
        score += -1 if ib_trend > 0.3 else (1 if ib_trend < -0.3 else 0)
    if copper_yoy is not None:
        score += 1 if copper_yoy > 8 else (-1 if copper_yoy < -8 else 0)

    if score >= 2:
        regime = "EASING"
        regime_read = ("China liquidity/credit is accelerating. Historically a 6-12 "
                       "month tailwind for global commodities, emerging markets and "
                       "cyclical/industrial equities — the credit impulse leads.")
    elif score <= -2:
        regime = "TIGHTENING"
        regime_read = ("China liquidity/credit is decelerating. A forward headwind for "
                       "commodities, EM and global cyclicals — typically felt 2-3 "
                       "quarters out as the credit impulse rolls over.")
    else:
        regime = "NEUTRAL"
        regime_read = ("China liquidity is roughly steady — no strong forward push or "
                       "pull on commodities and global cyclicals from the credit impulse.")

    hist = {"snapshots": []}
    try:
        hist = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)["Body"].read())
    except Exception:
        pass
    prior_regime = hist["snapshots"][-1]["regime"] if hist.get("snapshots") else None

    out = {
        "schema_version": "1.0",
        "method": "china_liquidity_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "fred_failed": failed,
        "series_resolved": resolved,
        "regime": regime,
        "regime_read": regime_read,
        "money": {
            "m1_yoy_pct": round(m1_yoy, 2) if m1_yoy is not None else None,
            "m2_yoy_pct": round(m2_yoy, 2) if m2_yoy is not None else None,
        },
        "credit_impulse": {
            "value_pp": round(impulse, 2) if impulse is not None else None,
            "is_proxy": True,
            "definition": ("acceleration of money-supply YoY growth (pp change). "
                           "A free proxy for the Total Social Financing credit "
                           "impulse — moves with the same signal, leads ~6-12mo."),
            "signal": ("credit accelerating — forward tailwind" if (impulse or 0) > 1.5
                       else "credit decelerating — forward headwind" if (impulse or 0) < -1.5
                       else "credit impulse roughly flat"),
        },
        "interbank_rate": {
            "latest_pct": round(ib_latest, 3) if ib_latest is not None else None,
            "change_3m_pp": round(ib_trend, 3) if ib_trend is not None else None,
        },
        "currency": {
            "usd_cny": round(cny_latest, 4) if cny_latest is not None else None,
            "cny_change_3m_pct": round(cny_chg_3m, 2) if cny_chg_3m is not None else None,
            "read": ("CNY weakening vs USD — capital-outflow / easing pressure"
                     if (cny_chg_3m or 0) > 1
                     else "CNY firm — stable capital picture"),
        },
        "dr_copper": {
            "copper_yoy_pct": round(copper_yoy, 1) if copper_yoy is not None else None,
            "copper_gold_ratio": round(copper_gold, 5) if copper_gold is not None else None,
            "read": ("copper strong — real China/global demand firming"
                     if (copper_yoy or 0) > 8
                     else "copper weak — real demand softening" if (copper_yoy or 0) < -8
                     else "copper neutral"),
        },
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    hist["snapshots"].append({"ts": out["generated_at"], "regime": regime,
                               "m2_yoy": m2_yoy, "credit_impulse": impulse,
                               "copper_yoy": copper_yoy})
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    if prior_regime and prior_regime != regime:
        maybe_telegram(
            f"[china] <b>CHINA LIQUIDITY REGIME CHANGE</b>\n"
            f"<b>{prior_regime} → {regime}</b>\n"
            f"credit impulse {round(impulse,1) if impulse is not None else '—'}pp · "
            f"M2 {round(m2_yoy,1) if m2_yoy is not None else '—'}%\n{regime_read}")

    print(f"[china-liquidity] done {out['elapsed_s']}s regime={regime} "
          f"impulse={impulse} failed={failed}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "regime": regime,
        "credit_impulse_pp": round(impulse, 2) if impulse is not None else None,
        "m2_yoy": round(m2_yoy, 2) if m2_yoy is not None else None,
        "fred_failed": failed})}
