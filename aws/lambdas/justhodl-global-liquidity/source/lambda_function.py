"""justhodl-global-liquidity — the Global Liquidity tide gauge.

Risk assets — and small-caps most of all — are driven by the global supply of
liquidity more than by earnings in any given year. Your LCE engine covers US
domestic liquidity in depth; this Lambda adds the piece that was missing: the
GLOBAL aggregate across the major central banks, plus the US "net liquidity"
formula and broad money growth.

THREE MEASURES:

1. GLOBAL LIQUIDITY INDEX — the combined balance sheets of the Fed, ECB and
   BOJ, each converted to USD, summed into one series. What matters is not the
   level but the IMPULSE — the 13-week and 52-week rate of change. Liquidity
   expanding = tailwind for risk; contracting = headwind.
     Fed   WALCL        (millions USD)
     ECB   ECBASSETSW   (millions EUR  -> USD via DEXUSEU)
     BOJ   JPNASSETS    (100 million JPY -> USD via DEXJPUS)

2. FED NET LIQUIDITY — the precise US measure that front-runs SPY ~2-4 weeks:
     Fed balance sheet  −  Treasury General Account  −  Overnight Reverse Repo
     WALCL − WTREGEN − RRPONTSYD
   When the TGA drains or RRP empties, liquidity floods markets even with a
   flat Fed balance sheet.

3. BROAD MONEY GROWTH — US M2 YoY. Money supply growth leads risk assets;
   M2 turning up after a contraction is historically a strong risk-on signal.

REGIME (global liquidity impulse): EXPANDING / NEUTRAL / CONTRACTING, each with
the historical read on what it has meant for risk assets and small-caps.

OUTPUT: data/global-liquidity.json
Telegram on regime change. Schedule: cron(0 14 ? * MON-FRI *) — daily, after
the Fed's H.4.1 and the daily TGA/RRP prints settle.
"""
import json, os, time
from datetime import datetime, timezone
from urllib import request, error
import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1073)

S3_BUCKET = "justhodl-dashboard-live"
S3_KEY = "data/global-liquidity.json"
S3_HISTORY_KEY = "data/global-liquidity-history.json"
HISTORY_MAX = 260  # ~5 years of weekly snapshots

FRED_KEY = os.environ.get("FRED_API_KEY", "")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

s3 = boto3.client("s3", region_name="us-east-1")

# series_id -> (label, unit_to_usd_billions_multiplier OR "fx")
# Balance sheets normalised to USD billions.
#   WALCL      millions USD            -> *1e-3
#   ECBASSETSW millions EUR            -> *1e-3 then *DEXUSEU (USD per EUR)
#   JPNASSETS  100 million JPY         -> *0.1 (=>JPY bn-scale) then /DEXJPUS (JPY per USD)
#              i.e. value*1e8 JPY /fx /1e9 = value*0.1/fx  USD bn
CB_SERIES = {
    "WALCL":      {"cb": "Fed", "native_unit": "millions USD",       "to_usd_bn": 1e-3},
    "ECBASSETSW": {"cb": "ECB", "native_unit": "millions EUR",       "to_usd_bn": 1e-3, "fx": "DEXUSEU"},
    "JPNASSETS":  {"cb": "BOJ", "native_unit": "100 million JPY",    "to_usd_bn": 0.1,  "fx": "DEXJPUS"},
}
NET_LIQ_SERIES = ["WALCL", "WTREGEN", "RRPONTSYD"]
FX_SERIES = ["DEXUSEU", "DEXJPUS"]


def _get_json(url, timeout=15, retries=3):
    last = None
    for i in range(retries):
        try:
            req = request.Request(url, headers={"User-Agent": "JustHodl-GLI/1.0"})
            with request.urlopen(req, timeout=timeout) as r:
                return json.loads(r.read().decode("utf-8"))
        except (error.HTTPError, error.URLError, TimeoutError) as e:
            last = e
            time.sleep(0.5 * (i + 1))
    if last:
        print(f"[fred] fetch failed: {last}")
    return None


def fred_series(series_id, limit=400):
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


def value_on_or_before(series, target_date):
    """series newest-first list of {date,value}; return value on/just before date."""
    for o in series:
        if o["date"] <= target_date:
            return o["value"]
    return series[-1]["value"] if series else None


def latest(series):
    return series[0]["value"] if series else None


def latest_date(series):
    return series[0]["date"] if series else None


def pct_change_n_weeks(series_dates_values, weeks):
    """series is list of (date,value) newest-first weekly-ish; compare latest to ~weeks ago."""
    if len(series_dates_values) < 2:
        return None
    cur = series_dates_values[0][1]
    idx = min(weeks, len(series_dates_values) - 1)
    past = series_dates_values[idx][1]
    if past and past != 0:
        return (cur - past) / abs(past) * 100
    return None


def lambda_handler(event, context):
    t0 = time.time()
    print(f"[global-liquidity] starting {datetime.now(timezone.utc).isoformat()}")
    if not FRED_KEY:
        return {"statusCode": 500, "body": json.dumps({"error": "FRED_API_KEY not set"})}

    # ── fetch raw series ──
    raw = {}
    for sid in set(list(CB_SERIES.keys()) + NET_LIQ_SERIES + FX_SERIES + ["M2SL"]):
        raw[sid] = fred_series(sid, limit=400)
        print(f"[fred] {sid}: {len(raw[sid])} obs")

    failed = [s for s, v in raw.items() if not v]

    # ── 1. Global Liquidity Index (USD billions) ──
    # Build a weekly-aligned combined series over the last ~260 weeks using the
    # Fed series dates as the spine (Fed publishes weekly).
    fed = raw.get("WALCL", [])
    components_latest = {}
    gli_points = []  # [(date, total_usd_bn)]
    spine = fed[:HISTORY_MAX]
    for o in spine:
        d = o["date"]
        total = 0.0
        ok = True
        for sid, meta in CB_SERIES.items():
            s = raw.get(sid, [])
            v = value_on_or_before(s, d)
            if v is None:
                ok = False
                break
            usd = v * meta["to_usd_bn"]
            if "fx" in meta:
                fx = value_on_or_before(raw.get(meta["fx"], []), d)
                if fx is None:
                    ok = False
                    break
                # DEXUSEU = USD per 1 EUR -> multiply.  DEXJPUS = JPY per 1 USD -> divide.
                if meta["fx"] == "DEXUSEU":
                    usd = usd * fx
                elif meta["fx"] == "DEXJPUS":
                    usd = usd / fx
            total += usd
        if ok:
            gli_points.append((d, total))
    gli_points.sort(key=lambda x: x[0], reverse=True)

    # latest component breakdown
    for sid, meta in CB_SERIES.items():
        s = raw.get(sid, [])
        v = latest(s)
        if v is None:
            continue
        usd = v * meta["to_usd_bn"]
        if "fx" in meta:
            fx = latest(raw.get(meta["fx"], []))
            if fx:
                usd = usd * fx if meta["fx"] == "DEXUSEU" else usd / fx
        components_latest[meta["cb"]] = round(usd, 1)

    gli_latest = gli_points[0][1] if gli_points else None
    gli_13w = pct_change_n_weeks(gli_points, 13)
    gli_52w = pct_change_n_weeks(gli_points, 52)

    # ── 2. Fed Net Liquidity = WALCL − WTREGEN − RRPONTSYD (USD billions) ──
    # All three are normalised to billions. FRED's H.4.1 series report in
    # millions; a value above 50,000 is treated as millions and scaled down,
    # which is robust to FRED's occasional unit inconsistency across series.
    def to_billions(v):
        if v is None:
            return None
        return v / 1e3 if abs(v) >= 50000 else v

    walcl = raw.get("WALCL", [])
    tga = raw.get("WTREGEN", [])
    rrp = raw.get("RRPONTSYD", [])
    net_liq_points = []
    for o in walcl[:HISTORY_MAX]:
        d = o["date"]
        bs = to_billions(o["value"])
        t = to_billions(value_on_or_before(tga, d))
        r = to_billions(value_on_or_before(rrp, d))
        if bs is None or t is None or r is None:
            continue
        net_liq_points.append((d, bs - t - r))
    net_liq_points.sort(key=lambda x: x[0], reverse=True)
    net_liq_latest = net_liq_points[0][1] if net_liq_points else None
    net_liq_13w = pct_change_n_weeks(net_liq_points, 13)

    # ── 3. Broad money — US M2 YoY ──
    m2 = raw.get("M2SL", [])
    m2_yoy = None
    if len(m2) >= 13:
        cur = m2[0]["value"]
        yr = m2[12]["value"]  # M2SL is monthly -> 12 obs ago ~= 1yr
        if yr:
            m2_yoy = (cur - yr) / yr * 100

    # ── regime classification on the global impulse ──
    impulse = gli_13w if gli_13w is not None else 0
    if impulse >= 1.5:
        regime = "EXPANDING"
        regime_read = ("Global central-bank liquidity is expanding. Historically a "
                       "tailwind for risk assets; small-caps and long-duration growth "
                       "benefit most when liquidity is rising.")
    elif impulse <= -1.5:
        regime = "CONTRACTING"
        regime_read = ("Global liquidity is contracting. A headwind for risk; "
                       "small-caps and speculative names tend to underperform, "
                       "quality and cash-flow outperform.")
    else:
        regime = "NEUTRAL"
        regime_read = ("Global liquidity is roughly flat. Liquidity is neither a "
                       "tailwind nor a headwind — earnings and rates dominate.")

    # ── history + regime change ──
    hist = {"snapshots": []}
    try:
        obj = s3.get_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY)
        hist = json.loads(obj["Body"].read())
    except Exception:
        pass
    prior_regime = hist["snapshots"][-1]["regime"] if hist.get("snapshots") else None

    out = {
        "schema_version": "1.0",
        "method": "global_liquidity_v1",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "elapsed_s": round(time.time() - t0, 1),
        "fred_failed": failed,
        "global_liquidity_index": {
            "total_usd_bn": round(gli_latest, 1) if gli_latest else None,
            "total_usd_trillions": round(gli_latest / 1000, 2) if gli_latest else None,
            "components_usd_bn": components_latest,
            "change_13w_pct": round(gli_13w, 2) if gli_13w is not None else None,
            "change_52w_pct": round(gli_52w, 2) if gli_52w is not None else None,
            "as_of": latest_date(fed),
        },
        "fed_net_liquidity": {
            "value_usd_bn": round(net_liq_latest, 1) if net_liq_latest else None,
            "value_usd_trillions": round(net_liq_latest / 1000, 2) if net_liq_latest else None,
            "change_13w_pct": round(net_liq_13w, 2) if net_liq_13w is not None else None,
            "formula": "Fed balance sheet - TGA - Overnight Reverse Repo",
            "as_of": latest_date(walcl),
        },
        "broad_money": {
            "us_m2_yoy_pct": round(m2_yoy, 2) if m2_yoy is not None else None,
            "as_of": latest_date(m2),
            "read": (
                "M2 growth accelerating — supportive of risk assets" if (m2_yoy or 0) > 4
                else "M2 contracting — historically a risk-asset headwind" if (m2_yoy or 0) < 0
                else "M2 growth subdued — neutral"
            ),
        },
        "regime": regime,
        "regime_read": regime_read,
        "global_impulse_13w_pct": round(impulse, 2),
        "history_points": [{"date": d, "gli_usd_bn": round(v, 1)} for d, v in gli_points[:104]],
        "net_liq_points": [{"date": d, "net_liq_usd_bn": round(v, 1)} for d, v in net_liq_points[:104]],
    }

    s3.put_object(Bucket=S3_BUCKET, Key=S3_KEY,
                   Body=json.dumps(out, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    # history
    hist["snapshots"].append({
        "ts": out["generated_at"], "regime": regime,
        "gli_usd_bn": out["global_liquidity_index"]["total_usd_bn"],
        "impulse_13w": impulse,
        "net_liq_usd_bn": out["fed_net_liquidity"]["value_usd_bn"],
        "m2_yoy": m2_yoy,
    })
    hist["snapshots"] = hist["snapshots"][-HISTORY_MAX:]
    hist["updated_at"] = out["generated_at"]
    s3.put_object(Bucket=S3_BUCKET, Key=S3_HISTORY_KEY,
                   Body=json.dumps(hist, default=str).encode("utf-8"),
                   ContentType="application/json", CacheControl="public, max-age=3600")

    if prior_regime and prior_regime != regime:
        maybe_telegram(
            f"[liquidity] <b>GLOBAL LIQUIDITY REGIME CHANGE</b>\n"
            f"<b>{prior_regime} -> {regime}</b>\n"
            f"13w impulse: {impulse:+.1f}%  ·  GLI: ${out['global_liquidity_index']['total_usd_trillions']}T\n"
            f"{regime_read}")

    print(f"[global-liquidity] done {out['elapsed_s']}s regime={regime} "
          f"impulse={impulse:+.1f}% gli=${out['global_liquidity_index'].get('total_usd_trillions')}T "
          f"failed={failed}")
    return {"statusCode": 200, "body": json.dumps({
        "ok": True, "regime": regime, "impulse_13w_pct": round(impulse, 2),
        "gli_trillions": out["global_liquidity_index"]["total_usd_trillions"],
        "fred_failed": failed})}
