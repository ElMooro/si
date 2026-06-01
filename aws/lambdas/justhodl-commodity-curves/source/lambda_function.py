"""
justhodl-commodity-curves — Commodity Cross-Asset Engine (BUILD 15/15)

WHY THIS EXISTS
===============
Commodity prices are the leading edge of inflation and growth expectations.
Bloomberg's commodity terminal is the single most-watched feature in the
macro/inflation playbook. Replicated via FRED spot prices + FMP ETF proxies.

DATA SOURCES (all free / public)
================================
FRED daily spot prices:
  DCOILWTICO          WTI Crude Oil (USD/bbl)
  DCOILBRENTEU        Brent Crude Oil (USD/bbl)
  DHHNGSP             Henry Hub Natural Gas Spot (USD/mmBtu)
  GOLDAMGBD228NLBM    LBMA Gold AM Fix (USD/oz)
  SLVPRUSD            Silver (USD/oz, monthly)
  PCOPPUSDM           Global Copper (USD/MT, monthly)

FMP /stable/ ETF quotes + history (instant proxies):
  USO  US Oil Fund (front-month WTI)
  UNG  US Natural Gas Fund
  GLD  Gold (SPDR)
  SLV  Silver (iShares)
  XLE  Energy Select Sector SPDR (oil&gas equities)
  XME  Metals & Mining SPDR
  CPER US Copper Index Fund
  DBA  Invesco DB Agriculture
  GUNR FlexShares Morningstar Global Upstream Natural Resources
  PDBC Invesco Optimum Yield Diversified Commodity

DERIVED METRICS
===============
WTI/Brent spread (transatlantic basis)
Gold/Silver ratio (>80 risk-off, <60 risk-on)
Gold/SPY ratio (real-asset preference)
XLE/SPY ratio (energy equity leadership)
Inflation hedge basket return (PDBC + GLD)

ETF METRICS PER PROXY
=====================
Current price + 1d %
Returns 5d / 20d / 60d / YTD
60d annualized realized vol
RS vs SPY at 20d

COMPOSITE REGIMES
=================
INFLATIONARY_PUSH    PDBC > +5% 20d AND XLE > +3% 20d
INFLATIONARY_COOLING PDBC < -3% 20d AND oil < 0% 20d
PRECIOUS_LEADING     GLD outperforms PDBC by 5+pp AND GLD > 0 (risk-off hedge)
INDUSTRIAL_LEADING   CPER + XME > GLD by 5+pp (cyclical growth play)
DIVERGENT            Mixed energy vs metals vs ag
SUBDUED              Most commodities tight range

SCHEDULE
========
cron(0 21 ? * MON-FRI *) — daily 21:00 UTC = 5PM ET (post-FRED publish)
"""
import io, json, os, time, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3
import _fred_shim  # noqa: F401  — cache-first FRED + 429 backoff (ops/1074)

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/commodity-curves.json"
HISTORY_KEY = "data/commodity-curves-history.json"

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
FRED_KEY = os.environ.get("FRED_KEY", "2f057499936072679d8843d7fce99989")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 20
MAX_PARALLEL = 8

FRED_SERIES = {
    "DCOILWTICO":       {"name": "WTI Crude",        "unit": "USD/bbl",   "category": "energy"},
    "DCOILBRENTEU":     {"name": "Brent Crude",      "unit": "USD/bbl",   "category": "energy"},
    "DHHNGSP":          {"name": "Natural Gas",      "unit": "USD/mmBtu", "category": "energy"},
    "GOLDAMGBD228NLBM": {"name": "Gold (LBMA AM)",   "unit": "USD/oz",    "category": "precious"},
}

ETF_UNIVERSE = [
    {"sym": "USO", "name": "US Oil Fund (WTI proxy)",       "category": "energy"},
    {"sym": "UNG", "name": "US Natural Gas Fund",           "category": "energy"},
    {"sym": "XLE", "name": "Energy Sector (equity)",         "category": "energy_equity"},
    {"sym": "GLD", "name": "Gold (SPDR)",                    "category": "precious"},
    {"sym": "SLV", "name": "Silver (iShares)",               "category": "precious"},
    {"sym": "XME", "name": "Metals & Mining",                "category": "industrial_metals_equity"},
    {"sym": "CPER", "name": "US Copper Index Fund",          "category": "industrial_metals"},
    {"sym": "DBA", "name": "Agriculture (Invesco DB)",       "category": "agriculture"},
    {"sym": "PDBC", "name": "Diversified Commodity Basket",  "category": "broad"},
    {"sym": "GUNR", "name": "Global Natural Resources",      "category": "broad"},
]

s3 = boto3.client("s3", region_name="us-east-1")
ssm = boto3.client("ssm", region_name="us-east-1")


# ═══════════════════════════════════════════════════════════════════════════
# STATS
# ═══════════════════════════════════════════════════════════════════════════

def _mean(xs):
    return sum(xs) / len(xs) if xs else 0.0


def _stdev(xs):
    if len(xs) < 2: return 0.0
    m = _mean(xs)
    return (sum((x - m) ** 2 for x in xs) / (len(xs) - 1)) ** 0.5


# ═══════════════════════════════════════════════════════════════════════════
# HTTP
# ═══════════════════════════════════════════════════════════════════════════

def http_get_json(url, timeout=HTTP_TIMEOUT):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


# ═══════════════════════════════════════════════════════════════════════════
# FRED FETCH
# ═══════════════════════════════════════════════════════════════════════════

def fetch_fred(series_id, n_years=3):
    end = datetime.now(timezone.utc).date()
    start = end.replace(year=end.year - n_years)
    url = (f"https://api.stlouisfed.org/fred/series/observations"
            f"?series_id={series_id}&api_key={FRED_KEY}&file_type=json"
            f"&observation_start={start.isoformat()}&observation_end={end.isoformat()}")
    try:
        data = http_get_json(url)
        obs = data.get("observations", [])
        out = []
        for o in obs:
            try:
                v = float(o.get("value", "."))
                out.append({"date": o["date"], "value": v})
            except: continue
        out.sort(key=lambda x: x["date"])
        return out
    except Exception as e:
        print(f"  fred {series_id} err: {str(e)[:80]}")
        return []


def analyze_fred(series_id, rows, meta):
    if not rows or len(rows) < 30:
        return {"series_id": series_id, "name": meta["name"], "category": meta["category"],
                 "err": f"insufficient data ({len(rows)})"}
    values = [r["value"] for r in rows]
    latest = rows[-1]

    def ret_pct(n):
        if len(values) < n + 1: return None
        return round((values[-1] / values[-1-n] - 1) * 100, 2)

    # YTD anchor
    cur_year = datetime.now(timezone.utc).year
    ytd_anchor = None
    for r in rows:
        if r["date"].startswith(str(cur_year)):
            ytd_anchor = r["value"]
            break
    ret_ytd = round((latest["value"] / ytd_anchor - 1) * 100, 2) if ytd_anchor else None

    daily = []
    for i in range(1, min(60, len(values))):
        prev, cur = values[-i-1], values[-i]
        if prev > 0: daily.append((cur - prev) / prev)
    vol = round(_stdev(daily) * (252 ** 0.5) * 100, 1) if daily else None

    return {
        "series_id": series_id,
        "name": meta["name"],
        "unit": meta["unit"],
        "category": meta["category"],
        "current": round(latest["value"], 2),
        "date": latest["date"],
        "ret_5d": ret_pct(5),
        "ret_20d": ret_pct(20),
        "ret_60d": ret_pct(60),
        "ret_ytd": ret_ytd,
        "vol_60d_ann_pct": vol,
        "n_history_days": len(rows),
    }


# ═══════════════════════════════════════════════════════════════════════════
# FMP ETF FETCH
# ═══════════════════════════════════════════════════════════════════════════

def fetch_quote(sym):
    url = f"https://financialmodelingprep.com/stable/quote?symbol={sym}&apikey={FMP_KEY}"
    try:
        d = http_get_json(url)
        if isinstance(d, list) and d: return d[0]
    except Exception as e:
        print(f"  {sym} quote err: {str(e)[:60]}")
    return None


def fetch_history_etf(sym, days=180):
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
            f"?symbol={sym}&apikey={FMP_KEY}")
    try:
        d = http_get_json(url)
        rows = d.get("historical", []) if isinstance(d, dict) else (d if isinstance(d, list) else [])
        out = []
        for r in rows[:days+5]:
            try:
                out.append({"date": r.get("date"),
                              "close": float(r.get("close") or r.get("adjClose") or 0)})
            except: continue
        out.sort(key=lambda x: x["date"])
        return out
    except Exception as e:
        print(f"  {sym} history err: {str(e)[:60]}")
        return []


def analyze_etf(meta):
    sym = meta["sym"]
    quote = fetch_quote(sym)
    history = fetch_history_etf(sym)
    if not history or len(history) < 30:
        return {"sym": sym, "name": meta["name"], "category": meta["category"],
                 "err": f"insufficient history ({len(history)})"}

    closes = [r["close"] for r in history]
    current = quote.get("price") if quote else closes[-1]
    change_1d = quote.get("changePercentage") if quote else 0

    def ret_pct(n):
        if len(closes) < n + 1: return None
        return round((closes[-1] / closes[-1-n] - 1) * 100, 2)

    cur_year = datetime.now(timezone.utc).year
    ytd_anchor = None
    for r in history:
        if r["date"].startswith(str(cur_year)):
            ytd_anchor = r["close"]
            break
    ret_ytd = round((closes[-1] / ytd_anchor - 1) * 100, 2) if ytd_anchor else None

    daily = []
    for i in range(1, min(60, len(closes))):
        prev, cur = closes[-i-1], closes[-i]
        if prev > 0: daily.append((cur - prev) / prev)
    vol = round(_stdev(daily) * (252 ** 0.5) * 100, 1) if daily else None

    return {
        "sym": sym, "name": meta["name"], "category": meta["category"],
        "current": round(current, 2) if current else None,
        "change_1d_pct": round(change_1d, 2) if change_1d is not None else None,
        "ret_5d": ret_pct(5),
        "ret_20d": ret_pct(20),
        "ret_60d": ret_pct(60),
        "ret_ytd": ret_ytd,
        "vol_60d_ann_pct": vol,
        "last_date": history[-1]["date"],
        "n_history_days": len(history),
    }


# ═══════════════════════════════════════════════════════════════════════════
# COMPOSITE
# ═══════════════════════════════════════════════════════════════════════════

def composite_analysis(fred_metrics, etf_metrics, spy_20d):
    by_etf = {e["sym"]: e for e in etf_metrics}
    by_fred = {f["series_id"]: f for f in fred_metrics}

    # Derived spot ratios
    wti = (by_fred.get("DCOILWTICO") or {}).get("current")
    brent = (by_fred.get("DCOILBRENTEU") or {}).get("current")
    gold = (by_fred.get("GOLDAMGBD228NLBM") or {}).get("current")

    # ETF-based ratios (more reliable cross-asset signal)
    gld_p = (by_etf.get("GLD") or {}).get("current")
    slv_p = (by_etf.get("SLV") or {}).get("current")
    cper_p = (by_etf.get("CPER") or {}).get("current")
    pdbc_20 = (by_etf.get("PDBC") or {}).get("ret_20d") or 0
    gld_20 = (by_etf.get("GLD") or {}).get("ret_20d") or 0
    xle_20 = (by_etf.get("XLE") or {}).get("ret_20d") or 0
    uso_20 = (by_etf.get("USO") or {}).get("ret_20d") or 0
    xme_20 = (by_etf.get("XME") or {}).get("ret_20d") or 0
    cper_20 = (by_etf.get("CPER") or {}).get("ret_20d") or 0
    slv_20 = (by_etf.get("SLV") or {}).get("ret_20d") or 0

    # Add RS-vs-SPY for ETFs
    for e in etf_metrics:
        if e.get("ret_20d") is not None:
            e["rs_vs_spy_20d"] = round((e["ret_20d"] or 0) - (spy_20d or 0), 2)

    ratios = {
        "wti_minus_brent": round((wti - brent), 2) if wti and brent else None,
        "gold_silver_ratio_etf": round(gld_p / slv_p, 2) if gld_p and slv_p and slv_p > 0 else None,
        "gold_minus_pdbc_20d_pp": round(gld_20 - pdbc_20, 2),
        "industrial_minus_precious_20d_pp": round((cper_20 + xme_20) / 2 - (gld_20 + slv_20) / 2, 2),
        "xle_minus_spy_20d_pp": round(xle_20 - (spy_20d or 0), 2),
        "energy_minus_metals_20d_pp": round((uso_20 + xle_20) / 2 - (cper_20 + xme_20) / 2, 2),
    }

    # Composite regime
    if pdbc_20 > 5 and xle_20 > 3:
        regime = "INFLATIONARY_PUSH"
        signal = (f"PDBC {pdbc_20:+.1f}% & XLE {xle_20:+.1f}% 20d — broad commodity rally; "
                   "inflation pressures rebuilding")
    elif pdbc_20 < -3 and uso_20 < 0:
        regime = "INFLATIONARY_COOLING"
        signal = (f"PDBC {pdbc_20:+.1f}% & USO {uso_20:+.1f}% 20d — commodities deflating; "
                   "disinflation tailwind")
    elif gld_20 - pdbc_20 > 5 and gld_20 > 0:
        regime = "PRECIOUS_LEADING"
        signal = (f"Gold {gld_20:+.1f}% beats broad commodities {pdbc_20:+.1f}% by "
                   f"{gld_20 - pdbc_20:.1f}pp 20d — defensive hedge bid")
    elif (cper_20 + xme_20) / 2 - (gld_20 + slv_20) / 2 > 5:
        regime = "INDUSTRIAL_LEADING"
        signal = (f"Industrial metals leading precious by {(cper_20+xme_20)/2 - (gld_20+slv_20)/2:+.1f}pp 20d — "
                   "growth/manufacturing recovery signal")
    elif abs(pdbc_20) < 2 and abs(gld_20) < 2:
        regime = "SUBDUED"
        signal = "Broad commodities tight range; commodity vol low"
    else:
        regime = "DIVERGENT"
        signal = (f"Mixed signals: oil {uso_20:+.1f}%, metals {(cper_20+xme_20)/2:+.1f}%, "
                   f"precious {(gld_20+slv_20)/2:+.1f}%, ag {(by_etf.get('DBA',{}).get('ret_20d') or 0):+.1f}%")

    # Rankings
    ranked = sorted(
        [e for e in etf_metrics if not e.get("err")],
        key=lambda x: x.get("ret_20d") or -999, reverse=True,
    )

    return {
        "ratios": ratios,
        "ranked_20d": [{"sym": r["sym"], "name": r["name"], "category": r["category"],
                          "current": r.get("current"),
                          "change_1d_pct": r.get("change_1d_pct"),
                          "ret_5d": r.get("ret_5d"), "ret_20d": r.get("ret_20d"),
                          "ret_60d": r.get("ret_60d"), "ret_ytd": r.get("ret_ytd"),
                          "vol_60d_ann_pct": r.get("vol_60d_ann_pct"),
                          "rs_vs_spy_20d": r.get("rs_vs_spy_20d")}
                         for r in ranked],
        "top_3_by_20d": [r["sym"] for r in ranked[:3]],
        "bottom_3_by_20d": [r["sym"] for r in ranked[-3:]],
        "composite_regime": regime,
        "composite_signal": signal,
    }


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
    print(f"=== commodity-curves v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")

    try:
        prior_regime = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)["Body"].read()).get("composite_regime")
    except Exception:
        prior_regime = None

    # Fetch SPY history for RS calc
    spy_history = fetch_history_etf("SPY", days=60)
    spy_20d = None
    if len(spy_history) >= 21:
        spy_20d = round((spy_history[-1]["close"] / spy_history[-21]["close"] - 1) * 100, 2)

    # Parallel FRED + ETF fetches
    fred_metrics = []
    etf_metrics = []

    print(f"  fetching {len(FRED_SERIES)} FRED series + {len(ETF_UNIVERSE)} ETFs in parallel...")
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        fred_futures = {ex.submit(fetch_fred, sid): (sid, meta)
                          for sid, meta in FRED_SERIES.items()}
        etf_futures = {ex.submit(analyze_etf, m): m for m in ETF_UNIVERSE}

        for f in as_completed(fred_futures):
            sid, meta = fred_futures[f]
            rows = f.result()
            r = analyze_fred(sid, rows, meta)
            fred_metrics.append(r)
            if not r.get("err"):
                print(f"  ✓ FRED {sid:20s} {meta['name']:18s} {r['current']:.2f} {meta['unit']:12s} "
                      f"5d={r.get('ret_5d'):+.2f}% 20d={r.get('ret_20d'):+.2f}% ytd={r.get('ret_ytd'):+.2f}%")
            else:
                print(f"  ✗ FRED {sid}: {r.get('err')}")

        for f in as_completed(etf_futures):
            m = etf_futures[f]
            r = f.result()
            etf_metrics.append(r)
            if not r.get("err"):
                print(f"  ✓ ETF {r['sym']:5s} {r.get('name')[:25]:25s} {r.get('current'):>8.2f} "
                      f"1d={r.get('change_1d_pct'):+.2f}% 5d={r.get('ret_5d'):+.2f}% "
                      f"20d={r.get('ret_20d'):+.2f}% ytd={r.get('ret_ytd'):+.2f}%")
            else:
                print(f"  ✗ ETF {m['sym']}: {r.get('err')}")

    composite = composite_analysis(fred_metrics, etf_metrics, spy_20d)

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "elapsed_seconds": round(time.time() - started, 1),
        "source": "FRED + FMP /stable/",
        "spy_20d": spy_20d,
        "fred_metrics": fred_metrics,
        "etf_metrics": etf_metrics,
        "n_fred": len(FRED_SERIES),
        "n_etf": len(ETF_UNIVERSE),
        "n_fred_with_data": sum(1 for r in fred_metrics if not r.get("err")),
        "n_etf_with_data": sum(1 for r in etf_metrics if not r.get("err")),
        "composite": composite,
        "composite_regime": composite["composite_regime"],
        "composite_signal": composite["composite_signal"],
        "regime_changed_from_prior": (prior_regime != composite["composite_regime"]) if prior_regime else False,
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ commodity-curves.json written")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # Telegram on regime change OR extreme readings
    alert_sent = False
    if (prior_regime and prior_regime != composite["composite_regime"]) or composite["composite_regime"] in ("INFLATIONARY_PUSH", "INFLATIONARY_COOLING"):
        ratios = composite["ratios"]
        lines = [f"🛢️ *Commodities · {datetime.now(timezone.utc).strftime('%b %d %H:%M')} UTC*\n",
                  f"⚡ {composite['composite_regime']}",
                  f"_{composite['composite_signal'][:200]}_\n",
                  f"📊 Top 3 (20d): {' · '.join(composite['top_3_by_20d'])}",
                  f"📉 Bottom 3: {' · '.join(composite['bottom_3_by_20d'])}",
                  f"⚖️  Gold/Silver: {ratios.get('gold_silver_ratio_etf')} · "
                  f"XLE-SPY: {ratios.get('xle_minus_spy_20d_pp')}pp"]
        if prior_regime and prior_regime != composite["composite_regime"]:
            lines.insert(2, f"_(was {prior_regime})_")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "regime": composite["composite_regime"],
        "n_fred_loaded": payload["n_fred_with_data"],
        "n_etf_loaded": payload["n_etf_with_data"],
        "spy_20d": spy_20d,
        "top_3": composite["top_3_by_20d"],
        "regime_changed": payload["regime_changed_from_prior"],
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
