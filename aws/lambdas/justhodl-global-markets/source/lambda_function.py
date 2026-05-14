"""
justhodl-global-markets — Cross-Asset Regional Markets Engine (BUILD 14/15)

WHY THIS EXISTS
===============
Bloomberg's "Global Markets" panel is one of its most-used tools — every
serious macro trader watches Asian close → European open → US handoff as
the daily flow. We replicate via 15 region/country ETF proxies, all
USD-denominated, fetched from FMP /stable/quote.

UNIVERSE (15 region/country/style ETFs)
=======================================
  ACWI   MSCI All Country World         baseline
  SPY    US Large Cap                    baseline
  QQQ    US Tech (NDX)                   style
  IWM    US Small Cap (RUT)              style
  EFA    Developed ex-US (EAFE)          region
  VEU    Developed All-World ex-US       region
  EEM    Emerging Markets                region
  EWJ    Japan                           country
  EWG    Germany                         country
  EWU    UK                              country
  MCHI   China                           country
  INDA   India                           country
  EWZ    Brazil                          country
  EWY    South Korea                     country
  EWA    Australia                       country

METRICS PER ETF
===============
Current price + change_1d
Returns: 5d, 20d, 60d, YTD (vs Jan 1 close)
60d realized volatility (annualized)
Relative strength vs SPY:
  RS_5d = (etf_5d_return − spy_5d_return) in pp
  RS_20d, RS_60d
RS rank within universe

COMPOSITE
=========
Region leadership:
  Top 3 by 20d return
  Bottom 3 by 20d return
  Risk-on/risk-off pulse: EEM/EFA pair → US pair return
  Tech vs broad: QQQ−SPY 20d
  Small vs Large: IWM−SPY 20d
Cross-asset regime:
  GLOBAL_BROAD_BULL  Most regions positive 20d, US/Asia/EU all leading
  US_LED_BULL        US ahead by >3pp 20d vs intl
  ROW_LED_BULL       Intl ahead by >3pp 20d vs US
  DIVERGENT          Wide dispersion, mixed signals
  GLOBAL_BROAD_BEAR  Most regions negative 20d

SCHEDULE
========
cron(15 13-21 ? * MON-FRI *) — every hour during US market hours
+ once before EU open (07:00 UTC) + once before Asia close (08:00 UTC)
"""
import io, json, os, time, urllib.request
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed
import boto3

VERSION = "1.0.0"

S3_BUCKET = "justhodl-dashboard-live"
OUTPUT_KEY = "data/global-markets.json"

FMP_KEY = os.environ.get("FMP_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
TELEGRAM_TOKEN = os.environ.get("TELEGRAM_TOKEN", "")
TELEGRAM_CHAT_ID = os.environ.get("TELEGRAM_CHAT_ID", "")

HTTP_TIMEOUT = 20
MAX_PARALLEL = 8

UNIVERSE = [
    {"sym": "ACWI", "name": "All Country World", "kind": "baseline", "region": "Global"},
    {"sym": "SPY", "name": "US Large Cap", "kind": "baseline", "region": "US"},
    {"sym": "QQQ", "name": "US Tech (NDX)", "kind": "style", "region": "US"},
    {"sym": "IWM", "name": "US Small Cap (RUT)", "kind": "style", "region": "US"},
    {"sym": "EFA", "name": "Developed ex-US", "kind": "region", "region": "DM ex-US"},
    {"sym": "VEU", "name": "All-World ex-US", "kind": "region", "region": "World ex-US"},
    {"sym": "EEM", "name": "Emerging Markets", "kind": "region", "region": "EM"},
    {"sym": "EWJ", "name": "Japan", "kind": "country", "region": "Japan"},
    {"sym": "EWG", "name": "Germany", "kind": "country", "region": "Germany"},
    {"sym": "EWU", "name": "UK", "kind": "country", "region": "UK"},
    {"sym": "MCHI", "name": "China (MSCI)", "kind": "country", "region": "China"},
    {"sym": "INDA", "name": "India", "kind": "country", "region": "India"},
    {"sym": "EWZ", "name": "Brazil", "kind": "country", "region": "Brazil"},
    {"sym": "EWY", "name": "South Korea", "kind": "country", "region": "South Korea"},
    {"sym": "EWA", "name": "Australia", "kind": "country", "region": "Australia"},
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
# FMP FETCH
# ═══════════════════════════════════════════════════════════════════════════

def http_get_json(url, timeout=HTTP_TIMEOUT):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (Macintosh) Chrome/120",
        "Accept": "application/json",
    })
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8"))


def fetch_quote(sym):
    """Returns current quote dict or None."""
    url = f"https://financialmodelingprep.com/stable/quote?symbol={sym}&apikey={FMP_KEY}"
    try:
        d = http_get_json(url)
        if isinstance(d, list) and d: return d[0]
        return None
    except Exception as e:
        print(f"  {sym} quote err: {str(e)[:60]}")
        return None


def fetch_history(sym, days=180):
    """Returns list of {date, close} dicts sorted ascending. Last ~180 trading days."""
    url = (f"https://financialmodelingprep.com/stable/historical-price-eod/full"
            f"?symbol={sym}&apikey={FMP_KEY}")
    try:
        d = http_get_json(url)
        if isinstance(d, dict) and "historical" in d:
            rows = d["historical"]
        elif isinstance(d, list):
            rows = d
        else:
            return []
        out = []
        for r in rows[:days+5]:
            try:
                out.append({"date": r.get("date"), "close": float(r.get("close") or r.get("adjClose") or 0)})
            except: continue
        out.sort(key=lambda x: x["date"])
        return out
    except Exception as e:
        print(f"  {sym} history err: {str(e)[:60]}")
        return []


# ═══════════════════════════════════════════════════════════════════════════
# PER-ETF ANALYSIS
# ═══════════════════════════════════════════════════════════════════════════

def analyze_etf(meta):
    sym = meta["sym"]
    result = {"sym": sym, "name": meta["name"], "kind": meta["kind"], "region": meta["region"]}
    quote = fetch_quote(sym)
    history = fetch_history(sym, days=180)

    if not history or len(history) < 30:
        result["err"] = f"insufficient history ({len(history)})"
        return result

    closes = [r["close"] for r in history]
    dates = [r["date"] for r in history]

    current = quote.get("price") if quote else closes[-1]
    change_1d_pct = quote.get("changePercentage") if quote else 0

    # Returns
    def ret_pct(n):
        if len(closes) < n + 1: return None
        return round((closes[-1] / closes[-1-n] - 1) * 100, 2)

    ret_5d = ret_pct(5)
    ret_20d = ret_pct(20)
    ret_60d = ret_pct(60)

    # YTD (price at Jan 1 or first trade of current year)
    cur_year = datetime.now(timezone.utc).year
    ytd_anchor = None
    for r in history:
        if r["date"].startswith(str(cur_year)):
            ytd_anchor = r["close"]
            break
    ret_ytd = round((closes[-1] / ytd_anchor - 1) * 100, 2) if ytd_anchor else None

    # Annualized vol from daily returns
    daily_rets = []
    for i in range(1, min(60, len(closes))):
        prev = closes[-i-1]
        cur = closes[-i]
        if prev > 0: daily_rets.append((cur - prev) / prev)
    vol_60d_ann = round(_stdev(daily_rets) * (252 ** 0.5) * 100, 1) if daily_rets else None

    result.update({
        "current": round(current, 2) if current else None,
        "change_1d_pct": round(change_1d_pct, 2) if change_1d_pct is not None else None,
        "ret_5d": ret_5d,
        "ret_20d": ret_20d,
        "ret_60d": ret_60d,
        "ret_ytd": ret_ytd,
        "vol_60d_ann_pct": vol_60d_ann,
        "n_history_days": len(history),
        "last_date": history[-1]["date"],
        "closes_last_60": [round(c, 2) for c in closes[-60:]],
        "dates_last_60": dates[-60:],
    })
    return result


# ═══════════════════════════════════════════════════════════════════════════
# COMPOSITE
# ═══════════════════════════════════════════════════════════════════════════

def composite_analysis(results):
    by_sym = {r["sym"]: r for r in results}
    spy = by_sym.get("SPY") or {}
    spy_5d = spy.get("ret_5d") or 0
    spy_20d = spy.get("ret_20d") or 0
    spy_60d = spy.get("ret_60d") or 0

    valid = [r for r in results if not r.get("err")]

    # Add RS-vs-SPY to each
    for r in valid:
        if r["sym"] == "SPY": continue
        for k in ("5d", "20d", "60d"):
            ret_etf = r.get(f"ret_{k}") or 0
            ret_spy = (spy.get(f"ret_{k}") or 0)
            r[f"rs_vs_spy_{k}"] = round(ret_etf - ret_spy, 2)

    # Top/bottom by 20d
    ranked_20d = sorted(valid, key=lambda x: x.get("ret_20d", -999) or -999, reverse=True)
    ranked_5d = sorted(valid, key=lambda x: x.get("ret_5d", -999) or -999, reverse=True)
    ranked_ytd = sorted(valid, key=lambda x: x.get("ret_ytd", -999) or -999, reverse=True)

    # Regional pair signals
    eem = by_sym.get("EEM", {}).get("ret_20d") or 0
    efa = by_sym.get("EFA", {}).get("ret_20d") or 0
    qqq = by_sym.get("QQQ", {}).get("ret_20d") or 0
    iwm = by_sym.get("IWM", {}).get("ret_20d") or 0

    pairs = {
        "qqq_minus_spy_20d": round(qqq - spy_20d, 2),  # tech leadership
        "iwm_minus_spy_20d": round(iwm - spy_20d, 2),  # small-cap leadership
        "eem_minus_spy_20d": round(eem - spy_20d, 2),  # EM vs US
        "efa_minus_spy_20d": round(efa - spy_20d, 2),  # DM ex-US vs US
        "eem_minus_efa_20d": round(eem - efa, 2),     # EM vs DM
    }

    # Composite regime
    n_positive_20d = sum(1 for r in valid if (r.get("ret_20d") or 0) > 0)
    n_total = len(valid)
    pct_positive = n_positive_20d / n_total if n_total else 0

    intl_avg = _mean([r.get("ret_20d") or 0 for r in valid
                       if r["sym"] not in ("SPY", "QQQ", "IWM", "ACWI") and not r.get("err")])
    us_intl_diff = spy_20d - intl_avg

    if pct_positive < 0.3:
        regime = "GLOBAL_BROAD_BEAR"
        signal = f"Only {n_positive_20d}/{n_total} regions positive 20d — broad risk-off"
    elif pct_positive < 0.5 and us_intl_diff > 3:
        regime = "US_LED_BULL"
        signal = f"US leading by {us_intl_diff:+.1f}pp over intl avg ({intl_avg:.1f}%) — narrow leadership"
    elif pct_positive < 0.5 and us_intl_diff < -3:
        regime = "ROW_LED_BULL"
        signal = f"ROW leading by {-us_intl_diff:+.1f}pp over US ({spy_20d:.1f}%) — rotation underway"
    elif pct_positive >= 0.7 and abs(us_intl_diff) <= 3:
        regime = "GLOBAL_BROAD_BULL"
        signal = f"{n_positive_20d}/{n_total} regions positive 20d, US-intl gap {us_intl_diff:+.1f}pp — synchronized advance"
    elif pct_positive >= 0.5 and us_intl_diff >= 3:
        regime = "US_LED_BULL"
        signal = f"US leading by {us_intl_diff:+.1f}pp — narrow but firm leadership"
    elif pct_positive >= 0.5 and us_intl_diff <= -3:
        regime = "ROW_LED_BULL"
        signal = f"ROW leading by {-us_intl_diff:+.1f}pp — possible US underperformance regime"
    else:
        regime = "DIVERGENT"
        signal = f"{n_positive_20d}/{n_total} positive · US vs intl spread {us_intl_diff:+.1f}pp — mixed signals"

    return {
        "ranked_20d": [{"sym": r["sym"], "name": r["name"], "region": r["region"],
                          "ret_20d": r.get("ret_20d"), "ret_5d": r.get("ret_5d"),
                          "ret_60d": r.get("ret_60d"), "ret_ytd": r.get("ret_ytd"),
                          "vol_60d_ann_pct": r.get("vol_60d_ann_pct"),
                          "rs_vs_spy_20d": r.get("rs_vs_spy_20d")}
                         for r in ranked_20d],
        "top_3_by_20d": [r["sym"] for r in ranked_20d[:3]],
        "bottom_3_by_20d": [r["sym"] for r in ranked_20d[-3:]],
        "top_3_by_5d": [r["sym"] for r in ranked_5d[:3]],
        "top_3_by_ytd": [r["sym"] for r in ranked_ytd[:3]],
        "pairs": pairs,
        "spy_20d": spy_20d,
        "intl_avg_20d": round(intl_avg, 2),
        "us_minus_intl_20d_pp": round(us_intl_diff, 2),
        "n_positive_20d": n_positive_20d,
        "n_total": n_total,
        "pct_positive": round(pct_positive * 100, 1),
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
    print(f"=== global-markets v{VERSION} · {datetime.now(timezone.utc).isoformat()} ===")
    print(f"  universe: {len(UNIVERSE)} ETFs")

    try:
        prior = json.loads(s3.get_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY)["Body"].read())
        prior_regime = prior.get("composite_regime")
    except Exception:
        prior_regime = None

    # Parallel fetch
    results = []
    with ThreadPoolExecutor(max_workers=MAX_PARALLEL) as ex:
        futures = {ex.submit(analyze_etf, m): m["sym"] for m in UNIVERSE}
        for f in as_completed(futures):
            r = f.result()
            if r.get("err"):
                print(f"  ✗ {r['sym']}: {r['err']}")
            else:
                print(f"  ✓ {r['sym']:6s} {r.get('current'):>8.2f} "
                      f"1d={r.get('change_1d_pct'):+.2f}% "
                      f"5d={r.get('ret_5d'):+.2f}% 20d={r.get('ret_20d'):+.2f}% "
                      f"ytd={r.get('ret_ytd'):+.2f}% σ={r.get('vol_60d_ann_pct')}%")
            results.append(r)

    composite = composite_analysis(results)
    by_sym = {r["sym"]: {k: v for k, v in r.items() if k not in ("closes_last_60", "dates_last_60")}
               for r in results}

    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "generated_at_unix": int(time.time()),
        "version": VERSION,
        "source": "FMP /stable/quote + /stable/historical-price-eod/full",
        "elapsed_seconds": round(time.time() - started, 1),
        "n_etfs": len(UNIVERSE),
        "n_with_data": sum(1 for r in results if not r.get("err")),
        "n_with_err": sum(1 for r in results if r.get("err")),
        "by_sym": by_sym,
        "composite": composite,
        "composite_regime": composite["composite_regime"],
        "composite_signal": composite["composite_signal"],
        "regime_changed_from_prior": (prior_regime != composite["composite_regime"]) if prior_regime else False,
        # 60-day price history per ETF (for charting)
        "history_60d": {r["sym"]: {"dates": r.get("dates_last_60", []),
                                       "closes": r.get("closes_last_60", [])}
                         for r in results if not r.get("err")},
    }

    try:
        s3.put_object(Bucket=S3_BUCKET, Key=OUTPUT_KEY,
            Body=json.dumps(payload, separators=(",", ":"), default=str).encode("utf-8"),
            ContentType="application/json", CacheControl="public, max-age=900")
        print(f"  ✓ global-markets.json written")
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"err": f"put: {e}"})}

    # Telegram on regime change or extreme dispersion
    alert_sent = False
    rc = (prior_regime != composite["composite_regime"]) if prior_regime else False
    extreme_diff = abs(composite["us_minus_intl_20d_pp"]) >= 5
    if rc or extreme_diff:
        lines = [f"🌍 *Global Markets · {datetime.now(timezone.utc).strftime('%b %d %H:%M')} UTC*\n",
                  f"⚡ {composite['composite_regime']}",
                  f"_{composite['composite_signal']}_\n",
                  f"📊 SPY: {composite['spy_20d']:+.1f}% 20d · Intl avg: {composite['intl_avg_20d']:+.1f}%",
                  f"🥇 Top 3 (20d): {' · '.join(composite['top_3_by_20d'])}",
                  f"🥉 Bottom 3: {' · '.join(composite['bottom_3_by_20d'])}",
                  f"⚖️  QQQ-SPY: {composite['pairs']['qqq_minus_spy_20d']:+.1f}pp · "
                  f"IWM-SPY: {composite['pairs']['iwm_minus_spy_20d']:+.1f}pp"]
        if prior_regime and prior_regime != composite["composite_regime"]:
            lines.insert(2, f"_(was {prior_regime})_")
        alert_sent = send_telegram("\n".join(lines))
        if alert_sent: print("  ✓ telegram sent")

    return {"statusCode": 200, "body": json.dumps({
        "success": True, "version": VERSION,
        "n_etfs": len(UNIVERSE),
        "n_with_data": payload["n_with_data"],
        "composite_regime": composite["composite_regime"],
        "spy_20d": composite["spy_20d"],
        "intl_avg_20d": composite["intl_avg_20d"],
        "us_minus_intl_20d_pp": composite["us_minus_intl_20d_pp"],
        "top_3": composite["top_3_by_20d"],
        "alert_sent": alert_sent,
        "elapsed_seconds": payload["elapsed_seconds"],
    })}
