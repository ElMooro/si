"""
justhodl-hkma-monitor — Hong Kong Monetary Authority funding monitor.

WHY: Hong Kong is a primary offshore-USD pressure point. The peg plumbing is the
tell: when USD/HKD hits the 7.85 weak-side Convertibility Undertaking, the HKMA
buys HKD / sells USD, which DRAINS the Aggregate Balance (the sum of banks'
clearing balances at the HKMA) — local HKD liquidity tightens and HIBOR jumps.
So Aggregate Balance + peg distance + HIBOR-vs-SOFR are the real funding signals,
not USD/HKD spot alone.

DATA (HKMA Open API, free, no key — https://api.hkma.gov.hk):
  - daily-figures-interbank-liquidity: closing Aggregate Balance, overnight & 1M
    HIBOR, Discount Window Base Rate, 7.75/7.85 CU levels, TWI (one call).
  - hk-interbank-ir-daily?segment=hibor.fixing: full HIBOR curve (1W..12M).
  + FRED SOFR (HIBOR-SOFR spread) + FMP USD/HKD spot (peg distance).

OUT: data/hkma.json — reusable HK funding feed for eurodollar-plumbing and any
other consumer. Each metric green/yellow/red → composite hk_funding status.
"""
import os, json, time, statistics, urllib.request, urllib.parse, datetime

import boto3

BUCKET = os.environ.get("S3_BUCKET", "justhodl-dashboard-live")
FRED_KEY = os.environ.get("FRED_API_KEY", "2f057499936072679d8843d7fce99989")
FMP_KEY = os.environ.get("FMP_API_KEY", "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb")
S3 = boto3.client("s3", region_name="us-east-1")
OUT_KEY = "data/hkma.json"

HKMA = "https://api.hkma.gov.hk/public"
IBL = HKMA + "/market-data-and-statistics/daily-monetary-statistics/daily-figures-interbank-liquidity"
HIBOR = HKMA + "/market-data-and-statistics/monthly-statistical-bulletin/er-ir/hk-interbank-ir-daily"
WEAK_SIDE = 7.85
STRONG_SIDE = 7.75


def http_get(url, timeout=25):
    req = urllib.request.Request(url, headers={"User-Agent": "Mozilla/5.0 JustHodl/HKMA"})
    return urllib.request.urlopen(req, timeout=timeout).read()


def num(v):
    try:
        if v in (None, "", "-"):
            return None
        return float(v)
    except (TypeError, ValueError):
        return None


def hkma_records(base_url, segment=None, pages=10, pagesize=100):
    """Paginate HKMA API (sorted desc by date). Returns combined records list."""
    out = []
    for p in range(pages):
        params = {"pagesize": pagesize, "offset": p * pagesize, "sortorder": "desc"}
        if segment:
            params["segment"] = segment
        url = base_url + "?" + urllib.parse.urlencode(params)
        try:
            d = json.loads(http_get(url).decode("utf-8", "replace"))
        except Exception as e:
            print("[hkma] %s page %d: %s" % (base_url.split("/")[-1], p, e))
            break
        recs = (d.get("result") or {}).get("records") or []
        out.extend(recs)
        if len(recs) < pagesize:
            break
    return out


def fred_latest(series_id):
    qs = urllib.parse.urlencode({"series_id": series_id, "api_key": FRED_KEY, "file_type": "json",
                                 "sort_order": "desc", "limit": 5})
    try:
        d = json.loads(http_get("https://api.stlouisfed.org/fred/series/observations?" + qs).decode())
        for o in d.get("observations", []):
            v = num(o.get("value"))
            if v is not None:
                return v, o["date"]
    except Exception as e:
        print("[hkma] fred %s: %s" % (series_id, e))
    return None, None


def fmp_spot(symbol):
    try:
        url = "https://financialmodelingprep.com/stable/quote?symbol=%s&apikey=%s" % (symbol, FMP_KEY)
        d = json.loads(http_get(url).decode())
        row = d[0] if isinstance(d, list) and d else (d if isinstance(d, dict) else {})
        return num(row.get("price"))
    except Exception as e:
        print("[hkma] fmp %s: %s" % (symbol, e))
        return None


def pctile(value, hist):
    hist = [h for h in hist if h is not None]
    if value is None or len(hist) < 20:
        return None
    return round(sum(1 for h in hist if h <= value) / len(hist) * 100, 1)


def flag(value, green_max, yellow_max, higher_is_worse=True):
    if value is None:
        return "info"
    if higher_is_worse:
        return "green" if value <= green_max else "yellow" if value <= yellow_max else "red"
    return "green" if value >= green_max else "yellow" if value >= yellow_max else "red"


def lambda_handler(event, context):
    t0 = time.time()

    # ---- interbank liquidity: Aggregate Balance + O/N & 1M HIBOR + base rate + CU + TWI ----
    ibl = hkma_records(IBL, pages=12, pagesize=100)   # ~1200 business days ≈ 4–5y
    # records carry end_of_date desc; build ascending AB history in HK$bn
    ab_hist = []
    for r in ibl:
        d, cb = r.get("end_of_date"), num(r.get("closing_balance"))
        if d and cb is not None:
            ab_hist.append((d, round(cb / 1000.0, 2)))   # HK$m → HK$bn
    ab_hist.sort()
    latest = ibl[0] if ibl else {}
    ab_latest = round(num(latest.get("closing_balance")) / 1000.0, 2) if num(latest.get("closing_balance")) is not None else None
    ab_vals = [v for _, v in ab_hist]
    ab_pct = pctile(ab_latest, ab_vals)
    ab_trend = None
    if len(ab_hist) > 22:
        ab_trend = round(ab_hist[-1][1] - ab_hist[-22][1], 2)
    base_rate = num(latest.get("disc_win_base_rate"))
    hibor_on = num(latest.get("hibor_overnight"))
    hibor_1m = num(latest.get("hibor_fixing_1m"))
    twi = num(latest.get("twi"))
    as_of = latest.get("end_of_date")

    # ---- full HIBOR curve (3M is the key funding tenor) ----
    hibor_curve = {}
    hib = hkma_records(HIBOR, segment="hibor.fixing", pages=1, pagesize=5)
    if hib:
        hr = hib[0]
        # parse defensively: keys carry tenor tokens (overnight/1w/1m/3m/6m/12m)
        for k, v in hr.items():
            kl = k.lower()
            val = num(v)
            if val is None:
                continue
            for tag, out in (("overnight", "overnight"), ("1week", "1w"), ("_1m", "1m"),
                             ("_3m", "3m"), ("_6m", "6m"), ("_12m", "12m"), ("1month", "1m"),
                             ("3month", "3m"), ("6month", "6m"), ("12month", "12m")):
                if tag in kl and out not in hibor_curve:
                    hibor_curve[out] = val
        if hr.get("end_of_date"):
            hibor_curve["as_of"] = hr["end_of_date"]
    hibor_3m = hibor_curve.get("3m") or num(latest.get("hibor_fixing_3m"))
    if hibor_1m is None:
        hibor_1m = hibor_curve.get("1m")

    # ---- USD/HKD spot + peg distance ----
    spot = fmp_spot("USDHKD")
    dist_to_weak = round((WEAK_SIDE - spot) / WEAK_SIDE * 100, 3) if spot else None  # % room before 7.85
    at_weak = bool(spot and spot >= 7.84)

    # ---- HIBOR – SOFR (offshore HKD funding cost vs USD risk-free) ----
    sofr, sofr_date = fred_latest("SOFR")
    hibor_ref = hibor_3m if hibor_3m is not None else hibor_1m
    hibor_sofr_bp = round((hibor_ref - sofr) * 100, 1) if (hibor_ref is not None and sofr is not None) else None

    # ---- grade ----
    metrics = []

    def add(mid, label, value, unit, status, detail, pct=None):
        metrics.append({"id": mid, "label": label, "value": value, "unit": unit,
                        "status": status, "detail": detail, "pctile": pct, "asof": as_of})

    # Aggregate Balance: lower percentile = tighter local liquidity
    ab_status = "info" if ab_pct is None else ("red" if ab_pct < 10 else "yellow" if ab_pct < 25 else "green")
    add("agg_balance", "Aggregate Balance", ab_latest, "HK$bn", ab_status,
        "Sum of banks' HKMA clearing balances — the HK liquidity buffer; drained when HKMA defends the 7.85 weak side. "
        "30d change %s HK$bn." % (("+%s" % ab_trend) if (ab_trend or 0) >= 0 else ab_trend), ab_pct)
    # Peg distance
    peg_status = "info" if dist_to_weak is None else ("red" if dist_to_weak < 0.05 else "yellow" if dist_to_weak < 0.20 else "green")
    add("usd_hkd", "USD/HKD vs 7.85 weak side", spot, "", peg_status,
        "Band 7.75–7.85. At 7.85 the HKMA sells USD/buys HKD → Aggregate Balance shrinks, HIBOR jumps. "
        "%s%% room to the weak side." % (dist_to_weak if dist_to_weak is not None else "?"))
    # HIBOR-SOFR: large negative = cheap HKD funding → carry shorts pressure peg to weak side; large positive = HKD tightness
    if hibor_sofr_bp is not None:
        hs_status = "green" if abs(hibor_sofr_bp) <= 75 else "yellow" if abs(hibor_sofr_bp) <= 150 else "red"
        add("hibor_sofr", "%s HIBOR − SOFR" % ("3M" if hibor_3m is not None else "1M"), hibor_sofr_bp, "bp", hs_status,
            "Offshore HKD funding cost vs USD risk-free. Deeply negative = cheap HKD → carry shorts push the peg weak; "
            "deeply positive = HKD liquidity squeeze.")
    if hibor_on is not None:
        add("hibor_on", "Overnight HIBOR", round(hibor_on, 3), "%", "info", "Overnight interbank HKD rate.")
    if base_rate is not None:
        add("base_rate", "Discount Window Base Rate", round(base_rate, 2), "%", "info",
            "HKMA policy base rate (tracks Fed via the peg).")

    reds = [m["label"] for m in metrics if m["status"] == "red"]
    yellows = [m["label"] for m in metrics if m["status"] == "yellow"]
    hk_funding = "TIGHT" if reds else "WATCH" if yellows else "EASY"

    payload = {
        "engine": "justhodl-hkma-monitor", "version": "1.0",
        "generated_at": datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z",
        "as_of": as_of,
        "hk_funding": hk_funding,
        "red_flags": reds, "yellow_flags": yellows,
        "aggregate_balance": {"latest_bn": ab_latest, "pctile": ab_pct, "trend_30d_bn": ab_trend,
                              "n_history": len(ab_hist), "window_from": ab_hist[0][0] if ab_hist else None,
                              "history": ab_hist[-260:]},
        "hibor": {"overnight": hibor_on, "1m": hibor_1m, "3m": hibor_3m, "curve": hibor_curve},
        "usd_hkd": {"spot": spot, "weak_side": WEAK_SIDE, "strong_side": STRONG_SIDE,
                    "distance_to_weak_pct": dist_to_weak, "at_weak_side": at_weak},
        "hibor_sofr_bp": hibor_sofr_bp, "sofr": {"value": sofr, "as_of": sofr_date},
        "base_rate": base_rate, "twi": twi,
        "metrics": metrics,
        "source": "HKMA Open API (interbank liquidity + HIBOR fixings), FRED SOFR, FMP USD/HKD",
        "honesty": "Peg plumbing, not spot alone: a dollar squeeze shows up via a draining Aggregate Balance and a "
                   "weak-side peg, not the USD/HKD level by itself. Threshold readings, not advice.",
        "duration_s": round(time.time() - t0, 1),
    }
    S3.put_object(Bucket=BUCKET, Key=OUT_KEY, Body=json.dumps(payload, indent=2, default=str).encode(),
                  ContentType="application/json", CacheControl="max-age=3600")
    print("[hkma] done %.1fs funding=%s AB=%s bn (pct %s) hibor3m=%s hibor-sofr=%sbp peg=%s" %
          (payload["duration_s"], hk_funding, ab_latest, ab_pct, hibor_3m, hibor_sofr_bp, spot))
    return {"statusCode": 200, "body": json.dumps({"ok": True, "hk_funding": hk_funding,
                                                   "agg_balance_bn": ab_latest, "n": len(ab_hist)})}
