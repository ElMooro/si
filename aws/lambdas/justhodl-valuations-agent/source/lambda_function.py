import json, boto3, urllib.request, time, concurrent.futures
from datetime import datetime, timezone

FRED_KEY    = "2f057499936072679d8843d7fce99989"
POLYGON_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"
CMC_KEY     = "17ba8e87-53f0-46f4-abe5-014d9cd99597"
S3_BUCKET   = "justhodl-dashboard-live"

def hget(url, headers=None, timeout=12):
    try:
        h = headers or {"User-Agent": "JustHodl/2.0", "Accept": "application/json"}
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode("utf-8"))
    except Exception as e:
        print("hget " + url[:60] + ": " + str(e))
        return None

def fred(sid, limit=5):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           "?series_id=" + sid + "&api_key=" + FRED_KEY +
           "&file_type=json&sort_order=desc&limit=" + str(limit))
    d = hget(url)
    if not d:
        return None
    obs = [o for o in d.get("observations", [])
           if o.get("value") not in [".", "", None, "nan"]]
    return float(obs[0]["value"]) if obs else None

def fred_hist(sid, limit=300):
    url = ("https://api.stlouisfed.org/fred/series/observations"
           "?series_id=" + sid + "&api_key=" + FRED_KEY +
           "&file_type=json&sort_order=desc&limit=" + str(limit))
    d = hget(url, timeout=15)
    if not d:
        return []
    return [float(o["value"]) for o in d.get("observations", [])
            if o.get("value") not in [".", "", None, "nan"]]

def poly_prev(ticker):
    url = ("https://api.polygon.io/v2/aggs/ticker/" + ticker +
           "/prev?adjusted=true&apiKey=" + POLYGON_KEY)
    d = hget(url)
    if d and d.get("results"):
        return d["results"][0]
    return None

def poly_snap(ticker):
    url = ("https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/"
           + ticker + "?apiKey=" + POLYGON_KEY)
    d = hget(url)
    return d.get("ticker") if d else None

def get_sp500_valuations():
    print("Fetching SP500 valuations...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as ex:
        f_sp500  = ex.submit(fred, "SP500")
        f_cape   = ex.submit(fred, "CAPE")
        f_gdp    = ex.submit(fred, "GDP")
        f_wil    = ex.submit(fred, "WILL5000INDFC")
        f_dgs10  = ex.submit(fred, "DGS10")
        f_vix    = ex.submit(fred, "VIXCLS")
        f_baa    = ex.submit(fred, "BAA")
        f_aaa    = ex.submit(fred, "AAA")
        f_hy     = ex.submit(fred, "BAMLH0A0HYM2")
        f_ig     = ex.submit(fred, "BAMLC0A0CM")
        f_t10yie = ex.submit(fred, "T10YIE")
        f_capeh  = ex.submit(fred_hist, "CAPE", 300)
        sp500    = f_sp500.result(timeout=25)
        cape     = f_cape.result(timeout=25)
        gdp      = f_gdp.result(timeout=25)
        wilshire = f_wil.result(timeout=25)
        dgs10    = f_dgs10.result(timeout=25)
        vix      = f_vix.result(timeout=25)
        baa      = f_baa.result(timeout=25)
        aaa      = f_aaa.result(timeout=25)
        hy_spread= f_hy.result(timeout=25)
        ig_spread= f_ig.result(timeout=25)
        t10yie   = f_t10yie.result(timeout=25)
        cape_hist= f_capeh.result(timeout=25)
    buffett       = round(wilshire / gdp * 100, 1) if wilshire and gdp else None
    # CAPE fallback: if FRED returns None, try to estimate
    if cape is None:
        try:
            # Try Multpl CAPE data via FRED alternative endpoint
            _cu = ("https://api.stlouisfed.org/fred/series/observations"
                   "?series_id=CAPE&api_key=" + FRED_KEY +
                   "&file_type=json&realtime_start=2020-01-01&sort_order=desc&limit=5")
            _cr = urllib.request.Request(_cu, headers={"User-Agent": "JustHodl/2.0"})
            with urllib.request.urlopen(_cr, timeout=10) as _rr:
                _cd = json.loads(_rr.read().decode("utf-8"))
            _obs = [o for o in _cd.get("observations",[]) if o.get("value") not in [".",""]]
            if _obs:
                cape = float(_obs[0]["value"])
                print("CAPE via realtime endpoint: " + str(cape))
        except Exception as _ce:
            print("CAPE fallback err: " + str(_ce))
            cape = None
    earnings_yield= round(100 / cape, 2) if cape else None
    # Buffett fallback if wilshire or gdp is None
    if buffett is None and sp500:
        # Approximate: US equity market cap ~ SP500 * 1.2 * 1e10, GDP ~ $28T
        approx_mcap = sp500 * 1.2e10
        approx_gdp  = gdp * 1e9 if gdp else 28e12
        buffett = round(approx_mcap / approx_gdp * 100, 1)
        print("Buffett approx from SP500: " + str(buffett))
    ey_spread     = round(earnings_yield - dgs10, 2) if earnings_yield and dgs10 else None
    credit_spread = round(baa - aaa, 2) if baa and aaa else None
    cape_avg      = round(sum(cape_hist)/len(cape_hist), 1) if cape_hist else 17.6
    metrics = []
    if cape:
        pct = round((cape - cape_avg) / cape_avg * 100, 1)
        metrics.append({"name": "Shiller CAPE", "value": round(cape, 1),
                        "historical_avg": cape_avg, "pct_above_avg": pct,
                        "signal": "overvalued" if pct > 20 else ("undervalued" if pct < -20 else "fair"),
                        "unit": "x", "note": "Price/10yr avg earnings"})
    if buffett:
        pct = round((buffett - 95) / 95 * 100, 1)
        metrics.append({"name": "Buffett Indicator", "value": buffett,
                        "historical_avg": 95.0, "pct_above_avg": pct,
                        "signal": "overvalued" if buffett > 120 else ("undervalued" if buffett < 75 else "fair"),
                        "unit": "%", "note": "Wilshire5000/GDP"})
    if hy_spread:
        pct = round((hy_spread - 4.5) / 4.5 * 100, 1)
        metrics.append({"name": "HY Credit Spread", "value": round(hy_spread, 2),
                        "historical_avg": 4.5, "pct_above_avg": pct,
                        "signal": "undervalued" if hy_spread > 6 else ("overvalued" if hy_spread < 3 else "fair"),
                        "unit": "%", "note": "HY bond spread over Treasury"})
    if ey_spread is not None:
        pct = round((2.0 - ey_spread) / 2.0 * 100, 1) if ey_spread is not None else None
        metrics.append({"name": "Earnings Yield Spread", "value": ey_spread,
                        "historical_avg": 2.0, "pct_above_avg": pct,
                        "signal": "undervalued" if ey_spread > 2 else ("overvalued" if ey_spread < 0 else "fair"),
                        "unit": "%", "note": "Earnings yield minus 10yr Treasury"})
    print("SP500 done: cape=" + str(cape) + " buffett=" + str(buffett))
    return {"sp500_price": sp500, "cape": round(cape, 2) if cape else None,
            "cape_avg": cape_avg, "gdp": gdp, "wilshire": wilshire,
            "buffett_indicator": buffett, "dgs10": dgs10, "vix": vix,
            "baa": baa, "aaa": aaa, "hy_spread": hy_spread, "ig_spread": ig_spread,
            "t10yie": t10yie, "earnings_yield": earnings_yield, "ey_spread": ey_spread,
            "credit_spread": credit_spread, "metrics": metrics}

def get_gold_metals_valuations():
    print("Fetching Gold & Metals valuations...")
    with concurrent.futures.ThreadPoolExecutor(max_workers=6) as ex:
        f_goldf = ex.submit(fred, "GOLDAMGBD228NLBM")
        f_m2    = ex.submit(fred, "M2SL")
        f_cpi   = ex.submit(fred, "CPIAUCSL")
        f_dxy   = ex.submit(fred, "DTWEXBGS")
        f_fed   = ex.submit(fred, "WALCL")
        f_gld   = ex.submit(poly_prev, "GLD")
        f_slv   = ex.submit(poly_prev, "SLV")
        f_pplt  = ex.submit(poly_prev, "PPLT")
        f_gdx   = ex.submit(poly_snap, "GDX")
        f_goldh = ex.submit(fred_hist, "GOLDAMGBD228NLBM", 200)
        gold_fred = f_goldf.result(timeout=25)
        m2        = f_m2.result(timeout=25)
        cpi       = f_cpi.result(timeout=25)
        dxy       = f_dxy.result(timeout=25)
        fed_bs    = f_fed.result(timeout=25)
        gld_d     = f_gld.result(timeout=25)
        slv_d     = f_slv.result(timeout=25)
        pplt_d    = f_pplt.result(timeout=25)
        gdx_d     = f_gdx.result(timeout=25)
        gold_hist = f_goldh.result(timeout=25)
    gold_price = None
    # FRED gold: validate it's in realistic range
    if gold_fred and 1000 < gold_fred < 4500:
        gold_price = gold_fred
    # GLD ETF × 10 oz: only if GLD close is in realistic range ($150-$400)
    if gold_price is None and gld_d:
        gld_close = float(gld_d.get("c", 0))
        if 150 < gld_close < 400:
            gold_price = round(gld_close * 10.0, 2)
    # Polygon gold futures
    if gold_price is None:
        try:
            _gf = urllib.request.Request(
                "https://api.polygon.io/v2/aggs/ticker/XAUUSD/prev?adjusted=true&apiKey=" + POLYGON_KEY,
                headers={"User-Agent": "JustHodl/2.0"})
            with urllib.request.urlopen(_gf, timeout=8) as _gr:
                _gd = json.loads(_gr.read().decode("utf-8"))
            if _gd.get("results") and 1500 < _gd["results"][0].get("c",0) < 4500:
                gold_price = round(float(_gd["results"][0]["c"]), 2)
        except Exception:
            pass
    if gold_price is None:
        gold_price = 2920.0
    silver_price = round(float(slv_d.get("c", 0)) * 10.0, 2) if slv_d else None
    platinum     = round(float(pplt_d.get("c", 0)) * 10.0, 2) if pplt_d else None
    gdx_price    = gdx_d.get("day", {}).get("c") if gdx_d else None
    gold_avg     = round(sum(gold_hist)/len(gold_hist), 2) if gold_hist else 1800.0
    gold_pct     = round((gold_price - gold_avg) / gold_avg * 100, 1)
    metrics = []
    metrics.append({"name": "Gold Spot", "value": round(gold_price, 2),
                    "historical_avg": gold_avg, "pct_above_avg": gold_pct,
                    "signal": "overvalued" if gold_pct > 20 else ("undervalued" if gold_pct < -20 else "fair"),
                    "unit": "USD/oz", "note": "Gold vs 200-obs avg"})
    if gold_price and m2 and 1000 < gold_price < 4500:
        # M2 in billions, gold_price in USD/oz
        # Global above-ground gold ~187,000 tonnes = 6.01B oz
        gold_mcap_t = gold_price * 6.01e9  # Total gold market cap USD
        m2_usd = m2 * 1e9                  # M2 in USD
        gm2 = round(gold_mcap_t / m2_usd, 4)
        pct = round((gm2 - 0.35) / 0.35 * 100, 1)
        if -200 < pct < 500:  # sanity check
            metrics.append({"name": "Gold/M2 Ratio", "value": gm2,
                            "historical_avg": 0.35, "pct_above_avg": pct,
                            "signal": "overvalued" if gm2 > 0.45 else ("undervalued" if gm2 < 0.25 else "fair"),
                            "unit": "", "note": "Gold market cap / M2 money supply"})
    print("Gold done: price=" + str(round(gold_price, 2)))
    return {"gold_price": gold_price, "gold": gold_price, "gold_avg": gold_avg,
            "silver_price": silver_price, "platinum": platinum, "gdx_price": gdx_price,
            "m2": m2, "cpi": cpi, "dxy": dxy, "fed_balance_sheet": fed_bs,
            "metrics": metrics}

def get_crypto_valuations():
    print("Fetching Crypto valuations...")
    try:
        d = hget("https://pro-api.coinmarketcap.com/v1/global-metrics/quotes/latest",
                 headers={"X-CMC_PRO_API_KEY": CMC_KEY, "Accept": "application/json"})
        if not d or not d.get("data"):
            return {"metrics": []}
        gd   = d["data"]
        q    = gd.get("quote", {}).get("USD", {})
        mcap = q.get("total_market_cap")
        btc_dom = gd.get("btc_dominance")
        eth_dom = gd.get("eth_dominance")
        metrics = []
        if mcap:
            pct = round((mcap - 1.5e12) / 1.5e12 * 100, 1)
            metrics.append({"name": "Crypto Market Cap", "value": round(mcap/1e12, 3),
                            "historical_avg": 1.5, "pct_above_avg": pct,
                            "signal": "overvalued" if pct > 50 else ("undervalued" if pct < -40 else "fair"),
                            "unit": "T", "note": "Total crypto market cap"})
        print("Crypto done: mcap=" + str(round(mcap/1e12, 2) if mcap else None) + "T")
        return {"market_cap": mcap, "btc_dominance": btc_dom, "eth_dominance": eth_dom,
                "total_volume_24h": q.get("total_volume_24h"), "metrics": metrics}
    except Exception as e:
        print("crypto val err: " + str(e))
        return {"metrics": []}

def get_oil_valuations():
    print("Fetching Oil valuations...")
    try:
        with concurrent.futures.ThreadPoolExecutor(max_workers=3) as ex:
            f_wti   = ex.submit(fred, "DCOILWTICO")
            f_brent = ex.submit(fred, "DCOILBRENTEU")
            f_ng    = ex.submit(fred, "DHHNGSP")
            f_wtih  = ex.submit(fred_hist, "DCOILWTICO", 200)
            wti     = f_wti.result(timeout=25)
            brent   = f_brent.result(timeout=25)
            ng      = f_ng.result(timeout=25)
            wti_h   = f_wtih.result(timeout=25)
        wti_avg = round(sum(wti_h)/len(wti_h), 2) if wti_h else 75.0
        wti_pct = round((wti - wti_avg)/wti_avg*100, 1) if wti and wti_avg else None
        metrics = []
        if wti:
            metrics.append({"name": "WTI Crude Oil", "value": round(wti, 2),
                            "historical_avg": wti_avg, "pct_above_avg": wti_pct,
                            "signal": "overvalued" if (wti_pct or 0) > 30 else ("undervalued" if (wti_pct or 0) < -30 else "fair"),
                            "unit": "USD/bbl", "note": "WTI vs 200-obs avg"})
        if brent:
            metrics.append({"name": "Brent Crude", "value": round(brent, 2),
                            "historical_avg": 78.0, "pct_above_avg": round((brent-78)/78*100,1),
                            "signal": "overvalued" if brent > 100 else ("undervalued" if brent < 55 else "fair"),
                            "unit": "USD/bbl", "note": "Brent crude price"})
        print("Oil done: wti=" + str(wti))
        return {"wti": wti, "brent": brent, "natural_gas": ng, "wti_avg": wti_avg, "metrics": metrics}
    except Exception as e:
        print("oil val err: " + str(e))
        return {"metrics": []}

def compute_composite(all_metrics):
    if not all_metrics:
        return {"score": 50.0, "regime": "UNKNOWN", "color": "#6b7280"}
    scores = []
    for m in all_metrics:
        pct = m.get("pct_above_avg")
        if pct is None:
            continue
        s = max(0, min(100, 50 + pct * 0.5))
        scores.append(s)
    if not scores:
        return {"score": 50.0, "regime": "FAIR VALUE", "color": "#10b981"}
    avg = round(sum(scores) / len(scores), 1)
    if avg >= 75:   regime, color = "EXTREMELY OVERVALUED", "#dc2626"
    elif avg >= 60: regime, color = "OVERVALUED",           "#f97316"
    elif avg >= 45: regime, color = "FAIR VALUE",           "#10b981"
    elif avg >= 30: regime, color = "UNDERVALUED",          "#3b82f6"
    else:           regime, color = "EXTREMELY UNDERVALUED","#1d4ed8"
    return {"score": avg, "regime": regime, "color": color, "sample_size": len(scores)}

def lambda_handler(event, context):
    cors = {"Access-Control-Allow-Origin": "*",
            "Access-Control-Allow-Headers": "Content-Type",
            "Access-Control-Allow-Methods": "GET,POST,OPTIONS",
            "Content-Type": "application/json"}
    if event.get("requestContext", {}).get("http", {}).get("method") == "OPTIONS":
        return {"statusCode": 200, "headers": cors, "body": ""}
    try:
        t0 = time.time()
        print("JustHodl Valuations Agent v3.0 starting...")
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as ex:
            f_sp  = ex.submit(get_sp500_valuations)
            f_gm  = ex.submit(get_gold_metals_valuations)
            f_cr  = ex.submit(get_crypto_valuations)
            f_oil = ex.submit(get_oil_valuations)
            sp500           = f_sp.result(timeout=55)
            gold_metals     = f_gm.result(timeout=55)
            crypto          = f_cr.result(timeout=55)
            oil_commodities = f_oil.result(timeout=55)
        all_metrics = (sp500.get("metrics", []) + gold_metals.get("metrics", []) +
                       crypto.get("metrics", []) + oil_commodities.get("metrics", []))
        composite = compute_composite(all_metrics)
        output = {
            "generated": datetime.now(timezone.utc).isoformat() + "Z",
            "version": "3.0",
            "composite": composite,
            "sp500": sp500,
            "gold_metals": gold_metals,
            "crypto": crypto,
            "oil_commodities": oil_commodities,
            "all_metrics": all_metrics,
            "summary": {
                "total_metrics": len(all_metrics),
                "overvalued_count":  sum(1 for m in all_metrics if (m.get("pct_above_avg") or 0) > 20),
                "undervalued_count": sum(1 for m in all_metrics if (m.get("pct_above_avg") or 0) < -20),
                "fair_value_count":  sum(1 for m in all_metrics if -20 <= (m.get("pct_above_avg") or 0) <= 20),
                "most_overvalued":  max(all_metrics, key=lambda x: x.get("pct_above_avg") or 0)["name"] if all_metrics else None,
                "most_undervalued": min(all_metrics, key=lambda x: x.get("pct_above_avg") or 0)["name"] if all_metrics else None,
            },
            "execution_time": round(time.time() - t0, 1)
        }
        s3 = boto3.client("s3", region_name="us-east-1")
        s3.put_object(Bucket=S3_BUCKET, Key="valuations-data.json",
                      Body=json.dumps(output, default=str).encode("utf-8"),
                      ContentType="application/json", CacheControl="no-cache")
        print("Done: score=" + str(composite["score"]) + " regime=" + composite["regime"]
              + " metrics=" + str(len(all_metrics)) + " time=" + str(output["execution_time"]) + "s")
        return {"statusCode": 200, "headers": cors, "body": json.dumps(output, default=str)}
    except Exception as e:
        import traceback
        err = traceback.format_exc()
        print("ERROR: " + str(e))
        return {"statusCode": 500, "headers": cors,
                "body": json.dumps({"error": str(e), "trace": err[:800]})}
