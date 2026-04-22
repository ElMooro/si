import json
import os
import ssl
from datetime import datetime, timedelta, timezone
from urllib import request, parse
from collections import defaultdict
import traceback

# === S3 CACHE FOR /cot/all ===
import boto3 as _boto3_cache
from datetime import datetime as _dt_cache, timedelta as _td_cache

_s3_cache = _boto3_cache.client('s3', region_name='us-east-1')
_CACHE_BUCKET = 'justhodl-dashboard-live'
_CACHE_KEY = 'data/cftc-all-cache.json'

def _get_cached_cot_all():
    """Return cached /cot/all data if fresh (< 24 hours old)"""
    try:
        obj = _s3_cache.get_object(Bucket=_CACHE_BUCKET, Key=_CACHE_KEY)
        data = json.loads(obj['Body'].read())
        cached_at = data.get('cached_at', '')
        if cached_at:
            age = (_dt_cache.utcnow() - _dt_cache.fromisoformat(cached_at.replace('Z',''))).total_seconds()
            if age < 86400:  # 24 hours
                data['from_cache'] = True
                data['cache_age_hours'] = round(age / 3600, 1)
                return data
    except Exception as e:
        print(f'[CACHE] Miss: {e}')
    return None

def _save_cot_all_cache(data):
    """Save /cot/all response to S3 cache"""
    try:
        data['cached_at'] = _dt_cache.utcnow().isoformat() + 'Z'
        _s3_cache.put_object(
            Bucket=_CACHE_BUCKET,
            Key=_CACHE_KEY,
            Body=json.dumps(data),
            ContentType='application/json'
        )
        print('[CACHE] Saved /cot/all to S3')
    except Exception as e:
        print(f'[CACHE] Save error: {e}')
# === END CACHE ===


POLYGON_API_KEY = os.environ.get("POLYGON_API_KEY", "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d")
ctx = ssl.create_default_context()

COT_CONTRACTS = {
    "ES": {"name": "S&P 500 E-Mini", "cftc_code": "13874A", "category": "equity_index"},
    "NQ": {"name": "NASDAQ 100 E-Mini", "cftc_code": "209742", "category": "equity_index"},
    "YM": {"name": "Dow Jones E-Mini", "cftc_code": "124603", "category": "equity_index"},
    "RTY": {"name": "Russell 2000 E-Mini", "cftc_code": "239742", "category": "equity_index"},
    "VX": {"name": "VIX Futures", "cftc_code": "1170E1", "category": "volatility"},
    "ZB": {"name": "30-Year T-Bond", "cftc_code": "020601", "category": "treasury"},
    "ZN": {"name": "10-Year T-Note", "cftc_code": "043602", "category": "treasury"},
    "ZF": {"name": "5-Year T-Note", "cftc_code": "044601", "category": "treasury"},
    "ZT": {"name": "2-Year T-Note", "cftc_code": "042601", "category": "treasury"},
    "6E": {"name": "Euro FX", "cftc_code": "099741", "category": "currency"},
    "6J": {"name": "Japanese Yen", "cftc_code": "097741", "category": "currency"},
    "6B": {"name": "British Pound", "cftc_code": "096742", "category": "currency"},
    "6C": {"name": "Canadian Dollar", "cftc_code": "090741", "category": "currency"},
    "6S": {"name": "Swiss Franc", "cftc_code": "092741", "category": "currency"},
    "DX": {"name": "US Dollar Index", "cftc_code": "098662", "category": "currency"},
    "CL": {"name": "Crude Oil WTI", "cftc_code": "067651", "category": "energy"},
    "NG": {"name": "Natural Gas", "cftc_code": "023651", "category": "energy"},
    "RB": {"name": "RBOB Gasoline", "cftc_code": "111659", "category": "energy"},
    "HO": {"name": "Heating Oil", "cftc_code": "022651", "category": "energy"},
    "GC": {"name": "Gold", "cftc_code": "088691", "category": "metals"},
    "SI": {"name": "Silver", "cftc_code": "084691", "category": "metals"},
    "HG": {"name": "Copper", "cftc_code": "085692", "category": "metals"},
    "PL": {"name": "Platinum", "cftc_code": "076651", "category": "metals"},
    "ZC": {"name": "Corn", "cftc_code": "002602", "category": "agriculture"},
    "ZS": {"name": "Soybeans", "cftc_code": "005602", "category": "agriculture"},
    "ZW": {"name": "Wheat", "cftc_code": "001602", "category": "agriculture"},
    "CT": {"name": "Cotton", "cftc_code": "033661", "category": "agriculture"},
    "KC": {"name": "Coffee", "cftc_code": "083731", "category": "agriculture"},
    "SB": {"name": "Sugar", "cftc_code": "080732", "category": "agriculture"},
}

def fetch_url(url, timeout=30):
    try:
        req = request.Request(url, headers={"User-Agent": "JustHodl-Financial-Agent/1.0"})
        with request.urlopen(req, timeout=timeout, context=ctx) as resp:
            return resp.read().decode("utf-8")
    except Exception as e:
        print(f"[ERROR] Fetch {url}: {e}")
        return None

def safe_int(val):
    try:
        if val is None or val == "":
            return 0
        return int(float(str(val).replace(",", "")))
    except:
        return 0

def get_field(rec, *field_names):
    for f in field_names:
        val = rec.get(f)
        if val is not None and val != "":
            return safe_int(val)
    return 0

def fetch_cftc_cot_data():
    results = {}
    disagg_url = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"
    tff_url = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
    legacy_url = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
    financial_categories = {"equity_index", "treasury", "currency", "volatility"}
    today = datetime.now(timezone.utc)
    eight_weeks_ago = (today - timedelta(weeks=8)).strftime("%Y-%m-%dT00:00:00.000")

    for contract_key, contract_info in COT_CONTRACTS.items():
        cftc_code = contract_info["cftc_code"]
        category = contract_info.get("category", "")
        is_financial = category in financial_categories
        where_clause = f"cftc_contract_market_code='{cftc_code}' AND report_date_as_yyyy_mm_dd > '{eight_weeks_ago}'"
        params = parse.urlencode({"$where": where_clause, "$order": "report_date_as_yyyy_mm_dd DESC", "$limit": 8})

        try:
            records = None
            report_type = None

            if is_financial:
                data = fetch_url(f"{tff_url}?{params}")
                if data:
                    parsed = json.loads(data)
                    if parsed:
                        records = parsed
                        report_type = "tff"

            if not records:
                data = fetch_url(f"{disagg_url}?{params}")
                if data:
                    parsed = json.loads(data)
                    if parsed:
                        records = parsed
                        report_type = "disagg"

            if not records:
                data = fetch_url(f"{legacy_url}?{params}")
                if data:
                    parsed = json.loads(data)
                    if parsed:
                        records = parsed
                        report_type = "legacy"

            if records:
                results[contract_key] = parse_cot_records(contract_key, contract_info, records, report_type)
                print(f"[OK] {contract_key} ({contract_info['name']}): {len(records)} weeks via {report_type}")
            else:
                print(f"[MISS] {contract_key}: no data")
        except Exception as e:
            print(f"[WARN] {contract_key}: {e}")
    return results

def parse_cot_records(contract_key, contract_info, records, report_type="unknown"):
    parsed = {"contract": contract_key, "name": contract_info["name"], "category": contract_info["category"], "cftc_code": contract_info["cftc_code"], "report_type": report_type, "weekly_reports": [], "current": {}, "signals": {}}
    weekly_data = []

    for rec in records:
        report_date = rec.get("report_date_as_yyyy_mm_dd", "")[:10]
        open_interest = get_field(rec, "open_interest_all", "oi_all", "open_interest")

        if report_type == "tff":
            # TFF: Dealer, Asset Manager, Leveraged Money, Other
            dealer_long = get_field(rec, "dealer_positions_long_all", "dealer_positions_long")
            dealer_short = get_field(rec, "dealer_positions_short_all", "dealer_positions_short")
            asset_mgr_long = get_field(rec, "asset_mgr_positions_long_all", "asset_mgr_positions_long")
            asset_mgr_short = get_field(rec, "asset_mgr_positions_short_all", "asset_mgr_positions_short")
            lev_long = get_field(rec, "lev_money_positions_long_all", "lev_money_positions_long")
            lev_short = get_field(rec, "lev_money_positions_short_all", "lev_money_positions_short")
            other_long = get_field(rec, "other_rept_positions_long_all", "other_rept_positions_long")
            other_short = get_field(rec, "other_rept_positions_short_all", "other_rept_positions_short")

            # Speculators = Asset Managers + Leveraged Money
            noncomm_long = asset_mgr_long + lev_long
            noncomm_short = asset_mgr_short + lev_short
            # Commercials = Dealers
            comm_long = dealer_long
            comm_short = dealer_short
            mm_long = lev_long
            mm_short = lev_short

        elif report_type == "disagg":
            # Disaggregated: Producer, Swap, Managed Money, Other
            prod_long = get_field(rec, "prod_merc_positions_long_all", "prod_merc_positions_long")
            prod_short = get_field(rec, "prod_merc_positions_short_all", "prod_merc_positions_short")
            swap_long = get_field(rec, "swap_positions_long_all", "swap_positions_long")
            swap_short = get_field(rec, "swap__positions_short_all", "swap_positions_short_all", "swap_positions_short")
            mm_long = get_field(rec, "m_money_positions_long_all", "m_money_positions_long")
            mm_short = get_field(rec, "m_money_positions_short_all", "m_money_positions_short")
            other_long = get_field(rec, "other_rept_positions_long_all", "other_rept_positions_long")
            other_short = get_field(rec, "other_rept_positions_short_all", "other_rept_positions_short")

            noncomm_long = mm_long + swap_long
            noncomm_short = mm_short + swap_short
            comm_long = prod_long
            comm_short = prod_short

        else:
            # Legacy: Non-Commercial, Commercial
            noncomm_long = get_field(rec, "noncomm_positions_long_all", "noncl_positions_long_all")
            noncomm_short = get_field(rec, "noncomm_positions_short_all", "noncl_positions_short_all")
            comm_long = get_field(rec, "comm_positions_long_all")
            comm_short = get_field(rec, "comm_positions_short_all")
            mm_long = noncomm_long
            mm_short = noncomm_short

        net_speculator = noncomm_long - noncomm_short
        net_commercial = comm_long - comm_short
        net_managed_money = mm_long - mm_short
        spec_total = noncomm_long + noncomm_short
        spec_ratio = (noncomm_long / max(spec_total, 1)) * 100 if spec_total > 0 else 50

        week_data = {
            "report_date": report_date,
            "open_interest": open_interest,
            "noncommercial_long": noncomm_long,
            "noncommercial_short": noncomm_short,
            "net_speculator": net_speculator,
            "commercial_long": comm_long,
            "commercial_short": comm_short,
            "net_commercial": net_commercial,
            "managed_money_long": mm_long,
            "managed_money_short": mm_short,
            "net_managed_money": net_managed_money,
            "speculator_long_ratio": round(spec_ratio, 1)
        }
        weekly_data.append(week_data)

    if weekly_data:
        parsed["current"] = weekly_data[0]
        parsed["weekly_reports"] = weekly_data
        parsed["signals"] = generate_positioning_signals(weekly_data, contract_info)
    return parsed

def generate_positioning_signals(weekly_data, contract_info):
    signals = {"trend": "NEUTRAL", "strength": 0, "extreme": False, "reversal_risk": False, "weekly_change": 0, "description": ""}
    if len(weekly_data) < 2:
        return signals
    current = weekly_data[0]
    previous = weekly_data[1]
    net_change = current["net_speculator"] - previous["net_speculator"]
    signals["weekly_change"] = net_change
    net_spec = current["net_speculator"]
    if net_spec > 0:
        signals["trend"] = "BULLISH"
        signals["strength"] = min(abs(net_spec) / max(current["open_interest"], 1) * 100, 100)
    elif net_spec < 0:
        signals["trend"] = "BEARISH"
        signals["strength"] = min(abs(net_spec) / max(current["open_interest"], 1) * 100, 100)
    signals["strength"] = round(signals["strength"], 1)
    spec_ratio = current["speculator_long_ratio"]
    if spec_ratio > 80 or spec_ratio < 20:
        signals["extreme"] = True
        signals["reversal_risk"] = True
        signals["description"] = f"EXTREME positioning ({spec_ratio}% long) - high reversal risk"
    elif spec_ratio > 70 or spec_ratio < 30:
        signals["extreme"] = True
        signals["description"] = f"Elevated positioning ({spec_ratio}% long) - monitor for reversal"
    else:
        signals["description"] = f"Normal positioning ({spec_ratio}% long)"
    if len(weekly_data) >= 4:
        four_week_change = current["net_speculator"] - weekly_data[3]["net_speculator"]
        if four_week_change > 0 and net_change > 0:
            signals["description"] += " | 4-week accumulation trend"
        elif four_week_change < 0 and net_change < 0:
            signals["description"] += " | 4-week distribution trend"
    return signals

POLYGON_FUTURES_TICKERS = {
    "ES": {"polygon": "I:SPX", "name": "S&P 500"},
    "NQ": {"polygon": "I:NDX", "name": "NASDAQ 100"},
    "CL": {"polygon": "C:CLUSD", "name": "Crude Oil WTI"},
    "GC": {"polygon": "C:GCUSD", "name": "Gold"},
    "SI": {"polygon": "C:SIUSD", "name": "Silver"},
    "NG": {"polygon": "C:NGUSD", "name": "Natural Gas"},
    "6E": {"polygon": "C:EURUSD", "name": "EUR/USD"},
    "6J": {"polygon": "C:USDJPY", "name": "USD/JPY"},
    "6B": {"polygon": "C:GBPUSD", "name": "GBP/USD"},
    "BTC": {"polygon": "X:BTCUSD", "name": "Bitcoin"},
    "ETH": {"polygon": "X:ETHUSD", "name": "Ethereum"},
}

def fetch_polygon_futures_data():
    results = {}
    for key, info in POLYGON_FUTURES_TICKERS.items():
        try:
            ticker = info["polygon"]
            url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev?adjusted=true&apiKey={POLYGON_API_KEY}"
            data = fetch_url(url)
            if data:
                parsed = json.loads(data)
                if parsed.get("results"):
                    r = parsed["results"][0]
                    results[key] = {"ticker": ticker, "name": info["name"], "open": r.get("o"), "high": r.get("h"), "low": r.get("l"), "close": r.get("c"), "volume": r.get("v"), "vwap": r.get("vw"), "timestamp": r.get("t"), "change_pct": round(((r.get("c", 0) - r.get("o", 1)) / max(r.get("o", 1), 0.001)) * 100, 2)}
        except Exception as e:
            print(f"[WARN] Polygon {key}: {e}")
    return results

def generate_composite_analysis(cot_data, polygon_data):
    analysis = {"timestamp": datetime.now(timezone.utc).isoformat(), "market_positioning_score": 0, "risk_appetite_index": 50, "sector_positioning": {}, "extreme_positions": [], "reversal_candidates": [], "smart_money_flow": "NEUTRAL", "crisis_indicators": {}, "summary": ""}
    bullish_count = bearish_count = extreme_count = total_scored = 0
    category_scores = defaultdict(lambda: {"bullish": 0, "bearish": 0, "count": 0})
    for ck, cot in cot_data.items():
        sig = cot.get("signals", {})
        cur = cot.get("current", {})
        cat = cot.get("category", "other")
        if sig.get("trend") == "BULLISH":
            bullish_count += 1
            category_scores[cat]["bullish"] += 1
        elif sig.get("trend") == "BEARISH":
            bearish_count += 1
            category_scores[cat]["bearish"] += 1
        category_scores[cat]["count"] += 1
        total_scored += 1
        if sig.get("extreme"):
            extreme_count += 1
            analysis["extreme_positions"].append({"contract": ck, "name": cot.get("name"), "ratio": cur.get("speculator_long_ratio"), "net_position": cur.get("net_speculator"), "risk": "HIGH REVERSAL RISK" if sig.get("reversal_risk") else "ELEVATED"})
        if sig.get("reversal_risk"):
            analysis["reversal_candidates"].append({"contract": ck, "name": cot.get("name"), "positioning": sig.get("description"), "trend": sig.get("trend")})
    if total_scored > 0:
        analysis["market_positioning_score"] = round(((bullish_count - bearish_count) / total_scored) * 100, 1)
    eq = category_scores.get("equity_index", {"bullish": 0, "count": 1})
    tr = category_scores.get("treasury", {"bearish": 0, "count": 1})
    analysis["risk_appetite_index"] = round((eq.get("bullish",0) / max(eq.get("count",1), 1)) * 60 + (tr.get("bearish",0) / max(tr.get("count",1), 1)) * 40, 1)
    for cat, sc in category_scores.items():
        n = sc["bullish"] - sc["bearish"]
        analysis["sector_positioning"][cat] = {"bullish": sc["bullish"], "bearish": sc["bearish"], "total": sc["count"], "bias": "BULLISH" if n > 0 else "BEARISH" if n < 0 else "NEUTRAL"}
    cb = cs = 0
    for ck, cot in cot_data.items():
        nc = cot.get("current", {}).get("net_commercial", 0)
        if nc > 0: cb += 1
        elif nc < 0: cs += 1
    if cb > cs * 1.3: analysis["smart_money_flow"] = "ACCUMULATING"
    elif cs > cb * 1.3: analysis["smart_money_flow"] = "DISTRIBUTING"
    vx = cot_data.get("VX", {})
    zn = cot_data.get("ZN", {})
    gc = cot_data.get("GC", {})
    crisis = 0
    if vx.get("signals", {}).get("trend") == "BULLISH": crisis += 30
    if zn.get("signals", {}).get("trend") == "BULLISH": crisis += 20
    if gc.get("signals", {}).get("trend") == "BULLISH" and gc.get("signals", {}).get("extreme"): crisis += 25
    if extreme_count > len(cot_data) * 0.3: crisis += 25
    analysis["crisis_indicators"] = {"score": min(crisis, 100), "level": "CRITICAL" if crisis >= 75 else "ELEVATED" if crisis >= 50 else "MODERATE" if crisis >= 25 else "LOW", "vix_positioning": vx.get("signals", {}).get("trend", "N/A"), "treasury_flight": zn.get("signals", {}).get("trend", "N/A"), "gold_safe_haven": gc.get("signals", {}).get("trend", "N/A"), "crowded_trade_count": extreme_count}
    analysis["summary"] = f"Market Positioning: {analysis['market_positioning_score']}/100 | Risk Appetite: {analysis['risk_appetite_index']}/100 | Smart Money: {analysis['smart_money_flow']} | Crisis: {analysis['crisis_indicators']['score']}/100 ({analysis['crisis_indicators']['level']}) | Extremes: {extreme_count}/{total_scored} | Reversals: {len(analysis['reversal_candidates'])}"
    return analysis

def lambda_handler(event, context):
    headers = {"Content-Type": "application/json", "Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, POST, OPTIONS", "Access-Control-Allow-Headers": "Content-Type, Authorization", "X-Agent": "CFTC-Futures-Positioning-Agent", "X-Version": "1.2.0"}
    hm = event.get("requestContext", {}).get("http", {}).get("method", event.get("httpMethod", "GET"))
    if hm == "OPTIONS":
        return {"statusCode": 200, "headers": headers, "body": ""}
    path = event.get("rawPath", event.get("path", "/"))
    try:
        if "/debug/" in path:
            contract = path.split("/debug/")[-1].strip("/").upper()
            cftc_code = COT_CONTRACTS.get(contract, {}).get("cftc_code", "")
            cat = COT_CONTRACTS.get(contract, {}).get("category", "")
            is_fin = cat in {"equity_index", "treasury", "currency", "volatility"}
            today = datetime.now(timezone.utc)
            two_weeks = (today - timedelta(weeks=2)).strftime("%Y-%m-%dT00:00:00.000")
            wc = f"cftc_contract_market_code='{cftc_code}' AND report_date_as_yyyy_mm_dd > '{two_weeks}'"
            p = parse.urlencode({"$where": wc, "$order": "report_date_as_yyyy_mm_dd DESC", "$limit": 1})
            tff_url = "https://publicreporting.cftc.gov/resource/gpe5-46if.json"
            disagg_url = "https://publicreporting.cftc.gov/resource/72hh-3qpy.json"
            legacy_url = "https://publicreporting.cftc.gov/resource/6dca-aqww.json"
            raw = {}
            for name, url in [("tff", tff_url), ("disagg", disagg_url), ("legacy", legacy_url)]:
                d = fetch_url(f"{url}?{p}")
                if d:
                    parsed = json.loads(d)
                    raw[name] = parsed[0] if parsed else "empty"
                else:
                    raw[name] = "fetch_failed"
            return respond(200, {"contract": contract, "cftc_code": cftc_code, "category": cat, "is_financial": is_fin, "raw_responses": raw}, headers)

        elif "/health" in path:
            return respond(200, {"status": "healthy", "agent": "CFTC Futures Positioning Agent", "version": "1.2.0", "data_sources": ["CFTC.gov (TFF+Disagg+Legacy)", "Polygon.io"], "contracts_tracked": len(COT_CONTRACTS), "timestamp": datetime.now(timezone.utc).isoformat()}, headers)
        elif "/cot/all" in path or "/positioning/all" in path:
            # Try S3 cache first (prevents timeout)
            cached = _get_cached_cot_all()
            if cached:
                return respond(200, cached, headers)
            # Cache miss - fetch live and save to cache
            cot_data = fetch_cftc_cot_data()
            result = {"source": "CFTC.gov", "contracts": len(cot_data), "data": cot_data, "timestamp": datetime.now(timezone.utc).isoformat()}
            try:
                _save_cot_all_cache(result)
            except:
                pass
            return respond(200, result, headers)
        elif "/cot/category/" in path:
            category = path.split("/cot/category/")[-1].strip("/")
            cot_data = fetch_cftc_cot_data()
            filtered = {k: v for k, v in cot_data.items() if v.get("category") == category}
            return respond(200, {"category": category, "contracts": len(filtered), "data": filtered, "timestamp": datetime.now(timezone.utc).isoformat()}, headers)
        elif "/cot/" in path and "/cot/all" not in path and "/cot/category" not in path:
            contract = path.split("/cot/")[-1].strip("/").upper()
            cot_data = fetch_cftc_cot_data()
            if contract in cot_data:
                return respond(200, cot_data[contract], headers)
            else:
                return respond(404, {"error": f"Contract {contract} not found", "available": list(COT_CONTRACTS.keys())}, headers)
        elif "/futures" in path or "/market" in path:
            return respond(200, {"source": "Polygon.io", "data": fetch_polygon_futures_data(), "timestamp": datetime.now(timezone.utc).isoformat()}, headers)
        elif "/analysis" in path or "/composite" in path or "/comprehensive" in path:
            cot_data = fetch_cftc_cot_data()
            polygon_data = fetch_polygon_futures_data()
            analysis = generate_composite_analysis(cot_data, polygon_data)
            return respond(200, {"analysis": analysis, "cot_data": cot_data, "market_data": polygon_data}, headers)
        elif "/signals" in path or "/alerts" in path:
            cot_data = fetch_cftc_cot_data()
            polygon_data = fetch_polygon_futures_data()
            analysis = generate_composite_analysis(cot_data, polygon_data)
            return respond(200, {"positioning_score": analysis["market_positioning_score"], "risk_appetite": analysis["risk_appetite_index"], "smart_money": analysis["smart_money_flow"], "crisis_indicators": analysis["crisis_indicators"], "extreme_positions": analysis["extreme_positions"], "reversal_candidates": analysis["reversal_candidates"], "sector_positioning": analysis["sector_positioning"], "summary": analysis["summary"], "timestamp": datetime.now(timezone.utc).isoformat()}, headers)
        elif "/contracts" in path:
            return respond(200, {"contracts": {k: {"name": v["name"], "category": v["category"]} for k, v in COT_CONTRACTS.items()}, "total": len(COT_CONTRACTS)}, headers)
        else:
            return respond(200, {"agent": "CFTC Futures Positioning Agent", "version": "1.2.0", "endpoints": {"/health": "Health", "/cot/all": "All COT", "/cot/{CONTRACT}": "Single", "/cot/category/{CAT}": "By category", "/futures": "Prices", "/analysis": "Full analysis", "/signals": "Signals", "/debug/{CONTRACT}": "Raw CFTC fields"}, "contracts": len(COT_CONTRACTS), "timestamp": datetime.now(timezone.utc).isoformat()}, headers)
    except Exception as e:
        return respond(500, {"error": str(e), "trace": traceback.format_exc()}, headers)

def respond(code, body, headers):
    return {"statusCode": code, "headers": headers, "body": json.dumps(body, default=str)}
