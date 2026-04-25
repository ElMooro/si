import json, urllib.request, os, traceback
from datetime import datetime

API_KEY = os.environ.get('EIA_API_KEY', 'trvQDpg2GdvBixLeieVMyaQwsnkFQlYSuecVm4Pl')

STEO = {"WTIPUUS":"WTI Crude Oil Price ($/bbl)","BREPROD":"Brent Crude Price ($/bbl)","COPRPUS":"US Crude Oil Production (Mb/d)","PAPR_OPEC":"OPEC Crude Production (Mb/d)","PATC_WORLD":"World Petroleum Consumption (Mb/d)","PASC_WORLD":"World Petroleum Supply (Mb/d)","COPS_US":"US Commercial Crude Inventories (Mb)","PRCE_NOM_HENRY":"Henry Hub Natural Gas ($/MMBtu)","NGPRPUS":"US Natural Gas Production (Bcf/d)","NGEXPUS":"US Natural Gas Exports (Bcf/d)","NGIMPUS":"US Natural Gas Imports (Bcf/d)","D_GASOLINE_US":"US Gasoline Demand (Mb/d)","DKRCPUS":"US Distillate Demand (Mb/d)","ELEPPUS":"US Electricity Retail Price (c/kWh)","ELNRPUS":"US Renewable Generation (BkWh)","CORIPUS":"US Crude Oil Imports (Mb/d)","COEXPUS":"US Crude Oil Exports (Mb/d)","COSXPUS":"US Net Petroleum Exports (Mb/d)","NGCNPUS":"US Natural Gas Consumption (Bcf/d)","ZWHDPUS":"US Heating Degree Days","ZWCDPUS":"US Cooling Degree Days","MGWHUUS":"US Gasoline Wholesale ($/gal)","D2WHUUS":"US Diesel Wholesale ($/gal)","RAIMUUS":"US Jet Fuel Price ($/gal)","PAPR_NON_OPEC":"Non-OPEC Production (Mb/d)","PATC_OECD":"OECD Consumption (Mb/d)","PATC_NON_OECD":"Non-OECD Consumption (Mb/d)"}

def fetch_steo(sid):
    try:
        url = (
            f"https://api.eia.gov/v2/steo/data/"
            f"?api_key={API_KEY}"
            f"&frequency=monthly"
            f"&data[0]=value"
            f"&facets[seriesId][]={sid}"
            f"&sort[0][column]=period"
            f"&sort[0][direction]=desc"
            f"&length=24"
        )
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/1.0'})
        with urllib.request.urlopen(req, timeout=20) as r:
            raw = r.read().decode()
            d = json.loads(raw)
            rows = d.get('response', {}).get('data', [])
            if not rows:
                return {"error": f"empty: keys={list(d.keys())}"}
            cur = float(rows[0]['value']) if rows[0].get('value') else None
            prev = float(rows[1]['value']) if len(rows) > 1 and rows[1].get('value') else None
            y = float(rows[12]['value']) if len(rows) > 12 and rows[12].get('value') else None
            return {
                "period": rows[0].get('period'),
                "value": cur, "prev": prev, "yago": y,
                "mom": round((cur - prev) / abs(prev) * 100, 2) if cur and prev and prev != 0 else None,
                "yoy": round((cur - y) / abs(y) * 100, 2) if cur and y and y != 0 else None,
                "history": [{"p": rr.get("period"), "v": float(rr["value"]) if rr.get("value") else None} for rr in rows[:24]]
            }
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)}"}

def lambda_handler(event, context):
    h = {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'}
    if isinstance(event, dict) and event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': h, 'body': '{}'}
    path = event.get('rawPath', '') if isinstance(event, dict) else ''
    if '/health' in path:
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({'status': 'healthy', 'agent': 'eia-energy-agent', 'metrics': len(STEO)})}
    if '/debug' in path:
        test = fetch_steo('WTIPUUS')
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({'debug': True, 'series': 'WTIPUUS', 'result': test}, default=str)}
    try:
        results = {}
        for sid, name in STEO.items():
            d = fetch_steo(sid)
            if d and 'error' not in d:
                results[sid] = {"name": name, "data": d}
            else:
                results[sid] = {"name": name, "error": d.get('error', 'unknown') if d else 'null'}
        oil = {k: v for k, v in results.items() if k in ["WTIPUUS","BREPROD","COPRPUS","PAPR_OPEC","COPS_US","CORIPUS","COEXPUS","COSXPUS"]}
        gas = {k: v for k, v in results.items() if k in ["PRCE_NOM_HENRY","NGPRPUS","NGEXPUS","NGIMPUS","NGCNPUS"]}
        demand = {k: v for k, v in results.items() if k in ["D_GASOLINE_US","DKRCPUS","ZWHDPUS","ZWCDPUS"]}
        power = {k: v for k, v in results.items() if k in ["ELEPPUS","ELNRPUS"]}
        prices = {k: v for k, v in results.items() if k in ["MGWHUUS","D2WHUUS","RAIMUUS"]}
        world = {k: v for k, v in results.items() if k in ["PATC_WORLD","PASC_WORLD","PAPR_NON_OPEC","PATC_OECD","PATC_NON_OECD"]}
        ok = len([v for v in results.values() if v.get('data')])
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({
            "agent": "eia-energy-agent", "ts": datetime.utcnow().isoformat(),
            "oil_markets": oil, "natural_gas": gas, "demand_indicators": demand,
            "electricity": power, "fuel_prices": prices, "global_supply_demand": world,
            "all_series": results, "metrics_ok": ok, "metrics_err": len(results) - ok
        }, default=str)}
    except Exception as e:
        return {'statusCode': 500, 'headers': h, 'body': json.dumps({'error': str(e), 'trace': traceback.format_exc()})}
