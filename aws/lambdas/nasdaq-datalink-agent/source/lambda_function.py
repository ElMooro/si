import json, urllib.request, os, traceback
from datetime import datetime

API_KEY = os.environ.get('NASDAQ_API_KEY', '965p8tUm6xa2xA8hVrx7')

# Free-tier datasets that work with basic NASDAQ Data Link keys
# WIKI is deprecated, use NASDAQOMX and other free sources
DATASETS = {
    "market_indices": {
        "NASDAQOMX/COMP-NASDAQ": "NASDAQ Composite",
        "NASDAQOMX/NDX-NASDAQ": "NASDAQ-100",
        "NASDAQOMX/XQC-NASDAQ": "NASDAQ Financial-100",
    },
    "economic_indicators": {
        "FRED/GDP": "US GDP",
        "FRED/UNRATE": "Unemployment Rate",
        "FRED/CPIAUCSL": "CPI All Urban",
        "FRED/FEDFUNDS": "Fed Funds Rate",
        "FRED/DGS10": "10Y Treasury Yield",
        "FRED/DGS2": "2Y Treasury Yield",
        "FRED/T10Y2Y": "10Y-2Y Spread",
        "FRED/T10Y3M": "10Y-3M Spread",
        "FRED/VIXCLS": "VIX Close",
        "FRED/BAMLH0A0HYM2": "HY OAS Spread",
        "FRED/UMCSENT": "Consumer Sentiment",
        "FRED/M2SL": "M2 Money Supply",
        "FRED/WALCL": "Fed Balance Sheet",
        "FRED/DTWEXBGS": "Trade-Weighted Dollar",
        "FRED/DCOILWTICO": "WTI Crude Oil",
    },
    "housing": {
        "FRED/CSUSHPINSA": "Case-Shiller Home Price",
        "FRED/MORTGAGE30US": "30Y Mortgage Rate",
        "FRED/HOUST": "Housing Starts",
    },
    "labor": {
        "FRED/PAYEMS": "Nonfarm Payrolls",
        "FRED/ICSA": "Initial Jobless Claims",
        "FRED/JTSJOL": "JOLTS Job Openings",
    },
}

def fetch(code, limit=24):
    try:
        url = f"https://data.nasdaq.com/api/v3/datasets/{code}/data.json?api_key={API_KEY}&limit={limit}&order=desc"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/1.0'})
        with urllib.request.urlopen(req, timeout=15) as r:
            d = json.loads(r.read().decode())
            ds = d.get('dataset_data', {})
            rows, cols = ds.get('data', []), ds.get('column_names', [])
            if not rows:
                return {"error": "empty dataset"}
            latest = dict(zip(cols, rows[0]))
            previous = dict(zip(cols, rows[1])) if len(rows) > 1 else {}
            # Calculate change
            val_col = cols[1] if len(cols) > 1 else None
            cur_val = latest.get(val_col) if val_col else None
            prev_val = previous.get(val_col) if val_col else None
            chg = None
            if cur_val is not None and prev_val is not None and prev_val != 0:
                try:
                    chg = round((float(cur_val) - float(prev_val)) / abs(float(prev_val)) * 100, 2)
                except:
                    pass
            return {
                "columns": cols,
                "latest": latest,
                "previous": previous,
                "value": cur_val,
                "change_pct": chg,
                "history": [dict(zip(cols, r)) for r in rows[:24]],
                "count": len(rows)
            }
    except Exception as e:
        return {"error": str(e)}

def lambda_handler(event, context):
    h = {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'}
    if isinstance(event, dict) and event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': h, 'body': '{}'}
    path = event.get('rawPath', '') if isinstance(event, dict) else ''
    if '/health' in path:
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({'status': 'healthy', 'agent': 'nasdaq-datalink-agent', 'datasets': sum(len(v) for v in DATASETS.values())})}
    if '/debug' in path:
        test = fetch('FRED/GDP')
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({'debug': True, 'dataset': 'FRED/GDP', 'result': test}, default=str)}
    try:
        result = {"agent": "nasdaq-datalink-agent", "ts": datetime.utcnow().isoformat(), "categories": {}}
        ok = err = 0
        for cat, datasets in DATASETS.items():
            cr = {}
            for code, name in datasets.items():
                d = fetch(code)
                cr[code] = {"name": name, **d}
                if 'error' in d:
                    err += 1
                else:
                    ok += 1
            result["categories"][cat] = cr
        result["metrics_ok"] = ok
        result["metrics_err"] = err
        return {'statusCode': 200, 'headers': h, 'body': json.dumps(result, default=str)}
    except Exception as e:
        return {'statusCode': 500, 'headers': h, 'body': json.dumps({'error': str(e), 'trace': traceback.format_exc()})}
