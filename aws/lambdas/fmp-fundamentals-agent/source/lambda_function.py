import json, urllib.request, os, traceback
from datetime import datetime

API_KEY = os.environ.get('FMP_API_KEY', 'wwVpi37SWHoNAzacFNVCDxEKBTUIS8xb')
BASE = 'https://financialmodelingprep.com/api/v3'
WATCH = ["AAPL","MSFT","GOOGL","AMZN","NVDA","META","TSLA","JPM","V","XOM","JNJ","WMT","MA","HD","CVX","LLY","AVGO","NFLX","AMD","VRT","PLTR","SMCI","ARM","MSTR"]

def fmp(ep, p=""):
    try:
        url = f"{BASE}/{ep}?apikey={API_KEY}{p}"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/1.0'})
        with urllib.request.urlopen(req, timeout=15) as r:
            raw = r.read().decode()
            d = json.loads(raw)
            return d
    except Exception as e:
        return {"error": f"{type(e).__name__}: {str(e)}"}

def lambda_handler(event, context):
    h = {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'}
    if isinstance(event, dict) and event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': h, 'body': '{}'}
    path = event.get('rawPath', '') if isinstance(event, dict) else ''
    if '/health' in path:
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({'status': 'healthy', 'agent': 'fmp-fundamentals-agent', 'watchlist': len(WATCH)})}
    if '/debug' in path:
        t1 = fmp("quote/AAPL")
        t2 = fmp("sector-performance")
        t3 = fmp("stock_market/gainers")
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({'debug': True, 'quote_AAPL': t1, 'sectors': t2, 'gainers': t3}, default=str)}
    try:
        # Batch quotes for all watchlist (this works on free tier)
        syms = ','.join(WATCH)
        quotes = fmp(f"quote/{syms}")
        quote_map = {}
        if isinstance(quotes, list):
            for q in quotes:
                quote_map[q.get('symbol', '')] = q

        # Try premium endpoints - they may return data or empty
        sectors = fmp("sector-performance")
        gainers = fmp("stock_market/gainers")
        losers = fmp("stock_market/losers")
        actives = fmp("stock_market/actives")

        # Index quotes (free tier)
        indices = fmp("quote/%5EGSPC,%5EIXIC,%5EDJI,%5EVIX,%5ETRN")

        return {'statusCode': 200, 'headers': h, 'body': json.dumps({
            "agent": "fmp-fundamentals-agent",
            "ts": datetime.utcnow().isoformat(),
            "watchlist_quotes": quote_map,
            "index_quotes": indices if isinstance(indices, list) else [],
            "sector_performance": sectors if isinstance(sectors, list) else [],
            "movers": {
                "gainers": (gainers[:15] if isinstance(gainers, list) else []),
                "losers": (losers[:15] if isinstance(losers, list) else []),
                "actives": (actives[:15] if isinstance(actives, list) else [])
            },
            "watchlist": WATCH,
            "quotes_ok": len(quote_map),
            "quotes_err": len(WATCH) - len(quote_map)
        }, default=str)}
    except Exception as e:
        return {'statusCode': 500, 'headers': h, 'body': json.dumps({'error': str(e), 'trace': traceback.format_exc()})}
