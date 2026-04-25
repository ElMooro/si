import json, urllib.request, os, traceback
from datetime import datetime, timedelta

API_KEY = os.environ.get('BENZINGA_API_KEY', 'bzMJ62WO2YP2OKVIE2YSF4ZWVSVOJ6CTNP')

def bz(ep, extra=""):
    errors = []
    # Method 1: token as query param
    try:
        url = f"https://api.benzinga.com/api/v2/{ep}?token={API_KEY}{extra}"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/1.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        errors.append(f"method1(token=): {e}")

    # Method 2: apikey as query param
    try:
        url = f"https://api.benzinga.com/api/v2/{ep}?apikey={API_KEY}{extra}"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/1.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        errors.append(f"method2(apikey=): {e}")

    # Method 3: header auth
    try:
        url = f"https://api.benzinga.com/api/v2/{ep}?{extra.lstrip('&')}" if extra else f"https://api.benzinga.com/api/v2/{ep}"
        req = urllib.request.Request(url, headers={
            'User-Agent': 'JustHodl/1.0',
            'Accept': 'application/json',
            'x-api-key': API_KEY
        })
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        errors.append(f"method3(x-api-key): {e}")

    return {"error": " | ".join(errors)}

def lambda_handler(event, context):
    h = {'Content-Type': 'application/json', 'Access-Control-Allow-Origin': '*', 'Access-Control-Allow-Headers': 'Content-Type', 'Access-Control-Allow-Methods': 'GET, POST, OPTIONS'}
    if isinstance(event, dict) and event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': h, 'body': '{}'}
    path = event.get('rawPath', '') if isinstance(event, dict) else ''
    if '/health' in path:
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({'status': 'healthy', 'agent': 'benzinga-news-agent'})}
    if '/debug' in path:
        test = bz("calendar/ratings", "&page=0&pageSize=3")
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({'debug': True, 'endpoint': 'calendar/ratings', 'key_prefix': API_KEY[:8], 'result': test}, default=str)}
    try:
        today = datetime.utcnow().strftime('%Y-%m-%d')
        wk = (datetime.utcnow() - timedelta(days=7)).strftime('%Y-%m-%d')
        ratings = bz("calendar/ratings", f"&page=0&pageSize=30&date_from={wk}&date_to={today}")
        earnings = bz("calendar/earnings", f"&page=0&pageSize=30&date_from={today}")
        economics = bz("calendar/economics", f"&page=0&pageSize=20&date_from={today}")
        dividends = bz("calendar/dividends", f"&page=0&pageSize=20&date_from={today}")
        news = bz("news", "&page=0&pageSize=20&displayOutput=full")
        def ex(d, k):
            return d.get(k, d) if isinstance(d, dict) else d if isinstance(d, list) else []
        return {'statusCode': 200, 'headers': h, 'body': json.dumps({
            "agent": "benzinga-news-agent", "ts": datetime.utcnow().isoformat(),
            "analyst_ratings": ex(ratings, "ratings"),
            "earnings_calendar": ex(earnings, "earnings"),
            "economic_events": ex(economics, "economics"),
            "dividends": ex(dividends, "dividends"),
            "market_news": news if isinstance(news, list) else []
        }, default=str)}
    except Exception as e:
        return {'statusCode': 500, 'headers': h, 'body': json.dumps({'error': str(e), 'trace': traceback.format_exc()})}
