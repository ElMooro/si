import json
import urllib.request
import ssl
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timezone

AGENTS = {
    "fed-liquidity": {"url": "https://mjqyipzzwjcmx44irtvijecswm0nkikf.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "global-liquidity": {"url": "https://lwybn3kjcpofq5ifcre5ybtuey0fddxz.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "treasury-api": {"url": "https://oanydg4qltq5emsnnb2m23mifm0bqjqh.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "ai-prediction": {"url": "https://6fa5qo7fov36efsq7vftugu2iy0wfecn.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "fred-api": {"url": "https://lnd6erie7y4rw2u6r4dpv4enty0mhtua.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "ice-bofa": {"url": "https://lnd6erie7y4rw2u6r4dpv4enty0mhtua.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8, "payload": {"operation": "ice-bofa"}},
    "coinmarketcap": {"url": "https://r6aa3h5dmuexztvixgll3s7jfe0fscwz.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "alphavantage": {"url": "https://ftbvriu6ftmbtw7luop7kxiepu0gwlcb.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "cross-currency": {"url": "https://cm6i7tzsb6fpyvus5zvy43igae0oxmuc.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "enhanced-repo": {"url": "https://uhuftf5gghrsnoeui66g24qeh40ovomr.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "census": {"url": "https://2lhhfitug2w2m4leajszuptafu0kgend.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "polygon-api": {"url": "https://ftbvriu6ftmbtw7luop7kxiepu0gwlcb.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8, "payload": {"operation": "market"}},
    "chatgpt": {"url": "https://aamyjez2avm6kvjowjowlcuk5m0pemhl.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 10},
    "ny-fed": {"url": "https://iru4ado3aki625pnswcpycrniq0ctjba.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "dollar-strength": {"url": "https://us3uynmi23u676v3szd27ldwuq0jeqee.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "bls-labor": {"url": "https://sc7wgo5xmcfgekvdwz7puk55f40ovfzg.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "volatility-monitor": {"url": "https://w4bvakowhpbkvy3mqkzp66xfaa0kpfqi.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "bond-indices": {"url": "https://s57bexwijusq7jukyxishguffe0nukpw.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "google-trends": {"url": "https://ohmu2l54tpbgm6jk7e5s6q3jpe0cqvij.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "news-sentiment": {"url": "https://kcm6voksx75rzhyxkblfagcbeq0fxdyb.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "manufacturing": {"url": "https://atjkdhikbinborcc2pujbs3jxm0iugqe.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "bea-economic": {"url": "https://hnqqkbf7y6avoda5v4rexwk3440ibbru.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
    "macro-intelligence": {"url": "https://qhmg6ybf5qhkviw2c6tlizar4e0xhbrc.lambda-url.us-east-1.on.aws/", "method": "POST", "timeout": 8},
}

CHART_AGENTS = {
    "charts": {"url": "https://wehli6nf3a6rq575td5w6jk7ii0yptqg.lambda-url.us-east-1.on.aws/"},
    "advanced-charts": {"url": "https://e6p3e3jhsgha45rl7inapnmz4m0qmcbc.lambda-url.us-east-1.on.aws/"},
}

ctx = ssl.create_default_context()

def fetch_agent(name, config):
    try:
        url = config['url']
        payload = config.get('payload', {"operation": "data"})
        method = config.get('method', 'POST')
        timeout = config.get('timeout', 8)
        if method == 'POST':
            data = json.dumps(payload).encode()
            req = urllib.request.Request(url, data=data, headers={'Content-Type': 'application/json'})
        else:
            req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:
            raw = resp.read().decode()
            try:
                result = json.loads(raw)
                if 'body' in result and isinstance(result['body'], str):
                    try: result = json.loads(result['body'])
                    except: pass
                return name, {"status": "healthy", "data": result}
            except:
                return name, {"status": "healthy", "data": raw[:500]}
    except Exception as e:
        return name, {"status": "error", "error": str(e)[:200]}

def lambda_handler(event, context):
    start = time.time()
    body = event
    if isinstance(event.get('body'), str):
        try: body = json.loads(event['body'])
        except: body = event
    operation = body.get('operation', 'data') if isinstance(body, dict) else 'data'

    headers = {
        'Content-Type': 'application/json',
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Headers': 'Content-Type,Authorization',
        'Access-Control-Allow-Methods': 'GET,POST,OPTIONS'
    }

    if operation == 'options' or event.get('requestContext', {}).get('http', {}).get('method') == 'OPTIONS':
        return {'statusCode': 200, 'headers': headers, 'body': ''}

    if operation == 'health':
        results = {}
        healthy = 0
        with ThreadPoolExecutor(max_workers=25) as ex:
            futures = {ex.submit(fetch_agent, n, c): n for n, c in AGENTS.items()}
            for f in as_completed(futures, timeout=15):
                try:
                    name, result = f.result()
                    results[name] = {"status": result["status"]}
                    if result["status"] == "healthy": healthy += 1
                except: pass
        return {'statusCode': 200, 'headers': headers, 'body': json.dumps({
            'status': 'operational',
            'total_agents': len(AGENTS),
            'healthy_agents': healthy,
            'health_percentage': round(healthy / len(AGENTS) * 100, 1),
            'details': results,
            'chart_agents': list(CHART_AGENTS.keys()),
            'timestamp': datetime.now(timezone.utc).isoformat()
        })}

    if operation == 'list':
        return {'statusCode': 200, 'headers': headers, 'body': json.dumps({
            'agents': list(AGENTS.keys()),
            'chart_agents': list(CHART_AGENTS.keys()),
            'total': len(AGENTS),
            'timestamp': datetime.now(timezone.utc).isoformat()
        })}

    # DEFAULT: operation == 'data' - fetch ALL agents in parallel
    raw_data = {}
    successful = 0
    failed = 0
    with ThreadPoolExecutor(max_workers=25) as ex:
        futures = {ex.submit(fetch_agent, n, c): n for n, c in AGENTS.items()}
        for f in as_completed(futures, timeout=20):
            try:
                name, result = f.result()
                if result["status"] == "healthy":
                    raw_data[name] = result.get("data", {})
                    successful += 1
                else:
                    raw_data[name] = {"error": result.get("error", "failed")}
                    failed += 1
            except Exception as e:
                failed += 1

    elapsed = round(time.time() - start, 2)
    return {'statusCode': 200, 'headers': headers, 'body': json.dumps({
        'raw_data': raw_data,
        'statistics': {
            'agents_responded': successful,
            'agents_failed': failed,
            'total_agents': len(AGENTS),
            'success_rate': round(successful / max(len(AGENTS), 1) * 100, 1),
            'response_time_seconds': elapsed
        },
        '_metadata': {
            'successful_agents': successful,
            'total_agents': len(AGENTS),
            'success_rate': round(successful / max(len(AGENTS), 1) * 100, 1),
            'generated_at': datetime.now(timezone.utc).isoformat(),
            'engine': 'justhodl-ultimate-orchestrator-v3'
        },
        'chart_endpoints': {k: v['url'] for k, v in CHART_AGENTS.items()}
    })}
