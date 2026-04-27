import json
import urllib.request
import ssl
from concurrent.futures import ThreadPoolExecutor, TimeoutError
import time

# Disable SSL verification for testing
ssl_context = ssl.create_default_context()
ssl_context.check_hostname = False
ssl_context.verify_mode = ssl.CERT_NONE

# CORRECT FUNCTION URLS FROM YOUR TESTING
AGENTS = {
    "global-liquidity": "https://lwybn3kjcpofq5ifcre5ybtuey0fddxz.lambda-url.us-east-1.on.aws/",
    "treasury-api": "https://oanydg4qltq5emsnnb2m23mifm0bqjqh.lambda-url.us-east-1.on.aws/",
    "polygon-api": "https://fjf6t3ne4h.execute-api.us-east-1.amazonaws.com/prod/stock/SPY",
    "ai-prediction": "https://6fa5qo7fov36efsq7vftugu2iy0wfecn.lambda-url.us-east-1.on.aws/",
    "fred-api": "https://klehdyiwrl.execute-api.us-east-1.amazonaws.com/prod/health",
    "ny-fed": "https://jc6ripzwk1.execute-api.us-east-1.amazonaws.com/prod",
    "coinmarketcap": "https://i5msak7bhk.execute-api.us-east-1.amazonaws.com/prod/crypto",
    "alphavantage": "https://ngqq4e3hmqi6j5nky2mzoixc5e0yybrh.lambda-url.us-east-1.on.aws/",
    "news-sentiment": "https://kcm6voksx75rzhyxkblfagcbeq0fxdyb.lambda-url.us-east-1.on.aws/",
    "enhanced-repo": "https://uhuftf5gghrsnoeui66g24qeh40ovomr.lambda-url.us-east-1.on.aws/",
    "cross-currency": "https://cm6i7tzsb6fpyvus5zvy43igae0oxmuc.lambda-url.us-east-1.on.aws/",
    "census": "https://2lhhfitug2w2m4leajszuptafu0kgend.lambda-url.us-east-1.on.aws/",
    "chatgpt": "https://awfcijftjvs5f4ajdf2hwnwtt40tvweb.lambda-url.us-east-1.on.aws/",
    "ice-bofa": "https://lnd6erie7y4rw2u6r4dpv4enty0mhtua.lambda-url.us-east-1.on.aws/",
    "fed-liquidity": "https://nmkverrwjnxsmgnogzckkoyuce0bqxyk.lambda-url.us-east-1.on.aws/health"
}

def lambda_handler(event, context):
    operation = event.get('operation', 'health')
    
    if operation == 'health':
        health_status = {}
        healthy_count = 0
        
        def check_agent(name, url):
            try:
                # POST request for Lambda URLs
                if 'lambda-url' in url or 'on.aws' in url:
                    req = urllib.request.Request(
                        url,
                        data=json.dumps({"test": True}).encode('utf-8'),
                        headers={'Content-Type': 'application/json'}
                    )
                else:
                    # GET request for API Gateway
                    req = urllib.request.Request(url)
                
                with urllib.request.urlopen(req, context=ssl_context, timeout=3) as response:
                    if response.status == 200:
                        return (name, "healthy")
                    else:
                        return (name, "unhealthy")
            except TimeoutError:
                return (name, "timeout")
            except Exception as e:
                if "403" in str(e):
                    return (name, "forbidden")
                elif "500" in str(e) or "Internal" in str(e):
                    return (name, "error")
                else:
                    return (name, "unhealthy")
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = []
            for agent_name, agent_url in AGENTS.items():
                futures.append(executor.submit(check_agent, agent_name, agent_url))
            
            for future in futures:
                try:
                    name, status = future.result(timeout=4)
                    health_status[name] = status
                    if status == "healthy":
                        healthy_count += 1
                except:
                    health_status[name] = "timeout"
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'health': health_status,
                'healthy_count': healthy_count,
                'total': len(AGENTS)
            })
        }
    
    elif operation == 'analyze':
        # Aggregate data from healthy agents
        results = {}
        
        def fetch_agent(name, url):
            try:
                if 'lambda-url' in url or 'on.aws' in url:
                    req = urllib.request.Request(
                        url,
                        data=json.dumps({"analyze": True}).encode('utf-8'),
                        headers={'Content-Type': 'application/json'}
                    )
                else:
                    req = urllib.request.Request(url)
                
                with urllib.request.urlopen(req, context=ssl_context, timeout=5) as response:
                    data = json.loads(response.read().decode())
                    return (name, data)
            except:
                return (name, None)
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = []
            for agent_name, agent_url in AGENTS.items():
                futures.append(executor.submit(fetch_agent, agent_name, agent_url))
            
            for future in futures:
                try:
                    name, data = future.result(timeout=6)
                    if data:
                        results[name] = data
                except:
                    pass
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'analysis': results,
                'agents_responding': len(results)
            })
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps({'message': 'Orchestrator operational'})
    }
