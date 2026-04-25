import json
import urllib.request
import ssl
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

def lambda_handler(event, context):
    operation = event.get('operation', 'health')
    
    agents = {
        "global-liquidity": ("https://lwybn3kjcpofq5ifcre5ybtuey0fddxz.lambda-url.us-east-1.on.aws/", 5),
        "treasury-api": ("https://oanydg4qltq5emsnnb2m23mifm0bqjqh.lambda-url.us-east-1.on.aws/", 5),
        "polygon-api": ("https://fjf6t3ne4h.execute-api.us-east-1.amazonaws.com/prod/stock/SPY", 3),
        "ai-prediction": ("https://6fa5qo7fov36efsq7vftugu2iy0wfecn.lambda-url.us-east-1.on.aws/", 5),
        "fred-api": ("https://klehdyiwrl.execute-api.us-east-1.amazonaws.com/prod/health", 3),
        "ny-fed": ("https://jc6ripzwk1.execute-api.us-east-1.amazonaws.com/prod", 3),
        "coinmarketcap": ("https://i5msak7bhk.execute-api.us-east-1.amazonaws.com/prod/crypto", 3),
        "alphavantage": ("https://ngqq4e3hmqi6j5nky2mzoixc5e0yybrh.lambda-url.us-east-1.on.aws/", 5),
        "news-sentiment": ("https://kcm6voksx75rzhyxkblfagcbeq0fxdyb.lambda-url.us-east-1.on.aws/", 3),
        "enhanced-repo": ("https://uhuftf5gghrsnoeui66g24qeh40ovomr.lambda-url.us-east-1.on.aws/", 3),
        "cross-currency": ("https://cm6i7tzsb6fpyvus5zvy43igae0oxmuc.lambda-url.us-east-1.on.aws/", 3),
        "census": ("https://2lhhfitug2w2m4leajszuptafu0kgend.lambda-url.us-east-1.on.aws/", 3),
        "chatgpt": ("https://aamyjez2avm6kvjowjowlcuk5m0pemhl.lambda-url.us-east-1.on.aws/", 5),
        "ice-bofa": ("https://s57bexwijusq7jukyxishguffe0nukpw.lambda-url.us-east-1.on.aws/", 8),
        "fed-liquidity": ("https://mjqyipzzwjcmx44irtvijecswm0nkikf.lambda-url.us-east-1.on.aws/", 8)
    }
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    if operation == 'health':
        health_status = {}
        
        def check_agent(name, url, timeout):
            try:
                if '.on.aws' in url:
                    req = urllib.request.Request(url,
                        data=json.dumps({"test": True}).encode('utf-8'),
                        headers={'Content-Type': 'application/json'})
                else:
                    req = urllib.request.Request(url)
                
                with urllib.request.urlopen(req, context=ctx, timeout=timeout) as response:
                    return (name, "healthy" if response.status == 200 else "unhealthy")
            except:
                return (name, "unhealthy")
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = {executor.submit(check_agent, name, url, timeout): name 
                      for name, (url, timeout) in agents.items()}
            
            for future in as_completed(futures, timeout=12):
                try:
                    name, status = future.result()
                    health_status[name] = status
                except:
                    name = futures[future]
                    health_status[name] = "timeout"
        
        healthy_count = sum(1 for s in health_status.values() if s == "healthy")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'health': health_status,
                'healthy_count': healthy_count,
                'total': len(agents)
            })
        }
    
    elif operation == 'analyze':
        # FETCH REAL DATA FROM ALL AGENTS
        aggregated_data = {}
        errors = {}
        
        def fetch_agent_data(name, url, timeout):
            try:
                if '.on.aws' in url:
                    req = urllib.request.Request(url,
                        data=json.dumps({}).encode('utf-8'),
                        headers={'Content-Type': 'application/json'})
                else:
                    req = urllib.request.Request(url)
                
                with urllib.request.urlopen(req, context=ctx, timeout=timeout) as response:
                    data = json.loads(response.read().decode('utf-8'))
                    return (name, "success", data)
            except Exception as e:
                return (name, "error", str(e)[:200])
        
        with ThreadPoolExecutor(max_workers=15) as executor:
            futures = {executor.submit(fetch_agent_data, name, url, timeout): name 
                      for name, (url, timeout) in agents.items()}
            
            for future in as_completed(futures, timeout=20):
                try:
                    name, status, data = future.result()
                    if status == "success":
                        aggregated_data[name] = data
                    else:
                        errors[name] = data
                except Exception as e:
                    name = futures[future]
                    errors[name] = "timeout"
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ'),
                'operation': 'analyze',
                'data': aggregated_data,
                'errors': errors,
                'success_count': len(aggregated_data),
                'error_count': len(errors),
                'total_agents': len(agents)
            })
        }
    
    return {
        'statusCode': 200,
        'body': json.dumps({'status': 'operational', 'operation': operation})
    }
