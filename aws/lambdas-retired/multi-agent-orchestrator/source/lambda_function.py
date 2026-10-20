import json
import urllib.request
import ssl
from concurrent.futures import ThreadPoolExecutor

def lambda_handler(event, context):
    operation = event.get('operation', 'health')
    
    # Simple health check without external calls for testing
    if operation == 'test':
        return {
            'statusCode': 200,
            'body': json.dumps({'status': 'orchestrator working', 'operation': operation})
        }
    
    if operation == 'health':
        agents = {
            "global-liquidity": "https://lwybn3kjcpofq5ifcre5ybtuey0fddxz.lambda-url.us-east-1.on.aws/",
            "treasury-api": "https://oanydg4qltq5emsnnb2m23mifm0bqjqh.lambda-url.us-east-1.on.aws/",
            "alphavantage": "https://ngqq4e3hmqi6j5nky2mzoixc5e0yybrh.lambda-url.us-east-1.on.aws/",
            "enhanced-repo": "https://uhuftf5gghrsnoeui66g24qeh40ovomr.lambda-url.us-east-1.on.aws/",
            "cross-currency": "https://cm6i7tzsb6fpyvus5zvy43igae0oxmuc.lambda-url.us-east-1.on.aws/",
            "ice-bofa": "https://s57bexwijusq7jukyxishguffe0nukpw.lambda-url.us-east-1.on.aws/",
            "polygon-api": "https://fjf6t3ne4h.execute-api.us-east-1.amazonaws.com/prod/stock/SPY",
            "fred-api": "https://klehdyiwrl.execute-api.us-east-1.amazonaws.com/prod/health",
            "ny-fed": "https://jc6ripzwk1.execute-api.us-east-1.amazonaws.com/prod",
            "census": "https://2lhhfitug2w2m4leajszuptafu0kgend.lambda-url.us-east-1.on.aws/",
            "ai-prediction": "https://6fa5qo7fov36efsq7vftugu2iy0wfecn.lambda-url.us-east-1.on.aws/",
            "chatgpt": "https://aamyjez2avm6kvjowjowlcuk5m0pemhl.lambda-url.us-east-1.on.aws/",
            "news-sentiment": "https://kcm6voksx75rzhyxkblfagcbeq0fxdyb.lambda-url.us-east-1.on.aws/",
            "fed-liquidity": "https://mjqyipzzwjcmx44irtvijecswm0nkikf.lambda-url.us-east-1.on.aws/",
            "coinmarketcap": "https://i5msak7bhk.execute-api.us-east-1.amazonaws.com/prod/crypto"
        }
        
        results = {}
        ctx = ssl.create_default_context()
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        
        def check(name, url):
            try:
                if '.on.aws' in url:
                    req = urllib.request.Request(url, 
                        data=b'{"test":true}',
                        headers={'Content-Type': 'application/json'})
                else:
                    req = urllib.request.Request(url)
                
                with urllib.request.urlopen(req, context=ctx, timeout=2) as r:
                    return (name, "healthy" if r.status == 200 else "unhealthy")
            except Exception as e:
                return (name, "timeout" if "timeout" in str(e) else "unhealthy")
        
        with ThreadPoolExecutor(max_workers=10) as ex:
            for name, status in ex.map(lambda x: check(*x), agents.items()):
                results[name] = status
        
        healthy = sum(1 for s in results.values() if s == "healthy")
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'health': results,
                'healthy_count': healthy,
                'total': len(agents)
            })
        }
    
    return {'statusCode': 200, 'body': json.dumps({'status': 'ok'})}
