import json
import time
import hashlib

# Simple in-memory cache (resets when Lambda cold starts)
cache = {}
CACHE_TTL = 300  # 5 minutes

def lambda_handler(event, context):
    # Generate cache key
    cache_key = hashlib.md5(json.dumps(event).encode()).hexdigest()
    
    # Check cache
    if cache_key in cache:
        entry = cache[cache_key]
        if time.time() - entry['timestamp'] < CACHE_TTL:
            return {
                'statusCode': 200,
                'body': entry['data'],
                'headers': {'X-Cache': 'HIT'}
            }
    
    # If not in cache, call the real orchestrator
    import urllib.request
    import ssl
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    req = urllib.request.Request(
        'https://jj4f4t5xv6zyrbbxtqebdpss2y0ebxof.lambda-url.us-east-1.on.aws/',
        data=json.dumps(event).encode(),
        headers={'Content-Type': 'application/json'}
    )
    
    with urllib.request.urlopen(req, context=ctx) as response:
        data = response.read().decode()
    
    # Cache the result
    cache[cache_key] = {
        'data': data,
        'timestamp': time.time()
    }
    
    return {
        'statusCode': 200,
        'body': data,
        'headers': {'X-Cache': 'MISS'}
    }
