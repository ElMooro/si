import json
import boto3
from datetime import datetime
import time

s3 = boto3.client('s3')
BUCKET = 'openbb-lambda-data'
KEY = 'ecb_data.json'
CACHE_DURATION = 604800
data_cache = None
cache_timestamp = None

def lambda_handler(event, context):
    """ECB API Lambda with proper routing"""
    global data_cache, cache_timestamp
    
    # Load cache if needed
    current_time = time.time()
    if data_cache is None or (cache_timestamp and (current_time - cache_timestamp) > CACHE_DURATION):
        try:
            response = s3.get_object(Bucket=BUCKET, Key=KEY)
            data_cache = json.loads(response['Body'].read())
            cache_timestamp = current_time
            print(f"Loaded {len(data_cache)} indicators from S3")
        except Exception as e:
            print(f"Error loading S3: {e}")
            if data_cache is None:
                data_cache = {}
    
    # Get the request path - IMPORTANT FIX
    # For HTTP API v2, path is in rawPath
    path = event.get('rawPath', '/')
    
    # Remove stage from path if present
    if path.startswith('/prod'):
        path = path[5:]
    elif path.startswith('/$default'):
        path = path[9:]
    
    # Also check 'path' for backwards compatibility
    if path == '/' and 'path' in event:
        path = event['path']
    
    print(f"Processing request - rawPath: {event.get('rawPath')}, path: {path}, event keys: {list(event.keys())}")
    
    # Route to appropriate handler
    try:
        # Health endpoint
        if path == '/' or path == '/health':
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'status': 'OK',
                    'service': 'ECB Data API v2',
                    'indicators': len(data_cache) if data_cache else 0,
                    'cache_type': 'WEEKLY',
                    'timestamp': datetime.now().isoformat()
                })
            }
        
        # Data endpoint - get specific indicator
        elif path.startswith('/data/'):
            symbol = path.replace('/data/', '').strip('/')
            print(f"Fetching data for symbol: {symbol}")
            
            if data_cache and symbol in data_cache:
                return {
                    'statusCode': 200,
                    'body': json.dumps(data_cache[symbol])
                }
            else:
                return {
                    'statusCode': 404,
                    'body': json.dumps({'error': 'Symbol not found', 'symbol': symbol})
                }
        
        # List all indicators
        elif path == '/list' or path.startswith('/list'):
            indicators_list = list(data_cache.values()) if data_cache else []
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'total': len(indicators_list),
                    'indicators': indicators_list[:100]
                })
            }
        
        # Risk assessment endpoint
        elif path == '/risk-assessment' or path.startswith('/risk-assessment'):
            ciss_values = {}
            
            if data_cache:
                for key, value in data_cache.items():
                    if 'CISS.M' in key and 'SOV_CI' in key:
                        country = value.get('country')
                        if country:
                            ciss_values[country] = value.get('value', 0)
            
            if ciss_values:
                avg_stress = sum(ciss_values.values()) / len(ciss_values)
                risk_level = 'CRITICAL' if avg_stress > 0.15 else 'HIGH' if avg_stress > 0.10 else 'MEDIUM' if avg_stress > 0.05 else 'LOW'
                
                country_risks = [
                    {'country': k, 'value': v, 'risk': 'HIGH' if v > 0.10 else 'MEDIUM' if v > 0.05 else 'LOW'}
                    for k, v in sorted(ciss_values.items(), key=lambda x: x[1], reverse=True)
                ]
                
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'systemic_risk': risk_level,
                        'average_stress': round(avg_stress, 4),
                        'total_countries': len(ciss_values),
                        'country_risks': country_risks[:10]
                    })
                }
            else:
                return {
                    'statusCode': 200,
                    'body': json.dumps({
                        'systemic_risk': 'UNKNOWN',
                        'average_stress': 0,
                        'total_countries': 0,
                        'country_risks': []
                    })
                }
        
        # Unknown endpoint
        else:
            return {
                'statusCode': 404,
                'body': json.dumps({
                    'error': 'Endpoint not found',
                    'path': path,
                    'available_endpoints': ['/health', '/data/{symbol}', '/list', '/risk-assessment']
                })
            }
            
    except Exception as e:
        print(f"Error processing request: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }
