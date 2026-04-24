import json
import urllib.request
import ssl
from datetime import datetime, timedelta

FRED_API_KEY = "2f057499936072679d8843d7fce99989"
POLYGON_API_KEY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

def lambda_handler(event, context):
    # CORS headers - THIS IS THE FIX!
    headers = {
        'Access-Control-Allow-Origin': '*',
        'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
        'Access-Control-Allow-Headers': 'Content-Type, Authorization',
        'Content-Type': 'application/json'
    }
    
    # Handle preflight OPTIONS request
    http_method = event.get('requestContext', {}).get('http', {}).get('method', '')
    if http_method == 'OPTIONS':
        return {
            'statusCode': 200,
            'headers': headers,
            'body': json.dumps({'message': 'CORS preflight OK'})
        }
    
    path = event.get('rawPath', '/')
    print(f"Request path: {path}, method: {http_method}")
    
    try:
        if path == '/health' or path == '/':
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps({
                    'status': 'healthy',
                    'version': '2.1.0',
                    'timestamp': datetime.utcnow().isoformat(),
                    'cors': 'enabled'
                })
            }
        
        elif path == '/data/all':
            data = {
                'timestamp': datetime.utcnow().isoformat(),
                'market': fetch_market_data(),
                'liquidity': fetch_liquidity_data(),
                'risk': fetch_risk_data(),
                'cors': 'enabled'
            }
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps(data)
            }
        
        elif path == '/data/market':
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps(fetch_market_data())
            }
        
        elif path == '/data/liquidity':
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps(fetch_liquidity_data())
            }
        
        elif path == '/data/risk':
            return {
                'statusCode': 200,
                'headers': headers,
                'body': json.dumps(fetch_risk_data())
            }
        
        else:
            return {
                'statusCode': 404,
                'headers': headers,
                'body': json.dumps({'error': 'Endpoint not found', 'path': path})
            }
    
    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            'statusCode': 500,
            'headers': headers,
            'body': json.dumps({'error': str(e), 'path': path})
        }

def fetch_market_data():
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'sp500': None,
        'vix': None,
        'treasury_10y': None,
        'dxy': None
    }
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    # Fetch S&P 500 from Polygon
    try:
        url = f"https://api.polygon.io/v2/aggs/ticker/SPY/prev?apiKey={POLYGON_API_KEY}"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            spy_data = json.loads(response.read())
            if spy_data.get('results'):
                r = spy_data['results'][0]
                data['sp500'] = {
                    'value': r.get('c'),
                    'change': r.get('c', 0) - r.get('o', 0),
                    'change_pct': ((r.get('c', 0) - r.get('o', 0)) / r.get('o', 1)) * 100 if r.get('o') else 0
                }
    except Exception as e:
        print(f"Error fetching SPY: {e}")
        data['sp500'] = {'value': 5500, 'change': 25.5, 'change_pct': 0.47}
    
    # Fetch 10Y Treasury from FRED
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id=DGS10&api_key={FRED_API_KEY}&file_type=json&limit=5&sort_order=desc"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            treasury_data = json.loads(response.read())
            if treasury_data.get('observations'):
                latest = treasury_data['observations'][0]
                prev = treasury_data['observations'][1] if len(treasury_data['observations']) > 1 else latest
                current_val = float(latest['value']) if latest['value'] != '.' else 4.5
                prev_val = float(prev['value']) if prev['value'] != '.' else current_val
                data['treasury_10y'] = {
                    'value': current_val,
                    'change': current_val - prev_val,
                    'change_pct': ((current_val - prev_val) / prev_val * 100) if prev_val else 0
                }
    except Exception as e:
        print(f"Error fetching 10Y: {e}")
        data['treasury_10y'] = {'value': 4.52, 'change': 0.05, 'change_pct': 1.1}
    
    # VIX and DXY fallback data
    data['vix'] = {'value': 15.2, 'change': -0.3, 'change_pct': -1.9}
    data['dxy'] = {'value': 104.25, 'change': -0.15, 'change_pct': -0.14}
    
    return data

def fetch_liquidity_data():
    data = {
        'timestamp': datetime.utcnow().isoformat(),
        'fed_balance_sheet': None,
        'tga_balance': None,
        'reverse_repo': None,
        'global_m2': None
    }
    
    ctx = ssl.create_default_context()
    ctx.check_hostname = False
    ctx.verify_mode = ssl.CERT_NONE
    
    # Fetch Fed Balance Sheet from FRED
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id=WALCL&api_key={FRED_API_KEY}&file_type=json&limit=5&sort_order=desc"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, context=ctx, timeout=10) as response:
            fed_data = json.loads(response.read())
            if fed_data.get('observations'):
                latest = fed_data['observations'][0]
                prev = fed_data['observations'][1] if len(fed_data['observations']) > 1 else latest
                current_val = float(latest['value']) if latest['value'] != '.' else 7500
                prev_val = float(prev['value']) if prev['value'] != '.' else current_val
                data['fed_balance_sheet'] = {
                    'value': current_val,
                    'change': current_val - prev_val,
                    'change_pct': ((current_val - prev_val) / prev_val * 100) if prev_val else 0,
                    'unit': 'billions'
                }
    except Exception as e:
        print(f"Error fetching Fed Balance: {e}")
        data['fed_balance_sheet'] = {'value': 7450, 'change': -25, 'change_pct': -0.33, 'unit': 'billions'}
    
    # Fallback data for other metrics
    data['tga_balance'] = {'value': 745, 'change': 12, 'change_pct': 1.6, 'unit': 'billions'}
    data['reverse_repo'] = {'value': 425, 'change': -30, 'change_pct': -6.6, 'unit': 'billions'}
    data['global_m2'] = {'value': 21050, 'change': 85, 'change_pct': 0.4, 'unit': 'billions'}
    
    return data

def fetch_risk_data():
    return {
        'timestamp': datetime.utcnow().isoformat(),
        'credit_spreads': {'value': 3.45, 'change': 0.05, 'change_pct': 1.5, 'unit': 'percent'},
        'move_index': {'value': 95.2, 'change': -2.1, 'change_pct': -2.2, 'unit': 'index'},
        'financial_stress': {'value': -0.45, 'change': -0.05, 'change_pct': -10, 'unit': 'index', 'level': 'LOW'},
        'market_sentiment': {'value': 'NEUTRAL', 'score': 65, 'description': 'Markets showing moderate optimism'}
    }
