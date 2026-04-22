
import json
import urllib.request
import urllib.parse

def lambda_handler(event, context):
    API_KEY = 'EOLGKSGAYZUXKPUL'
    
    # Parse request
    body = json.loads(event.get('body', '{}')) if event.get('body') else event
    function = body.get('function', 'GLOBAL_QUOTE')
    symbol = body.get('symbol', 'SPY')
    
    # Build URL
    params = {
        'function': function,
        'symbol': symbol,
        'apikey': API_KEY
    }
    
    # Add technical indicator parameters
    if function in ['SMA', 'EMA', 'RSI', 'MACD', 'STOCH', 'BBANDS', 'ADX', 'CCI']:
        params['interval'] = body.get('interval', 'daily')
        params['time_period'] = body.get('time_period', '14')
        params['series_type'] = body.get('series_type', 'close')
    
    url = f"https://www.alphavantage.co/query?{urllib.parse.urlencode(params)}"
    
    try:
        with urllib.request.urlopen(url, timeout=10) as response:
            data = json.loads(response.read())
        
        return {
            'statusCode': 200,
            'headers': {
                'Content-Type': 'application/json',
                'Access-Control-Allow-Origin': '*'
            },
            'body': json.dumps(data)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'headers': {'Access-Control-Allow-Origin': '*'},
            'body': json.dumps({'error': str(e)})
        }
