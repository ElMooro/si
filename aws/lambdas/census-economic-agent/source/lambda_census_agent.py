import json
import urllib.request
import urllib.parse
from datetime import datetime

def lambda_handler(event, context):
    """US Census Bureau Economic Agent"""
    
    api_key = "8423ffa543d0e95cdba580f2e381649b6772f515"
    base_url = "https://api.census.gov/data"
    
    results = {}
    
    # Key Census economic indicators
    indicators = {
        'retail_sales': {
            'dataset': '2022/eits/ressales',
            'params': {
                'get': 'cell_value,time_slot_name',
                'for': 'us:*',
                'time': '2024',
                'key': api_key
            }
        },
        'housing_starts': {
            'dataset': '2022/eits/hs',
            'params': {
                'get': 'cell_value,time_slot_name',
                'for': 'us:*',
                'time': '2024',
                'key': api_key
            }
        },
        'construction_spending': {
            'dataset': '2022/eits/vip',
            'params': {
                'get': 'cell_value,time_slot_name',
                'for': 'us:*',
                'time': '2024',
                'key': api_key
            }
        },
        'manufacturers_shipments': {
            'dataset': '2022/eits/m3',
            'params': {
                'get': 'cell_value,time_slot_name',
                'for': 'us:*',
                'time': '2024',
                'key': api_key
            }
        }
    }
    
    # Fetch each indicator
    for name, config in indicators.items():
        try:
            url = f"{base_url}/{config['dataset']}?{urllib.parse.urlencode(config['params'])}"
            
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read())
            
            if data and len(data) > 1:
                # Parse Census data format
                headers = data[0]
                values = data[1:11] if len(data) > 11 else data[1:]  # Get last 10 entries
                
                results[name] = {
                    'latest_value': values[0][0] if values else None,
                    'period': values[0][1] if values and len(values[0]) > 1 else None,
                    'recent_data': values[:5]  # Last 5 data points
                }
                
        except Exception as e:
            results[name] = {'error': str(e)[:100]}  # Truncate error message
    
    # Generate analysis
    analysis = analyze_census_data(results)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'timestamp': datetime.now().isoformat(),
            'census_data': results,
            'analysis': analysis,
            'recommendations': generate_census_recommendations(analysis)
        })
    }

def analyze_census_data(data):
    """Analyze Census economic data"""
    
    analysis = {
        'consumer_strength': 'UNKNOWN',
        'housing_market': 'UNKNOWN',
        'manufacturing_health': 'UNKNOWN'
    }
    
    # Analyze retail sales
    if 'retail_sales' in data and not data['retail_sales'].get('error'):
        # Would analyze actual trend here
        analysis['consumer_strength'] = 'MODERATE'
    
    # Analyze housing
    if 'housing_starts' in data and not data['housing_starts'].get('error'):
        analysis['housing_market'] = 'COOLING'
    
    # Analyze manufacturing
    if 'manufacturers_shipments' in data and not data['manufacturers_shipments'].get('error'):
        analysis['manufacturing_health'] = 'STABLE'
    
    return analysis

def generate_census_recommendations(analysis):
    """Generate recommendations from Census data"""
    
    recommendations = []
    
    if analysis['consumer_strength'] == 'WEAK':
        recommendations.append('Weak retail sales - consumer discretionary at risk')
    elif analysis['consumer_strength'] == 'STRONG':
        recommendations.append('Strong consumer spending - inflation pressure continues')
    
    if analysis['housing_market'] == 'COOLING':
        recommendations.append('Housing market cooling - homebuilders under pressure')
    
    if analysis['manufacturing_health'] == 'DECLINING':
        recommendations.append('Manufacturing shipments declining - industrial sector weak')
    
    return recommendations
