import json
import urllib.request
import urllib.parse
from datetime import datetime

def lambda_handler(event, context):
    """Bureau of Economic Analysis Agent"""
    
    api_key = "997E5691-4F0E-4774-8B4E-CAE836D4AC47"
    base_url = "https://apps.bea.gov/api/data"
    
    results = {}
    
    # Key BEA datasets
    datasets = {
        'gdp': {
            'dataset': 'NIPA',
            'table': 'T10101',
            'frequency': 'Q',
            'year': '2024,2025'
        },
        'personal_income': {
            'dataset': 'Regional',
            'table': 'CAINC1',
            'linecode': '1',
            'geofips': 'STATE'
        },
        'corporate_profits': {
            'dataset': 'NIPA', 
            'table': 'T61600D',
            'frequency': 'Q',
            'year': '2024,2025'
        }
    }
    
    # Fetch GDP data
    try:
        gdp_params = {
            'UserID': api_key,
            'method': 'GetData',
            'datasetname': 'NIPA',
            'TableName': 'T10101',
            'Frequency': 'Q',
            'Year': '2024,2025',
            'ResultFormat': 'JSON'
        }
        
        gdp_url = f"{base_url}?{urllib.parse.urlencode(gdp_params)}"
        
        with urllib.request.urlopen(gdp_url) as response:
            gdp_data = json.loads(response.read())
            
        if 'BEAAPI' in gdp_data and 'Results' in gdp_data['BEAAPI']:
            results['gdp'] = parse_bea_results(gdp_data['BEAAPI']['Results'])
    except Exception as e:
        results['gdp'] = {'error': str(e)}
    
    # Fetch Personal Income
    try:
        income_params = {
            'UserID': api_key,
            'method': 'GetData',
            'datasetname': 'Regional',
            'TableName': 'CAINC1',
            'LineCode': '1',
            'GeoFIPS': 'STATE',
            'Year': 'LAST5',
            'ResultFormat': 'JSON'
        }
        
        income_url = f"{base_url}?{urllib.parse.urlencode(income_params)}"
        
        with urllib.request.urlopen(income_url) as response:
            income_data = json.loads(response.read())
            
        if 'BEAAPI' in income_data and 'Results' in income_data['BEAAPI']:
            results['personal_income'] = parse_bea_results(income_data['BEAAPI']['Results'])
    except Exception as e:
        results['personal_income'] = {'error': str(e)}
    
    # Fetch Corporate Profits
    try:
        profits_params = {
            'UserID': api_key,
            'method': 'GetData',
            'datasetname': 'NIPA',
            'TableName': 'T61600D',
            'Frequency': 'Q',
            'Year': '2024,2025',
            'ResultFormat': 'JSON'
        }
        
        profits_url = f"{base_url}?{urllib.parse.urlencode(profits_params)}"
        
        with urllib.request.urlopen(profits_url) as response:
            profits_data = json.loads(response.read())
            
        if 'BEAAPI' in profits_data and 'Results' in profits_data['BEAAPI']:
            results['corporate_profits'] = parse_bea_results(profits_data['BEAAPI']['Results'])
    except Exception as e:
        results['corporate_profits'] = {'error': str(e)}
    
    # Generate analysis
    analysis = analyze_bea_data(results)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'timestamp': datetime.now().isoformat(),
            'bea_data': results,
            'analysis': analysis,
            'recommendations': generate_bea_recommendations(analysis)
        })
    }

def parse_bea_results(results):
    """Parse BEA API results"""
    if isinstance(results, dict) and 'Data' in results:
        return results['Data']
    elif isinstance(results, list):
        return results[:10]  # Return first 10 results
    return results

def analyze_bea_data(data):
    """Analyze BEA economic data"""
    
    analysis = {
        'gdp_trend': 'UNKNOWN',
        'income_trend': 'UNKNOWN',
        'profits_trend': 'UNKNOWN'
    }
    
    # Analyze GDP trend
    if 'gdp' in data and not isinstance(data['gdp'], dict) or not data['gdp'].get('error'):
        # Would parse actual GDP growth rates here
        analysis['gdp_trend'] = 'EXPANSION'  # Placeholder
    
    # Analyze income trend
    if 'personal_income' in data:
        analysis['income_trend'] = 'GROWING'  # Placeholder
    
    # Analyze profits
    if 'corporate_profits' in data:
        analysis['profits_trend'] = 'STRONG'  # Placeholder
    
    return analysis

def generate_bea_recommendations(analysis):
    """Generate recommendations from BEA data"""
    
    recommendations = []
    
    if analysis['gdp_trend'] == 'EXPANSION':
        recommendations.append('GDP expanding - growth assets favored')
    elif analysis['gdp_trend'] == 'CONTRACTION':
        recommendations.append('GDP contracting - defensive positioning')
    
    if analysis['profits_trend'] == 'DECLINING':
        recommendations.append('Corporate profits declining - earnings risk ahead')
    
    return recommendations
