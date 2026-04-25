import json
import urllib.request
from datetime import datetime, timedelta

def lambda_handler(event, context):
    """Bureau of Labor Statistics Agent"""
    
    api_key = "a759447531f04f1f861f29a381aab863"
    base_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    
    # Key BLS series
    series_ids = {
        'unemployment_rate': 'LNS14000000',
        'nonfarm_payrolls': 'CES0000000001',
        'average_hourly_earnings': 'CES0500000003',
        'labor_force_participation': 'LNS11300000',
        'job_openings': 'JTS00000000JOL',
        'quits_rate': 'JTS00000000QUR',
        'cpi_all_items': 'CUUR0000SA0',
        'cpi_core': 'CUUR0000SA0L1E',
        'ppi_final_demand': 'WPUFD4',
        'employment_cost_index': 'CIU1010000000000A'
    }
    
    results = {}
    
    # Calculate date range (last 3 years)
    end_year = datetime.now().year
    start_year = end_year - 2
    
    # Fetch all series data
    for name, series_id in series_ids.items():
        try:
            # BLS API request payload
            headers = {'Content-Type': 'application/json'}
            data = json.dumps({
                "seriesid": [series_id],
                "startyear": str(start_year),
                "endyear": str(end_year),
                "registrationkey": api_key
            })
            
            req = urllib.request.Request(
                base_url,
                data=data.encode('utf-8'),
                headers=headers
            )
            
            with urllib.request.urlopen(req) as response:
                result = json.loads(response.read())
            
            if result['status'] == 'REQUEST_SUCCEEDED':
                series_data = result['Results']['series'][0]['data']
                
                # Get latest values and calculate changes
                latest = float(series_data[0]['value'])
                year_ago = float(series_data[12]['value']) if len(series_data) > 12 else latest
                
                results[name] = {
                    'current': latest,
                    'period': series_data[0]['period'],
                    'year': series_data[0]['year'],
                    'year_over_year_change': latest - year_ago,
                    'year_over_year_percent': ((latest - year_ago) / year_ago * 100) if year_ago != 0 else 0,
                    'trend': analyze_trend(series_data[:12])
                }
                
        except Exception as e:
            results[name] = {'error': str(e)}
    
    # Generate labor market analysis
    analysis = analyze_labor_market(results)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'timestamp': datetime.now().isoformat(),
            'labor_statistics': results,
            'analysis': analysis,
            'recommendations': generate_bls_recommendations(analysis, results)
        })
    }

def analyze_trend(data):
    """Analyze trend from time series data"""
    if len(data) < 3:
        return 'INSUFFICIENT_DATA'
    
    # Simple trend analysis
    recent = float(data[0]['value'])
    older = float(data[2]['value']) if len(data) > 2 else recent
    
    if recent > older * 1.01:
        return 'RISING'
    elif recent < older * 0.99:
        return 'FALLING'
    else:
        return 'STABLE'

def analyze_labor_market(data):
    """Analyze overall labor market conditions"""
    
    analysis = {
        'labor_market_strength': 'UNKNOWN',
        'wage_pressure': 'UNKNOWN',
        'inflation_pressure': 'UNKNOWN'
    }
    
    # Check unemployment
    if 'unemployment_rate' in data and not data['unemployment_rate'].get('error'):
        unemployment = data['unemployment_rate']['current']
        if unemployment < 4:
            analysis['labor_market_strength'] = 'TIGHT'
        elif unemployment < 5:
            analysis['labor_market_strength'] = 'NORMAL'
        else:
            analysis['labor_market_strength'] = 'WEAK'
    
    # Check wage growth
    if 'average_hourly_earnings' in data and not data['average_hourly_earnings'].get('error'):
        wage_growth = data['average_hourly_earnings']['year_over_year_percent']
        if wage_growth > 4:
            analysis['wage_pressure'] = 'HIGH'
        elif wage_growth > 2.5:
            analysis['wage_pressure'] = 'MODERATE'
        else:
            analysis['wage_pressure'] = 'LOW'
    
    # Check inflation
    if 'cpi_all_items' in data and not data['cpi_all_items'].get('error'):
        inflation = data['cpi_all_items']['year_over_year_percent']
        if inflation > 3:
            analysis['inflation_pressure'] = 'HIGH'
        elif inflation > 2:
            analysis['inflation_pressure'] = 'MODERATE'
        else:
            analysis['inflation_pressure'] = 'LOW'
    
    return analysis

def generate_bls_recommendations(analysis, data):
    """Generate recommendations from labor data"""
    
    recommendations = []
    
    if analysis['labor_market_strength'] == 'TIGHT':
        recommendations.append('Tight labor market - wage inflation risk')
    elif analysis['labor_market_strength'] == 'WEAK':
        recommendations.append('Weak labor market - deflationary pressure')
    
    if analysis['wage_pressure'] == 'HIGH':
        recommendations.append('High wage growth - margin pressure for companies')
    
    if analysis['inflation_pressure'] == 'HIGH':
        recommendations.append('Elevated inflation - Fed likely to remain hawkish')
    
    # Check job openings vs unemployment
    if 'job_openings' in data and 'unemployment_rate' in data:
        openings = data['job_openings'].get('current', 0)
        unemployment = data['unemployment_rate'].get('current', 5)
        if openings > unemployment * 2000:  # Rough calculation
            recommendations.append('Labor shortage continues - structural inflation risk')
    
    return recommendations
