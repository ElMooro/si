import json
import boto3
import urllib.request
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    """Enhanced Repo Agent with updated NY Fed endpoints"""
    
    repo_data = {}
    
    # Updated NY Fed endpoints (these work as of Sept 2024)
    try:
        # Get RRP data from summary endpoint
        rrp_url = "https://markets.newyorkfed.org/api/rp/all/all/results/latest.json"
        with urllib.request.urlopen(rrp_url) as response:
            data = json.loads(response.read())
            
        if data and 'repo' in data:
            operations = data['repo']
            if operations:
                latest = operations[0]
                repo_data['rrp'] = {
                    'volume': latest.get('totalAmtAccepted', 0),
                    'rate': latest.get('stopOutRate', 0),
                    'participants': latest.get('totalBidCount', 0)
                }
    except Exception as e:
        repo_data['rrp'] = {'note': 'Using FRED RRP data instead', 'error': str(e)}
    
    # Get SOFR and other rates (this is working)
    try:
        rates_url = "https://markets.newyorkfed.org/api/rates/all/latest.json"
        with urllib.request.urlopen(rates_url) as response:
            rates = json.loads(response.read())
            
        if rates and 'refRates' in rates:
            repo_data['rates'] = {
                'SOFR': next((r['percentRate'] for r in rates['refRates'] if r['type'] == 'SOFR'), None),
                'EFFR': next((r['percentRate'] for r in rates['refRates'] if r['type'] == 'EFFR'), None),
                'OBFR': next((r['percentRate'] for r in rates['refRates'] if r['type'] == 'OBFR'), None)
            }
    except:
        pass
    
    # Use FRED as backup for RRP data
    fred_key = "2f057499936072679d8843d7fce99989"
    try:
        rrp_url = f"https://api.stlouisfed.org/fred/series/observations?series_id=RRPONTSYD&api_key={fred_key}&file_type=json&limit=1&sort_order=desc"
        with urllib.request.urlopen(rrp_url) as response:
            fred_data = json.loads(response.read())
            
        if fred_data and 'observations' in fred_data:
            obs = fred_data['observations'][0]
            repo_data['rrp_fred'] = {
                'volume_billions': float(obs['value']),
                'date': obs['date']
            }
    except:
        pass
    
    # Calculate stress based on available data
    stress_score = 0
    components = []
    
    # Check SOFR-EFFR spread
    if 'rates' in repo_data:
        sofr = repo_data['rates'].get('SOFR', 0)
        effr = repo_data['rates'].get('EFFR', 0)
        if sofr and effr:
            spread = abs(sofr - effr)
            if spread > 0.10:
                stress_score += 30
                components.append(f'SOFR-EFFR spread wide: {spread:.2f}%')
    
    # Check RRP from FRED
    if 'rrp_fred' in repo_data:
        rrp_vol = repo_data['rrp_fred'].get('volume_billions', 0)
        if rrp_vol > 2000:  # $2T
            stress_score += 30
            components.append(f'RRP extremely high: ${rrp_vol:.0f}B')
        elif rrp_vol > 1000:  # $1T
            stress_score += 20
            components.append(f'RRP elevated: ${rrp_vol:.0f}B')
    
    # Determine stress level
    if stress_score > 60:
        level = 'CRITICAL'
    elif stress_score > 40:
        level = 'HIGH'
    elif stress_score > 20:
        level = 'MODERATE'
    else:
        level = 'LOW'
    
    analysis = {
        'repo_stress_index': stress_score,
        'stress_level': level,
        'components': components,
        'market_functioning': 'impaired' if stress_score > 40 else 'stressed' if stress_score > 20 else 'normal'
    }
    
    # Generate recommendations
    recommendations = []
    if level in ['CRITICAL', 'HIGH']:
        recommendations.append('Repo markets showing stress - monitor collateral availability')
    if 'rrp_fred' in repo_data and repo_data['rrp_fred'].get('volume_billions', 0) > 1500:
        recommendations.append('High RRP usage indicates excess liquidity')
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'timestamp': datetime.now().isoformat(),
            'repo_markets': repo_data,
            'stress_analysis': analysis,
            'recommendations': recommendations
        }, cls=DecimalEncoder)
    }

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
