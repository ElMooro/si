import json
import urllib.request
import urllib.parse
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    """Main handler for Cross-Currency Basis Agent"""
    
    fred_key = "2f057499936072679d8843d7fce99989"
    
    # FRED doesn't have direct cross-currency basis, but we can use related indicators
    indicators = {
        'DXY': 'DTWEXBGS',  # Dollar Index
        'EURUSD': 'DEXUSEU',  # EUR/USD exchange rate
        'JPYUSD': 'DEXJPUS',  # JPY/USD exchange rate
        'GBPUSD': 'DEXUSUK',  # GBP/USD exchange rate
        'TED_SPREAD': 'TEDRATE',  # Dollar funding stress proxy
        'LIBOR_3M': 'USD3MTD156N',  # 3-Month LIBOR
        'TREASURY_3M': 'DGS3MO'  # 3-Month Treasury
    }
    
    data = {}
    
    for name, series_id in indicators.items():
        try:
            # Build FRED URL
            base_url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': fred_key,
                'file_type': 'json',
                'limit': '90',
                'sort_order': 'desc'
            }
            
            url = f"{base_url}?{urllib.parse.urlencode(params)}"
            
            # Make request
            with urllib.request.urlopen(url) as response:
                result = json.loads(response.read())
            
            if 'observations' in result and result['observations']:
                obs = result['observations']
                
                current = float(obs[0]['value']) if obs[0]['value'] != '.' else None
                
                if current is not None:
                    # Calculate changes
                    changes = {}
                    
                    if len(obs) > 7 and obs[7]['value'] != '.':
                        changes['1W'] = current - float(obs[7]['value'])
                    
                    if len(obs) > 30 and obs[30]['value'] != '.':
                        changes['1M'] = current - float(obs[30]['value'])
                    
                    if len(obs) > 90 and obs[90]['value'] != '.':
                        changes['3M'] = current - float(obs[90]['value'])
                    
                    data[name] = {
                        'current': current,
                        'date': obs[0]['date'],
                        'changes': changes
                    }
                    
        except Exception as e:
            print(f"Error fetching {name}: {str(e)}")
    
    # Calculate synthetic dollar funding stress
    funding_stress = calculate_dollar_funding_stress(data)
    
    response_body = {
        'timestamp': datetime.now().isoformat(),
        'currency_indicators': data,
        'dollar_funding_stress': funding_stress,
        'analysis': generate_currency_analysis(data, funding_stress),
        'recommendations': generate_currency_recommendations(data, funding_stress)
    }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response_body, cls=DecimalEncoder)
    }

def calculate_dollar_funding_stress(data):
    """Calculate synthetic dollar funding stress from available data"""
    
    stress_score = 0
    components = []
    
    # Check DXY strength
    if 'DXY' in data:
        dxy = data['DXY'].get('current', 100)
        dxy_change = data['DXY'].get('changes', {}).get('1M', 0)
        
        if dxy > 110:
            stress_score += 30
            components.append('Dollar extremely strong')
        elif dxy > 105:
            stress_score += 20
            components.append('Dollar strength creating funding pressure')
            
        if dxy_change > 5:
            stress_score += 20
            components.append('Rapid dollar appreciation')
    
    # Check TED spread
    if 'TED_SPREAD' in data:
        ted = data['TED_SPREAD'].get('current', 0)
        if ted > 100:
            stress_score += 30
            components.append('TED spread indicating banking stress')
        elif ted > 50:
            stress_score += 20
            components.append('TED spread elevated')
    
    # Check LIBOR-Treasury spread
    if 'LIBOR_3M' in data and 'TREASURY_3M' in data:
        libor = data['LIBOR_3M'].get('current', 0)
        treasury = data['TREASURY_3M'].get('current', 0)
        spread = libor - treasury
        
        if spread > 100:
            stress_score += 25
            components.append('LIBOR-Treasury spread wide')
        elif spread > 50:
            stress_score += 15
            components.append('Funding spreads elevated')
    
    # Determine stress level
    if stress_score > 70:
        level = 'CRITICAL'
    elif stress_score > 50:
        level = 'HIGH'
    elif stress_score > 30:
        level = 'MODERATE'
    else:
        level = 'LOW'
    
    return {
        'funding_stress_score': stress_score,
        'stress_level': level,
        'stress_components': components,
        'interpretation': interpret_funding_stress(stress_score)
    }

def interpret_funding_stress(score):
    """Interpret the funding stress score"""
    
    if score > 70:
        return 'Severe dollar shortage. Central bank intervention likely needed.'
    elif score > 50:
        return 'Significant dollar funding stress building globally.'
    elif score > 30:
        return 'Moderate dollar funding pressure. Monitor closely.'
    else:
        return 'Normal dollar funding conditions.'

def generate_currency_analysis(data, funding_stress):
    """Generate currency market analysis"""
    
    analysis = {
        'dollar_trend': 'UNKNOWN',
        'funding_conditions': funding_stress['stress_level'],
        'key_risks': []
    }
    
    # Analyze dollar trend
    if 'DXY' in data:
        dxy_change = data['DXY'].get('changes', {}).get('3M', 0)
        if dxy_change > 5:
            analysis['dollar_trend'] = 'STRONG_UPTREND'
            analysis['key_risks'].append('Rapid dollar strengthening')
        elif dxy_change > 2:
            analysis['dollar_trend'] = 'UPTREND'
        elif dxy_change < -5:
            analysis['dollar_trend'] = 'STRONG_DOWNTREND'
        elif dxy_change < -2:
            analysis['dollar_trend'] = 'DOWNTREND'
        else:
            analysis['dollar_trend'] = 'SIDEWAYS'
    
    # Check for currency stress
    if 'EURUSD' in data:
        eur_change = data['EURUSD'].get('changes', {}).get('1M', 0)
        if abs(eur_change) > 0.05:
            analysis['key_risks'].append('EUR/USD volatility elevated')
    
    if 'JPYUSD' in data:
        jpy = data['JPYUSD'].get('current', 100)
        if jpy > 150:
            analysis['key_risks'].append('JPY extremely weak - intervention risk')
    
    return analysis

def generate_currency_recommendations(data, funding_stress):
    """Generate recommendations based on currency conditions"""
    
    recommendations = []
    
    if funding_stress['stress_level'] in ['CRITICAL', 'HIGH']:
        recommendations.append('Dollar funding stress elevated - monitor swap lines')
        
    if 'DXY' in data and data['DXY'].get('current', 100) > 110:
        recommendations.append('Dollar strength extreme - expect intervention')
        
    if 'TED_SPREAD' in data and data['TED_SPREAD'].get('current', 0) > 50:
        recommendations.append('Banking stress indicators rising - reduce leverage')
        
    return recommendations

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
