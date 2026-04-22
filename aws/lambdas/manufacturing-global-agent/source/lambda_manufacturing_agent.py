import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    """Manufacturing Agent - Global Manufacturing Intelligence"""
    
    fred_key = "2f057499936072679d8843d7fce99989"
    
    # Complete manufacturing indicators
    manufacturing_indicators = {
        # US Manufacturing (ISM)
        'ISM_COMPOSITE': 'NAPM',
        'ISM_NEW_ORDERS': 'NAPMNOI',
        'ISM_PRODUCTION': 'NAPMPRI',
        'ISM_EMPLOYMENT': 'NAPMEI',
        'ISM_SUPPLIER_DELIVERIES': 'NAPMSDI',
        'ISM_INVENTORIES': 'NAPMII',
        'ISM_PRICES_PAID': 'NAPMPRI',
        'ISM_BACKLOG': 'NAPMBI',
        'ISM_EXPORTS': 'NAPMEI',
        'ISM_IMPORTS': 'NAPMII',
        
        # Regional Fed Manufacturing Surveys
        'EMPIRE_STATE': 'GAFDIMSA',
        'PHILLY_FED': 'GAPHDFBA',
        'RICHMOND_FED': 'RMTSPL',
        'DALLAS_FED': 'DALLASFEDFAB',
        'KANSAS_CITY_FED': 'KCLFEDFAB',
        
        # Industrial Production
        'INDUSTRIAL_PRODUCTION': 'INDPRO',
        'CAPACITY_UTILIZATION': 'TCU',
        'MANUFACTURING_PRODUCTION': 'IPMAN',
        'DURABLE_GOODS': 'IPDMAN',
        'NONDURABLE_GOODS': 'IPNMAN',
        'BUSINESS_EQUIPMENT': 'IPBUSEQ',
        'CONSUMER_GOODS': 'IPCONGD',
        'MATERIALS': 'IPMAT',
        
        # Global Manufacturing PMIs
        'CHINA_MANUFACTURING_PMI': 'CHEFMNM156N',
        'EUROZONE_MANUFACTURING_PMI': 'EA19PRMNTO01IXOBM',
        'JAPAN_MANUFACTURING_PMI': 'JPNPRMNTO01IXOBM',
        'UK_MANUFACTURING_PMI': 'GBRPRMNTO01IXOBM',
        'GERMANY_MANUFACTURING_PMI': 'DEUPRMNTO01IXOBM',
        'FRANCE_MANUFACTURING_PMI': 'FRAPRMNTO01IXOBM',
        
        # Manufacturing Employment
        'MANUFACTURING_EMPLOYMENT': 'MANEMP',
        'MANUFACTURING_HOURS': 'AWHMAN',
        'MANUFACTURING_EARNINGS': 'CES3000000003',
        'MANUFACTURING_OVERTIME': 'CES3000000004',
        
        # Orders and Inventories
        'DURABLE_GOODS_ORDERS': 'DGORDER',
        'NEW_ORDERS': 'NEWORDER',
        'UNFILLED_ORDERS': 'AMTUNO',
        'INVENTORIES_TO_SALES': 'ISRATIO',
        'MANUFACTURING_INVENTORIES': 'MNFCTRIMSA'
    }
    
    results = {}
    
    # Fetch all manufacturing data
    for name, series_id in manufacturing_indicators.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': fred_key,
                'file_type': 'json',
                'limit': '90',
                'sort_order': 'desc'
            }
            
            full_url = f"{url}?{urllib.parse.urlencode(params)}"
            
            with urllib.request.urlopen(full_url) as response:
                data = json.loads(response.read())
            
            if 'observations' in data and data['observations']:
                obs = data['observations']
                current = float(obs[0]['value']) if obs[0]['value'] != '.' else None
                
                if current is not None:
                    # Calculate trends
                    changes = {}
                    if len(obs) > 1 and obs[1]['value'] != '.':
                        changes['1D'] = current - float(obs[1]['value'])
                    if len(obs) > 30 and obs[30]['value'] != '.':
                        changes['1M'] = current - float(obs[30]['value'])
                    if len(obs) > 90 and obs[90]['value'] != '.':
                        changes['3M'] = current - float(obs[90]['value'])
                    
                    results[name] = {
                        'current': current,
                        'date': obs[0]['date'],
                        'changes': changes,
                        'signal': interpret_manufacturing_signal(name, current)
                    }
                    
        except Exception as e:
            print(f"Error fetching {name}: {str(e)}")
    
    # Generate comprehensive analysis
    analysis = analyze_global_manufacturing(results)
    
    # ECB manufacturing data (would add actual ECB API calls here)
    ecb_data = {
        'note': 'ECB manufacturing data integration pending',
        'eurozone_sentiment': 'MODERATE'
    }
    
    response_body = {
        'timestamp': datetime.now().isoformat(),
        'us_manufacturing': {k: v for k, v in results.items() if 'ISM' in k or 'FED' in k},
        'global_manufacturing': {k: v for k, v in results.items() if 'PMI' in k or 'CHINA' in k or 'EURO' in k},
        'industrial_production': {k: v for k, v in results.items() if 'PRODUCTION' in k or 'CAPACITY' in k},
        'orders_inventories': {k: v for k, v in results.items() if 'ORDER' in k or 'INVENTOR' in k},
        'ecb_data': ecb_data,
        'analysis': analysis,
        'recommendations': generate_manufacturing_recommendations(analysis)
    }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response_body, cls=DecimalEncoder)
    }

def interpret_manufacturing_signal(name, value):
    """Interpret manufacturing indicators"""
    
    if 'ISM' in name or 'PMI' in name:
        if value >= 60:
            return 'EXPANSION_STRONG'
        elif value >= 55:
            return 'EXPANSION_MODERATE'
        elif value >= 50:
            return 'EXPANSION_WEAK'
        elif value >= 45:
            return 'CONTRACTION_MILD'
        elif value >= 40:
            return 'CONTRACTION_MODERATE'
        else:
            return 'CONTRACTION_SEVERE'
    
    elif 'CAPACITY' in name:
        if value >= 80:
            return 'OVERHEATING'
        elif value >= 75:
            return 'NORMAL_HIGH'
        elif value >= 70:
            return 'NORMAL'
        else:
            return 'UNDERUTILIZED'
    
    elif 'INVENTORIES' in name:
        if value > 1.4:
            return 'EXCESSIVE_INVENTORY'
        elif value > 1.3:
            return 'HIGH_INVENTORY'
        elif value > 1.2:
            return 'NORMAL'
        else:
            return 'LOW_INVENTORY'
    
    return 'CHECK_DATA'

def analyze_global_manufacturing(data):
    """Analyze global manufacturing conditions"""
    
    # Check ISM level
    ism = data.get('ISM_COMPOSITE', {}).get('current', 50)
    
    # Determine manufacturing cycle
    if ism >= 60:
        cycle = 'BOOM'
    elif ism >= 55:
        cycle = 'EXPANSION'
    elif ism >= 50:
        cycle = 'SLOW_GROWTH'
    elif ism >= 45:
        cycle = 'CONTRACTION'
    else:
        cycle = 'RECESSION'
    
    # Check global synchronization
    global_pmis = []
    for key in ['CHINA_MANUFACTURING_PMI', 'EUROZONE_MANUFACTURING_PMI', 'JAPAN_MANUFACTURING_PMI']:
        if key in data:
            global_pmis.append(data[key].get('current', 50))
    
    if global_pmis:
        avg_global = sum(global_pmis) / len(global_pmis)
        if avg_global > 52:
            global_status = 'SYNCHRONIZED_GROWTH'
        elif avg_global > 50:
            global_status = 'MIXED_GROWTH'
        else:
            global_status = 'SYNCHRONIZED_CONTRACTION'
    else:
        global_status = 'UNKNOWN'
    
    return {
        'us_cycle': cycle,
        'global_status': global_status,
        'ism_level': ism,
        'expansion_probability': max(0, min(100, (ism - 42) * 2)),
        'recession_risk': 'HIGH' if ism < 45 else 'MODERATE' if ism < 50 else 'LOW'
    }

def generate_manufacturing_recommendations(analysis):
    """Generate actionable recommendations"""
    
    recommendations = []
    
    if analysis['us_cycle'] == 'RECESSION':
        recommendations.append('Manufacturing in recession - defensive positioning')
    
    if analysis['recession_risk'] == 'HIGH':
        recommendations.append('High recession risk - reduce cyclical exposure')
    
    if analysis['global_status'] == 'SYNCHRONIZED_CONTRACTION':
        recommendations.append('Global manufacturing weakness - favor defensive sectors')
    
    if analysis['ism_level'] < 43:
        recommendations.append('ISM below 43 - historical recession indicator')
    
    return recommendations

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
