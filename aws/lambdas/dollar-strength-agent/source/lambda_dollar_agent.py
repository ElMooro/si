import json
import urllib.request
import urllib.parse
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    """Dollar Strength & Currency Metrics Agent"""
    
    fred_key = "2f057499936072679d8843d7fce99989"
    
    # Complete currency indicators
    currency_indicators = {
        # Dollar Index and Components
        'DXY': 'DTWEXBGS',  # Trade Weighted Dollar
        'DXY_BROAD': 'DTWEXBGS',  # Broad Dollar Index
        'DXY_MAJOR': 'DTWEXM',  # Major Currencies
        'DXY_OITP': 'DTWEXO',  # Other Important Trading Partners
        
        # Major Pairs
        'EURUSD': 'DEXUSEU',
        'USDJPY': 'DEXJPUS',
        'GBPUSD': 'DEXUSUK',
        'USDCAD': 'DEXCAUS',
        'USDCHF': 'DEXSFUS',
        'AUDUSD': 'DEXUSAL',
        'NZDUSD': 'DEXUSNZ',
        
        # Emerging Market Currencies
        'USDCNY': 'DEXCHUS',
        'USDMXN': 'DEXMXUS',
        'USDBRL': 'DEXBZUS',
        'USDINR': 'DEXINUS',
        'USDKRW': 'DEXKOUS',
        'USDTRY': 'DEXTOUS',
        'USDRUB': 'DEXRUUS',
        'USDZAR': 'DEXZAUS',
        
        # Nordic Currencies
        'USDNOK': 'DEXNOUS',
        'USDSEK': 'DEXSDUS',
        'USDDKK': 'DEXDNUS',
        
        # Asian Currencies
        'USDTHB': 'DEXTAUS',
        'USDSGD': 'DEXSIUS',
        'USDMYR': 'DEXMAUS',
        'USDIDR': 'DEXIDUS',
        'USDPHP': 'DEXPHUS',
        'USDTWD': 'DEXTEUS',
        
        # Real Exchange Rates
        'REAL_BROAD': 'RTWEXBGS',
        'REAL_MAJOR': 'RTWEXM',
        'REAL_OITP': 'RTWEXO'
    }
    
    results = {}
    
    # Fetch all currency data
    for name, series_id in currency_indicators.items():
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
                    # Calculate changes
                    changes = {}
                    if len(obs) > 1 and obs[1]['value'] != '.':
                        changes['1D'] = ((current / float(obs[1]['value']) - 1) * 100) if float(obs[1]['value']) != 0 else 0
                    if len(obs) > 7 and obs[7]['value'] != '.':
                        changes['1W'] = ((current / float(obs[7]['value']) - 1) * 100) if float(obs[7]['value']) != 0 else 0
                    if len(obs) > 30 and obs[30]['value'] != '.':
                        changes['1M'] = ((current / float(obs[30]['value']) - 1) * 100) if float(obs[30]['value']) != 0 else 0
                    if len(obs) > 90 and obs[90]['value'] != '.':
                        changes['3M'] = ((current / float(obs[90]['value']) - 1) * 100) if float(obs[90]['value']) != 0 else 0
                    
                    results[name] = {
                        'current': current,
                        'date': obs[0]['date'],
                        'changes': changes,
                        'signal': interpret_dollar_signal(name, current, changes)
                    }
                    
        except Exception as e:
            print(f"Error fetching {name}: {str(e)}")
    
    # Analyze dollar strength
    analysis = analyze_dollar_strength(results)
    
    response_body = {
        'timestamp': datetime.now().isoformat(),
        'dollar_indices': {k: v for k, v in results.items() if 'DXY' in k or 'REAL' in k},
        'major_pairs': {k: v for k, v in results.items() if k in ['EURUSD', 'USDJPY', 'GBPUSD', 'USDCAD', 'USDCHF', 'AUDUSD', 'NZDUSD']},
        'emerging_markets': {k: v for k, v in results.items() if k in ['USDCNY', 'USDMXN', 'USDBRL', 'USDINR', 'USDTRY']},
        'asian_currencies': {k: v for k, v in results.items() if k in ['USDTHB', 'USDSGD', 'USDMYR', 'USDIDR', 'USDPHP']},
        'analysis': analysis,
        'recommendations': generate_dollar_recommendations(analysis, results)
    }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response_body, cls=DecimalEncoder)
    }

def interpret_dollar_signal(name, value, changes):
    """Interpret currency signals"""
    
    if 'DXY' in name:
        # Dollar index levels
        if value > 120:
            signal = 'EXTREME_STRENGTH'
        elif value > 110:
            signal = 'VERY_STRONG'
        elif value > 105:
            signal = 'STRONG'
        elif value > 95:
            signal = 'NEUTRAL'
        elif value > 90:
            signal = 'WEAK'
        else:
            signal = 'VERY_WEAK'
        
        # Check momentum
        if changes.get('1M', 0) > 5:
            signal += '_RAPID_RISE'
        elif changes.get('1M', 0) < -5:
            signal += '_RAPID_FALL'
            
        return signal
    
    elif name in ['EURUSD', 'GBPUSD', 'AUDUSD']:
        # For these pairs, lower = stronger dollar
        if changes.get('1M', 0) < -3:
            return 'DOLLAR_STRENGTHENING'
        elif changes.get('1M', 0) > 3:
            return 'DOLLAR_WEAKENING'
        else:
            return 'STABLE'
    
    elif name == 'USDJPY':
        # For USDJPY, higher = stronger dollar
        if value > 150:
            return 'YEN_EXTREMELY_WEAK'
        elif value > 140:
            return 'YEN_VERY_WEAK'
        elif value > 130:
            return 'YEN_WEAK'
        else:
            return 'NORMAL'
    
    return 'CHECK_DATA'

def analyze_dollar_strength(data):
    """Comprehensive dollar strength analysis"""
    
    # Get DXY level and trend
    dxy = data.get('DXY', {})
    dxy_level = dxy.get('current', 100)
    dxy_change_1m = dxy.get('changes', {}).get('1M', 0)
    dxy_change_3m = dxy.get('changes', {}).get('3M', 0)
    
    # Determine dollar trend
    if dxy_change_3m > 5:
        trend = 'STRONG_UPTREND'
    elif dxy_change_3m > 2:
        trend = 'UPTREND'
    elif dxy_change_3m < -5:
        trend = 'STRONG_DOWNTREND'
    elif dxy_change_3m < -2:
        trend = 'DOWNTREND'
    else:
        trend = 'SIDEWAYS'
    
    # Check for extremes
    extremes = []
    if dxy_level > 115:
        extremes.append('Dollar at multi-year highs')
    if dxy_level < 90:
        extremes.append('Dollar at multi-year lows')
    
    # Check emerging market stress
    em_stress = 0
    for currency in ['USDMXN', 'USDBRL', 'USDTRY']:
        if currency in data:
            change = data[currency].get('changes', {}).get('1M', 0)
            if change > 5:
                em_stress += 1
    
    em_status = 'CRISIS' if em_stress >= 3 else 'STRESSED' if em_stress >= 2 else 'STABLE'
    
    # Calculate carry trade impact
    usdjpy = data.get('USDJPY', {}).get('current', 110)
    carry_unwind_risk = 'HIGH' if usdjpy < 105 else 'MODERATE' if usdjpy < 115 else 'LOW'
    
    return {
        'dxy_level': dxy_level,
        'dollar_trend': trend,
        'strength_percentile': min(100, max(0, (dxy_level - 80) * 2.5)),  # 80-120 range
        'extremes': extremes,
        'em_currency_status': em_status,
        'carry_trade_risk': carry_unwind_risk,
        'intervention_risk': 'HIGH' if dxy_level > 120 else 'MODERATE' if dxy_level > 115 else 'LOW'
    }

def generate_dollar_recommendations(analysis, data):
    """Generate currency recommendations"""
    
    recommendations = []
    
    if analysis['dxy_level'] > 120:
        recommendations.append('Dollar extremely strong - intervention risk high')
    
    if analysis['dollar_trend'] == 'STRONG_UPTREND':
        recommendations.append('Dollar in strong uptrend - favor USD assets')
    
    if analysis['em_currency_status'] in ['CRISIS', 'STRESSED']:
        recommendations.append('EM currencies under pressure - avoid EM debt')
    
    if analysis['carry_trade_risk'] == 'HIGH':
        recommendations.append('Carry trade unwind risk - reduce leveraged positions')
    
    if analysis['intervention_risk'] == 'HIGH':
        recommendations.append('Central bank intervention likely - prepare for volatility')
    
    # Check USDJPY for BOJ intervention
    usdjpy = data.get('USDJPY', {}).get('current', 110)
    if usdjpy > 150:
        recommendations.append('USDJPY above 150 - BOJ intervention imminent')
    
    return recommendations

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
