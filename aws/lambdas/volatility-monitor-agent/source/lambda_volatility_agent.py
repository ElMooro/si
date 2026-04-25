import json
import urllib.request
import urllib.parse
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    """Volatility Agent - All volatility indices"""
    
    fred_key = "2f057499936072679d8843d7fce99989"
    
    # Complete volatility indicators
    volatility_indicators = {
        'VIX': 'VIXCLS',  # S&P 500 Volatility
        'VIX_OF_VIX': 'VVIXCLS',  # Volatility of VIX
        'VXN': 'VXNCLS',  # NASDAQ Volatility
        'RVX': 'RVXCLS',  # Russell 2000 Volatility
        'VXD': 'VXDCLS',  # Dow Jones Volatility
        'OVX': 'OVXCLS',  # Oil Volatility
        'GVZ': 'GVZCLS',  # Gold Volatility
        'EVZ': 'EVZCLS',  # Euro Currency Volatility
        'MOVE': 'MOVE',  # Bond Volatility
        
        # Term Structure
        'VIX9D': 'VXSTCLS',  # 9-Day VIX
        'VIX3M': 'VXMTCLS',  # 3-Month VIX
        'VIX6M': 'VXMTCLS',  # 6-Month VIX
        
        # Historical Volatility
        'SP500_REALIZED_VOL': 'DSPVOL',
        'NASDAQ_REALIZED_VOL': 'DNAVOL'
    }
    
    results = {}
    
    for name, series_id in volatility_indicators.items():
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
                    # Calculate statistics
                    values = [float(o['value']) for o in obs[:90] if o['value'] != '.']
                    
                    stats = {
                        'current': current,
                        'date': obs[0]['date'],
                        '1d_change': current - float(obs[1]['value']) if len(obs) > 1 and obs[1]['value'] != '.' else 0,
                        '1w_change': current - float(obs[7]['value']) if len(obs) > 7 and obs[7]['value'] != '.' else 0,
                        '1m_change': current - float(obs[30]['value']) if len(obs) > 30 and obs[30]['value'] != '.' else 0,
                        '30d_avg': sum(values[:30]) / min(30, len(values)) if values else current,
                        '90d_avg': sum(values) / len(values) if values else current,
                        '30d_high': max(values[:30]) if len(values) >= 30 else max(values) if values else current,
                        '30d_low': min(values[:30]) if len(values) >= 30 else min(values) if values else current,
                        'percentile_90d': calculate_percentile(current, values),
                        'signal': interpret_volatility_signal(name, current)
                    }
                    
                    results[name] = stats
                    
        except Exception as e:
            print(f"Error fetching {name}: {str(e)}")
    
    # Generate comprehensive volatility analysis
    analysis = analyze_volatility_regime(results)
    
    response_body = {
        'timestamp': datetime.now().isoformat(),
        'equity_volatility': {k: v for k, v in results.items() if k in ['VIX', 'VXN', 'RVX', 'VXD']},
        'asset_volatility': {k: v for k, v in results.items() if k in ['MOVE', 'OVX', 'GVZ', 'EVZ']},
        'term_structure': {k: v for k, v in results.items() if 'VIX' in k and k != 'VIX'},
        'analysis': analysis,
        'recommendations': generate_volatility_recommendations(analysis, results)
    }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response_body, cls=DecimalEncoder)
    }

def calculate_percentile(value, historical_values):
    """Calculate percentile rank"""
    if not historical_values:
        return 50
    
    count_below = sum(1 for v in historical_values if v < value)
    return (count_below / len(historical_values)) * 100

def interpret_volatility_signal(name, value):
    """Interpret volatility levels"""
    
    if 'VIX' in name and name != 'VIX_OF_VIX':
        if value > 80:
            return 'PANIC'
        elif value > 40:
            return 'EXTREME_FEAR'
        elif value > 30:
            return 'HIGH_FEAR'
        elif value > 20:
            return 'MODERATE_FEAR'
        elif value > 15:
            return 'NORMAL'
        elif value > 12:
            return 'LOW_VOL'
        else:
            return 'COMPLACENCY'
    
    elif name == 'MOVE':
        if value > 150:
            return 'BOND_PANIC'
        elif value > 120:
            return 'BOND_STRESS'
        elif value > 100:
            return 'ELEVATED'
        elif value > 80:
            return 'NORMAL'
        else:
            return 'LOW'
    
    elif name == 'OVX':
        if value > 60:
            return 'OIL_PANIC'
        elif value > 40:
            return 'OIL_STRESS'
        else:
            return 'NORMAL'
    
    return 'CHECK_DATA'

def analyze_volatility_regime(data):
    """Analyze overall volatility regime"""
    
    vix = data.get('VIX', {}).get('current', 20)
    vix_percentile = data.get('VIX', {}).get('percentile_90d', 50)
    move = data.get('MOVE', {}).get('current', 100)
    
    # Determine regime
    if vix > 30:
        regime = 'CRISIS'
    elif vix > 25:
        regime = 'STRESS'
    elif vix > 20:
        regime = 'ELEVATED'
    elif vix > 15:
        regime = 'NORMAL'
    elif vix > 12:
        regime = 'LOW'
    else:
        regime = 'SUPPRESSED'
    
    # Check for divergences
    divergences = []
    
    # VIX vs MOVE divergence
    if vix < 20 and move > 120:
        divergences.append('Bond vol high while equity vol low')
    elif vix > 30 and move < 80:
        divergences.append('Equity vol high while bond vol low')
    
    # Term structure
    if 'VIX9D' in data and 'VIX3M' in data:
        short_vol = data['VIX9D'].get('current', vix)
        long_vol = data['VIX3M'].get('current', vix)
        if short_vol > long_vol + 5:
            divergences.append('Inverted VIX term structure - near-term stress')
    
    return {
        'volatility_regime': regime,
        'vix_level': vix,
        'vix_percentile': vix_percentile,
        'move_level': move,
        'divergences': divergences,
        'risk_environment': 'RISK_OFF' if vix > 25 else 'RISK_ON' if vix < 15 else 'NEUTRAL'
    }

def generate_volatility_recommendations(analysis, data):
    """Generate volatility-based recommendations"""
    
    recommendations = []
    
    regime = analysis['volatility_regime']
    vix = analysis['vix_level']
    
    if regime == 'CRISIS':
        recommendations.append('VIX > 30: Crisis mode - maximum hedging needed')
    elif regime == 'SUPPRESSED':
        recommendations.append('VIX < 12: Complacency extreme - buy protection')
    
    if analysis['vix_percentile'] > 90:
        recommendations.append('VIX at 90th percentile - consider selling volatility')
    elif analysis['vix_percentile'] < 10:
        recommendations.append('VIX at 10th percentile - buy cheap protection')
    
    if analysis['divergences']:
        for divergence in analysis['divergences']:
            recommendations.append(f'Divergence detected: {divergence}')
    
    # Check VVIX for volatility of volatility
    if 'VIX_OF_VIX' in data:
        vvix = data['VIX_OF_VIX'].get('current', 100)
        if vvix > 140:
            recommendations.append('VVIX elevated - expect volatile swings in VIX')
    
    return recommendations

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
