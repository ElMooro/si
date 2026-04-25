import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime, timedelta
from decimal import Decimal

def lambda_handler(event, context):
    """Main handler for Bond Indices Agent"""
    
    fred_key = "2f057499936072679d8843d7fce99989"
    
    # Bond indices mapping
    bond_indices = {
        # ICE BofA indices that actually exist in FRED
        'US_CORP_MASTER': 'BAMLC0A0CM',  # ICE BofA US Corporate Master
        'US_HIGH_YIELD': 'BAMLH0A0HYM2',  # ICE BofA US High Yield
        'AAA_CORP': 'BAMLC0A1CAAA',  # AAA Corporate
        'BBB_CORP': 'BAMLC0A4CBBB',  # BBB Corporate  
        'CCC_AND_LOWER': 'BAMLH0A3HYC',  # CCC & Lower
        'US_TREASURY_MASTER': 'BAMLCC0A1AAATRIV',  # Treasury Master
        
        # Additional key indices
        'MOVE_INDEX': 'MOVE',  # Bond volatility
        'TREASURY_10Y': 'DGS10',  # 10Y Treasury Yield
        'TREASURY_2Y': 'DGS2',  # 2Y Treasury Yield
        'TREASURY_30Y': 'DGS30',  # 30Y Treasury Yield
        'TERM_SPREAD': 'T10Y2Y',  # 10Y-2Y Spread
        'TED_SPREAD': 'TEDRATE',  # TED Spread
        
        # Credit spreads
        'BAA_TREASURY_SPREAD': 'BAA10Y',  # Moody's Baa-Treasury
        'AAA_TREASURY_SPREAD': 'AAA10Y',  # Moody's Aaa-Treasury
    }
    
    results = {}
    
    for name, series_id in bond_indices.items():
        try:
            # Build FRED URL
            base_url = "https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': fred_key,
                'file_type': 'json',
                'limit': '365',
                'sort_order': 'desc'
            }
            
            url = f"{base_url}?{urllib.parse.urlencode(params)}"
            
            # Make request
            with urllib.request.urlopen(url) as response:
                data = json.loads(response.read())
            
            if 'observations' in data and data['observations']:
                obs = data['observations']
                
                # Get current value
                current = float(obs[0]['value']) if obs[0]['value'] != '.' else None
                
                if current is not None:
                    # Calculate changes
                    changes = {}
                    
                    # 1 day change
                    if len(obs) > 1 and obs[1]['value'] != '.':
                        changes['1D'] = current - float(obs[1]['value'])
                    
                    # 1 week change  
                    if len(obs) > 7 and obs[7]['value'] != '.':
                        changes['1W'] = current - float(obs[7]['value'])
                    
                    # 1 month change
                    if len(obs) > 30 and obs[30]['value'] != '.':
                        changes['1M'] = current - float(obs[30]['value'])
                        
                    # 3 month change
                    if len(obs) > 90 and obs[90]['value'] != '.':
                        changes['3M'] = current - float(obs[90]['value'])
                    
                    # 1 year change
                    if len(obs) > 365 and obs[365]['value'] != '.':
                        changes['1Y'] = current - float(obs[365]['value'])
                    
                    results[name] = {
                        'current': current,
                        'date': obs[0]['date'],
                        'changes': changes,
                        'signal': interpret_signal(name, current, changes)
                    }
                    
        except Exception as e:
            print(f"Error fetching {name}: {str(e)}")
            
    # Generate analysis
    analysis = generate_bond_analysis(results)
    
    response_body = {
        'timestamp': datetime.now().isoformat(),
        'bond_indices': results,
        'analysis': analysis,
        'recommendations': generate_recommendations(results, analysis)
    }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response_body, cls=DecimalEncoder)
    }

def interpret_signal(name, current, changes):
    """Interpret what the current level means"""
    
    if 'HIGH_YIELD' in name:
        if current > 800:
            return 'DISTRESS'
        elif current > 500:
            return 'STRESS'
        elif current > 350:
            return 'ELEVATED'
        else:
            return 'NORMAL'
            
    elif 'TERM_SPREAD' in name:
        if current < 0:
            return 'INVERTED_RECESSION_SIGNAL'
        elif current < 0.5:
            return 'FLATTENING'
        elif current > 2:
            return 'STEEPENING'
        else:
            return 'NORMAL'
            
    elif 'MOVE' in name:
        if current > 150:
            return 'EXTREME_VOLATILITY'
        elif current > 100:
            return 'HIGH_VOLATILITY'
        elif current > 80:
            return 'ELEVATED'
        else:
            return 'LOW_VOLATILITY'
            
    elif 'TED_SPREAD' in name:
        if current > 100:
            return 'BANKING_STRESS'
        elif current > 50:
            return 'ELEVATED_STRESS'
        else:
            return 'NORMAL'
            
    else:
        return 'CHECK_DATA'

def generate_bond_analysis(data):
    """Generate comprehensive bond market analysis"""
    
    analysis = {
        'credit_conditions': 'UNKNOWN',
        'yield_curve': 'UNKNOWN',
        'volatility_regime': 'UNKNOWN',
        'risk_sentiment': 'UNKNOWN'
    }
    
    # Analyze credit conditions
    if 'US_HIGH_YIELD' in data:
        hy_spread = data['US_HIGH_YIELD'].get('current', 0)
        if hy_spread > 500:
            analysis['credit_conditions'] = 'STRESSED'
        elif hy_spread > 350:
            analysis['credit_conditions'] = 'TIGHT'
        else:
            analysis['credit_conditions'] = 'LOOSE'
    
    # Analyze yield curve
    if 'TERM_SPREAD' in data:
        term_spread = data['TERM_SPREAD'].get('current', 0)
        if term_spread < 0:
            analysis['yield_curve'] = 'INVERTED'
        elif term_spread < 0.5:
            analysis['yield_curve'] = 'FLAT'
        else:
            analysis['yield_curve'] = 'NORMAL'
    
    # Analyze volatility
    if 'MOVE_INDEX' in data:
        move = data['MOVE_INDEX'].get('current', 0)
        if move > 100:
            analysis['volatility_regime'] = 'HIGH'
        elif move > 80:
            analysis['volatility_regime'] = 'ELEVATED'
        else:
            analysis['volatility_regime'] = 'LOW'
    
    # Overall risk sentiment
    risk_score = 0
    if analysis['credit_conditions'] == 'STRESSED':
        risk_score += 40
    if analysis['yield_curve'] == 'INVERTED':
        risk_score += 30
    if analysis['volatility_regime'] == 'HIGH':
        risk_score += 30
        
    if risk_score > 70:
        analysis['risk_sentiment'] = 'RISK_OFF'
    elif risk_score > 40:
        analysis['risk_sentiment'] = 'CAUTIOUS'
    else:
        analysis['risk_sentiment'] = 'RISK_ON'
        
    analysis['risk_score'] = risk_score
    
    return analysis

def generate_recommendations(data, analysis):
    """Generate actionable recommendations"""
    
    recommendations = []
    
    if analysis.get('yield_curve') == 'INVERTED':
        recommendations.append('Yield curve inverted - consider defensive positioning')
        
    if analysis.get('credit_conditions') == 'STRESSED':
        recommendations.append('Credit spreads wide - potential opportunity in quality high yield')
        
    if analysis.get('volatility_regime') == 'HIGH':
        recommendations.append('Bond volatility elevated - consider reducing duration')
        
    if analysis.get('risk_sentiment') == 'RISK_OFF':
        recommendations.append('Risk-off environment - favor treasuries over credit')
        
    # Check for specific opportunities
    if 'CCC_AND_LOWER' in data:
        ccc = data['CCC_AND_LOWER'].get('current', 0)
        if ccc > 1500:
            recommendations.append('Distressed debt at extreme levels - selective opportunities')
            
    return recommendations

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
