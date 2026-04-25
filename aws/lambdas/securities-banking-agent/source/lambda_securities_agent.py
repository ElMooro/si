import json
import boto3
import urllib.request
import urllib.parse
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    """Securities & Banking Agent"""
    
    fred_key = "2f057499936072679d8843d7fce99989"
    
    # Banking and Securities indicators
    securities_indicators = {
        # Banking Metrics
        'TOTAL_LOANS': 'TOTLL',
        'BUSINESS_LOANS': 'BUSLOANS',
        'REAL_ESTATE_LOANS': 'REALLN',
        'CONSUMER_LOANS': 'CONSUMER',
        'LOANS_INVESTMENTS': 'LOANINV',
        'TREASURY_SECURITIES': 'USGSEC',
        'OTHER_SECURITIES': 'OTHSEC',
        'TOTAL_BANK_ASSETS': 'TLAACBW027SBOG',
        'TOTAL_DEPOSITS': 'DPSACBW027SBOG',
        'CASH_ASSETS': 'H8B1001NCBCMG',
        
        # Bank Performance
        'NONPERFORMING_LOANS': 'NDFACBS',
        'CHARGE_OFF_RATE': 'CHARGEOFF',
        'RETURN_ON_ASSETS': 'USROA',
        'RETURN_ON_EQUITY': 'USROE',
        'NET_INTEREST_MARGIN': 'USNIM',
        
        # Settlement Fails
        'FAILS_DELIVER': 'FAILSDEL',
        'FAILS_RECEIVE': 'FAILSREC',
        'TOTAL_FAILS': 'FAILSTOT',
        'TREASURY_FAILS': 'FAILTOT',
        
        # Securities Markets
        'PRIMARY_DEALER_POSITIONS': 'PDPOSLNG',
        'DEALER_FINANCING': 'PDFINLNG',
        'SECURITIES_LENDING': 'SLENTOT',
        
        # Bank Reserves
        'TOTAL_RESERVES': 'TOTRESNS',
        'EXCESS_RESERVES': 'EXCSRESNS',
        'REQUIRED_RESERVES': 'REQRESNS',
        'RESERVE_BALANCES': 'RESSBAL'
    }
    
    results = {}
    
    # Fetch all securities data
    for name, series_id in securities_indicators.items():
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations"
            params = {
                'series_id': series_id,
                'api_key': fred_key,
                'file_type': 'json',
                'limit': '30',
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
                    if len(obs) > 7 and obs[7]['value'] != '.':
                        changes['1W'] = current - float(obs[7]['value'])
                    if len(obs) > 30 and obs[30]['value'] != '.':
                        changes['1M'] = current - float(obs[30]['value'])
                    
                    results[name] = {
                        'current': current,
                        'date': obs[0]['date'],
                        'changes': changes,
                        'signal': interpret_securities_signal(name, current)
                    }
                    
        except Exception as e:
            print(f"Error fetching {name}: {str(e)}")
    
    # Analyze banking health
    analysis = analyze_banking_health(results)
    
    # If you have a separate securities API, call it here
    # securities_api_data = call_securities_api()
    
    response_body = {
        'timestamp': datetime.now().isoformat(),
        'banking_metrics': {k: v for k, v in results.items() if 'LOAN' in k or 'DEPOSIT' in k},
        'securities_metrics': {k: v for k, v in results.items() if 'SECURITIES' in k or 'DEALER' in k},
        'settlement_fails': {k: v for k, v in results.items() if 'FAIL' in k},
        'bank_reserves': {k: v for k, v in results.items() if 'RESERVE' in k},
        'analysis': analysis,
        'recommendations': generate_securities_recommendations(analysis, results)
    }
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps(response_body, cls=DecimalEncoder)
    }

def interpret_securities_signal(name, value):
    """Interpret securities indicators"""
    
    if 'FAIL' in name:
        # Settlement fails in billions
        if value > 200:
            return 'CRITICAL_STRESS'
        elif value > 100:
            return 'ELEVATED_STRESS'
        elif value > 50:
            return 'MODERATE_STRESS'
        else:
            return 'NORMAL'
    
    elif 'NONPERFORMING' in name:
        if value > 3:
            return 'HIGH_CREDIT_RISK'
        elif value > 2:
            return 'MODERATE_CREDIT_RISK'
        elif value > 1:
            return 'LOW_CREDIT_RISK'
        else:
            return 'MINIMAL_RISK'
    
    elif 'NET_INTEREST_MARGIN' in name:
        if value > 3.5:
            return 'HEALTHY_MARGINS'
        elif value > 3.0:
            return 'NORMAL_MARGINS'
        elif value > 2.5:
            return 'COMPRESSED_MARGINS'
        else:
            return 'STRESSED_MARGINS'
    
    return 'CHECK_DATA'

def analyze_banking_health(data):
    """Analyze overall banking system health"""
    
    health_score = 100
    issues = []
    
    # Check NPLs
    if 'NONPERFORMING_LOANS' in data:
        npl = data['NONPERFORMING_LOANS'].get('current', 0)
        if npl > 2:
            health_score -= 20
            issues.append('High nonperforming loans')
    
    # Check fails
    if 'TOTAL_FAILS' in data:
        fails = data['TOTAL_FAILS'].get('current', 0)
        if fails > 100:
            health_score -= 25
            issues.append('Settlement fails elevated')
    
    # Check margins
    if 'NET_INTEREST_MARGIN' in data:
        nim = data['NET_INTEREST_MARGIN'].get('current', 3)
        if nim < 3:
            health_score -= 15
            issues.append('Net interest margins compressed')
    
    # Determine health level
    if health_score >= 80:
        health = 'HEALTHY'
    elif health_score >= 60:
        health = 'MODERATE'
    elif health_score >= 40:
        health = 'STRESSED'
    else:
        health = 'CRITICAL'
    
    return {
        'banking_health': health,
        'health_score': health_score,
        'issues': issues,
        'systemic_risk': 'HIGH' if health_score < 50 else 'MODERATE' if health_score < 70 else 'LOW'
    }

def generate_securities_recommendations(analysis, data):
    """Generate recommendations"""
    
    recommendations = []
    
    if analysis['banking_health'] == 'CRITICAL':
        recommendations.append('Banking system critical - reduce financial sector exposure')
    
    if analysis['systemic_risk'] == 'HIGH':
        recommendations.append('Systemic risk elevated - increase cash reserves')
    
    if 'Settlement fails elevated' in analysis['issues']:
        recommendations.append('Settlement fails high - collateral shortage likely')
    
    return recommendations

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
