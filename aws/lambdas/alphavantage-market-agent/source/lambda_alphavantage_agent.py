import json
import urllib.request
import urllib.parse
from datetime import datetime
from decimal import Decimal

def lambda_handler(event, context):
    """AlphaVantage Market Data Agent"""
    
    api_key = "EOLGKSGAYZUXKPUL"
    
    # Define what data to fetch based on path
    path = event.get('path', '/')
    
    results = {}
    
    # Core market data endpoints
    endpoints = {
        'market_overview': {
            'SPY': 'SPY',
            'QQQ': 'QQQ', 
            'IWM': 'IWM',
            'DIA': 'DIA',
            'VTI': 'VTI'
        },
        'sector_etfs': {
            'XLF': 'XLF',  # Financials
            'XLK': 'XLK',  # Technology
            'XLE': 'XLE',  # Energy
            'XLV': 'XLV',  # Healthcare
            'XLI': 'XLI',  # Industrials
            'XLY': 'XLY',  # Consumer Discretionary
            'XLP': 'XLP',  # Consumer Staples
            'XLU': 'XLU',  # Utilities
            'XLRE': 'XLRE', # Real Estate
            'XLB': 'XLB',  # Materials
            'XLC': 'XLC'   # Communications
        }
    }
    
    # Fetch real-time quotes
    for category, symbols in endpoints.items():
        results[category] = {}
        for name, symbol in symbols.items():
            try:
                url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol={symbol}&apikey={api_key}"
                
                with urllib.request.urlopen(url) as response:
                    data = json.loads(response.read())
                
                if 'Global Quote' in data:
                    quote = data['Global Quote']
                    results[category][name] = {
                        'symbol': quote.get('01. symbol'),
                        'price': float(quote.get('05. price', 0)),
                        'change': float(quote.get('09. change', 0)),
                        'change_percent': quote.get('10. change percent', '0%').replace('%', ''),
                        'volume': int(quote.get('06. volume', 0)),
                        'latest_trading_day': quote.get('07. latest trading day'),
                        'previous_close': float(quote.get('08. previous close', 0))
                    }
                    
            except Exception as e:
                print(f"Error fetching {symbol}: {str(e)}")
    
    # Fetch market sentiment indicators
    try:
        # Get Fear & Greed data (using VIX as proxy)
        vix_url = f"https://www.alphavantage.co/query?function=GLOBAL_QUOTE&symbol=VIX&apikey={api_key}"
        with urllib.request.urlopen(vix_url) as response:
            vix_data = json.loads(response.read())
            
        if 'Global Quote' in vix_data:
            vix_quote = vix_data['Global Quote']
            results['sentiment'] = {
                'vix': float(vix_quote.get('05. price', 0)),
                'fear_greed': calculate_fear_greed(float(vix_quote.get('05. price', 20)))
            }
    except:
        pass
    
    # Analyze market breadth
    analysis = analyze_market_breadth(results)
    
    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*'
        },
        'body': json.dumps({
            'timestamp': datetime.now().isoformat(),
            'market_data': results,
            'analysis': analysis,
            'recommendations': generate_av_recommendations(analysis, results)
        }, cls=DecimalEncoder)
    }

def calculate_fear_greed(vix):
    """Convert VIX to fear/greed score"""
    if vix > 30:
        return {'level': 'EXTREME_FEAR', 'score': 10}
    elif vix > 25:
        return {'level': 'FEAR', 'score': 25}
    elif vix > 20:
        return {'level': 'NEUTRAL_FEAR', 'score': 40}
    elif vix > 15:
        return {'level': 'NEUTRAL', 'score': 50}
    elif vix > 12:
        return {'level': 'GREED', 'score': 65}
    else:
        return {'level': 'EXTREME_GREED', 'score': 80}

def analyze_market_breadth(data):
    """Analyze market internals"""
    
    breadth = {
        'advancing': 0,
        'declining': 0,
        'unchanged': 0
    }
    
    # Check sectors
    if 'sector_etfs' in data:
        for sector, values in data['sector_etfs'].items():
            if values and 'change' in values:
                if values['change'] > 0:
                    breadth['advancing'] += 1
                elif values['change'] < 0:
                    breadth['declining'] += 1
                else:
                    breadth['unchanged'] += 1
    
    # Calculate breadth ratio
    total = breadth['advancing'] + breadth['declining']
    breadth_ratio = breadth['advancing'] / total if total > 0 else 0.5
    
    if breadth_ratio > 0.7:
        market_breadth = 'POSITIVE'
    elif breadth_ratio > 0.3:
        market_breadth = 'NEUTRAL'
    else:
        market_breadth = 'NEGATIVE'
    
    return {
        'market_breadth': market_breadth,
        'breadth_ratio': breadth_ratio,
        'advancing_sectors': breadth['advancing'],
        'declining_sectors': breadth['declining']
    }

def generate_av_recommendations(analysis, data):
    """Generate recommendations based on AlphaVantage data"""
    
    recommendations = []
    
    if analysis['market_breadth'] == 'POSITIVE':
        recommendations.append('Broad market participation - risk-on environment')
    elif analysis['market_breadth'] == 'NEGATIVE':
        recommendations.append('Narrow market breadth - selective positioning advised')
    
    # Check sentiment
    if 'sentiment' in data:
        vix = data['sentiment'].get('vix', 20)
        if vix > 30:
            recommendations.append('VIX > 30: Extreme fear - contrarian buying opportunity')
        elif vix < 12:
            recommendations.append('VIX < 12: Complacency high - consider hedging')
    
    return recommendations

class DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, Decimal):
            return float(obj)
        return super(DecimalEncoder, self).default(obj)
