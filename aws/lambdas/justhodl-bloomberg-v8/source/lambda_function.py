"""
JUSTHODL BLOOMBERG TERMINAL - V8 ULTIMATE
==========================================
Professional-grade financial terminal rivaling Bloomberg
80+ FRED series | 40+ stocks/ETFs | 14 agent endpoints
Khalid Index | Sector Rotation | Crisis Detection
Yield Curve | Credit Spreads | Global Liquidity
Real-time data aggregation from 10+ sources

Author: JustHodl.AI
"""

import json
import os
import boto3
import urllib.request
import urllib.error
import ssl
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# === CONFIG ===
FRED_KEY = os.environ.get('FRED_API_KEY', '2f057499936072679d8843d7fce99989')
POLYGON_KEY = os.environ.get('POLYGON_API_KEY', 'zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d')
ALPHAVANTAGE_KEY = os.environ.get('ALPHAVANTAGE_KEY', 'EOLGKSGAYZUXKPUL')
CMC_KEY = os.environ.get('CMC_KEY', '17ba8e87-53f0-46f4-abe5-014d9cd99597')
NEWS_KEY = os.environ.get('NEWS_KEY', '17d36cdd13c44e139853b3a6876cf940')
S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-bloomberg-terminal')
EMAIL_TO = os.environ.get('EMAIL_TO', 'raafouis@gmail.com')
EMAIL_FROM = os.environ.get('EMAIL_FROM', 'raafouis@gmail.com')

s3 = boto3.client('s3')
ses = boto3.client('ses', region_name='us-east-1')
ctx = ssl.create_default_context()

# === FRED SERIES (80+) ===
FRED_SERIES = {
    # Yield Curve (11 points)
    'DGS1MO': 'Treasury 1M', 'DGS3MO': 'Treasury 3M', 'DGS6MO': 'Treasury 6M',
    'DGS1': 'Treasury 1Y', 'DGS2': 'Treasury 2Y', 'DGS3': 'Treasury 3Y',
    'DGS5': 'Treasury 5Y', 'DGS7': 'Treasury 7Y', 'DGS10': 'Treasury 10Y',
    'DGS20': 'Treasury 20Y', 'DGS30': 'Treasury 30Y',
    # Fed & Policy
    'DFF': 'Fed Funds Rate', 'WALCL': 'Fed Balance Sheet',
    'RRPONTSYD': 'Reverse Repo', 'WTREGEN': 'Treasury General Account',
    'TOTRESNS': 'Total Reserves',
    # Spreads
    'T10Y2Y': '10Y-2Y Spread', 'T10Y3M': '10Y-3M Spread',
    'T10YFF': '10Y-FF Spread', 'BAMLH0A0HYM2': 'HY Spread',
    'BAMLC0A4CBBB': 'BBB Spread', 'BAMLC0A1CAAA': 'AAA Spread',
    'BAMLH0A0HYM2EY': 'HY OAS', 'BAMLEMCBPIOAS': 'EM Corp Spread',
    # Inflation
    'CPIAUCSL': 'CPI', 'CPILFESL': 'Core CPI', 'PCEPI': 'PCE',
    'PCEPILFE': 'Core PCE', 'T5YIE': '5Y Breakeven',
    'T10YIE': '10Y Breakeven', 'DFII10': '10Y Real Rate',
    'DFII5': '5Y Real Rate', 'PPIFIS': 'PPI Final Demand',
    # Employment
    'UNRATE': 'Unemployment', 'PAYEMS': 'Nonfarm Payrolls',
    'ICSA': 'Initial Claims', 'CCSA': 'Continued Claims',
    'AWHAETP': 'Avg Weekly Hours', 'CES0500000003': 'Avg Hourly Earnings',
    'U6RATE': 'U6 Underemployment',
    # GDP & Output
    'GDP': 'GDP', 'GDPC1': 'Real GDP', 'INDPRO': 'Industrial Production',
    'TCU': 'Capacity Utilization', 'DGORDER': 'Durable Goods',
    # Housing
    'HOUST': 'Housing Starts', 'PERMIT': 'Building Permits',
    'CSUSHPISA': 'Case-Shiller', 'MSACSR': 'Months Supply',
    'MORTGAGE30US': '30Y Mortgage', 'MORTGAGE15US': '15Y Mortgage',
    # Consumer
    'UMCSENT': 'Michigan Sentiment', 'RSAFS': 'Retail Sales',
    'PI': 'Personal Income', 'PSAVERT': 'Savings Rate',
    'TOTALSA': 'Auto Sales', 'PCE': 'Personal Consumption',
    # Credit & Lending
    'TOTCI': 'Commercial Loans', 'TOTALSL': 'Consumer Credit',
    'BUSLOANS': 'Business Loans', 'DRCCLACBS': 'CC Delinquency',
    'STLFSI2': 'Financial Stress',
    # Money & Dollar
    'M2SL': 'M2 Money Supply', 'M1SL': 'M1 Money Supply',
    'MZMSL': 'MZM Money', 'M2V': 'Money Velocity',
    'DTWEXBGS': 'Trade-Weighted Dollar', 'DEXUSEU': 'EUR/USD',
    'DEXJPUS': 'USD/JPY', 'DEXUSUK': 'GBP/USD',
    'DEXCHUS': 'USD/CNY', 'DEXCAUS': 'USD/CAD',
    'DEXMXUS': 'USD/MXN', 'DEXSFUS': 'USD/CHF',
    # Commodities
    'DCOILWTICO': 'WTI Crude', 'DCOILBRENTEU': 'Brent Crude',
    'DHHNGSP': 'Natural Gas', 'GOLDAMGBD228NLBM': 'Gold',
    'WPRIME': 'Prime Rate',
    # VIX & Vol
    'VIXCLS': 'VIX',
    # Debt
    'GFDEBTN': 'National Debt', 'GFDEGDQ188S': 'Debt-to-GDP',
}

# === STOCKS & ETFs ===
TICKERS = [
    'SPY', 'QQQ', 'DIA', 'IWM', 'VTI',      # Major indices
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', # Mega tech
    'META', 'TSLA', 'AMD', 'AVGO', 'CRM',     # Tech leaders
    'XLF', 'XLE', 'XLV', 'XLI', 'XLK',        # Sectors
    'XLP', 'XLU', 'XLB', 'XLRE', 'XLC',       # Sectors
    'GLD', 'SLV', 'USO', 'UNG', 'TLT',        # Commodities/Bonds
    'HYG', 'LQD', 'JNK', 'AGG', 'TIP',        # Fixed Income
    'EEM', 'EFA', 'VWO', 'FXI',               # International
    'BTC-USD', 'ETH-USD',                       # Crypto placeholders
]


def fetch_url(url, timeout=8):
    """Fetch URL with SSL context"""
    try:
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/8.0'})
        with urllib.request.urlopen(req, timeout=timeout, context=ctx) as r:
            return json.loads(r.read())
    except Exception as e:
        return {'error': str(e)}


def fetch_fred_batch(series_ids):
    """Fetch multiple FRED series"""
    results = {}
    def fetch_one(sid):
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&limit=260&sort_order=desc"
        data = fetch_url(url, timeout=10)
        if 'observations' in data:
            obs = [o for o in data['observations'] if o['value'] != '.']
            if obs:
                current = float(obs[0]['value'])
                prev_values = {}
                for o in obs:
                    d = o['date']
                    v = float(o['value'])
                    if '1w' not in prev_values and len([x for x in obs if x['date'] <= d]) >= 5:
                        prev_values['1w'] = v
                    if '1m' not in prev_values and len([x for x in obs if x['date'] <= d]) >= 22:
                        prev_values['1m'] = v
                    if '3m' not in prev_values and len([x for x in obs if x['date'] <= d]) >= 65:
                        prev_values['3m'] = v
                    if '1y' not in prev_values and len([x for x in obs if x['date'] <= d]) >= 252:
                        prev_values['1y'] = v

                # Simpler change calc based on index positions
                chg = {}
                if len(obs) > 5:
                    chg['1w'] = ((current - float(obs[5]['value'])) / abs(float(obs[5]['value']))) * 100 if float(obs[5]['value']) != 0 else 0
                if len(obs) > 22:
                    chg['1m'] = ((current - float(obs[22]['value'])) / abs(float(obs[22]['value']))) * 100 if float(obs[22]['value']) != 0 else 0
                if len(obs) > 65:
                    chg['3m'] = ((current - float(obs[65]['value'])) / abs(float(obs[65]['value']))) * 100 if float(obs[65]['value']) != 0 else 0
                if len(obs) > 252:
                    chg['1y'] = ((current - float(obs[min(252, len(obs)-1)]['value'])) / abs(float(obs[min(252, len(obs)-1)]['value']))) * 100 if float(obs[min(252, len(obs)-1)]['value']) != 0 else 0

                results[sid] = {
                    'value': current,
                    'date': obs[0]['date'],
                    'name': FRED_SERIES.get(sid, sid),
                    'chg_1w': round(chg.get('1w', 0), 2),
                    'chg_1m': round(chg.get('1m', 0), 2),
                    'chg_3m': round(chg.get('3m', 0), 2),
                    'chg_1y': round(chg.get('1y', 0), 2),
                    'history': [{'date': o['date'], 'value': float(o['value'])} for o in obs[:60]]
                }
        return sid

    with ThreadPoolExecutor(max_workers=20) as ex:
        futures = {ex.submit(fetch_one, sid): sid for sid in series_ids}
        for f in as_completed(futures, timeout=45):
            try:
                f.result()
            except:
                pass
    return results


def fetch_polygon_stocks(tickers):
    """Fetch stock data from Polygon"""
    results = {}
    def fetch_one(t):
        # Skip crypto placeholders
        if '-' in t:
            return t
        url = f"https://api.polygon.io/v2/aggs/ticker/{t}/range/1/day/2024-01-01/{datetime.now().strftime('%Y-%m-%d')}?adjusted=true&sort=desc&limit=260&apiKey={POLYGON_KEY}"
        data = fetch_url(url, timeout=10)
        if 'results' in data and data['results']:
            bars = data['results']
            current = bars[0]['c']
            sma50 = sum(b['c'] for b in bars[:50]) / min(50, len(bars)) if len(bars) >= 50 else current
            sma200 = sum(b['c'] for b in bars[:200]) / min(200, len(bars)) if len(bars) >= 200 else current

            # RSI
            gains, losses = [], []
            for i in range(min(14, len(bars)-1)):
                diff = bars[i]['c'] - bars[i+1]['c']
                if diff > 0: gains.append(diff)
                else: losses.append(abs(diff))
            avg_gain = sum(gains)/14 if gains else 0
            avg_loss = sum(losses)/14 if losses else 0.01
            rsi = 100 - (100 / (1 + avg_gain/avg_loss)) if avg_loss > 0 else 50

            # MACD
            ema12 = current
            ema26 = current
            for i in range(min(26, len(bars))):
                if i < 12: ema12 = ema12 * 0.846 + bars[i]['c'] * 0.154
                ema26 = ema26 * 0.925 + bars[i]['c'] * 0.075
            macd = ema12 - ema26

            chg_1d = ((current - bars[1]['c'])/bars[1]['c']*100) if len(bars) > 1 else 0
            chg_1w = ((current - bars[5]['c'])/bars[5]['c']*100) if len(bars) > 5 else 0
            chg_1m = ((current - bars[22]['c'])/bars[22]['c']*100) if len(bars) > 22 else 0
            chg_3m = ((current - bars[65]['c'])/bars[65]['c']*100) if len(bars) > 65 else 0
            chg_1y = ((current - bars[min(252,len(bars)-1)]['c'])/bars[min(252,len(bars)-1)]['c']*100) if len(bars) > 252 else 0
            chg_ytd = ((current - bars[-1]['c'])/bars[-1]['c']*100) if bars else 0

            results[t] = {
                'close': round(current, 2),
                'volume': bars[0]['v'],
                'change_pct': round(chg_1d, 2),
                'chg_1w': round(chg_1w, 2),
                'chg_1m': round(chg_1m, 2),
                'chg_3m': round(chg_3m, 2),
                'chg_1y': round(chg_1y, 2),
                'chg_ytd': round(chg_ytd, 2),
                'sma50': round(sma50, 2),
                'sma200': round(sma200, 2),
                'rsi': round(rsi, 1),
                'macd': round(macd, 2),
                'above_sma50': current > sma50,
                'above_sma200': current > sma200,
                'high_52w': max(b['h'] for b in bars[:252]) if bars else current,
                'low_52w': min(b['l'] for b in bars[:252]) if bars else current,
            }
        return t

    with ThreadPoolExecutor(max_workers=15) as ex:
        futures = {ex.submit(fetch_one, t): t for t in tickers}
        for f in as_completed(futures, timeout=40):
            try:
                f.result()
            except:
                pass
    return results


def fetch_crypto():
    """Fetch crypto from CoinMarketCap"""
    try:
        req = urllib.request.Request(
            'https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=20&convert=USD',
            headers={'X-CMC_PRO_API_KEY': CMC_KEY, 'User-Agent': 'JustHodl/8.0'}
        )
        with urllib.request.urlopen(req, timeout=10, context=ctx) as r:
            data = json.loads(r.read())
        results = {}
        for coin in data.get('data', []):
            q = coin['quote']['USD']
            results[coin['symbol']] = {
                'name': coin['name'],
                'price': round(q['price'], 2),
                'chg_24h': round(q.get('percent_change_24h', 0), 2),
                'chg_7d': round(q.get('percent_change_7d', 0), 2),
                'chg_30d': round(q.get('percent_change_30d', 0), 2),
                'market_cap': round(q.get('market_cap', 0)),
                'volume_24h': round(q.get('volume_24h', 0)),
                'dominance': round(q.get('market_cap_dominance', 0), 2),
            }
        return results
    except Exception as e:
        return {'error': str(e)}


def fetch_news():
    """Fetch market news"""
    try:
        url = f"https://newsapi.org/v2/top-headlines?category=business&country=us&pageSize=10&apiKey={NEWS_KEY}"
        data = fetch_url(url, timeout=8)
        articles = []
        for a in data.get('articles', [])[:10]:
            articles.append({
                'title': a.get('title', ''),
                'source': a.get('source', {}).get('name', ''),
                'url': a.get('url', ''),
                'time': a.get('publishedAt', ''),
            })
        return articles
    except:
        return []


def calculate_khalid_index(fred, stocks):
    """Calculate proprietary Khalid Index (0-100)"""
    score = 50
    components = {}

    # 1. VIX Component (0-15 pts)
    vix = fred.get('VIXCLS', {}).get('value', 20)
    if vix < 15: vix_score = 15
    elif vix < 20: vix_score = 10
    elif vix < 25: vix_score = 5
    elif vix < 30: vix_score = -5
    else: vix_score = -15
    components['vix'] = {'value': vix, 'score': vix_score, 'label': 'Volatility'}

    # 2. Yield Curve (0-15 pts)
    yc = fred.get('T10Y2Y', {}).get('value', 0)
    if yc > 1.0: yc_score = 15
    elif yc > 0.5: yc_score = 10
    elif yc > 0: yc_score = 5
    elif yc > -0.5: yc_score = -5
    else: yc_score = -15
    components['yield_curve'] = {'value': yc, 'score': yc_score, 'label': 'Yield Curve'}

    # 3. Credit Spreads (0-15 pts)
    hy = fred.get('BAMLH0A0HYM2', {}).get('value', 400)
    if hy < 300: cs_score = 15
    elif hy < 400: cs_score = 10
    elif hy < 500: cs_score = 5
    elif hy < 700: cs_score = -5
    else: cs_score = -15
    components['credit'] = {'value': hy, 'score': cs_score, 'label': 'HY Spread'}

    # 4. Financial Stress (0-10 pts)
    stress = fred.get('STLFSI2', {}).get('value', 0)
    if stress < -1: fs_score = 10
    elif stress < 0: fs_score = 5
    elif stress < 1: fs_score = -5
    else: fs_score = -10
    components['stress'] = {'value': stress, 'score': fs_score, 'label': 'Fin Stress'}

    # 5. Market Breadth (0-10 pts)
    above_200 = sum(1 for s in stocks.values() if isinstance(s, dict) and s.get('above_sma200', False))
    total = max(len([s for s in stocks.values() if isinstance(s, dict) and 'above_sma200' in s]), 1)
    breadth = above_200 / total * 100
    if breadth > 70: mb_score = 10
    elif breadth > 50: mb_score = 5
    elif breadth > 30: mb_score = -5
    else: mb_score = -10
    components['breadth'] = {'value': round(breadth, 1), 'score': mb_score, 'label': 'Mkt Breadth'}

    # 6. Fed Balance Sheet trend
    fed_bs = fred.get('WALCL', {}).get('chg_1m', 0)
    if fed_bs > 1: fed_score = 10
    elif fed_bs > 0: fed_score = 5
    elif fed_bs > -1: fed_score = -5
    else: fed_score = -10
    components['fed'] = {'value': fed_bs, 'score': fed_score, 'label': 'Fed B/S Trend'}

    # 7. Unemployment
    unemp = fred.get('UNRATE', {}).get('value', 4)
    if unemp < 4: ue_score = 10
    elif unemp < 5: ue_score = 5
    elif unemp < 6: ue_score = -5
    else: ue_score = -10
    components['unemployment'] = {'value': unemp, 'score': ue_score, 'label': 'Unemployment'}

    # 8. Initial Claims
    claims = fred.get('ICSA', {}).get('value', 250000)
    if claims < 220000: ic_score = 10
    elif claims < 260000: ic_score = 5
    elif claims < 300000: ic_score = -5
    else: ic_score = -10
    components['claims'] = {'value': claims, 'score': ic_score, 'label': 'Init Claims'}

    # 9. Dollar Strength
    dxy = fred.get('DTWEXBGS', {}).get('chg_1m', 0)
    if dxy < -2: dx_score = 5  # Weakening dollar = positive for risk
    elif dxy < 0: dx_score = 3
    elif dxy < 2: dx_score = -3
    else: dx_score = -5
    components['dollar'] = {'value': dxy, 'score': dx_score, 'label': 'USD Trend'}

    total_score = 50 + sum(c['score'] for c in components.values())
    total_score = max(0, min(100, total_score))

    if total_score >= 80: regime = 'STRONG BULL'
    elif total_score >= 65: regime = 'BULL'
    elif total_score >= 50: regime = 'NEUTRAL'
    elif total_score >= 35: regime = 'CAUTIOUS'
    elif total_score >= 20: regime = 'BEAR'
    else: regime = 'CRISIS'

    return {
        'score': round(total_score, 1),
        'regime': regime,
        'components': components,
    }


def generate_signals(fred, stocks):
    """Generate buy/sell/warning signals"""
    signals = {'buys': [], 'sells': [], 'warnings': [], 'reversals': []}

    for ticker, data in stocks.items():
        if not isinstance(data, dict) or 'rsi' not in data:
            continue

        # Oversold bounces
        if data['rsi'] < 30 and data.get('above_sma200', False):
            signals['buys'].append({
                'ticker': ticker, 'signal': 'OVERSOLD BOUNCE',
                'reason': f"RSI {data['rsi']} < 30 while above SMA200",
                'strength': 'STRONG'
            })
        elif data['rsi'] < 35 and data.get('chg_1w', 0) > 0:
            signals['buys'].append({
                'ticker': ticker, 'signal': 'RECOVERY',
                'reason': f"RSI {data['rsi']} recovering, +{data['chg_1w']:.1f}% this week",
                'strength': 'MODERATE'
            })

        # Overbought warnings
        if data['rsi'] > 75:
            signals['sells'].append({
                'ticker': ticker, 'signal': 'OVERBOUGHT',
                'reason': f"RSI {data['rsi']} > 75",
                'strength': 'STRONG' if data['rsi'] > 80 else 'MODERATE'
            })

        # Death/Golden cross
        if data.get('sma50') and data.get('sma200'):
            if data['sma50'] < data['sma200'] and data.get('chg_1m', 0) > 3:
                signals['reversals'].append({
                    'ticker': ticker, 'signal': 'POTENTIAL GOLDEN CROSS',
                    'reason': f"SMA50 approaching SMA200 from below, +{data['chg_1m']:.1f}% monthly",
                })
            elif data['sma50'] > data['sma200'] and data.get('chg_1m', 0) < -3:
                signals['reversals'].append({
                    'ticker': ticker, 'signal': 'POTENTIAL DEATH CROSS',
                    'reason': f"SMA50 declining toward SMA200, {data['chg_1m']:.1f}% monthly",
                })

    # Macro warnings
    vix = fred.get('VIXCLS', {}).get('value', 20)
    if vix > 25:
        signals['warnings'].append({
            'signal': 'ELEVATED VIX',
            'reason': f"VIX at {vix:.1f} — elevated fear",
            'severity': 'HIGH' if vix > 30 else 'MODERATE'
        })

    yc = fred.get('T10Y2Y', {}).get('value', 0)
    if yc < 0:
        signals['warnings'].append({
            'signal': 'YIELD CURVE INVERTED',
            'reason': f"10Y-2Y spread at {yc:.2f}% — recession signal",
            'severity': 'HIGH'
        })

    hy = fred.get('BAMLH0A0HYM2', {}).get('value', 400)
    if hy > 500:
        signals['warnings'].append({
            'signal': 'CREDIT STRESS',
            'reason': f"HY spread at {hy:.0f}bps — credit deterioration",
            'severity': 'HIGH' if hy > 700 else 'MODERATE'
        })

    return signals


def sector_analysis(stocks):
    """Sector rotation analysis"""
    sectors = {
        'XLK': 'Technology', 'XLF': 'Financials', 'XLE': 'Energy',
        'XLV': 'Healthcare', 'XLI': 'Industrials', 'XLP': 'Staples',
        'XLU': 'Utilities', 'XLB': 'Materials', 'XLRE': 'Real Estate',
        'XLC': 'Comm Services'
    }
    results = []
    for etf, name in sectors.items():
        d = stocks.get(etf, {})
        if not isinstance(d, dict) or 'close' not in d:
            continue
        # Momentum score
        mom = (d.get('chg_1w', 0) * 0.3 + d.get('chg_1m', 0) * 0.4 + d.get('chg_3m', 0) * 0.3)
        # Phase detection
        if d.get('chg_1m', 0) > 2 and d.get('chg_3m', 0) > 5:
            phase = 'LEADING'
        elif d.get('chg_1m', 0) > 0 and d.get('chg_3m', 0) > 0:
            phase = 'IMPROVING'
        elif d.get('chg_1m', 0) < 0 and d.get('chg_3m', 0) > 0:
            phase = 'WEAKENING'
        else:
            phase = 'LAGGING'

        results.append({
            'etf': etf, 'name': name, 'price': d.get('close', 0),
            'chg_1w': d.get('chg_1w', 0), 'chg_1m': d.get('chg_1m', 0),
            'chg_3m': d.get('chg_3m', 0), 'chg_1y': d.get('chg_1y', 0),
            'rsi': d.get('rsi', 50), 'momentum': round(mom, 2), 'phase': phase,
        })
    results.sort(key=lambda x: x['momentum'], reverse=True)
    return results


def calculate_net_liquidity(fred):
    """Fed Net Liquidity = Fed B/S - TGA - RRP"""
    bs = fred.get('WALCL', {}).get('value', 0)
    tga = fred.get('WTREGEN', {}).get('value', 0)
    rrp = fred.get('RRPONTSYD', {}).get('value', 0)
    if bs and tga and rrp:
        # All in millions for WALCL
        net = bs - tga - rrp
        return {
            'net_liquidity': round(net, 0),
            'fed_bs': round(bs, 0),
            'tga': round(tga, 0),
            'rrp': round(rrp, 0),
            'net_liquidity_chg_1m': round(fred.get('WALCL', {}).get('chg_1m', 0) - fred.get('RRPONTSYD', {}).get('chg_1m', 0), 2),
        }
    return {}


def lambda_handler(event, context):
    """Main handler — aggregates all data"""
    start = time.time()
    now = datetime.now(timezone.utc)
    et = now - timedelta(hours=5)

    print(f"🚀 JustHodl Bloomberg Terminal V8 — {et.strftime('%Y-%m-%d %H:%M ET')}")

    # Parallel data fetching
    with ThreadPoolExecutor(max_workers=4) as ex:
        fred_future = ex.submit(fetch_fred_batch, list(FRED_SERIES.keys()))
        stock_future = ex.submit(fetch_polygon_stocks, TICKERS)
        crypto_future = ex.submit(fetch_crypto)
        news_future = ex.submit(fetch_news)

        fred = fred_future.result()
        stocks = stock_future.result()
        crypto = crypto_future.result()
        news = news_future.result()

    # Analytics
    khalid = calculate_khalid_index(fred, stocks)
    signals = generate_signals(fred, stocks)
    sectors = sector_analysis(stocks)
    liquidity = calculate_net_liquidity(fred)

    # Build yield curve
    yc_points = ['DGS1MO', 'DGS3MO', 'DGS6MO', 'DGS1', 'DGS2', 'DGS3', 'DGS5', 'DGS7', 'DGS10', 'DGS20', 'DGS30']
    yield_curve = []
    for sid in yc_points:
        if sid in fred:
            yield_curve.append({'maturity': FRED_SERIES[sid], 'rate': fred[sid]['value']})

    elapsed = round(time.time() - start, 1)

    # Assemble payload
    payload = {
        'timestamp': et.strftime('%Y-%m-%d %H:%M:%S ET'),
        'utc': now.isoformat(),
        'elapsed_sec': elapsed,
        'fred': fred,
        'stocks': stocks,
        'crypto': crypto,
        'news': news,
        'khalid_index': khalid,
        'signals': signals,
        'sectors': sectors,
        'liquidity': liquidity,
        'yield_curve': yield_curve,
        'stats': {
            'fred_count': len(fred),
            'stock_count': len(stocks),
            'crypto_count': len(crypto) if isinstance(crypto, dict) and 'error' not in crypto else 0,
            'news_count': len(news) if isinstance(news, list) else 0,
            'signal_count': sum(len(v) for v in signals.values()),
        }
    }

    # Save to S3
    try:
        s3.put_object(
            Bucket=S3_BUCKET,
            Key='data/report.json',
            Body=json.dumps(payload, default=str),
            ContentType='application/json',
            CacheControl='max-age=60',
        )
        # Also save timestamped copy
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f"archive/{et.strftime('%Y/%m/%d/%H%M')}.json",
            Body=json.dumps(payload, default=str),
            ContentType='application/json',
        )
        print(f"✅ Data saved to S3: {len(fred)} FRED + {len(stocks)} stocks in {elapsed}s")
    except Exception as e:
        print(f"❌ S3 save error: {e}")

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        'body': json.dumps(payload, default=str)
    }
