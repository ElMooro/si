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
from managed_secret import managed_secret

try:
    import _fred_shim  # noqa: F401
except Exception:
    pass
# Phase 2 KA rebrand — recursive khalid_* → ka_* alias helper.
try:
    from ka_aliases import add_ka_aliases
except Exception as _e:
    print(f"WARN: ka_aliases unavailable: {_e}")
    def add_ka_aliases(obj, **_kwargs):
        return obj

import json
import os
import boto3
import urllib.request
import urllib.error
import urllib.parse
from zoneinfo import ZoneInfo
from bloomberg_math import number, percent, fred_statistics, stock_statistics, liquidity
import ssl
import time
from datetime import datetime, timezone, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from managed_secret import managed_secret  # audit 2026-09-08 INST-06: no literal credentials

# === CONFIG ===
FRED_KEY = managed_secret(('FRED_API_KEY', 'FRED_KEY'), ("/justhodl/fred/api-key",))
POLYGON_KEY = managed_secret(('POLYGON_API_KEY', 'POLYGON_KEY', 'POLY_KEY'), ("/justhodl/polygon/api-key",))
ALPHAVANTAGE_KEY = managed_secret(("AV_KEY", "ALPHAVANTAGE_KEY", "ALPHA_VANTAGE_API_KEY", "ALPHAVANTAGE_API_KEY"), ("/justhodl/alphavantage/api-key",))
CMC_KEY = managed_secret(('CMC_KEY', 'CMC_API_KEY', 'COINMARKETCAP_API_KEY'), ("/justhodl/cmc/api-key",))
NEWS_KEY = managed_secret(('NEWS_KEY', 'NEWSAPI_KEY', 'NEWS_API_KEY'), ("/justhodl/newsapi/api-key",))
S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
EMAIL_TO = os.environ.get('EMAIL_TO', 'PLACEHOLDER@example.com')
EMAIL_FROM = os.environ.get('EMAIL_FROM', 'PLACEHOLDER@example.com')

s3 = boto3.client('s3')
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
    'STLFSI4': 'Financial Stress',
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


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args):
        return None


def fetch_url(url, timeout=8, headers=None):
    """Bounded provider reads; no redirects or credential-bearing errors."""
    try:
        parsed = urllib.parse.urlsplit(url)
        if (parsed.scheme != 'https' or parsed.hostname not in
                {'api.stlouisfed.org', 'api.polygon.io', 'newsapi.org', 'pro-api.coinmarketcap.com'}
                or parsed.username or parsed.password or parsed.port not in (None, 443)):
            raise ValueError('provider_host')
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/8.1', **(headers or {})})
        with urllib.request.build_opener(NoRedirect).open(req, timeout=min(timeout, 10)) as response:
            raw = response.read(5_000_001)
        if len(raw) > 5_000_000:
            raise ValueError('response_size')
        def reject(value):
            raise ValueError('non_finite')
        data = json.loads(raw, parse_constant=reject)
        json.dumps(data, allow_nan=False)
        if not isinstance(data, dict):
            raise ValueError('provider_schema')
        return data
    except Exception:
        return {'error': 'PROVIDER_DATA_UNAVAILABLE'}


def fetch_fred_batch(series_ids):
    def fetch_one(sid):
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&limit=260&sort_order=desc"
        data = fetch_url(url, timeout=10)
        observations = data.get('observations')
        result = fred_statistics(observations if isinstance(observations, list) else [], FRED_SERIES.get(sid, sid))
        if not observations:
            result['error'] = 'PROVIDER_DATA_UNAVAILABLE'
        return sid, result
    with ThreadPoolExecutor(max_workers=8) as executor:
        return dict(executor.map(fetch_one, series_ids))


def fetch_polygon_stocks(tickers):
    today = datetime.now(ZoneInfo('America/New_York')).date()
    start = today - timedelta(days=550)
    def fetch_one(ticker):
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{today}?adjusted=true&sort=desc&limit=500"
        data = fetch_url(url + '&apiKey=' + POLYGON_KEY, timeout=10)
        bars = data.get('results')
        result = stock_statistics(bars if isinstance(bars, list) else [], today=today)
        if not bars:
            result['error'] = 'PROVIDER_DATA_UNAVAILABLE'
        return ticker, result
    with ThreadPoolExecutor(max_workers=8) as executor:
        return dict(executor.map(fetch_one, [ticker for ticker in tickers if '-' not in ticker]))


def fetch_crypto():
    """Fetch crypto from CoinMarketCap"""
    try:
        data = fetch_url('https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit=20&convert=USD',
                         timeout=10, headers={'X-CMC_PRO_API_KEY': CMC_KEY})
        if not isinstance(data.get('data'), list):
            return {'error': 'PROVIDER_DATA_UNAVAILABLE'}
        def rounded(value, digits=2):
            value = number(value)
            return round(value, digits) if value is not None else None
        results = {}
        for coin in data.get('data', []):
            q = coin['quote']['USD']
            results[coin['symbol']] = {
                'name': coin['name'],
                'price': rounded(q.get('price')),
                'chg_24h': rounded(q.get('percent_change_24h')),
                'chg_7d': rounded(q.get('percent_change_7d')),
                'chg_30d': rounded(q.get('percent_change_30d')),
                'market_cap': rounded(q.get('market_cap'), 0),
                'volume_24h': rounded(q.get('volume_24h'), 0),
                'dominance': rounded(q.get('market_cap_dominance')),
            }
        return results
    except Exception as e:
        return {'error': 'PROVIDER_DATA_UNAVAILABLE'}


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


def calculate_khalid_index(fred, stocks, leading=None):
    """Calculate proprietary Khalid Index (0-100)"""
    required = {'VIXCLS': ('value', 7), 'T10Y2Y': ('value', 7), 'BAMLH0A0HYM2': ('value', 7),
                'STLFSI4': ('value', 21), 'WALCL': ('chg_1m', 21), 'UNRATE': ('value', 62),
                'ICSA': ('value', 21), 'DTWEXBGS': ('chg_1m', 10)}
    today = datetime.now(timezone.utc).date()
    missing = []
    for key, (field, max_age) in required.items():
        row = fred.get(key, {})
        try:
            age = (today - datetime.fromisoformat(row.get('date', '')).date()).days
            valid = 0 <= age <= max_age and number(row.get(field)) is not None and not row.get('invalid_or_duplicate_rows')
        except (TypeError, ValueError):
            valid = False
        if not valid:
            missing.append(key + '.' + field)
    for ticker in (name for name in TICKERS if '-' not in name):
        row = stocks.get(ticker, {})
        try:
            age = (today - datetime.fromisoformat(row.get('date', '')).date()).days
            valid = 0 <= age <= 7 and type(row.get('above_sma200')) is bool
        except (TypeError, ValueError):
            valid = False
        if not valid:
            missing.append(ticker + '.completed_sma200')
    if missing:
        return {'score': None, 'regime': 'UNAVAILABLE', 'components': {}, 'missing_inputs': missing,
                'analysis_basis': 'UNCALIBRATED_DESCRIPTIVE_HEURISTIC', 'execution_eligible': False}
    # Optional cross-engine context must carry an explicit recent UTC clock.
    if leading:
        try:
            stamp = datetime.fromisoformat(leading.get('generated_at', '').replace('Z', '+00:00'))
            valid = (stamp.tzinfo is not None and 0 <= (datetime.now(timezone.utc)-stamp).total_seconds() <= 48*3600
                     and number(leading.get('expansion_breadth_pct')) is not None
                     and 0 <= float(leading['expansion_breadth_pct']) <= 100)
        except (TypeError, ValueError):
            valid = False
        if not valid:
            leading = None
    score = 50
    components = {}

    # 1. VIX Component (0-15 pts)
    vix = number(fred.get('VIXCLS', {}).get('value'))
    if vix < 15: vix_score = 15
    elif vix < 20: vix_score = 10
    elif vix < 25: vix_score = 5
    elif vix < 30: vix_score = -5
    else: vix_score = -15
    components['vix'] = {'value': vix, 'score': vix_score, 'label': 'Volatility'}

    # 2. Yield Curve (0-15 pts)
    yc = number(fred.get('T10Y2Y', {}).get('value'))
    if yc > 1.0: yc_score = 15
    elif yc > 0.5: yc_score = 10
    elif yc > 0: yc_score = 5
    elif yc > -0.5: yc_score = -5
    else: yc_score = -15
    components['yield_curve'] = {'value': yc, 'score': yc_score, 'label': 'Yield Curve'}

    # 3. Credit Spreads (0-15 pts)
    hy_value = number(fred.get('BAMLH0A0HYM2', {}).get('value'))
    hy = hy_value * 100 if hy_value is not None else None
    if hy < 300: cs_score = 15
    elif hy < 400: cs_score = 10
    elif hy < 500: cs_score = 5
    elif hy < 700: cs_score = -5
    else: cs_score = -15
    components['credit'] = {'value': hy, 'score': cs_score, 'label': 'HY Spread'}

    # 4. Financial Stress (0-10 pts)
    stress = fred.get('STLFSI4', {}).get('value', 0)
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

    # 10. Global Leading Markets (canary markets that lead macro tops/bottoms)
    if leading:
        lead_breadth = leading.get('expansion_breadth_pct')
        lead_signal = leading.get('turning_point_signal')
        if lead_breadth is not None:
            if lead_signal in ('TOP_WARNING', 'BROAD_CONTRACTION'):
                lm_score = -10
            elif lead_signal == 'BOTTOM_SIGNAL':
                lm_score = 5
            elif lead_breadth > 65:
                lm_score = 10
            elif lead_breadth > 50:
                lm_score = 5
            elif lead_breadth > 35:
                lm_score = -5
            else:
                lm_score = -10
            components['leading_markets'] = {
                'value': lead_breadth, 'score': lm_score,
                'label': 'Leading Markets', 'signal': lead_signal}

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
        'analysis_basis': 'UNCALIBRATED_DESCRIPTIVE_HEURISTIC', 'execution_eligible': False,
    }


def generate_signals(fred, stocks):
    """Generate buy/sell/warning signals"""
    signals = {'buys': [], 'sells': [], 'warnings': [], 'reversals': []}

    for ticker, data in stocks.items():
        if not isinstance(data, dict) or number(data.get('rsi')) is None:
            continue

        # Oversold bounces
        if data['rsi'] < 30 and data.get('above_sma200', False):
            signals['buys'].append({
                'ticker': ticker, 'signal': 'OVERSOLD BOUNCE',
                'reason': f"RSI {data['rsi']} < 30 while above SMA200",
                'strength': 'STRONG'
            })
        elif data['rsi'] < 35 and number(data.get('chg_1w')) is not None and data['chg_1w'] > 0:
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
            if data['sma50'] < data['sma200'] and number(data.get('chg_1m')) is not None and data['chg_1m'] > 3:
                signals['reversals'].append({
                    'ticker': ticker, 'signal': 'POTENTIAL GOLDEN CROSS',
                    'reason': f"SMA50 approaching SMA200 from below, +{data['chg_1m']:.1f}% monthly",
                })
            elif data['sma50'] > data['sma200'] and number(data.get('chg_1m')) is not None and data['chg_1m'] < -3:
                signals['reversals'].append({
                    'ticker': ticker, 'signal': 'POTENTIAL DEATH CROSS',
                    'reason': f"SMA50 declining toward SMA200, {data['chg_1m']:.1f}% monthly",
                })

    # Macro warnings
    vix = number(fred.get('VIXCLS', {}).get('value'))
    if vix is not None and vix > 25:
        signals['warnings'].append({
            'signal': 'ELEVATED VIX',
            'reason': f"VIX at {vix:.1f} — elevated fear",
            'severity': 'HIGH' if vix > 30 else 'MODERATE'
        })

    yc = number(fred.get('T10Y2Y', {}).get('value'))
    if yc is not None and yc < 0:
        signals['warnings'].append({
            'signal': 'YIELD CURVE INVERTED',
            'reason': f"10Y-2Y spread at {yc:.2f}% — recession signal",
            'severity': 'HIGH'
        })

    hy_value = number(fred.get('BAMLH0A0HYM2', {}).get('value'))
    hy = hy_value * 100 if hy_value is not None else None
    if hy is not None and hy > 500:
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
        week, month, quarter = (number(d.get('chg_' + label)) for label in ('1w', '1m', '3m'))
        mom = None; phase = 'UNAVAILABLE'
        if all(value is not None for value in (week, month, quarter)):
            mom = week * 0.3 + month * 0.4 + quarter * 0.3
            phase = 'LEADING' if month > 2 and quarter > 5 else 'IMPROVING' if month > 0 and quarter > 0 else 'WEAKENING' if month < 0 and quarter > 0 else 'LAGGING'
        results.append({
            'etf': etf, 'name': name, 'price': d.get('close', 0),
            'chg_1w': d.get('chg_1w', 0), 'chg_1m': d.get('chg_1m', 0),
            'chg_3m': d.get('chg_3m', 0), 'chg_1y': d.get('chg_1y', 0),
            'rsi': d.get('rsi'), 'momentum': round(mom, 2) if mom is not None else None, 'phase': phase,
        })
    results.sort(key=lambda x: (x['momentum'] is not None, x['momentum'] or 0), reverse=True)
    return results


def calculate_net_liquidity(fred):
    return liquidity(fred)


def lambda_handler(event, context):
    """Main handler — aggregates all data"""
    start = time.time()
    now = datetime.now(timezone.utc)
    et = now.astimezone(ZoneInfo('America/New_York'))

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
    leading_mkts = None
    try:
        leading_mkts = json.loads(s3.get_object(
            Bucket='justhodl-dashboard-live',
            Key='data/leading-markets.json')['Body'].read())
    except Exception as e:
        print("leading-markets context unavailable")
    khalid = calculate_khalid_index(fred, stocks, leading_mkts)
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
        'engine': 'justhodl-bloomberg-v8', 'schema_version': 'bloomberg-report.v8.1',
        'execution_eligible': False, 'analysis_basis': 'UNCALIBRATED_DESCRIPTIVE_HEURISTIC',
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
            'fred_count': sum(number(row.get('value')) is not None for row in fred.values()),
            'fred_expected': len(FRED_SERIES),
            'stock_count': sum(number(row.get('close')) is not None for row in stocks.values()),
            'stock_expected': sum('-' not in ticker for ticker in TICKERS),
            'crypto_count': len(crypto) if isinstance(crypto, dict) and 'error' not in crypto else 0,
            'news_count': len(news) if isinstance(news, list) else 0,
            'signal_count': sum(len(v) for v in signals.values()),
        }
    }

    payload = add_ka_aliases(payload)
    payload['coverage_status'] = ('READY' if payload['stats']['fred_count'] == payload['stats']['fred_expected']
                                  and payload['stats']['stock_count'] == payload['stats']['stock_expected'] else 'PARTIAL')
    payload['archive_status'] = 'PUBLISHED'
    try:
        s3.put_object(Bucket=S3_BUCKET, Key=f"data/bloomberg-archive/{now.strftime('%Y/%m/%d/%H%M%S')}.json",
                      Body=json.dumps(payload, allow_nan=False), ContentType='application/json')
    except Exception:
        payload['archive_status'] = 'UNAVAILABLE'
    try:
        s3.put_object(Bucket=S3_BUCKET, Key='data/bloomberg-report.json',
                      Body=json.dumps(payload, allow_nan=False), ContentType='application/json', CacheControl='max-age=60')
    except Exception:
        raise RuntimeError('BLOOMBERG_PRIMARY_PUBLICATION_FAILED') from None

    return {
        'statusCode': 200,
        'headers': {
            'Content-Type': 'application/json',
            'Access-Control-Allow-Origin': '*',
            'Access-Control-Allow-Headers': 'Content-Type',
        },
        'body': json.dumps(payload, allow_nan=False)
    }
