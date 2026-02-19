"""
JUSTHODL BLOOMBERG TERMINAL V9 - MEGA MACRO INTELLIGENCE
=========================================================
200+ FRED Series | 40+ Stocks | 25+ Dashboard Tabs
Macro | Risk | Liquidity | DXY | ECB | Global Business Cycle
Systematic Risk | Manufacturing | PMI | ICE BofA | Credit | Global Liquidity
Auto-updates every 5 minutes via EventBridge
=========================================================
"""
import json
import urllib.request
import urllib.parse
import os
import time
import boto3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

# ============================================================
# CONFIGURATION
# ============================================================
FRED_API_KEY = os.environ.get('FRED_API_KEY', '2f057499936072679d8843d7fce99989')
POLYGON_API_KEY = os.environ.get('POLYGON_API_KEY', 'zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d')
S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
REGION = os.environ.get('AWS_REGION', 'us-east-1')

s3 = boto3.client('s3', region_name=REGION)

# ============================================================
# FRED SERIES DEFINITIONS - 200+ SERIES
# ============================================================

FRED_SERIES = {
    # ── MACRO ECONOMY (30+ series) ──
    'macro': {
        'GDP': 'Real GDP',
        'GDPC1': 'Real GDP (Chained)',
        'A191RL1Q225SBEA': 'Real GDP Growth Rate',
        'INDPRO': 'Industrial Production Index',
        'TCU': 'Capacity Utilization',
        'PAYEMS': 'Total Nonfarm Payrolls',
        'UNRATE': 'Unemployment Rate',
        'U6RATE': 'U-6 Unemployment Rate',
        'CIVPART': 'Labor Force Participation Rate',
        'ICSA': 'Initial Jobless Claims',
        'CCSA': 'Continued Jobless Claims',
        'UMCSENT': 'Consumer Sentiment (U of Michigan)',
        'RSAFS': 'Retail Sales',
        'HOUST': 'Housing Starts',
        'PERMIT': 'Building Permits',
        'HSN1F': 'New Home Sales',
        'EXHOSLUSM495S': 'Existing Home Sales',
        'PI': 'Personal Income',
        'PCE': 'Personal Consumption Expenditures',
        'DGORDER': 'Durable Goods Orders',
        'NEWORDER': 'Manufacturers New Orders',
        'AWHMAN': 'Avg Weekly Hours Manufacturing',
        'CES0500000003': 'Avg Hourly Earnings',
        'JTSJOL': 'Job Openings (JOLTS)',
        'JTSHIR': 'Hires (JOLTS)',
        'JTSQUR': 'Quits Rate (JOLTS)',
        'CPILFESL': 'Core CPI',
        'CPIAUCSL': 'CPI All Items',
        'PCEPI': 'PCE Price Index',
        'PCEPILFE': 'Core PCE Price Index',
        'PPIFIS': 'PPI Final Demand',
        'USSLIND': 'Leading Economic Index',
        'CFNAI': 'Chicago Fed National Activity Index',
    },

    # ── DXY / DOLLAR DATA (15+ series) ──
    'dxy': {
        'DTWEXBGS': 'Trade Weighted US Dollar (Broad)',
        'DTWEXEMEGS': 'Trade Weighted USD vs EM',
        'DTWEXAFEGS': 'Trade Weighted USD vs Advanced',
        'DEXUSEU': 'USD/EUR Exchange Rate',
        'DEXJPUS': 'JPY/USD Exchange Rate',
        'DEXUSUK': 'USD/GBP Exchange Rate',
        'DEXSZUS': 'CHF/USD Exchange Rate',
        'DEXCAUS': 'CAD/USD Exchange Rate',
        'DEXMXUS': 'MXN/USD Exchange Rate',
        'DEXCHUS': 'CNY/USD Exchange Rate',
        'DEXKOUS': 'KRW/USD Exchange Rate',
        'DEXBZUS': 'BRL/USD Exchange Rate',
        'DEXINUS': 'INR/USD Exchange Rate',
        'RTWEXBGS': 'Real Trade Weighted USD (Broad)',
        'TWEXBGSMTH': 'Trade Weighted USD Monthly',
    },

    # ── TREASURY & YIELD CURVE (20+ series) ──
    'treasury': {
        'DGS1MO': 'Treasury 1-Month',
        'DGS3MO': 'Treasury 3-Month',
        'DGS6MO': 'Treasury 6-Month',
        'DGS1': 'Treasury 1-Year',
        'DGS2': 'Treasury 2-Year',
        'DGS3': 'Treasury 3-Year',
        'DGS5': 'Treasury 5-Year',
        'DGS7': 'Treasury 7-Year',
        'DGS10': 'Treasury 10-Year',
        'DGS20': 'Treasury 20-Year',
        'DGS30': 'Treasury 30-Year',
        'T10Y2Y': '10Y-2Y Spread',
        'T10Y3M': '10Y-3M Spread',
        'T10YFF': '10Y-Fed Funds Spread',
        'T5YFF': '5Y-Fed Funds Spread',
        'T10YIE': '10Y Breakeven Inflation',
        'T5YIE': '5Y Breakeven Inflation',
        'T5YIFR': '5Y5Y Forward Inflation',
        'DFII10': '10Y TIPS Real Yield',
        'DFII5': '5Y TIPS Real Yield',
        'DFII30': '30Y TIPS Real Yield',
    },

    # ── ICE BofA CREDIT & BOND DATA (25+ series) ──
    'ice_bofa': {
        'BAMLH0A0HYM2': 'HY OAS Spread',
        'BAMLC0A0CM': 'US Corp Master OAS',
        'BAMLH0A1HYBB': 'BB OAS Spread',
        'BAMLH0A2HYBEY': 'B OAS Spread',
        'BAMLH0A3HYC': 'CCC & Lower OAS',
        'BAMLC0A1CAAA': 'AAA OAS Spread',
        'BAMLC0A2CAA': 'AA OAS Spread',
        'BAMLC0A3CA': 'A OAS Spread',
        'BAMLC0A4CBBB': 'BBB OAS Spread',
        'BAMLEMCBPIOAS': 'EM Corporate OAS',
        'BAMLEMHBHYCRPIOAS': 'EM HY Corporate OAS',
        'BAMLEMRECRPIOAS': 'EM Sovereign OAS',
        'BAMLHE00EHYIOAS': 'Euro HY OAS',
        'BAMLC0A0CMEY': 'US Corp Effective Yield',
        'BAMLH0A0HYM2EY': 'HY Effective Yield',
        'BAMLC0A1CAAAEY': 'AAA Effective Yield',
        'BAMLC0A4CBBBEY': 'BBB Effective Yield',
        'BAMLC1A0C13YEY': 'US Corp 1-3Y Yield',
        'BAMLC2A0C35YEY': 'US Corp 3-5Y Yield',
        'BAMLC3A0C57YEY': 'US Corp 5-7Y Yield',
        'BAMLC4A0C710YEY': 'US Corp 7-10Y Yield',
        'BAMLC7A0C1015YEY': 'US Corp 10-15Y Yield',
        'BAMLC8A0C15PYEY': 'US Corp 15Y+ Yield',
        'BAMLHYH0A0HYM2TRIV': 'HY Total Return Index',
        'BAMLCC0A0CMTRIV': 'US Corp Total Return Index',
    },

    # ── LIQUIDITY DATA (25+ series) ──
    'liquidity': {
        'WALCL': 'Fed Total Assets',
        'WTREGEN': 'Treasury General Account (TGA)',
        'RRPONTSYD': 'Overnight Reverse Repo',
        'RPONTSYD': 'Overnight Repo',
        'TOTRESNS': 'Total Reserves',
        'EXCSRESNS': 'Excess Reserves',
        'WLCFLPCL': 'Fed Loans to Banks',
        'H41RESPPALDKNWW': 'Fed Discount Window',
        'WSHOSHO': 'Fed Treasury Holdings',
        'WSHOMCB': 'Fed MBS Holdings',
        'M2SL': 'M2 Money Supply',
        'WM2NS': 'M2 Money Supply (Weekly)',
        'M1SL': 'M1 Money Supply',
        'BOGMBASE': 'Monetary Base',
        'MULT': 'Money Multiplier (M1)',
        'M2V': 'Velocity of M2',
        'FEDFUNDS': 'Federal Funds Rate',
        'DFF': 'Effective Fed Funds Rate',
        'SOFR': 'SOFR Rate',
        'OBFR': 'Overnight Bank Funding Rate',
        'EFFR': 'Effective Federal Funds Rate',
        'IORB': 'Interest on Reserve Balances',
        'WORAL': 'Fed Other Assets',
        'TERMT': 'Term Funding (BTFP)',
        'RESPPLLOPNWW': 'Primary Credit Loans',
    },

    # ── CREDIT & LENDING DATA (20+ series) ──
    'credit': {
        'TOTALSL': 'Total Consumer Credit',
        'REVOLSL': 'Revolving Consumer Credit',
        'NONREVSL': 'Non-Revolving Consumer Credit',
        'BUSLOANS': 'Commercial & Industrial Loans',
        'REALLN': 'Real Estate Loans',
        'CONSUMER': 'Consumer Loans at Banks',
        'DRCCLACBS': 'Credit Card Delinquency Rate',
        'DRSFRMACBS': 'Mortgage Delinquency Rate',
        'DRALACBS': 'Auto Loan Delinquency Rate',
        'DRCLACBS': 'C&I Loan Delinquency Rate',
        'CHARGE': 'Charge-Off Rate All Loans',
        'CORCBS': 'C&I Charge-Off Rate',
        'CRELACBS': 'RE Loan Charge-Off Rate',
        'CORCCACBS': 'Credit Card Charge-Off Rate',
        'DRTSCILM': 'Bank Lending Standards (C&I Large)',
        'DRTSCIS': 'Bank Lending Standards (C&I Small)',
        'DRTSSP': 'Lending Standards (Subprime)',
        'TOTCI': 'Total C&I Loans',
        'CCLACBM027NBOG': 'Credit Card Loans Outstanding',
        'MVLOAS': 'Auto Loans Outstanding',
        'SLOAS': 'Student Loans Outstanding',
    },

    # ── RISK & FINANCIAL CONDITIONS (20+ series) ──
    'risk': {
        'VIXCLS': 'VIX (CBOE Volatility Index)',
        'STLFSI2': 'St. Louis Financial Stress',
        'NFCI': 'Chicago Fed NFCI',
        'ANFCI': 'Adjusted NFCI',
        'KCFSI': 'Kansas City Financial Stress',
        'CLVMNFCI': 'Cleveland Financial Conditions',
        'TEDRATE': 'TED Spread',
        'DPRIME': 'Bank Prime Rate',
        'AAA': 'Moody\'s AAA Corporate Yield',
        'BAA': 'Moody\'s BAA Corporate Yield',
        'AAA10Y': 'Moody\'s AAA-10Y Treasury Spread',
        'BAA10Y': 'Moody\'s BAA-10Y Treasury Spread',
        'MORTGAGE30US': '30-Year Mortgage Rate',
        'MORTGAGE15US': '15-Year Mortgage Rate',
        'DCOILWTICO': 'WTI Crude Oil',
        'DCOILBRENTEU': 'Brent Crude Oil',
        'GOLDAMGBD228NLBM': 'Gold Price (London)',
        'DEXSFEVS': 'Silver Price',
        'PALLFNFINDEXQ': 'Global Price All Commodities',
        'CPIENGSL': 'Energy CPI',
    },

    # ── INFLATION MONITOR (15+ series) ──
    'inflation': {
        'CPIAUCSL': 'CPI All Items',
        'CPILFESL': 'Core CPI (Ex Food & Energy)',
        'CPIUFDSL': 'CPI Food',
        'CPIENGSL': 'CPI Energy',
        'CUSR0000SAH1': 'CPI Shelter',
        'CUSR0000SETB01': 'CPI Gasoline',
        'CUSR0000SAM2': 'CPI Medical Care Services',
        'PCEPI': 'PCE Price Index',
        'PCEPILFE': 'Core PCE',
        'PPIFIS': 'PPI Final Demand',
        'WPSFD4131': 'PPI Finished Goods',
        'CPALTT01USM657N': 'CPI YoY Change',
        'MICH': 'Michigan Inflation Expectations',
        'EXPINF1YR': '1Y Expected Inflation',
        'EXPINF10YR': '10Y Expected Inflation',
        'T5YIE': '5Y Breakeven Inflation',
        'T10YIE': '10Y Breakeven Inflation',
        'T5YIFR': '5Y5Y Forward Inflation Rate',
    },

    # ── GLOBAL BUSINESS CYCLE & MANUFACTURING (20+ series) ──
    'global_cycle': {
        'NAPM': 'ISM Manufacturing PMI',
        'NAPMNOI': 'ISM New Orders',
        'NAPMPI': 'ISM Production Index',
        'NAPMPRI': 'ISM Prices Paid',
        'NAPMSDI': 'ISM Supplier Deliveries',
        'NAPMII': 'ISM Inventories',
        'NAPMEI': 'ISM Employment',
        'NMFBAI': 'ISM Non-Manufacturing PMI',
        'MNFCTIRSA': 'Manufacturing Inventories',
        'AMTMTI': 'Manufacturing Tech Shipments',
        'IPMAN': 'Industrial Prod: Manufacturing',
        'IPMANSICS': 'IP: Manufacturing (SIC)',
        'MCUMFN': 'Manufacturing Capacity Util',
        'ACDGNO': 'Core Capital Goods Orders',
        'NEWORDER': 'Manufacturers New Orders',
        'UMCSENT': 'Consumer Sentiment',
        'CSCICP03USM665S': 'Consumer Confidence (OECD)',
        'BSCICP03USM665S': 'Business Confidence (OECD)',
        'USALOLITONOSTSAM': 'US Leading Indicator (OECD CLI)',
        'OABOREGM665S': 'OECD G7 Business Confidence',
    },

    # ── PURCHASING MANAGERS & WORLD PMI (15+ series) ──
    'pmi_world': {
        'NAPM': 'US ISM Manufacturing PMI',
        'NMFBAI': 'US ISM Services PMI',
        'MPMICTMN': 'PMI Composite (OECD)',
        'CSCICP03USM665S': 'US Consumer Confidence',
        'BSCICP03USM665S': 'US Business Confidence',
        'USALOLITONOSTSAM': 'US CLI (OECD)',
        'CHNLOLITONOSTSAM': 'China CLI (OECD)',
        'JPLOLITONOSTSAM': 'Japan CLI (OECD)',
        'DEULOLIT02IXOBSAM': 'Germany CLI (OECD)',
        'GBRLOLIT02IXOBSAM': 'UK CLI (OECD)',
        'BRALOLITONOSTSAM': 'Brazil CLI (OECD)',
        'INDLOLITONOSTSAM': 'India CLI (OECD)',
        'FRALOLITONOSTSAM': 'France CLI (OECD)',
        'ITALOLIT02IXOBSAM': 'Italy CLI (OECD)',
        'CANLOLITONOSTSAM': 'Canada CLI (OECD)',
        'MEXLOLITONOSTSAM': 'Mexico CLI (OECD)',
        'KORLOLITONOSTSAM': 'South Korea CLI (OECD)',
    },

    # ── ECB & EUROPEAN DATA (15+ series) ──
    'ecb': {
        'ECBASSETSW': 'ECB Total Assets',
        'ECBDFR': 'ECB Deposit Facility Rate',
        'ECBMLFR': 'ECB Main Refinancing Rate',
        'INTDSREZM193N': 'Euro Area Deposit Rate',
        'CLVMNACSCAB1GQEA19': 'Euro Area Real GDP',
        'EA19CPALTT01GYM': 'Euro Area CPI YoY',
        'LRHUTTTTEZM156S': 'Euro Area Unemployment',
        'IRSTCB01EZM156N': 'Euro Short-Term Rate',
        'IR3TIB01EZM156N': 'Euro 3M Interbank Rate',
        'IRLTLT01EZM156N': 'Euro Long-Term Govt Bond',
        'MANMM101EZM189S': 'Euro Manufacturing PMI Proxy',
        'LORSGPNOSTSAM': 'OECD Euro Area CLI',
        'CP0000EZ19M086NEST': 'Euro HICP',
        'CPALTT01EZM659N': 'Euro Area Inflation YoY',
        'MABMM301EZM189S': 'Euro Area M3 Money Supply',
    },

    # ── GLOBAL LIQUIDITY (15+ series) ──
    'global_liquidity': {
        'WALCL': 'Fed Balance Sheet',
        'ECBASSETSW': 'ECB Balance Sheet',
        'JPNASSETS': 'BOJ Balance Sheet',
        'BOGZ1FL893020005Q': 'Total Financial Assets',
        'GFDEBTN': 'Federal Debt Total',
        'GFDEGDQ188S': 'Federal Debt to GDP',
        'MTSDS133FMS': 'Federal Surplus/Deficit',
        'FYGFDPUN': 'Federal Debt Held by Public',
        'FDHBFRBN': 'Fed Holdings of Treasuries',
        'FDHBFIN': 'Foreign Holdings of Treasuries',
        'HQLA': 'High Quality Liquid Assets',
        'TOTRESNS': 'Total Bank Reserves',
        'WTREGEN': 'Treasury General Account',
        'RRPONTSYD': 'Reverse Repo Outstanding',
        'WLCFLPCL': 'Fed Lending Facilities',
    },

    # ── COMMODITIES (12+ series) ──
    'commodities': {
        'DCOILWTICO': 'WTI Crude Oil',
        'DCOILBRENTEU': 'Brent Crude Oil',
        'GASREGW': 'Regular Gasoline Price',
        'GOLDAMGBD228NLBM': 'Gold Price London Fix',
        'DEXSFEVS': 'Silver Price',
        'DHHNGSP': 'Henry Hub Natural Gas',
        'PCU2122212122210': 'Copper Price Index',
        'WPU0561': 'Aluminum Price',
        'PCOTTINDUSDM': 'Cotton Price',
        'PMAIZMTUSDM': 'Corn Price (Global)',
        'PWHEAMTUSDM': 'Wheat Price (Global)',
        'PSOYBUSDM': 'Soybean Price (Global)',
        'PNRGINDEXM': 'Global Energy Price Index',
        'PALLFNFINDEXM': 'All Commodities Index',
    },

    # ── SYSTEMIC RISK (10+ series) ──
    'systemic_risk': {
        'STLFSI2': 'Financial Stress Index',
        'NFCI': 'Natl Financial Conditions',
        'ANFCI': 'Adjusted NFCI',
        'KCFSI': 'KC Financial Stress',
        'TEDRATE': 'TED Spread',
        'BAA10Y': 'BAA-10Y Spread (Default Risk)',
        'AAA10Y': 'AAA-10Y Spread',
        'T10Y2Y': 'Yield Curve Spread',
        'T10Y3M': 'Yield Curve 10Y-3M',
        'VIXCLS': 'VIX Index',
        'DRTSCILM': 'Lending Tightening (Large)',
        'DPRIME': 'Prime Rate',
    },
}

# ── STOCK TICKERS (40+ via Polygon) ──
STOCK_TICKERS = [
    'SPY', 'QQQ', 'DIA', 'IWM', 'VTI',  # Major indices
    'XLF', 'XLE', 'XLK', 'XLV', 'XLI', 'XLU', 'XLP', 'XLY', 'XLB', 'XLC', 'XLRE',  # Sectors
    'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'META', 'TSLA',  # Mega caps
    'TLT', 'IEF', 'SHY', 'HYG', 'LQD', 'JNK', 'AGG', 'BND',  # Bonds
    'GLD', 'SLV', 'USO', 'UNG', 'DBA',  # Commodities
    'UUP', 'FXE', 'FXY', 'FXB',  # Currency ETFs
    'EEM', 'VWO', 'EFA', 'VEA',  # International
    'VNQ', 'VNQI',  # REITs
]

# ============================================================
# DATA FETCHING ENGINE
# ============================================================

def fetch_fred(series_id, limit=120):
    """Fetch FRED series data with error handling"""
    try:
        url = f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json&sort_order=desc&limit={limit}"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/9.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            obs = data.get('observations', [])
            clean = []
            for o in obs:
                if o['value'] != '.':
                    try:
                        clean.append({'date': o['date'], 'value': float(o['value'])})
                    except:
                        pass
            return clean
    except Exception as e:
        return []


def fetch_fred_meta(series_id):
    """Fetch FRED series metadata"""
    try:
        url = f"https://api.stlouisfed.org/fred/series?series_id={series_id}&api_key={FRED_API_KEY}&file_type=json"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/9.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            s = data.get('seriess', [{}])[0]
            return {
                'title': s.get('title', ''),
                'units': s.get('units', ''),
                'frequency': s.get('frequency', ''),
                'last_updated': s.get('last_updated', ''),
            }
    except:
        return {}


def fetch_polygon_ticker(ticker):
    """Fetch stock/ETF data from Polygon"""
    try:
        today = datetime.utcnow()
        start = (today - timedelta(days=400)).strftime('%Y-%m-%d')
        end = today.strftime('%Y-%m-%d')
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=desc&limit=250&apiKey={POLYGON_API_KEY}"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/9.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            data = json.loads(resp.read())
            results = data.get('results', [])
            if not results:
                return None
            bars = []
            for r in results:
                bars.append({
                    'date': datetime.utcfromtimestamp(r['t']/1000).strftime('%Y-%m-%d'),
                    'open': r['o'], 'high': r['h'], 'low': r['l'],
                    'close': r['c'], 'volume': r.get('v', 0)
                })
            return bars
    except:
        return None


def compute_changes(data_points):
    """Compute change metrics from data"""
    if not data_points or len(data_points) < 2:
        return {'current': None, 'prev': None, 'change': None, 'pct_change': None}
    
    current = data_points[0]['value']
    prev = data_points[1]['value']
    change = round(current - prev, 4)
    pct = round((change / abs(prev) * 100) if prev != 0 else 0, 2)
    
    # Extended lookbacks
    result = {
        'current': current,
        'date': data_points[0]['date'],
        'prev': prev,
        'change': change,
        'pct_change': pct,
    }
    
    # 1-week (5 data points for daily)
    if len(data_points) >= 6:
        w = data_points[5]['value']
        result['week_change'] = round(current - w, 4)
        result['week_pct'] = round((current - w) / abs(w) * 100, 2) if w != 0 else 0
    
    # 1-month (22 data points for daily)
    if len(data_points) >= 23:
        m = data_points[22]['value']
        result['month_change'] = round(current - m, 4)
        result['month_pct'] = round((current - m) / abs(m) * 100, 2) if m != 0 else 0
    
    # 3-month
    if len(data_points) >= 66:
        q = data_points[65]['value']
        result['quarter_change'] = round(current - q, 4)
        result['quarter_pct'] = round((current - q) / abs(q) * 100, 2) if q != 0 else 0
    
    # 6-month
    if len(data_points) >= 120:
        h = data_points[-1]['value']
        result['half_year_change'] = round(current - h, 4)
        result['half_year_pct'] = round((current - h) / abs(h) * 100, 2) if h != 0 else 0
    
    # Historical range
    values = [d['value'] for d in data_points]
    result['high'] = max(values)
    result['low'] = min(values)
    result['avg'] = round(sum(values) / len(values), 4)
    result['data_points'] = len(data_points)
    
    return result


def compute_stock_metrics(bars):
    """Compute stock/ETF metrics"""
    if not bars or len(bars) < 2:
        return None
    
    c = bars[0]
    p = bars[1]
    
    result = {
        'price': c['close'],
        'date': c['date'],
        'open': c['open'],
        'high': c['high'],
        'low': c['low'],
        'volume': c['volume'],
        'day_change': round(c['close'] - p['close'], 2),
        'day_pct': round((c['close'] - p['close']) / p['close'] * 100, 2) if p['close'] else 0,
    }
    
    # Weekly
    if len(bars) >= 6:
        w = bars[5]['close']
        result['week_pct'] = round((c['close'] - w) / w * 100, 2) if w else 0
    
    # Monthly
    if len(bars) >= 22:
        m = bars[21]['close']
        result['month_pct'] = round((c['close'] - m) / m * 100, 2) if m else 0
    
    # 3-month
    if len(bars) >= 66:
        q = bars[65]['close']
        result['quarter_pct'] = round((c['close'] - q) / q * 100, 2) if q else 0
    
    # YTD
    year_start = None
    for b in bars:
        if b['date'][:4] != c['date'][:4]:
            year_start = b
            break
    if year_start:
        result['ytd_pct'] = round((c['close'] - year_start['close']) / year_start['close'] * 100, 2)
    
    # SMA
    closes = [b['close'] for b in bars]
    if len(closes) >= 20:
        result['sma20'] = round(sum(closes[:20]) / 20, 2)
    if len(closes) >= 50:
        result['sma50'] = round(sum(closes[:50]) / 50, 2)
    if len(closes) >= 200:
        result['sma200'] = round(sum(closes[:200]) / 200, 2)
    
    # Range
    all_highs = [b['high'] for b in bars[:250]]
    all_lows = [b['low'] for b in bars[:250]]
    result['52w_high'] = max(all_highs) if all_highs else None
    result['52w_low'] = min(all_lows) if all_lows else None
    
    return result


# ============================================================
# KHALID INDEX™ V9 - ENHANCED SCORING
# ============================================================

def compute_khalid_index(fred_data, stock_data):
    """Compute the Khalid Index™ - proprietary market regime scoring"""
    score = 50  # Neutral baseline
    signals = []
    
    # DXY Impact (-15 to +5)
    dxy = fred_data.get('dxy', {}).get('DTWEXBGS', {})
    if dxy.get('current'):
        dxy_val = dxy['current']
        if dxy_val > 115:
            score -= 12
            signals.append(('DXY VERY_STRONG', -12, f'{dxy_val:.1f}'))
        elif dxy_val > 110:
            score -= 8
            signals.append(('DXY Strong', -8, f'{dxy_val:.1f}'))
        elif dxy_val > 105:
            score -= 3
            signals.append(('DXY Moderate', -3, f'{dxy_val:.1f}'))
        elif dxy_val < 95:
            score += 5
            signals.append(('DXY Weak (Bullish)', +5, f'{dxy_val:.1f}'))
    
    # Credit Spreads (-15 to +5)
    hy = fred_data.get('ice_bofa', {}).get('BAMLH0A0HYM2', {})
    if hy.get('current'):
        spread = hy['current']
        if spread > 6:
            score -= 15
            signals.append(('HY Spread CRISIS', -15, f'{spread:.2f}%'))
        elif spread > 5:
            score -= 10
            signals.append(('HY Spread Elevated', -10, f'{spread:.2f}%'))
        elif spread > 4:
            score -= 5
            signals.append(('HY Spread Warning', -5, f'{spread:.2f}%'))
        elif spread < 3:
            score += 5
            signals.append(('HY Spread Tight (Bullish)', +5, f'{spread:.2f}%'))
    
    # Yield Curve (-10 to +5)
    curve = fred_data.get('treasury', {}).get('T10Y2Y', {})
    if curve.get('current'):
        spread = curve['current']
        if spread < -0.5:
            score -= 10
            signals.append(('Yield Curve INVERTED', -10, f'{spread:.2f}%'))
        elif spread < 0:
            score -= 5
            signals.append(('Yield Curve Flat/Inverted', -5, f'{spread:.2f}%'))
        elif spread > 1:
            score += 5
            signals.append(('Yield Curve Steep (Bullish)', +5, f'{spread:.2f}%'))
    
    # VIX (-12 to +5)
    vix = fred_data.get('risk', {}).get('VIXCLS', {})
    if vix.get('current'):
        v = vix['current']
        if v > 35:
            score -= 12
            signals.append(('VIX EXTREME FEAR', -12, f'{v:.1f}'))
        elif v > 25:
            score -= 6
            signals.append(('VIX Elevated', -6, f'{v:.1f}'))
        elif v < 15:
            score += 5
            signals.append(('VIX Low (Complacent)', +5, f'{v:.1f}'))
    
    # Financial Stress (-10 to +5)
    nfci = fred_data.get('risk', {}).get('NFCI', {})
    if nfci.get('current'):
        n = nfci['current']
        if n > 0.5:
            score -= 10
            signals.append(('NFCI Tight Conditions', -10, f'{n:.2f}'))
        elif n > 0:
            score -= 3
            signals.append(('NFCI Above Average', -3, f'{n:.2f}'))
        elif n < -0.5:
            score += 5
            signals.append(('NFCI Loose Conditions', +5, f'{n:.2f}'))
    
    # Fed Balance Sheet Direction (-5 to +5)
    fed = fred_data.get('liquidity', {}).get('WALCL', {})
    if fed.get('pct_change') is not None:
        if fed['pct_change'] < -1:
            score -= 5
            signals.append(('Fed Tightening (QT)', -5, f"{fed['pct_change']:.1f}%"))
        elif fed['pct_change'] > 1:
            score += 5
            signals.append(('Fed Easing', +5, f"{fed['pct_change']:.1f}%"))
    
    # Unemployment Direction (-8 to +3)
    unemp = fred_data.get('macro', {}).get('UNRATE', {})
    if unemp.get('current') and unemp.get('month_pct') is not None:
        if unemp['month_pct'] > 5:
            score -= 8
            signals.append(('Unemployment Rising Fast', -8, f"{unemp['current']:.1f}%"))
        elif unemp['month_pct'] > 0:
            score -= 3
            signals.append(('Unemployment Rising', -3, f"{unemp['current']:.1f}%"))
        elif unemp['month_pct'] < -2:
            score += 3
            signals.append(('Unemployment Falling', +3, f"{unemp['current']:.1f}%"))
    
    # ISM PMI (-8 to +5)
    pmi = fred_data.get('global_cycle', {}).get('NAPM', {})
    if pmi.get('current'):
        p = pmi['current']
        if p < 45:
            score -= 8
            signals.append(('PMI Deep Contraction', -8, f'{p:.1f}'))
        elif p < 50:
            score -= 4
            signals.append(('PMI Contraction', -4, f'{p:.1f}'))
        elif p > 55:
            score += 5
            signals.append(('PMI Strong Expansion', +5, f'{p:.1f}'))
    
    # Global Liquidity (-5 to +5)
    tga = fred_data.get('liquidity', {}).get('WTREGEN', {})
    rrp = fred_data.get('liquidity', {}).get('RRPONTSYD', {})
    if tga.get('pct_change') is not None and rrp.get('pct_change') is not None:
        # TGA draining + RRP draining = liquidity injection
        if tga['pct_change'] < -5 and rrp['pct_change'] < -5:
            score += 5
            signals.append(('Liquidity Injection', +5, 'TGA+RRP Draining'))
        elif tga['pct_change'] > 10:
            score -= 5
            signals.append(('Liquidity Drain (TGA Fill)', -5, f"TGA {tga['pct_change']:.1f}%"))
    
    # SPY Trend (-5 to +5)
    spy = stock_data.get('SPY', {})
    if spy and spy.get('sma50') and spy.get('sma200'):
        if spy['price'] > spy['sma50'] > spy['sma200']:
            score += 5
            signals.append(('SPY Bullish Trend', +5, f"${spy['price']:.0f}"))
        elif spy['price'] < spy['sma50'] < spy['sma200']:
            score -= 5
            signals.append(('SPY Bearish Trend', -5, f"${spy['price']:.0f}"))
    
    # Clamp
    score = max(0, min(100, score))
    
    # Regime
    if score >= 75:
        regime = 'STRONG_BULL'
    elif score >= 60:
        regime = 'BULL'
    elif score >= 45:
        regime = 'NEUTRAL'
    elif score >= 30:
        regime = 'BEAR'
    else:
        regime = 'CRISIS'
    
    return {
        'score': score,
        'regime': regime,
        'signals': signals,
        'timestamp': datetime.utcnow().isoformat(),
    }


# ============================================================
# RISK DASHBOARD METRICS
# ============================================================

def compute_risk_dashboard(fred_data):
    """Compute comprehensive risk metrics"""
    risk_metrics = {}
    
    # 1. Credit Risk Score (0-100)
    credit_score = 50
    hy = fred_data.get('ice_bofa', {}).get('BAMLH0A0HYM2', {})
    if hy.get('current'):
        if hy['current'] < 3: credit_score = 80
        elif hy['current'] < 4: credit_score = 60
        elif hy['current'] < 5: credit_score = 40
        elif hy['current'] < 6: credit_score = 20
        else: credit_score = 10
    risk_metrics['credit_risk'] = credit_score
    
    # 2. Liquidity Risk Score
    liq_score = 50
    fed = fred_data.get('liquidity', {}).get('WALCL', {})
    rrp = fred_data.get('liquidity', {}).get('RRPONTSYD', {})
    if fed.get('pct_change') is not None:
        if fed['pct_change'] > 0: liq_score += 15
        else: liq_score -= 15
    if rrp.get('pct_change') is not None:
        if rrp['pct_change'] < 0: liq_score += 10  # RRP drain = good
        else: liq_score -= 10
    risk_metrics['liquidity_risk'] = max(0, min(100, liq_score))
    
    # 3. Market Risk Score
    mkt_score = 50
    vix = fred_data.get('risk', {}).get('VIXCLS', {})
    if vix.get('current'):
        if vix['current'] < 15: mkt_score = 85
        elif vix['current'] < 20: mkt_score = 65
        elif vix['current'] < 25: mkt_score = 45
        elif vix['current'] < 30: mkt_score = 25
        else: mkt_score = 10
    risk_metrics['market_risk'] = mkt_score
    
    # 4. Recession Risk Score
    rec_score = 50
    curve = fred_data.get('treasury', {}).get('T10Y2Y', {})
    if curve.get('current'):
        if curve['current'] < -0.5: rec_score = 15
        elif curve['current'] < 0: rec_score = 30
        elif curve['current'] < 0.5: rec_score = 50
        else: rec_score = 70
    risk_metrics['recession_risk'] = rec_score
    
    # 5. Systemic Risk Score
    sys_score = 50
    nfci = fred_data.get('risk', {}).get('NFCI', {})
    stlfsi = fred_data.get('risk', {}).get('STLFSI2', {})
    if nfci.get('current'):
        if nfci['current'] > 0.5: sys_score -= 25
        elif nfci['current'] > 0: sys_score -= 10
        elif nfci['current'] < -0.5: sys_score += 20
    if stlfsi.get('current'):
        if stlfsi['current'] > 2: sys_score -= 20
        elif stlfsi['current'] > 0: sys_score -= 5
        elif stlfsi['current'] < -1: sys_score += 10
    risk_metrics['systemic_risk'] = max(0, min(100, sys_score))
    
    # 6. Inflation Risk
    inf_score = 50
    cpi = fred_data.get('inflation', {}).get('CPALTT01USM657N', {})
    if cpi.get('current'):
        if cpi['current'] > 6: inf_score = 10
        elif cpi['current'] > 4: inf_score = 30
        elif cpi['current'] > 3: inf_score = 45
        elif cpi['current'] > 2: inf_score = 70
        else: inf_score = 85
    risk_metrics['inflation_risk'] = inf_score
    
    # Overall composite
    scores = [v for v in risk_metrics.values() if isinstance(v, (int, float))]
    risk_metrics['composite'] = round(sum(scores) / len(scores)) if scores else 50
    
    return risk_metrics


# ============================================================
# NET LIQUIDITY CALCULATOR
# ============================================================

def compute_net_liquidity(fred_data):
    """Compute Fed Net Liquidity = Fed Assets - TGA - RRP"""
    fed = fred_data.get('liquidity', {}).get('WALCL', {})
    tga = fred_data.get('liquidity', {}).get('WTREGEN', {})
    rrp = fred_data.get('liquidity', {}).get('RRPONTSYD', {})
    
    result = {}
    if fed.get('current') and tga.get('current') and rrp.get('current'):
        # All in millions for WALCL, billions for TGA/RRP
        fed_val = fed['current']  # Millions
        tga_val = tga['current']  # Millions
        rrp_val = rrp['current']  # Billions -> convert
        
        net_liq = fed_val - tga_val - (rrp_val * 1000 if rrp_val < 10000 else rrp_val)
        result['net_liquidity'] = round(net_liq)
        result['fed_assets'] = round(fed_val)
        result['tga'] = round(tga_val)
        result['rrp'] = round(rrp_val)
        result['unit'] = 'millions'
    
    return result


# ============================================================
# MAIN HANDLER
# ============================================================

def lambda_handler(event, context):
    start_time = time.time()
    
    print(f"[V9] Starting mega data fetch at {datetime.utcnow().isoformat()}")
    
    # ── PHASE 1: Fetch ALL FRED data in parallel ──
    all_fred_data = {}
    all_series = []
    
    # Collect all unique series across all categories
    seen = set()
    for category, series_dict in FRED_SERIES.items():
        for series_id, name in series_dict.items():
            if series_id not in seen:
                all_series.append((category, series_id, name))
                seen.add(series_id)
    
    print(f"[V9] Fetching {len(all_series)} unique FRED series...")
    
    fred_raw = {}
    with ThreadPoolExecutor(max_workers=25) as executor:
        future_map = {}
        for cat, sid, name in all_series:
            f = executor.submit(fetch_fred, sid)
            future_map[f] = (cat, sid, name)
        
        for future in as_completed(future_map):
            cat, sid, name = future_map[future]
            try:
                data = future.result()
                if data:
                    fred_raw[sid] = data
            except Exception as e:
                print(f"  Error fetching {sid}: {e}")
    
    print(f"[V9] Got {len(fred_raw)} FRED series in {time.time()-start_time:.1f}s")
    
    # Process into categories
    for category, series_dict in FRED_SERIES.items():
        if category not in all_fred_data:
            all_fred_data[category] = {}
        for sid, name in series_dict.items():
            raw = fred_raw.get(sid, [])
            metrics = compute_changes(raw)
            metrics['name'] = name
            metrics['series_id'] = sid
            metrics['history'] = raw[:60]  # Last 60 data points for charting
            all_fred_data[category][sid] = metrics
    
    # ── PHASE 2: Fetch stock/ETF data ──
    print(f"[V9] Fetching {len(STOCK_TICKERS)} stock/ETF tickers...")
    stock_phase_start = time.time()
    
    stock_data = {}
    with ThreadPoolExecutor(max_workers=15) as executor:
        future_map = {executor.submit(fetch_polygon_ticker, t): t for t in STOCK_TICKERS}
        for future in as_completed(future_map):
            ticker = future_map[future]
            try:
                bars = future.result()
                if bars:
                    metrics = compute_stock_metrics(bars)
                    if metrics:
                        metrics['history'] = [{'date': b['date'], 'close': b['close']} for b in bars[:120]]
                        stock_data[ticker] = metrics
            except:
                pass
    
    print(f"[V9] Got {len(stock_data)} tickers in {time.time()-stock_phase_start:.1f}s")
    
    # ── PHASE 3: Compute Analytics ──
    ki = compute_khalid_index(all_fred_data, stock_data)
    risk = compute_risk_dashboard(all_fred_data)
    net_liq = compute_net_liquidity(all_fred_data)
    
    # ── PHASE 4: Sector Analysis ──
    sector_etfs = {
        'XLF': 'Financials', 'XLE': 'Energy', 'XLK': 'Technology',
        'XLV': 'Healthcare', 'XLI': 'Industrials', 'XLU': 'Utilities',
        'XLP': 'Staples', 'XLY': 'Discretionary', 'XLB': 'Materials',
        'XLC': 'Communications', 'XLRE': 'Real Estate'
    }
    sectors = {}
    for etf, name in sector_etfs.items():
        if etf in stock_data:
            s = stock_data[etf]
            sectors[etf] = {
                'name': name, 'price': s['price'],
                'day_pct': s.get('day_pct', 0),
                'week_pct': s.get('week_pct', 0),
                'month_pct': s.get('month_pct', 0),
                'quarter_pct': s.get('quarter_pct', 0),
            }
    
    # ── PHASE 5: Trading Signals ──
    signals = {'buys': [], 'sells': [], 'warnings': [], 'at_risk': []}
    for ticker, s in stock_data.items():
        if not s.get('sma50') or not s.get('sma200'):
            continue
        
        if s['price'] > s['sma50'] > s['sma200']:
            if s.get('day_pct', 0) > 0:
                signals['buys'].append(ticker)
        elif s['price'] < s['sma50'] < s['sma200']:
            signals['sells'].append(ticker)
        
        if s.get('sma50') and s.get('sma200'):
            if abs(s['sma50'] - s['sma200']) / s['sma200'] < 0.01:
                signals['at_risk'].append(ticker)
        
        if s.get('day_pct', 0) < -3:
            signals['warnings'].append(f"{ticker} down {s['day_pct']:.1f}%")
    
    # ── PHASE 6: Build Report JSON ──
    report = {
        'version': 'V9',
        'generated_at': datetime.utcnow().isoformat() + 'Z',
        'fetch_time_seconds': round(time.time() - start_time, 1),
        'khalid_index': ki,
        'risk_dashboard': risk,
        'net_liquidity': net_liq,
        'sectors': sectors,
        'signals': signals,
        'fred': all_fred_data,
        'stocks': stock_data,
        'stats': {
            'fred_series': len(fred_raw),
            'stock_tickers': len(stock_data),
            'total_data_points': sum(len(v) for v in fred_raw.values()) + sum(len(s.get('history', [])) for s in stock_data.values()),
        }
    }
    
    # ── PHASE 7: Upload to S3 ──
    try:
        report_json = json.dumps(report, default=str)
        s3.put_object(
            Bucket=S3_BUCKET,
            Key='data/report.json',
            Body=report_json,
            ContentType='application/json',
            CacheControl='max-age=60',
        )
        
        # Also save timestamped archive
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M')
        s3.put_object(
            Bucket=S3_BUCKET,
            Key=f'data/archive/report_{ts}.json',
            Body=report_json,
            ContentType='application/json',
        )
        
        elapsed = round(time.time() - start_time, 1)
        summary = {
            'status': 'published',
            'ki': ki['score'],
            'regime': ki['regime'],
            'fred': len(fred_raw),
            'stocks': len(stock_data),
            'risk_composite': risk.get('composite', 0),
            'fetch_time': elapsed,
            'dxy': all_fred_data.get('dxy', {}).get('DTWEXBGS', {}).get('current'),
            'hy_spread': all_fred_data.get('ice_bofa', {}).get('BAMLH0A0HYM2', {}).get('current'),
            'vix': all_fred_data.get('risk', {}).get('VIXCLS', {}).get('current'),
            'fed_assets_T': round(all_fred_data.get('liquidity', {}).get('WALCL', {}).get('current', 0) / 1e6, 2),
        }
        
        print(f"[V9] DONE in {elapsed}s: {json.dumps(summary)}")
        return {'statusCode': 200, 'body': json.dumps(summary)}
        
    except Exception as e:
        print(f"[V9] S3 Error: {e}")
        return {'statusCode': 500, 'body': json.dumps({'error': str(e)})}
