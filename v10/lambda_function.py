"""
JUSTHODL BLOOMBERG TERMINAL V10 - MEGA INTELLIGENCE
=====================================================
200+ FRED | 80+ Stocks/ETFs | 25 Crypto | AI Analysis
Portfolio Construction | Risk Signals | Auto 8AM+6PM ET
=====================================================
"""
import json, urllib.request, os, time, boto3
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed

FRED_KEY = os.environ.get('FRED_API_KEY', '2f057499936072679d8843d7fce99989')
POLY_KEY = os.environ.get('POLYGON_API_KEY', 'zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d')
S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
s3 = boto3.client('s3', region_name=os.environ.get('AWS_REGION','us-east-1'))

# ============================================================
# ALL FRED SERIES - VERIFIED IDs, ORGANIZED BY PRIORITY
# ============================================================
# Format: series_id -> (category, display_name)
FRED_SERIES = {
    # ── TREASURY YIELDS (Daily) ──
    'DGS1MO':('treasury','1-Month'), 'DGS3MO':('treasury','3-Month'), 'DGS6MO':('treasury','6-Month'),
    'DGS1':('treasury','1-Year'), 'DGS2':('treasury','2-Year'), 'DGS3':('treasury','3-Year'),
    'DGS5':('treasury','5-Year'), 'DGS7':('treasury','7-Year'), 'DGS10':('treasury','10-Year'),
    'DGS20':('treasury','20-Year'), 'DGS30':('treasury','30-Year'),
    'T10Y2Y':('treasury','10Y-2Y Spread'), 'T10Y3M':('treasury','10Y-3M Spread'),
    'T10YFF':('treasury','10Y-FF Spread'), 'T5YFF':('treasury','5Y-FF Spread'),
    'T10YIE':('treasury','10Y Breakeven'), 'T5YIE':('treasury','5Y Breakeven'),
    'T5YIFR':('treasury','5Y5Y Forward'), 'DFII10':('treasury','10Y TIPS'),
    'DFII5':('treasury','5Y TIPS'), 'DFII30':('treasury','30Y TIPS'),

    # ── DXY / FX (Daily) ──
    'DTWEXBGS':('dxy','USD Broad Index'), 'DTWEXEMEGS':('dxy','USD vs EM'),
    'DTWEXAFEGS':('dxy','USD vs Advanced'), 'DEXUSEU':('dxy','USD/EUR'),
    'DEXJPUS':('dxy','JPY/USD'), 'DEXUSUK':('dxy','USD/GBP'), 'DEXSZUS':('dxy','CHF/USD'),
    'DEXCAUS':('dxy','CAD/USD'), 'DEXMXUS':('dxy','MXN/USD'), 'DEXCHUS':('dxy','CNY/USD'),
    'DEXKOUS':('dxy','KRW/USD'), 'DEXBZUS':('dxy','BRL/USD'), 'DEXINUS':('dxy','INR/USD'),
    'RTWEXBGS':('dxy','Real Trade Weighted USD'), 'TWEXBGSMTH':('dxy','USD Broad Monthly'),

    # ── ICE BofA CREDIT (Daily) ──
    'BAMLH0A0HYM2':('ice_bofa','HY OAS'), 'BAMLC0A0CM':('ice_bofa','IG Corp OAS'),
    'BAMLH0A1HYBB':('ice_bofa','BB OAS'), 'BAMLH0A2HYBEY':('ice_bofa','B OAS'),
    'BAMLH0A3HYC':('ice_bofa','CCC OAS'), 'BAMLC0A1CAAA':('ice_bofa','AAA OAS'),
    'BAMLC0A2CAA':('ice_bofa','AA OAS'), 'BAMLC0A3CA':('ice_bofa','A OAS'),
    'BAMLC0A4CBBB':('ice_bofa','BBB OAS'), 'BAMLEMCBPIOAS':('ice_bofa','EM Corp OAS'),
    'BAMLEMHBHYCRPIOAS':('ice_bofa','EM HY OAS'), 'BAMLHE00EHYIOAS':('ice_bofa','Euro HY OAS'),
    'BAMLC0A0CMEY':('ice_bofa','IG Eff Yield'), 'BAMLH0A0HYM2EY':('ice_bofa','HY Eff Yield'),
    'BAMLC0A1CAAAEY':('ice_bofa','AAA Eff Yield'), 'BAMLC0A4CBBBEY':('ice_bofa','BBB Eff Yield'),
    'BAMLC1A0C13YEY':('ice_bofa','Corp 1-3Y'), 'BAMLC2A0C35YEY':('ice_bofa','Corp 3-5Y'),
    'BAMLC3A0C57YEY':('ice_bofa','Corp 5-7Y'), 'BAMLC4A0C710YEY':('ice_bofa','Corp 7-10Y'),
    'BAMLC7A0C1015YEY':('ice_bofa','Corp 10-15Y'), 'BAMLC8A0C15PYEY':('ice_bofa','Corp 15Y+'),
    'BAMLHYH0A0HYM2TRIV':('ice_bofa','HY Total Return'), 'BAMLCC0A0CMTRIV':('ice_bofa','IG Total Return'),

    # ── RISK / VOLATILITY (Daily + Monthly) ──
    'VIXCLS':('risk','VIX'), 'TEDRATE':('risk','TED Spread'),
    'DPRIME':('risk','Prime Rate'), 'DAAA':('risk','Moody AAA Yield'),
    'DBAA':('risk','Moody BAA Yield'), 'AAA10Y':('risk','AAA-10Y Spread'),
    'BAA10Y':('risk','BAA-10Y Spread'), 'MORTGAGE30US':('risk','30Y Mortgage'),
    'MORTGAGE15US':('risk','15Y Mortgage'),
    'STLFSI4':('risk','StL Financial Stress'), 'NFCI':('risk','Chicago NFCI'),
    'ANFCI':('risk','Adjusted NFCI'), 'KCFSI':('risk','KC Financial Stress'),

    # ── LIQUIDITY (Weekly + Monthly) ──
    'WALCL':('liquidity','Fed Total Assets'), 'WTREGEN':('liquidity','Treasury General Acct'),
    'RRPONTSYD':('liquidity','Overnight RRP'), 'RPONTSYD':('liquidity','Overnight Repo'),
    'TOTRESNS':('liquidity','Total Reserves'), 'WLCFLPCL':('liquidity','Fed Loans to Banks'),
    'WSHOSHO':('liquidity','Fed Treasury Holdings'), 'WSHOMCB':('liquidity','Fed MBS Holdings'),
    'DFF':('liquidity','Eff Fed Funds Rate'), 'SOFR':('liquidity','SOFR'),
    'M2SL':('liquidity','M2 Money Supply'), 'BOGMBASE':('liquidity','Monetary Base'),
    'M2V':('liquidity','M2 Velocity'), 'EFFR':('liquidity','EFFR'),

    # ── MACRO ECONOMY (Monthly/Quarterly) ──
    'GDP':('macro','Nominal GDP'), 'GDPC1':('macro','Real GDP'),
    'A191RL1Q225SBEA':('macro','GDP Growth Rate'), 'INDPRO':('macro','Industrial Production'),
    'TCU':('macro','Capacity Utilization'), 'PAYEMS':('macro','Nonfarm Payrolls'),
    'UNRATE':('macro','Unemployment'), 'U6RATE':('macro','U-6 Unemployment'),
    'CIVPART':('macro','Labor Participation'), 'UMCSENT':('macro','Consumer Sentiment'),
    'RSAFS':('macro','Retail Sales'), 'HOUST':('macro','Housing Starts'),
    'PERMIT':('macro','Building Permits'), 'HSN1F':('macro','New Home Sales'),
    'EXHOSLUSM495S':('macro','Existing Home Sales'), 'PI':('macro','Personal Income'),
    'PCE':('macro','Personal Consumption'), 'DGORDER':('macro','Durable Goods'),
    'NEWORDER':('macro','Mfg New Orders'), 'AWHMAN':('macro','Avg Weekly Hours Mfg'),
    'CES0500000003':('macro','Avg Hourly Earnings'), 'JTSJOL':('macro','Job Openings'),
    'JTSHIR':('macro','Hires'), 'JTSQUR':('macro','Quits Rate'),
    'ICSA':('macro','Initial Claims'), 'CCSA':('macro','Continued Claims'),
    'USSLIND':('macro','Leading Index'), 'CFNAI':('macro','Chicago Fed Activity'),
    'CPILFESL':('macro','Core CPI'), 'CPIAUCSL':('macro','CPI All Items'),
    'PCEPI':('macro','PCE Price Index'), 'PCEPILFE':('macro','Core PCE'),
    'PPIFIS':('macro','PPI Final Demand'),

    # ── INFLATION (Monthly) ──
    'CPALTT01USM657N':('inflation','CPI YoY'), 'MICH':('inflation','Michigan Inflation Exp'),
    'EXPINF1YR':('inflation','1Y Inflation Exp'), 'EXPINF10YR':('inflation','10Y Inflation Exp'),
    'CPIUFDSL':('inflation','CPI Food'), 'CPIENGSL':('inflation','CPI Energy'),
    'CUSR0000SAH1':('inflation','CPI Shelter'), 'CUSR0000SETB01':('inflation','CPI Gasoline'),
    'CUSR0000SAM2':('inflation','CPI Medical'), 'WPSFD49207':('inflation','PPI Finished Goods'),

    # ── CREDIT & LENDING (Monthly/Quarterly) ──
    'TOTALSL':('credit','Total Consumer Credit'), 'REVOLSL':('credit','Revolving Credit'),
    'NONREVSL':('credit','Non-Revolving Credit'), 'BUSLOANS':('credit','C&I Loans'),
    'REALLN':('credit','Real Estate Loans'), 'CONSUMER':('credit','Consumer Loans Banks'),
    'DRCCLACBS':('credit','Credit Card Delinquency'), 'DRSFRMACBS':('credit','Mortgage Delinquency'),
    'DRALACBS':('credit','Auto Delinquency'), 'DRCLACBS':('credit','C&I Delinquency'),
    'CORCCACBS':('credit','CC Charge-Off'), 'CORCBS':('credit','C&I Charge-Off'),
    'CRELACBS':('credit','RE Charge-Off'), 'DRTSCILM':('credit','Lending Std C&I Large'),
    'DRTSCIS':('credit','Lending Std C&I Small'), 'TOTCI':('credit','Total C&I Loans'),
    'SLOAS':('credit','Student Loans'),

    # ── GLOBAL CYCLE / PMI (Monthly) ──
    'MANEMP':('global_cycle','ISM Mfg Employment'), 'NAPMPRI':('global_cycle','ISM Prices'),
    'IPMAN':('global_cycle','IP Manufacturing'), 'MCUMFN':('global_cycle','Mfg Capacity Util'),
    'ACDGNO':('global_cycle','Core Cap Goods Orders'), 'AMTMTI':('global_cycle','Mfg Trade Inventories'),
    'IPMANSICS':('global_cycle','IP Mfg SIC'), 'MNFCTIRSA':('global_cycle','Mfg Inventories'),

    # ── PMI WORLD / OECD CLI (Monthly) ──
    'USALOLITONOSTSAM':('pmi_world','US CLI'), 'CHNLOLITONOSTSAM':('pmi_world','China CLI'),
    'BRALOLITONOSTSAM':('pmi_world','Brazil CLI'), 'INDLOLITONOSTSAM':('pmi_world','India CLI'),
    'FRALOLITONOSTSAM':('pmi_world','France CLI'), 'CANLOLITONOSTSAM':('pmi_world','Canada CLI'),
    'MEXLOLITONOSTSAM':('pmi_world','Mexico CLI'), 'KORLOLITONOSTSAM':('pmi_world','Korea CLI'),
    'JPLOLITONOSTSAM':('pmi_world','Japan CLI'), 'DEULOLIT02IXOBSAM':('pmi_world','Germany CLI'),
    'GBRLOLIT02IXOBSAM':('pmi_world','UK CLI'),

    # ── ECB / EUROPE (Monthly) ──
    'ECBASSETSW':('ecb','ECB Total Assets'), 'ECBDFR':('ecb','ECB Deposit Rate'),
    'ECBMLFR':('ecb','ECB Main Refi Rate'), 'INTDSREZM193N':('ecb','Euro Deposit Rate'),
    'CLVMNACSCAB1GQEA19':('ecb','Euro Real GDP'), 'EA19CPALTT01GYM':('ecb','Euro CPI YoY'),
    'LRHUTTTTEZM156S':('ecb','Euro Unemployment'), 'IR3TIB01EZM156N':('ecb','Euro 3M Interbank'),
    'IRLTLT01EZM156N':('ecb','Euro LT Govt Bond'), 'CP0000EZ19M086NEST':('ecb','Euro HICP'),
    'MABMM301EZM189S':('ecb','Euro M3 Money'),

    # ── GLOBAL LIQUIDITY (Monthly/Quarterly) ──
    'JPNASSETS':('global_liquidity','BOJ Assets'), 'GFDEBTN':('global_liquidity','Federal Debt'),
    'GFDEGDQ188S':('global_liquidity','Debt to GDP'), 'FDHBFRBN':('global_liquidity','Fed Tsy Holdings'),
    'FDHBFIN':('global_liquidity','Foreign Tsy Holdings'), 'BOGZ1FL893020005Q':('global_liquidity','Unidentified Financial'),
    'MTSDS133FMS':('global_liquidity','Monthly Tsy Statement'),

    # ── COMMODITIES (Daily + Monthly) ──
    'DCOILWTICO':('commodities','WTI Crude'), 'DCOILBRENTEU':('commodities','Brent Crude'),
    'GOLDAMGBD228NLBM':('commodities','Gold London Fix'), 'DHHNGSP':('commodities','Natural Gas'),
    'GASREGW':('commodities','Gasoline'), 'PCU2122212122210':('commodities','Copper Index'),
    'PMAIZMTUSDM':('commodities','Corn Global'), 'PWHEAMTUSDM':('commodities','Wheat Global'),
    'PSOYBUSDM':('commodities','Soybean Global'), 'PNRGINDEXM':('commodities','Energy Index'),
    'PALLFNFINDEXM':('commodities','All Commodities'),

    # ── SYSTEMIC RISK (cross-reference) ──
    'SP500':('systemic_risk','S&P 500 FRED'),
}

# ── STOCK/ETF TICKERS (80+) ──
STOCK_TICKERS = [
    # Major Indices
    'SPY','QQQ','DIA','IWM','VTI','VOO','RSP','MDY',
    # Sectors
    'XLF','XLE','XLK','XLV','XLI','XLU','XLP','XLY','XLB','XLC','XLRE',
    # Mega Caps
    'AAPL','MSFT','GOOGL','AMZN','NVDA','META','TSLA','BRK.B','JPM','V',
    'UNH','JNJ','WMT','MA','PG','HD','BAC','XOM','CVX','COST',
    'ABBV','MRK','AVGO','LLY','CRM','AMD','NFLX','INTC','DIS','CSCO',
    # Bond ETFs
    'TLT','IEF','SHY','HYG','LQD','JNK','AGG','BND','GOVT','MBB',
    'VCSH','VCLT','EMB','BWX','TIP','VTIP','BIL','FLOT',
    # Commodity ETFs
    'GLD','SLV','USO','UNG','DBA','PDBC','DBC','IAU','PPLT','COPX',
    # FX ETFs
    'UUP','FXE','FXY','FXB','FXA','FXC',
    # International
    'EEM','VWO','EFA','VEA','IEMG','INDA','FXI','EWJ','EWZ','EWG',
    # REITs
    'VNQ','VNQI','IYR',
    # Leveraged (Khalid interest)
    'TQQQ','SOXL','UPRO','UDOW','NUGT','JNUG','UCO',
]

# ============================================================
# DATA FETCH FUNCTIONS
# ============================================================
def fetch_fred(sid):
    for attempt in range(3):
        try:
            url = f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=120"
            req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.0'})
            with urllib.request.urlopen(req, timeout=15) as resp:
                obs = json.loads(resp.read()).get('observations', [])
                out = []
                for o in obs:
                    if o['value'] != '.':
                        try: out.append({'date': o['date'], 'value': float(o['value'])})
                        except: pass
                return out if out else []
        except urllib.error.HTTPError as e:
            if e.code == 429:
                time.sleep(3 * (attempt + 1))
                continue
            return []
        except:
            if attempt < 2: time.sleep(1)
            else: return []
    return []

def fetch_polygon(ticker):
    try:
        today = datetime.utcnow()
        start = (today - timedelta(days=400)).strftime('%Y-%m-%d')
        end = today.strftime('%Y-%m-%d')
        url = f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start}/{end}?adjusted=true&sort=desc&limit=250&apiKey={POLY_KEY}"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.0'})
        with urllib.request.urlopen(req, timeout=15) as resp:
            results = json.loads(resp.read()).get('results', [])
            if not results: return None
            return [{'date': datetime.utcfromtimestamp(r['t']/1000).strftime('%Y-%m-%d'),
                     'o':r['o'],'h':r['h'],'l':r['l'],'c':r['c'],'v':r.get('v',0)} for r in results]
    except: return None

def fetch_crypto():
    try:
        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=25&page=1&sparkline=true&price_change_percentage=1h%2C24h%2C7d%2C30d"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.0', 'Accept': 'application/json'})
        with urllib.request.urlopen(req, timeout=20) as resp:
            coins = json.loads(resp.read())
            out = {}
            for c in coins:
                out[c['symbol'].upper()] = {
                    'name': c['name'], 'price': c['current_price'], 'market_cap': c['market_cap'],
                    'volume_24h': c['total_volume'], 'rank': c.get('market_cap_rank'),
                    'change_1h': c.get('price_change_percentage_1h_in_currency'),
                    'change_24h': c.get('price_change_percentage_24h_in_currency', c.get('price_change_percentage_24h')),
                    'change_7d': c.get('price_change_percentage_7d_in_currency'),
                    'change_30d': c.get('price_change_percentage_30d_in_currency'),
                    'ath': c.get('ath'), 'ath_pct': c.get('ath_change_percentage'),
                    'sparkline': c.get('sparkline_in_7d', {}).get('price', [])[-48:],
                    'circulating': c.get('circulating_supply'), 'total_supply': c.get('total_supply'),
                    'image': c.get('image'),
                }
            return out
    except Exception as e:
        print(f"Crypto error: {e}")
        return {}

def fetch_crypto_global():
    try:
        url = "https://api.coingecko.com/api/v3/global"
        req = urllib.request.Request(url, headers={'User-Agent': 'JustHodl/10.0'})
        with urllib.request.urlopen(req, timeout=10) as resp:
            d = json.loads(resp.read()).get('data', {})
            return {'total_mcap': d.get('total_market_cap',{}).get('usd'),
                    'total_vol': d.get('total_volume',{}).get('usd'),
                    'btc_dom': d.get('market_cap_percentage',{}).get('btc'),
                    'eth_dom': d.get('market_cap_percentage',{}).get('eth'),
                    'active_coins': d.get('active_cryptocurrencies'),
                    'mcap_change_24h': d.get('market_cap_change_percentage_24h_usd')}
    except: return {}

# ============================================================
# COMPUTE FUNCTIONS
# ============================================================
def compute_changes(pts):
    if not pts or len(pts) < 1:
        return {'current': None}
    c = pts[0]['value']
    r = {'current': c, 'date': pts[0]['date']}
    if len(pts) >= 2:
        p = pts[1]['value']
        r['prev'] = p
        r['change'] = round(c - p, 4)
        r['pct_change'] = round((c - p) / abs(p) * 100, 2) if p != 0 else 0
    if len(pts) >= 6:
        w = pts[5]['value']
        r['week_pct'] = round((c - w) / abs(w) * 100, 2) if w else 0
    if len(pts) >= 23:
        m = pts[22]['value']
        r['month_pct'] = round((c - m) / abs(m) * 100, 2) if m else 0
    if len(pts) >= 66:
        q = pts[65]['value']
        r['quarter_pct'] = round((c - q) / abs(q) * 100, 2) if q else 0
    vals = [d['value'] for d in pts]
    r['high'] = max(vals); r['low'] = min(vals); r['avg'] = round(sum(vals)/len(vals), 4)
    return r

def compute_stock(bars):
    if not bars or len(bars) < 2: return None
    c, p = bars[0], bars[1]
    r = {'price':c['c'],'date':c['date'],'open':c['o'],'high':c['h'],'low':c['l'],
         'volume':c['v'],'day_change':round(c['c']-p['c'],2),
         'day_pct':round((c['c']-p['c'])/p['c']*100,2) if p['c'] else 0}
    if len(bars)>=6: r['week_pct']=round((c['c']-bars[5]['c'])/bars[5]['c']*100,2) if bars[5]['c'] else 0
    if len(bars)>=22: r['month_pct']=round((c['c']-bars[21]['c'])/bars[21]['c']*100,2) if bars[21]['c'] else 0
    if len(bars)>=66: r['quarter_pct']=round((c['c']-bars[65]['c'])/bars[65]['c']*100,2) if bars[65]['c'] else 0
    for b in bars:
        if b['date'][:4]!=c['date'][:4]:
            r['ytd_pct']=round((c['c']-b['c'])/b['c']*100,2) if b['c'] else 0; break
    closes=[b['c'] for b in bars]
    if len(closes)>=20: r['sma20']=round(sum(closes[:20])/20,2)
    if len(closes)>=50: r['sma50']=round(sum(closes[:50])/50,2)
    if len(closes)>=200: r['sma200']=round(sum(closes[:200])/200,2)
    r['w52_high']=max(b['h'] for b in bars[:250])
    r['w52_low']=min(b['l'] for b in bars[:250])
    # RSI 14
    if len(closes) >= 15:
        gains, losses = [], []
        for i in range(1, 15):
            d = closes[i-1] - closes[i]
            if d > 0: gains.append(d); losses.append(0)
            else: gains.append(0); losses.append(abs(d))
        ag = sum(gains)/14; al = sum(losses)/14
        r['rsi14'] = round(100 - (100/(1+ag/al)),1) if al else 100
    return r

# ============================================================
# KHALID INDEX V10 (10 components)
# ============================================================
def compute_ki(fd, sd):
    score = 50; signals = []
    def gv(cat, sid):
        d = fd.get(cat, {}).get(sid, {})
        return d.get('current')

    # 1. DXY
    v = gv('dxy','DTWEXBGS')
    if v:
        s = -12 if v>115 else -8 if v>110 else -3 if v>105 else 5 if v<95 else 0
        if s: score+=s; signals.append(('DXY',s,f'{v:.1f}'))

    # 2. HY Spread
    v = gv('ice_bofa','BAMLH0A0HYM2')
    if v:
        s = -15 if v>6 else -10 if v>5 else -5 if v>4 else 5 if v<3 else 0
        if s: score+=s; signals.append(('HY Spread',s,f'{v:.2f}%'))

    # 3. Yield Curve
    v = gv('treasury','T10Y2Y')
    if v is not None:
        s = -10 if v<-0.5 else -5 if v<0 else 5 if v>1 else 0
        if s: score+=s; signals.append(('Yield Curve',s,f'{v:.2f}%'))

    # 4. VIX
    v = gv('risk','VIXCLS')
    if v:
        s = -12 if v>35 else -6 if v>25 else 5 if v<15 else 0
        if s: score+=s; signals.append(('VIX',s,f'{v:.1f}'))

    # 5. NFCI
    v = gv('risk','NFCI')
    if v is not None:
        s = -10 if v>0.5 else -3 if v>0 else 5 if v<-0.5 else 0
        if s: score+=s; signals.append(('NFCI',s,f'{v:.2f}'))

    # 6. Fed Balance Sheet
    d = fd.get('liquidity',{}).get('WALCL',{})
    if d.get('pct_change') is not None:
        v = d['pct_change']
        s = -5 if v<-1 else 5 if v>1 else 0
        if s: score+=s; signals.append(('Fed BS',s,f'{v:.1f}%'))

    # 7. Unemployment
    d = fd.get('macro',{}).get('UNRATE',{})
    if d.get('month_pct') is not None:
        v = d['month_pct']
        s = -8 if v>5 else -3 if v>0 else 3 if v<-2 else 0
        if s: score+=s; signals.append(('Unemployment',s,f"{d['current']:.1f}%"))

    # 8. PMI
    v = gv('global_cycle','MANEMP')
    if v:
        s = 5 if v>55 else 3 if v>52 else -3 if v<48 else -5 if v<45 else 0
        if s: score+=s; signals.append(('ISM Mfg',s,f'{v:.1f}'))

    # 9. Net Liquidity
    fed_a = gv('liquidity','WALCL'); tga = gv('liquidity','WTREGEN'); rrp = gv('liquidity','RRPONTSYD')
    if fed_a and tga and rrp:
        rrp_adj = rrp * 1000 if rrp < 10000 else rrp
        nl = (fed_a - tga - rrp_adj) / 1e6
        s = 3 if nl > 5.5 else -3 if nl < 4.5 else 0
        if s: score+=s; signals.append(('Net Liq',s,f'${nl:.2f}T'))

    # 10. SPY Trend
    spy = sd.get('SPY',{})
    if spy.get('sma50') and spy.get('sma200'):
        if spy['price']>spy['sma50']>spy['sma200']: s=5
        elif spy['price']<spy['sma50']<spy['sma200']: s=-5
        else: s=0
        if s: score+=s; signals.append(('SPY Trend',s,f"${spy['price']:.0f}"))

    score = max(0, min(100, score))
    regime = 'STRONG_BULL' if score>=75 else 'BULL' if score>=60 else 'NEUTRAL' if score>=45 else 'BEAR' if score>=30 else 'CRISIS'
    return {'score':score,'regime':regime,'signals':signals,'ts':datetime.utcnow().isoformat()}

def compute_risk(fd):
    r = {}
    # Credit risk
    hy = fd.get('ice_bofa',{}).get('BAMLH0A0HYM2',{}).get('current')
    r['credit'] = (80 if hy<3 else 60 if hy<4 else 40 if hy<5 else 20 if hy<6 else 10) if hy else 50

    # Liquidity risk
    fed_chg = fd.get('liquidity',{}).get('WALCL',{}).get('pct_change')
    r['liquidity'] = max(0,min(100, 50 + (15 if (fed_chg or 0)>0 else -15 if (fed_chg or 0)<0 else 0)))

    # Market risk
    vix = fd.get('risk',{}).get('VIXCLS',{}).get('current')
    r['market'] = (85 if vix<15 else 65 if vix<20 else 45 if vix<25 else 25 if vix<30 else 10) if vix else 50

    # Recession
    curve = fd.get('treasury',{}).get('T10Y2Y',{}).get('current')
    r['recession'] = (15 if curve<-0.5 else 30 if curve<0 else 50 if curve<0.5 else 70) if curve is not None else 50

    # Systemic
    nfci = fd.get('risk',{}).get('NFCI',{}).get('current')
    r['systemic'] = max(0,min(100, 50 + (20 if (nfci or 0)<-0.5 else -25 if (nfci or 0)>0.5 else 0)))

    # Inflation
    cpi = fd.get('inflation',{}).get('CPALTT01USM657N',{}).get('current')
    r['inflation'] = (10 if cpi>6 else 30 if cpi>4 else 45 if cpi>3 else 70 if cpi>2 else 85) if cpi else 50

    scores = list(r.values())
    r['composite'] = round(sum(scores)/len(scores)) if scores else 50
    return r

def compute_net_liq(fd):
    fed = fd.get('liquidity',{}).get('WALCL',{}).get('current')
    tga = fd.get('liquidity',{}).get('WTREGEN',{}).get('current')
    rrp = fd.get('liquidity',{}).get('RRPONTSYD',{}).get('current')
    if fed and tga and rrp:
        rrp_adj = rrp * 1000 if rrp < 10000 else rrp
        return {'net': round(fed-tga-rrp_adj), 'fed': round(fed), 'tga': round(tga), 'rrp': round(rrp)}
    return {}

# ============================================================
# AI ANALYSIS ENGINE
# ============================================================
def ai_analysis(fd, sd, crypto, ki, risk, nl):
    a = {'generated_at': datetime.utcnow().isoformat()+'Z', 'sections': {}}
    def gv(cat,sid): return fd.get(cat,{}).get(sid,{}).get('current')

    # ── MACRO ──
    unemp=gv('macro','UNRATE'); gdp=gv('macro','A191RL1Q225SBEA'); sent=gv('macro','UMCSENT')
    ms = []
    if unemp:
        ms.append(f"{'Tight' if unemp<4 else 'Normalizing' if unemp<5 else 'Weak'} labor market at {unemp:.1f}% unemployment{'.' if unemp<5 else '. Recession risk elevated.'}")
    if gdp:
        ms.append(f"GDP {'expanding' if gdp>1 else 'stalling' if gdp>0 else 'contracting'} at {gdp:.1f}%.")
    if sent:
        ms.append(f"Consumer sentiment {'depressed' if sent<60 else 'moderate' if sent<80 else 'strong'} at {sent:.0f}.")
    claims = gv('macro','ICSA')
    if claims: ms.append(f"Initial claims at {claims:.0f}K - {'healthy' if claims<250 else 'elevated' if claims<350 else 'recessionary'} level.")
    a['sections']['macro'] = {'title':'Macro Economy','outlook':'EXPANSION' if (gdp or 2)>1 and (unemp or 4)<5 else 'SLOWDOWN' if (gdp or 2)>0 else 'CONTRACTION','signals':ms}

    # ── LIQUIDITY ──
    ls = []
    fed_a = gv('liquidity','WALCL'); tga = gv('liquidity','WTREGEN'); rrp = gv('liquidity','RRPONTSYD')
    fed_chg = fd.get('liquidity',{}).get('WALCL',{}).get('pct_change')
    if fed_a: ls.append(f"Fed balance sheet ${fed_a/1e6:.2f}T. {'QT ongoing.' if (fed_chg or 0)<0 else 'Expanding.'}")
    if tga: ls.append(f"TGA ${tga/1e6:.2f}T - {'draining reserves' if tga>700000 else 'injecting reserves'}.")
    if rrp: ls.append(f"RRP ${rrp:.0f}B - {'buffer available' if rrp>200 else 'nearly drained'}.")
    if nl.get('net'): ls.append(f"Net Liquidity ${nl['net']/1e6:.2f}T. {'Rising = bullish risk assets.' if (fed_chg or 0)>0 else 'Declining = headwind.'}")
    sofr = gv('liquidity','SOFR')
    if sofr: ls.append(f"SOFR at {sofr:.2f}% - {'restrictive' if sofr>4 else 'neutral' if sofr>2 else 'accommodative'}.")
    a['sections']['liquidity'] = {'title':'Liquidity','outlook':'EASING' if (fed_chg or 0)>0 else 'TIGHTENING','signals':ls}

    # ── RISK ──
    rs = []
    vix = gv('risk','VIXCLS'); hy = gv('ice_bofa','BAMLH0A0HYM2'); curve = gv('treasury','T10Y2Y')
    if vix:
        if vix>30: rs.append(f"VIX {vix:.1f}: EXTREME FEAR. Hedge costs high. Defensive positioning recommended.")
        elif vix>20: rs.append(f"VIX {vix:.1f}: Elevated concern. Markets pricing uncertainty.")
        elif vix<15: rs.append(f"VIX {vix:.1f}: COMPLACENT. Low vol precedes corrections. Buy protection cheap.")
        else: rs.append(f"VIX {vix:.1f}: Normal range.")
    if hy:
        if hy>5: rs.append(f"HY spread {hy:.2f}%: STRESS. Avoid HY bonds and leveraged companies.")
        elif hy<3: rs.append(f"HY spread {hy:.2f}%: Very tight. Poor risk/reward for HY. Favor IG.")
        else: rs.append(f"HY spread {hy:.2f}%: Normal. Credit stable.")
    if curve is not None:
        if curve<0: rs.append(f"Yield curve INVERTED at {curve:.2f}%. Recession signal active.")
        elif curve<0.5: rs.append(f"Yield curve flat at {curve:.2f}%. Transitional period.")
        else: rs.append(f"Yield curve positive {curve:.2f}%. Normal expansion signal.")
    nfci_v = gv('risk','NFCI')
    if nfci_v is not None: rs.append(f"NFCI at {nfci_v:.2f}: {'Tight conditions, stress.' if nfci_v>0 else 'Loose conditions, supportive.'}")
    mort = gv('risk','MORTGAGE30US')
    if mort: rs.append(f"30Y mortgage at {mort:.2f}% - {'constraining housing' if mort>6.5 else 'moderating' if mort>5.5 else 'supportive of housing'}.")
    a['sections']['risk'] = {'title':'Risk Assessment','outlook':'HIGH_RISK' if risk.get('composite',50)<40 else 'MODERATE' if risk.get('composite',50)<60 else 'LOW_RISK','signals':rs}

    # ── DOLLAR ──
    ds = []
    dxy = gv('dxy','DTWEXBGS')
    if dxy:
        if dxy>115: ds.append(f"USD Index {dxy:.1f}: EXTREMELY STRONG. Major headwind for EM, commodities, gold, US multinationals. Underweight international.")
        elif dxy>105: ds.append(f"USD Index {dxy:.1f}: Moderately strong. Selective headwind for commodity sectors and EM.")
        elif dxy<95: ds.append(f"USD Index {dxy:.1f}: Weak dollar. Tailwind for gold, EM, commodities, US exporters.")
        else: ds.append(f"USD Index {dxy:.1f}: Neutral range.")
    eur = gv('dxy','DEXUSEU')
    if eur: ds.append(f"EUR/USD at {eur:.4f}.")
    jpy = gv('dxy','DEXJPUS')
    if jpy: ds.append(f"USD/JPY at {jpy:.2f}.")
    a['sections']['dollar'] = {'title':'Dollar Analysis','signals':ds}

    # ── INFLATION ──
    ins = []
    cpi_yoy = gv('inflation','CPALTT01USM657N'); pce = gv('macro','PCEPILFE')
    be10 = gv('treasury','T10YIE'); mich = gv('inflation','MICH')
    if cpi_yoy: ins.append(f"CPI YoY at {cpi_yoy:.1f}%: {'Above target, restrictive policy likely.' if cpi_yoy>3 else 'Near target.' if cpi_yoy>1.5 else 'Below target, deflationary risk.'}")
    if pce: ins.append(f"Core PCE (Fed preferred) at {pce:.1f}.")
    if be10: ins.append(f"10Y breakeven inflation at {be10:.2f}% - market expects this inflation over next decade.")
    if mich: ins.append(f"Michigan inflation expectations at {mich:.1f}%.")
    a['sections']['inflation'] = {'title':'Inflation Monitor','signals':ins}

    # ── CRYPTO ANALYSIS ──
    cs = []
    btc = crypto.get('BTC',{}); eth = crypto.get('ETH',{}); sol = crypto.get('SOL',{})
    if btc.get('price'):
        cs.append(f"Bitcoin ${btc['price']:,.0f} (7d: {btc.get('change_7d',0):+.1f}%, 30d: {btc.get('change_30d',0):+.1f}%). {'Strong momentum.' if (btc.get('change_7d') or 0)>5 else 'Weak momentum.' if (btc.get('change_7d') or 0)<-5 else 'Consolidating.'}")
        if btc.get('ath'): cs.append(f"BTC is {btc.get('ath_pct',0):.1f}% from ATH of ${btc['ath']:,.0f}.")
    if eth.get('price'): cs.append(f"Ethereum ${eth['price']:,.0f} (7d: {eth.get('change_7d',0):+.1f}%).")
    if sol.get('price'): cs.append(f"Solana ${sol['price']:,.2f} (7d: {sol.get('change_7d',0):+.1f}%).")
    if (fed_chg or 0)>0: cs.append("Fed easing is historically bullish for crypto. Consider adding on dips.")
    elif (fed_chg or 0)<-1: cs.append("Fed tightening is headwind for speculative assets including crypto.")
    if vix and vix>30: cs.append("High VIX: risk-off. Crypto correlates with equities in stress.")
    a['sections']['crypto'] = {'title':'Crypto Analysis','signals':cs}

    # ── PORTFOLIO SUGGESTIONS ──
    port = {}

    # GOLD
    gr = []; ga = 'HOLD'
    real_y = gv('treasury','DFII10')
    if dxy and dxy>110: gr.append("Strong USD headwind for gold."); ga='UNDERWEIGHT'
    elif dxy and dxy<100: gr.append("Weak USD supports gold."); ga='OVERWEIGHT'
    if real_y is not None:
        if real_y>2: gr.append(f"Real yields {real_y:.2f}% high - reduces gold appeal vs TIPS.")
        elif real_y<0: gr.append(f"Negative real yields {real_y:.2f}% strongly support gold.")
    if risk.get('composite',50)<40: gr.append("Elevated risk supports safe haven gold 10-15% allocation."); ga='OVERWEIGHT'
    gld = sd.get('GLD',{})
    port['gold'] = {'action':ga,'reasons':gr,'vehicles':['GLD','IAU','SGOL','Physical Gold'],
        'price':gld.get('price'),'trend':'ABOVE SMA50' if gld.get('sma50') and gld.get('price',0)>gld['sma50'] else 'BELOW SMA50'}

    # CRYPTO
    cr = []; ca = 'HOLD'
    if btc.get('change_7d',0)>10: cr.append(f"BTC +{btc['change_7d']:.1f}% weekly. Strong momentum but watch overextension.")
    elif btc.get('change_7d',0)<-10: cr.append(f"BTC {btc['change_7d']:.1f}% weekly. Potential buying opportunity.")
    if (fed_chg or 0)>0: cr.append("Fed easing bullish for crypto."); ca='OVERWEIGHT'
    elif (fed_chg or 0)<-1: cr.append("Fed tightening headwind."); ca='UNDERWEIGHT'
    if vix and vix>30: cr.append("High VIX = risk-off for crypto."); ca='UNDERWEIGHT'
    port['crypto'] = {'action':ca,'reasons':cr,'top_picks':['BTC - Store of value','ETH - Smart contracts + staking','SOL - High performance L1'],
        'btc_price':btc.get('price'),'eth_price':eth.get('price')}

    # STOCKS
    sr = []; sa = 'NEUTRAL'
    spy = sd.get('SPY',{})
    if spy.get('sma50') and spy.get('sma200'):
        if spy['price']>spy['sma50']>spy['sma200']:
            sr.append(f"SPY bullish: ${spy['price']:.0f} > SMA50 ${spy['sma50']:.0f} > SMA200 ${spy['sma200']:.0f}. Favor equities."); sa='OVERWEIGHT'
        elif spy['price']<spy['sma50']<spy['sma200']:
            sr.append("SPY bearish trend. Reduce equity exposure."); sa='UNDERWEIGHT'
    sector_map = {'XLF':'Financials','XLE':'Energy','XLK':'Technology','XLV':'Healthcare','XLI':'Industrials',
                  'XLU':'Utilities','XLP':'Staples','XLY':'Discretionary','XLB':'Materials','XLC':'Comms','XLRE':'Real Estate'}
    ranked = sorted([(k,sd.get(k,{}).get('month_pct',0)) for k in sector_map if sd.get(k)], key=lambda x:x[1], reverse=True)
    best = [(sector_map[k],p) for k,p in ranked[:3]] if ranked else []
    worst = [(sector_map[k],p) for k,p in ranked[-3:]] if ranked else []
    if best: sr.append(f"Leading sectors: {', '.join(f'{n} ({p:+.1f}%)' for n,p in best)}")
    if worst: sr.append(f"Lagging sectors: {', '.join(f'{n} ({p:+.1f}%)' for n,p in worst)}")
    port['stocks'] = {'action':sa,'reasons':sr,'overweight':[n for n,_ in best],'underweight':[n for n,_ in worst],
        'spy_price':spy.get('price'),'trend':sa}

    # BONDS
    br = []; ba = 'NEUTRAL'
    t10 = gv('treasury','DGS10'); t2 = gv('treasury','DGS2')
    if t10:
        if t10>4.5: br.append(f"10Y at {t10:.2f}%: Attractive entry for duration. Lock in yields."); ba='OVERWEIGHT_DURATION'
        elif t10<3: br.append(f"10Y at {t10:.2f}%: Limited income. Underweight duration.")
    if t2 and t10: br.append(f"2Y={t2:.2f}% vs 10Y={t10:.2f}%. {'Inverted - favor short duration.' if (curve or 0)<0 else 'Normal - duration compensated.'}")
    if hy and hy<3.5: br.append(f"HY spread tight at {hy:.2f}%. Favor IG over HY."); ba='FAVOR_IG'
    elif hy and hy>5: br.append(f"HY spread wide at {hy:.2f}%. Potential value for risk-tolerant.")
    port['bonds'] = {'action':ba,'reasons':br,
        'vehicles':{'short':'SHY,VCSH,BIL','mid':'IEF,GOVT,AGG','long':'TLT,VCLT,EDV','hy':'HYG,JNK','ig':'LQD,VCIT','tips':'TIP,VTIP','em':'EMB,VWOB'},
        't10':t10,'t2':t2}

    # PORTFOLIO CONSTRUCTION
    ks = ki['score']
    if ks>=70:
        con = {'regime':'RISK-ON','alloc':{'US Equities':45,'Intl Equities':15,'Bonds':15,'Gold':5,'Crypto':10,'Cash':5,'Commodities':5},
            'rationale':'Strong bull. Overweight equities + risk assets. Crypto justified by easing.'}
    elif ks>=50:
        con = {'regime':'BALANCED','alloc':{'US Equities':35,'Intl Equities':10,'Bonds':25,'Gold':10,'Crypto':5,'Cash':10,'Commodities':5},
            'rationale':'Neutral. Balanced diversification. Moderate risk.'}
    elif ks>=30:
        con = {'regime':'DEFENSIVE','alloc':{'US Equities':20,'Intl Equities':5,'Bonds':30,'Gold':15,'Crypto':3,'Cash':20,'Commodities':7},
            'rationale':'Bear regime. Defensive with elevated cash + gold. Short-duration bonds.'}
    else:
        con = {'regime':'CRISIS','alloc':{'US Equities':10,'Intl Equities':0,'Bonds':25,'Gold':20,'Crypto':0,'Cash':35,'Commodities':10},
            'rationale':'Crisis. Capital preservation. Maximum cash + gold + short treasuries.'}

    moves = []
    if sa=='OVERWEIGHT': moves.append({'act':'BUY','asset':'SPY/QQQ','why':'Bullish equity trend'})
    if sa=='UNDERWEIGHT': moves.append({'act':'REDUCE','asset':'Equities','why':'Bearish trend, raise cash'})
    if ga=='OVERWEIGHT': moves.append({'act':'ADD','asset':'GLD/IAU','why':'Safe haven demand elevated'})
    if ca=='OVERWEIGHT': moves.append({'act':'ADD','asset':'BTC/ETH','why':'Easing supports digital assets'})
    if ba=='FAVOR_IG': moves.append({'act':'ROTATE','asset':'HYG to LQD','why':'Tight HY spreads, favor IG'})
    if t10 and t10>4.5: moves.append({'act':'ADD','asset':'TLT/IEF','why':f'Lock {t10:.2f}% near cycle highs'})
    if vix and vix<15: moves.append({'act':'BUY','asset':'VIX Hedge','why':'Cheap insurance in low vol'})
    if best: moves.append({'act':'OVERWEIGHT','asset':best[0][0],'why':f'Sector leader {best[0][1]:+.1f}%'})
    if worst: moves.append({'act':'UNDERWEIGHT','asset':worst[-1][0],'why':f'Sector laggard {worst[-1][1]:+.1f}%'})
    con['moves'] = moves

    port['construction'] = con
    a['portfolio'] = port
    return a

# ============================================================
# MAIN HANDLER
# ============================================================
def lambda_handler(event, context):
    t0 = time.time()
    print(f"[V10] Start {datetime.utcnow().isoformat()}")

    # ── PHASE 1: FRED (batched 8 at a time, 5 workers, 2.5s gap, retry on 429) ──
    fred_raw = {}
    all_sids = list(FRED_SERIES.keys())
    batch_sz = 8
    for i in range(0, len(all_sids), batch_sz):
        batch = all_sids[i:i+batch_sz]
        with ThreadPoolExecutor(max_workers=4) as ex:
            fm = {ex.submit(fetch_fred, sid): sid for sid in batch}
            for f in as_completed(fm):
                sid = fm[f]
                try:
                    d = f.result()
                    if d: fred_raw[sid] = d
                except: pass
        if i + batch_sz < len(all_sids):
            time.sleep(2.5)
        if (i // batch_sz) % 5 == 0:
            print(f"  FRED batch {i//batch_sz+1}: {len(fred_raw)} series")

    print(f"[V10] FRED: {len(fred_raw)}/{len(all_sids)} in {time.time()-t0:.1f}s")

    # Process into categories
    fd = {}
    for sid, (cat, name) in FRED_SERIES.items():
        if cat not in fd: fd[cat] = {}
        raw = fred_raw.get(sid, [])
        m = compute_changes(raw)
        m['name'] = name; m['series_id'] = sid; m['history'] = raw[:60]
        fd[cat][sid] = m

    # ── PHASE 2: STOCKS (batched) ──
    print(f"[V10] Fetching {len(STOCK_TICKERS)} stocks...")
    sd = {}
    for i in range(0, len(STOCK_TICKERS), 10):
        batch = STOCK_TICKERS[i:i+10]
        with ThreadPoolExecutor(max_workers=5) as ex:
            fm = {ex.submit(fetch_polygon, t): t for t in batch}
            for f in as_completed(fm):
                t = fm[f]
                try:
                    bars = f.result()
                    if bars:
                        m = compute_stock(bars)
                        if m:
                            m['history'] = [{'d':b['date'],'c':b['c']} for b in bars[:120]]
                            sd[t] = m
                except: pass
        time.sleep(0.3)
    print(f"[V10] Stocks: {len(sd)}")

    # ── PHASE 3: CRYPTO ──
    print("[V10] Crypto...")
    crypto = fetch_crypto()
    crypto_g = fetch_crypto_global()
    print(f"[V10] Crypto: {len(crypto)} coins")

    # ── PHASE 4: ANALYTICS ──
    ki = compute_ki(fd, sd)
    risk = compute_risk(fd)
    nl = compute_net_liq(fd)
    sectors = {}
    sn = {'XLF':'Financials','XLE':'Energy','XLK':'Technology','XLV':'Healthcare','XLI':'Industrials',
          'XLU':'Utilities','XLP':'Staples','XLY':'Discretionary','XLB':'Materials','XLC':'Comms','XLRE':'Real Estate'}
    for etf, name in sn.items():
        if etf in sd:
            s = sd[etf]
            sectors[etf] = {'name':name,'price':s['price'],'day_pct':s.get('day_pct',0),
                'week_pct':s.get('week_pct',0),'month_pct':s.get('month_pct',0),'quarter_pct':s.get('quarter_pct',0)}

    sigs = {'buys':[],'sells':[],'warnings':[]}
    for t,s in sd.items():
        if not s.get('sma50') or not s.get('sma200'): continue
        if s['price']>s['sma50']>s['sma200'] and s.get('day_pct',0)>0: sigs['buys'].append(t)
        elif s['price']<s['sma50']<s['sma200']: sigs['sells'].append(t)
        if s.get('day_pct',0)<-3: sigs['warnings'].append(f"{t} {s['day_pct']:.1f}%")

    # ── PHASE 5: AI ANALYSIS ──
    print("[V10] AI Analysis...")
    ai = ai_analysis(fd, sd, crypto, ki, risk, nl)

    # ── PHASE 6: PUBLISH ──
    report = {
        'version':'V10','generated_at':datetime.utcnow().isoformat()+'Z',
        'fetch_time_seconds':round(time.time()-t0,1),
        'khalid_index':ki,'risk_dashboard':risk,'net_liquidity':nl,
        'sectors':sectors,'signals':sigs,
        'fred':fd,'stocks':sd,'crypto':crypto,'crypto_global':crypto_g,
        'ai_analysis':ai,
        'stats':{'fred':len(fred_raw),'stocks':len(sd),'crypto':len(crypto),
                 'data_points':sum(len(v) for v in fred_raw.values())+sum(len(s.get('history',[])) for s in sd.values())}
    }

    try:
        rj = json.dumps(report, default=str)
        s3.put_object(Bucket=S3_BUCKET, Key='data/report.json', Body=rj, ContentType='application/json', CacheControl='max-age=60')
        ts = datetime.utcnow().strftime('%Y%m%d_%H%M')
        s3.put_object(Bucket=S3_BUCKET, Key=f'data/archive/report_{ts}.json', Body=rj, ContentType='application/json')

        elapsed = round(time.time()-t0,1)
        summary = {'status':'published','ki':ki['score'],'regime':ki['regime'],
                    'fred':len(fred_raw),'stocks':len(sd),'crypto':len(crypto),
                    'risk_composite':risk.get('composite',0),'fetch_time':elapsed,
                    'dxy':fd.get('dxy',{}).get('DTWEXBGS',{}).get('current'),
                    'hy_spread':fd.get('ice_bofa',{}).get('BAMLH0A0HYM2',{}).get('current'),
                    'vix':fd.get('risk',{}).get('VIXCLS',{}).get('current')}
        print(f"[V10] DONE {elapsed}s: {json.dumps(summary)}")
        return {'statusCode':200,'body':json.dumps(summary)}
    except Exception as e:
        print(f"[V10] Error: {e}")
        return {'statusCode':500,'body':json.dumps({'error':str(e)})}
