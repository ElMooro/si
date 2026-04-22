
"""
JUSTHODL FINANCIAL SECRETARY v1.0
Self-Hosted AI Financial Analyst & Personal Portfolio Secretary
AWS: 857687956942 | Region: us-east-1
"""
import json,boto3,os,ssl,time,math,statistics,traceback,hashlib
from datetime import datetime,timezone,timedelta
from urllib import request as urllib_request
from urllib.parse import urlencode,quote
from concurrent.futures import ThreadPoolExecutor,as_completed
from collections import defaultdict

FRED_KEY=os.environ.get('FRED_API_KEY','')
POLY_KEY=os.environ.get('POLYGON_API_KEY','')
AV_KEY=os.environ.get('ALPHAVANTAGE_KEY','')
NEWS_KEY=os.environ.get('NEWS_API_KEY','')
CMC_KEY=os.environ.get('CMC_API_KEY','')
ANTHROPIC_KEY=os.environ.get('ANTHROPIC_API_KEY','')
BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')
EMAIL_TO=os.environ.get('EMAIL_TO','raafouis@gmail.com')
EMAIL_FROM=os.environ.get('EMAIL_FROM','raafouis@gmail.com')

s3=boto3.client('s3')
ses=boto3.client('ses',region_name='us-east-1')
ctx=ssl.create_default_context()
ctx.check_hostname=False
ctx.verify_mode=ssl.CERT_NONE

def http_get(url,headers=None,timeout=15):
    try:
        req=urllib_request.Request(url,headers=headers or {})
        with urllib_request.urlopen(req,timeout=timeout,context=ctx) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"HTTP ERR: {url[:80]} -> {e}")
        return None

def http_post(url,data,headers=None,timeout=30):
    try:
        body=json.dumps(data).encode()
        req=urllib_request.Request(url,data=body,headers=headers or {'Content-Type':'application/json'})
        with urllib_request.urlopen(req,timeout=timeout,context=ctx) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"HTTP POST ERR: {url[:80]} -> {e}")
        return None

# ═══ FRED ═══
FRED_SERIES={
    'WALCL':'Fed Balance Sheet','RRPONTSYD':'Reverse Repo','WTREGEN':'TGA','WRESBAL':'Bank Reserves',
    'SOFR':'SOFR Rate','FEDFUNDS':'Fed Funds Rate','DGS2':'2Y Treasury','DGS10':'10Y Treasury',
    'DGS30':'30Y Treasury','T10Y2Y':'2s10s Spread','T10Y3M':'3m10Y Spread',
    'T5YIE':'5Y Breakeven','T10YIE':'10Y Breakeven',
    'BAMLH0A0HYM2':'HY Spread','BAMLC0A0CM':'IG Spread','BAMLC0A4CBBB':'BBB Spread',
    'VIXCLS':'VIX','STLFSI2':'St Louis Stress','NFCI':'Chicago FinCond',
    'UNRATE':'Unemployment','CPIAUCSL':'CPI','CPILFESL':'Core CPI','GDPC1':'Real GDP',
    'DTWEXBGS':'Dollar Index','DCOILWTICO':'WTI Crude',
}

def fetch_fred():
    results={}
    def _get(sid,nm):
        url=f"https://api.stlouisfed.org/fred/series/observations?series_id={sid}&api_key={FRED_KEY}&file_type=json&sort_order=desc&limit=60"
        d=http_get(url)
        if d and 'observations' in d:
            obs=[o for o in d['observations'] if o.get('value','.')!='.']
            if obs:
                val=float(obs[0]['value']);prev=float(obs[1]['value']) if len(obs)>1 else val
                prev_1m=float(obs[min(22,len(obs)-1)]['value']) if len(obs)>22 else val
                results[sid]={'name':nm,'value':val,'prev':prev,'chg_1d':round(val-prev,4),'chg_1m':round(val-prev_1m,4),'date':obs[0]['date'],'history':[float(o['value']) for o in obs[:30] if o.get('value','.')!='.']}
    with ThreadPoolExecutor(max_workers=10) as ex:
        futs={ex.submit(_get,sid,nm):sid for sid,nm in FRED_SERIES.items()}
        for f in as_completed(futs):f.result()
    return results

# ═══ POLYGON ═══
UNIVERSE={
    'AAPL':'Apple','MSFT':'Microsoft','GOOGL':'Alphabet','AMZN':'Amazon','NVDA':'NVIDIA',
    'META':'Meta','TSLA':'Tesla','BRK.B':'Berkshire','JPM':'JPMorgan','V':'Visa',
    'UNH':'UnitedHealth','MA':'Mastercard','HD':'Home Depot','PG':'Procter Gamble',
    'JNJ':'Johnson Johnson','COST':'Costco','ABBV':'AbbVie','CRM':'Salesforce',
    'NFLX':'Netflix','AMD':'AMD','LLY':'Eli Lilly','AVGO':'Broadcom','ORCL':'Oracle',
    'PLTR':'Palantir','COIN':'Coinbase','MSTR':'MicroStrategy','SMCI':'Super Micro',
    'ARM':'ARM Holdings','SNOW':'Snowflake','NET':'Cloudflare','CRWD':'CrowdStrike','PANW':'Palo Alto',
    'SPY':'S&P 500','QQQ':'Nasdaq 100','DIA':'Dow Jones','IWM':'Russell 2000',
    'VTI':'Total Market','EFA':'Intl Developed','EEM':'Emerging Markets',
    'XLK':'Tech','XLF':'Financials','XLE':'Energy','XLV':'Healthcare',
    'XLI':'Industrials','XLU':'Utilities','XLP':'Staples','XLY':'Discretionary',
    'XLB':'Materials','XLRE':'Real Estate',
    'TLT':'Long Treasury','IEF':'Mid Treasury','SHY':'Short Treasury',
    'LQD':'IG Corporate','HYG':'HY Corporate','TIP':'TIPS',
    'GLD':'Gold','SLV':'Silver','GDX':'Gold Miners','USO':'Oil',
    'UNG':'Natural Gas','DBA':'Agriculture','PPLT':'Platinum',
    'IBIT':'Bitcoin ETF','ETHA':'Ether ETF',
    'ARKK':'ARK Innovation','KWEB':'China Tech','XBI':'Biotech','HACK':'Cybersecurity','BOTZ':'Robotics AI',
}

ETF_TICKERS={'SPY','QQQ','DIA','IWM','VTI','EFA','EEM','XLK','XLF','XLE','XLV','XLI','XLU','XLP','XLY','XLB','XLRE','TLT','IEF','SHY','LQD','HYG','TIP','GLD','SLV','GDX','USO','UNG','DBA','PPLT','IBIT','ETHA','ARKK','KWEB','XBI','HACK','BOTZ'}

def fetch_polygon_prices():
    results={}
    tickers=list(UNIVERSE.keys())
    def _batch(batch):
        t_str=','.join(batch)
        url=f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers?tickers={t_str}&apiKey={POLY_KEY}"
        d=http_get(url,timeout=20)
        if d and 'tickers' in d:
            for t in d['tickers']:
                tk=t.get('ticker','');day=t.get('day',{});prev=t.get('prevDay',{})
                results[tk]={'name':UNIVERSE.get(tk,tk),'price':day.get('c',0) or t.get('lastTrade',{}).get('p',0) or prev.get('c',0),'open':day.get('o',0),'high':day.get('h',0),'low':day.get('l',0),'volume':day.get('v',0),'prev_close':prev.get('c',0),'change_pct':round(((day.get('c',0)/prev.get('c',1))-1)*100,2) if prev.get('c',0) and day.get('c',0) else 0}
    for i in range(0,len(tickers),30):_batch(tickers[i:i+30])
    return results

def fetch_historical(ticker,days=365):
    end=datetime.now();start=end-timedelta(days=days)
    url=f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}?adjusted=true&sort=asc&apiKey={POLY_KEY}"
    d=http_get(url,timeout=20)
    if d and 'results' in d:
        return [{'date':datetime.fromtimestamp(bar['t']/1000).strftime('%Y-%m-%d'),'open':bar['o'],'high':bar['h'],'low':bar['l'],'close':bar['c'],'volume':bar.get('v',0)} for bar in d['results']]
    return []

def fetch_financials(ticker,limit=4):
    url=f"https://api.polygon.io/vX/reference/financials?ticker={ticker}&limit={limit}&apiKey={POLY_KEY}"
    d=http_get(url,timeout=20)
    if not d or 'results' not in d:return {'error':f'No financials for {ticker}'}
    income_statements=[];balance_sheets=[]
    for filing in d['results']:
        period=filing.get('fiscal_period','');year=filing.get('fiscal_year','')
        inc=filing.get('financials',{}).get('income_statement',{})
        bal=filing.get('financials',{}).get('balance_sheet',{})
        if inc:
            income_statements.append({'period':f"{period} {year}",'revenue':inc.get('revenues',{}).get('value',0),'cost_of_revenue':inc.get('cost_of_revenue',{}).get('value',0),'gross_profit':inc.get('gross_profit',{}).get('value',0),'operating_income':inc.get('operating_income_loss',{}).get('value',0),'net_income':inc.get('net_income_loss',{}).get('value',0),'eps_basic':inc.get('basic_earnings_per_share',{}).get('value',0),'eps_diluted':inc.get('diluted_earnings_per_share',{}).get('value',0)})
        if bal:
            balance_sheets.append({'period':f"{period} {year}",'total_assets':bal.get('assets',{}).get('value',0),'total_liabilities':bal.get('liabilities',{}).get('value',0),'equity':bal.get('equity',{}).get('value',0),'cash':bal.get('current_assets',{}).get('value',0),'total_debt':bal.get('noncurrent_liabilities',{}).get('value',0)})
    return {'ticker':ticker,'income_statements':income_statements,'balance_sheets':balance_sheets,'quarters':len(income_statements)}

def fetch_company_news(ticker=None,limit=15):
    url=f"https://api.polygon.io/v2/reference/news?limit={limit}&apiKey={POLY_KEY}"
    if ticker:url+=f"&ticker={ticker}"
    d=http_get(url,timeout=15)
    if d and 'results' in d:
        return [{'title':a.get('title',''),'source':a.get('publisher',{}).get('name',''),'url':a.get('article_url',''),'published':a.get('published_utc',''),'tickers':a.get('tickers',[]),'description':a.get('description','')[:300]} for a in d['results']]
    return []

# ═══ CRYPTO ═══
def fetch_crypto(limit=50):
    url=f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/listings/latest?limit={limit}&convert=USD"
    d=http_get(url,headers={'X-CMC_PRO_API_KEY':CMC_KEY},timeout=15)
    if d and 'data' in d:
        total_mc=sum(x['quote']['USD']['market_cap'] for x in d['data']) or 1
        return [{'symbol':c['symbol'],'name':c['name'],'price':round(c['quote']['USD']['price'],6),'market_cap':c['quote']['USD']['market_cap'],'volume_24h':c['quote']['USD']['volume_24h'],'change_1h':round(c['quote']['USD'].get('percent_change_1h',0),2),'change_24h':round(c['quote']['USD'].get('percent_change_24h',0),2),'change_7d':round(c['quote']['USD'].get('percent_change_7d',0),2),'change_30d':round(c['quote']['USD'].get('percent_change_30d',0),2),'rank':c['cmc_rank'],'dominance':round(c['quote']['USD']['market_cap']/total_mc*100,2)} for c in d['data']]
    return []

def fetch_crypto_price(symbol):
    url=f"https://pro-api.coinmarketcap.com/v1/cryptocurrency/quotes/latest?symbol={symbol.upper()}&convert=USD"
    d=http_get(url,headers={'X-CMC_PRO_API_KEY':CMC_KEY},timeout=10)
    if d and 'data' in d:
        coin=list(d['data'].values())[0];q=coin['quote']['USD']
        return {'symbol':coin['symbol'],'name':coin['name'],'price':q['price'],'market_cap':q['market_cap'],'volume_24h':q['volume_24h'],'change_1h':q.get('percent_change_1h',0),'change_24h':q.get('percent_change_24h',0),'change_7d':q.get('percent_change_7d',0),'change_30d':q.get('percent_change_30d',0),'change_90d':q.get('percent_change_90d',0)}
    return None

def fetch_news(query=None):
    if query:url=f"https://newsapi.org/v2/everything?q={quote(query)}&sortBy=publishedAt&pageSize=10&apiKey={NEWS_KEY}"
    else:url=f"https://newsapi.org/v2/top-headlines?country=us&category=business&pageSize=15&apiKey={NEWS_KEY}"
    d=http_get(url,timeout=10)
    if d and 'articles' in d:
        return [{'title':a.get('title',''),'source':a.get('source',{}).get('name',''),'description':a.get('description',''),'url':a.get('url',''),'published':a.get('publishedAt','')} for a in d['articles'] if a.get('title')]
    return []

def fetch_fear_greed():
    d=http_get("https://api.alternative.me/fng/?limit=7")
    if d and 'data' in d:
        return {'current':int(d['data'][0]['value']),'label':d['data'][0]['value_classification'],'yesterday':int(d['data'][1]['value']) if len(d['data'])>1 else None,'last_week':int(d['data'][6]['value']) if len(d['data'])>6 else None}
    return {'current':50,'label':'Neutral'}

def fetch_existing_data():
    results={}
    try:
        obj=s3.get_object(Bucket=BUCKET,Key='data/dashboard.json')
        results['dashboard']=json.loads(obj['Body'].read().decode())
    except:pass
    try:
        d=http_get('https://35t3serkv4gn2hk7utwvp7t2sa0flbum.lambda-url.us-east-1.on.aws/analysis',timeout=15)
        if d:results['cftc']=d
    except:pass
    return results

# ═══ LIQUIDITY ENGINE ═══
def calc_liquidity(fred):
    fed_bs=fred.get('WALCL',{}).get('value',0);rrp=fred.get('RRPONTSYD',{}).get('value',0);tga=fred.get('WTREGEN',{}).get('value',0);reserves=fred.get('WRESBAL',{}).get('value',0)
    net_liq=(fed_bs-rrp-tga)/1000 if fed_bs else 0
    fed_bs_chg=fred.get('WALCL',{}).get('chg_1m',0);rrp_chg=fred.get('RRPONTSYD',{}).get('chg_1m',0);tga_chg=fred.get('WTREGEN',{}).get('chg_1m',0)
    net_liq_chg=(fed_bs_chg-rrp_chg-tga_chg)/1000
    if net_liq_chg>50:regime='EXPANSION'
    elif net_liq_chg>0:regime='STABLE'
    elif net_liq_chg>-50:regime='TIGHTENING'
    elif net_liq_chg>-200:regime='CONTRACTION'
    else:regime='CRISIS'
    return {'net_liquidity':round(net_liq,1),'net_liq_change_1m':round(net_liq_chg,1),'regime':regime,'fed_balance_sheet':round(fed_bs/1000,1),'rrp':round(rrp/1000,1),'tga':round(tga/1000,1),'reserves':round(reserves/1000,1),'sofr':fred.get('SOFR',{}).get('value',0),'stress_index':fred.get('STLFSI2',{}).get('value',0),'nfci':fred.get('NFCI',{}).get('value',0),'components':{'fed_bs_trend':'expanding' if fed_bs_chg>0 else 'contracting','rrp_trend':'draining' if rrp_chg<0 else 'building','tga_trend':'drawing down' if tga_chg<0 else 'building up'}}

# ═══ RISK ENGINE ═══
def calc_risk(fred,stocks):
    vix=fred.get('VIXCLS',{}).get('value',20);hy=fred.get('BAMLH0A0HYM2',{}).get('value',4);ig=fred.get('BAMLC0A0CM',{}).get('value',1);s2s10=fred.get('T10Y2Y',{}).get('value',0);stress=fred.get('STLFSI2',{}).get('value',0);unemp=fred.get('UNRATE',{}).get('value',4)
    vs=min(100,max(0,(vix-12)/40*100));cs=min(100,max(0,(hy-2.5)/8*100));ys=min(100,max(0,(0.5-s2s10)/3*100));ss=min(100,max(0,(stress+1)/4*100));ls=min(100,max(0,(unemp-3.5)/5*100))
    spy=stocks.get('SPY',{});ms=min(100,max(0,50-(spy.get('change_pct',0) if spy.get('price',0)>0 else 0)*10))
    comp=vs*0.25+cs*0.20+ys*0.15+ss*0.15+ls*0.10+ms*0.15
    level='CRITICAL' if comp>=75 else 'ELEVATED' if comp>=55 else 'MODERATE' if comp>=35 else 'LOW'
    return {'composite':round(comp,1),'level':level,'vix':vix,'hy_spread':hy,'ig_spread':ig,'yield_curve':s2s10,'stress_index':stress,'scores':{'volatility':round(vs,1),'credit':round(cs,1),'yield_curve':round(ys,1),'financial_stress':round(ss,1),'labor_market':round(ls,1),'market_momentum':round(ms,1)}}

# ═══ RECOMMENDATIONS ═══
def generate_recommendations(fred,stocks,crypto,risk,liquidity):
    recs=[];regime=liquidity['regime'];vix=risk['vix'];hy=risk['hy_spread']
    for ticker,data in stocks.items():
        price_val=data.get('price',0) or data.get('prev_close',0)
        if not price_val or price_val==0:continue
        price=data.get('price',0) or data.get('prev_close',0);chg=data.get('change_pct',0);name=data.get('name',ticker);score=0;reasons=[]
        if chg>2:score+=20;reasons.append(f'Strong momentum (+{chg:.1f}%)')
        elif chg>0.5:score+=10;reasons.append(f'Positive momentum (+{chg:.1f}%)')
        elif chg<-3:score+=15;reasons.append(f'Oversold bounce ({chg:.1f}%)')
        elif chg<-1:score+=5;reasons.append(f'Pullback ({chg:.1f}%)')
        if regime in ['EXPANSION','STABLE']:
            if ticker in ['QQQ','ARKK','XLK','NVDA','AMD','PLTR','COIN','IBIT']:score+=25;reasons.append('Risk-on favored in expansion')
            elif ticker in ['SPY','DIA','VTI']:score+=15;reasons.append('Broad market supported')
        elif regime in ['TIGHTENING','CONTRACTION']:
            if ticker in ['XLU','XLP','XLV','TLT','GLD','SLV']:score+=25;reasons.append('Defensives favored')
            elif ticker in ['SHY','TIP']:score+=20;reasons.append('Short duration for tightening')
        elif regime=='CRISIS':
            if ticker in ['GLD','SLV','TLT','SHY']:score+=30;reasons.append('Safe haven demand')
        if vix>25 and ticker in ['GLD','TLT','XLU']:score+=15;reasons.append(f'High VIX ({vix:.0f})')
        elif vix<15 and ticker in ['QQQ','ARKK','IBIT','XBI']:score+=15;reasons.append(f'Low VIX ({vix:.0f})')
        if hy>5 and ticker in ['GLD','TLT']:score+=20;reasons.append(f'Wide spreads ({hy:.1f}%)')
        if regime in ['EXPANSION','STABLE']:up=round(price*1.12,2);dn=round(price*0.93,2)
        elif regime=='TIGHTENING':up=round(price*1.07,2);dn=round(price*0.90,2)
        else:up=round(price*1.05,2);dn=round(price*0.85,2)
        action='BUY' if score>=20 else 'WATCH' if score>=15 else 'AVOID'
        recs.append({'ticker':ticker,'name':name,'type':'etf' if ticker in ETF_TICKERS else 'stock','price':price,'change_pct':chg,'score':score,'action':action,'upside_target':up,'downside_target':dn,'upside_pct':round((up/price-1)*100,1),'downside_pct':round((1-dn/price)*100,1),'risk_reward':round((up-price)/max(price-dn,0.01),2),'reasons':reasons})
    for coin in (crypto or [])[:25]:
        score=0;reasons=[]
        if coin['change_7d']>10:score+=20;reasons.append(f'Strong 7D (+{coin["change_7d"]:.1f}%)')
        if coin['change_24h']<-5:score+=15;reasons.append(f'Oversold ({coin["change_24h"]:.1f}%)')
        if regime in ['EXPANSION','STABLE']:score+=15;reasons.append('Liquidity favors crypto')
        up=round(coin['price']*1.25,6);dn=round(coin['price']*0.80,6)
        action='BUY' if score>=25 else 'WATCH' if score>=10 else 'AVOID'
        recs.append({'ticker':coin['symbol'],'name':coin['name'],'type':'crypto','price':coin['price'],'change_pct':coin['change_24h'],'score':score,'action':action,'upside_target':up,'downside_target':dn,'upside_pct':round((up/max(coin['price'],0.0001)-1)*100,1),'downside_pct':round((1-dn/max(coin['price'],0.0001))*100,1),'risk_reward':round((up-coin['price'])/max(coin['price']-dn,0.0001),2),'reasons':reasons,'market_cap':coin.get('market_cap',0),'rank':coin.get('rank',999)})
    recs.sort(key=lambda x:x['score'],reverse=True)
    return recs

# ═══ CLAUDE AI ═══
def ask_claude(prompt,max_tokens=4000):
    # Try existing AI chat Lambda first
    result=http_post('https://zh3c6izcbzcqwcia4m6dmjnupy0dbnns.lambda-url.us-east-1.on.aws/',{'message':prompt},timeout=90)
    if result and result.get('response'):return result['response']
    if result and result.get('message'):return result['message']
    # Fallback to direct API
    if not ANTHROPIC_KEY:return "AI credits needed - visit console.anthropic.com/settings/billing"
    result2=http_post('https://api.anthropic.com/v1/messages',{'model':'claude-sonnet-4-20250514','max_tokens':max_tokens,'messages':[{'role':'user','content':prompt}]},headers={'Content-Type':'application/json','x-api-key':ANTHROPIC_KEY,'anthropic-version':'2023-06-01'},timeout=60)
    if result2 and 'content' in result2:return result2['content'][0].get('text','')
    return 'AI analysis unavailable - add credits at console.anthropic.com'

def generate_ai_briefing(liq,risk,recs,fred,crypto,news):
    top_buys=[r for r in recs if r['action']=='BUY'][:10]
    buy_str='\n'.join([f"  {r['ticker']} ({r['name']}): ${r['price']:.2f}, Score:{r['score']}, Up:{r['upside_pct']}% Down:{r['downside_pct']}%, R:R {r['risk_reward']}x — {', '.join(r['reasons'])}" for r in top_buys])
    crypto_str='\n'.join([f"  {c['symbol']}: ${c['price']:,.2f} ({c['change_24h']:+.1f}% 24h)" for c in (crypto or [])[:10]])
    news_str='\n'.join([f"  - {n['title']} ({n['source']})" for n in (news or [])[:8]])
    prompt=f"""You are Khalid's personal financial secretary. Analyze this REAL market data and give actionable advice.

LIQUIDITY: Net=${liq['net_liquidity']:,.0f}B Regime={liq['regime']} 1M_Chg=${liq['net_liq_change_1m']:+,.0f}B
  Fed=${liq['fed_balance_sheet']:,.0f}B RRP=${liq['rrp']:,.0f}B TGA=${liq['tga']:,.0f}B SOFR={liq['sofr']:.2f}%

RISK: {risk['composite']:.0f}/100 ({risk['level']}) VIX={risk['vix']:.1f} HY={risk['hy_spread']:.2f}% 2s10s={risk['yield_curve']:.2f}%

RATES: FF={fred.get('FEDFUNDS',{}).get('value','?')}% 2Y={fred.get('DGS2',{}).get('value','?')}% 10Y={fred.get('DGS10',{}).get('value','?')}% 30Y={fred.get('DGS30',{}).get('value','?')}%
Dollar={fred.get('DTWEXBGS',{}).get('value','?')} Oil=${fred.get('DCOILWTICO',{}).get('value','?')}

TOP BUYS:
{buy_str}

CRYPTO:
{crypto_str}

NEWS:
{news_str}

Provide: 1.MARKET OUTLOOK 2.LIQUIDITY THESIS 3.RISK ASSESSMENT 4.TOP 5 TRADES(ticker,entry,target,stop,thesis) 5.PORTFOLIO ALLOCATION(%equities/bonds/gold/crypto/cash) 6.CRYPTO OUTLOOK 7.KEY EVENTS THIS WEEK
Be specific with numbers. Act like a Goldman Sachs portfolio strategist."""
    return ask_claude(prompt)

# ═══ EMAIL ═══
def send_email(subject,html_body):
    try:
        ses.send_email(Source=EMAIL_FROM,Destination={'ToAddresses':[EMAIL_TO]},Message={'Subject':{'Data':subject,'Charset':'UTF-8'},'Body':{'Html':{'Data':html_body,'Charset':'UTF-8'}}})
        return True
    except Exception as e:print(f"Email error: {e}");return False

def build_email_html(scan):
    liq=scan.get('liquidity',{});risk=scan.get('risk',{});recs=scan.get('recommendations',[]);ai=scan.get('ai_briefing','');ts=scan.get('timestamp','')
    top_buys=[r for r in recs if r['action']=='BUY'][:15]
    rc='#ff4444' if risk.get('level')=='CRITICAL' else '#ff8800' if risk.get('level')=='ELEVATED' else '#44cc44' if risk.get('level')=='LOW' else '#ffaa00'
    lc='#44cc44' if liq.get('regime') in ['EXPANSION','STABLE'] else '#ff8800' if liq.get('regime')=='TIGHTENING' else '#ff4444'
    rows=''.join([f"<tr><td style='padding:6px;border-bottom:1px solid #333;color:#00ddff;font-weight:700'>{r['ticker']}</td><td style='padding:6px;border-bottom:1px solid #333'>{r['name']}</td><td style='padding:6px;border-bottom:1px solid #333;font-family:monospace'>${r['price']:,.2f}</td><td style='padding:6px;border-bottom:1px solid #333;color:#00ff88'>+{r['upside_pct']}%</td><td style='padding:6px;border-bottom:1px solid #333;color:#ff4444'>-{r['downside_pct']}%</td><td style='padding:6px;border-bottom:1px solid #333;color:#00ddff'>{r['risk_reward']}x</td><td style='padding:6px;border-bottom:1px solid #333;font-size:11px'>{', '.join(r['reasons'][:2])}</td></tr>" for r in top_buys])
    ai_html=(ai or '').replace('\n','<br>')
    return f"""<!DOCTYPE html><html><body style="background:#0a0a0f;color:#e0e0e0;font-family:sans-serif;padding:20px"><div style="max-width:800px;margin:0 auto"><h1 style="color:#00ddff">JUSTHODL SECRETARY | {ts}</h1><div style="display:flex;gap:12px;margin:20px 0"><div style="flex:1;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;text-align:center"><div style="color:#888;font-size:11px">NET LIQUIDITY</div><div style="font-size:24px;font-weight:700;color:{lc}">${liq.get('net_liquidity',0):,.0f}B</div><div style="color:{lc}">{liq.get('regime','--')}</div></div><div style="flex:1;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;text-align:center"><div style="color:#888;font-size:11px">RISK</div><div style="font-size:24px;font-weight:700;color:{rc}">{risk.get('composite',0):.0f}/100</div><div style="color:{rc}">{risk.get('level','--')}</div></div><div style="flex:1;background:#161b22;border:1px solid #30363d;border-radius:8px;padding:16px;text-align:center"><div style="color:#888;font-size:11px">VIX</div><div style="font-size:24px;font-weight:700">{risk.get('vix',0):.1f}</div></div></div><div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px;margin:20px 0"><h2 style="color:#00ddff">AI ANALYSIS</h2><div style="font-size:13px;line-height:1.7">{ai_html}</div></div><div style="background:#161b22;border:1px solid #30363d;border-radius:8px;padding:20px"><h2 style="color:#00ddff">TOP RECOMMENDATIONS</h2><table style="width:100%;border-collapse:collapse;font-size:12px"><tr style="color:#888"><th style="padding:6px;text-align:left">Ticker</th><th>Name</th><th>Price</th><th>Upside</th><th>Downside</th><th>R:R</th><th>Why</th></tr>{rows}</table></div></div></body></html>"""

# ═══ CHAT HANDLER ═══
def handle_chat(message):
    msg=message.lower().strip()
    if any(msg.startswith(p) for p in ['price ','check ','quote ']):
        ticker=msg.split()[-1].upper()
        url=f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}?apiKey={POLY_KEY}"
        d=http_get(url)
        if d and 'ticker' in d:
            t=d['ticker'];day=t.get('day',{});prev=t.get('prevDay',{})
            chg=((day.get('c',0)/prev.get('c',1))-1)*100 if prev.get('c') else 0
            return {'type':'price','ticker':ticker,'price':day.get('c',0),'change':round(chg,2),'high':day.get('h',0),'low':day.get('l',0),'volume':day.get('v',0)}
        crypto_data=fetch_crypto_price(ticker)
        if crypto_data:return {'type':'crypto_price','data':crypto_data}
        return {'type':'error','message':f'Ticker {ticker} not found'}
    if any(k in msg for k in ['income','revenue','earnings','balance sheet','financials']):
        ticker=None
        for w in msg.upper().split():
            if len(w)<=5 and w.isalpha() and w not in ['THE','FOR','AND','GET','SHOW','ME']:ticker=w;break
        if ticker:return {'type':'financials','data':fetch_financials(ticker)}
        return {'type':'error','message':'Specify ticker (e.g. financials AAPL)'}
    if any(k in msg for k in ['news','headlines','breaking']):
        q=msg.replace('news','').replace('headlines','').replace('breaking','').strip()
        ticker=q.upper() if q and len(q)<=5 else None
        if ticker:return {'type':'news','data':fetch_company_news(ticker)}
        return {'type':'news','data':fetch_news(q if q else None)}
    if any(k in msg for k in ['history','historical','chart']):
        parts=msg.split();ticker=parts[-1].upper() if parts else 'SPY';days=365
        if '1m' in msg or '1 month' in msg:days=30
        elif '3m' in msg:days=90
        elif '6m' in msg:days=180
        elif '2y' in msg:days=730
        return {'type':'historical','ticker':ticker,'data':fetch_historical(ticker,days)}
    if any(k in msg for k in ['scan','update','refresh','report']):return {'type':'scan_requested'}
    if any(k in msg for k in ['crypto','bitcoin','btc','eth','altcoin']):return {'type':'crypto','data':fetch_crypto(25)}
    if any(k in msg for k in ['risk','danger','safe','warning']):
        fred=fetch_fred();stocks=fetch_polygon_prices()
        return {'type':'risk','data':calc_risk(fred,stocks)}
    if any(k in msg for k in ['liquidity','fed','rrp','tga']):
        fred=fetch_fred()
        return {'type':'liquidity','data':calc_liquidity(fred)}
    if ANTHROPIC_KEY:
        fred=fetch_fred();stocks=fetch_polygon_prices();crypto=fetch_crypto(15);liq=calc_liquidity(fred);risk=calc_risk(fred,stocks)
        spy=stocks.get('SPY',{});btc=next((c for c in crypto if c['symbol']=='BTC'),{})
        ctx_str=f"SPY:${spy.get('price',0):.2f}({spy.get('change_pct',0):+.2f}%) BTC:${btc.get('price',0):,.2f}({btc.get('change_24h',0):+.2f}%) VIX:{risk['vix']:.1f} HY:{risk['hy_spread']:.2f}% NetLiq:${liq['net_liquidity']:,.0f}B({liq['regime']}) Risk:{risk['composite']:.0f}/100({risk['level']}) FF:{fred.get('FEDFUNDS',{}).get('value','?')}% 10Y:{fred.get('DGS10',{}).get('value','?')}%\n\nUser question: {message}\n\nAnswer with real numbers. Be specific. You are Khalid's financial secretary."
        return {'type':'ai_response','message':ask_claude(ctx_str,max_tokens=2000)}
    return {'type':'error','message':'Try: price AAPL, financials NVDA, news TSLA, scan, risk, liquidity'}

# ═══ FULL SCAN ═══
def run_full_scan():
    start=time.time();print("=== FULL MARKET SCAN ===")
    with ThreadPoolExecutor(max_workers=6) as ex:
        f1=ex.submit(fetch_fred);f2=ex.submit(fetch_polygon_prices);f3=ex.submit(fetch_crypto,50);f4=ex.submit(fetch_news);f5=ex.submit(fetch_fear_greed);f6=ex.submit(fetch_existing_data)
        fred=f1.result();stocks=f2.result();crypto=f3.result();news=f4.result();fg=f5.result();existing=f6.result()
    print(f"  Data: FRED={len(fred)} Stocks={len(stocks)} Crypto={len(crypto)} News={len(news)}")
    liq=calc_liquidity(fred);risk=calc_risk(fred,stocks);recs=generate_recommendations(fred,stocks,crypto,risk,liq)
    buys=[r for r in recs if r['action']=='BUY']
    print(f"  Liq={liq['regime']} Risk={risk['composite']:.0f} Buys={len(buys)}")
    ai=generate_ai_briefing(liq,risk,recs,fred,crypto,news)
    print(f"  AI={len(ai)} chars")
    now=datetime.now(timezone(timedelta(hours=-5)))
    scan={'version':'1.0','type':'secretary_scan','timestamp':now.strftime('%Y-%m-%d %H:%M:%S ET'),'scan_time_seconds':round(time.time()-start,1),'liquidity':liq,'risk':risk,'fear_greed':fg,'recommendations':recs[:50],'top_buys':buys[:15],'ai_briefing':ai,'fred':{k:{'name':v['name'],'value':v['value'],'chg_1d':v['chg_1d']} for k,v in fred.items()},'stocks_count':len(stocks),'crypto_count':len(crypto),'crypto_top10':crypto[:10],'news':news[:10],'market_snapshot':{'spy':stocks.get('SPY',{}),'qqq':stocks.get('QQQ',{}),'dia':stocks.get('DIA',{}),'iwm':stocks.get('IWM',{}),'gld':stocks.get('GLD',{}),'tlt':stocks.get('TLT',{}),'btc':next((c for c in crypto if c['symbol']=='BTC'),{}),'eth':next((c for c in crypto if c['symbol']=='ETH'),{})},'cftc':existing.get('cftc',{})}
    s3.put_object(Bucket=BUCKET,Key='data/secretary-latest.json',Body=json.dumps(scan,default=str),ContentType='application/json',CacheControl='max-age=300')
    s3.put_object(Bucket=BUCKET,Key=f'data/secretary-history/{now.strftime("%Y-%m-%d_%H%M")}.json',Body=json.dumps(scan,default=str),ContentType='application/json')
    subj=f"Secretary: {liq['regime']} | Risk {risk['composite']:.0f} | {len(buys)} Buys | {now.strftime('%b %d %I:%M %p')}"
    send_email(subj,build_email_html(scan))
    print(f"=== DONE {time.time()-start:.1f}s ===")
    return scan

# ═══ LAMBDA HANDLER ═══
def lambda_handler(event,context):
    headers={'Content-Type':'application/json','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'GET,POST,OPTIONS','Access-Control-Allow-Headers':'Content-Type'}
    def respond(code,body):return {'statusCode':code,'headers':headers,'body':json.dumps(body,default=str)}
    method=event.get('requestContext',{}).get('http',{}).get('method','')
    if method=='OPTIONS':return respond(200,{'status':'ok'})
    if event.get('source')=='aws.events' or event.get('detail-type')=='Scheduled Event':
        scan=run_full_scan()
        return respond(200,{'status':'scan_complete','regime':scan['liquidity']['regime'],'risk':scan['risk']['composite'],'buys':len(scan['top_buys'])})
    path=event.get('rawPath','') or event.get('path','')
    body={}
    raw=event.get('body','{}')
    if raw:
        try:
            if event.get('isBase64Encoded'):
                import base64;raw=base64.b64decode(raw).decode()
            body=json.loads(raw) if isinstance(raw,str) else raw
        except:body={}
    if path=='/latest' or (method=='GET' and not path.strip('/')):
        try:
            obj=s3.get_object(Bucket=BUCKET,Key='data/secretary-latest.json')
            return respond(200,json.loads(obj['Body'].read().decode()))
        except:return respond(200,{'status':'no_scan_yet'})
    if path=='/scan' or body.get('action')=='scan':return respond(200,run_full_scan())
    if path=='/chat' or body.get('action')=='chat':
        msg=body.get('message','')
        if not msg:return respond(400,{'error':'Missing message'})
        result=handle_chat(msg)
        if result.get('type')=='scan_requested':
            scan=run_full_scan()
            return respond(200,{'type':'scan_complete','message':f"Scan done. {scan['liquidity']['regime']}, Risk {scan['risk']['composite']:.0f}, {len(scan['top_buys'])} buys.",'data':scan})
        return respond(200,result)
    if path.startswith('/price/'):return respond(200,handle_chat(f"price {path.split('/')[-1]}"))
    if path.startswith('/financials/'):return respond(200,{'type':'financials','data':fetch_financials(path.split('/')[-1].upper())})
    if path.startswith('/news/'):return respond(200,{'type':'news','data':fetch_company_news(path.split('/')[-1].upper())})
    if path.startswith('/history/'):return respond(200,{'type':'historical','ticker':path.split('/')[-1].upper(),'data':fetch_historical(path.split('/')[-1].upper(),int(body.get('days',365)))})
    if path=='/crypto':return respond(200,{'type':'crypto','data':fetch_crypto(50)})
    if path.startswith('/crypto/'):return respond(200,{'type':'crypto_price','data':fetch_crypto_price(path.split('/')[-1].upper())})
    if path=='/recommendations':
        try:
            obj=s3.get_object(Bucket=BUCKET,Key='data/secretary-latest.json')
            data=json.loads(obj['Body'].read().decode())
            return respond(200,{'recommendations':data.get('recommendations',[])})
        except:return respond(200,{'status':'run_scan_first'})
    return respond(200,{'service':'JustHodl Financial Secretary v1.0','endpoints':{'GET /latest':'Latest scan','POST /scan':'Force scan','POST /chat':'Chat','GET /price/TICKER':'Price','GET /financials/TICKER':'Financials','GET /news/TICKER':'News','GET /history/TICKER':'History','GET /crypto':'Top 50','GET /crypto/SYMBOL':'Crypto price','GET /recommendations':'Signals'}})
