"""
JustHodl.AI Crypto Intelligence Engine v4.1
TIER 1-4 + Full System AI + Wyckoff + Binance Fallbacks
"""
import json, os, time, math, urllib.request, urllib.error, ssl, statistics, traceback
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor, as_completed

S3_BUCKET = os.environ.get('S3_BUCKET', 'justhodl-dashboard-live')
CMC_API_KEY = os.environ.get('CMC_API_KEY', '')
ANTHROPIC_API_KEY = os.environ.get('ANTHROPIC_API_KEY', '')

import boto3
s3 = boto3.client('s3')

TARGET_COINS = ['BTC', 'ETH', 'PEPE', 'DOGE', 'POL']
TIMEFRAMES = {'4h': '4h', '1d': '1d', '1w': '1w'}
BINANCE_SYMBOLS = {'BTC': 'BTCUSDT', 'ETH': 'ETHUSDT', 'PEPE': 'PEPEUSDT', 'DOGE': 'DOGEUSDT', 'POL': 'POLUSDT'}
CG_IDS = {'BTC': 'bitcoin', 'ETH': 'ethereum', 'PEPE': 'pepe', 'DOGE': 'dogecoin', 'POL': 'matic-network'}
BN_MIRRORS = ['https://api.binance.com', 'https://api1.binance.com', 'https://api2.binance.com', 'https://api3.binance.com', 'https://data-api.binance.vision']
FP_MIRRORS = ['https://fapi.binance.com']

def http_get(url, headers=None, timeout=15):
    try:
        h = headers or {'User-Agent': 'Mozilla/5.0 (compatible; JustHodl/4.1)', 'Accept': 'application/json'}
        req = urllib.request.Request(url, headers=h)
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        print(f"  HTTP{e.code} {url[:80]}")
        return None
    except Exception as e:
        print(f"  ERR {url[:60]}: {type(e).__name__}")
        return None

def http_mirror(path, mirrors, headers=None, timeout=12):
    for base in mirrors:
        r = http_get(f"{base}{path}", headers, timeout)
        if r is not None: return r
    return None

def http_post(url, data, headers, timeout=60):
    try:
        req = urllib.request.Request(url, data=json.dumps(data).encode(), headers=headers, method='POST')
        with urllib.request.urlopen(req, timeout=timeout) as r:
            return json.loads(r.read().decode())
    except Exception as e:
        print(f"  POST ERR: {e}")
        return None

def s3_read(key):
    try:
        return json.loads(s3.get_object(Bucket=S3_BUCKET, Key=key)['Body'].read().decode())
    except: return None

def fmt(n):
    if n is None: return '-'
    n = float(n)
    if abs(n)>=1e12: return f"${n/1e12:.2f}T"
    if abs(n)>=1e9: return f"${n/1e9:.2f}B"
    if abs(n)>=1e6: return f"${n/1e6:.1f}M"
    if abs(n)>=1e3: return f"${n/1e3:.1f}K"
    return f"${n:.2f}"

def sr(v, d=2):
    try: return round(float(v or 0), d)
    except: return 0

# ═══ TIER 1: TECHNICAL INDICATORS ═══
def ema(c, p):
    if len(c)<p: return [None]*len(c)
    k=2/(p+1); r=[None]*(p-1); r.append(sum(c[:p])/p)
    for i in range(p,len(c)): r.append(c[i]*k+r[-1]*(1-k))
    return r

def rsi(c, p=14):
    if len(c)<p+1: return None,[]
    d=[c[i]-c[i-1] for i in range(1,len(c))]
    g=[max(x,0) for x in d]; l=[abs(min(x,0)) for x in d]
    ag=sum(g[:p])/p; al=sum(l[:p])/p; rv=[]
    for i in range(p,len(d)):
        ag=(ag*(p-1)+g[i])/p; al=(al*(p-1)+l[i])/p
        rv.append(round(100-100/(1+(ag/al if al>0 else 100)),2))
    return rv[-1] if rv else None, rv

def macd(c, f=12, s=26, sg=9):
    ef=ema(c,f); es=ema(c,s)
    ml=[ef[i]-es[i] if ef[i] and es[i] else None for i in range(len(c))]
    v=[x for x in ml if x is not None]
    if len(v)<sg: return None,None,None,'NONE'
    sl=ema(v,sg); cm,cs=v[-1],sl[-1] if sl else None
    h=round(cm-cs,6) if cs else None
    x='BULLISH' if len(v)>=2 and cs and sl[-2] and v[-2]<sl[-2] and cm>cs else 'BEARISH' if len(v)>=2 and cs and sl[-2] and v[-2]>sl[-2] and cm<cs else 'NONE'
    return round(cm,6),round(cs,6) if cs else None,h,x

def bb(c, p=20, m=2):
    if len(c)<p: return None,None,None,None,None
    s=sum(c[-p:])/p; sd=statistics.stdev(c[-p:]); u=s+m*sd; l=s-m*sd
    return round(u,6),round(s,6),round(l,6),round((u-l)/s*100,2) if s else 0,round((c[-1]-l)/(u-l)*100,1) if u!=l else 50

def atr(h,l,c,p=14):
    if len(c)<p+1: return None
    tr=[max(h[i]-l[i],abs(h[i]-c[i-1]),abs(l[i]-c[i-1])) for i in range(1,len(c))]
    a=sum(tr[:p])/p
    for i in range(p,len(tr)): a=(a*(p-1)+tr[i])/p
    return round(a,6)

def stochrsi(c, rp=14, sp=14, kp=3, dp=3):
    _,rv=rsi(c,rp)
    if len(rv)<sp: return None,None
    sv=[]
    for i in range(sp-1,len(rv)):
        w=rv[i-sp+1:i+1]; lo,hi=min(w),max(w)
        sv.append((rv[i]-lo)/(hi-lo)*100 if hi!=lo else 50)
    if len(sv)<kp: return None,None
    kv=[sum(sv[i:i+kp])/kp for i in range(len(sv)-kp+1)]
    dv=[sum(kv[i:i+dp])/dp for i in range(len(kv)-dp+1)] if len(kv)>=dp else []
    return round(kv[-1],2) if kv else None, round(dv[-1],2) if dv else None

def supertrend(h,l,c,p=10,m=3):
    a=atr(h,l,c,p)
    if not a: return None,None
    hl2=(h[-1]+l[-1])/2; u=hl2+m*a; lo=hl2-m*a
    t='BULLISH' if c[-1]>u else 'BEARISH' if c[-1]<lo else ('BULLISH' if c[-1]>hl2 else 'BEARISH')
    return round(lo if t=='BULLISH' else u,6),t

def klines(sym, intv, lim=200):
    path=f"/api/v3/klines?symbol={sym}&interval={intv}&limit={lim}"
    d=http_mirror(path, BN_MIRRORS, timeout=12)
    if not d: return None
    r=[]
    for k in d:
        try: r.append({'o':float(k[1]),'h':float(k[2]),'l':float(k[3]),'c':float(k[4]),'v':float(k[5])})
        except: continue
    return r if len(r)>=30 else None

def klines_cg(cid, days):
    d=http_get(f"https://api.coingecko.com/api/v3/coins/{cid}/ohlc?vs_currency=usd&days={days}",timeout=12)
    if not d: return None
    r=[]
    for k in d:
        try: r.append({'o':float(k[1]),'h':float(k[2]),'l':float(k[3]),'c':float(k[4]),'v':0})
        except: continue
    return r if len(r)>=20 else None

def pivots(d, left=5, right=5):
    hi,lo=[],[]
    for i in range(left,len(d)-right):
        if all(d[i]>=d[i-j] for j in range(1,left+1)) and all(d[i]>=d[i+j] for j in range(1,right+1)): hi.append((i,d[i]))
        if all(d[i]<=d[i-j] for j in range(1,left+1)) and all(d[i]<=d[i+j] for j in range(1,right+1)): lo.append((i,d[i]))
    return hi,lo

def patterns(closes, highs, lows):
    P=[]
    try:
        ph,pl=pivots(closes,5,5)
        if len(ph)>=2:
            a,b=ph[-2],ph[-1]
            if abs(a[1]-b[1])/max(a[1],1e-8)<0.03 and b[0]-a[0]>=10:
                nk=min(closes[a[0]:b[0]+1])
                P.append({'type':'DOUBLE_TOP' if closes[-1]<nk else 'DOUBLE_TOP_FORMING','confidence':'HIGH' if closes[-1]<nk else 'MEDIUM','bias':'BEARISH','detail':f'Peaks ~{a[1]:.4f}, nk {nk:.4f}'})
        if len(pl)>=2:
            a,b=pl[-2],pl[-1]
            if abs(a[1]-b[1])/max(a[1],1e-8)<0.03 and b[0]-a[0]>=10:
                nk=max(closes[a[0]:b[0]+1])
                P.append({'type':'DOUBLE_BOTTOM' if closes[-1]>nk else 'DBL_BOTTOM_FORMING','confidence':'HIGH' if closes[-1]>nk else 'MEDIUM','bias':'BULLISH','detail':f'Troughs ~{a[1]:.4f}, nk {nk:.4f}'})
        if len(ph)>=3:
            s1,hd,s2=ph[-3],ph[-2],ph[-1]
            if hd[1]>s1[1] and hd[1]>s2[1] and abs(s1[1]-s2[1])/max(s1[1],1e-8)<0.05:
                nk=(min(closes[s1[0]:hd[0]+1])+min(closes[hd[0]:s2[0]+1]))/2
                P.append({'type':'HEAD_SHOULDERS' if closes[-1]<nk else 'H&S_FORMING','confidence':'HIGH' if closes[-1]<nk else 'MEDIUM','bias':'BEARISH','detail':f'Head {hd[1]:.4f}, nk {nk:.4f}'})
        if len(ph)>=2 and len(pl)>=2:
            hs=(ph[-1][1]-ph[-2][1])/max(ph[-1][0]-ph[-2][0],1)
            ls=(pl[-1][1]-pl[-2][1])/max(pl[-1][0]-pl[-2][0],1)
            if abs(hs)<0.001*closes[-1] and ls>0: P.append({'type':'ASC_TRIANGLE','confidence':'MEDIUM','bias':'BULLISH','detail':f'Flat ~{ph[-1][1]:.4f}'})
            elif abs(ls)<0.001*closes[-1] and hs<0: P.append({'type':'DESC_TRIANGLE','confidence':'MEDIUM','bias':'BEARISH','detail':f'Flat ~{pl[-1][1]:.4f}'})
        if len(closes)>=40:
            r,p=closes[-20:],closes[-40:-20]
            pm=(p[-1]-p[0])/p[0]*100 if p[0] else 0
            rr=(max(r)-min(r))/min(r)*100 if min(r) else 0
            if pm>10 and rr<5: P.append({'type':'BULL_FLAG','confidence':'MEDIUM','bias':'BULLISH','detail':f'{pm:.1f}% rally + {rr:.1f}% tight'})
            elif pm<-10 and rr<5: P.append({'type':'BEAR_FLAG','confidence':'MEDIUM','bias':'BEARISH','detail':f'{pm:.1f}% drop + {rr:.1f}% tight'})
    except: pass
    return P

def analyze(coin, tf):
    sym=BINANCE_SYMBOLS.get(coin)
    if not sym: return {'status':'error'}
    cd=klines(sym,tf,200)
    if not cd:
        cid=CG_IDS.get(coin)
        days={'4h':14,'1d':90,'1w':365}.get(tf,90)
        if cid: cd=klines_cg(cid,days)
    if not cd: return {'status':'error','error':'No candle data'}
    
    try:
        C=[x['c'] for x in cd]; H=[x['h'] for x in cd]; L=[x['l'] for x in cd]; V=[x['v'] for x in cd]
        cur=C[-1]
        rv,_=rsi(C); mv,ms,mh,mc=macd(C)
        e20=ema(C,20);e50=ema(C,50);e200=ema(C,200)
        bu,bm,bl,bw,bp=bb(C); av=atr(H,L,C); sk,sd=stochrsi(C); sv,st=supertrend(H,L,C)
        e20c=e20[-1] if e20 and e20[-1] else None
        e50c=e50[-1] if e50 and e50[-1] else None
        e200c=e200[-1] if e200 and e200[-1] else None
        et='STRONG_BULL' if e20c and e50c and e200c and e20c>e50c>e200c else 'BULL' if e20c and e50c and e20c>e50c else 'STRONG_BEAR' if e20c and e50c and e200c and e20c<e50c<e200c else 'BEAR' if e20c and e50c and e20c<e50c else 'NEUTRAL'
        xs=None
        if e50 and e200 and len(e50)>=2 and len(e200)>=2 and e50[-1] and e200[-1] and e50[-2] and e200[-2]:
            if e50[-2]<e200[-2] and e50[-1]>e200[-1]: xs='GOLDEN_CROSS'
            elif e50[-2]>e200[-2] and e50[-1]<e200[-1]: xs='DEATH_CROSS'
        
        # Wyckoff
        r20=C[-20:] if len(C)>=20 else C; v20=V[-20:] if len(V)>=20 else V
        rng=(max(r20)-min(r20))/min(r20)*100 if min(r20)>0 else 0
        vt=(sum(v20[10:])/max(len(v20)-10,1))/(sum(v20[:10])/10) if len(v20)>=10 and sum(v20[:10])>0 else 1
        pt=(r20[-1]-r20[0])/r20[0]*100 if r20[0]>0 else 0
        r50=C[-50:] if len(C)>=50 else C; avg50=sum(r50)/len(r50)
        
        if rng<8 and vt<0.8: wyck='ACCUMULATION'
        elif pt>10 and vt>1.2: wyck='MARKUP'
        elif rng<8 and pt>-3 and cur>avg50: wyck='DISTRIBUTION'
        elif pt<-10 and vt>1.2: wyck='MARKDOWN'
        elif rv and rv>75: wyck='DISTRIBUTION'
        elif rv and rv<25: wyck='ACCUMULATION'
        else: wyck='MARKUP' if pt>3 else 'MARKDOWN' if pt<-3 else 'RANGING'
        
        pats=patterns(C,H,L)
        
        bull=bear=0; sigs=[]
        if rv and rv<30: sigs.append('RSI oversold');bull+=2
        elif rv and rv>70: sigs.append('RSI overbought');bear+=2
        if mc=='BULLISH': sigs.append('MACD bull');bull+=2
        elif mc=='BEARISH': sigs.append('MACD bear');bear+=2
        if st=='BULLISH': bull+=1
        else: bear+=1
        if et in('STRONG_BULL','BULL'): bull+=1
        elif et in('STRONG_BEAR','BEAR'): bear+=1
        if sk and sk<20: bull+=1
        elif sk and sk>80: bear+=1
        if bp and bp<10: bull+=1;sigs.append('BB lower')
        elif bp and bp>90: bear+=1;sigs.append('BB upper')
        if xs=='GOLDEN_CROSS': bull+=3;sigs.append('GOLDEN CROSS!')
        elif xs=='DEATH_CROSS': bear+=3;sigs.append('DEATH CROSS!')
        
        tot=bull+bear; score=round((bull/tot*100) if tot>0 else 50)
        bias='STRONG_BUY' if score>=80 else 'BUY' if score>=60 else 'NEUTRAL' if score>=40 else 'SELL' if score>=20 else 'STRONG_SELL'
        
        return {'status':'ok','coin':coin,'timeframe':tf,'price':round(cur,8),'change_pct':round((cur-C[0])/C[0]*100,2),
            'indicators':{'rsi':rv,'macd':{'value':mv,'signal':ms,'histogram':mh,'cross':mc},'ema':{'ema20':round(e20c,8) if e20c else None,'ema50':round(e50c,8) if e50c else None,'ema200':round(e200c,8) if e200c else None,'trend':et,'cross_signal':xs},'bollinger':{'upper':bu,'middle':bm,'lower':bl,'width':bw,'position':bp},'atr':av,'atr_pct':round(av/cur*100,2) if av and cur>0 else None,'stochrsi':{'k':sk,'d':sd},'supertrend':{'value':sv,'trend':st}},
            'wyckoff':wyck,'patterns':pats,'score':score,'bias':bias,'signals':sigs}
    except Exception as e:
        traceback.print_exc()
        return {'status':'error','error':str(e)}

def fetch_technicals():
    print("  📊 TIER 1-4: Multi-TF analysis...")
    results={}
    with ThreadPoolExecutor(max_workers=5) as ex:
        fs={}
        for coin in TARGET_COINS:
            for tk,tv in TIMEFRAMES.items():
                fs[ex.submit(analyze,coin,tv)]=f"{coin}_{tk}"
        for f in as_completed(fs):
            k=fs[f]
            try: results[k]=f.result()
            except Exception as e: results[k]={'status':'error','error':str(e)}
    
    sums={}
    for coin in TARGET_COINS:
        cd={tk:results.get(f"{coin}_{tk}",{'status':'error'}) for tk in TIMEFRAMES}
        b=be=0;sigs=[];wyks=[]
        for t in cd.values():
            if t.get('status')=='ok':
                if t.get('score',50)>60: b+=1
                elif t.get('score',50)<40: be+=1
                sigs.extend(t.get('signals',[])); wyks.append(t.get('wyckoff','?'))
        from collections import Counter
        dw=Counter(wyks).most_common(1)[0][0] if wyks else 'UNKNOWN'
        con='STRONG_BUY' if b==3 else 'BUY' if b>=2 else 'STRONG_SELL' if be==3 else 'SELL' if be>=2 else 'MIXED'
        sums[coin]={'timeframes':cd,'consensus':con,'bull_timeframes':b,'bear_timeframes':be,'key_signals':list(set(sigs))[:8],'price':cd.get('1d',{}).get('price') or cd.get('4h',{}).get('price') or 0,'wyckoff_phase':dw}
    return {'status':'ok','coins':sums}

# ═══ DATA SOURCES ═══
def fetch_stablecoins():
    d=http_get("https://stablecoins.llama.fi/stablecoins?includePrices=true")
    if not d or 'peggedAssets' not in d: return {'status':'error'}
    S=[];tot=0;mc=bc=sc=0
    for s in sorted(d['peggedAssets'],key=lambda x:(x.get('circulating',{}).get('peggedUSD',0) or 0),reverse=True)[:25]:
        c=s.get('circulating',{}).get('peggedUSD',0) or 0
        pd=s.get('circulatingPrevDay',{}).get('peggedUSD',0) or 0
        pw=s.get('circulatingPrevWeek',{}).get('peggedUSD',0) or 0
        pm=s.get('circulatingPrevMonth',{}).get('peggedUSD',0) or 0
        c1=((c-pd)/pd*100) if pd>0 else 0;c7=((c-pw)/pw*100) if pw>0 else 0;c30=((c-pm)/pm*100) if pm>0 else 0
        sg='MINTING' if c7>0.5 else 'BURNING' if c7<-0.5 else 'STABLE'
        if sg=='MINTING':mc+=1
        elif sg=='BURNING':bc+=1
        else:sc+=1
        tot+=c
        if c>50e6: S.append({'name':s.get('name','?'),'symbol':s.get('symbol','?'),'mcap':round(c),'mcap_fmt':fmt(c),'change_1d':sr(c1),'change_7d':sr(c7),'change_30d':sr(c30),'signal':sg,'mechanism':s.get('pegMechanism','?')})
    return {'status':'ok','stablecoins':S,'total_mcap':round(tot),'total_mcap_fmt':fmt(tot),'minting_count':mc,'burning_count':bc,'stable_count':sc,'net_signal':'INFLOW' if mc>bc+2 else 'OUTFLOW' if bc>mc+2 else 'NEUTRAL'}

def fetch_tvl():
    ch=http_get("https://api.llama.fi/v2/chains");pr=http_get("https://api.llama.fi/protocols")
    r={'status':'ok','total_tvl':0,'chains':[],'top_protocols':[]}
    if ch:
        cs=sorted(ch,key=lambda x:x.get('tvl',0),reverse=True)[:15]
        t=sum(c.get('tvl',0) for c in ch);r['total_tvl']=round(t);r['total_tvl_fmt']=fmt(t)
        r['chains']=[{'name':c.get('name','?'),'tvl':round(c.get('tvl',0)),'tvl_fmt':fmt(c.get('tvl',0)),'share':sr(c.get('tvl',0)/t*100 if t else 0,1)} for c in cs]
    if pr:
        tp=sorted(pr,key=lambda x:x.get('tvl',0),reverse=True)[:10]
        r['top_protocols']=[{'name':p.get('name','?'),'tvl':round(p.get('tvl',0)),'tvl_fmt':fmt(p.get('tvl',0)),'chain':p.get('chain','?'),'category':p.get('category','?'),'change_1d':sr(p.get('change_1d',0) or 0),'change_7d':sr(p.get('change_7d',0) or 0)} for p in tp]
    return r

def fetch_dex():
    d=http_get("https://api.llama.fi/overview/dexs?excludeTotalDataChart=true&excludeTotalDataChartBreakdown=true&dataType=dailyVolume")
    if not d: return {'status':'error'}
    ps=d.get('protocols',[]);top=sorted(ps,key=lambda x:(x.get('total24h',0) or 0),reverse=True)[:10]
    tv=sum((x.get('total24h',0) or 0) for x in ps)
    return {'status':'ok','total_24h_volume':round(tv),'total_24h_fmt':fmt(tv),'top_dexes':[{'name':x.get('name','?'),'volume_24h':round(x.get('total24h',0) or 0),'volume_fmt':fmt(x.get('total24h',0) or 0),'change_1d':sr(x.get('change_1d',0) or 0)} for x in top]}

def fetch_yields():
    d=http_get("https://yields.llama.fi/pools")
    if not d or 'data' not in d: return {'status':'error'}
    g=[p for p in d['data'] if (p.get('tvlUsd',0) or 0)>1e6]
    t=sorted(g,key=lambda x:(x.get('apy',0) or 0),reverse=True)[:15]
    return {'status':'ok','top_yields':[{'project':p.get('project','?'),'chain':p.get('chain','?'),'symbol':p.get('symbol','?'),'apy':sr(p.get('apy',0) or 0),'tvl':round(p.get('tvlUsd',0) or 0),'tvl_fmt':fmt(p.get('tvlUsd',0) or 0),'stablecoin':p.get('stablecoin',False)} for p in t]}

def fetch_funding():
    d=http_mirror("/fapi/v1/premiumIndex",FP_MIRRORS,timeout=12)
    if not d: return {'status':'error','error':'Futures unavailable'}
    pairs=[]
    for p in d:
        s=p.get('symbol','')
        if not s.endswith('USDT'): continue
        r=float(p.get('lastFundingRate',0));m=float(p.get('markPrice',0));idx=float(p.get('indexPrice',0))
        pairs.append({'symbol':s.replace('USDT',''),'funding_rate':round(r*100,4),'annualized':round(r*3*365*100,1),'mark_price':round(m,2),'index_price':round(idx,2),'basis_bps':round((m-idx)/idx*10000,1) if idx>0 else 0})
    pairs=sorted(pairs,key=lambda x:abs(x['funding_rate']),reverse=True)
    pos=sum(1 for p in pairs if p['funding_rate']>0);neg=sum(1 for p in pairs if p['funding_rate']<0)
    avg=sum(p['funding_rate'] for p in pairs[:20])/min(20,len(pairs)) if pairs else 0
    sent='EXTREME_GREED' if avg>0.05 else 'GREEDY' if avg>0.01 else 'NEUTRAL' if avg>-0.01 else 'FEARFUL' if avg>-0.05 else 'EXTREME_FEAR'
    return {'status':'ok','top_funding':pairs[:20],'most_shorted':sorted(pairs,key=lambda x:x['funding_rate'])[:5],'most_longed':sorted(pairs,key=lambda x:x['funding_rate'],reverse=True)[:5],'avg_funding':round(avg,4),'positive_count':pos,'negative_count':neg,'leverage_sentiment':sent}

def fetch_oi():
    syms=['BTCUSDT','ETHUSDT','SOLUSDT','BNBUSDT','XRPUSDT','DOGEUSDT']
    oi=[]
    for s in syms:
        d=http_mirror(f"/fapi/v1/openInterest?symbol={s}",FP_MIRRORS,timeout=8)
        if d: oi.append({'symbol':s.replace('USDT',''),'open_interest':round(float(d.get('openInterest',0)),2)})
    return {'status':'ok' if oi else 'error','open_interest':oi}

def fetch_global():
    d=http_get("https://api.coingecko.com/api/v3/global")
    if not d or 'data' not in d: return {'status':'error'}
    g=d['data'];mc=g.get('total_market_cap',{}).get('usd',0) or 0;vol=g.get('total_volume',{}).get('usd',0) or 0
    return {'status':'ok','total_mcap':round(mc),'total_mcap_fmt':fmt(mc),'total_volume':round(vol),'total_volume_fmt':fmt(vol),'btc_dominance':sr(g.get('market_cap_percentage',{}).get('btc',0),1),'eth_dominance':sr(g.get('market_cap_percentage',{}).get('eth',0),1),'mcap_change_24h':sr(g.get('market_cap_change_percentage_24h_usd',0)),'active_coins':g.get('active_cryptocurrencies',0)}

def fetch_top_coins():
    d=http_get("https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=25&sparkline=false&price_change_percentage=1h,24h,7d,30d")
    if not d: return {'status':'error'}
    return {'status':'ok','coins':[{'rank':c.get('market_cap_rank',0),'name':c.get('name','?'),'symbol':(c.get('symbol','?') or '?').upper(),'price':c.get('current_price',0) or 0,'price_fmt':f"${c.get('current_price',0):,.2f}" if (c.get('current_price',0) or 0)>=1 else f"${c.get('current_price',0):.6f}",'change_1h':sr(c.get('price_change_percentage_1h_in_currency',0)),'change_24h':sr(c.get('price_change_percentage_24h_in_currency',0)),'change_7d':sr(c.get('price_change_percentage_7d_in_currency',0)),'change_30d':sr(c.get('price_change_percentage_30d_in_currency',0)),'mcap':c.get('market_cap',0),'mcap_fmt':fmt(c.get('market_cap',0)),'ath':c.get('ath',0),'ath_change':sr(c.get('ath_change_percentage',0))} for c in d]}

def fetch_onchain():
    eps={'hash_rate':'hash-rate','n_transactions':'n-transactions','miners_revenue':'miners-revenue'}
    m={}
    for k,e in eps.items():
        d=http_get(f"https://api.blockchain.info/charts/{e}?timespan=30days&format=json",timeout=10)
        if d and 'values' in d and d['values']: m[k]={'value':round(d['values'][-1].get('y',0),2),'unit':d.get('unit',''),'name':d.get('name',k)}
    return {'status':'ok' if m else 'error','metrics':m}

def fetch_fg():
    d=http_get("https://api.alternative.me/fng/?limit=31")
    full=http_get("https://api.alternative.me/fng/?limit=0")
    if not d or 'data' not in d: return {'status':'error'}
    es=d['data'];cur=es[0] if es else {}
    hist=[{'value':int(e.get('value',50)),'label':e.get('value_classification','?'),'date':datetime.fromtimestamp(int(e.get('timestamp',0))).strftime('%Y-%m-%d')} for e in es[:30]]
    vals=[h['value'] for h in hist]
    
    # Full history (all available data since Feb 2018)
    full_hist=[]
    if full and 'data' in full:
        for e in full['data']:
            try:
                full_hist.append({'value':int(e.get('value',50)),'date':datetime.fromtimestamp(int(e.get('timestamp',0))).strftime('%Y-%m-%d')})
            except: pass
    
    # Synthetic pre-2018 sentiment from BTC price data
    synthetic=[]
    try:
        btc_hist=http_get("https://api.coingecko.com/api/v3/coins/bitcoin/market_chart?vs_currency=usd&days=max&interval=daily",timeout=20)
        if btc_hist and 'prices' in btc_hist:
            prices=btc_hist['prices']  # [[timestamp_ms, price], ...]
            cutoff=datetime(2018,2,1).timestamp()*1000
            for i in range(30, len(prices)):
                ts_ms=prices[i][0]
                if ts_ms >= cutoff: break  # F&G data exists from here
                price=prices[i][1]
                avg30=sum(p[1] for p in prices[max(0,i-30):i])/min(30,i) if i>0 else price
                avg90=sum(p[1] for p in prices[max(0,i-90):i])/min(90,i) if i>0 else price
                # Derive sentiment: momentum + volatility based
                mom30=(price-avg30)/avg30*100 if avg30>0 else 0
                mom90=(price-avg90)/avg90*100 if avg90>0 else 0
                # Map to 0-100 scale
                raw=50+mom30*0.8+mom90*0.3
                fg_val=max(1,min(99,int(raw)))
                dt=datetime.fromtimestamp(ts_ms/1000)
                if dt.year>=2016:
                    synthetic.append({'value':fg_val,'date':dt.strftime('%Y-%m-%d'),'synthetic':True})
            print(f"    📊 Synthetic sentiment: {len(synthetic)} days (2016-2018)")
    except Exception as e:
        print(f"    ⚠️ Synthetic sentiment failed: {e}")
    
    # Merge: synthetic (oldest first) + full_hist (already newest first, reverse it)
    combined = synthetic + list(reversed(full_hist))
    # Sample to weekly for older data to reduce size
    weekly=[];last_date=''
    for entry in combined:
        d_str=entry['date']
        # Keep daily for last 90 days, weekly for rest
        if len(combined)-combined.index(entry) <= 90:
            weekly.append(entry)
        elif d_str[:7] != last_date[:7] or (len(weekly)==0 or abs(datetime.strptime(d_str,'%Y-%m-%d').toordinal()-datetime.strptime(weekly[-1]['date'],'%Y-%m-%d').toordinal())>=7):
            weekly.append(entry)
            last_date=d_str
    
    return {'status':'ok','current':int(cur.get('value',50)),'label':cur.get('value_classification','?'),'history':hist,'full_history':weekly,'avg_7d':round(sum(vals[:7])/min(7,len(vals))) if vals else 50,'avg_30d':round(sum(vals)/len(vals)) if vals else 50}

def fetch_cmc():
    if not CMC_API_KEY: return {'status':'error'}
    h={'User-Agent':'JustHodl/4.1','X-CMC_PRO_API_KEY':CMC_API_KEY,'Accept':'application/json'}
    d=http_get("https://pro-api.coinmarketcap.com/v1/cryptocurrency/trending/gainers-losers?limit=5&time_period=24h",h)
    r={'status':'ok','top_gainers':[],'top_losers':[]}
    if d and 'data' in d:
        for c in d['data'][:5]: r['top_gainers'].append({'symbol':c.get('symbol','?'),'name':c.get('name','?'),'price':round(c.get('quote',{}).get('USD',{}).get('price',0) or 0,4),'change_24h':sr(c.get('quote',{}).get('USD',{}).get('percent_change_24h',0))})
        for c in sorted(d['data'],key=lambda x:(x.get('quote',{}).get('USD',{}).get('percent_change_24h',0) or 0))[:5]: r['top_losers'].append({'symbol':c.get('symbol','?'),'name':c.get('name','?'),'price':round(c.get('quote',{}).get('USD',{}).get('price',0) or 0,4),'change_24h':sr(c.get('quote',{}).get('USD',{}).get('percent_change_24h',0))})
    return r

def fetch_gas():
    d=http_get("https://api.etherscan.io/api?module=gastracker&action=gasoracle")
    if d and d.get('status')=='1':
        r=d.get('result',{});return {'status':'ok','low':sr(r.get('SafeGasPrice',0),0),'standard':sr(r.get('ProposeGasPrice',0),0),'fast':sr(r.get('FastGasPrice',0),0),'base_fee':sr(r.get('suggestBaseFee',0),1)}
    d2=http_get("https://api.gasprice.io/v1/estimates",timeout=8)
    if d2: return {'status':'ok','low':sr(d2.get('result',{}).get('slow',{}).get('feeCap',0),0),'standard':sr(d2.get('result',{}).get('standard',{}).get('feeCap',0),0),'fast':sr(d2.get('result',{}).get('fast',{}).get('feeCap',0),0),'base_fee':0}
    return {'status':'error'}

def fetch_whales():
    d=http_get("https://blockchain.info/unconfirmed-transactions?format=json",timeout=10)
    if not d: return {'status':'error'}
    L=[]
    for tx in d.get('txs',[])[:200]:
        t=sum(o.get('value',0) for o in tx.get('out',[]))/1e8
        if t>=50: L.append({'hash':tx.get('hash','?')[:16]+'...','btc_amount':round(t,2),'usd_est':fmt(t*95000),'inputs':len(tx.get('inputs',[])),'outputs':len(tx.get('out',[]))})
    L.sort(key=lambda x:x['btc_amount'],reverse=True)
    return {'status':'ok','large_txs':L[:10],'whale_count':len(L),'total_whale_btc':round(sum(t['btc_amount'] for t in L),2)}

def fetch_mvrv():
    d=http_get("https://api.blockchain.info/charts/market-cap?timespan=365days&format=json")
    if not d or 'values' not in d or len(d['values'])<30: return {'status':'error'}
    v=d['values'];cur=v[-1]['y'];a365=sum(x['y'] for x in v)/len(v);a30=sum(x['y'] for x in v[-30:])/30
    mvrv=cur/a365 if a365>0 else 1;mom=(cur-a30)/a30*100 if a30>0 else 0
    sig='OVERVALUED' if mvrv>3 else 'EXPENSIVE' if mvrv>2 else 'FAIR' if mvrv>0.8 else 'UNDERVALUED'
    return {'status':'ok','mvrv_approx':round(mvrv,2),'signal':sig,'market_cap':round(cur),'market_cap_fmt':fmt(cur),'momentum_30d':round(mom,2)}

# ═══ AI INTELLIGENCE ═══
def gen_ai(results, tech):
    if not ANTHROPIC_API_KEY: return {'status':'error','error':'No key'}
    print("  🤖 AI: Reading ALL system data...")
    md=s3_read('data.json') or {};rd=s3_read('repo-data.json') or {};pd=s3_read('predictions.json') or {}
    ki=md.get('ki',md.get('khalid_index','?'));reg=md.get('regime','?')
    sofr=rd.get('metrics',{}).get('SOFR',{}).get('value','?');rrp=rd.get('metrics',{}).get('RRP',{}).get('value','?')
    ps=rd.get('score','?');mlr=pd.get('regime','?');mlrsk=pd.get('risk_score','?')
    
    tl=[]
    for coin,data in tech.get('coins',{}).items():
        l=f"\n=== {coin} ${data.get('price',0):,.6f} | Consensus: {data.get('consensus','?')} | Wyckoff: {data.get('wyckoff_phase','?')} ==="
        for tk,td in data.get('timeframes',{}).items():
            if td.get('status')!='ok': continue
            i=td.get('indicators',{});l+=f"\n  [{tk}] RSI:{i.get('rsi','?')} MACD:{i.get('macd',{}).get('cross','?')} EMA:{i.get('ema',{}).get('trend','?')} ST:{i.get('supertrend',{}).get('trend','?')} BB%:{i.get('bollinger',{}).get('position','?')} Score:{td.get('score','?')}/100 Wyckoff:{td.get('wyckoff','?')}"
            ps2=td.get('patterns',[])
            if ps2: l+=f"\n  Patterns: {', '.join(p['type']+'('+p['bias']+')' for p in ps2)}"
        tl.append(l)
    
    R=results.get('risk_score',{});fg=results.get('fear_greed',{});fn=results.get('funding',{})
    st=results.get('stablecoins',{});gl=results.get('global_market',{});onr=results.get('onchain_ratios',{})
    wh=results.get('whale_txs',{});gas=results.get('eth_gas',{});tvl=results.get('tvl',{})
    
    prompt=f"""You are an elite crypto quantitative analyst with access to the COMPLETE JustHodl.AI financial system. Analyze ALL data and give SPECIFIC predictions with percentages.

═══ MACRO (from main terminal) ═══
Khalid Index: {ki} | Regime: {reg} | SOFR: {sofr} | RRP: {rrp}
Plumbing Stress: {ps} | ML Risk: {mlrsk} | ML Regime: {mlr}

═══ CRYPTO MARKET ═══
Risk: {R.get('score','?')}/100 ({R.get('regime','?')}) | F&G: {fg.get('current','?')} ({fg.get('label','?')})
MCap: {gl.get('total_mcap_fmt','?')} ({gl.get('mcap_change_24h','?')}% 24h)
BTC Dom: {gl.get('btc_dominance','?')}% | TVL: {tvl.get('total_tvl_fmt','?')}

═══ DERIVATIVES ═══
Funding: {fn.get('avg_funding','?')}% | Sent: {fn.get('leverage_sentiment','?')} | L/S: {fn.get('positive_count','?')}/{fn.get('negative_count','?')}

═══ STABLECOINS ═══
Supply: {st.get('total_mcap_fmt','?')} | {st.get('net_signal','?')} | Mint:{st.get('minting_count',0)} Burn:{st.get('burning_count',0)}

═══ ON-CHAIN ═══
MVRV: {onr.get('mvrv_approx','?')} ({onr.get('signal','?')}) | Mom: {onr.get('momentum_30d','?')}%
Whales (50+BTC): {wh.get('whale_count','?')} ({wh.get('total_whale_btc','?')} BTC)
Gas: L{gas.get('low','?')} S{gas.get('standard','?')} F{gas.get('fast','?')}

═══ TECHNICALS (Multi-TF + Wyckoff) ═══
{''.join(tl)}

═══ PROVIDE THIS EXACT FORMAT ═══

**🔮 MARKET REGIME & WYCKOFF PHASE**
State phase for each coin. Is this accumulation, markup, distribution, or markdown?

**📊 RISK ASSESSMENT** (1-10)
Risks from BOTH macro (KI {ki}, SOFR, plumbing) and crypto.

**💰 BTC: [PUMP/DUMP] [+X% to -X%]**
Wyckoff phase. 1-week & 1-month price targets. Key S/R levels. Conviction %.

**💎 ETH: [PUMP/DUMP] [+X% to -X%]**
Same. Include ETH/BTC outlook.

**🐸 PEPE: [PUMP/DUMP] [+X% to -X%]**
Phase, entry zone, target.

**🐕 DOGE: [PUMP/DUMP] [+X% to -X%]**
Phase, entry zone, target.

**🔷 POL: [PUMP/DUMP] [+X% to -X%]**
Phase, entry zone, target.

**🔍 PATTERN ALERTS**
Detected patterns & predictions.

**🐋 SMART MONEY FLOW**
Stablecoin + whale + funding signals.

**📈 TOP 3 TRADES**
Entry, stop, target, R:R, expected % gain, timeframe.

**⚠️ CRASH/PUMP SCENARIOS**
Bear case: what triggers, expected % decline.
Bull case: what triggers, expected % gain.

**🎯 #1 CONVICTION CALL**
Single best trade. Expected % gain. Timeframe.

**📉 MACRO → CRYPTO PIPELINE**
How KI({ki}), SOFR({sofr}), plumbing({ps}) affect crypto. Liquidity expanding or contracting?

Be AGGRESSIVE with predictions. Give SPECIFIC numbers."""

    r=http_post("https://api.anthropic.com/v1/messages",{'model':'claude-sonnet-4-20250514','max_tokens':3000,'messages':[{'role':'user','content':prompt}]},{'Content-Type':'application/json','x-api-key':ANTHROPIC_API_KEY,'anthropic-version':'2023-06-01'},timeout=60)
    if r and 'content' in r:
        return {'status':'ok','analysis':r['content'][0].get('text',''),'model':'claude-sonnet-4-20250514','generated_at':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),'macro':{'khalid_index':ki,'regime':reg,'sofr':sofr,'plumbing':ps,'ml_risk':mlrsk}}
    return {'status':'error','error':'Claude failed'}

# ═══ RISK ═══
def risk(fg,fn,st,gl,tech):
    s=50;sg=[]
    fv=fg.get('current',50) if isinstance(fg,dict) else 50
    if fv<20:s+=15;sg.append('🔴 Extreme Fear')
    elif fv<35:s+=8;sg.append('🟡 Fearful')
    elif fv>80:s+=12;sg.append('🔴 Extreme Greed')
    elif fv>65:s+=5;sg.append('🟡 Greedy')
    af=fn.get('avg_funding',0) if isinstance(fn,dict) else 0
    if abs(af)>0.05:s+=10;sg.append('🔴 Extreme funding')
    elif abs(af)>0.02:s+=5;sg.append('🟡 Elevated funding')
    ns=st.get('net_signal','NEUTRAL') if isinstance(st,dict) else 'NEUTRAL'
    if ns=='OUTFLOW':s+=8;sg.append('🔴 Stablecoin outflows')
    elif ns=='INFLOW':s-=5;sg.append('🟢 Stablecoin inflows')
    mc=gl.get('mcap_change_24h',0) if isinstance(gl,dict) else 0
    if mc<-5:s+=10;sg.append('🔴 MCap -5%+')
    elif mc<-2:s+=5;sg.append('🟡 Declining')
    elif mc>5:s-=3;sg.append('🟢 Strong rally')
    coins=tech.get('coins',{}) if isinstance(tech,dict) else {}
    bc=sum(1 for c in coins.values() if c.get('consensus','').startswith('S'))
    if bc>=4:s+=8;sg.append('🔴 4+ coins bearish')
    s=max(0,min(100,s))
    rg='CRITICAL' if s>=80 else 'HIGH' if s>=65 else 'ELEVATED' if s>=50 else 'MODERATE' if s>=35 else 'LOW'
    return {'score':s,'regime':rg,'action':'REDUCE' if s>=80 else 'HEDGE' if s>=65 else 'CAUTION' if s>=50 else 'NORMAL' if s>=35 else 'ACCUMULATE','signals':sg}

# ═══ MAIN ═══
def lambda_handler(event, context):
    start=time.time()
    print("═══ CRYPTO INTELLIGENCE v4.1 ═══")
    R={}
    print("  Phase 1: 14 data sources...")
    with ThreadPoolExecutor(max_workers=8) as ex:
        fs={ex.submit(fetch_stablecoins):'stablecoins',ex.submit(fetch_tvl):'tvl',ex.submit(fetch_dex):'dex',ex.submit(fetch_yields):'yields',ex.submit(fetch_funding):'funding',ex.submit(fetch_oi):'open_interest',ex.submit(fetch_global):'global_market',ex.submit(fetch_top_coins):'top_coins',ex.submit(fetch_onchain):'btc_onchain',ex.submit(fetch_fg):'fear_greed',ex.submit(fetch_cmc):'cmc_movers',ex.submit(fetch_gas):'eth_gas',ex.submit(fetch_whales):'whale_txs',ex.submit(fetch_mvrv):'onchain_ratios'}
        for f in as_completed(fs):
            k=fs[f]
            try:R[k]=f.result();print(f"    {'✅' if R[k].get('status')=='ok' else '⚠️'} {k}")
            except Exception as e:R[k]={'status':'error','error':str(e)};print(f"    ❌ {k}")
    
    print(f"  Phase 2: Technicals...")
    tech=fetch_technicals();R['technicals']=tech
    ok_t=sum(1 for c in tech.get('coins',{}).values() for t in c.get('timeframes',{}).values() if t.get('status')=='ok')
    print(f"    ✅ {ok_t}/{len(TARGET_COINS)*len(TIMEFRAMES)} TFs")
    
    rs=risk(R.get('fear_greed',{}),R.get('funding',{}),R.get('stablecoins',{}),R.get('global_market',{}),tech)
    R['risk_score']=rs
    
    print("  Phase 3: AI...")
    ai=gen_ai(R,tech);R['ai_intelligence']=ai
    
    out={'generated_at':datetime.now(timezone.utc).strftime('%Y-%m-%dT%H:%M:%SZ'),'fetch_time':round(time.time()-start,1),'version':'4.1',**R}
    try:s3.put_object(Bucket=S3_BUCKET,Key='crypto-intel.json',Body=json.dumps(out,default=str),ContentType='application/json',CacheControl='max-age=60')
    except Exception as e:print(f"  S3 ERR: {e}")
    
    ok=len([k for k,v in R.items() if isinstance(v,dict) and v.get('status')=='ok'])
    print(f"═══ {ok}/{len(R)} | Risk {rs['score']} | {round(time.time()-start,1)}s ═══")
    return {'statusCode':200,'body':json.dumps({'status':'published','risk':rs['score'],'regime':rs['regime'],'ok':ok,'total':len(R),'ai':ai.get('status')=='ok','tfs':ok_t,'time':round(time.time()-start,1)})}
