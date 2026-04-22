import json,os,urllib.request,urllib.error,boto3,traceback,time
from datetime import datetime,timedelta,timezone

S3_BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')
FRED_KEY=os.environ.get('FRED_API_KEY','2f057499936072679d8843d7fce99989')
POLYGON_KEY=os.environ.get('POLYGON_API_KEY','zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d')
ANTHROPIC_KEY=os.environ.get('ANTHROPIC_API_KEY','')
s3=boto3.client('s3',region_name='us-east-1')

def cors_response(status,body):
    return{'statusCode':status,'headers':{'Content-Type':'application/json','Access-Control-Allow-Origin':'*','Access-Control-Allow-Methods':'*','Access-Control-Allow-Headers':'Content-Type'},'body':json.dumps(body,default=str)}

def load_config():
    try:obj=s3.get_object(Bucket=S3_BUCKET,Key='data/khalid-config.json');return json.loads(obj['Body'].read())
    except:return{"metrics":[],"categories":[],"version":1}

def save_config(config):
    s3.put_object(Bucket=S3_BUCKET,Key='data/khalid-config.json',Body=json.dumps(config,indent=2).encode('utf-8'),ContentType='application/json')

# ═══════════════════════════════════════════
# FRED (with rate limit protection)
# ═══════════════════════════════════════════
def fetch_fred_series(series_id,days_back=400):
    end=datetime.now();start=end-timedelta(days=days_back)
    url=f"https://api.stlouisfed.org/fred/series/observations?series_id={series_id}&api_key={FRED_KEY}&file_type=json&observation_start={start.strftime('%Y-%m-%d')}&observation_end={end.strftime('%Y-%m-%d')}&sort_order=desc"
    for attempt in range(3):
        try:
            req=urllib.request.Request(url,headers={'User-Agent':'JustHodl/1.0'})
            with urllib.request.urlopen(req,timeout=15) as resp:
                data=json.loads(resp.read())
                obs=[{'date':o['date'],'value':float(o['value'])}for o in data.get('observations',[])if o['value']!='.']
                if obs:return obs
                else:print(f"FRED {series_id}: 0 observations");return[]
        except urllib.error.HTTPError as e:
            if e.code==429:
                wait=3*(attempt+1);print(f"FRED rate limit {series_id}, wait {wait}s");time.sleep(wait);continue
            print(f"FRED HTTP {e.code} {series_id}");return[]
        except Exception as e:
            print(f"FRED err {series_id}:{e}");return[]
    print(f"FRED {series_id}: all retries failed");return[]

# ═══════════════════════════════════════════
# POLYGON
# ═══════════════════════════════════════════
def fetch_polygon_ticker(ticker,days_back=400):
    end=datetime.now();start=end-timedelta(days=days_back)
    url=f"https://api.polygon.io/v2/aggs/ticker/{ticker}/range/1/day/{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}?adjusted=true&sort=desc&limit=400&apiKey={POLYGON_KEY}"
    try:
        with urllib.request.urlopen(urllib.request.Request(url),timeout=15) as resp:
            data=json.loads(resp.read());return[{'date':datetime.fromtimestamp(r['t']/1000).strftime('%Y-%m-%d'),'value':r['c']}for r in data.get('results',[])]
    except Exception as e:print(f"Polygon err {ticker}:{e}");return[]

# ═══════════════════════════════════════════
# ECB — FIXED URL construction + multiple fallbacks
# ═══════════════════════════════════════════
def fetch_ecb_series(ecb_key,days_back=730):
    """ECB SDMX API. Key format: DATAFLOW.dim1.dim2...dimN
    URL format: /service/data/DATAFLOW/dim1.dim2...dimN"""
    parts=ecb_key.split('.')
    dataflow=parts[0]
    series_key='.'.join(parts[1:])
    
    end=datetime.now();start=end-timedelta(days=days_back)
    start_y=str(start.year)
    
    # Try multiple URL patterns — ECB API is inconsistent across datasets
    urls=[
        # Pattern 1: standard SDMX with yearly startPeriod (works for monthly/quarterly)
        f"https://data-api.ecb.europa.eu/service/data/{dataflow}/{series_key}?startPeriod={start_y}&format=csvdata",
        # Pattern 2: with full date range
        f"https://data-api.ecb.europa.eu/service/data/{dataflow}/{series_key}?startPeriod={start.strftime('%Y-%m-%d')}&endPeriod={end.strftime('%Y-%m-%d')}&format=csvdata",
        # Pattern 3: monthly period format
        f"https://data-api.ecb.europa.eu/service/data/{dataflow}/{series_key}?startPeriod={start.strftime('%Y-%m')}&format=csvdata",
        # Pattern 4: no date filter (get all recent)
        f"https://data-api.ecb.europa.eu/service/data/{dataflow}/{series_key}?detail=dataonly&lastNObservations=50&format=csvdata",
    ]
    
    for i,url in enumerate(urls):
        try:
            print(f"ECB try {i+1}/4: {dataflow}/{series_key[:25]}...")
            req=urllib.request.Request(url,headers={'User-Agent':'JustHodl/1.0','Accept':'text/csv'})
            with urllib.request.urlopen(req,timeout=25) as resp:
                text=resp.read().decode('utf-8')
                obs=parse_ecb_csv(text)
                if obs:
                    print(f"ECB OK {dataflow}/{series_key[:25]}: {len(obs)} obs, latest={obs[0]['date']}: {obs[0]['value']}")
                    return obs
                print(f"ECB try {i+1}: parsed 0 obs")
        except urllib.error.HTTPError as e:
            print(f"ECB try {i+1} HTTP {e.code}: {dataflow}/{series_key[:25]}")
        except Exception as e:
            print(f"ECB try {i+1} err: {e}")
    
    # Pattern 5: JSON format as last resort
    try:
        url=f"https://data-api.ecb.europa.eu/service/data/{dataflow}/{series_key}?startPeriod={start_y}&format=jsondata"
        print(f"ECB JSON fallback: {dataflow}/{series_key[:25]}...")
        req=urllib.request.Request(url,headers={'Accept':'application/json','User-Agent':'JustHodl/1.0'})
        with urllib.request.urlopen(req,timeout=25) as resp:
            data=json.loads(resp.read())
            obs=parse_ecb_json(data)
            if obs:
                print(f"ECB JSON OK: {len(obs)} obs")
                return obs
    except Exception as e:
        print(f"ECB JSON err: {e}")
    
    print(f"ECB ALL FAILED: {ecb_key}")
    return[]

def parse_ecb_csv(text):
    obs=[]
    lines=text.strip().split('\n')
    if len(lines)<2:return obs
    header=[h.strip().strip('"').upper() for h in lines[0].split(',')]
    tp_idx=ov_idx=None
    for i,h in enumerate(header):
        if 'TIME' in h and 'PERIOD' in h:tp_idx=i
        elif 'OBS_VALUE' in h or h=='OBS_VALUE':ov_idx=i
    if tp_idx is None or ov_idx is None:
        # Try tab-separated
        header=[h.strip().strip('"').upper() for h in lines[0].split('\t')]
        for i,h in enumerate(header):
            if 'TIME' in h and 'PERIOD' in h:tp_idx=i
            elif 'OBS_VALUE' in h:ov_idx=i
        if tp_idx is None or ov_idx is None:
            print(f"ECB CSV: cant find TIME_PERIOD/OBS_VALUE in: {header[:8]}")
            return obs
        sep='\t'
    else:
        sep=','
    
    for line in lines[1:]:
        cols=line.split(sep)
        if len(cols)<=max(tp_idx,ov_idx):continue
        try:
            period=cols[tp_idx].strip().strip('"')
            val=cols[ov_idx].strip().strip('"')
            if not val or val in('NaN','','NA'):continue
            date=period_to_date(period)
            if date:obs.append({'date':date,'value':float(val)})
        except:continue
    obs.sort(key=lambda x:x['date'],reverse=True)
    return obs

def parse_ecb_json(data):
    obs=[]
    datasets=data.get('dataSets',[])
    if not datasets:return obs
    ds=datasets[0]
    dims=data.get('structure',{}).get('dimensions',{}).get('observation',[])
    time_vals=None
    for d in dims:
        if d.get('id')=='TIME_PERIOD':time_vals=d.get('values',[]);break
    if not time_vals:return obs
    series=ds.get('series',{})
    for sk,sv in series.items():
        observations=sv.get('observations',{})
        for idx_str,val_arr in observations.items():
            try:
                idx=int(idx_str)
                val=val_arr[0]if isinstance(val_arr,list)else val_arr
                if val is None:continue
                if idx<len(time_vals):
                    period=time_vals[idx].get('id','')
                    date=period_to_date(period)
                    if date:obs.append({'date':date,'value':float(val)})
            except:continue
    obs.sort(key=lambda x:x['date'],reverse=True)
    return obs

def period_to_date(period):
    if not period:return None
    p=period.strip()
    if len(p)==10:return p
    if len(p)==7:return p+'-01'
    if len(p)==4:return p+'-01-01'
    if 'Q' in p.upper():
        parts=p.split('-')
        if len(parts)==2:
            yr=parts[0];q=parts[1].upper().replace('Q','')
            qmap={'1':'01','2':'04','3':'07','4':'10'}
            return f"{yr}-{qmap.get(q,'01')}-01"
    return p[:10]if len(p)>=10 else None

# ═══════════════════════════════════════════
# CALCULATIONS
# ═══════════════════════════════════════════
def calc_pct_changes(obs):
    if not obs or len(obs)<2:return{'current':None,'1w':None,'1m':None,'3m':None,'6m':None,'1y':None}
    cur=obs[0]['value'];cd=datetime.strptime(obs[0]['date'][:10],'%Y-%m-%d')
    periods={'1w':7,'1m':30,'3m':90,'6m':180,'1y':365};changes={'current':cur}
    for pn,days in periods.items():
        tgt=cd-timedelta(days=days);best=None;bd=float('inf')
        for o in obs:
            try:diff=abs((datetime.strptime(o['date'][:10],'%Y-%m-%d')-tgt).days)
            except:continue
            if diff<bd:bd=diff;best=o
        if best and best['value']!=0 and bd<=days*0.5:changes[pn]=round(((cur-best['value'])/abs(best['value']))*100,2)
        else:changes[pn]=None
    return changes

def calc_risk(md,config):
    tw=0;rs=0
    for m in config.get('metrics',[]):
        if not m.get('enabled',True):continue
        d=md.get(m['id'])
        if not d:continue
        w=m.get('weight',5);f=m.get('flash','red_up')
        c=d.get('3m')if d.get('3m')is not None else d.get('1m')
        if c is None:continue
        if f=='red_up':n=min(max((c+5)/10*50,0),100)
        else:n=min(max((-c+5)/10*50,0),100)
        rs+=n*w;tw+=w
    return round(rs/tw,1)if tw>0 else 50

def refresh_data(config):
    md={};errors=[];call_count=0
    for m in config.get('metrics',[]):
        if not m.get('enabled',True):continue
        mid=m['id'];src=m.get('source','fred')
        print(f"[{call_count+1}] {mid} ({src})")
        try:
            if src=='fred':
                # Rate limit: pause every 30 FRED calls
                call_count+=1
                if call_count>1 and call_count%20==0:
                    print(f"  Rate limit pause 4s...");time.sleep(4)
                obs=fetch_fred_series(mid)
            elif src=='polygon':obs=fetch_polygon_ticker(mid)
            elif src=='ecb':
                ecb_key=m.get('ecb_key','')
                if not ecb_key:print(f"  No ecb_key");errors.append(mid);continue
                obs=fetch_ecb_series(ecb_key)
            else:obs=fetch_fred_series(mid)
            if obs:md[mid]=calc_pct_changes(obs)
            else:errors.append(mid)
        except Exception as e:print(f"  Err:{e}");errors.append(mid)
    ri=calc_risk(md,config)
    cr={}
    for cat in config.get('categories',[]):
        cm=[m for m in config['metrics']if m.get('category')==cat and m.get('enabled',True)]
        if cm:cr[cat]=calc_risk(md,{'metrics':cm,'categories':[cat]})
    now=datetime.now(timezone(timedelta(hours=-5)))
    result={'metrics':md,'risk_index':ri,'category_risks':cr,'errors':errors,'generated':now.isoformat(),'count':len(md),'version':config.get('version',1)}
    s3.put_object(Bucket=S3_BUCKET,Key='data/khalid-metrics.json',Body=json.dumps(result,indent=2).encode('utf-8'),ContentType='application/json')
    print(f"\n{'='*50}\nPUBLISHED: {len(md)} OK, {len(errors)} errors, risk={ri}\n{'='*50}")
    if errors:print(f"ERRORS: {errors}")
    return result

# ═══════════════════════════════════════════
# AI ANALYSIS
# ═══════════════════════════════════════════
def run_ai_analysis(config, data):
    if not ANTHROPIC_KEY:return{"error":"No Anthropic API key"}
    lines=[]
    for cat in config.get('categories',[]):
        cat_risk=data.get('category_risks',{}).get(cat,'N/A')
        lines.append(f"\n=== {cat} (Risk: {cat_risk}/100) ===")
        for m in config['metrics']:
            if m.get('category')!=cat or not m.get('enabled',True):continue
            d=data.get('metrics',{}).get(m['id'],{})
            if not d:
                lines.append(f"  {m['name']} ({m['id']}): NO DATA")
                continue
            flash='Rising=BAD'if m.get('flash')=='red_up'else'Rising=GOOD'
            lines.append(f"  {m['name']} ({m['id']}): Current={d.get('current','N/A')} | 1W={d.get('1w','N/A')}% | 1M={d.get('1m','N/A')}% | 3M={d.get('3m','N/A')}% | 6M={d.get('6m','N/A')}% | 1Y={d.get('1y','N/A')}% [{flash}, W:{m.get('weight',5)}]")
    metrics_text="\n".join(lines)
    error_count=len(data.get('errors',[]))
    total=data.get('count',0)
    prompt=f"""You are an elite institutional macro strategist and crypto market analyst. Analyze these {total} real-time financial metrics from FRED, ECB, and Polygon covering the COMPLETE financial plumbing of both the US and European economies including:
- Fed & ECB balance sheets, monetary policy rates
- US & Euro area money supply (M1, M3)
- Dealer stress, repo markets, overnight rates (SOFR, EFFR, ESTR)
- Bank credit conditions, lending, delinquencies (US + Europe)
- Treasury/sovereign yields and curves
- Financial stress indices (VIX, HY OAS, NFCI, STL FSI)
- Industrial production and employment (US + Europe)
- Dollar/FX and cross-currency dynamics
- ECB inflation (HICP), economic sentiment

OVERALL RISK INDEX: {data.get('risk_index',50)}/100
METRICS WITH DATA: {total} | ERRORS: {error_count}

COMPLETE METRICS:
{metrics_text}

HISTORICAL REFERENCE POINTS:
- 2008 GFC: Credit freeze, HY >2000bp, VIX >80, dealer fails exploded, ECB emergency LTRO
- 2011 Euro Debt Crisis: Sovereign spreads blowout, ECB SMP, Greek default risk
- 2020 COVID: Liquidity crisis, Fed+ECB emergency facilities, repo stress
- 2022 Crypto Winter: Fed+ECB hiking, QT, dollar strength, European energy crisis
- BTC Tops: Nov 2013, Dec 2017, Nov 2021 — peak global liquidity, loose conditions, weak dollar
- BTC Bottoms: Jan 2015, Dec 2018, Nov 2022 — peak tightening, max stress, strong dollar

Compare ECB vs Fed policy divergence — drives EUR/USD and global capital flows impacting crypto.

Return ONLY valid JSON:
{{"plumbing_health":{{"score":<1-100>,"grade":"<A+ to F>","summary":"<2-3 sentences covering US+Europe>","key_signals":["<6 signals with numbers>"],"stress_points":["<areas of strain>"],"positive_signs":["<areas of strength>"]}},"crisis_comparison":{{"current_vs_2008":{{"similarity_pct":<0-100>,"summary":"<2-3 sentences>","key_differences":["<3 differences>"]}},"current_vs_2020":{{"similarity_pct":<0-100>,"summary":"<2-3 sentences>","key_differences":["<3 differences>"]}},"current_vs_2022":{{"similarity_pct":<0-100>,"summary":"<2-3 sentences>","key_differences":["<3 differences>"]}},"closest_historical_analog":"<period>","crisis_probability_6mo":<0-100>,"crisis_type_if_occurs":"<type>"}},"risk_regime":{{"stance":"<RISK-ON|RISK-OFF|NEUTRAL|TRANSITIONING>","confidence":<0-100>,"summary":"<3-4 sentences citing US+ECB metrics>","risk_on_signals":["<signals>"],"risk_off_signals":["<signals>"],"regime_duration_estimate":"<duration>","trigger_to_flip":"<trigger>","ecb_fed_divergence":"<policy divergence analysis>"}},"crypto_outlook":{{"btc_regime":"<ACCUMULATE|HOLD|DISTRIBUTE|AVOID>","cycle_position":"<Early Bull|Mid Bull|Late Bull|Blow-off Top|Early Bear|Capitulation|Accumulation>","cycle_confidence":<0-100>,"summary":"<4-5 sentences using global liquidity>","btc_correlation_to_liquidity":"<global liquidity impact>","dollar_impact_on_crypto":"<DXY+EUR/USD impact>","key_metrics_for_crypto":["<6 metrics including ECB>"],"comparison_to_past_tops":"<vs Nov 2021>","comparison_to_past_bottoms":"<vs Nov 2022>","expected_performance_3mo":"<outlook>","expected_performance_12mo":"<outlook>","biggest_risk_for_crypto":"<risk>","biggest_catalyst_for_crypto":"<catalyst>"}},"boom_bust_cycle":{{"phase":"<phase>","confidence":<0-100>,"position_in_cycle":<0-100>,"summary":"<3-4 sentences>","leading_indicators":["<indicators>"],"cycle_risks":["<risks>"]}},"risk_analysis":{{"overall_risk":"<LOW|MODERATE|ELEVATED|HIGH|EXTREME>","risk_score":<1-100>,"systemic_risk":"<US+Europe>","liquidity_risk":"<global>","credit_risk":"<US+Europe>","dollar_risk":"<USD+EUR>","dealer_stress":"<dealer+repo>","ecb_risk":"<European risks>","tail_risks":["<risks>"],"risk_trajectory":"<IMPROVING|STABLE|DETERIORATING>"}},"portfolio_recommendation":{{"regime":"<Risk-On|Neutral|Risk-Off|Defensive|Crisis>","conviction":"<LOW|MEDIUM|HIGH>","summary":"<3-4 sentences>","allocations":{{"us_equities":{{"weight":<0-100>,"bias":"<bias>","reasoning":"<why>"}},"european_equities":{{"weight":<0-100>,"bias":"<bias>","reasoning":"<why>"}},"international_equities":{{"weight":<0-100>,"bias":"<DM|EM|avoid>","reasoning":"<why>"}},"us_treasuries":{{"weight":<0-100>,"duration":"<duration>","reasoning":"<why>"}},"european_bonds":{{"weight":<0-100>,"bias":"<core|periphery|avoid>","reasoning":"<why>"}},"credit":{{"weight":<0-100>,"quality":"<quality>","reasoning":"<why>"}},"gold_commodities":{{"weight":<0-100>,"bias":"<bias>","reasoning":"<why>"}},"crypto":{{"weight":<0-100>,"bias":"<BTC heavy|ETH heavy|altcoins|avoid>","reasoning":"<why>"}},"cash":{{"weight":<0-100>,"reasoning":"<why>"}},"dollar_position":{{"stance":"<long|neutral|short>","reasoning":"<why>"}},"eur_position":{{"stance":"<long|neutral|short>","reasoning":"<why>"}}}},"top_trades":[{{"trade":"<idea>","rationale":"<why>","risk":"<risk>"}},{{"trade":"<idea>","rationale":"<why>","risk":"<risk>"}},{{"trade":"<idea>","rationale":"<why>","risk":"<risk>"}}],"hedges":["<hedge>","<hedge>"]}},"outlook":{{"1_month":"<outlook>","3_month":"<outlook>","6_month":"<outlook>","12_month":"<outlook>","biggest_risk":"<risk>","biggest_opportunity":"<opportunity>"}}}}"""

    req_body=json.dumps({"model":"claude-sonnet-4-20250514","max_tokens":7000,"messages":[{"role":"user","content":prompt}]}).encode('utf-8')
    req=urllib.request.Request("https://api.anthropic.com/v1/messages",data=req_body,headers={"Content-Type":"application/json","x-api-key":ANTHROPIC_KEY,"anthropic-version":"2023-06-01"},method="POST")
    try:
        print("Calling Claude...")
        with urllib.request.urlopen(req,timeout=120) as resp:
            result=json.loads(resp.read());text=""
            for block in result.get("content",[]):
                if block.get("type")=="text":text+=block["text"]
            text=text.strip()
            if text.startswith("```"):text=text.split("\n",1)[1]if"\n"in text else text[3:]
            if text.endswith("```"):text=text[:-3]
            analysis=json.loads(text.strip())
            analysis['generated']=datetime.now(timezone(timedelta(hours=-5))).isoformat()
            s3.put_object(Bucket=S3_BUCKET,Key='data/khalid-analysis.json',Body=json.dumps(analysis,indent=2).encode('utf-8'),ContentType='application/json')
            print(f"AI: grade={analysis.get('plumbing_health',{}).get('grade','?')}, crypto={analysis.get('crypto_outlook',{}).get('btc_regime','?')}")
            return analysis
    except Exception as e:print(f"AI err:{e}");traceback.print_exc();return{"error":str(e)}

# ═══════════════════════════════════════════
# HANDLER
# ═══════════════════════════════════════════
def lambda_handler(event,context):
    print(f"Khalid Metrics: {datetime.now()}")
    hm=None;path='/';body=None
    if 'requestContext' in event and 'http' in event.get('requestContext',{}):
        hm=event['requestContext']['http']['method'];path=event.get('rawPath','/');body=event.get('body','')
        if event.get('isBase64Encoded')and body:
            import base64;body=base64.b64decode(body).decode('utf-8')
    elif 'httpMethod' in event:hm=event['httpMethod'];path=event.get('path','/');body=event.get('body','')
    if hm=='OPTIONS':return cors_response(200,{'status':'ok'})
    if hm:
        config=load_config()
        if hm=='GET':
            if path=='/config':return cors_response(200,config)
            elif path=='/data':
                try:obj=s3.get_object(Bucket=S3_BUCKET,Key='data/khalid-metrics.json');return cors_response(200,json.loads(obj['Body'].read()))
                except:return cors_response(200,{'metrics':{},'risk_index':50})
            elif path=='/analysis':
                try:obj=s3.get_object(Bucket=S3_BUCKET,Key='data/khalid-analysis.json');return cors_response(200,json.loads(obj['Body'].read()))
                except:return cors_response(200,{'error':'No analysis yet'})
            elif path=='/refresh':return cors_response(200,refresh_data(config))
            elif path=='/analyze':
                try:obj=s3.get_object(Bucket=S3_BUCKET,Key='data/khalid-metrics.json');data=json.loads(obj['Body'].read())
                except:data={'metrics':{},'risk_index':50}
                return cors_response(200,run_ai_analysis(config,data))
            else:
                try:obj=s3.get_object(Bucket=S3_BUCKET,Key='data/khalid-metrics.json');data=json.loads(obj['Body'].read())
                except:data={'metrics':{},'risk_index':50}
                try:obj2=s3.get_object(Bucket=S3_BUCKET,Key='data/khalid-analysis.json');analysis=json.loads(obj2['Body'].read())
                except:analysis=None
                return cors_response(200,{'config':config,'data':data,'analysis':analysis})
        elif hm in('POST','PUT'):
            try:
                nc=json.loads(body)if body else{}
                if 'metrics' in nc:config['metrics']=nc['metrics']
                if 'categories' in nc:config['categories']=nc['categories']
                config['version']=config.get('version',0)+1;save_config(config);result=refresh_data(config)
                return cors_response(200,{'status':'saved','config':config,'data':result})
            except Exception as e:return cors_response(400,{'error':str(e)})
    try:
        config=load_config();result=refresh_data(config)
        analysis=run_ai_analysis(config,result)
        grade=analysis.get('plumbing_health',{}).get('grade','?')if isinstance(analysis,dict)else'?'
        phase=analysis.get('boom_bust_cycle',{}).get('phase','?')if isinstance(analysis,dict)else'?'
        crypto=analysis.get('crypto_outlook',{}).get('btc_regime','?')if isinstance(analysis,dict)else'?'
        errs=len(result.get('errors',[]))
        return{'statusCode':200,'body':json.dumps({'status':'refreshed+analyzed','metrics':result['count'],'risk_index':result['risk_index'],'grade':grade,'phase':phase,'crypto':crypto,'errors':errs})}
    except Exception as e:
        print(f"ERROR:{e}");traceback.print_exc();return{'statusCode':500,'body':json.dumps({'error':str(e)})}
