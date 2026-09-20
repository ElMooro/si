"""Original-source cross-market research; descriptive comparisons confer no trade authority."""
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,InvalidOperation,localcontext,ROUND_HALF_EVEN
import hashlib,json
from urllib.parse import parse_qs,urlsplit

CONTRACT='global-stress-research.v1'
PREFIX='data/global-stress-research/'
CURRENT='data/global-stress.json'
PERMISSIONS=dict(calls_eligible=False,sizing_eligible=False,execution_eligible=False)
# Reviewed security identities. USD ETF returns are not local-currency country indices.
INSTRUMENTS={
 'SPY':('US78462F1030','AMEX','US equity','Equity'),
 'FEZ':('US78463X2027','AMEX','Euro-area equity','Equity'),
 'EWU':('US46435G3341','AMEX','UK equity','Equity'),
 'EWJ':('US46434G8226','AMEX','Japan equity','Equity'),
 'MCHI':('US46429B6719','NASDAQ','China equity','Equity'),
 'INDA':('US46429B5984','CBOE','India equity','Equity'),
 'EWY':('US4642867729','AMEX','Korea equity','Equity'),
 'EEM':('US4642872349','AMEX','Emerging-market equity','Equity'),
 'IEF':('US4642874402','NASDAQ','US 7–10 year Treasuries','Bond'),
 'LQD':('US4642872422','AMEX','US investment-grade credit','Bond'),
 'HYG':('US4642885135','AMEX','US high-yield credit','Bond'),
 'BWX':('US78464A5166','AMEX','International government bonds','Bond'),
 'EMB':('US4642882819','NASDAQ','USD emerging-market bonds','Bond'),
 'GLD':('US78463V1070','AMEX','Gold ETF','Gold'),
}
SERIES=('BAMLH0A1HYBB','BAMLH0A2HYB','BAMLH0A3HYC','BAMLEMCBPIOAS','BAMLEMHBHYCRPIOAS','DGS10',
    'BAMLH0A0HYM2','BAMLC0A0CM','VIXCLS','T10Y2Y')
LONG_HISTORY=frozenset(('BAMLH0A0HYM2','BAMLC0A0CM','VIXCLS','T10Y2Y'))

def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(v):return hashlib.sha256(encoded(v)).hexdigest()
def clock(v):
    at=datetime.fromisoformat(v.replace('Z','+00:00'))
    if at.tzinfo is None:raise ValueError('timezone required')
    return at.astimezone(timezone.utc)
def number(v):
    if v in (None,'','.') or isinstance(v,bool):return None
    try:
        n=Decimal(str(v))
        return n if n.is_finite() and abs(n)<Decimal('1e40') else None
    except (InvalidOperation,ValueError):return None
def exported(value):
    """A fixed decimal policy removes platform libm/FMA differences without relaxing replay."""
    if value is None:return None
    rounded=value.quantize(Decimal('1e-12'),rounding=ROUND_HALF_EVEN)
    return float(rounded) if rounded else 0.0
def safe_url(v,path):
    u=urlsplit(v)
    host='api.stlouisfed.org' if path.startswith('/fred/') else 'financialmodelingprep.com'
    if u.scheme!='https' or u.netloc!=host or u.path!=path or u.fragment:raise ValueError('provider request identity differs')
    q=parse_qs(u.query,keep_blank_values=True)
    if any(len(a)!=1 or k.lower() in ('api_key','apikey','token','authorization') for k,a in q.items()):raise ValueError('ambiguous or credential-bearing query')
    return {k:v[0] for k,v in q.items()}
def original(inputs,bodies,name,path,query):
    ref=inputs['sources'].get(name) or {}
    if ref.get('status')!='captured':raise ValueError('source unavailable')
    raw=bodies[name]
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('original bytes differ')
    if not 0<=(clock(inputs['generated_at'])-clock(ref['acquired_at'])).total_seconds()<=3600:raise ValueError('acquisition outside run')
    if hashlib.sha256(ref['request_url'].encode()).hexdigest()!=ref['request_sha256'] or safe_url(ref['request_url'],path)!=query:raise ValueError('complete request binding differs')
    return json.loads(raw),ref
def quality(day,evaluation,available=True):
    age=(date.fromisoformat(evaluation)-date.fromisoformat(day)).days
    return {'status':'unavailable' if not available else 'stale' if age>7 else 'fresh','age_days':age,'max_age_days':7,
        'basis':'observation_date','note':'Calendar-age ceiling; exchange session completeness and first availability are not established.'}
def metric(value,unit,rows,formula,reason=None):
    return {'value':value,'unit':unit,'start_date':rows[0]['date'] if rows else None,'end_date':rows[-1]['date'] if rows else None,
        'n_observations':len(rows),'formula':formula,'reason':reason,**PERMISSIONS}
def performance(rows,field,intervals):
    window=rows[-intervals-1:]
    valid=len(window)==intervals+1 and all(r[field] is not None for r in window)
    value=exported(100*(Decimal(window[-1][field])/Decimal(window[0][field])-1)) if valid else None
    return metric(value,'percent',window,'100 * (last / first - 1); '+str(intervals)+' observed intervals',None if valid else 'insufficient_or_missing_observations')
def interval_returns(rows,field):
    out={}
    for a,b in zip(rows,rows[1:]):
        gap=(date.fromisoformat(b['date'])-date.fromisoformat(a['date'])).days
        if a[field] is not None and b[field] is not None and 0<gap<=7:
            out[(a['date'],b['date'])]=(Decimal(b[field])/Decimal(a[field])).ln()
    return out
def instrument(inputs,bodies,symbol):
    profile,pr=original(inputs,bodies,'profile:'+symbol,'/stable/profile',{'symbol':symbol})
    isin,exchange,label,group=INSTRUMENTS[symbol]
    if not isinstance(profile,list) or len(profile)!=1:raise ValueError('unique instrument identity required')
    p=profile[0]
    if p.get('isEtf') is not True or any(p.get(k)!=v for k,v in [('symbol',symbol),('isin',isin),('exchange',exchange),('currency','USD')]):raise ValueError('reviewed security identity differs')
    data={};refs={'profile':pr}
    for kind,field in [('full','close'),('dividend-adjusted','adjClose')]:
        doc,ref=original(inputs,bodies,kind+':'+symbol,'/stable/historical-price-eod/'+kind,
            {'symbol':symbol,'from':inputs['history_start'],'to':inputs['price_end']})
        if not isinstance(doc,list) or not doc or len(doc)>2000:raise ValueError('price history unavailable or oversized')
        dated={};last=None
        for i,r in enumerate(doc):
            day=str(date.fromisoformat(r['date']))
            if r.get('symbol')!=symbol or not inputs['history_start']<=day<=inputs['price_end'] or day in dated:raise ValueError('price identity, duplicate or bounds differ')
            if last is not None and day>=last:raise ValueError('provider price ordering differs')
            value=number(r.get(field))
            if r.get(field) not in (None,'','.') and value is None or value is not None and value<=0:raise ValueError('invalid price observation')
            dated[day]={'decimal':str(value) if value is not None else None,'row_index':i};last=day
        data[kind]=dated;refs[kind]=ref
    rows=[]
    for day in sorted(set(data['full'])|set(data['dividend-adjusted'])):
        a,b=data['full'].get(day,{}),data['dividend-adjusted'].get(day,{})
        rows.append({'date':day,'close':a.get('decimal'),'adjusted_close':b.get('decimal'),
            'price_row_index':a.get('row_index'),'adjusted_row_index':b.get('row_index')})
    latest=rows[-1];w20=rows[-21:];returns=interval_returns(w20,'adjusted_close')
    vol=None
    if len(returns)==20:
        mean=sum(returns.values())/20
        variance=sum((v-mean)**2 for v in returns.values())/19
        vol=exported((variance*252).sqrt()*100)
    w252=rows[-252:];w200=rows[-200:]
    dd_ok=len(w252)==252 and all(r['adjusted_close'] is not None for r in w252)
    ma_ok=len(w200)==200 and all(r['adjusted_close'] is not None for r in w200)
    dd=exported(100*(Decimal(latest['adjusted_close'])/max(Decimal(r['adjusted_close']) for r in w252)-1)) if dd_ok else None
    trend=exported(100*(Decimal(latest['adjusted_close'])/(sum(Decimal(r['adjusted_close']) for r in w200)/200)-1)) if ma_ok else None
    return {'symbol':symbol,'isin':isin,'exchange':exchange,'currency':'USD','label':label,'group':group,'title':p.get('companyName'),
        'observation_date':latest['date'],'close':float(latest['close']) if latest['close'] is not None else None,
        'adjusted_close':float(latest['adjusted_close']) if latest['adjusted_close'] is not None else None,
        'quality':quality(latest['date'],inputs['evaluation_date'],latest['close'] is not None and latest['adjusted_close'] is not None),
        'history':rows,'originals':refs,'current_vintage_date':inputs['evaluation_date'],'first_publication_at':None,
        'coverage':{'n_union_dates':len(rows),'n_price_dates':len(data['full']),'n_adjusted_dates':len(data['dividend-adjusted']),
            'missing_in_one_ledger':len(set(data['full'])^set(data['dividend-adjusted'])),'requested_start':inputs['history_start'],
            'first_observed':rows[0]['date'],'calendar_completeness_verified':False},
        'returns':{str(n):{'price':performance(rows,'close',n),'dividend_adjusted':performance(rows,'adjusted_close',n)} for n in (21,63)},
        'volatility':metric(vol,'percent_annualized',w20,'Sample standard deviation of 20 consecutive observed log returns × sqrt(252) × 100; 252 is an annualization convention.',None if vol is not None else 'missing_or_gapped_20_intervals'),
        'drawdown':metric(dd,'percent',w252,'100 × (latest / maximum in 252 observations - 1)',None if dd_ok else 'insufficient_or_missing_observations'),
        'trend':metric(trend,'percent',w200,'100 × (latest / mean of 200 observations - 1)',None if ma_ok else 'insufficient_or_missing_observations'),
        'interpretation':'USD ETF proxy. Provider split-adjusted close excludes dividends; dividend-adjusted close includes provider split/dividend adjustments. Neither is an independently reconstructed executable total-return series.',**PERMISSIONS}

def fred(inputs,bodies,sid):
    base=dict(series_id=sid,file_type='json',realtime_start=inputs['evaluation_date'],realtime_end=inputs['evaluation_date'])
    definition,dr=original(inputs,bodies,'definition:'+sid,'/fred/series',base)
    start='1990-01-01' if sid in LONG_HISTORY else inputs['history_start'];limit=20000 if sid in LONG_HISTORY else 10000
    query={**base,'observation_start':start,'observation_end':inputs['evaluation_date'],'units':'lin','sort_order':'asc','limit':str(limit),'offset':'0','output_type':'1'}
    doc,ref=original(inputs,bodies,'observations:'+sid,'/fred/series/observations',query)
    defs=definition.get('seriess',[])
    unit='Index' if sid=='VIXCLS' else 'Percent'
    if len(defs)!=1 or any(defs[0].get(k)!=v for k,v in [('id',sid),('units',unit),('frequency_short','D'),('seasonal_adjustment','Not Seasonally Adjusted')]):raise ValueError('official source definition differs')
    meta=defs[0]
    for k,v in [('units','lin'),('sort_order','asc'),('output_type',1),('offset',0),('limit',limit),('realtime_start',inputs['evaluation_date']),('realtime_end',inputs['evaluation_date'])]:
        if doc.get(k)!=v:raise ValueError('observation response contract differs')
    source_rows=doc.get('observations')
    if not isinstance(source_rows,list) or not source_rows or type(doc.get('count')) is not int or len(source_rows)!=doc['count'] or len(source_rows)>limit:raise ValueError('incomplete native history')
    rows=[]
    for i,r in enumerate(source_rows):
        day=str(date.fromisoformat(r['date']));value=number(r.get('value'))
        if not start<=day<=inputs['evaluation_date'] or rows and day<=rows[-1]['date']:raise ValueError('invalid native observation date')
        if r.get('realtime_start')!=inputs['evaluation_date'] or r.get('realtime_end')!=inputs['evaluation_date']:raise ValueError('mixed vintages')
        if r.get('value') not in (None,'','.') and value is None or value is not None and value<0 and sid not in ('T10Y2Y','DGS10'):raise ValueError('invalid source level')
        if value is not None and not meta['observation_start']<=day<=meta['observation_end']:raise ValueError('finite value outside official span')
        rows.append({'date':day,'value':float(value) if value is not None else None,'decimal':str(value) if value is not None else None,'row_index':i})
    latest=rows[-1];target=str(date.fromisoformat(latest['date'])-timedelta(weeks=13));prior=next((r for r in rows if r['date']==target),None)
    scale=100 if unit=='Percent' else 1
    delta=(Decimal(latest['decimal'])-Decimal(prior['decimal']))*scale if prior and prior['decimal'] is not None and latest['decimal'] is not None else None
    return {'series_id':sid,'title':meta['title'],'value':latest['value'],'value_decimal':latest['decimal'],'unit':'percent' if unit=='Percent' else 'index_points','observation_date':latest['date'],
        'frequency':meta['frequency'],'seasonal_adjustment':meta['seasonal_adjustment'],'source_row_index':latest['row_index'],
        'provider_updated_at':meta.get('last_updated'),'acquired_at':ref['acquired_at'],'first_publication_at':None,'current_vintage_date':inputs['evaluation_date'],
        'quality':quality(latest['date'],inputs['evaluation_date'],latest['value'] is not None),'history':rows,'originals':{'definition':dr,'observations':ref},
        'change':{'value':exported(delta),'unit':'basis_points' if unit=='Percent' else 'index_points','start_date':target,'end_date':latest['date'],
            'baseline_row_index':prior['row_index'] if prior else None,'current_row_index':latest['row_index'],'formula':'(latest - exact 13-calendar-week baseline) × '+str(scale),
            'reason':None if delta is not None else 'missing_exact_dated_comparison'},**PERMISSIONS}

def correlations(instruments):
    end_dates=sorted({r['date'] for x in instruments.values() for r in x.get('history',[])})[-60:]
    values={s:{k:v for k,v in interval_returns(r.get('history',[]),'adjusted_close').items() if k[1] in end_dates} for s,r in instruments.items()}
    pairs=[];symbols=list(INSTRUMENTS)
    for i,left in enumerate(symbols):
        for right in symbols[i+1:]:
            a,b=values.get(left,{}),values.get(right,{})
            dates=sorted(set(a)&set(b));union=set(a)|set(b);value=None
            if len(dates)>=40:
                xs=[a[d] for d in dates];ys=[b[d] for d in dates]
                mx=sum(xs)/len(xs);my=sum(ys)/len(ys)
                vx=sum((x-mx)**2 for x in xs);vy=sum((y-my)**2 for y in ys)
                if vx>0 and vy>0:value=exported(sum((x-mx)*(y-my) for x,y in zip(xs,ys))/(vx*vy).sqrt())
            pairs.append({'left':left,'right':right,'value':value,'unit':'correlation_-1_1','n_matched_intervals':len(dates),
                'n_available_union_intervals':len(union),'matched_intervals':[list(d) for d in dates],
                'reason':None if value is not None else 'fewer_than_40_matching_intervals_or_zero_variance',**PERMISSIONS})
    return {'window_end_dates':end_dates,'pairs':pairs,'method':'Pearson correlation of dividend-adjusted log returns; both start and end dates must match. Last 60 union observation end dates, minimum 40 matched intervals, no forward fill.',
        'interpretation':'Co-movement is descriptive. It does not identify transmission, causality or a contagion probability.'}

def credit_dispersion(measurements):
    a=measurements.get('BAMLH0A3HYC',{});b=measurements.get('BAMLH0A1HYBB',{});day=a.get('observation_date')
    same=next((r for r in b.get('history',[]) if r['date']==day),None)
    va=number(a.get('value_decimal'));vb=number(same.get('decimal')) if same else None
    return {'value':exported((va-vb)*100) if va is not None and vb is not None else None,'unit':'basis_points','observation_date':day,
        'left':'BAMLH0A3HYC','right':'BAMLH0A1HYBB','left_row_index':a.get('source_row_index'),'right_row_index':same.get('row_index') if same else None,
        'formula':'(CCC-and-lower OAS − BB OAS) × 100 at the latest CCC observation date',**PERMISSIONS}

def _build(inputs,bodies):
    if inputs.get('contract')!='global-stress-inputs.v1' or str(clock(inputs['generated_at']).date())!=inputs['evaluation_date']:raise ValueError('input contract or date differs')
    evaluation=date.fromisoformat(inputs['evaluation_date'])
    if inputs['history_start']!=str(evaluation-timedelta(days=800)) or inputs['price_end']!=str(evaluation-timedelta(days=1)):raise ValueError('collection bounds differ')
    instruments={};measurements={};failures={}
    for symbol in INSTRUMENTS:
        try:instruments[symbol]=instrument(inputs,bodies,symbol)
        except (KeyError,ValueError,TypeError,IndexError,AttributeError,OverflowError) as exc:
            failures[symbol]=str(exc) if isinstance(exc,ValueError) else type(exc).__name__
            instruments[symbol]={'symbol':symbol,'label':INSTRUMENTS[symbol][2],'group':INSTRUMENTS[symbol][3],'history':[],'quality':{'status':'unavailable'},**PERMISSIONS}
    for sid in SERIES:
        try:measurements[sid]=fred(inputs,bodies,sid)
        except (KeyError,ValueError,TypeError,IndexError,AttributeError) as exc:
            failures[sid]=str(exc) if isinstance(exc,ValueError) else type(exc).__name__
            measurements[sid]={'series_id':sid,'value':None,'unit':'index_points' if sid=='VIXCLS' else 'percent','history':[],'quality':{'status':'unavailable'},**PERMISSIONS}
    return {'contract':CONTRACT,'version':'2.0.1','generated_at':inputs['generated_at'],'ok':not failures,
        'numerical_policy':{'arithmetic':'Decimal, precision 50, ROUND_HALF_EVEN, decimal logarithm and square root',
            'derived_output_decimal_places':12,'native_source_values':'Original decimals retained; no derived rounding is applied to source records',
            'replay':'Exact equality and output hash; no tolerance or ignored fields'},
        'instruments':instruments,'measurements':measurements,'correlations':correlations(instruments),'credit_dispersion':credit_dispersion(measurements),
        'source_failures':failures,'context':inputs.get('context',{}),
        'quality':{'status':'research_only','fresh_instruments':sum(r['quality']['status']=='fresh' for r in instruments.values()),'expected_instruments':14,
            'fresh_native_series':sum(r['quality']['status']=='fresh' for r in measurements.values()),'expected_native_series':len(SERIES)},
        'decision':{'verb':'WAIT','meaning':'abstain','reason':'No independently qualified Global Stress forecast or sizing model.'},
        'decision_qualification':{'status':'research_only','model_id':None,'scorecard_manifest_key':None},
        'global_stress_index':None,'global_stress_level':None,'equity_stress':None,'bond_stress':None,'worst_market':None,
        'equities':[],'bonds':[],'flashing_red':[],'hot_signals':[],'gsi_by_horizon':{},'dimension_scores':{},
        'schema_version':'2.0','engine':'justhodl-global-stress','call':None,'validation_status':'RESEARCH_ONLY',
        'global_stress_index_ciss_adj':None,'market_stress_index':None,'implied_vol':None,'credit_spreads':None,
        'rates':None,'sovereign':None,'funding':None,'contagion':None,'breadth':None,'safe_haven':None,'stress_momentum':None,
        'ciss_systemic':{'value':None,'floor':None,'applied':False,'source':'data/ciss-stress.json','use':'linked_context_only'},
        'weights':{'values':{},'mode':'unqualified','sample_size':0,'calibrated_at':None,'priors':{}},'thresholds':{},'escalation':None,
        'headline':'Original-source cross-market measurements; no qualified Global Stress score or portfolio instruction.',
        'how_to_read':'Use instruments, measurements and exact-date correlations. Legacy composite fields are explicitly unqualified.',
        'methodology':{'adjustments':'Provider split-adjusted and dividend-adjusted prices are retained separately. Current adjustment vintage, not point-in-time or executable total-return history.',
            'dates':'Observed intervals carry exact endpoints. Missing values stay missing; no positional cross-market alignment or forward fill.',
            'sovereign':'EMB and IEF differ in duration, credit, holdings and distributions. Their ratio does not isolate sovereign credit risk.',
            'gold':'A rise in GLD price is not a measurement of safe-haven demand or fund inflows.',
            'rates':'DGS10 is a nominal Treasury yield, not MOVE or implied rate volatility.',
            'authority':'No arbitrary blend, calibration weights, crisis probability, automated alert or portfolio multiplier.',
            'dependencies':'Country ETFs overlap broad-market ETFs; credit categories overlap. Correlations are not independent votes.'},**PERMISSIONS}


def build(inputs,bodies):
    # Isolate all arithmetic from ambient context and machine floating-point libraries.
    with localcontext() as ctx:
        ctx.prec=50;ctx.rounding=ROUND_HALF_EVEN
        return _build(inputs,bodies)
