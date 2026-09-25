"""Dated ETF observations and matched-date ratios; no forecast or trade sizing."""
from datetime import date,datetime,timezone,timedelta
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from urllib.parse import urlencode
import hashlib,json,re

CONTRACT='gold-rotation-original-research.v1'
PREFIX='data/gold-rotation-research/'
PRIVATE='audit-private/20260909-originals/gold-rotation-research/'
CURRENT='data/gold-equity-rotation.json'
FLAGS=('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')
PERMISSIONS=dict.fromkeys(FLAGS,False)
INSTRUMENTS={
 'GLD':('US78463V1070','AMEX','Gold trust shares'),
 'SPY':('US78462F1030','AMEX','US large-cap equity ETF'),
 'GDX':('US92189F1066','AMEX','Gold mining equity ETF'),
 'SLV':('US46428Q1094','AMEX','Silver trust shares'),
 'UUP':('US46141D2036','AMEX','Dollar futures fund proxy; not a dollar spot index'),
 'TLT':('US4642874329','NASDAQ','Long Treasury bond ETF'),
 'VNQ':('US9229085538','AMEX','Real-estate equity ETF'),
 'REM':('US46435G3424','CBOE','Mortgage real-estate equity ETF'),
}
KINDS=('profile','light','full','dividend-adjusted')
FIELDS={'light':('price','volume'),'full':('open','high','low','close','volume','vwap'),
        'dividend-adjusted':('adjOpen','adjHigh','adjLow','adjClose','volume')}
PRICE={'light':'price','full':'close','dividend-adjusted':'adjClose'}
OLD_METRICS=('spy_ratio_gld','ratio_ma50','ratio_ma200','ratio_zscore_252d','ratio_20d_pct','spy_20d_pct','gld_20d_pct',
 'gdx_20d_pct','slv_20d_pct','uup_20d_pct','tlt_20d_pct','vnq_20d_pct','rem_20d_pct','ratio_persistence_5d','dxy_falling_20d','gold_strength','equity_strength')

def sha(raw):return hashlib.sha256(raw).hexdigest()
def encoded(v):return json.dumps(v,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(v):return sha(encoded(v))
def strict(raw,decimals=False):
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def invalid(_):raise ValueError('Nonfinite JSON value')
    return json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid,**({'parse_float':Decimal,'parse_int':Decimal} if decimals else {}))
def clock(value):
    if not isinstance(value,str):raise ValueError('Explicit clock required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Timezone required')
    return result.astimezone(timezone.utc)
def number(value,positive=False):
    if value is None:return None
    if isinstance(value,bool) or not isinstance(value,(Decimal,str,int,float)) or len(str(value))>80:raise ValueError('Bounded finite source number required')
    value=Decimal(str(value))
    if not value.is_finite() or value<0 or positive and value==0:raise ValueError('Positive price or nonnegative volume required')
    return value
def ds(value):return None if value is None else format(value,'f')
def derived(value):return None if value is None else ds(value.quantize(Decimal('0.000000000001'),rounding=ROUND_HALF_EVEN))
def source_url(symbol,kind,start,end):
    if symbol not in INSTRUMENTS or kind not in KINDS:raise ValueError('Reviewed source identity required')
    q={'symbol':symbol}
    if kind!='profile':q.update({'from':start,'to':end})
    return 'https://financialmodelingprep.com/stable/'+('profile' if kind=='profile' else 'historical-price-eod/'+kind)+'?'+urlencode(q)
def original(ref,read):
    if (not isinstance(ref,dict) or not isinstance(ref.get('sha256'),str) or not re.fullmatch('[a-f0-9]{64}',ref['sha256'])
        or ref.get('key')!=PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int):raise ValueError('Protected complete original required')
    raw=read(ref['key'])
    if len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Original bytes differ')
    return raw
def source(inputs,symbol,kind,read):
    capture=inputs['captures'][symbol+':'+kind]
    if capture['symbol']!=symbol or capture['kind']!=kind or capture['source_url']!=source_url(symbol,kind,inputs['range']['from'],inputs['range']['to']):raise ValueError('Source request identity differs')
    if clock(capture['received_at'])>clock(inputs['generated_at']):raise ValueError('Source received after compilation')
    ref=capture['original'];raw=original(ref,read) if ref else None
    if capture['status']!='response_retained' or capture.get('http_status')!=200 or not raw:return None,capture
    return strict(raw,True),capture

def history(doc,symbol,kind,inputs):
    if not isinstance(doc,list) or not 0<len(doc)<=2000:raise ValueError('Bounded price history required')
    rows={}
    for i,row in enumerate(doc):
        if not isinstance(row,dict) or row.get('symbol')!=symbol:raise ValueError('Price instrument identity differs')
        day=row.get('date')
        if not isinstance(day,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',day):raise ValueError('Observation date required')
        date.fromisoformat(day)
        if day in rows or not inputs['range']['from']<=day<=inputs['range']['to']:raise ValueError('Duplicate or out-of-range observation')
        values={field:ds(number(row.get(field),positive=field!='volume')) for field in FIELDS[kind]}
        fields=('open','high','low','close') if kind=='full' else ('adjOpen','adjHigh','adjLow','adjClose') if kind=='dividend-adjusted' else ()
        if fields and all(values[k] is not None for k in fields):
            opening,high,low,closing=(Decimal(values[k]) for k in fields)
            if not low<=opening<=high or not low<=closing<=high:raise ValueError('OHLC range is inconsistent')
        rows[day]={'date':day,'source_row_index':i,'values':values}
    return rows
def metric(value,unit,rows,required,formula,reason=None):
    return {'value_decimal':derived(value),'unit':unit,'start_date':rows[0]['date'] if rows else None,
        'end_date':rows[-1]['date'] if rows else None,'observations_available':len(rows),'observations_required':required,
        'formula':formula,'reason':reason if value is None else None,**PERMISSIONS}
def performance(rows,field,n):
    window=rows[-n-1:];ok=len(window)==n+1 and all(r.get(field) is not None for r in window)
    value=(Decimal(window[-1][field])/Decimal(window[0][field])-1)*100 if ok else None
    return metric(value,'percent',window,n+1,'100 × (last / first - 1); '+str(n)+' intervals in the union observation calendar',None if ok else 'insufficient_or_missing_observations')
def instrument(inputs,symbol,docs,captures):
    profile=docs['profile'];isin,exchange,label=INSTRUMENTS[symbol]
    if not isinstance(profile,list) or len(profile)!=1:raise ValueError('Unique profile required')
    p=profile[0]
    if not isinstance(p,dict) or p.get('isEtf') is not True or any(p.get(k)!=v for k,v in (('symbol',symbol),('isin',isin),('exchange',exchange),('currency','USD'))):raise ValueError('Reviewed security identity differs')
    ledgers={};failures={}
    for kind in PRICE:
        try:ledgers[kind]=history(docs[kind],symbol,kind,inputs)
        except (ValueError,TypeError,KeyError):ledgers[kind]={};failures[kind]='source_unavailable_or_invalid'
    days=sorted(set().union(*(set(v) for v in ledgers.values())))
    rows=[]
    for day in days:
        row={'date':day,'source_rows':{kind:ledgers[kind].get(day) for kind in PRICE}}
        for kind,field in PRICE.items():row[kind]=((ledgers[kind].get(day) or {}).get('values') or {}).get(field)
        rows.append(row)
    matched=[d for d in days if d in ledgers['light'] and d in ledgers['full']]
    differences=[d for d in matched if ledgers['light'][d]['values']['price']!=ledgers['full'][d]['values']['close']]
    latest=days[-1] if days else None
    return {'symbol':symbol,'isin':isin,'exchange':exchange,'currency':'USD','label':label,'provider_title':p.get('companyName'),
        'history':rows,'latest':rows[-1] if rows else None,'source_failures':failures,'originals':captures,
        'observation_date':latest,'observation_age_calendar_days':(clock(inputs['generated_at']).date()-date.fromisoformat(latest)).days if latest else None,
        'current_vintage_acquired_at':{k:v['received_at'] for k,v in captures.items()},'requested_observation_end':inputs['range']['to'],'first_publication_at':None,
        'coverage':{'union_dates':len(days),'dates_by_endpoint':{k:len(v) for k,v in ledgers.items()},'market_calendar_completeness_verified':False},
        'legacy_light_reconciliation':{'matched_dates':len(matched),'different_close_dates':differences,'equality_establishes_adjustment_method':False},
        'returns':{kind:{str(n):performance(rows,kind,n) for n in (5,20,63,252)} for kind in PRICE},
        'interpretation':'USD fund share observations. Dividend-adjusted prices are provider-adjusted, not independently reconstructed executable returns. Trading volume is not AUM or investor cash flow.',**PERMISSIONS}

def ratios(instruments,kind):
    left={r['date']:r for r in instruments['SPY']['history']};right={r['date']:r for r in instruments['GLD']['history']}
    rows=[]
    for day in sorted(set(left)|set(right)):
        a=left.get(day,{}).get(kind);b=right.get(day,{}).get(kind)
        rows.append({'date':day,'numerator_decimal':a,'denominator_decimal':b,
            'value_decimal':ds(Decimal(a)/Decimal(b)) if a is not None and b is not None else None,
            'spy_row_index':((left.get(day,{}).get('source_rows',{}).get(kind) or {}).get('source_row_index')),
            'gld_row_index':((right.get(day,{}).get('source_rows',{}).get(kind) or {}).get('source_row_index'))})
    mas={}
    for n in (50,200):
        window=rows[-n:];ok=len(window)==n and all(r['value_decimal'] is not None for r in window)
        value=sum(Decimal(r['value_decimal']) for r in window)/n if ok else None
        mas[str(n)]=metric(value,'share_price_ratio',window,n,'Arithmetic mean of '+str(n)+' dated SPY/GLD ratios',None if ok else 'insufficient_or_unmatched_dates')
    window=rows[-252:];complete=len(window)==252 and all(r['value_decimal'] is not None for r in window);z=None;reason='insufficient_or_unmatched_dates'
    if complete:
        refs=[Decimal(r['value_decimal']) for r in window[:-1]];mean=sum(refs)/len(refs);variance=sum((v-mean)**2 for v in refs)/(len(refs)-1)
        reason='zero_reference_variance'
        if variance:z=(Decimal(window[-1]['value_decimal'])-mean)/variance.sqrt();reason=None
    recent=rows[-6:];sign=None
    if len(recent)==6 and all(r['value_decimal'] is not None for r in recent):
        values=[Decimal(r['value_decimal']) for r in recent];signs=[(b>a)-(b<a) for a,b in zip(values,values[1:])]
        sign=signs[0] if len(set(signs))==1 else 0
    return {'numerator':'SPY','denominator':'GLD','basis':kind,'unit':'share_price_ratio','history':rows,'latest':rows[-1] if rows else None,
        'returns':{str(n):performance(rows,'value_decimal',n) for n in (5,20,63,252)},'moving_averages':mas,
        'zscore':metric(z,'standard_deviations',window,252,'Latest ratio minus mean of preceding 251 observations, divided by their sample standard deviation (ddof=1)',reason),
        'persistence':{'direction':sign,'intervals_required':5,'observation_dates':[r['date'] for r in recent],
            'meaning':'1 rising every interval; -1 falling every interval; 0 no strict directional persistence; null incomplete'},
        'coverage':{'union_dates':len(rows),'matched_dates':sum(r['value_decimal'] is not None for r in rows),'calendar_completeness_verified':False},
        'interpretation':'Relative fund-share prices, not relative fund valuations, cash transfers or an optimal hedge ratio.',**PERMISSIONS}

def compile_output(inputs,read):
    with localcontext() as ctx:
        ctx.prec=60;ctx.rounding=ROUND_HALF_EVEN
        return _compile(inputs,read)
def _compile(inputs,read):
    if inputs.get('contract')!='gold-rotation-inputs.v1':raise ValueError('Native input contract required')
    generated=clock(inputs['generated_at']);start=date.fromisoformat(inputs['range']['from']);end=date.fromisoformat(inputs['range']['to'])
    if not start<end<generated.date() or (end-start).days!=899:raise ValueError('Exact 900-calendar-day collection range required')
    if set(inputs['captures'])!={s+':'+k for s in INSTRUMENTS for k in KINDS}:raise ValueError('Complete declared source inventory required')
    predecessor=original(inputs['predecessor'],read);old=strict(predecessor)
    if old.get('engine') not in ('gold-equity-rotation','justhodl-gold-equity-rotation'):raise ValueError('Whole predecessor identity required')
    instruments={};failures={}
    for symbol in INSTRUMENTS:
        docs={};captures={}
        for kind in KINDS:docs[kind],captures[kind]=source(inputs,symbol,kind,read)
        try:instruments[symbol]=instrument(inputs,symbol,docs,captures)
        except (ValueError,TypeError,KeyError):
            failures[symbol]='instrument_identity_unavailable_or_invalid'
            instruments[symbol]={'symbol':symbol,'history':[],'latest':None,'originals':captures,'observation_date':None,'source_failures':{'profile':failures[symbol]},**PERMISSIONS}
    return {'contract':CONTRACT,'engine':'gold-equity-rotation','version':'2.0.0','generated_at':inputs['generated_at'],'as_of':inputs['generated_at'],
        'instruments':instruments,'ratios':{kind:ratios(instruments,kind) for kind in ('full','dividend-adjusted')},
        'source_range':inputs['range'],'source_failures':failures,
        'quality':{'status':'dated_descriptive_research','expected_instruments':8,
            'instruments_with_history':sum(bool(v['history']) for v in instruments.values()),'source_endpoints_retained':sum(v['original'] is not None for v in inputs['captures'].values()),
            'source_endpoints_expected':32,'provider_original_replay_required':True,'market_calendar_completeness_verified':False},
        'retained_predecessor':inputs['predecessor'],
        'numerical_policy':{'arithmetic':'Decimal precision 60, ROUND_HALF_EVEN','derived_decimal_places':12,
            'source_values':'Exact source decimals retained with original row indices; no forward fill or positional join'},
        'state':None,'signal_strength':None,'current_metrics':dict.fromkeys(OLD_METRICS),'trade_tickets':[],'n_tickets':0,
        'regime_explanation':None,'why_now':None,'call':None,'score':None,'independent_investment_votes':0,
        'decision':{'verb':'WAIT','meaning':'abstain','reason':'Dated descriptive observations have no independently qualified forecast, position size or hedge.'},
        'methodology':'Each ETF history keeps its dates, source fields and adjustment basis. SPY/GLD ratios join exact observation dates; missing inputs remain missing. Moving averages, returns, persistence and z-scores are descriptive statistics.',
        'sources':['FMP profile and historical-price-eod light/full/dividend-adjusted, original bytes retained'],
        'validation':{'forecast_qualified':False,'scorecard':None,'point_in_time_vintages_available_for_backtest':False,
            'limitations':['Provider revisions may change historical adjusted prices.','No verified market calendar, AUM series, trade-cost model or optimal portfolio weights.','No inferred dollar-index return from UUP, or commodity-spot return from fund shares.']},**PERMISSIONS}
