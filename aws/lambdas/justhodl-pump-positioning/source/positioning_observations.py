"""Pure descriptive price-window calculations; no probabilities or allocations.

Source occurrences and every measurement operand are retained. Calendar dates
are not a verified exchange-session calendar; price changes are not total returns.
"""
from collections import Counter
from datetime import date,datetime,timezone,timedelta
from decimal import Decimal,localcontext
import hashlib,json,re,zlib
from urllib.parse import quote
from context_evidence_store import clock,validate_ref

CONTRACT='positioning-price-observations.v1'
PRIVATE='audit-private/20260909-originals/momentum-leaders-research/positioning-context/'
HEAD='data/pump-positioning.json'
INPUTS={'radar':'data/convergence-radar.json','earnings':'data/earnings-tracker.json',
        'macro':'data/ai-website-synthesis.json','momentum':'data/momentum-leaders.json',
        'catalysts':'data/catalysts.json','clusters':'data/catalyst-clusters.json'}
FLAGS=dict.fromkeys(('calls_eligible','ranking_eligible','sizing_eligible','execution_eligible','forecast_qualified'),False)
MAX_SOURCE_BYTES=8*1024*1024


def sha(raw):return hashlib.sha256(raw).hexdigest()


def strict(raw,encoding=''):
    if not isinstance(raw,bytes) or len(raw)>MAX_SOURCE_BYTES:raise ValueError('Whole bounded original required')
    packed=raw.startswith(b'\x1f\x8b')
    if encoding not in ('','identity','gzip') or encoding=='gzip' and not packed or encoding=='identity' and packed:raise ValueError('Source encoding mismatch')
    if packed:
        decoder=zlib.decompressobj(16+zlib.MAX_WBITS);decoded=decoder.decompress(raw,MAX_SOURCE_BYTES+1)
        if len(decoded)>MAX_SOURCE_BYTES or decoder.unconsumed_tail or decoder.unused_data or not decoder.eof:raise ValueError('Whole bounded gzip member required')
        raw=decoded
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON key')
            out[key]=value
        return out
    def decimal(value):
        n=Decimal(value)
        if not n.is_finite() or abs(n)>Decimal('1e30') or (n and abs(n)<Decimal('1e-30')):raise ValueError('Source precision bound exceeded')
        return n
    def bad(_):raise ValueError('Nonfinite JSON number')
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=decimal,parse_constant=bad)


def symbol(value):
    return value if isinstance(value,str) and re.fullmatch(r'[A-Z0-9][A-Z0-9.\-^]{0,24}',value) else None


def day(value):
    try:return date.fromisoformat(value) if isinstance(value,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',value) else None
    except ValueError:return None


def number(value,zero=False):
    if isinstance(value,bool) or not isinstance(value,(int,float,Decimal)):return None
    try:n=Decimal(str(value))
    except (ValueError,ArithmeticError):return None
    return n if n.is_finite() and (n>=0 if zero else n>0) and n<=Decimal('1e20') and (not n or n>=Decimal('1e-20')) else None


def selection(doc):
    if not isinstance(doc,dict) or doc.get('status')=='error' or not isinstance(doc.get('pump_candidates'),list):
        raise ValueError('Original recognized candidate population required')
    rows=doc['pump_candidates'];occurrences=[];selected=[]
    for index,row in enumerate(rows):
        ticker=symbol(row.get('ticker')) if isinstance(row,dict) else None
        status='invalid_literal_symbol' if ticker is None else 'outside_original_twelve_occurrences' if index>=12 else 'selected_unique'
        if status=='selected_unique':
            if ticker in selected:status='selected_duplicate_occurrence'
            else:selected.append(ticker)
        occurrences.append({'source_pointer':'/pump_candidates/'+str(index),'source_index':index,
            'ticker':ticker,'status':status,'inside_original_window':index<12})
    if rows and not selected:raise ValueError('Nonempty source selection has no valid original-window issuer')
    return {'status':'reported_empty_selection' if not rows else 'source_selection_unqualified',
        'limit_occurrences':12,'selected_tickers':selected,'occurrences':occurrences,
        'selection_is_rank_or_recommendation':False}


def price_history(doc,ticker,as_of):
    if symbol(ticker) is None or type(as_of) is not date:raise ValueError('Literal issuer and date required')
    records=doc if isinstance(doc,list) else doc.get('historical') if isinstance(doc,dict) else None
    root='' if isinstance(doc,list) else '/historical'
    if not isinstance(records,list):return {'status':'unrecognized_shape','rows':[],'issues':[],'reported_count':None}
    if isinstance(doc,dict) and doc.get('symbol') not in (None,ticker):return {'status':'wrong_issuer','rows':[],'issues':[],'reported_count':len(records)}
    rows=[];issues=[];dates=[]
    for index,r in enumerate(records):
        pointer=root+'/'+str(index)
        if not isinstance(r,dict):issues.append({'source_pointer':pointer,'reason':'non_object_row'});continue
        d=day(r.get('date'))
        if d is None:issues.append({'source_pointer':pointer,'reason':'invalid_observation_date'});continue
        if r.get('symbol') not in (None,ticker):issues.append({'source_pointer':pointer,'reason':'wrong_issuer'});continue
        dates.append(d)
        if d>=as_of:
            issues.append({'source_pointer':pointer,'reason':'current_or_future_utc_date_excluded'});continue
        if d<as_of-timedelta(days=90):
            issues.append({'source_pointer':pointer,'reason':'outside_declared_lookback_excluded'});continue
        values={key:number(r.get(key),zero=key=='volume') for key in ('open','high','low','close','volume')}
        reason=None
        if any(v is None for v in values.values()):reason='invalid_or_missing_ohlcv'
        elif not values['low']<=values['open']<=values['high'] or not values['low']<=values['close']<=values['high']:reason='inconsistent_ohlcv'
        # Invalid members remain in their dated window; never drop and backfill.
        rows.append({'date':d.isoformat(),'source_index':index,'source_pointer':pointer,'values':values,'issue':reason})
        if reason:issues.append({'source_pointer':pointer,'reason':reason})
    duplicates=[d.isoformat() for d,n in Counter(dates).items() if n>1]
    if duplicates:issues.append({'source_pointer':root,'reason':'duplicate_observation_dates','dates':duplicates})
    structural={'non_object_row','invalid_observation_date','wrong_issuer','duplicate_observation_dates'}
    rows.sort(key=lambda r:r['date'])
    return {'status':'source_identity_or_date_invalid' if any(r['reason'] in structural for r in issues) else 'parsed',
            'rows':rows,'issues':issues,'reported_count':len(records),'completed_date_count':len(rows),
            'exchange_sessions_verified':False,'corporate_actions_verified':False,'currency_verified':False}


def _operand(row,key):
    return {'source_pointer':row['source_pointer']+'/'+key,'source_index':row['source_index'],'observation_date':row['date'],
            'value_exact':str(row['values'][key]) if row['values'][key] is not None else None}


def measurements(history):
    rows=history.get('rows',[]);out=[]
    def calculate(name,n,unit,definition,fn,fields):
        window=rows[-n:];record={'name':name,'unit':unit,'definition':definition,'required_observations':n,
            'observations_used':len(window),'start_date':window[0]['date'] if window else None,
            'end_date':window[-1]['date'] if window else None,'operands':[_operand(r,k) for r in window for k in fields],
            'value':None,'value_exact':None,'status':'unavailable','reason':None,**FLAGS}
        if history.get('status')!='parsed':record['reason']='source_identity_or_date_unavailable'
        elif len(window)!=n:record['reason']='insufficient_completed_observations'
        elif any(r['issue'] for r in window):record['reason']='invalid_member_in_selected_window'
        else:
            with localcontext() as ctx:
                ctx.prec=34;value=fn(window)
            record.update(value=float(value),value_exact=str(value),status='measured',reason=None)
        out.append(record)
    for span in (5,20,60):
        calculate('price_change_'+str(span),span+1,'percent_price_change',
            '(last close / first close - 1) * 100 over consecutive reported completed-date observations; not total return',
            lambda w:(w[-1]['values']['close']/w[0]['values']['close']-1)*100,('close',))
    def atr(w):
        return sum(max(r['values']['high']-r['values']['low'],abs(r['values']['high']-p['values']['close']),abs(r['values']['low']-p['values']['close'])) for p,r in zip(w,w[1:]))/14
    calculate('mean_true_range_14',15,'reported_price_units',
        'Arithmetic mean of fourteen max(high-low, abs(high-prior close), abs(low-prior close)) pairs; not Wilder smoothing',atr,('high','low','close'))
    def volatility(w):
        returns=[(r['values']['close']/p['values']['close']).ln() for p,r in zip(w,w[1:])]
        mean=sum(returns)/30
        return (sum((r-mean)**2 for r in returns)/29).sqrt()*100
    calculate('sample_log_return_std_30',31,'percent_per_reported_observation',
        'Sample standard deviation of thirty consecutive close log returns; denominator 29; no annualization or forecast',volatility,('close',))
    calculate('mean_volume_20',20,'reported_volume_units',
        'Arithmetic mean of latest twenty completed-date volumes, including genuine zero; volume units unverified',
        lambda w:sum(r['values']['volume'] for r in w)/20,('volume',))
    calculate('last_completed_close',1,'reported_price_units',
        'Last complete reported OHLCV bar with a date before the acquisition UTC date; not a current executable quote',
        lambda w:w[-1]['values']['close'],('close',))
    return out


def endpoint(ticker,kind,as_of):
    if symbol(ticker) is None or kind not in ('history','quote','profile') or type(as_of) is not date:raise ValueError('Fixed original request scope required')
    path='historical-price-eod/full' if kind=='history' else kind
    url='https://financialmodelingprep.com/stable/'+path+'?symbol='+quote(ticker,safe='')
    if kind=='history':url+='&from='+(as_of-timedelta(days=90)).isoformat()+'&to='+as_of.isoformat()
    return url


def content(attempt,sources):
    ref=attempt.get('original_ref')
    if ref is None:return None
    validate_ref(ref,PRIVATE,'sources');raw=sources.get(ref['key'])
    if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256'] or len(raw)>MAX_SOURCE_BYTES:
        raise ValueError('Complete declared original differs')
    return raw


def _clocks(a,generated):
    requested=clock(a.get('requested_at'));received=clock(a.get('received_at'))
    if requested is None or received is None or not requested<=received<=generated:raise ValueError('Ordered acquisition clocks required')


def _object(raw,encoding):
    try:return strict(raw,encoding) if raw is not None else None
    except (ValueError,UnicodeError,zlib.error):return None


def _issuer_record(doc,ticker):
    if not isinstance(doc,list):return None,'unrecognized_shape',None
    matches=[(i,row) for i,row in enumerate(doc) if isinstance(row,dict) and row.get('symbol')==ticker]
    if len(matches)!=1:return None,'missing_or_ambiguous_issuer',None
    return matches[0][1],'issuer_matched',matches[0][0]


def build(input_attempts,attempts,sources,generated_at):
    generated=clock(generated_at)
    if generated is None or not isinstance(input_attempts,dict) or set(input_attempts)!=set(INPUTS):raise ValueError('Exact original context paths required')
    docs={};inputs=[]
    for name,key in INPUTS.items():
        a=input_attempts[name]
        if a.get('source_key')!=key or a.get('status') not in ('received','source_read_unavailable'):raise ValueError('Unexpected context acquisition')
        _clocks(a,generated);raw=content(a,sources)
        if (raw is None)!=(a['status']=='source_read_unavailable'):raise ValueError('Context outcome and original differ')
        doc=_object(raw,a.get('content_encoding',''));docs[name]=doc
        inputs.append({'source':name,'source_key':key,'status':a['status'],'original_ref':a.get('original_ref'),
            'requested_at':a['requested_at'],'received_at':a['received_at'],'interpretation_qualified':False})
    selected=selection(docs['radar']);expected=[(ticker,kind) for ticker in selected['selected_tickers'] for kind in ('history','quote','profile')]
    if not isinstance(attempts,list) or [(a.get('ticker'),a.get('kind')) for a in attempts]!=expected:raise ValueError('Every original bounded request outcome required in declared order')
    statuses=('received','http_error','transport_unavailable','source_limit_exceeded','not_attempted_budget','not_attempted_stop')
    captures=[];by={}
    for index,a in enumerate(attempts):
        ticker,kind=expected[index];_clocks(a,generated)
        if a.get('endpoint')!=endpoint(ticker,kind,generated.date()) or a.get('status') not in statuses:raise ValueError('Unexpected provider request scope')
        raw=content(a,sources)
        status=a.get('http_status');network=a.get('network_attempted')
        if type(network) is not bool or (status is not None and (type(status) is not int or not 100<=status<=599)):
            raise ValueError('Literal transport outcome required')
        if a['status']=='received' and (raw is None or status!=200 or not network):raise ValueError('Complete successful provider response required')
        if a['status']=='http_error' and (raw is None or status is None or status==200 or not network):raise ValueError('Complete failed HTTP response required')
        if a['status'].startswith('not_attempted_') and (network or status is not None):raise ValueError('Unattempted request cannot claim HTTP response')
        if a['status'] in ('transport_unavailable','source_limit_exceeded') and not network:raise ValueError('Transport outcome requires attempted request')
        if a['status'] not in ('received','http_error') and raw is not None:raise ValueError('Unavailable request cannot claim a complete body')
        doc=_object(raw,a.get('content_encoding','')) if a['status']=='received' else None
        by[(ticker,kind)]=(doc,a)
        captures.append({k:a.get(k) for k in ('ticker','kind','endpoint','status','http_status','network_attempted','requested_at','received_at','original_ref')})
    observations=[];measured=0
    for ticker in selected['selected_tickers']:
        hdoc,ha=by[(ticker,'history')];history=price_history(hdoc,ticker,generated.date());metrics=measurements(history)
        measured+=sum(row['status']=='measured' for row in metrics)
        qdoc,qa=by[(ticker,'quote')];quote_row,qstatus,qi=_issuer_record(qdoc,ticker)
        qprice=number(quote_row.get('price')) if quote_row is not None else None
        qtime=None
        if quote_row is not None and type(quote_row.get('timestamp')) is int:
            try:qtime=datetime.fromtimestamp(quote_row['timestamp'],timezone.utc)
            except (ValueError,OverflowError,OSError):pass
        if qtime is not None and qtime>generated:qprice=None;qstatus='future_quote_timestamp'
        pdoc,pa=by[(ticker,'profile')];profile,pstatus,pi=_issuer_record(pdoc,ticker)
        currency=profile.get('currency') if profile is not None else None
        if not isinstance(currency,str) or not re.fullmatch('[A-Z]{3}',currency):currency=None
        observations.append({'ticker':ticker,'history_original_ref':ha.get('original_ref'),
            'history_quality':{k:v for k,v in history.items() if k!='rows'},'measurements':metrics,
            'reported_quote':{'status':qstatus if qprice is not None or qstatus!='issuer_matched' else 'price_unavailable',
                'price':float(qprice) if qprice is not None else None,'price_exact':str(qprice) if qprice is not None else None,
                'source_timestamp':qtime.isoformat() if qtime is not None else None,'source_pointer':'/'+str(qi) if qi is not None else None,
                'original_ref':qa.get('original_ref'),'executable_price_verified':False},
            'reported_profile_currency':{'value':currency,'source_pointer':'/'+str(pi)+'/currency' if pi is not None else None,
                'original_ref':pa.get('original_ref'),'quote_currency_binding_verified':False},**FLAGS})
    if selected['selected_tickers'] and not measured:raise ValueError('No valid completed-date measurement; preserve prior publication')
    basket={'status':'unqualified','positions':[],'n_positions':None,'total_exposure':None,'cash_pct':None,
            'max_risk_at_stops_pct':None,'sector_breakdown':{},'suggested_additions':[],**FLAGS}
    return {'schema_version':'2.0','measurement_contract':CONTRACT,'generated_at':generated_at,'status':'research_only',
        'call':'WAIT','call_semantics':'abstain',**FLAGS,'macro_regime':None,'candidates':[],'n_candidates':None,
        'portfolio_basket':dict(basket),'aggressive_basket':dict(basket),'sizing_assumptions':None,
        'selection':selected,'source_inputs':inputs,'acquisitions':captures,'price_observations':observations,
        'coverage':{'selected_unique_tickers':len(selected['selected_tickers']),'planned_provider_requests':len(expected),
            'measured_descriptive_fields':measured,'independent_roots':None,'eligible_votes':0},
        'model_requests':0,'notifications_sent':0,'quality':{'status':'research_only','observation_freshness':'unqualified'},
        'limitations':['Selection order is inherited source scope, not an investment rank.',
            'Reported-date windows do not establish exchange sessions, corporate-action adjustment or total returns.',
            'Profile currency does not establish quote currency, FX conversion or portfolio valuation.',
            'No score is treated as a win probability, and no grade, allocation, stop, profit target or downside bound is recommended.',
            'WAIT means abstain, not a recommendation to hold. Complete originals remain private.']}
