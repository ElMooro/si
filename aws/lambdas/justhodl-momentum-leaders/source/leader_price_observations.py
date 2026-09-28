"""Pure, replayable descriptive OHLCV measurements. No forecasting authority."""
from collections import Counter
from datetime import date, datetime, timedelta
from decimal import Decimal, localcontext
from urllib.parse import quote
import hashlib, json, math, re

CONTRACT = 'leader-price-observations.v1'
HEAD = 'data/momentum-leaders.json'
PREFIX = 'data/momentum-leaders/sources/'
FLAGS = {k: False for k in ('calls_eligible','forecast_qualified','sizing_eligible',
    'execution_eligible','independent_evidence_eligible','private_state_read_or_written')}


def sha(raw): return hashlib.sha256(raw).hexdigest()


def encode(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def strict(raw, exact=False):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result: raise ValueError('Duplicate JSON key')
            result[key] = value
        return result
    def invalid(value): raise ValueError('Nonfinite JSON number')
    def number(value):
        n = Decimal(value)
        if not n.is_finite() or abs(n) > Decimal('1e30') or (n and abs(n) < Decimal('1e-30')):
            raise ValueError('Number outside reviewed precision bounds')
        return n if exact else float(n)
    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs, parse_float=number, parse_constant=invalid)


def clock(value):
    try:
        t = datetime.fromisoformat(value.replace('Z', '+00:00'))
        return t if t.tzinfo is not None else None
    except (ValueError, TypeError, AttributeError): return None


def day(value):
    try: return date.fromisoformat(value) if isinstance(value, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', value) else None
    except ValueError: return None


def symbol(value):
    return value if isinstance(value, str) and re.fullmatch(r'[A-Z0-9][A-Z0-9.\-^]{0,24}', value) else None


def endpoint(ticker):
    if symbol(ticker) is None: raise ValueError('Declared literal symbol required')
    return 'https://financialmodelingprep.com/stable/historical-price-eod/full?symbol=' + quote(ticker, safe='')


def number(value, zero=False):
    if isinstance(value, bool) or not isinstance(value, (int, float, Decimal)): return None
    try: n = Decimal(str(value))
    except (ValueError, ArithmeticError): return None
    return n if n.is_finite() and (n >= 0 if zero else n > 0) and n <= Decimal('1e20') and (not n or n >= Decimal('1e-20')) else None


def source_ref(raw):
    if not isinstance(raw, bytes) or len(raw) > 8*1024*1024: raise ValueError('Whole bounded original required')
    return {'key': PREFIX+sha(raw)+'.json', 'bytes':len(raw), 'sha256':sha(raw), 'format':'json'}


def validate_ref(ref):
    if (not isinstance(ref,dict) or set(ref) != {'key','bytes','sha256','format'} or ref['format'] != 'json'
            or not isinstance(ref['sha256'],str) or not re.fullmatch('[a-f0-9]{64}',ref['sha256'])
            or type(ref['bytes']) is not int or not 0 <= ref['bytes'] <= 8*1024*1024
            or ref['key'] != PREFIX+ref['sha256']+'.json'): raise ValueError('Declared original identity invalid')
    return ref


def content(attempt, sources):
    if attempt.get('status') != 'received':
        if 'original_ref' in attempt: raise ValueError('Unexpected original on unavailable request')
        return None
    ref = validate_ref(attempt.get('original_ref')); raw = sources.get(ref['key'])
    if not isinstance(raw,bytes) or source_ref(raw) != ref: raise ValueError('Whole original differs')
    return raw



INPUTS=('data/convergence-radar.json','data/ticker-trends.json')
EXCLUDED='data/momentum-breakout.json'


def universe(input_attempts,sources,limit):
    """Retain all source occurrences and original selected-universe rules.

    Selection is a description of the original bounded queue, not a claim about
    a broad market or a recommendation. The existing Breakout abstention stays.
    """
    from momentum_research_boundary import current
    if limit!=60 or set(input_attempts)!=set(INPUTS):raise ValueError('Original two eligible input paths and population cap required')
    occurrences=[];members={};outcomes=[]
    def append(path,field,index,row,category,in_scope,priority):
        r=row if isinstance(row,dict) else {};ticker=r.get('ticker') if path==INPUTS[0] else (r.get('symbol') or r.get('ticker'))
        valid=isinstance(ticker,str) and bool(ticker)
        reason='in_original_scope' if valid and in_scope else 'outside_original_list_limit' if valid else 'invalid_literal_symbol'
        occurrence={'index':len(occurrences),'input_key':path,'field':field,'source_index':index,
                    'ticker':ticker if isinstance(ticker,str) else None,'category':category,'raw':row,
                    'in_original_scope':bool(valid and in_scope),'selected':False,'status':reason}
        occurrences.append(occurrence)
        if not valid or not in_scope:return
        if ticker not in members:members[ticker]={'ticker':ticker,'occurrence_indices':[],'categories':[],'priority':0}
        entry=members[ticker];entry['occurrence_indices'].append(occurrence['index'])
        if category not in entry['categories']:entry['categories'].append(category);entry['priority']+=priority
    for path in INPUTS:
        a=input_attempts[path]
        if a.get('endpoint')!=path:raise ValueError('Declared universe source identity differs')
        raw=content(a,sources);doc=None;status=a.get('status')
        if raw is not None:
            try:doc=strict(raw)
            except (ValueError,UnicodeError,RecursionError):status='malformed_original'
            else:status='parsed_public_selection_source' if isinstance(doc,dict) else 'unexpected_original_shape'
        if status=='parsed_public_selection_source' and path==INPUTS[0] and not current(doc):status='excluded_pre_breakout_boundary'
        outcomes.append({'key':path,'status':status,'generated_at':doc.get('generated_at') if isinstance(doc,dict) else None,
                         'freshness_qualified':False,'acquisition':a})
        if status!='parsed_public_selection_source':continue
        if path==INPUTS[0]:
            for field,limit_rows,category,priority in [('pump_candidates',None,'convergence-pump',100),('tickers',30,'convergence-multi',10)]:
                values=doc.get(field)
                if not isinstance(values,list):outcomes[-1].setdefault('shape_issues',[]).append(field);continue
                for i,row in enumerate(values):
                    in_scope=limit_rows is None or i<limit_rows
                    # Original top-list membership excludes tickers already present
                    # in pump candidates or an earlier top-list occurrence.
                    ticker=row.get('ticker') if isinstance(row,dict) else None
                    if field=='tickers' and isinstance(ticker,str) and ticker in members:in_scope=False
                    append(path,field,i,row,category,in_scope,priority)
        else:
            if doc.get('schema_version')=='2.0' and doc.get('method')=='wikipedia_primary_gtrends_fallback_v2':
                # Current declared producer uses all_results, not either legacy
                # alias. Keep the original top-25 selection bound, with the exact
                # source field recorded on every retained occurrence.
                field='all_results'
                outcomes[-1]['source_contract']='ticker-trends.v2.all_results'
            else:
                field='trends' if doc.get('trends') or (doc.get('trends')==[] and 'tickers' not in doc) else 'tickers'
            values=doc.get(field)
            if not isinstance(values,list):outcomes[-1].setdefault('shape_issues',[]).append(field);continue
            for i,row in enumerate(values):append(path,field,i,row,'ticker-trends',i<25,25)
    queue=list(members.values())
    if len(queue)>limit:queue=sorted(queue,key=lambda row:-row['priority'])
    selected=[]
    for entry in queue[:limit]:
        member={**entry,'request_index':len(selected),'universe_index':entry['occurrence_indices'][0]};selected.append(member)
        for i in entry['occurrence_indices']:occurrences[i].update(selected=True,status='selected_original_scope')
    for row in occurrences:
        if row['in_original_scope'] and not row['selected']:row['status']='original_population_limit'
    return {'occurrences':occurrences,'selected':selected,'configured_limit':limit,'input_outcomes':outcomes,
            'excluded_source':{'key':EXCLUDED,'status':'existing_research_abstention_preserved'},
            'historical_membership_verified':False,'market_coverage_verified':False,
            'method':'Original bounded composite-selected queue. All source occurrences retained, literal tickers deduplicated for acquisition. Rank-dependent membership is not independent evidence.'}


def measurements(rows):
    n=len(rows)
    if n<25:return {'status':'insufficient_observations','required':25,'observations':n}
    with localcontext() as ctx:
        ctx.prec=40
        result={'status':'descriptive_observations','observations':n,'first_date':rows[0]['date'],'last_date':rows[-1]['date']}
        for length in (5,20,60):
            item={'value':None,'unit':'percent','intervals':length,'required_observations':length+1,'status':'insufficient_observations'}
            if n>length:
                a,b=rows[-length-1],rows[-1];value=(b['close']/a['close']-1)*100
                item.update(value=float(value),exact=str(value),status='descriptive_local_price_change',start_date=a['date'],end_date=b['date'],
                    start_source_index=a['index'],end_source_index=b['index'],start_close=str(a['close']),end_close=str(b['close']),total_return_verified=False)
            result['price_change_'+str(length)]=item
        prior=rows[-21:-1];mean=sum(r['volume'] for r in prior)/20;latest=rows[-1]
        result['relative_volume_prior_20']={'value':float(latest['volume']/mean) if mean else None,
            'exact':str(latest['volume']/mean) if mean else None,'unit':'ratio','current_volume':str(latest['volume']),
            'prior_mean_volume':str(mean),'reference_source_indices':[r['index'] for r in prior],
            'start_date':prior[0]['date'],'end_date':prior[-1]['date'],'zero_denominator':mean==0,
            'definition':'Latest volume / preceding 20-observation mean including reported zero volumes; current volume excluded from denominator.'}
        peak=max(r['high'] for r in rows);ratio=latest['close']/peak
        result['close_vs_observed_window_high']={'value':float(ratio),'exact':str(ratio),'unit':'ratio',
            'status':'descriptive_received_window','reference_source_indices':[r['index'] for r in rows],
            'first_date':rows[0]['date'],'last_date':latest['date'],'observations':n,
            'calendar_span_days':(day(latest['date'])-day(rows[0]['date'])).days,
            'high':str(peak),'current_close':str(latest['close']),'annual_window_verified':False,
            'definition':'Current close / maximum reported high in the validated received window. Not a 52-week high or proximity claim.'}
        result['fifty_two_week_high']={'value':None,'unit':'provider_price_unit','status':'unavailable_original_request_is_only_290_calendar_days',
            'definition':'A 290-calendar-day acquisition cannot establish a complete 52-week window. No annual high, annual rank or AT_52W_HIGH tag is inferred.'}
        pairs=[]
        for i in range(n-3,n):
            a,b=rows[i-1],rows[i];change=(b['open']/a['close']-1)*100
            pairs.append({'previous_date':a['date'],'date':b['date'],'previous_source_index':a['index'],'source_index':b['index'],
                          'previous_close':str(a['close']),'open':str(b['open']),'gap_percent_exact':str(change),'gap_up':b['open']>a['close']})
        result['gap_up_count_last_3_reported_pairs']={'value':sum(r['gap_up'] for r in pairs),'unit':'reported pairs','pairs':pairs,
            'definition':'Open exceeds preceding reported close, across the last three reported pairs. Exchange continuity and corporate-action basis unverified.'}
        result['method_notes']=['No percentile rank, composite score, pump confirmation, annualized acceleration, investment direction or size is produced.',
            'One OHLCV root; related transforms are not independent evidence. Reported dates are not verified exchange sessions.',
            'Currency, corporate actions, security continuity and total-return basis remain unverified.']
        return result

def comparisons(stock,benchmark):
    """Match SPY to exact stock endpoint dates; never shift an endpoint or impute zero."""
    output={}
    mapped={r['date']:r for r in benchmark.get('selected_rows',[])}
    for length in (20,60):
        change=(stock.get('measurements') or {}).get('price_change_'+str(length),{})
        item={'value':None,'unit':'percentage_points','status':'stock_window_unavailable',
            'currency_adjustment_total_return_verified':False,'independent_evidence_eligible':False}
        if change.get('value') is not None:
            start,end=change['start_date'],change['end_date']
            item.update(start_date=start,end_date=end,stock_change=change,status='benchmark_exact_endpoints_unavailable')
            if start in mapped and end in mapped:
                a,b=mapped[start],mapped[end]
                with localcontext() as ctx:
                    ctx.prec=40
                    spy=(Decimal(b['close'])/Decimal(a['close'])-1)*100
                    stock_change=(Decimal(change['end_close'])/Decimal(change['start_close'])-1)*100
                    gap=stock_change-spy
                item.update(value=float(gap),exact=str(gap),status='descriptive_same_date_local_price_difference',
                    benchmark_change={'value':float(spy),'exact':str(spy),'start_close':a['close'],'end_close':b['close'],
                        'start_source_index':a['index'],'end_source_index':b['index'],
                        'observations_between_endpoints':sum(start<=r['date']<=end for r in mapped.values())},
                    stock_reported_currency=stock.get('currency'),benchmark_reported_currency=benchmark.get('currency'),
                    definition='Stock reported local-price percentage change minus SPY reported local-price percentage change on exact common dates. Currency/adjustments and intervening sessions unverified; not tradable excess return or alpha.')
        output[str(length)]=item
    return output

def history(attempt,sources,ticker,checked_at):
    at=clock(checked_at)
    if at is None: raise ValueError('Aware observation clock required')
    if attempt.get('endpoint') not in (None, endpoint(ticker)): raise ValueError('Requested issuer differs')
    raw=content(attempt,sources)
    window=attempt.get('request_window')
    if raw is not None:
        if not isinstance(window,dict) or set(window)!={'from','to'} or day(window['to']) is None or day(window['from']) is None or day(window['to'])>at.date() or (day(window['to'])-day(window['from'])).days!=290:
            raise ValueError('Original 290-calendar-day requested span required')
    base={'ticker':ticker,'status':attempt.get('status'),'source_records':None,'selected_indices':[],
          'row_issues':[],'selected_rows':[],'measurements':None,'currency':None,'currency_verified':False,
          'adjustment_basis_verified':False,'session_calendar_verified':False,**FLAGS}
    if raw is None: return base
    if attempt.get('endpoint') != endpoint(ticker): raise ValueError('Original requested issuer differs')
    if attempt.get('http_status') != 200: return {**base,'status':'http_unavailable'}
    try: doc=strict(raw,exact=True)
    except (ValueError,UnicodeError,RecursionError): return {**base,'status':'malformed_original'}
    if not isinstance(doc,list): return {**base,'status':'provider_error_or_unexpected_shape'}
    base['source_records']=len(doc)
    if not doc: return {**base,'status':'reported_empty_history'}
    dates=[day(r.get('date')) if isinstance(r,dict) else None for r in doc]; counts=Counter(dates)
    records=[]; fatal_identity=False; invalid_dates=False
    for i,row in enumerate(doc):
        issues=[]; r=row if isinstance(row,dict) else {}; d=dates[i]
        if r.get('symbol')!=ticker: issues.append('reported_symbol_missing_or_mismatched'); fatal_identity=True
        if d is None: issues.append('invalid_observation_date'); invalid_dates=True
        elif d >= at.date(): issues.append('current_or_future_utc_date')
        if d is not None and counts[d]>1: issues.append('duplicate_observation_date')
        nums={k:number(r.get(k),zero=k=='volume') for k in ('open','high','low','close','volume')}
        if any(v is None for v in nums.values()): issues.append('missing_or_invalid_ohlcv')
        elif not nums['low'] <= min(nums['open'],nums['close']) <= max(nums['open'],nums['close']) <= nums['high']:
            issues.append('inconsistent_ohlc')
        if 'currency' in r and (not isinstance(r['currency'],str) or not re.fullmatch('[A-Z]{3}',r['currency'])): issues.append('invalid_reported_currency')
        if issues: base['row_issues'].append({'index':i,'issues':issues})
        records.append({'index':i,'date':d.isoformat() if d else None,'issues':issues,**nums,'currency':r.get('currency')})
    # Select completed-date observations before quality filtering. Invalid members
    # of that window cannot disappear and manufacture a clean sequence.
    eligible=sorted((r for r in records if r['date'] and day(window['from']) <= day(r['date']) < min(day(window['to'])+timedelta(days=1),at.date())),key=lambda r:(r['date'],r['index']))
    selected=eligible[-250:]; base['selected_indices']=[r['index'] for r in selected]
    base['completed_date_records']=len(eligible);base['request_window']=window; base['selection']='Within the original requested 290-calendar-day span: latest at most 250 reported completed-date occurrences, sorted by date and original index; validated after selection.'
    currencies={r['currency'] for r in selected if r['currency'] is None or isinstance(r['currency'],str)}; base['currency']=next(iter(currencies)) if len(currencies)==1 else None
    if fatal_identity or invalid_dates or any(r['issues'] for r in selected) or len(currencies)>1:
        return {**base,'status':'unresolved_identity_or_window','mixed_reported_currencies':len(currencies)>1}
    if not selected: return {**base,'status':'no_completed_observations'}
    base['latest_observation_age_calendar_days']=(at.date()-day(selected[-1]['date'])).days
    base['age_policy']={'within_five_calendar_days':base['latest_observation_age_calendar_days']<=5,'session_calendar_verified':False}
    base['selected_rows']=[{k:(str(v) if isinstance(v,Decimal) else v) for k,v in r.items()} for r in selected]
    return {**base,'status':'parsed_completed_observations','measurements':measurements(selected)}


def build(input_attempts,attempts,sources,checked_at,limit,benchmark_attempt):
    membership=universe(input_attempts,sources,limit)
    if len(attempts)!=len(membership['selected']):raise ValueError('Every selected ticker requires an acquisition outcome')
    benchmark=history(benchmark_attempt,sources,'SPY',checked_at);records=[]
    for member,a in zip(membership['selected'],attempts):
        if symbol(member['ticker']) is None:
            if a.get('status')!='invalid_symbol_not_requested':raise ValueError('Invalid literal ticker was requested')
            result={'ticker':member['ticker'],'status':a['status'],'measurements':None,**FLAGS}
        else:result=history(a,sources,member['ticker'],checked_at)
        result['benchmark_comparisons']=comparisons(result,benchmark)
        records.append({**member,'acquisition':a,'observations':result})
    return {'universe_membership':membership,'benchmark':{'ticker':'SPY','acquisition':benchmark_attempt,'observations':benchmark},
            'request_records':records,'quality':{'status':'research_only','acquisition_outcomes':dict(Counter(a['status'] for a in attempts)),
            'observation_outcomes':dict(Counter(r['observations']['status'] for r in records)),
            'measured_occurrences':sum((r['observations']['measurements'] or {}).get('status')=='descriptive_observations' for r in records),
            'selection_status':'selected_original_scope' if records else 'no_accepted_membership',
            'market_coverage_verified':False,'independent_roots':1,'selected_universe_has_composite_ancestry':True}}
