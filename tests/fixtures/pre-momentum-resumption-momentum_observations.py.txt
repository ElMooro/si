"""Pure, replayable descriptive OHLCV measurements. No forecasting authority."""
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, localcontext
from urllib.parse import quote
import hashlib, json, math, re

CONTRACT = 'momentum-price-observations.v1'
HEAD = 'data/momentum-breakout.json'
PREFIX = 'data/momentum-breakout/sources/'
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


def universe(attempt, sources, limit):
    if type(limit) is not int or not 1 <= limit <= 100000: raise ValueError('Original configured population bound required')
    if attempt.get('endpoint') != 'data/universe.json': raise ValueError('Original universe endpoint required')
    raw = content(attempt,sources); doc = strict(raw) if raw is not None else None
    if not isinstance(doc,dict) or not isinstance(doc.get('stocks'),list): raise ValueError('Whole declared universe required')
    result=[]; selected=[]
    for i, row in enumerate(doc['stocks']):
        r = row if isinstance(row,dict) else {}; value = r.get('symbol')
        ticker = value.upper().strip() if isinstance(value,str) else None
        candidate = bool(ticker)
        chosen = candidate and len(selected) < limit
        item={'index':i,'ticker':ticker,'raw':row,'selected':chosen,
              'status':'selected_original_scope' if chosen else 'invalid_or_empty_original_symbol' if not candidate else 'original_population_limit'}
        result.append(item)
        if chosen: selected.append({'universe_index':i,'ticker':ticker,'request_index':len(selected)})
    return {'occurrences':result,'selected':selected,'configured_limit':limit,
            'historical_membership_verified':False,'duplicate_memberships_retained':True}


def _float(value):
    return None if value is None else float(value)


def measurements(rows):
    """Dated descriptions only; rows have already passed whole-window validation."""
    n=len(rows)
    if n<30:return {'status':'insufficient_observations','required':30,'observations':n}
    with localcontext() as ctx:
        ctx.prec=40
        close=[r['close'] for r in rows];volume=[r['volume'] for r in rows]
        result={'status':'descriptive_observations','observations':n,'first_date':rows[0]['date'],'last_date':rows[-1]['date']}
        for length in (5,10,20,60):
            item={'value':None,'unit':'percent','intervals':length,'required_observations':length+1,'status':'insufficient_observations'}
            if n>length:
                first,last=rows[-length-1],rows[-1];value=(last['close']/first['close']-1)*100
                item.update(value=float(value),exact=str(value),status='descriptive_local_price_change',
                    start_date=first['date'],end_date=last['date'],start_source_index=first['index'],end_source_index=last['index'],
                    start_close=str(first['close']),end_close=str(last['close']),total_return_verified=False)
            result['price_change_'+str(length)]=item
        for length in (20,60):
            item={'value':None,'unit':'percent','preceding_observations':length,'status':'insufficient_observations',
                  'strict_new_closing_high':None,'equals_previous_closing_high':None}
            if n>length:
                reference=rows[-length-1:-1];peak=max(r['close'] for r in reference);value=(close[-1]/peak-1)*100
                item.update(value=float(value),exact=str(value),status='descriptive_vs_preceding_closes',
                    strict_new_closing_high=close[-1]>peak,equals_previous_closing_high=close[-1]==peak,
                    previous_max_close=str(peak),current_close=str(close[-1]),start_date=reference[0]['date'],end_date=reference[-1]['date'],
                    reference_source_indices=[r['index'] for r in reference],latest_source_index=rows[-1]['index'])
            result['close_vs_preceding_high_'+str(length)]=item
        prior=rows[-21:-1];mean=sum(r['volume'] for r in prior)/20
        result['relative_volume_prior_20']={'value':float(volume[-1]/mean) if mean else None,
            'exact':str(volume[-1]/mean) if mean else None,'unit':'ratio','current_volume':str(volume[-1]),
            'prior_mean_volume':str(mean),'reference_source_indices':[r['index'] for r in prior],
            'start_date':prior[0]['date'],'end_date':prior[-1]['date'],'zero_denominator':mean==0,
            'definition':'Latest reported volume / preceding 20-observation mean; current observation excluded from denominator.'}
        mean_inclusive=sum(volume[-20:])/20
        matches=[rows[i]['index'] for i in range(n-19,n) if volume[i]>mean_inclusive and close[i]>close[i-1]]
        result['up_price_high_volume_pairs_19']={'value':len(matches),'unit':'reported pairs','pairs':19,'source_indices':matches,
            'start_date':rows[-20]['date'],'end_date':rows[-1]['date'],'volume_mean_exact':str(mean_inclusive),
            'definition':'19 adjacent close increases with volume above the same trailing 20-observation mean. Descriptive self-inclusive benchmark; not institutional accumulation.'}
        nominal=sum(c*v for c,v in zip(close[-20:],volume[-20:]))/20
        result['nominal_price_volume_20']={'value':float(nominal),'exact':str(nominal),
            'unit':'provider_price_unit_times_provider_volume_unit','currency_verified':False,
            'source_indices':[r['index'] for r in rows[-20:]],'start_date':rows[-20]['date'],'end_date':rows[-1]['date']}
        result['method_notes']=['Reported observation counts are not verified exchange sessions.',
            'Price levels, currency, corporate actions, security continuity and total-return basis are not verified.',
            'No high tolerance is relabeled as a breakout. Missing 60-observation windows stay missing.',
            'Descriptions of one OHLCV source are not independent evidence or qualified direction.']
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
    eligible=sorted((r for r in records if r['date'] and day(r['date']) < at.date()),key=lambda r:(r['date'],r['index']))
    selected=eligible[-90:]; base['selected_indices']=[r['index'] for r in selected]
    base['completed_date_records']=len(eligible);base['selection']='Latest at most 90 reported completed-date occurrences, sorted by date and original index; validated after selection.'
    currencies={r['currency'] for r in selected if r['currency'] is None or isinstance(r['currency'],str)}; base['currency']=next(iter(currencies)) if len(currencies)==1 else None
    if fatal_identity or invalid_dates or any(r['issues'] for r in selected) or len(currencies)>1:
        return {**base,'status':'unresolved_identity_or_window','mixed_reported_currencies':len(currencies)>1}
    if not selected: return {**base,'status':'no_completed_observations'}
    base['latest_observation_age_calendar_days']=(at.date()-day(selected[-1]['date'])).days
    base['age_policy']={'within_five_calendar_days':base['latest_observation_age_calendar_days']<=5,'session_calendar_verified':False}
    base['selected_rows']=[{k:(str(v) if isinstance(v,Decimal) else v) for k,v in r.items()} for r in selected]
    return {**base,'status':'parsed_completed_observations','measurements':measurements(selected)}


def build(universe_attempt,attempts,sources,checked_at,limit,benchmark_attempt,nominal_gate):
    membership=universe(universe_attempt,sources,limit)
    if len(attempts)!=len(membership['selected']): raise ValueError('Every selected occurrence requires an acquisition outcome')
    benchmark=history(benchmark_attempt,sources,'SPY',checked_at)
    records=[]
    for member,a in zip(membership['selected'],attempts):
        if symbol(member['ticker']) is None:
            if a.get('status')!='invalid_symbol_not_requested': raise ValueError('Invalid issuer was requested')
            result={'ticker':member['ticker'],'status':a['status'],'measurements':None,**FLAGS}
        else: result=history(a,sources,member['ticker'],checked_at)
        if result.get('measurements',{} ) and result['measurements']['status']=='descriptive_observations':
            result['measurements']['nominal_price_volume_20']['original_nominal_gate_passed']=Decimal(result['measurements']['nominal_price_volume_20']['exact'])>=Decimal(str(nominal_gate))
            result['measurements']['nominal_price_volume_20']['configured_nominal_gate']=nominal_gate
        result['benchmark_comparisons']=comparisons(result,benchmark)
        records.append({**member,'acquisition':a,'observations':result})
    counts=dict(Counter(r['observations']['status'] for r in records))
    return {'benchmark':{'ticker':'SPY','acquisition':benchmark_attempt,'observations':benchmark},'universe_membership':membership,'request_records':records,'quality':{'status':'research_only',
        'acquisition_outcomes':dict(Counter(a['status'] for a in attempts)),'observation_outcomes':counts,
        'measured_occurrences':sum((r['observations']['measurements'] or {}).get('status')=='descriptive_observations' for r in records),
        'market_coverage_verified':False,'independent_roots':1}}
