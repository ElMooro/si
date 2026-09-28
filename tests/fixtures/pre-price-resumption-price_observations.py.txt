"""Pure, replayable descriptive OHLCV measurements. No forecasting authority."""
from collections import Counter
from datetime import date, datetime
from decimal import Decimal, localcontext
from urllib.parse import quote
import hashlib, json, math, re

CONTRACT = 'price-compression-observations.v1'
HEAD = 'data/volatility-squeeze.json'
PREFIX = 'data/volatility-squeeze/sources/'
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
    raw = content(attempt,sources); doc = strict(raw) if raw is not None else None
    if not isinstance(doc,dict) or not isinstance(doc.get('stocks'),list): raise ValueError('Whole declared universe required')
    result=[]; selected=[]
    for i, row in enumerate(doc['stocks']):
        r = row if isinstance(row,dict) else {}; value = r.get('symbol')
        ticker = value.upper() if isinstance(value,str) else None
        candidate = r.get('cap_bucket') in ('micro','small','mid','large','mega') and bool(value)
        chosen = candidate and len(selected) < limit
        item={'index':i,'ticker':ticker,'raw':row,'selected':chosen,
              'status':'selected_original_scope' if chosen else 'outside_original_cap_scope' if not candidate else 'original_population_limit'}
        result.append(item)
        if chosen: selected.append({'universe_index':i,'ticker':ticker,'request_index':len(selected)})
    return {'occurrences':result,'selected':selected,'configured_limit':limit,
            'historical_membership_verified':False,'duplicate_memberships_retained':True}


def _float(value):
    return None if value is None else float(value)


def _cdf(values):
    current=values[-1]; reference=values[-252:]
    below=sum(v < current for v in reference); tied=sum(v == current for v in reference)
    return {'value':100*(below+tied)/len(reference),'unit':'percent','reference_count':len(reference),
            'strictly_below':below,'equal':tied,'definition':'Inclusive empirical CDF; includes current observation; all ties rank 100, not a forecast probability.'}


def measurements(rows):
    """rows are complete, unique, ordered, validated observations, maximum 300."""
    n=len(rows)
    if n < 200: return {'status':'insufficient_observations','required':200,'observations':n}
    with localcontext() as ctx:
        ctx.prec=40
        close=[r['close'] for r in rows]; high=[r['high'] for r in rows]; low=[r['low'] for r in rows]; volume=[r['volume'] for r in rows]
        bb=[]
        for end in range(20,n+1):
            win=close[end-20:end]; mean=sum(win)/20
            variance=sum((x-mean)**2 for x in win)/20
            bb.append(4*variance.sqrt()/mean*100)
        tr=[max(high[i]-low[i],abs(high[i]-close[i-1]),abs(low[i]-close[i-1])) for i in range(1,n)]
        atr=[sum(tr[end-20:end])/20/close[end]*100 for end in range(20,n)]
        atr_value=sum(tr[-20:])/20; mean=sum(close[-20:])/20
        sd=(sum((x-mean)**2 for x in close[-20:])/20).sqrt()
        nr_strict=[]; nr_tied=[]; inside=[]; equal=[]
        for i in range(n-30,n):
            width=high[i]-low[i]; prior=[high[j]-low[j] for j in range(i-6,i)]
            if width < min(prior): nr_strict.append(rows[i]['index'])
            elif width == min(prior): nr_tied.append(rows[i]['index'])
            if high[i] <= high[i-1] and low[i] >= low[i-1]:
                (equal if high[i]==high[i-1] and low[i]==low[i-1] else inside).append(rows[i]['index'])
        ranges={}
        for length in (66,126,252):
            if n < length: ranges[str(length)]={'value':None,'status':'insufficient_observations','required':length}
            else:
                hi=max(high[-length:]); lo=min(low[-length:])
                ranges[str(length)]={'value':_float((hi-lo)/hi*100),'unit':'percent','observations':length,
                    'first_date':rows[-length]['date'],'last_date':rows[-1]['date']}
        volume_ratio=(sum(volume[-60:])/60)/(sum(volume[-180:])/180) if sum(volume[-180:]) else None
        tail=0
        for x in reversed(close[-200:]):
            if abs(x/close[-1]-1) > Decimal('.15'): break
            tail+=1
        lo=min(close[-60:]); hi=max(close[-60:])
        nominal=sum(c*v for c,v in zip(close[-30:],volume[-30:]))/30
        changes={str(w):{'value':_float((close[-1]/close[-w-1]-1)*100),'unit':'percent',
                    'start_date':rows[-w-1]['date'],'end_date':rows[-1]['date'],'observation_intervals':w} for w in (5,30,90)}
        return {'status':'descriptive_observations','observations':n,'first_date':rows[0]['date'],'last_date':rows[-1]['date'],
            'bb_width_20':{'value':_float(bb[-1]),'unit':'percent','definition':'4 × population standard deviation / mean of latest 20 closes × 100; includes latest close.'},
            'bb_width_cdf':_cdf(bb),'atr20_close_percent':{'value':_float(atr[-1]),'unit':'percent','definition':'Simple mean of latest 20 true ranges / latest close × 100; requires previous close.'},
            'atr_cdf':_cdf(atr),'bb_inside_sma_atr_envelope':{'value':2*sd < Decimal('1.5')*atr_value,'definition':'Strict BB(20,2 population sigma) inside SMA20 ± 1.5 × simple ATR20. Not an EMA Keltner/TTM implementation.'},
            'nr7_30':{'strict_count':len(nr_strict),'tied_minimum_count':len(nr_tied),'strict_source_indices':nr_strict,'tied_source_indices':nr_tied,'observations':30},
            'inside_30':{'strictly_contained_count':len(inside),'equal_range_count':len(equal),'strict_source_indices':inside,'equal_source_indices':equal,'observations':30},
            'nested_high_low_range':ranges,'volume_mean_60_over_180':{'value':_float(volume_ratio),'unit':'ratio','zero_denominator':not bool(sum(volume[-180:]))},
            'trailing_closes_within_15pct':{'count':tail,'maximum_observations':200,'reference_close':str(close[-1]),'definition':'Consecutive closes within ±15% of latest close; observations, not calendar/session days.'},
            'close_range_position_60':{'value':_float((close[-1]-lo)/(hi-lo)*100) if hi>lo else None,'unit':'percent','zero_denominator':hi==lo},
            'price_changes':changes,'nominal_price_volume_30':{'value':_float(nominal),'exact':str(nominal),'unit':'provider_price_unit_times_provider_volume_unit',
                'original_nominal_gate_passed':nominal>=1000000,'currency_verified':False},
            'method_notes':['Nested high-low ranges are not sequential drawdown corrections or a VCP pattern.',
                'No exchange-calendar, split/dividend adjustment, currency, security continuity or total-return identity is proven.',
                'Transforms share one OHLCV root. No independent signal count, score, tier, trade direction or forecast probability.']}


def history(attempt,sources,ticker,checked_at):
    at=clock(checked_at)
    if at is None: raise ValueError('Aware observation clock required')
    raw=content(attempt,sources)
    base={'ticker':ticker,'status':attempt.get('status'),'source_records':None,'selected_indices':[],
          'row_issues':[],'measurements':None,'currency':None,'currency_verified':False,
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
    selected=eligible[-300:]; base['selected_indices']=[r['index'] for r in selected]
    base['completed_date_records']=len(eligible);base['selection']='Latest at most 300 reported completed-date occurrences, sorted by date and original index; validated after selection.'
    currencies={r['currency'] for r in selected if r['currency'] is None or isinstance(r['currency'],str)}; base['currency']=next(iter(currencies)) if len(currencies)==1 else None
    if fatal_identity or invalid_dates or any(r['issues'] for r in selected) or len(currencies)>1:
        return {**base,'status':'unresolved_identity_or_window','mixed_reported_currencies':len(currencies)>1}
    if not selected: return {**base,'status':'no_completed_observations'}
    base['latest_observation_age_calendar_days']=(at.date()-day(selected[-1]['date'])).days
    base['age_policy']={'within_five_calendar_days':base['latest_observation_age_calendar_days']<=5,'session_calendar_verified':False}
    return {**base,'status':'parsed_completed_observations','measurements':measurements(selected)}


def build(universe_attempt,attempts,sources,checked_at,limit):
    membership=universe(universe_attempt,sources,limit)
    if len(attempts)!=len(membership['selected']): raise ValueError('Every selected occurrence requires an acquisition outcome')
    records=[]
    for member,a in zip(membership['selected'],attempts):
        if symbol(member['ticker']) is None:
            if a.get('status')!='invalid_symbol_not_requested': raise ValueError('Invalid issuer was requested')
            result={'ticker':member['ticker'],'status':a['status'],'measurements':None,**FLAGS}
        else: result=history(a,sources,member['ticker'],checked_at)
        records.append({**member,'acquisition':a,'observations':result})
    counts=dict(Counter(r['observations']['status'] for r in records))
    return {'universe_membership':membership,'request_records':records,'quality':{'status':'research_only',
        'acquisition_outcomes':dict(Counter(a['status'] for a in attempts)),'observation_outcomes':counts,
        'measured_occurrences':sum((r['observations']['measurements'] or {}).get('status')=='descriptive_observations' for r in records),
        'market_coverage_verified':False,'independent_roots':1}}
