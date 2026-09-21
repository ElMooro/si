"""Exact, dated comparisons of retained FX quote bars; no investment authority."""
from collections import Counter
from datetime import datetime, timezone, timedelta
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
from fractions import Fraction
import json, re, urllib.parse
import fx_quote_capture as capture

CONTRACT='fx-original-quote-research.v1'
CURRENT='data/fx-quote-research.json'
LEGACY='data/polygon-fx-regime.json'
PREFIX='data/fx-quote-research/'
PRIVATE='audit-private/20260909-originals/fx-research/'
MAX=16*1024*1024
FLAGS=('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')
PERMISSIONS=dict.fromkeys(FLAGS,False)
FIELDS=('o','h','l','c','v','vw','n','t')
WINDOWS=(1,5,20)
BLOCK=128
EPOCH=datetime(1970,1,1,tzinfo=timezone.utc)
sha=capture.sha


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def clock(value):
    if not isinstance(value,str):raise ValueError('Aware source clock required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Timezone required')
    return result.astimezone(timezone.utc)
def instant(ms):return (EPOCH+timedelta(milliseconds=ms)).isoformat()
def strict(raw):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Bounded bytes required')
    return capture.decode(raw)
def fraction(value):return {'numerator':str(value.numerator),'denominator':str(value.denominator)}
def shown(value):
    with localcontext() as context:
        context.prec=160;context.rounding=ROUND_HALF_EVEN
        result=Decimal(value.numerator)/Decimal(value.denominator) if isinstance(value,Fraction) else value
        rounded=result.quantize(Decimal('0.000000000001'))
        return format(rounded,'f').rstrip('0').rstrip('.') if rounded else '0'
def checked_original(ref,read):
    if (not isinstance(ref,dict) or not isinstance(ref.get('sha256'),str) or not re.fullmatch('[a-f0-9]{64}',ref['sha256'])
            or ref.get('key')!=PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):
        raise ValueError('Exact FX original reference required')
    raw=read(ref['key'])
    if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('FX original bytes differ')
    return raw
def ref(raw,kind):
    if kind not in ('bars','inputs','outputs','runs','compilers'):raise ValueError('Reviewed FX artifact kind required')
    return {'key':PREFIX+kind+'/'+sha(raw)+('.py' if kind=='compilers' else '.json'),'sha256':sha(raw),'bytes':len(raw)}


def numeric(row,key):
    state=capture.numeric_state(row,key);value=row.get(key)
    if state in ('positive','zero','negative'):
        value=Decimal(value)
        if abs(value.adjusted())>32 or len(value.as_tuple().digits)>80:return {'state':'outside_numeric_bound','decimal':None}
        return {'state':state,'decimal':format(value,'f')}
    return {'state':state,'decimal':None}


def bar(row,ordinal,page,index):
    valid=isinstance(row,dict);source=row if valid else {}
    values={key:numeric(source,key) for key in FIELDS}
    t=source.get('t');stamp=None;issues=[]
    if type(t) is int and 946684800000<=t<=4102444800000:
        stamp=instant(t)
        if clock(stamp)>clock(page['acquired_at']):stamp=None;issues.append('future_window_start')
    else:issues.append('invalid_or_missing_window_start')
    if not valid:issues.append('non_object_source_row')
    prices={key:Decimal(values[key]['decimal']) for key in ('o','h','l','c') if values[key]['decimal'] is not None}
    consistent=None
    if all(key in prices and prices[key]>0 for key in ('o','h','l','c')):
        consistent=prices['l']<=min(prices['o'],prices['c'])<=max(prices['o'],prices['c'])<=prices['h']
        if not consistent:issues.append('inconsistent_reported_ohlc')
    usable=values['c']['state']=='positive' and consistent is not False
    return {'ordinal':ordinal,'original_object_row':valid,'values':values,'window_start_utc':stamp,
        'quote_observed_at':None,'close_observed_at':None,'bar_finality_verified':False,
        'positive_close_usable_as_reported':usable,'ohlc_bounds_consistent':consistent,'issues':issues,
        'extra_field_names':sorted(set(source)-set(FIELDS)),
        'source':{'original':page['original'],'pointer':'/results/'+str(index),'page':page['page'],
            'request_sha256':page['request_sha256'],'acquired_at':page['acquired_at']}}


def source_rows(pair,source,generated_at,read):
    if (not isinstance(source,dict) or source.get('contract')!=capture.CONTRACT or source.get('pair')!=pair
            or source.get('provider_ticker')!=capture.PAIRS[pair] or not isinstance(source.get('pages'),list)
            or not 1<=len(source['pages'])<=capture.MAX_PAGES):raise ValueError('Exact FX capture required')
    start,end=clock(source['started_at']),clock(source['completed_at'])
    if not start<=end<=clock(generated_at):raise ValueError('FX capture clocks differ')
    expected=capture.initial(pair,source['from'],source['to']);rows=[];complete=True;last_doc=None
    seen=set()
    for index,page in enumerate(source['pages']):
        url=page['request_url'];digest=sha(url.encode())
        if (page['page']!=index+1 or url!=expected or url!=capture.canonical(url,pair,source['from'],source['to'])
                or page['request_sha256']!=digest or digest in seen or not start<=clock(page['acquired_at'])<=end):
            raise ValueError('FX captured request chain differs')
        seen.add(digest);last_doc=None
        if page.get('original') is None:
            if page.get('status')=='received':raise ValueError('Received FX original missing')
            complete=False
        else:
            raw=checked_original(page['original'],read)
            if page.get('status')!='received':raise ValueError('Unexpected FX retained response state')
            if page.get('http_status')==200:
                try:last_doc=capture.envelope(raw,pair)
                except (ValueError,UnicodeDecodeError):complete=False
                if last_doc is not None:
                    for position,item in enumerate(last_doc['results']):rows.append(bar(item,len(rows),page,position))
            else:complete=False
        next_value=last_doc.get('next_url') if last_doc is not None else None
        if index+1<len(source['pages']):
            if not next_value:raise ValueError('FX capture contains an unlinked later page')
            expected=capture.canonical(next_value,pair,source['from'],source['to'])
    complete=complete and last_doc is not None and not last_doc.get('next_url')
    limit=int(dict(urllib.parse.parse_qsl(urllib.parse.urlsplit(source['pages'][-1]['request_url']).query)).get('limit','5000'))
    if last_doc is not None and type(last_doc.get('queryCount')) is int and last_doc['queryCount']>=limit:complete=False
    if source.get('pagination_complete') is not complete or ((source.get('stop')=='complete_returned_pagination') is not complete):
        raise ValueError('FX pagination completeness differs')
    return rows,complete


def endpoint(row):
    return {'ordinal':row['ordinal'],'window_start_utc':row['window_start_utc'],
        'reported_close_decimal':row['values']['c']['decimal'],'source':row['source'],
        'close_state':row['values']['c']['state'],'positive_close_usable_as_reported':row['positive_close_usable_as_reported'],
        'ohlc_bounds_consistent':row['ohlc_bounds_consistent'],
        'quote_observed_at':None,'close_observed_at':None,'bar_finality_verified':False}


def comparison(ordered,n,available):
    out={'requested_row_offset':n,'unit':'percent_change_in_reported_quote_close','available':False,
        'quoted_rate_change_decimal':None,'inverse_rate_change_decimal':None,
        'from':None,'to':None,'elapsed_calendar_days_decimal':None,
        'meaning':'Observed provider-row offsets, not assumed trading days; window starts are not close timestamps.',**PERMISSIONS}
    if not available:out['reason']='incomplete_capture_or_ambiguous_chronology';return out
    if len(ordered)<=n:out['reason']='insufficient_returned_rows';return out
    first,last=ordered[-1-n],ordered[-1];window=ordered[-1-n:]
    out.update({'from':endpoint(first),'to':endpoint(last),'returned_rows_in_window':len(window),
        'positive_close_rows_in_window':sum(r['positive_close_usable_as_reported'] for r in window),
        'source_row_ordinals':[r['ordinal'] for r in window],
        'elapsed_calendar_days_decimal':shown(Fraction(int(last['values']['t']['decimal'])-int(first['values']['t']['decimal']),86400000))})
    if not first['positive_close_usable_as_reported'] or not last['positive_close_usable_as_reported']:
        out['reason']='endpoint_close_unavailable_or_inconsistent';return out
    a,b=Fraction(Decimal(first['values']['c']['decimal'])),Fraction(Decimal(last['values']['c']['decimal']))
    direct=(b/a-1)*100;inverse=(a/b-1)*100
    out.update(available=True,reason=None,quoted_rate_change_decimal=shown(direct),inverse_rate_change_decimal=shown(inverse),
        quoted_rate_change_exact=fraction(direct),inverse_rate_change_exact=fraction(inverse),
        formula='100 * (new_close / old_close - 1); inverse: 100 * (old_close / new_close - 1)',
        decimal_display_places=12,decimal_rounding='ROUND_HALF_EVEN')
    return out


def compile_output(sources,generated_at,read,emit):
    clock(generated_at)
    if not isinstance(sources,dict) or set(sources)!=set(capture.PAIRS):raise ValueError('All nineteen FX identities required')
    pairs={};total=0
    for pair in capture.PAIRS:
        source=sources[pair];rows,complete=source_rows(pair,source,generated_at,read)
        stamps=[r['window_start_utc'] for r in rows];valid_clock=bool(rows) and all(x is not None for x in stamps) and len(set(stamps))==len(stamps)
        ordered=sorted(rows,key=lambda r:r['window_start_utc']) if valid_clock else []
        blocks=[]
        for offset in range(0,len(rows),BLOCK):
            doc={'contract':'fx-original-bars.v1','pair':pair,'offset':offset,'rows':rows[offset:offset+BLOCK]}
            expected=ref(encoded(doc),'bars');actual=emit('bars',doc)
            if actual!=expected:raise ValueError('FX emitted row block differs')
            blocks.append(actual)
        base,quote=pair.split('_');metal=base in ('XAU','XAG')
        latest=endpoint(ordered[-1]) if ordered else None
        unit=quote+'_per_'+('provider_'+base+'_unit' if metal else base)
        pairs[pair]={'pair':pair,'provider_ticker':capture.PAIRS[pair],'base_code':base,'quote_code':quote,
            'price_unit':unit,'metal_base_quantity_unit_verified':False if metal else None,
            'source_capture_completed_at':source['completed_at'],
            'source_review_due_at':(clock(source['completed_at'])+timedelta(hours=26)).isoformat(),
            'coverage':{'returned_rows':len(rows),'object_rows':sum(r['original_object_row'] for r in rows),
                'positive_close_rows':sum(r['positive_close_usable_as_reported'] for r in rows),
                'close_states':dict(Counter(r['values']['c']['state'] for r in rows)),
                'invalid_window_start_rows':sum(x is None for x in stamps),
                'duplicate_window_start_rows':sum(n-1 for stamp,n in Counter(stamps).items() if stamp is not None and n>1),
                'pagination_complete':complete,'full_calendar_coverage_verified':False,'stop':source['stop']},
            'request_window':{'from':source['from'],'to':source['to'],'calendar':'provider-described Eastern Time request dates'},
            'bar_blocks':blocks,'chronological_source_ordinals':[r['ordinal'] for r in ordered],
            'chronology_unambiguous':valid_clock,'latest_reported_row':latest,
            'comparisons':{str(n):comparison(ordered,n,complete and valid_clock) for n in WINDOWS},**PERMISSIONS}
        total+=len(rows)
    completed=max(clock(s['completed_at']) for s in sources.values()).isoformat()
    return {'contract':CONTRACT,'generated_at':generated_at,'source_capture_completed_at':completed,
        'source_review_due_at':(clock(completed)+timedelta(hours=26)).isoformat(),
        'pairs':pairs,'configured_pairs':len(capture.PAIRS),'returned_rows':total,
        'quality':{'status':'descriptive','complete_returned_captures':sum(p['coverage']['pagination_complete'] for p in pairs.values()),
            'unambiguous_chronologies':sum(p['chronology_unambiguous'] for p in pairs.values())},
        'field_definitions':{'o':'Reported aggregate opening quote price','h':'Reported aggregate highest quote price',
            'l':'Reported aggregate lowest quote price','c':'Reported aggregate closing quote price',
            'vw':'Provider-reported weighted price; weighting basis unqualified',
            'v':'Provider-reported v; not asserted exchange traded volume',
            'n':'Provider-reported n; not asserted executed trades',
            't':'Unix milliseconds at aggregate window start, not quote or close observation time'},
        'source_documentation':'https://massive.com/docs/rest/forex/aggregates/custom-bars',
        'method':'Exact retained quote-close comparisons across stated provider-row offsets and actual dated spans.',
        'limits':['Bar finality and individual quote/close clocks are not independently established.',
            'Shared currency legs are not independent investment evidence.',
            'Spot quote changes do not establish dollar-index returns, funding rates, carry profit or a risk regime.'],
        'call':None,'score':None,'portfolio_action':'WAIT','independent_investment_votes':0,**PERMISSIONS}
