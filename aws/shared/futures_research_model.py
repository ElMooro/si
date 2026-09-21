"""Original, dated futures measurements. No continuous roll or investment authority."""
from collections import Counter
from datetime import date,datetime,timezone,timedelta
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from fractions import Fraction
import json,re
import futures_source_capture as capture

CONTRACT='futures-original-research.v1'
CURRENT='data/futures-research.json'
LEGACY='data/polygon-futures-curves.json'
PREFIX='data/futures-research/'
PRIVATE='audit-private/20260909-originals/futures-research/'
MAX=16*1024*1024
FLAGS=('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')
PERMISSIONS=dict.fromkeys(FLAGS,False)
NUMERIC=('open','high','low','close','settlement_price','volume','dollar_volume','transactions','window_start')
BLOCK=128
UNITS={'ES':('IPNT','index point'),'NQ':('IPNT','index point'),'CL':('BBL','barrel'),
    'GC':('TRYOZ','troy ounce'),'SI':('TRYOZ','troy ounce'),'HG':('LBS','pound'),'NG':('MMBTU','MMBtu')}
sha=capture.sha


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def strict(raw):
    if not isinstance(raw,bytes) or not 0<len(raw)<=MAX:raise ValueError('Bounded futures bytes required')
    return capture.decode(raw)
def clock(value):
    if not isinstance(value,str):raise ValueError('Aware source clock required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None:raise ValueError('Timezone required')
    return result.astimezone(timezone.utc)
def fraction(value):return {'numerator':str(value.numerator),'denominator':str(value.denominator)}
def shown(value):
    with localcontext() as context:
        context.prec=160;context.rounding=ROUND_HALF_EVEN
        v=Decimal(value.numerator)/Decimal(value.denominator) if isinstance(value,Fraction) else value
        rounded=v.quantize(Decimal('0.000000000001'))
        return format(rounded,'f').rstrip('0').rstrip('.') if rounded else '0'
def numeric(row,key):
    state=capture.numeric_state(row,key)
    if state in ('positive','negative','zero'):
        v=Decimal(row[key])
        if abs(v.adjusted())>32 or len(v.as_tuple().digits)>80:return {'state':'outside_numeric_bound','decimal':None}
        return {'state':state,'decimal':format(v,'f')}
    return {'state':state,'decimal':None}
def checked_original(ref,read):
    if (not isinstance(ref,dict) or not isinstance(ref.get('sha256'),str) or not re.fullmatch('[a-f0-9]{64}',ref['sha256'])
            or ref.get('key')!=PRIVATE+ref['sha256']+'.bin' or type(ref.get('bytes')) is not int or not 0<ref['bytes']<=MAX):
        raise ValueError('Exact original futures reference required')
    raw=read(ref['key'])
    if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or sha(raw)!=ref['sha256']:raise ValueError('Original futures bytes differ')
    return raw
def ref(raw,kind):
    if kind not in ('records','inputs','outputs','runs','compilers'):raise ValueError('Reviewed futures artifact required')
    return {'key':PREFIX+kind+'/'+sha(raw)+('.py' if kind=='compilers' else '.json'),'sha256':sha(raw),'bytes':len(raw)}
def pointer(page,index):return {'original':page['original'],'pointer':'/results/'+str(index),'page':page['page'],
    'request_sha256':page['request_sha256'],'acquired_at':page['acquired_at']}
def typed(value):
    if value is None or type(value) in (str,bool):return value
    if type(value) is int or isinstance(value,Decimal):return {'number':numeric({'value':value},'value')}
    if isinstance(value,list):return [typed(v) for v in value]
    if isinstance(value,dict):return {k:typed(v) for k,v in value.items()}
    raise ValueError('Unsupported source value')


def source_rows(source,generated_at,read):
    if not isinstance(source,dict) or source.get('contract')!=capture.CONTRACT or not 1<=len(source.get('pages',[]))<=capture.MAX_PAGES:
        raise ValueError('Exact futures capture required')
    scope=source['scope'];expected=capture.initial(scope)
    start,end=clock(source['started_at']),clock(source['completed_at'])
    if not start<=end<=clock(generated_at):raise ValueError('Futures capture clocks differ')
    rows=[];pointers=[];complete=True;last=None;seen=set()
    for n,page in enumerate(source['pages']):
        url=page['request_url'];digest=sha(url.encode())
        if (page['page']!=n+1 or url!=expected or url!=capture.canonical(url,scope) or digest!=page['request_sha256']
                or digest in seen or not start<=clock(page['acquired_at'])<=end):raise ValueError('Futures request chain differs')
        seen.add(digest);last=None
        if page.get('original') is None:
            if page.get('status')=='received':raise ValueError('Received futures original missing')
            complete=False
        else:
            raw=checked_original(page['original'],read)
            if page.get('status')!='received':raise ValueError('Retained futures response state differs')
            if page.get('http_status')==200:
                try:last=capture.envelope(raw)
                except (ValueError,UnicodeDecodeError):complete=False
                if last is not None:
                    rows.extend(last['results']);pointers.extend(pointer(page,i) for i in range(len(last['results'])))
            else:complete=False
        if n+1<len(source['pages']):
            if last is None or not last.get('next_url'):raise ValueError('Unlinked futures page')
            expected=capture.canonical(last['next_url'],scope)
    complete=complete and last is not None and not last.get('next_url')
    if source.get('pagination_complete') is not complete or ((source.get('stop')=='complete_returned_pagination') is not complete):
        raise ValueError('Futures pagination completeness differs')
    return rows,pointers,complete


def bar(row,ordinal,source,ticker,definition):
    valid=isinstance(row,dict);r=row if valid else {};issues=[]
    values={k:numeric(r,k) for k in NUMERIC};session=r.get('session_end_date');stamp=r.get('window_start')
    try:
        d=date.fromisoformat(session)
        if d.isoformat()!=session or d>clock(source['acquired_at']).date():raise ValueError('Invalid session date')
    except (ValueError,TypeError):session=None;issues.append('invalid_session_date')
    if session is not None and not definition['first_trade_date']<=session<=definition['last_trade_date']:
        session=None;issues.append('session_outside_contract_trade_dates')
    if type(stamp) is not int or not 946684800000000000<=stamp<=4102444800000000000:
        issues.append('invalid_nanosecond_window');stamp=None
    elif stamp>int(clock(source['acquired_at']).timestamp())*1000000000:
        issues.append('future_window');stamp=None
    if r.get('ticker')!=ticker:issues.append('contract_identity_mismatch')
    px={k:Decimal(values[k]['decimal']) for k in ('open','high','low','close') if values[k]['decimal'] is not None}
    consistent=None
    if len(px)==4:
        consistent=px['low']<=min(px['open'],px['close'])<=max(px['open'],px['close'])<=px['high']
        if not consistent:issues.append('inconsistent_reported_ohlc')
    identity=valid and session is not None and stamp is not None and r.get('ticker')==ticker
    return {'ordinal':ordinal,'ticker':r.get('ticker'),'session_end_date':session,'window_start_ns':str(stamp) if stamp is not None else None,
        'values':values,'identity_and_clock_qualified':identity,'ohlc_bounds_consistent':consistent,'issues':issues,
        'close_usable_as_reported':identity and values['close']['decimal'] is not None and consistent is not False,
        'settlement_usable_as_reported':identity and values['settlement_price']['decimal'] is not None,
        'bar_finality_independently_verified':False,'close_observed_at':None,'source':source}


def endpoint(row,field):return {'ordinal':row['ordinal'],'ticker':row['ticker'],'session_end_date':row['session_end_date'],
    'window_start_ns':row['window_start_ns'],'field':field,'reported_decimal':row['values'][field]['decimal'],'source':row['source']}
def comparison(ordered,n,field,available):
    out={'requested_row_offset':n,'price_field':field,'available':False,'from':None,'to':None,
        'absolute_change_decimal':None,'percent_change_decimal':None,**PERMISSIONS}
    if not available:out['reason']='incomplete_capture_or_ambiguous_identity_or_chronology';return out
    if len(ordered)<=n:out['reason']='insufficient_returned_rows';return out
    a,b=ordered[-1-n],ordered[-1];out.update({'from':endpoint(a,field),'to':endpoint(b,field),
        'elapsed_calendar_days':(date.fromisoformat(b['session_end_date'])-date.fromisoformat(a['session_end_date'])).days,
        'source_row_ordinals':[r['ordinal'] for r in ordered[-1-n:]]})
    flag='close_usable_as_reported' if field=='close' else 'settlement_usable_as_reported'
    if not a[flag] or not b[flag]:out['reason']='reported_endpoint_unavailable_or_inconsistent';return out
    first,last=(Fraction(Decimal(r['values'][field]['decimal'])) for r in (a,b));delta=last-first
    out.update(available=True,reason=None,absolute_change_decimal=shown(delta),absolute_change_exact=fraction(delta),
        percent_change_reason=None if first>0 else 'nonpositive_base_percent_withheld')
    if first>0:
        pct=100*delta/first;out.update(percent_change_decimal=shown(pct),percent_change_exact=fraction(pct))
    return out


def specification(product,rows,pointers,complete,asof):
    out={'available':False,'provider_reported':None,'source':None,'usd_value_per_price_unit_per_contract_decimal':None,
        'quantity_conversion_qualified':False,'independent_exchange_crosscheck':False}
    if not complete or len(rows)!=1 or not isinstance(rows[0],dict):out['reason']='incomplete_or_ambiguous_product_definition';return out
    row=rows[0];out.update(provider_reported=typed(row),source=pointers[0])
    if any(row.get(k)!=v for k,v in {'product_code':product,'date':asof,'trading_venue':capture.PRODUCTS[product],'type':'single'}.items()):
        out['reason']='product_identity_or_vintage_mismatch';return out
    out.update(available=True,reason=None)
    unit,noun=UNITS[product];quantity=numeric(row,'unit_of_measure_qty')
    if (row.get('trade_currency_code')=='USD' and row.get('settlement_currency_code')=='USD'
            and row.get('unit_of_measure')==unit and row.get('price_quotation')=='U.S. dollars and cents per '+noun
            and quantity['state']=='positive'):
        out.update(quantity_conversion_qualified=True,usd_value_per_price_unit_per_contract_decimal=quantity['decimal'])
    return out


def matched_curve(first,second,field,available):
    out={'near_ticker':first['ticker'],'far_ticker':second['ticker'],'price_field':field,'available':False,
        'meaning':'Far minus near reported price on the latest common returned session; not a roll return or supply inference.',**PERMISSIONS}
    if not available or not first['chronology_unambiguous'] or not second['chronology_unambiguous']:
        out['reason']='unqualified_contracts_units_or_chronology';return out
    left={r['session_end_date']:r for r in first['_ordered']};right={r['session_end_date']:r for r in second['_ordered']}
    common=sorted(set(left)&set(right))
    if not common:out['reason']='no_common_reported_session';return out
    session=common[-1];a,b=left[session],right[session];out.update(session_end_date=session,near=endpoint(a,field),far=endpoint(b,field),
        common_returned_sessions=len(common),latest_near_session=first['_ordered'][-1]['session_end_date'],
        latest_far_session=second['_ordered'][-1]['session_end_date'],bar_finality_independently_verified=False)
    flag='close_usable_as_reported' if field=='close' else 'settlement_usable_as_reported'
    if not a[flag] or not b[flag]:out['reason']='common_session_price_unavailable';return out
    delta=Fraction(Decimal(b['values'][field]['decimal']))-Fraction(Decimal(a['values'][field]['decimal']))
    out.update(available=True,reason=None,far_minus_near_decimal=shown(delta),far_minus_near_exact=fraction(delta))
    return out


def compile_output(sources,generated_at,read,emit):
    clock(generated_at)
    if not isinstance(sources,dict):raise ValueError('Complete futures source inventory required')
    required={product+':'+kind for product in capture.PRODUCTS for kind in ('products','contracts','schedules')}
    if not required<=set(sources):raise ValueError('Every product catalog and schedule capture required')
    parsed={};datasets={};windows=set()
    for name,source in sources.items():
        scope=source['scope'];expected=scope['product']+':'+scope['kind']+(':'+scope['ticker'] if scope['ticker'] else '')
        if name!=expected:raise ValueError('Futures dataset identity differs')
        windows.add((scope['from'],scope['to']));rows,pointers,complete=source_rows(source,generated_at,read);parsed[name]=(rows,pointers,complete)
        records=[{'ordinal':i,'values':typed(row),'source':pointers[i]} for i,row in enumerate(rows)];blocks=[]
        for offset in range(0,len(records),BLOCK):
            body={'contract':'futures-original-records.v1','dataset':name,'offset':offset,'rows':records[offset:offset+BLOCK]}
            expected_ref=ref(encoded(body),'records')
            if emit('records',body)!=expected_ref:raise ValueError('Futures record block differs')
            blocks.append(expected_ref)
        datasets[name]={'scope':scope,'records':blocks,'returned_rows':len(rows),'pagination_complete':complete,
            'source_capture_completed_at':source['completed_at'],'stop':source['stop']}
    if len(windows)!=1:raise ValueError('Consistent futures definition and request window required')
    start,asof=next(iter(windows));products={};expected_names=set(required)
    for product in capture.PRODUCTS:
        definitions,points,complete=parsed[product+':products'];meta=specification(product,definitions,points,complete,asof)
        contracts,pointers,catalog_complete=parsed[product+':contracts']
        selection=capture.select_contracts(contracts,product,asof,catalog_complete);series=[]
        for chosen in selection['selected']:
            ticker=chosen['ticker'];name=product+':bars:'+ticker;expected_names.add(name)
            if name not in parsed:raise ValueError('Selected futures contract capture missing')
            original,bar_pointers,done=parsed[name];definition=contracts[chosen['source_row_index']]
            bars=[bar(r,i,bar_pointers[i],ticker,definition) for i,r in enumerate(original)]
            dates=[r['session_end_date'] for r in bars];times=[r['window_start_ns'] for r in bars]
            identity=bool(bars) and all(r['identity_and_clock_qualified'] for r in bars)
            chronology=identity and len(set(dates))==len(dates) and len(set(times))==len(times)
            ordered=sorted(bars,key=lambda r:r['session_end_date']) if chronology else []
            if chronology and any(int(b['window_start_ns'])<=int(a['window_start_ns']) for a,b in zip(ordered,ordered[1:])):
                chronology=False;ordered=[]
            series.append({'ticker':ticker,'definition':typed(contracts[chosen['source_row_index']]),
                'definition_source':pointers[chosen['source_row_index']],'dataset':name,'chronology_unambiguous':chronology,
                'coverage':{'returned_rows':len(bars),'pagination_complete':done,'invalid_identity_or_clock_rows':sum(not r['identity_and_clock_qualified'] for r in bars),
                    'missing_settlement_rows':sum(r['values']['settlement_price']['state']=='missing' for r in bars),
                    'full_calendar_coverage_verified':False,'bar_finality_independently_verified':False},
                'comparisons':{field:{str(n):comparison(ordered,n,field,done and chronology) for n in (1,5,20)} for field in ('close','settlement_price')},
                'latest_reported_row':bars[ordered[-1]['ordinal']] if ordered else None,
                '_ordered':ordered,**PERMISSIONS})
        curves=[matched_curve(a,b,field,meta['available'] and meta['quantity_conversion_qualified'] and a['coverage']['pagination_complete'] and b['coverage']['pagination_complete'])
            for a,b in zip(series,series[1:]) for field in ('close','settlement_price')]
        for item in series:item.pop('_ordered')
        products[product]={'product_code':product,'venue':capture.PRODUCTS[product],'specification':meta,'contract_selection':selection,
            'contracts':series,'matched_curves':curves,'schedule_dataset':product+':schedules',
            'scheduled_session_end_independently_matched':False,**PERMISSIONS}
    if set(sources)!=expected_names:raise ValueError('Unexpected futures source dataset')
    completed=max(clock(s['completed_at']) for s in sources.values()).isoformat()
    return {'contract':CONTRACT,'generated_at':generated_at,'source_capture_completed_at':completed,
        'definition_date':asof,'request_window':{'from':start,'to':asof},'datasets':datasets,'products':products,
        'quality':{'status':'descriptive','products':len(products),'selected_contracts':sum(len(p['contracts']) for p in products.values()),
            'complete_datasets':sum(d['pagination_complete'] for d in datasets.values())},
        'field_definitions':{'close':'Last reported trade price in this window, not settlement.',
            'settlement_price':'Separately reported session settlement, possibly missing.',
            'volume':'Reported number of contracts traded.',
            'dollar_volume':'Sum of quoted price times trade size; contract multiplier not applied; not USD notional.',
            'window_start':'Unix nanoseconds at aggregation window start, not trade or session closing time.'},
        'limits':['Nearest expiry is not necessarily the most liquid or safe delivery contract.',
            'Matching session labels does not establish synchronized intraday prices or final bars.',
            'Row offsets do not establish trading-day coverage. No continuous contract or roll return is constructed.',
            'Schedules are retained evidence; their product identity and session finality have not yet been independently reconciled.',
            'Provider quantity conversions are not broker margin, costs, delivery eligibility or an execution instruction.'],
        'call':None,'score':None,'portfolio_action':'WAIT','independent_investment_votes':0,**PERMISSIONS}
