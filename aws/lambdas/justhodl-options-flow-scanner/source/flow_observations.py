"""Exact option contract/bar populations and separate FINRA trading flows."""
from datetime import date,datetime,timezone,timedelta
from decimal import Decimal,localcontext
import base64,hashlib,json,math,re
from urllib.parse import urlsplit,urlunsplit,parse_qsl,urlencode,quote
from zoneinfo import ZoneInfo

CONTRACT='options-flow-observations.v1'



def number(value):
    if type(value) not in (int,float):return None
    try:return value if math.isfinite(value) and abs(value)<=2**53-1 else None
    except OverflowError:return None


def strict(raw):
    def pairs(items):
        out={}
        for key,value in items:
            if key in out:raise ValueError('Duplicate JSON member')
            out[key]=value
        return out
    def real(value):
        out=float(value)
        if not math.isfinite(out) or out==0 and Decimal(value)!=0:raise ValueError('Unrepresentable JSON number')
        return out
    def constant(_):raise ValueError('Nonfinite JSON')
    return json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_float=real,parse_constant=constant)


def day(value):
    try:
        if not isinstance(value,str) or len(value)!=10:return None
        out=date.fromisoformat(value);return out if out.isoformat()==value else None
    except ValueError:return None


def clock(value):
    try:
        if not isinstance(value,str):return None
        out=datetime.fromisoformat(value.replace('Z','+00:00'));return out.astimezone(timezone.utc) if out.tzinfo else None
    except ValueError:return None


def symbol(value):
    if not isinstance(value,str):return None
    value=value.strip().upper();return value if re.fullmatch(r'[A-Z0-9][A-Z0-9.\-]{0,19}',value) else None


def calculate(values,operation):
    if any(number(v) is None for v in values):return None
    try:
        with localcontext() as ctx:
            ctx.prec=40;exact=operation(*[Decimal(str(v)) for v in values]);out=float(exact)
            return number(out) if not(out==0 and exact!=0) else None
    except (ArithmeticError,ValueError,OverflowError):return None


SOURCE_PREFIX='data/options-flow-scanner/sources/'


def source_ref(raw,kind):
    if not isinstance(raw,bytes) or kind not in ('json','txt'):raise ValueError('Whole declared source required')
    digest=hashlib.sha256(raw).hexdigest()
    return {'key':SOURCE_PREFIX+digest+'.'+kind,'bytes':len(raw),'sha256':digest,'format':kind}


def validate_ref(ref):
    if not isinstance(ref,dict) or set(ref)!= {'key','bytes','sha256','format'}:raise ValueError('Exact source identity required')
    if ref['format'] not in ('json','txt') or not isinstance(ref['sha256'],str) or not re.fullmatch(r'[a-f0-9]{64}',ref['sha256']):raise ValueError('Invalid source identity')
    if ref['key']!=SOURCE_PREFIX+ref['sha256']+'.'+ref['format'] or type(ref['bytes']) is not int or not 0<=ref['bytes']<=8*1024*1024:raise ValueError('Reviewed bounded source required')
    return ref


def content(acquisition,sources):
    if acquisition.get('status') not in ('received','invalid_original'):return None
    ref=validate_ref(acquisition.get('original_ref'));raw=sources.get(ref['key'])
    if not isinstance(raw,bytes) or source_ref(raw,ref['format'])!=ref:raise ValueError('Whole original response differs')
    return raw


def original(acquisition,sources):
    raw=content(acquisition,sources)
    if raw is None:return None
    if acquisition['original_ref']['format']!='json':raise ValueError('JSON source required')
    try:parsed=strict(raw)
    except (ValueError,UnicodeError,RecursionError):
        if acquisition['status']=='invalid_original':return None
        raise
    if acquisition['status']=='invalid_original':raise ValueError('Invalid-source classification differs')
    return parsed


def envelope(raw,endpoint,received_at,kind='json'):
    if clock(received_at) is None:raise ValueError('Explicit receipt clock required')
    out={'endpoint':endpoint,'status':'received','received_at':received_at,'original_ref':source_ref(raw,kind)}
    if kind=='json':
        try:strict(raw)
        except (ValueError,UnicodeError,RecursionError):out['status']='invalid_original'
    return out


def finra_files(acquisitions,sources,selected,checked_as_of):
    import offexchange_measurements as finra
    today=day(checked_as_of)
    if today is None:raise ValueError('Explicit check date required')
    names={r['ticker'] for r in selected if r['ticker']};files=[];by_symbol={name:[] for name in names};seen=set()
    for index,a in enumerate(acquisitions):
        stamp=day(a.get('observation_date'))
        if not stamp or stamp>=today or stamp.weekday()>=5 or stamp in seen:raise ValueError('Unique prior weekday source request required')
        seen.add(stamp)
        if a.get('endpoint')!='https://cdn.finra.org/equity/regsho/daily/CNMSshvol'+stamp.strftime('%Y%m%d')+'.txt':raise ValueError('Exact declared FINRA path required')
        entry={'acquisition_index':index,'observation_date':stamp.isoformat(),'status':a.get('status'),'reported_rows':None,'matched_rows':None,'parse_error':None}
        raw=content(a,sources)
        if raw is not None and a.get('http_status')==200:
            if a['original_ref']['format']!='txt':raise ValueError('Original CNMS text required')
            try:rows=finra.cnms(raw,stamp.isoformat())
            except (ValueError,UnicodeError) as exc:entry.update(status='retained_file_rejected',parse_error=str(exc))
            else:
                entry.update(status='whole_cnms_file_parsed',reported_rows=len(rows),matched_rows=0)
                for row in rows:
                    if row['symbol'] in names:
                        by_symbol[row['symbol']].append({'acquisition_index':index,**row});entry['matched_rows']+=1
        files.append(entry)
    return {'files':files,'by_literal_symbol':by_symbol,'security_identity_continuity_verified':False,
        'calendar_and_outage_completeness_verified':False,'source_family':'FINRA CNMS daily trade-reporting flow',
        'short_volume_includes_exempt':True,'independent_investment_votes':0}


def universe(acquisition,limit,sources):
    if type(limit) is not int or not 1<=limit<=300:raise ValueError('Original bounded universe required')
    p=original(acquisition,sources);rows=p.get('stocks') if isinstance(p,dict) else None
    occurrences=[];selected=[]
    if isinstance(rows,list):
        for i,raw in enumerate(rows):
            row=raw if isinstance(raw,dict) else {};ticker=symbol(row.get('symbol'))
            chosen=bool(row.get('symbol')) and len(selected)<limit
            index=len(selected) if chosen else None
            if chosen:selected.append({'source_index':i,'ticker':ticker,'raw':raw})
            occurrences.append({'source_index':i,'raw':raw,'ticker':ticker,'request_index':index,
                'status':'invalid_symbol' if ticker is None else 'selected' if chosen else 'outside_original_request_cap'})
    return {'source_status':'received_stocks_array' if isinstance(rows,list) else 'unavailable',
            'occurrences':occurrences,'selected':selected,'request_limit':limit,'historical_membership_verified':False}


def spot(acquisition,sources,ticker):
    rows=original(acquisition,sources)
    if acquisition.get('http_status')!=200 or not isinstance(rows,list) or len(rows)!=1 or not isinstance(rows[0],dict):return None
    row=rows[0];price=number(row.get('price'))
    return price if row.get('symbol')==ticker and price is not None and price>0 else None


def initial_url(ticker,price,checked):
    if symbol(ticker)!=ticker or number(price) is None or price<=0 or day(checked) is None:raise ValueError('Exact request inputs required')
    d=day(checked)
    return 'https://api.polygon.io/v3/reference/options/contracts?'+urlencode({
        'underlying_ticker':ticker,'expiration_date.gte':(d+timedelta(days=14)).isoformat(),
        'expiration_date.lte':(d+timedelta(days=90)).isoformat(),'strike_price.gte':str(round(price*.9,2)),
        'strike_price.lte':str(round(price*1.1,2)),'limit':200,'sort':'ticker','order':'asc'})


def cursor_url(url):
    if not isinstance(url,str) or len(url)>16000:raise ValueError('Bounded provider cursor required')
    p=urlsplit(url)
    if p.scheme!='https' or p.netloc not in ('api.polygon.io','api.massive.com') or p.path!='/v3/reference/options/contracts' or p.fragment:raise ValueError('Unreviewed cursor address')
    items=parse_qsl(p.query,keep_blank_values=True,strict_parsing=True);seen=set();clean=[]
    for key,value in items:
        if key in seen or not value or len(value)>10000:raise ValueError('Ambiguous cursor')
        seen.add(key)
        if key=='apiKey':continue
        if key not in ('cursor','limit','order','sort'):raise ValueError('Changed provider scope')
        if key=='limit' and value!='200' or key=='order' and value!='asc' or key=='sort' and value!='ticker':raise ValueError('Changed cursor bound/order')
        clean.append((key,value))
    if 'cursor' not in seen:raise ValueError('Cursor required')
    return urlunsplit(('https',p.netloc,p.path,urlencode(sorted(clean)),''))


def provider_rows(a,sources):
    p=original(a,sources)
    if a.get('http_status')!=200 or not isinstance(p,dict) or p.get('status') not in ('OK','DELAYED') or not isinstance(p.get('results'),list):return None,p
    return p['results'],p


def exact_rows(a,sources):
    # The whole response already passed strict JSON validation. Keep original
    # decimal precision when validating quantities and OCC contract identity.
    return json.loads(content(a,sources),parse_float=Decimal)['results']


def exact_number(value):
    if type(value) not in (int,Decimal):return None
    d=Decimal(value)
    return d if d.is_finite() and abs(d)<=2**53-1 else None


def contracts(ticker,price,pages,sources,checked):
    records=[];seen_urls=set();expected=initial_url(ticker,price,checked);complete=False;stop='not_attempted';seen={}
    for page,a in enumerate(pages):
        if page>=10 or a.get('endpoint')!=expected or expected in seen_urls:raise ValueError('Contract page sequence differs')
        seen_urls.add(expected);rows,p=provider_rows(a,sources)
        if rows is None:
            stop='unavailable_or_invalid_provider_envelope'
            if page!=len(pages)-1:raise ValueError('Acquisition continued after unavailable page')
            break
        if len(rows)>200:raise ValueError('Provider page exceeds declared bound')
        exact=exact_rows(a,sources)
        for i,raw in enumerate(rows):
            row=raw if isinstance(raw,dict) else {};identity=row.get('ticker');kind=row.get('contract_type');expiry=day(row.get('expiration_date'));strike=number(row.get('strike_price'));issues=[]
            exact_row=exact[i] if isinstance(exact[i],dict) else {}
            exact_strike=exact_number(exact_row.get('strike_price'))
            match=re.fullmatch(r'O:([A-Z0-9.]+)(\d{6})([CP])(\d{8})',identity if isinstance(identity,str) else '')
            if not match or match[1]!=ticker or row.get('underlying_ticker')!=ticker:issues.append('underlying_or_contract_identity_differs')
            if kind not in ('call','put') or match and kind!=('call' if match[3]=='C' else 'put'):issues.append('contract_type_differs')
            try:encoded=date(2000+int(match[2][:2]),int(match[2][2:4]),int(match[2][4:])) if match else None
            except ValueError:encoded=None
            if expiry is None or encoded!=expiry or not day(checked)+timedelta(days=14)<=expiry<=day(checked)+timedelta(days=90):issues.append('expiration_identity_or_window_differs')
            if strike is None or not match or exact_strike!=Decimal(int(match[4]))/1000 or not round(price*.9,2)<=strike<=round(price*1.1,2):issues.append('strike_identity_or_window_differs')
            if exact_number(exact_row.get('shares_per_contract'))!=100 or row.get('additional_underlyings') or row.get('exercise_style')!='american':issues.append('unqualified_deliverable_or_exercise_style')
            rec={'page_index':page,'source_index':i,'contract_id':identity if isinstance(identity,str) else None,'contract_type':kind if kind in ('call','put') else None,
                'strike_price':strike,'expiration_date':expiry.isoformat() if expiry else None,'identity_issues':issues,'raw':raw}
            if isinstance(identity,str):seen.setdefault(identity,[]).append(len(records))
            records.append(rec)
        if not p.get('next_url'):
            complete=True;stop='complete_returned_pagination'
            if page!=len(pages)-1:raise ValueError('Unexpected extra contract page')
            break
        try:expected=cursor_url(p['next_url'])
        except ValueError:
            stop='invalid_pagination_address'
            if page!=len(pages)-1:raise ValueError('Continued invalid pagination')
            break
        stop='pagination_cycle' if expected in seen_urls else 'pagination_incomplete'
    for indices in seen.values():
        if len(indices)>1:
            for i in indices:records[i]['identity_issues'].append('duplicate_contract_occurrence')
    selected=[]
    for kind in ('call','put'):
        eligible=[(i,r) for i,r in enumerate(records) if r['contract_type']==kind and not r['identity_issues']]
        selected.extend(i for i,r in sorted(eligible,key=lambda ir:(abs(ir[1]['strike_price']-price),ir[0]))[:10])
    return {'records':records,'selected_record_indices':selected,'pagination_complete':complete,'stop_reason':stop,
        'selection':'At most ten closest valid unique contracts per type in the received current 14–90-day, ±10% strike population.',
        'exchange_chain_completeness_verified':False,'historical_membership_verified':False}


def bars_url(identity,checked,days_back):
    if not isinstance(identity,str) or not re.fullmatch(r'O:[A-Z0-9.]+\d{6}[CP]\d{8}',identity):raise ValueError('Contract ticker required')
    if type(days_back) is not int or not 1<=days_back<=20 or day(checked) is None:raise ValueError('Original bounded bar lookback required')
    return 'https://api.polygon.io/v2/aggs/ticker/'+quote(identity,safe=':')+'/range/1/day/'+(day(checked)-timedelta(days=days_back+5)).isoformat()+'/'+checked+'?sort=asc&limit=5000&adjusted=true'


def bar_records(contract,a,sources,checked,days_back):
    rows,p=provider_rows(a,sources);out=[];dates={};identity=contract['contract_id'];today=day(checked)
    if a.get('endpoint')!=bars_url(identity,checked,days_back):raise ValueError('Exact bar request required')
    envelope_ok=rows is not None and p.get('ticker')==identity and not p.get('next_url')
    if rows is not None:
        exact=exact_rows(a,sources)
        for i,raw in enumerate(rows):
            row=raw if isinstance(raw,dict) else {};issues=[];stamp=row.get('t');d=None
            if type(stamp) is int and 946684800000<=stamp<=4102444800000:
                d=datetime.fromtimestamp(stamp/1000,timezone.utc).astimezone(ZoneInfo('America/New_York')).date()
            if not envelope_ok:issues.append('incomplete_or_mismatched_bar_envelope')
            if d is None or not today-timedelta(days=days_back+5)<=d<today:issues.append('date_missing_future_or_uncompleted')
            v=exact_number(exact[i].get('v')) if isinstance(exact[i],dict) else None
            if v is None or v<0 or v!=int(v):issues.append('volume_missing_invalid_or_noninteger')
            if d:dates.setdefault(d.isoformat(),[]).append(i)
            out.append({'source_index':i,'contract_id':identity,'contract_type':contract['contract_type'],'observation_date':d.isoformat() if d else None,
                'reported_volume_contracts':str(int(v)) if v is not None and v>=0 and v==int(v) else None,'issues':issues,'raw':raw})
    for indices in dates.values():
        if len(indices)>1:
            for i in indices:out[i]['issues'].append('duplicate_contract_date')
    return {'records':out,'response_complete':bool(envelope_ok),'status':'received_bar_population' if envelope_ok else 'unavailable_incomplete_or_wrong_contract',
        'session_calendar_verified':False,'missing_session_means_zero':False,'adjustments_verified':False}


def dossier(member,acquisitions,sources,flow,checked,days_back):
    if set(acquisitions)!={'quote','contract_pages','bar_requests'}:raise ValueError('Declared acquisition groups required')
    ticker=member['ticker'];a=acquisitions['quote'];price=spot(a,sources,ticker)
    if price is None:
        if acquisitions['contract_pages'] or acquisitions['bar_requests']:raise ValueError('Contracts requested without matching positive spot')
        chain={'records':[],'selected_record_indices':[],'pagination_complete':False,'stop_reason':'quote_gate_unavailable'}
    else:chain=contracts(ticker,price,acquisitions['contract_pages'],sources,checked)
    selected=chain['selected_record_indices'];groups=acquisitions['bar_requests'];compiled=[]
    if len(groups)!=len(selected):raise ValueError('Every selected contract needs a request outcome')
    for index,a in zip(selected,groups):
        evidence=bar_records(chain['records'][index],a,sources,checked,days_back);compiled.append({'contract_record_index':index,**evidence})
    dates=sorted({r['observation_date'] for g in compiled for r in g['records'] if r['observation_date'] is not None});daily=[]
    for d in dates:
        sums={};counts={};issues=[]
        for kind in ('call','put'):
            pop=[g for g in compiled if chain['records'][g['contract_record_index']]['contract_type']==kind];values=[]
            for g in pop:
                matches=[r for r in g['records'] if r['observation_date']==d]
                if g['response_complete'] and len(matches)==1 and not matches[0]['issues']:values.append(int(matches[0]['reported_volume_contracts']))
            counts[kind]={'selected_contracts':len(pop),'valid_observed_contracts':len(values)}
            sums[kind]=str(sum(values)) if pop and len(pop)==len(values) and chain['pagination_complete'] else None
        ratio=None
        if sums['call'] is not None and sums['put'] is not None and int(sums['put'])>0:
            with localcontext() as ctx:ctx.prec=80;ratio=format((Decimal(sums['call'])/Decimal(sums['put'])).quantize(Decimal('0.000000000001')),'f')
        daily.append({'observation_date':d,'call_volume_contracts':sums['call'],'put_volume_contracts':sums['put'],'call_put_volume_ratio':ratio,'coverage':counts,
            'scope':'Fixed current selected-contract population; reported bars only, not all-market flow or a historical fixed universe.'})
    return {'ticker':ticker,'universe_member':member,'acquisitions':acquisitions,'spot_request_gate':price,'spot_currency_verified':False,
        'contract_population':chain,'bar_populations':compiled,'daily_observations':daily,'finra_observations':flow['by_literal_symbol'].get(ticker,[]),
        'short_interest_shares':None,'open_interest':None,'implied_volatility':None,'trade_direction':None,'score':None,'call':None,
        'calls_eligible':False,'forecast_qualified':False,'sizing_eligible':False,'execution_eligible':False}
