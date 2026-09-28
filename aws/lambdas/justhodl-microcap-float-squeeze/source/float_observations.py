"""Original FINRA flows and reported market observations, without squeeze inference."""
from datetime import date,datetime,timezone,timedelta
from decimal import Decimal,localcontext
import base64,hashlib,json,math,re

CONTRACT='microcap-flow-observations.v1'



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


SOURCE_PREFIX='data/microcap-float-squeeze/sources/'


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


def universe(acquisition,limit,sources):
    if type(limit) is not int or not 1<=limit<=600:raise ValueError('Original request bound required')
    p=original(acquisition,sources);rows=p.get('stocks') if isinstance(p,dict) else None
    occurrences=[];selected=[]
    if isinstance(rows,list):
        for i,raw in enumerate(rows):
            row=raw if isinstance(raw,dict) else {};ticker=symbol(row.get('symbol'))
            # Preserve occurrence order and duplicate memberships; do not silently
            # change the original universe into a deduplicated selection.
            in_bucket=row.get('cap_bucket') in ('nano','micro','small','mid')
            chosen=in_bucket and len(selected)<limit
            index=len(selected) if chosen else None
            if chosen:selected.append({'source_index':i,'ticker':ticker,'raw':raw})
            occurrences.append({'source_index':i,'raw':raw,'ticker':ticker,'request_index':index,
                'status':'invalid_symbol' if ticker is None else 'outside_original_cap_buckets' if not in_bucket else 'selected' if chosen else 'outside_original_request_cap'})
    return {'source_status':'received_stocks_array' if isinstance(rows,list) else 'missing_or_unavailable_stocks_array',
            'occurrences':occurrences,'selected':selected,'request_limit':limit,'historical_membership_verified':False}



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
        if raw is not None:
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


def price_evidence(requested,acquisition,sources,checked_as_of):
    rows=original(acquisition,sources);today=day(checked_as_of)
    if today is None:raise ValueError('Explicit check date required')
    if not isinstance(rows,list):return {'status':'price_array_unavailable','records':None,'averages':{},'returns_qualified':False}
    dated=[];invalid=[];seen={}
    for i,value in enumerate(rows):
        row=value if isinstance(value,dict) else {};stamp=day(row.get('date'));issues=[]
        if row.get('symbol')!=requested:issues.append('issuer_symbol_missing_or_mismatched')
        if stamp is None or stamp>today:issues.append('date_invalid_or_future')
        close=number(row.get('close'));volume=number(row.get('volume'))
        if close is None or close<=0:issues.append('close_invalid_or_nonpositive')
        if volume is None or volume<0:issues.append('volume_missing_or_invalid')
        if stamp:
            seen.setdefault(stamp,[]).append(i);dated.append((stamp,i,close,volume,issues))
        if issues:invalid.append({'source_index':i,'issues':issues})
    duplicates={stamp.isoformat():indices for stamp,indices in seen.items() if len(indices)>1}
    dated.sort(reverse=True,key=lambda r:(r[0],r[1]));averages={}
    for size in (30,60):
        window=dated[:size];reason=None
        if len(dated)!=len(rows) or len(window)!=size:reason='incomplete_or_undated_source_population'
        elif any(r[4] or r[0].isoformat() in duplicates for r in window):reason='invalid_or_duplicate_window'
        average=calculate([r[3] for r in window],lambda *v:sum(v)/Decimal(size)) if reason is None else None
        averages[str(size)]={'reported_volume_mean':average,'unit':'provider_reported_volume_units','source_indices':[r[1] for r in window],
            'first_date':window[-1][0].isoformat() if window else None,'last_date':window[0][0].isoformat() if window else None,
            'missing_reason':reason,'session_calendar_verified':False,'scope':str(size)+' latest distinct received dated rows; not a verified session calendar'}
    return {'status':'complete_eod_response_retained','records':len(rows),'invalid_records':invalid,'duplicate_dates':duplicates,
        'averages':averages,'returns_qualified':False,'price_currency_and_adjustments_verified':False,
        'daily_dollar_volume':None,'float_turnover_pct':None,'post_earnings_return_pct':None}


def dossier(member,acquisitions,sources,flow,checked_as_of):
    groups={a['endpoint']:a for a in acquisitions}
    if len(groups)!=len(acquisitions) or set(groups)-{'quote','historical-price-eod/full'}:raise ValueError('Unique declared FMP endpoints required')
    quotes=original(groups.get('quote',{}),sources);observations=flow['by_literal_symbol'].get(member['ticker'],[])
    files=[f for f in flow['files'] if f['status']=='whole_cnms_file_parsed']
    latest=max((f['observation_date'] for f in files),default=None)
    latest_rows=[r for r in observations if r['observation_date']==latest]
    return {'ticker':member['ticker'],'universe_member':member,'acquisitions':acquisitions,'quote_records':quotes if isinstance(quotes,list) else [],
        'quote_source_status':'received_array' if isinstance(quotes,list) else 'unavailable_or_not_requested',
        'price_evidence':price_evidence(member['ticker'],groups.get('historical-price-eod/full',{}),sources,checked_as_of),
        'finra_observations':observations,'latest_parsed_finra_date':latest,'latest_short_sale_volume_pct':latest_rows[0]['short_volume_pct'] if len(latest_rows)==1 else None,
        'latest_missing_reason':None if len(latest_rows)==1 and latest_rows[0]['short_volume_pct'] is not None else 'absent_latest_row_or_zero_denominator',
        'float_shares':None,'short_interest_shares':None,'days_to_cover':None,'borrow_rate':None,'score':None,'call':None,
        'security_identity_continuity_verified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}
