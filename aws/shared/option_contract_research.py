"""Exact, field-level option snapshot research; no assumed dealer positions.

Every returned row survives. Daily-bar update clocks are not OI/Greek clocks,
and volumes from different last-reported bars never become one session total.
"""
from collections import Counter
from datetime import date, datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation, localcontext
from zoneinfo import ZoneInfo
import re
import option_snapshot_capture as capture

CONTRACT='option-contract-research.v1'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
EASTERN=ZoneInfo('America/New_York')


def clock(value):
    if not isinstance(value,str):raise ValueError('Aware acquisition clock required')
    d=datetime.fromisoformat(value.replace('Z','+00:00'))
    if d.tzinfo is None:raise ValueError('Aware acquisition clock required')
    return d.astimezone(timezone.utc)


def decimal(value):
    if isinstance(value,bool) or not isinstance(value,(int,Decimal)):
        raise ValueError('Exact original JSON number required')
    d=Decimal(value)
    if not d.is_finite():raise ValueError('Finite original number required')
    if d==0:return Decimal(0)
    _,digits,exp=d.as_tuple()
    if len(digits)>128 or exp < -128 or d.adjusted()>18:
        raise ValueError('Source precision or magnitude outside reviewed bound')
    return d


def text(value):
    d=decimal(value)
    return '0' if d==0 else format(d,'f')


def metric(parent,key,path,unit,lower=None,upper=None,integer=False,positive=False):
    out={'value':None,'state':'missing','unit':unit,'source_field':path,'observed_at':None}
    if not isinstance(parent,dict):return {**out,'state':'invalid_parent'}
    if key not in parent:return out
    v=parent[key]
    if v is None:return {**out,'state':'null'}
    try:d=decimal(v)
    except (ValueError,InvalidOperation):return {**out,'state':'invalid_number'}
    out['reported_value']=text(d)
    if (lower is not None and d<lower) or (upper is not None and d>upper) or (positive and d<=0):
        return {**out,'state':'outside_domain'}
    if integer and d!=d.to_integral_value():return {**out,'state':'non_integer_quantity'}
    return {**out,'value':text(d),'state':'reported_zero' if d==0 else 'reported'}


def reported_timestamp(parent,key,path,received):
    out={'value':None,'raw_nanoseconds':None,'source_field':path,'state':'missing',
        'clock_role':'provider_field_update','independent_metric_observation_verified':False}
    if not isinstance(parent,dict):return {**out,'state':'invalid_parent'}
    if key not in parent:return out
    v=parent[key]
    if v is None:return {**out,'state':'null'}
    if type(v) is not int or not 946684800000000000<=v<=4102444800000000000:
        return {**out,'state':'invalid_nanoseconds'}
    out['raw_nanoseconds']=str(v)
    d=datetime.fromtimestamp(v//1000000000,timezone.utc)+timedelta(microseconds=(v%1000000000)//1000)
    acquired=clock(received)-datetime(1970,1,1,tzinfo=timezone.utc)
    acquired_ns=(acquired.days*86400+acquired.seconds)*1000000000+acquired.microseconds*1000
    if v>acquired_ns:return {**out,'state':'future_timestamp'}
    return {**out,'value':d.isoformat(),'date_new_york':d.astimezone(EASTERN).date().isoformat(),'state':'reported'}


def original_ref(ref):
    if (not isinstance(ref,dict) or not re.fullmatch(r'[a-f0-9]{64}',ref.get('sha256',''))
            or type(ref.get('bytes')) is not int or ref['bytes']<=0
            or ref.get('key')!='audit-private/20260909-originals/options-research/'+ref['sha256']+'.bin'):
        raise ValueError('Exact protected original identity required')
    return {k:ref[k] for k in ('key','sha256','bytes')}


def normalize(row,symbol,page,index):
    symbol=capture.ticker(symbol);received=clock(page['acquired_at'])
    if type(page['page']) is not int or page['page']<1 or type(index) is not int or index<0:
        raise ValueError('Original page and row positions required')
    out={'underlying':symbol,'contract_id':None,'identity_eligible':False,'identity_reasons':[],
        'source_received_at':received.isoformat(),'evidence':{'original':original_ref(page['original']),
            'page':page['page'],'row_index':index,'row_pointer':'/results/'+str(index)},**PERMISSIONS}
    if not isinstance(row,dict):return {**out,'identity_reasons':['non_object_contract'],'metrics':{},'clocks':{}}
    details=row.get('details');d=details if isinstance(details,dict) else {}
    under=row.get('underlying_asset');u=under if isinstance(under,dict) else {}
    identity=d.get('ticker');kind=d.get('contract_type');expiry=d.get('expiration_date')
    out.update(contract_id=identity if isinstance(identity,str) and len(identity)<=64 else None,
        contract_type=kind if kind in ('call','put') else None,
        expiration_date=expiry if isinstance(expiry,str) and len(expiry)<=10 else None,
        exercise_style=d.get('exercise_style') if d.get('exercise_style') in ('american','european','bermudan') else None)
    m=re.fullmatch(r'O:([A-Z0-9.]+)(\d{6})([CP])(\d{8})',identity if isinstance(identity,str) else '')
    reasons=out['identity_reasons']
    if not m or m[1]!=symbol:reasons.append('contract_root_differs')
    if kind not in ('call','put') or (m and kind!=('call' if m[3]=='C' else 'put')):reasons.append('contract_type_differs')
    try:
        if not isinstance(expiry,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',expiry):raise ValueError('Canonical expiration date required')
        exp=date.fromisoformat(expiry)
        if not m or exp!=date(2000+int(m[2][:2]),int(m[2][2:4]),int(m[2][4:])):
            reasons.append('contract_expiry_differs')
        if exp<received.astimezone(EASTERN).date():reasons.append('expired_before_capture_date')
    except (ValueError,TypeError):reasons.append('invalid_contract_expiry')
    if u.get('ticker')!=symbol:reasons.append('underlying_identity_differs')
    metrics={
        'strike':metric(d,'strike_price','/details/strike_price','USD_per_share',positive=True),
        'shares_per_contract':metric(d,'shares_per_contract','/details/shares_per_contract','shares_per_contract',integer=True,positive=True),
        'open_interest':metric(row,'open_interest','/open_interest','contracts',lower=0,integer=True),
        'vendor_iv':metric(row,'implied_volatility','/implied_volatility','decimal_annualized_volatility',positive=True),
        'underlying_price':metric(under,'price','/underlying_asset/price','USD_per_share',positive=True),
    }
    if not m or metrics['strike']['value'] is None or Decimal(metrics['strike']['value'])!=Decimal(int(m[4]))/1000:
        reasons.append('strike_differs_from_contract_id')
    multiplier=metrics['shares_per_contract']['value']
    if multiplier is None or Decimal(multiplier)!=100 or d.get('additional_underlyings'):
        reasons.append('nonstandard_or_unqualified_deliverable')
    if d.get('exercise_style')!='american':reasons.append('exercise_style_not_qualified_for_this_universe')
    day=row.get('day');g=row.get('greeks');quote=row.get('last_quote');trade=row.get('last_trade')
    for name in ('open','high','low','close','previous_close','vwap'):
        metrics['daily_'+name]=metric(day,name,'/day/'+name,'USD_per_share',lower=0)
    metrics['daily_volume']=metric(day,'volume','/day/volume','contracts',lower=0,integer=True)
    for name in ('bid','ask'):
        metrics[name]=metric(quote,name,'/last_quote/'+name,'USD_per_share',lower=0)
    for name in ('bid_size','ask_size'):
        metrics[name]=metric(quote,name,'/last_quote/'+name,'contracts',lower=0,integer=True)
    metrics['trade_price']=metric(trade,'price','/last_trade/price','USD_per_share',lower=0)
    for name,unit,lower,upper in (('gamma','per_USD_underlying_price',0,None),
            ('delta','option_price_change_per_underlying_price_change',-1 if kind=='put' else 0,0 if kind=='put' else 1),
            ('theta','vendor_theta_unit_unqualified',None,None),('vega','vendor_vega_unit_unqualified',None,None)):
        metrics['vendor_'+name]=metric(g,name,'/greeks/'+name,unit,lower=lower,upper=upper)
    clocks={
        'daily_bar_updated':reported_timestamp(day,'last_updated','/day/last_updated',page['acquired_at']),
        'underlying_updated':reported_timestamp(under,'last_updated','/underlying_asset/last_updated',page['acquired_at']),
        'quote_updated':reported_timestamp(quote,'last_updated','/last_quote/last_updated',page['acquired_at']),
        'trade_sip':reported_timestamp(trade,'sip_timestamp','/last_trade/sip_timestamp',page['acquired_at'])}
    if metrics['bid']['value'] is not None and metrics['ask']['value'] is not None and Decimal(metrics['ask']['value'])<Decimal(metrics['bid']['value']):
        for name in ('bid','ask'):metrics[name].update(value=None,state='crossed_quote')
    out.update(identity_eligible=not reasons,metrics=metrics,clocks=clocks,
        open_interest_date=None,greek_observation_date=None,iv_observation_date=None,
        dealer_inventory_observed=False,executable_quote=False,
        meaning='A returned vendor snapshot row. Independent OI/IV/Greek clocks and dealer ownership are not supplied.')
    return out


def sum_values(rows,field,kind=None):
    selected=[r for r in rows if kind is None or r.get('contract_type')==kind]
    values=[r['metrics'][field]['value'] for r in selected if r.get('metrics',{}).get(field,{}).get('value') is not None]
    with localcontext() as ctx:
        ctx.prec=180
        total=sum((Decimal(v) for v in values),Decimal(0))
    return {'value':text(total) if values else None,'included_rows':len(values),'population_rows':len(selected),
        'missing_or_invalid_rows':len(selected)-len(values),'complete_field_coverage':bool(selected) and len(selected)==len(values),
        'unit':'contracts'}


def ratio(numerator,denominator):
    out={'value':None,'numerator':numerator['value'],'denominator':denominator['value'],
        'unit':'ratio','rounding_decimal_places':12,'status':'incomplete_field_coverage'}
    if not numerator['complete_field_coverage'] or not denominator['complete_field_coverage']:return out
    a,b=Decimal(numerator['value']),Decimal(denominator['value'])
    if b==0:return {**out,'status':'zero_denominator'}
    with localcontext() as ctx:
        ctx.prec=180;v=(a/b).quantize(Decimal('0.000000000001'))
    return {**out,'value':text(v),'status':'descriptive_reported_ratio'}


def compile_rows(symbol,pages,pagination_complete):
    """Pages already passed descriptor/body checks; independently bind row identities."""
    capture.ticker(symbol)
    if type(pagination_complete) is not bool:raise ValueError('Explicit pagination status required')
    rows=[];last_received=None;expected=capture.next_url(capture.initial_url(symbol),symbol);seen=set()
    for i,page in enumerate(pages):
        if page['page']!=i+1 or type(page['page']) is not int:raise ValueError('Contiguous source pages required')
        if (page.get('request_url')!=expected or page.get('request_sha256')!=capture.sha(expected.encode())
                or expected in seen):raise ValueError('Original request identity differs')
        seen.add(expected)
        received=clock(page['acquired_at'])
        if last_received and received<last_received:raise ValueError('Source capture clock moved backwards')
        last_received=received
        raw=page['raw'];ref=original_ref(page['original'])
        if not isinstance(raw,bytes) or len(raw)!=ref['bytes'] or capture.sha(raw)!=ref['sha256']:raise ValueError('Original body identity differs')
        doc=capture.decode(raw)
        if not isinstance(doc,dict) or doc.get('status') not in ('OK','DELAYED') or not isinstance(doc.get('results'),list) or len(doc['results'])>250:
            raise ValueError('Reviewed successful provider envelope required')
        if i<len(pages)-1 and not doc.get('next_url'):raise ValueError('Page after terminal response')
        if i==len(pages)-1 and pagination_complete!= (not bool(doc.get('next_url'))):raise ValueError('Pagination completion differs from original')
        rows.extend(normalize(row,symbol,page,index) for index,row in enumerate(doc['results']))
        if doc.get('next_url'):expected=capture.next_url(doc['next_url'],symbol)
    if not pages:raise ValueError('Source pages required')
    duplicates={k for k,n in Counter(r['contract_id'] for r in rows if r['contract_id']).items() if n>1}
    for row in rows:
        if row['contract_id'] in duplicates:
            row['identity_eligible']=False;row['identity_reasons'].append('duplicate_contract_identity')
    eligible=[r for r in rows if r['identity_eligible']]
    groups={}
    for row in eligible:
        date_key=row['clocks']['daily_bar_updated'].get('date_new_york')
        if date_key:groups.setdefault(date_key,[]).append(row)
    by_date=[]
    for day,population in sorted(groups.items()):
        calls=sum_values(population,'daily_volume','call');puts=sum_values(population,'daily_volume','put')
        by_date.append({'daily_bar_update_date_new_york':day,'calls':calls,'puts':puts,'call_put_ratio':ratio(calls,puts),
            'meaning':'Returned most-recent daily bars grouped by their update date; not a complete market session volume.',
            'market_session_completeness_verified':False})
    call_oi=sum_values(eligible,'open_interest','call');put_oi=sum_values(eligible,'open_interest','put')
    field_quality={}
    for row in rows:
        for name,cell in row['metrics'].items():field_quality.setdefault(name,Counter())[cell['state']]+=1
    return {'contract':CONTRACT,'underlying':symbol,'rows':rows,
        'coverage':{'returned_rows':len(rows),'eligible_identity_rows':len(eligible),'rejected_identity_rows':len(rows)-len(eligible),
            'duplicate_identities':len(duplicates),'pages':len(pages),'pagination_complete':pagination_complete,
            'capture_is_atomic':False,'exchange_chain_completeness_verified':False,
            'first_received_at':pages[0]['acquired_at'],'last_received_at':pages[-1]['acquired_at'],
            'identity_reasons':dict(Counter(reason for r in rows for reason in r['identity_reasons'])),
            'field_quality':{k:dict(v) for k,v in field_quality.items()},
            'eligible_rows_without_daily_bar_update_clock':sum(not r['clocks']['daily_bar_updated'].get('value') for r in eligible)},
        'daily_bar_update_groups':by_date,
        'aggregation_scope':'Identity-valid returned rows only; incomplete source or field coverage stays explicit.',
        'definitions':{
            'snapshot':'https://massive.com/docs/rest/options/snapshots/option-chain-snapshot',
            'gamma':'https://www.optionseducation.org/advancedconcepts/gamma',
            'open_interest':'https://www.optionseducation.org/referencelibrary/faq/general-information',
            'daily_bar':'The provider returns each contract\'s most recent daily bar; update dates can differ across contracts.',
            'gamma_domain':'Nonnegative long-option gamma is the reviewed convention. Negative reported values remain retained but unqualified; no dealer position sign is inferred.',
            'open_interest_scope':'An outstanding contract count does not identify the dealer side, trade initiation or current execution liquidity.'},
        'reported_open_interest':{'calls':call_oi,'puts':put_oi,'call_put_ratio':ratio(call_oi,put_oi),'observed_at':None,
            'meaning':'Reported outstanding contract counts in the captured rows. Provider defines end of last trading day, without a per-row date here.'},
        'total_session_volume':None,'dealer_position':None,'call':None,'score':None,'portfolio_action':'WAIT',
        'independent_investment_votes':0,**PERMISSIONS}
