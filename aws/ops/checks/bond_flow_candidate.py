"""Dated ETF net-issuance cohort candidate; no network, storage or trade vote.

The source packet is derived research. A separate original-provider replay is
required before native admission; hashes alone do not confer that qualification.
"""
from datetime import datetime,date,timezone
from decimal import Decimal,localcontext
import hashlib,json,re

BUCKETS={
 'front_gov':['SHY','BIL','SGOV','VGSH','SHV'],
 'belly_gov':['IEI','IEF','VGIT','GOVT'],
 'long_gov':['TLT','VGLT','EDV','ZROZ','SPTL'],
 'tips':['TIP','SCHP','VTIP','STIP','LTPZ'],
 'ig_credit':['LQD','VCIT','VCSH','IGSB','USIG'],
 'hy_credit':['HYG','JNK','SJNK','USHY','SHYG'],
 'loans':['BKLN','SRLN'],'em_debt':['EMB','EMLC','VWOB'],
 'aggregate':['AGG','BND','BNDX'],'muni':['MUB','VTEB'],
 'equity_core':['SPY','IVV','VOO','QQQ','IWM','VTI','RSP']}
FLAGS=('calls_eligible','sizing_eligible','execution_eligible')
encoded=lambda value:json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def clock(value):
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('Aware source clock required')
    return stamp.astimezone(timezone.utc)


def decimal(value):
    if not isinstance(value,str) or len(value)>80 or not re.fullmatch(r'-?\d+(?:\.\d+)?',value):raise ValueError('Exact bounded finite source decimal required')
    result=Decimal(value)
    if not result.is_finite() or abs(result)>Decimal('1e20'):raise ValueError('Amount outside reviewed numeric bound')
    return result


def qualified_window(packet,ticker,n,at):
    """A dated descriptive estimate; deliberately no forecast permission."""
    row=packet['by_etf'].get(ticker)
    if not isinstance(row,dict):return None,'not_in_source_universe'
    if any(row.get(k) is not False for k in FLAGS):return None,'source_permission_contract_differs'
    if row.get('ticker')!=ticker or row.get('identity',{}).get('ticker')!=ticker or row.get('identity',{}).get('currency')!='USD':return None,'instrument_or_currency_unverified'
    if row.get('source_status')!='retained_native_history' or row.get('evidence_tier')!='issuer_nav_valued_share_change_estimate':return None,'native_issuer_history_unavailable'
    if row.get('quality',{}).get('status')!='recent_source_check':return None,'source_quality_unavailable'
    try:
        acquired=clock(row['source']['acquired_at']);latest=date.fromisoformat(row['observation_date'])
        if not 0<=(at-acquired).total_seconds()<=48*3600 or not 0<=(at.date()-latest).days<=4 or acquired>clock(packet['generated_at']) or latest>clock(packet['generated_at']).date():return None,'source_expired_or_future'
        w=row['flow_windows'][str(n)+'d'];start=date.fromisoformat(w['start_date']);end=date.fromisoformat(w['end_date'])
        if not start<end<=latest or w['end_date']!=packet['aggregation_period']['end_date']:return None,'window_not_aligned'
        if w['status']!='complete_descriptive_estimate' or type(w['observations_required']) is not int or type(w['observations_available']) is not int or w['observations_required']!=n or w['observations_available']!=n or w.get('excluded_dates')!=[]:return None,'incomplete_window'
        value=decimal(w['value_decimal']);sensitivity=decimal(w['precision_sensitivity_decimal'])
        if sensitivity<0:return None,'invalid_precision_sensitivity'
        ref=row['history'];evidence=row['source']['evidence'];digest=ref['sha256']
        if ref['key']!='data/etf-research/histories/'+digest+'.json' or not re.fullmatch('[a-f0-9]{64}',digest) or type(ref['bytes']) is not int or ref['bytes']<=0:return None,'history_reference_unverified'
        if evidence.get('captured') is not True or not re.fullmatch('[a-f0-9]{64}',str(evidence.get('sha256'))):return None,'original_reference_unavailable'
    except (ValueError,TypeError,KeyError,AttributeError):return None,'invalid_source_window'
    return {'ticker':ticker,'source_path':'/by_etf/'+ticker+'/flow_windows/'+str(n)+'d','unit':'USD',
        'start_date':start.isoformat(),'end_date':end.isoformat(),'observations':n,
        'value_decimal':format(value,'f'),'precision_sensitivity_decimal':format(sensitivity,'f'),
        'acquired_at':row['source']['acquired_at'],'latest_observation':latest.isoformat(),
        'history':ref,'original_evidence':evidence,'calendar_independently_complete':False},None


def compile_cohorts(raw,evaluated_at,buckets=None):
    if not isinstance(raw,bytes) or not 0<len(raw)<=16*1024*1024:raise ValueError('Whole bounded source packet required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result:raise ValueError('Duplicate source JSON key')
            result[key]=value
        return result
    def invalid(value):raise ValueError('Nonfinite source JSON value')
    packet=json.loads(raw,object_pairs_hook=pairs,parse_constant=invalid);encoded(packet)
    if packet.get('contract')!='etf-original-research.v1' or not isinstance(packet.get('by_etf'),dict) or any(packet.get(k) is not False for k in FLAGS):raise ValueError('Explicit native source research contract required')
    at=clock(evaluated_at);generated=clock(packet['generated_at'])
    if not 0<=(at-generated).total_seconds()<=48*3600:raise ValueError('Source publication is future or expired')
    scope=BUCKETS if buckets is None else buckets;seen=set()
    if not isinstance(scope,dict) or not scope:raise ValueError('Explicit fund scope required')
    for name,tickers in scope.items():
        if not isinstance(name,str) or not re.fullmatch('[a-z_]+',name) or not isinstance(tickers,list) or not tickers:raise ValueError('Explicit named cohort required')
        for ticker in tickers:
            if not isinstance(ticker,str) or not re.fullmatch('[A-Z]{1,6}',ticker) or ticker in seen:raise ValueError('Unique instrument membership required')
            seen.add(ticker)
    cohorts={}
    with localcontext() as context:
        context.prec=100
        for name,tickers in scope.items():
            windows={}
            for n in (1,5,20):
                members=[];excluded=[]
                for ticker in tickers:
                    item,reason=qualified_window(packet,ticker,n,at)
                    if item is None:excluded.append({'ticker':ticker,'reason':reason})
                    else:members.append(item)
                endpoints={(r['start_date'],r['end_date']) for r in members}
                aligned=len(endpoints)==1
                subtotal=sum((decimal(r['value_decimal']) for r in members),Decimal(0)) if members and aligned else None
                precision=sum((decimal(r['precision_sensitivity_decimal']) for r in members),Decimal(0)) if members and aligned else None
                complete=aligned and len(members)==len(tickers)
                windows[str(n)+'_observations']={'unit':'USD','observations':n,'members':members,'excluded':excluded,
                    'configured_tickers':tickers,'included_count':len(members),'configured_count':len(tickers),
                    'start_date':next(iter(endpoints))[0] if aligned else None,'end_date':next(iter(endpoints))[1] if aligned else None,
                    'coverage_subtotal_decimal':format(subtotal,'f') if subtotal is not None else None,
                    'complete_cohort_decimal':format(subtotal,'f') if complete else None,
                    'precision_sensitivity_decimal':format(precision,'f') if precision is not None else None,
                    'status':'complete_configured_cohort' if complete else 'partial_coverage_subtotal' if subtotal is not None else 'unaligned_windows' if members else 'unavailable',
                    'whole_market_total':False,'predictive_probability':False}
            cohorts[name]={'configured_tickers':tickers,'windows':windows,'flow_21d_usd':None,'avg_z90':None}
    return {'contract':'bond-flow-cohorts.v1','evaluated_at':evaluated_at,'cohorts':cohorts,
        'source':{'key':'data/etf-true-flows.json','bytes':len(raw),'sha256':hashlib.sha256(raw).hexdigest(),
            'generated_at':packet['generated_at'],'replay':packet.get('replay'),'original_replay_verified_here':False},
        'configured_funds':len(seen),'source_funds':len(packet['by_etf']),
        'issuer_coverage':'Only the explicit configured funds and their admitted dated issuer estimates; not the whole bond market.',
        'method':'Sum exact NAV-valued share-change estimates only when both window endpoints match. Counts are observations, not calendar days.',
        'duration_tilt':None,'equity_to_bond_transfer':None,'credit_appetite_signal':None,
        'warnings':['Twenty observations are never labeled twenty-one days.',
            'Net issuance differences do not identify transfers between asset classes.',
            'Precision sensitivity is not a confidence interval or guaranteed error bound.',
            'Shared issuers and overlapping underlying holdings do not create independent votes.'],
        'decision':{'verb':'WAIT','meaning':'abstain'},'forecast_qualified':False,
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'independent_votes':0}
