"""Original-source risk observations; model hypotheses grant no portfolio authority."""
from copy import deepcopy
from decimal import Decimal
import re

from risk_gate_research_catalog import SERIES
from report_observations import measurement
from research_brief_model import clock,digest,encoded,row_status,SOURCE_CONTRACT
from lce_research_model import statistics
from ciss_readthrough import context as ciss_context

CONTRACT='risk-gate-research.v1'
REASON='No qualified Risk Gate forecast or portfolio-sizing model is available. WAIT means abstain; it is not an instruction to liquidate existing holdings.'


def matched_difference(rows,left,right,label,scale='100',unit='basis_points',limitation=''):
    """Subtract two original current rows only at an identical observation date."""
    a,b=rows[left],rows[right]
    result={'label':label,'series_ids':[left,right],'value':None,'value_decimal':None,
        'unit':unit,'observation_date':None,'status':'unavailable','formula':f'({left} - {right}) * {scale}',
        'inputs':[{k:r.get(k) for k in ('series_id','latest_date','latest_value_decimal','_units','source_row','evidence')} for r in (a,b)],
        'calls_eligible':False,'sizing_eligible':False,'independent_votes':0,'limitation':limitation}
    if not all(r.get('available') for r in (a,b)):
        result['reason']='Both observations must be current under their stated age ceilings';return result
    if a['latest_date']!=b['latest_date']:
        result['reason']='Latest observation dates differ; no forward fill or substitution';return result
    expected='Percent' if unit=='basis_points' else 'Index'
    if a['_units']!=expected or b['_units']!=expected:
        result['reason']='Official units do not match this reviewed formula';return result
    value=(Decimal(a['latest_value_decimal'])-Decimal(b['latest_value_decimal']))*Decimal(scale)
    result.update(value=float(value),value_decimal=format(value,'f'),observation_date=a['latest_date'],
        status='descriptive',reason='Exact arithmetic on matched observation dates; no calibrated threshold or executable quote')
    return result


def build(source,ciss,fleet,originals,generated_at):
    now=clock(generated_at)
    if not isinstance(source,dict) or source.get('contract')!=SOURCE_CONTRACT:raise ValueError('canonical macro packet required')
    ref=source.get('replay') or {}
    if not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key',''))):raise ValueError('macro replay required')
    if digest({k:v for k,v in source.items() if k!='replay'})!=ref.get('output_sha256'):raise ValueError('macro content binding differs')
    age=(now-clock(source['generated_at'])).total_seconds()
    if age<0:raise ValueError('future macro source')
    rows={};categories={};fresh=0
    for sid,(category,requested_name) in SERIES.items():
        row=deepcopy(source.get('measurements',{}).get(sid,{}))
        if row:
            original=originals.get(sid)
            if not isinstance(original,dict):raise ValueError('original input missing: '+sid)
            rebuilt=measurement(sid,original['definition'],original['observations'],original['evidence'],source['generated_at'],original['acquired_at'])
            if rebuilt!=row:raise ValueError('measurement differs from original source: '+sid)
        state=row_status(row,now,age) if row else 'unavailable';usable=state=='fresh';fresh+=usable
        item={'series_id':sid,'available':usable,'quality':{'status':state,'evaluated_at':generated_at},
            '_category':category,'_label':row.get('name') or sid,'requested_label':requested_name,'_units':row.get('unit'),
            'frequency':row.get('frequency'),'seasonal_adjustment':row.get('seasonal_adjustment'),
            'latest_date':row.get('date'),'latest_value':row.get('current') if usable else None,
            'latest_value_decimal':row.get('current_decimal') if usable else None,'last_observed_value':row.get('current_decimal'),
            'acquired_at':row.get('acquired_at'),'published_at':None,'source_definition':row.get('definition'),
            'provider_updated_at':row.get('provider_updated_at'),'evidence':row.get('evidence'),
            'source_row':row.get('current_row_index'),'source_replay':ref,
            'calendar_comparisons':deepcopy(row.get('changes',{})) if usable else {},
            'historical_calendar_comparisons':deepcopy(row.get('changes',{})) if not usable else {},
            'history':deepcopy(row.get('history',[])),'history_scope':row.get('embedded_history_scope'),
            'statistics':{},'call':None,'calls_eligible':False,'sizing_eligible':False,
            'error':None if row else source.get('errors',{}).get(sid,'source_not_collected')}
        if usable:
            for years in (1,5):item['statistics'][str(years)+'y']=statistics(originals[sid],row,years)
        rows[sid]=item;categories.setdefault(category,[]).append(sid)
    derived={
        'cp_a2p2_minus_aa_90d':matched_difference(rows,'RIFSPPNA2P2D90NB','DCPN3M','90-day A2/P2 minus AA nonfinancial commercial-paper rate',
            limitation='Estimated issuer-bucket rate difference from the Federal Reserve CP release. Not an individual traded credit spread; absent trades remain missing.'),
        'sofr_minus_iorb':matched_difference(rows,'SOFR','IORB','SOFR minus IORB',
            limitation='Secured market rate minus an administered reserve rate; their instruments, counterparties and access differ. No funding-crisis inference.'),
        'treasury_10y_minus_2y':matched_difference(rows,'DGS10','DGS2','10-year minus 2-year Treasury constant-maturity yield',
            limitation='Constant-maturity curve slope, not an executable bond or recession probability.'),
        'hy_minus_ig_oas':matched_difference(rows,'BAMLH0A0HYM2','BAMLC0A0CM','High-yield minus investment-grade index OAS',
            limitation='Different index universes, maturities and durations; not a matched issuer or duration trade.'),
        'cp_aa_90d_minus_fed_funds':matched_difference(rows,'DCPN3M','DFF','90-day AA CP minus overnight effective federal funds',
            limitation='Tenors and quotation conventions differ; this rate difference is not a matched credit spread.'),
        'vix_minus_vix3m':matched_difference(rows,'VIXCLS','VXVCLS','VIX minus three-month volatility index','1','index_points',
            'Different option horizons. An index-point difference is not a variance risk premium, probability or executable carry.')}
    context={key:{'status':'UNQUALIFIED_CONTEXT','generated_at':doc.get('generated_at') if isinstance(doc,dict) else None,
        'retained_input_field':'fleet.'+key,'content_sha256':digest(doc),'independent_votes':0,'score_adjustment':0,
        'calls_eligible':False,'sizing_eligible':False,'reason':'Retained existing public input; original source and model qualification pending'}
        for key,doc in sorted(fleet.items())}
    q=ciss_context(ciss,now)
    return {'engine':'justhodl-risk-gate','version':'3.0.0','schema_version':'risk-gate.v3','contract':CONTRACT,
        'generated_at':generated_at,'source_generated_at':source['generated_at'],'source_replay':ref,
        'series':rows,'by_category':categories,'derived':derived,'ciss_systemic':q,
        'quality':{'status':'fresh' if fresh==len(SERIES) else 'degraded' if fresh else 'unavailable',
            'expected_series':len(SERIES),'fresh_series':fresh,'unavailable_or_stale':len(SERIES)-fresh,
            'basis':'Original responses and official units; observation/acquisition age ceilings; exact release calendar unverified'},
        'posture':'UNAVAILABLE','composite':None,'sizing_multiplier':None,'call':None,
        'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'decision':{'verb':'WAIT','meaning':'abstain','reason':REASON},
        'portfolio_consequences':{'status':'UNAVAILABLE','allows_new_entries':False,'target_weights':None,
            'expected_return':None,'expected_loss':None,'forced_liquidation':False,'reason':REASON},
        'legs':{cat:{'score':None,'score_fused':None,'weight':None,'state':'RESEARCH_ONLY',
            'series_ids':ids,'fresh_series':sum(rows[s]['available'] for s in ids),'reason':REASON} for cat,ids in categories.items()},
        'fleet_context':{'inputs':context,'method':'Retained existing public inputs, zero independent votes and zero sizing authority'},
        'dependency_groups':{'federal_reserve_balance_sheet':['RRPONTSYD','WRESBAL','TOTRESNS','BOGMBBM','CASACBW027SBOG'],
            'commercial_paper_release':['RIFSPPNA2P2D90NB','DCPN3M'],'cboe_volatility':['VIXCLS','VXVCLS'],
            'ice_credit_indices':['BAMLH0A3HYC','BAMLC0A4CBBB','BAMLH0A0HYM2','BAMLHE00EHYIOAS','BAMLC0A0CM']},
        'dependency_note':'Source families indicate dependence, not independent votes. Reserve balances and commercial-bank cash assets overlap and are never summed here.',
        'overlays':[],'indicators':{'indicators':derived},'recent_timeline':[],
        'event_study':{'status':'NOT_QUALIFIED','flips':[],'gate_adds_value_if':None,
            'validation':{'point_in_time':False,'out_of_sample':False,'reason':'Prior current-vintage threshold reconstruction is retained in the legacy artifact; it does not qualify the current model.'}},
        'source_qualification':{'scope':'25 requested FRED identities and canonical ECB CISS','other_inputs':'Retained, unqualified context'},
        'consume_as':REASON,'validation_status':'DESCRIPTIVE_MEASUREMENTS_ONLY',
        'scope':'Original-source dated research and matched-date differences. No calibrated regime, crisis probability, predictive confidence or allocation authority.'}
