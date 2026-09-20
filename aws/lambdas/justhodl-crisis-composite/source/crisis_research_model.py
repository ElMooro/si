"""Dated crisis research from native observations; no unqualified DEFCON authority."""
from copy import deepcopy
from datetime import date
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import re

from report_observations import measurement,decimal as source_decimal
from research_brief_model import clock,digest,encoded,row_status,SOURCE_CONTRACT
import ciss_source_model

CONTRACT='crisis-research.v1'
PREFIX='data/crisis-research/'
CURRENT='data/crisis-composite.json'
PERMISSIONS={'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'forecast_eligible':False}
REASON='No independently qualified crisis forecast or portfolio policy is available. WAIT is research abstention; it does not instruct liquidation.'

# Membership describes transmission channels, never independent votes or weights.
GROUPS={
 'funding':('SOFR','IORB','DFF','DCPN3M','DCPF3M','DTB3'),
 'credit':('BAMLH0A0HYM2','BAMLC0A0CM','BAMLH0A1HYBB','BAMLH0A3HYC','BAMLEMCBPIOAS'),
 'dollar_and_rates':('DTWEXBGS','DGS2','DGS10'),
 'volatility':('VIXCLS','VXVCLS'),
 'reserve_buffers':('WRESBAL','RRPONTSYD','WALCL','WTREGEN'),
 'official_conditions':('NFCI','ANFCI','STLFSI4'),
}
SERIES=tuple(sid for group in GROUPS.values() for sid in group)
CONTEXT_KEYS=('eurodollar-plumbing','global-sovereign','crisis-plumbing','treasury-noise',
 'credit-stress','regime-composite','vol-surface','market-internals','global-liquidity',
 'leading-markets','canary-grid','global-stress','dollar-radar','ecb-hist/ciss_ea')
FAMILIES={
 'nyfed_money_markets':('SOFR','DFF'),
 'federal_reserve_administered_rate':('IORB',),
 'federal_reserve_commercial_paper':('DCPN3M','DCPF3M'),
 'treasury_rates':('DTB3','DGS2','DGS10'),
 'ice_credit_indices':GROUPS['credit'],
 'federal_reserve_broad_dollar':('DTWEXBGS',),
 'cboe_volatility_indices':GROUPS['volatility'],
 'federal_reserve_balance_sheet':('WRESBAL','WALCL','WTREGEN'),
 'nyfed_reverse_repo':('RRPONTSYD',),
 'chicago_fed_conditions':('NFCI','ANFCI'),
 'st_louis_fed_conditions':('STLFSI4',),
}


def native_rows(original,through):
    rows=[]
    for n,r in enumerate(original['observations']['observations']):
        if date.fromisoformat(r['date'])>date.fromisoformat(through):continue
        number=source_decimal(r.get('value'))
        rows.append({'date':r['date'],'value_decimal':str(number) if number is not None else None,'source_row_index':n})
    return sorted(rows,key=lambda row:row['date'])


def native(source,originals,generated_at):
    now=clock(generated_at);age=(now-clock(source['generated_at'])).total_seconds()
    if source.get('contract')!=SOURCE_CONTRACT or age<0:raise ValueError('canonical dated macro source required')
    ref=source.get('replay') or {}
    if not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',str(ref.get('manifest_key',''))):raise ValueError('macro replay missing')
    if digest({k:v for k,v in source.items() if k!='replay'})!=ref.get('output_sha256'):raise ValueError('macro packet differs')
    result={}
    for sid in SERIES:
        row=source.get('measurements',{}).get(sid);original=originals.get(sid)
        if row:
            if not original:raise ValueError('original source absent: '+sid)
            # Reproduce the reviewed upstream compiler under its original context.
            with localcontext() as ctx:
                ctx.prec=28;ctx.rounding=ROUND_HALF_EVEN
                rebuilt=measurement(sid,original['definition'],original['observations'],original['evidence'],source['generated_at'],original['acquired_at'])
            if rebuilt!=row:raise ValueError('original macro reconstruction differs: '+sid)
        row=row or {};status=row_status(row,now,age);fresh=status=='fresh'
        result[sid]={'series_id':sid,'label':row.get('name') or sid,'unit':row.get('unit'),
            'value':row.get('current') if fresh else None,'value_decimal':row.get('current_decimal') if fresh else None,
            'last_observed_value':row.get('current_decimal'),'observation_date':row.get('date'),
            'source_row_index':row.get('current_row_index'),'frequency':row.get('frequency'),
            'seasonal_adjustment':row.get('seasonal_adjustment'),'acquired_at':row.get('acquired_at'),
            'source_generated_at':source['generated_at'],'first_publication_at':None,
            'provider_updated_at':row.get('provider_updated_at'),'definition':deepcopy(row.get('definition')),
            'quality':{'status':status,'evaluated_at':generated_at,'basis':'Native observation and acquisition age ceilings; release-calendar qualification is not established'},
            'changes':deepcopy(row.get('changes',{})) if fresh else {},
            'history':native_rows(original,row['date']) if original and row.get('date') else [],'vintage':deepcopy(row.get('vintage')),
            'history_scope':'Original provider observations through the compiled source observation date, with missing rows and original row indices retained.',
            'evidence':deepcopy(row.get('evidence')),'source_replay':ref,
            'reason':None if row else source.get('errors',{}).get(sid,'not_collected'),**PERMISSIONS}
    return result


def difference(rows,left,right,label,limitation,scale='100',unit='basis_points'):
    a,b=rows[left],rows[right]
    out={'label':label,'series_ids':[left,right],'value':None,'value_decimal':None,'unit':unit,
        'observation_date':None,'source_latest_dates':{left:a['observation_date'],right:b['observation_date']},
        'formula':f'({left} - {right}) * {scale}', 'limitation':limitation,'source_rows':{},
        'reason':'Both source observations must pass their age ceilings',**PERMISSIONS}
    if any(r['quality']['status']!='fresh' for r in (a,b)):return out
    expected='Index' if unit=='index_points' else 'Percent'
    if any(r['unit']!=expected for r in (a,b)):
        out['reason']='Official native units differ from the reviewed comparison';return out
    ah={r['date']:r for r in a['history']};bh={r['date']:r for r in b['history']}
    common=set(ah)&set(bh)
    if not common:out['reason']='No identical observation date';return out
    day=max(common);x,y=ah[day],bh[day]
    out.update(observation_date=day,source_rows={left:x['source_row_index'],right:y['source_row_index']})
    if any((date.fromisoformat(r['observation_date'])-date.fromisoformat(day)).days>7 for r in (a,b)):
        out['reason']='Latest shared date is more than seven days behind a source';return out
    if x['value_decimal'] is None or y['value_decimal'] is None:
        out['reason']='Missing value on latest shared date; no older value substituted';return out
    with localcontext() as ctx:
        ctx.prec=50;ctx.rounding=ROUND_HALF_EVEN
        value=(Decimal(x['value_decimal'])-Decimal(y['value_decimal']))*Decimal(scale)
    out.update(value=float(value),value_decimal=format(value,'f'),reason=None)
    return out


def ciss_native(original,generated_at):
    """Reconstruct the official headline value from its native CSV, not derived ranks."""
    packet,entry=original['packet'],original['entry'];key=ciss_source_model.HEAD
    if packet.get('contract')!=ciss_source_model.CONTRACT:raise ValueError('canonical ECB packet required')
    now=clock(generated_at);source_at=clock(packet['generated_at']);acquired=clock(entry['acquired_at'])
    if not clock(entry['evidence']['first_received_at'])<=acquired<=source_at<=now:raise ValueError('ECB source clock differs')
    parsed=ciss_source_model.csv_series(original['raw'],key)[key];meta=parsed['metadata']
    if meta['FREQ']!='D' or meta['UNIT']!='PURE_NUMB' or meta['UNIT_MULT']!='0':raise ValueError('native ECB unit/frequency differs')
    rows=[r for r in parsed['rows'] if r['period_end']<=source_at.date().isoformat()]
    if not rows:raise ValueError('ECB has no completed observation')
    current=rows[-1]
    existing=[r for r in packet['series'] if r.get('key')==key]
    if len(existing)!=1:raise ValueError('exact ECB headline required once')
    row=existing[0]
    if (row['source_metadata']!=meta or row['source_row']!=current['source_row'] or row['latest_date']!=current['period']
            or row['evidence']!=entry['evidence'] or row['acquired_at']!=entry['acquired_at']):raise ValueError('ECB original row binding differs')
    valid=ciss_source_model.usable(current)
    if valid and not Decimal('0')<=Decimal(current['decimal'])<=Decimal('1'):raise ValueError('ECB headline outside native index range')
    if row['quality']['status']=='fresh' and (row['latest_decimal']!=current['decimal'] or row['latest']!=current['value']):
        raise ValueError('ECB published value differs from original')
    age=(now.date()-date.fromisoformat(current['period_end'])).days
    fresh=valid and 0<=age<=14 and (now-acquired).total_seconds()<=72*3600 and (now-source_at).total_seconds()<=72*3600
    status='fresh' if fresh else 'stale' if valid else 'unavailable'
    return {'series_id':key,'label':meta.get('TITLE_COMPL') or meta.get('TITLE') or key,'unit':'dimensionless_index',
        'value':current['value'] if fresh else None,'value_decimal':current['decimal'] if fresh else None,
        'last_observed_value':current['decimal'] if valid else None,'observation_date':current['period'],
        'period_end':current['period_end'],'frequency':meta['FREQ'],'acquired_at':entry['acquired_at'],
        'source_generated_at':packet['generated_at'],'first_publication_at':None,'source_row_index':current['source_row'],
        'definition':meta,'evidence':entry['evidence'],'source_replay':packet['replay'],
        'quality':{'status':status,'observation_age_days':age,'maximum_observation_age_days':14,
            'maximum_acquisition_age_seconds':72*3600,'evaluated_at':generated_at},
        'history':[{'date':r['period'],'period_end':r['period_end'],'value_decimal':r['decimal'] if ciss_source_model.usable(r) else None,
            'observation_status':r['status'],'source_row_index':r['source_row']} for r in rows],
        'scope':'Official euro-area CISS headline; native value only. No mapped crisis score, calibrated probability or global portfolio instruction.',**PERMISSIONS}


def build(source,originals,context,generated_at,ciss=None):
    rows=native(source,originals,generated_at)
    comparisons={name:difference(rows,*args) for name,args in {
        'sofr_iorb':('SOFR','IORB','SOFR minus IORB','Secured market and administered reserve rates have different access and counterparties.'),
        'sofr_fed_funds':('SOFR','DFF','SOFR minus effective federal funds','Different secured/unsecured markets; this spread alone does not diagnose a funding crisis.'),
        'cp_bill':('DCPN3M','DTB3','90-day AA nonfinancial CP minus three-month bill','Bank-discount bill and CP quotation/tenor conventions differ; not an executable matched spread.'),
        'hy_ig':('BAMLH0A0HYM2','BAMLC0A0CM','High-yield minus investment-grade OAS','Different issuers, maturities and durations; not a matched credit trade.'),
        'ccc_bb':('BAMLH0A3HYC','BAMLH0A1HYBB','CCC-and-lower minus BB OAS','Nested/related credit universes do not create independent observations of systemic stress.'),
        'treasury_curve':('DGS10','DGS2','10-year minus 2-year Treasury yield','Constant-maturity comparison, not an executable bond trade or recession probability.'),
        'vix_vix3m':('VIXCLS','VXVCLS','VIX minus three-month volatility index','Implied-volatility index horizons differ; not a futures curve or carry return.','1','index_points'),
    }.items()}
    fresh=sum(r['quality']['status']=='fresh' for r in rows.values())
    context_view={key:{**deepcopy(context.get(key,{})),'independent_votes':0,**PERMISSIONS} for key in CONTEXT_KEYS}
    return {'engine':'justhodl-crisis-composite','version':'2.0.0','contract':CONTRACT,'generated_at':generated_at,'source_generated_at':source['generated_at'],
        'measurements':rows,'comparisons':comparisons,'ciss':deepcopy(ciss),
        'quality':{'status':'fresh' if fresh==len(SERIES) and ciss and ciss['quality']['status']=='fresh' else 'degraded' if fresh else 'unavailable',
            'fresh_native_series':fresh,'expected_native_series':len(SERIES),'ecb_headline_status':ciss['quality']['status'] if ciss else 'unavailable'},
        'channels':{name:{'series_ids':list(ids),'fresh':sum(rows[s]['quality']['status']=='fresh' for s in ids),
            'expected':len(ids),'score':None,'weight':None} for name,ids in GROUPS.items()},
        'dependency_graph':{'roots':{**{f'FRED:{sid}':{'series_id':sid,'provider':'FRED','source_replay':rows[sid]['source_replay']} for sid in SERIES},
            **({'ECB:'+ciss['series_id']:{'series_id':ciss['series_id'],'provider':'ECB','source_replay':ciss['source_replay']}} if ciss else {})},
            'source_families':{**{k:list(v) for k,v in FAMILIES.items()},**({'ecb_systemic_stress':[ciss['series_id']]} if ciss else {})},
            'comparisons':{k:['FRED:'+s for s in row['series_ids']] for k,row in comparisons.items()},
            'independent_votes':0,'note':'A source may support multiple comparisons. Families overlap economically; their count is not effective independent evidence.'},
        'context':context_view,'master_crisis_score':None,'composite_score':None,'score':None,
        'defcon_level':None,'defcon_name':'RESEARCH_ONLY','defcon_color':None,'trend':None,'playbook':None,
        'primary_drivers':[],'components_available':0,
        'components':[{'source':key,'label':key,'available':False,'crisis_contribution':None,'weight':None,'reason':'Unqualified context'} for key in CONTEXT_KEYS],
        'decision':{'verb':'WAIT','meaning':'abstain','reason':REASON},'call':None,
        'portfolio_consequences':{'status':'SCENARIOS_ONLY','target_weights':None,'sizing_multiplier':None,'expected_return':None,
            'crisis_probability':None,'forced_liquidation':False,'scenario_page':'/position-sizer.html','reason':REASON,
            'channels':{'funding':'Inspect financing cost and collateral/margin requirements under explicit assumptions.',
                'credit':'Inspect spread sensitivity, issuer concentration and liquidity under explicit spread shocks.',
                'dollar_and_rates':'Inspect duration and currency exposures with separately stated rate and FX shocks.',
                'volatility':'Inspect nonlinear option exposure using instrument-specific pricing and volatility assumptions.'}},
        'methodology':{'selection':'23 requested native FRED identities, retained original definitions and observations; every former input remains linked as context.',
            'pairs':'Latest identical date, at most seven days behind either source; a missing value on that date is not replaced.',
            'clocks':'Observation, acquisition, provider update and compilation are distinct; historical publication availability remains unverified.',
            'buffers':'Reserve balances, Fed assets, TGA and RRP have distinct scopes and native units; they are not summed into crisis severity.',
            'authority':REASON},**PERMISSIONS}
