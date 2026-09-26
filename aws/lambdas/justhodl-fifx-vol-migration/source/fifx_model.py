"""Native compact projection; source arithmetic and complete histories stay intact."""
from copy import deepcopy
from datetime import date,datetime,time,timedelta,timezone
import re
import fifx_candidate as arithmetic
import fifx_catalog as catalog
from fifx_originals import clock
import fifx_timezones as timezones

CONTRACT='fifx-vol-research.v1'
PREFIX='data/fifx-vol-research/'
CURRENT='data/fifx-vol.json'
HISTORY='data/fifx-vol-history.json'
PRIVATE='audit-private/20260909-originals/fifx-vol-research/'
encoded=arithmetic.encoded


def private_ref(ref):
    return isinstance(ref,dict) and re.fullmatch('[a-f0-9]{64}',str(ref.get('sha256'))) and ref.get('key')==PRIVATE+ref['sha256']+'.bin' and type(ref.get('bytes')) is int and 0<ref['bytes']<=64*1024*1024


def empty_watermarks():
    return {sid:{'acquired_at':None,'observation_date':None} for sid in catalog.SOURCES}


def watermarks(packet):
    marks=packet.get('source_watermarks')
    if not isinstance(marks,dict) or set(marks)!=set(catalog.SOURCES):raise ValueError('All source watermarks required')
    for row in marks.values():
        if set(row)!={'acquired_at','observation_date'}:raise ValueError('Complete source clocks required')
        if row['acquired_at'] is not None:clock(row['acquired_at'])
        if row['observation_date'] is not None and str(date.fromisoformat(row['observation_date']))!=row['observation_date']:
            raise ValueError('Canonical source date required')
    return deepcopy(marks)


def compact_source(output, ref, previous):
    sid=output['source_id'];stamp=clock(output['generated_at'])
    before=watermarks({'source_watermarks':previous})[sid]
    if before['acquired_at'] and clock(before['acquired_at'])>stamp:raise ValueError('Future acquisition watermark')
    # A prior Tokyo/Sydney session may have a later date than UTC; the accepted
    # source clock is compared with its own local date in candidate arithmetic.
    acquired=(output.get('receipt') or {}).get('acquired_at')
    eligible=bool((output.get('source_identity') or {}).get('identity_reviewed'))
    dates=[r['date'] for r in output['original_rows'] if r['date']<=output['quality'].get('acquisition_cutoff_date','')]
    observed=max(dates,default=None) if eligible else None
    regressed=bool(before['acquired_at'] and acquired and clock(acquired)<clock(before['acquired_at']) or
                   before['observation_date'] and observed and observed<before['observation_date'])
    summary={k:deepcopy(v) for k,v in output.items() if k not in ('original_rows','history','methodology','candidate_only')}
    summary.update(complete_source_artifact=deepcopy(ref),retained_original_rows=len(output['original_rows']),
                   retained_history_rows=len(output['history']),source_arithmetic_contract=output['contract'])
    summary['contract']='fifx-source-summary.v1'
    summary['current_expires_at']=None
    if summary['current'] is not None:
        zone=timezones.zone(output['source_identity']['timezone']['name']) if output['source_identity'].get('timezone') else timezone.utc
        day=date.fromisoformat(summary['latest_reported']['date'])+timedelta(days=summary['specification']['max_observation_age_days']+1)
        expires=min(clock(acquired)+timedelta(seconds=catalog.MAX_ACQUISITION_SECONDS),datetime.combine(day,time.min,zone).astimezone(timezone.utc))
        summary['current_expires_at']=expires.isoformat()
    if regressed:
        summary['current']=None
        summary['current_expires_at']=None
        summary['quality'].update(status='source_regression',calculation_quality=output['quality']['status'])
    mark={'acquired_at':max((v for v in (before['acquired_at'],acquired) if v),key=clock,default=None),
          'observation_date':max((v for v in (before['observation_date'],observed) if v),default=None)}
    return summary,mark


def packet(stamp, summaries, marks, predecessors, context, acquisition):
    if set(summaries)!=set(catalog.SOURCES):raise ValueError('Complete compact source inventory required')
    if set(predecessors)!={'packet','history'} or not all(private_ref(v) for v in predecessors.values()):
        raise ValueError('Both complete predecessor packets required')
    if (context.get('independent_votes')!=0 or context.get('source_key')!='data/bond-vol.json' or
        context.get('status') not in ('retained_unqualified_context','unavailable') or
        (context['status']=='retained_unqualified_context' and not private_ref(context.get('original'))) or
        (context['status']=='unavailable' and context.get('original') is not None)):
        raise ValueError('Complete Bond Vol context must remain unqualified')
    count=sum(row['current'] is not None for row in summaries.values())
    return {'contract':CONTRACT,'engine':'fifx-vol-migration','version':'2.0.0','generated_at':stamp,
        'series':summaries,'source_watermarks':watermarks({'source_watermarks':marks}),
        'predecessors':deepcopy(predecessors),'bond_vol_context':deepcopy(context),'acquisition':deepcopy(acquisition),
        'quality':{'status':'complete_descriptive_sources' if count==len(catalog.SOURCES) else 'degraded',
            'current_sources':count,'total_sources':len(catalog.SOURCES),'original_rows':sum(r['retained_original_rows'] for r in summaries.values()),
            'history_rows':sum(r['retained_history_rows'] for r in summaries.values())},
        'methodology':deepcopy(catalog.METHOD),'dependency_graph':{'series_roots':{s:catalog.SPECS[s]['dependency_roots'] for s in catalog.SOURCES},
            'independent_votes':0,'statistical_independence_qualified':False},
        # Old consumer keys remain explicit, without mixed-unit or causal values.
        'legs':{k:{'level':None,'level_pct':None,'z':None,'pctile':None,'measure':'Separate dated source measurements; see series.'}
                for k in ('fixed_income','fx','equity','asia','global')},
        'ratios':{'move_vix':{'last':None,'z':None,'pctile':None},'fxvol_vix':{'last':None,'z':None,'pctile':None}},
        'migration':{'upstream_z':None,'equity_z':None,'spillover':None,'state':None,'read':None,'asia_spill':None,'asia_state':None},
        'history':[],'deep_history_key':HISTORY,'history_format':'complete separate source artifacts, not a synchronized cross-asset composite',
        'regime':None,'call':None,'signals':[],'decision':{'verb':'WAIT','meaning':'abstain'},
        'portfolio_consequences':{'status':'UNAVAILABLE','target_weights':None,
            'reason':'Separate descriptive histories do not establish predictive edge, portfolio covariance, costs or investor constraints.'},
        **catalog.AUTHORITY}
