"""Native research projection and source continuity; accepted arithmetic is unchanged."""
from copy import deepcopy
from datetime import date
import bond_vol_candidate as arithmetic
import verify_bond_vol_arithmetic as independent
from report_observations import encoded,digest
from research_brief_model import clock

CONTRACT='bond-vol-research.v1'
PREFIX='data/bond-vol-research/'
CURRENT='data/bond-vol.json'

def empty_watermarks():return {sid:{'acquired_at':None,'observation_date':None} for sid in (*arithmetic.SERIES,'MOVE')}

def watermarks(packet):
    result=packet.get('source_watermarks')
    if not isinstance(result,dict) or set(result)!=set(empty_watermarks()):raise ValueError('Complete source watermarks required')
    for row in result.values():
        if set(row)!={'acquired_at','observation_date'}:raise ValueError('Complete source clocks required')
        if row['acquired_at'] is not None:clock(row['acquired_at'])
        if row['observation_date'] is not None:
            if date.fromisoformat(row['observation_date']).isoformat()!=row['observation_date']:raise ValueError('Canonical observation date required')
    return deepcopy(result)

def build(source,originals,stamp,context,predecessors,previous,quote_raw,quote_receipt,acquisition):
    if set(predecessors)!={'packet','history'} or not all(arithmetic.original_ref(v) for v in predecessors.values()):
        raise ValueError('Both complete predecessor packets required')
    previous=watermarks({'source_watermarks':previous})
    out=arithmetic.build(source,originals,stamp,context,predecessors['packet'],quote_raw,quote_receipt)
    proof=independent.verify(out,source,originals,quote_raw,quote_receipt)
    out.pop('candidate_only');out['contract']=CONTRACT;out['predecessors']=deepcopy(predecessors)
    out['source_watermarks']={};out['original_arithmetic_checks']=proof;out['quote_acquisition']=deepcopy(acquisition)
    out['portfolio_consequences']['status']='UNAVAILABLE'
    for sid in (*arithmetic.SERIES,'MOVE'):
        row=out['move'] if sid=='MOVE' else out['series'][sid];before=previous[sid]
        if before['acquired_at'] and clock(before['acquired_at'])>clock(stamp):raise ValueError('Future acquisition watermark')
        if before['observation_date'] and before['observation_date']>str(clock(stamp).date()):raise ValueError('Future observation watermark')
        if sid=='MOVE':
            eligible=row.get('identity_reviewed') is True
            acquired=(row.get('receipt') or {}).get('acquired_at') if eligible else None
            observed=(row.get('last_observed') or {}).get('session_date') if eligible else None
        else:
            row['reviewed_definition']=list(row['reviewed_definition'])
            acquired=row['acquired_at'];observed=(row['latest_observation'] or {}).get('date')
        regressed=bool(before['acquired_at'] and acquired and clock(acquired)<clock(before['acquired_at'])
            or before['observation_date'] and observed and observed<before['observation_date'])
        if regressed:
            row['current']=None
            if sid=='MOVE':row['status']='source_regression'
            else:row['current_distribution']=None;row['quality'].update(status='source_regression',current_dispersion_available=False)
        out['source_watermarks'][sid]={
            'acquired_at':max((v for v in (before['acquired_at'],acquired) if v),key=clock,default=None),
            'observation_date':max((v for v in (before['observation_date'],observed) if v),default=None)}
    count=sum(row['current'] is not None for row in out['series'].values())
    out['quality'].update(current_series=count,status='complete_descriptive_rates' if count==len(arithmetic.SERIES) else 'degraded',move_status=out['move']['status'])
    return out

def compact(output,series_refs,quote_ref):
    if set(series_refs)!=set(arithmetic.SERIES):raise ValueError('Every complete series artifact required')
    out={k:deepcopy(v) for k,v in output.items() if k not in ('series','move')}
    out['series']={sid:{**{k:deepcopy(v) for k,v in row.items() if k not in ('original_rows','rolling_history')},
        'complete_history_artifact':deepcopy(series_refs[sid]),'retained_original_rows':len(row['original_rows']),
        'retained_rolling_windows':len(row['rolling_history'])} for sid,row in output['series'].items()}
    out['move']={**{k:deepcopy(v) for k,v in output['move'].items() if k not in ('original','history')},
        'complete_quote_artifact':deepcopy(quote_ref),'retained_quote_rows':len(output['move']['history'])}
    out['view']={'contract':'bond-vol-summary.v1','complete_series_artifacts':len(series_refs),
        'complete_original_rows':sum(len(r['original_rows']) for r in output['series'].values()),
        'complete_rolling_windows':sum(len(r['rolling_history']) for r in output['series'].values()),
        'scope':'Every original row and rolling calculation retained in immutable artifacts; all current statistics present here.'}
    return out
