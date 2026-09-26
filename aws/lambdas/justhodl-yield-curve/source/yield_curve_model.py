"""Native curve projection with explicit acquisition continuity and full histories."""
from copy import deepcopy
import yield_curve_arithmetic as arithmetic
from report_observations import encoded,digest
from research_brief_model import clock

CONTRACT='yield-curve-research.v1'
PREFIX='data/yield-curve-research/'
CURRENT='data/yield-curve.json'


def watermarks(packet):
    result=packet.get('source_acquisition_watermarks') or {}
    if set(result)!=set(arithmetic.SERIES):raise ValueError('Complete curve acquisition inventory required')
    for value in result.values():
        if value is not None:clock(value)
    return deepcopy(result)


def build(source,originals,generated_at,context,predecessor,previous):
    if set(previous)!=set(arithmetic.SERIES):raise ValueError('Complete previous acquisition inventory required')
    output=arithmetic.build(source,originals,generated_at,context,predecessor)
    output.pop('candidate_only');output.pop('publication_eligible');output['contract']=CONTRACT
    output['source_acquisition_watermarks']={};regressed=set()
    for sid,row in output['series'].items():
        # The reviewed catalog uses tuples; the native JSON contract uses lists.
        # This does not change their encoded bytes or accepted arithmetic.
        row['reviewed_definition']=list(row['reviewed_definition'])
        before,current=previous[sid],row['acquired_at']
        if before and clock(before)>clock(generated_at):raise ValueError('Future acquisition watermark')
        if before and current and clock(current)<clock(before):
            regressed.add(sid);row['quality']['status']='source_regression'
            row['current']={'value':None,'exact_decimal':None};row['current_observation_comparisons']=None
        output['source_acquisition_watermarks'][sid]=max((v for v in (before,current) if v is not None),key=clock,default=None)
    for row in output['derived'].values():
        if regressed.intersection(row['coefficients']):
            row['current']=None;row['current_comparisons']=None
            row['quality'].update(status='unavailable',reason='source_regression')
    for row in output['curves'].values():
        if regressed.intersection(row['requested_series']):
            row.update(observation_date=None,points=[],complete=False)
    if regressed.intersection(('DGS2','DGS10')):
        output['shape']=arithmetic.shape(output['derived']['2s10s'])
    fresh=sum(row['quality']['status']=='within_age_ceiling' for row in output['series'].values())
    output['quality'].update(current_series=fresh,status='within_age_ceiling' if fresh==len(arithmetic.SERIES) else 'degraded' if fresh else 'unavailable')
    return output


def compact(output,series_refs):
    if set(series_refs)!=set(arithmetic.SERIES):raise ValueError('Every complete curve series artifact required')
    result={k:deepcopy(v) for k,v in output.items() if k!='series'}
    result['series']={sid:{**{k:deepcopy(v) for k,v in row.items() if k!='history'},
        'complete_history_artifact':deepcopy(series_refs[sid]),'retained_original_rows':len(row['history'])}
        for sid,row in output['series'].items()}
    result['view']={'contract':'yield-curve-summary.v1','complete_series_artifacts':len(series_refs),
        'complete_original_rows':sum(len(row['history']) for row in output['series'].values()),
        'scope':'All measurements and comparisons are present; complete histories load by immutable series reference.'}
    return result
