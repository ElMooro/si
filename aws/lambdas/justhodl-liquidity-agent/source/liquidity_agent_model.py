"""Native measurements, acquisition continuity and a compact original-bound view."""
from copy import deepcopy
import liquidity_agent_arithmetic as arithmetic
from report_observations import encoded,digest
from research_brief_model import clock

CONTRACT='liquidity-agent-research.v1'
PREFIX='data/liquidity-agent-research/'
CURRENT='liquidity-data.json'


def watermarks(packet):
    result=packet.get('source_acquisition_watermarks') or {}
    if set(result)!=set(arithmetic.SERIES):raise ValueError('Complete acquisition inventory required')
    for value in result.values():
        if value is not None:clock(value)
    return deepcopy(result)


def build(source,originals,generated_at,contexts,predecessor,previous):
    if set(previous)!=set(arithmetic.SERIES):raise ValueError('Complete previous acquisition inventory required')
    output=arithmetic.build(source,originals,generated_at,contexts,predecessor)
    output.pop('candidate_only');output.pop('publication_eligible');output['contract']=CONTRACT
    output['source_acquisition_watermarks']={}
    for sid,row in output['series'].items():
        before,current=previous[sid],row['acquired_at']
        if before and clock(before)>clock(generated_at):raise ValueError('Future acquisition watermark')
        if before and current and clock(current)<clock(before):
            row['quality']['status']='source_regression'
            row['current']={'value':None,'exact_decimal':None};row['calendar_comparisons']={}
            row['usd_billions'].update(value=None,exact_decimal=None)
        output['source_acquisition_watermarks'][sid]=max((v for v in (before,current) if v is not None),key=clock,default=None)
    fresh=sum(row['quality']['status']=='within_age_ceiling' for row in output['series'].values())
    output['quality'].update(current_series=fresh,status='within_age_ceiling' if fresh==len(arithmetic.SERIES) else 'degraded' if fresh else 'unavailable')
    if any(output['series'][s]['quality']['status']!='within_age_ceiling' for s in ('WALCL','WTREGEN','RRPONTSYD')):
        output['derived']['net_liquidity']['current']=None
    if output['series']['SP500']['quality']['status']!='within_age_ceiling':
        output['derived']['sp500_60_calendar_days']['current_comparison']=None
    return output


def compact(output,series_refs):
    """History stays in exact per-series artifacts, never discarded or inline on load."""
    if set(series_refs)!=set(arithmetic.SERIES):raise ValueError('Every requested series artifact required')
    # Avoid a second full copy of all histories during Lambda publication.
    result={k:deepcopy(v) for k,v in output.items() if k not in ('series','derived')}
    result['series']={sid:{**{k:deepcopy(v) for k,v in row.items() if k!='history'},
        'complete_history_artifact':deepcopy(series_refs[sid]),'retained_original_rows':len(row['history'])}
        for sid,row in output['series'].items()}
    net=output['derived']['net_liquidity'];reconstruction=net['reconstruction']
    projected_net={k:deepcopy(v) for k,v in net.items() if k!='reconstruction'}
    if reconstruction:
        projected_net['reconstruction']={k:deepcopy(v) for k,v in reconstruction.items() if k not in ('series','calendar_history_180d')}
        projected_net['reconstruction']['series']={sid:{k:deepcopy(v) for k,v in row.items() if k!='history'}
            for sid,row in reconstruction['series'].items()}
        projected_net['reconstruction']['retained_calendar_dates']=len(reconstruction['calendar_history_180d'])
    else:projected_net['reconstruction']=None
    result['derived']={**{k:deepcopy(v) for k,v in output['derived'].items() if k!='net_liquidity'},'net_liquidity':projected_net}
    result['view']={'contract':'liquidity-agent-summary.v1','complete_series_artifacts':len(series_refs),
        'complete_original_rows':sum(len(row['history']) for row in output['series'].values()),
        'scope':'All measurements and comparisons are present; complete histories load by immutable series reference.'}
    return result
