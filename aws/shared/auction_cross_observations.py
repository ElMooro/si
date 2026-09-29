"""Dated arithmetic for the existing five auction-context FRED series.

Pure calculations on complete supplied adapter responses. Original HTTP bytes,
source acquisition/cache age, definitions and first-release vintages remain
unverified. These descriptive measurements grant no investment authority.
"""
from copy import deepcopy
from datetime import date,timedelta
import math
import json
from auction_benchmarks import day,number
from funding_research_catalog import SERIES as FUNDING_SERIES

CONTRACT='auction-cross-observations.v1'
LIMITS={'SOFR':5,'IORB':5,'DTWEXBGS':60,'T10Y2Y':5,'T5YIFR':5}
MAX_AGE={sid:FUNDING_SERIES[sid][3] if sid in FUNDING_SERIES else 5 for sid in LIMITS}
UNITS={'SOFR':'percent','IORB':'percent','DTWEXBGS':'index_jan_2006_100','T10Y2Y':'percentage_points','T5YIFR':'percent'}
PERMISSIONS=dict.fromkeys(('source_capture_verified','source_definition_verified','historical_point_in_time_verified',
                           'forecast_eligible','calls_eligible','sizing_eligible','execution_eligible'),False)


def series(frame,sid,today):
    result={'series_id':sid,'unit':UNITS[sid],'maximum_observation_age_calendar_days':MAX_AGE[sid],
            'status':'unavailable','reason':'source_unavailable','observations':[],
            'latest_date':None,'latest_value':None,'observation_age_days':None}
    if not isinstance(frame,dict) or frame.get('series_id')!=sid or type(frame.get('requested_limit')) is not int or frame['requested_limit']!=LIMITS[sid]:
        return result
    response=frame.get('response')
    if frame.get('read_status')!='received' or not isinstance(response,dict) or not isinstance(response.get('observations'),list):
        return result
    raw=response['observations']
    if len(raw)>LIMITS[sid]:return {**result,'reason':'response_exceeds_requested_limit'}
    seen=set();indexed=[]
    for index,row in enumerate(raw):
        observed=day(row.get('date')) if isinstance(row,dict) else None
        value=number(row.get('value')) if isinstance(row,dict) else None
        if observed is None or observed>today or observed in seen:
            return {**result,'reason':'invalid_future_or_duplicate_observation_date'}
        seen.add(observed)
        indexed.append({'source_row_index':index,'date':observed.isoformat(),'value':value})
    indexed.sort(key=lambda row:row['date'])
    result['observations']=indexed
    if not indexed:return {**result,'reason':'no_observations'}
    latest=indexed[-1];age=(today-day(latest['date'])).days
    result.update(latest_date=latest['date'],latest_value=latest['value'],observation_age_days=age)
    if age>MAX_AGE[sid]:return {**result,'reason':'observation_too_old'}
    if latest['value'] is None:return {**result,'reason':'latest_observation_missing'}
    return {**result,'status':'available','reason':None}


def measurement(unit,today,**extra):
    return {'measurement_contract':CONTRACT,'unit':unit,'calculation_as_of':today.isoformat(),
            'measurement_status':'unavailable','err':'required_observation_unavailable',
            'regime':'UNAVAILABLE',**PERMISSIONS,**extra}


def repo(inputs,today):
    sofr,iorb=inputs['SOFR'],inputs['IORB']
    out=measurement('bp',today,sofr_pct=None,iorb_pct=None,spread_bp=None,observation_date=None,
                    legacy_threshold_band=None,trace=None,
                    interpretation='A same-date observed SOFR/IORB pair is required. No forward filling or funding-crisis inference.')
    if any(v['status']!='available' for v in (sofr,iorb)):return out
    left={row['date']:row for row in sofr['observations']};right={row['date']:row for row in iorb['observations']}
    common=sorted(set(left)&set(right))
    if not common:return {**out,'err':'no_common_observation_date'}
    at=common[-1];a,b=left[at],right[at];age=(today-day(at)).days
    if age>min(MAX_AGE['SOFR'],MAX_AGE['IORB']):return {**out,'err':'common_observation_too_old'}
    if a['value'] is None or b['value'] is None:return {**out,'err':'latest_common_observation_missing'}
    spread=(a['value']-b['value'])*100
    if not math.isfinite(spread):return {**out,'err':'nonfinite_rate_difference'}
    spread=round(spread,2)
    return {**out,'measurement_status':'complete','err':None,'sofr_pct':a['value'],'iorb_pct':b['value'],
            'spread_bp':round(spread,2),'observation_date':at,
            'regime':'POSITIVE' if spread>0 else 'NEGATIVE' if spread<0 else 'ZERO',
            'legacy_threshold_band':'ACUTE' if spread>8 else 'ELEVATED' if spread>4 else 'WATCH' if spread>1 else 'CALM',
            'trace':{'selection':'latest_common_observation_date_without_forward_fill','sofr':a,'iorb':b,
                     'sofr_latest_source_date':sofr['latest_date'],'iorb_latest_source_date':iorb['latest_date'],
                     'age_calendar_days':age,'maximum_age_calendar_days':5,'formula':'100 * (SOFR percent - IORB percent)',
                     'rounding':'two decimal basis points; fixed legacy band uses the same rounded measurement'},
            'interpretation':'Dated secured/administered rate difference. Its sign alone does not identify collateral scarcity, a cash shortage or funding distress. Fixed legacy bands remain unvalidated.'}


def dollar(inputs,today):
    source=inputs['DTWEXBGS']
    out=measurement('index_jan_2006_100',today,level=None,observation_date=None,
                    change_30d_pct=None,change_30d_target_pct=None,comparison=None,
                    interpretation='Broad trade-weighted dollar index; no investor-motive or auction-demand conclusion.',
                    legacy_key_note='change_30d_pct is available only when the observed interval is exactly 30 calendar days; the target comparison exposes its actual dates.')
    if source['status']!='available':return {**out,'err':source['reason']}
    latest=source['observations'][-1];at=day(latest['date']);target=at-timedelta(days=30)
    if latest['value']<0:return {**out,'err':'negative_index_level'}
    out.update(level=latest['value'],observation_date=latest['date'])
    candidates=[row for row in source['observations'] if day(row['date'])<=target]
    baseline=candidates[-1] if candidates else None
    if baseline is None:return {**out,'err':'missing_30_day_baseline'}
    delay=(target-day(baseline['date'])).days;span=(at-day(baseline['date'])).days
    trace={'series_id':'DTWEXBGS','unit':'percent_change','target_date':target.isoformat(),
           'start':baseline,'end':latest,'elapsed_calendar_days':span,'baseline_delay_calendar_days':delay,
           'maximum_baseline_delay_calendar_days':7,'formula':'100 * (end index / start index - 1)',
           'change_pct':None,'status':'unavailable'}
    out['comparison']=trace
    if delay>7:return {**out,'err':'baseline_too_far_before_target'}
    if baseline['value'] is None or baseline['value']<=0 or latest['value']<0:
        return {**out,'err':'missing_or_invalid_index_basis'}
    change=(latest['value']/baseline['value']-1)*100
    if not math.isfinite(change):return {**out,'err':'nonfinite_index_change'}
    change=round(change,2);trace.update(change_pct=change,status='complete')
    return {**out,'measurement_status':'complete','err':None,'change_30d_target_pct':change,
            'change_30d_pct':change if span==30 else None,
            'regime':'HIGHER' if change>0 else 'LOWER' if change<0 else 'UNCHANGED'}


def single(inputs,sid,today):
    source=inputs[sid];curve=sid=='T10Y2Y';key='spread_bp' if curve else 'rate_pct'
    out=measurement('bp' if curve else 'percent',today,**{key:None},observation_date=None,trace=None,
        interpretation='Dated 10-year minus 2-year Treasury yield difference; not a recession probability.' if curve else
                       'Reported forward inflation-compensation series. No inference about anchored expectations or a comparison with the policy inflation target is established.')
    if source['status']!='available':return {**out,'err':source['reason']}
    latest=source['observations'][-1];value=latest['value']*(100 if curve else 1)
    if not math.isfinite(value):return {**out,'err':'nonfinite_unit_conversion'}
    return {**out,'measurement_status':'complete','err':None,key:round(value,2),'observation_date':latest['date'],
            'regime':('POSITIVE' if value>0 else 'NEGATIVE' if value<0 else 'ZERO') if curve else 'DESCRIPTIVE',
            'trace':{'series_id':sid,'source':latest,'source_unit':UNITS[sid],
                     'formula':'100 * percentage-point spread' if curve else 'reported percent, no transformation'}}


def build(frames,today):
    if type(today) is not date or not isinstance(frames,dict):raise ValueError('Dated cross-source frames required')
    json.dumps(frames,allow_nan=False)  # Retained parsed responses must also be valid finite JSON.
    inputs={sid:series(frames.get(sid),sid,today) for sid in LIMITS}
    return {'measurement_contract':CONTRACT,'calculation_as_of':today.isoformat(),
            'source_frames':deepcopy(frames),'source_observations':inputs,
            'repo_stress':repo(inputs,today),'dollar_strength':dollar(inputs,today),
            'curve_slope':single(inputs,'T10Y2Y',today),'inflation_expectations':single(inputs,'T5YIFR',today),
            **PERMISSIONS,
            'limitations':['Observation-age policies are conservative bounds, not verified provider release calendars.',
                          'Adapter read time is not original HTTP acquisition time; the existing FRED cache can supply older response bytes.',
                          'Parsed responses are retained here. Original HTTP bytes, independent source replay, definitions and first-release vintages remain unverified.',
                          'These five series are contextual measurements, not independent corroborating votes or portfolio instructions.']}
