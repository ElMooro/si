"""Exact calendar windows over the complete retained shipping observation ledger.

Counts are never replaced with cargo weight. Missing dates are never filled.
The retained population is not a census of world shipping or economic activity.
"""
from collections import defaultdict
from datetime import date,datetime,timedelta,timezone
import math,re
CONTRACT='portwatch-calendar-measurements.v1'
FAMILIES={'choke':('n_total','transit_calls'), 'ports':('portcalls','port_calls'), 'choke_fallback':('portcalls','port_calls_fallback_not_transit_calls')}


def source_date(value):
    try:
        if type(value) in (int,float):
            if not math.isfinite(value) or value%86400000:return None
            return datetime.fromtimestamp(value/1000,timezone.utc).date()
        if isinstance(value,str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):return date.fromisoformat(value)
        if isinstance(value,str) and 'T' in value:
            stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
            if stamp.tzinfo is None:return None
            stamp=stamp.astimezone(timezone.utc)
            if any((stamp.hour,stamp.minute,stamp.second,stamp.microsecond)):return None
            return stamp.date()
    except (ValueError,OverflowError,OSError):pass
    return None


def count(value):return type(value) in (int,float) and 0<=value<=9007199254740991 and math.isfinite(value) and value==int(value)


def window(observations,end,days):
    if end is None:return {'start':None,'end':None,'expected_days':days,'available_days':0,'missing_dates':[], 'status':'no_compatible_calendar_anchor','mean':None,'sum':None}
    dates=[end-timedelta(days=days-1-i) for i in range(days)]
    missing=[d.isoformat() for d in dates if not count(observations.get(d))]
    values=[observations[d] for d in dates if count(observations.get(d))]
    total=sum(int(v) for v in values) if not missing else None
    exact=total is None or total<=9007199254740991
    if not exact:total=None
    return {'start':dates[0].isoformat(),'end':end.isoformat(),'expected_days':days,'available_days':len(values),
            'missing_dates':missing,'status':'outside_exact_json_range' if not exact else 'complete' if not missing else 'missing_observations','mean':total/days if total is not None else None,'sum':total}


def comparison(current,previous):
    a,b=current['mean'],previous['mean']
    if a is None or b is None:return {'delta_calls_per_day':None,'percent':None,'status':'incomplete_calendar_windows'}
    return {'delta_calls_per_day':a-b,'percent':100*(a/b-1) if b>0 else None,
            'status':'defined' if b>0 else 'zero_comparison_denominator'}


def entity(family,ident,rows,today):
    field,unit=FAMILIES[family];observations=[];daily={};identity_errors=0;future=0;names=set();countries=set()
    for key,row in sorted(rows):
        parts=key.split('|',1);declared=source_date(row.get('date'));key_day=source_date(parts[1]) if len(parts)==2 else None
        valid_identity=declared is not None and declared==key_day and row.get('portid',row.get('chokepoint_id'))==ident
        value=row.get(field);is_count=count(value);state='observed'
        if not valid_identity:state='invalid_date_or_entity_identity';identity_errors+=1
        elif declared>today:state='future_observation';future+=1
        elif not is_count:state='missing_or_invalid_count'
        if valid_identity and declared<=today:daily[declared]=value if is_count else None
        if isinstance(row.get('portname'),str) and row['portname']:names.add(row['portname'])
        if isinstance(row.get('country'),str) and row['country']:countries.add(row['country'])
        observations.append({'row_key':key,'date':declared.isoformat() if declared else None,'source_field':field,
                             'source_value':value,'count':value if state=='observed' else None,'status':state})
    # A malformed row cannot be skipped to create a clean-looking series.
    anchor=max(daily,default=None) if not identity_errors else None
    last=window(daily,anchor,7);prior30=window(daily,anchor-timedelta(days=7) if anchor else None,30)
    baseline=window(daily,anchor-timedelta(days=7) if anchor else None,358)
    try:anniversary=anchor.replace(year=anchor.year-1) if anchor else None
    except ValueError:anniversary=None
    prior_year=window(daily,anniversary,7)
    return {'family':family,'entity_id':ident,'names':sorted(names),'countries':sorted(countries),'source_field':field,'unit':unit,
            'observation_rows':len(observations),'observations':observations,'last_observation_date':anchor.isoformat() if anchor else None,
            'observation_lag_days':(today-anchor).days if anchor else None,'invalid_identity_rows':identity_errors,'future_rows':future,
            'invalid_count_rows':sum(e['status']=='missing_or_invalid_count' for e in observations),
            'current_7d':last,'previous_30d':prior30,'preceding_358d':baseline,'prior_year_7d':prior_year,
            'year_over_year':comparison(last,prior_year),'versus_preceding_358d':comparison(last,baseline),
            'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False}


def build(history,at):
    stamp=datetime.fromisoformat(at.replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('Aware calculation date required')
    today=stamp.astimezone(timezone.utc).date();entities=[];rows_count=0
    for family in FAMILIES:
        rows=history.get(family,{})
        if not isinstance(rows,dict):raise ValueError('Complete observation family required')
        groups=defaultdict(list)
        for key,row in rows.items():
            if not isinstance(key,str) or not isinstance(row,dict):raise ValueError('Whole identified history row required')
            ident=key.split('|',1)[0];groups[ident].append((key,row));rows_count+=1
        for ident,group in sorted(groups.items()):entities.append(entity(family,ident,group,today))
    return {'contract':CONTRACT,'calculation_at':at,'source':'complete retained native PortWatch history; not a complete world-port census',
            'history_rows':rows_count,'entity_count':len(entities),'entities':entities,'forecast_qualified':False,'sizing_eligible':False,
            'definitions':{'counts':'Only the exact n_total (chokepoints) or portcalls (ports/fallback) count field. Cargo imports, exports and capacity cannot substitute.',
                'windows':'Every calendar date in a window must have one identified nonnegative whole-number count. Missing days are not zero; the latest dated null cannot be replaced by an older reading.',
                'year_over_year':'Seven consecutive calendar dates ending at the latest observation compared with seven ending on that date in the prior calendar year. February 29 without an anniversary stays unavailable. Not a weekday- or holiday-adjusted comparison.',
                'baseline':'The 358 calendar days immediately preceding the current seven-day window. This is not a full one-year baseline. A complete denominator is required.',
                'scope':'All stored rows and entities are inspectable, including invalid, future and unavailable values. Source selection, query completeness, release vintages, AIS methodology, seasonal effects and economic causation remain unqualified.',
                'interpretation':'A change in vessel calls is not a change in tons, cargo value, national exports or corporate earnings. No disruption, inflation, industry-supply or portfolio conclusion follows from these arithmetic comparisons.'}}
