"""Source-defined credit measurements and descriptive statistics; pure arithmetic.

FRED response vintages are current snapshots, not historical availability.
Index OAS, effective yields and a Treasury slope have distinct interpretations.
No return forecast, default probability, position size or crisis score is inferred.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation, localcontext
import hashlib
import json
import math
import re
import urllib.parse

CONTRACT='credit-native-research.v1'
PREFIX='data/credit-research/'
PRIVATE='audit-private/20260909-originals/credit-research/'
PERMISSIONS={'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}
MAX_BYTES=4*1024*1024
MAX_ROWS=12000
HISTORY_DAYS=3660
SOURCE_AGE_CEILING_DAYS=5
PIPELINE_HOURS=36

OAS={
 'BAMLC0A0CM':('US investment grade master','ig','Master'),
 'BAMLC0A1CAAA':('US AAA','ig','AAA'),
 'BAMLC0A2CAA':('US AA','ig','AA'),
 'BAMLC0A3CA':('US A','ig','A'),
 'BAMLC0A4CBBB':('US BBB','ig','BBB'),
 'BAMLH0A0HYM2':('US high yield master','hy','Master'),
 'BAMLH0A1HYBB':('US BB','hy','BB'),
 'BAMLH0A2HYB':('US B','hy','B'),
 'BAMLH0A3HYC':('US CCC and lower','hy','CCC and lower'),
 'BAMLEMCBPIOAS':('Emerging markets corporate plus — mixed ratings','em','IG and below IG'),
 'BAMLEMHBHYCRPIOAS':('Emerging markets corporate plus — high yield','em','HY'),
}
YIELDS={
 'BAMLC0A0CMEY':'IG Master','BAMLC0A1CAAAEY':'AAA','BAMLC0A2CAAEY':'AA',
 'BAMLC0A3CAEY':'A','BAMLC0A4CBBBEY':'BBB','BAMLH0A0HYM2EY':'HY Master',
 'BAMLH0A1HYBBEY':'BB','BAMLH0A2HYBEY':'B','BAMLH0A3HYCEY':'CCC',
 'BAMLEMCBPIEY':'EM Corp',
}
MATURITIES={
 'BAMLC1A0C13YEY':'1-3y','BAMLC2A0C35YEY':'3-5y','BAMLC3A0C57YEY':'5-7y',
 'BAMLC4A0C710YEY':'7-10y','BAMLC7A0C1015YEY':'10-15y','BAMLC8A0C15PYEY':'15y+',
}
SERIES=tuple(sorted((*OAS,*YIELDS,*MATURITIES,'T10Y2Y')))
PAIRS={
 'bbb_minus_aaa':('BAMLC0A4CBBB','BAMLC0A1CAAA','BBB minus AAA index OAS'),
 'ccc_minus_bb':('BAMLH0A3HYC','BAMLH0A1HYBB','CCC-and-lower minus BB index OAS'),
 'hy_minus_ig':('BAMLH0A0HYM2','BAMLC0A0CM','HY minus IG master index OAS'),
 'em_hy_minus_us_hy':('BAMLEMHBHYCRPIOAS','BAMLH0A0HYM2','EM HY minus US HY index OAS'),
}


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')
def sha(raw): return hashlib.sha256(raw).hexdigest()
def stamp(value):
    if not isinstance(value,str): raise ValueError('Explicit timestamp required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None: raise ValueError('Timezone required')
    return result.astimezone(timezone.utc)


def number(value):
    if value=='.': return None
    if not isinstance(value,str) or len(value)>32 or not re.fullmatch(r'-?\d{1,6}(?:\.\d{1,12})?',value):
        raise ValueError('Canonical finite provider decimal required')
    try: out=Decimal(value)
    except InvalidOperation: raise ValueError('Invalid decimal') from None
    if not out.is_finite() or not math.isfinite(float(out)): raise ValueError('Nonfinite observation')
    return out


def shown(value):
    if value is None: return None
    if not value.is_finite() or not math.isfinite(float(value)): raise ValueError('Arithmetic overflow')
    rounded=value.quantize(Decimal('0.000001'))
    return float(value if rounded==0 and value!=0 else rounded)


def source_url(sid,kind,evaluation):
    if sid not in SERIES or kind not in ('definition','observations'): raise ValueError('Unreviewed source identity')
    day=date.fromisoformat(evaluation)
    q={'series_id':sid,'file_type':'json','realtime_start':str(day),'realtime_end':str(day)}
    if kind=='observations':
        q.update(observation_start=str(day-timedelta(days=HISTORY_DAYS)),observation_end=str(day),
                 units='lin',output_type=1,sort_order='asc',limit=100000,offset=0)
    return 'https://api.stlouisfed.org/fred/'+('series' if kind=='definition' else 'series/observations')+'?'+urllib.parse.urlencode(q)


def json_object(raw):
    if not isinstance(raw,bytes) or len(raw)>MAX_BYTES: raise ValueError('Bounded original bytes required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise ValueError('Duplicate JSON member')
            result[key]=value
        return result
    value=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,
                     parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))
    if not isinstance(value,dict): raise ValueError('Original object required')
    return value


def parse(sid,definition_raw,observations_raw,evaluation):
    """Validate the complete requested response; preserve provider null rows."""
    if sid not in SERIES: raise ValueError('Unreviewed source identity')
    at=date.fromisoformat(evaluation)
    definitions=json_object(definition_raw).get('seriess');obs=json_object(observations_raw)
    if not isinstance(definitions,list) or len(definitions)!=1 or definitions[0].get('id')!=sid:
        raise ValueError('Definition identity differs')
    definition=definitions[0]
    for k in ('realtime_start','realtime_end'):
        if definition.get(k)!=evaluation or obs.get(k)!=evaluation: raise ValueError('One current response vintage required')
    if definition.get('units')!='Percent' or definition.get('frequency_short')!='D' or definition.get('seasonal_adjustment_short')!='NSA':
        raise ValueError('Native percent/daily/unadjusted definition required')
    title=definition.get('title')
    if not isinstance(title,str) or not 1<=len(title)<=300: raise ValueError('Publisher title required')
    if sid in OAS and ('Option-Adjusted Spread' not in title or 'ICE BofA' not in title): raise ValueError('OAS definition changed')
    if sid in YIELDS or sid in MATURITIES:
        if 'Effective Yield' not in title or 'ICE BofA' not in title: raise ValueError('Effective-yield definition changed')
    if sid=='T10Y2Y' and ('10-Year' not in title or '2-Year' not in title): raise ValueError('Treasury slope definition changed')
    expected={'units':'lin','output_type':1,'offset':0,'sort_order':'asc','limit':100000,
              'observation_start':str(at-timedelta(days=HISTORY_DAYS)),'observation_end':evaluation}
    if any(obs.get(k)!=v or type(obs.get(k)) is not type(v) for k,v in expected.items()):
        raise ValueError('Requested native observation window differs')
    rows=obs.get('observations')
    if not isinstance(rows,list) or not 1<=len(rows)<=MAX_ROWS or type(obs.get('count')) is not int or obs['count']!=len(rows):
        raise ValueError('Complete nonempty response required')
    result=[];previous=None
    for index,row in enumerate(rows):
        if not isinstance(row,dict): raise ValueError('Observation row required')
        day=date.fromisoformat(row['date'])
        if str(day)!=row['date'] or not at-timedelta(days=HISTORY_DAYS)<=day<=at or (previous is not None and day<=previous):
            raise ValueError('Duplicate, unordered or out-of-window observation')
        if row.get('realtime_start')!=evaluation or row.get('realtime_end')!=evaluation: raise ValueError('Mixed row vintage')
        value=number(row.get('value'))
        # Negative Treasury slopes and exceptional negative spreads remain measurements.
        if value is not None and not Decimal('-1000')<=value<=Decimal('1000'): raise ValueError('Percent observation outside reviewed arithmetic range')
        result.append({'date':str(day),'value':value,'row_index':index,'raw_value':row['value']});previous=day
    return definition,result


def statistics(rows,value,count):
    """Last N reported numeric observations, with actual span and missingness.

    This is not N trading days. Null records inside the span are counted and
    left unfilled. The current value may not jump over a missing latest row.
    """
    numeric=[r for r in rows if r['value'] is not None]
    chosen=numeric[-count:]
    out={'requested_observations':count,'numeric_observations':len(chosen),
         'first_date':chosen[0]['date'] if chosen else None,'last_date':chosen[-1]['date'] if chosen else None,
         'mean_pct':None,'sample_stddev_pp':None,'z_score':None,'missing_rows_inside_span':0,
         'basis':'Last N numeric provider observations, inclusive; actual dates, not assumed trading-day counts.',
         'status':'insufficient_history',**PERMISSIONS}
    if chosen: out['missing_rows_inside_span']=sum(r['value'] is None and chosen[0]['date']<=r['date']<=chosen[-1]['date'] for r in rows)
    if len(chosen)!=count: return out
    with localcontext() as arithmetic:
        arithmetic.prec=40
        mean=sum((r['value'] for r in chosen),Decimal(0))/Decimal(count)
        sd=(sum(((r['value']-mean)**2 for r in chosen),Decimal(0))/Decimal(count-1)).sqrt()
        out.update(mean_pct=shown(mean),sample_stddev_pp=shown(sd),
                   z_score=shown((value-mean)/sd) if value is not None and sd else None,
                   status='constant_window' if not sd else 'latest_unavailable' if value is None else 'available')
    return out


def percentile(rows,value,start):
    """Midrank empirical percentile over an explicit dated reference window."""
    selected=[r for r in rows if r['date']>=start]
    values=[r['value'] for r in selected if r['value'] is not None]
    complete=bool(rows and rows[0]['date']<=start)
    out={'requested_start':start,'first_returned_date':selected[0]['date'] if selected else None,
         'last_returned_date':selected[-1]['date'] if selected else None,'numeric_observations':len(values),
         'missing_observations':len(selected)-len(values),'full_requested_span_available':complete,
         'percentile_pct':None,'formula':'100 * (count below + 0.5 * count equal) / numeric observations',
         'predictive_probability':False,'status':'insufficient_history'}
    if not complete or not values or value is None: return out
    with localcontext() as arithmetic:
        arithmetic.prec=40
        rank=Decimal(sum(v<value for v in values))+Decimal('0.5')*sum(v==value for v in values)
        out.update(percentile_pct=shown(100*rank/Decimal(len(values))),status='available')
    return out


def metric(sid,definition,rows,collected_at):
    at=stamp(collected_at);last=rows[-1];prior=rows[-2] if len(rows)>1 else None
    value=last['value'];previous=prior['value'] if prior else None
    numeric=[r for r in rows if r['value'] is not None];latest_numeric=numeric[-1] if numeric else None
    age=(at.date()-date.fromisoformat(last['date'])).days
    status='unavailable' if value is None else 'stale' if age>SOURCE_AGE_CEILING_DAYS else 'within_age_ceiling'
    delta=value-previous if value is not None and previous is not None else None
    kind='oas' if sid in OAS else 'treasury_slope' if sid=='T10Y2Y' else 'effective_yield'
    meta=(dict(zip(('name','tier','rating'),OAS[sid])) if sid in OAS else
          {'name':'US Treasury 10y minus 2y','tier':'rates','rating':None} if sid=='T10Y2Y' else
          {'name':(YIELDS|MATURITIES)[sid],'tier':'effective_yield','rating':YIELDS.get(sid)})
    stat={str(n):statistics(rows,value,n) for n in (60,252)}
    one_year=percentile(rows,value,str(date.fromisoformat(last['date'])-timedelta(days=365)))
    span_end=date.fromisoformat(last['date']);source_valid_until=datetime.combine(span_end+timedelta(days=SOURCE_AGE_CEILING_DAYS+1),datetime.min.time(),timezone.utc)
    return {'series_id':sid,'kind':kind,'meta':meta,'source_url':'https://fred.stlouisfed.org/series/'+sid,
        'definition':definition['title'],'source_unit':'Percent','unit':'percentage_points' if kind=='treasury_slope' else 'percent',
        'seasonal_adjustment':definition['seasonal_adjustment'],'frequency':definition['frequency'],
        'provider_updated_at':definition.get('last_updated'),'published_at':None,'observation_date':last['date'],
        'collected_at':collected_at,'value_pct':shown(value),'value_bps':shown(value*100) if value is not None else None,
        'previous_observation_date':prior['date'] if prior else None,'previous_value_pct':shown(previous),
        'change_pp':shown(delta),'change_bps':shown(delta*100) if delta is not None else None,
        'comparison_gap_days':(date.fromisoformat(last['date'])-date.fromisoformat(prior['date'])).days if prior else None,
        'comparison_basis':'Previous provider row; nulls are not skipped and gaps are not daily returns.',
        'exact':{'value_pct':str(value) if value is not None else None,'previous_pct':str(previous) if previous is not None else None,
                 'change_pp':str(delta) if delta is not None else None},
        'latest_numeric_context':{'value_pct':shown(latest_numeric['value']),'observation_date':latest_numeric['date']} if latest_numeric else None,
        'original_row_index':last['row_index'],
        'quality':{'status':status,'observation_age_days':age,'max_observation_age_days':SOURCE_AGE_CEILING_DAYS,
                   'release_calendar_verified':False,'historical_availability_verified':False},
        'source_valid_until':source_valid_until.isoformat(),
        'history_coverage':{'first_date':rows[0]['date'],'last_date':last['date'],'provider_rows':len(rows),
            'numeric_rows':len(numeric),'null_rows':len(rows)-len(numeric),
            'weekend_numeric_rows':sum(date.fromisoformat(r['date']).weekday()>=5 for r in numeric),
            'history_limit':'FRED ICE history currently limited to approximately three years; exact returned span shown.' if sid!='T10Y2Y' else 'Complete requested current-vintage response, not full inception history.',
            'current_response_vintage':at.date().isoformat(),'point_in_time_history':False},
        'descriptive_statistics':{'observation_windows':stat,'calendar_365_day_percentile':one_year},
        # Compatibility fields keep their documented percent scale; ambiguous
        # day/all-time statistics are withheld rather than silently redefined.
        'current':shown(value),'date':last['date'],'dod_change_bps':None,
        'ma_60d':None,'ma_252d':None,'z_score_60d':None,'pct_1y':None,'pct_5y':None,'pct_all_time':None,
        'n_history_days':len(numeric),**PERMISSIONS}


def difference(name,measurements,series_rows):
    left,right,label=PAIRS[name];a=measurements[left];b=measurements[right]
    ar={r['date']:r for r in series_rows.get(left,[])};br={r['date']:r for r in series_rows.get(right,[])}
    shared=sorted(set(ar)&set(br));day=shared[-1] if shared else None
    av=ar[day]['value'] if day else None;bv=br[day]['value'] if day else None
    value=av-bv if av is not None and bv is not None else None
    same_latest=bool(day and a.get('observation_date')==b.get('observation_date')==day)
    age_valid=all(m['quality']['status']=='within_age_ceiling' for m in (a,b))
    available=same_latest and age_valid and value is not None
    return {'id':name,'label':label,'left_series_id':left,'right_series_id':right,
        'unit':'basis_points','value_bps':shown(value*100) if value is not None else None,
        'value_pp':shown(value),'observation_date':day,'left_latest_date':a.get('observation_date'),
        'right_latest_date':b.get('observation_date'),'both_latest_dates_match':same_latest,
        'current_comparison_available':available,'status':'available' if available else 'unavailable' if value is None else 'lagged_context',
        'left_original_row_index':ar[day]['row_index'] if day else None,
        'right_original_row_index':br[day]['row_index'] if day else None,
        'formula':'100 * (left OAS percent - right OAS percent) at the same provider date',
        'interpretation':'Index OAS difference; constituent ratings, maturity, currency and sector mix may differ. Not a pure default premium or executable spread.',
        **PERMISSIONS}


def compute(sources,collected_at):
    at=stamp(collected_at);evaluation=at.date().isoformat()
    if set(sources)!=set(SERIES): raise ValueError('Complete reviewed 28-series inventory required')
    measurements={};rows_by_series={};evidence=[];errors=[]
    for sid in SERIES:
        src=sources[sid]
        try:
            if src.get('error'): raise ValueError('source_unavailable')
            definition,rows=parse(sid,src['definition']['raw'],src['observations']['raw'],evaluation)
            measurements[sid]=metric(sid,definition,rows,collected_at);rows_by_series[sid]=rows
        except (ValueError,KeyError,TypeError,OverflowError,RecursionError):
            measurements[sid]={'series_id':sid,'current':None,'value_pct':None,'value_bps':None,'observation_date':None,
                'quality':{'status':'unavailable'},'err':'source_definition_observations_or_arithmetic_unavailable',**PERMISSIONS}
            errors.append(sid)
        for kind in ('definition','observations'):
            item=src.get(kind,{})
            if item.get('evidence'): evidence.append({'series_id':sid,'kind':kind,'acquired_at':item.get('acquired_at'),**item['evidence']})
    pairs={name:difference(name,measurements,rows_by_series) for name in PAIRS}
    fresh=sum(m['quality']['status']=='within_age_ceiling' for m in measurements.values())
    status='fresh' if fresh==len(SERIES) else 'partial' if fresh else 'unavailable'
    deadline=at+timedelta(hours=PIPELINE_HOURS)
    source_deadlines=[stamp(m['source_valid_until']) for m in measurements.values() if m.get('source_valid_until')]
    valid_until=min([deadline,*source_deadlines])
    core=(*OAS,'T10Y2Y')
    out={'contract':CONTRACT,'version':'2.0.0','generated_at':collected_at,'as_of':measurements['BAMLH0A0HYM2'].get('observation_date'),
        'data_date':measurements['BAMLH0A0HYM2'].get('observation_date'),'measurements':measurements,
        'metrics':{sid:measurements[sid] for sid in core},
        'current_pct':{sid:measurements[sid]['value_pct'] for sid in core},
        'current_bps':{sid:measurements[sid]['value_bps'] for sid in core},
        'derived_spreads':{name:p['value_pp'] if p['current_comparison_available'] else None for name,p in pairs.items()},
        'comparisons':pairs,
        'current_yields_pct':{label:measurements[sid]['value_pct'] for sid,label in YIELDS.items()},
        'ig_yield_curve_pct':{label:measurements[sid]['value_pct'] for sid,label in MATURITIES.items()},
        'quality':{'status':status,'basis':'Strict native response checks and explicit observation-age ceiling; not strategy qualification.',
            'within_age_ceiling':fresh,'reviewed_series':len(SERIES),'unavailable_series':errors,
            'release_calendar_verified':False,
            'provider_families_observed':(['ICE_BofA_indices_via_FRED'] if any(measurements[sid]['quality']['status']=='within_age_ceiling' for sid in SERIES if sid!='T10Y2Y') else [])
                +(['US_Treasury_curve_via_FRED'] if measurements['T10Y2Y']['quality']['status']=='within_age_ceiling' else []),
            'independence_note':'Rating and maturity subindices share a provider and overlapping constituents; 28 series are not 28 independent votes.'},
        'freshness':{'pipeline_check_due_at':deadline.isoformat(),'valid_until':valid_until.isoformat(),
            'source_age_rule':'Five calendar-day ceiling per observed row; source and collection clocks remain separate. Release deadlines not verified.'},
        'source_evidence':evidence,
        'regimes':{'hy_regime':'RESEARCH_ONLY','ig_regime':'RESEARCH_ONLY','composite_regime':'RESEARCH_ONLY',
            'composite_signal':'Dated credit measurements; no qualified equity or default forecast.'},
        'composite_regime':'RESEARCH_ONLY','composite_signal':'Dated credit measurements; no qualified equity or default forecast.',
        'regime_changed_from_prior':None,'thresholds':None,'call':None,'portfolio_action':'WAIT',**PERMISSIONS,
        'portfolio_consequences':{'automatic_position_change':False,'target_weight':None,
            'scenario_url':'/position-sizer.html','interpretation':'A spread shock can affect a bond position through its spread duration. Supply position-specific spread duration, rate duration, currency exposure and an explicit shock; an index OAS does not supply those exposures.',
            'first_order_formula':'Approximate price change fraction = -spread_duration_years * spread_shock_bps / 10000; excludes rate, convexity, FX, default, liquidity and carry effects.'},
        'definitions':{'oas':'Provider model-based option-adjusted spread, not a default probability or total expected return.',
            'effective_yield':'Publisher effective-yield index statistic; not realized carry, a bond quote or automatically yield-to-worst.',
            'maturity_buckets':'Different IG constituent baskets; not a fitted zero curve or holding other exposures constant.',
            'em_corporate_plus':'Mixed investment-grade and below-investment-grade corporate/quasi-government debt, USD and EUR; excludes sovereign/supranational debt.',
            'weekends':'ICE may report month-end weekend accrual adjustments; retained rather than forced onto an equity calendar.',
            'vintage':'Current response only. Backfilled dates are not historical first-availability timestamps.',
            'statistics':'Descriptive sample statistics over explicitly dated windows; no probability or predictive edge claim.'},
        'compatibility':{'current_bps':'Correct basis-point values in v2; old v1 field incorrectly contained percent. Use current_pct for percent.',
            'metrics_current':'Percent (Treasury slope in percentage points).',
            'derived_spreads':'Percentage-point differences only when both latest dates match and source ages pass.',
            'retired_ambiguous_fields':['dod_change_bps','ma_60d','ma_252d','z_score_60d','pct_1y','pct_5y','pct_all_time'],
            'n_history_days':'Legacy count of numeric observations, not calendar days.'},
        'data_rights':{'originals':'Protected AWS IAM archive; no anonymous original/history redistribution added.',
            'source_terms_url':'https://fred.stlouisfed.org/series/BAMLH0A0HYM2','historical_redistribution_permission_verified':False}}
    return out
