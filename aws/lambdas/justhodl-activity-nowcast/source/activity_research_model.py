"""Dated activity coordinates reconstructed from retained originals.

Weekly source periods remain explicit; daily endpoints are dated, gaps are never
row-shifted and standardization never becomes a GDP forecast or portfolio vote.
"""
from copy import deepcopy
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,math
import report_observations
from research_brief_model import clock,row_status,AGE_LIMITS
from activity_research_catalog import SPECS,SERIES,CORE,WEEKDAY,NOTES

CONTRACT='activity-native-research.v1';PREFIX='data/activity-research/'
CURRENT='data/activity-nowcast.json';PRIVATE='audit-private/20260909-originals/activity-research/'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def shown(value):
    if value is None:return None
    out=float(round(value,8))
    if not math.isfinite(out):raise ValueError('Nonfinite derived value')
    return out
def point(row):
    if row is None:return None
    return {'date':row['date'],'value':shown(row['value']),'exact_value':str(row['value']) if row['value'] is not None else None,'original_row_index':row['original_row_index']}

def history(original,sid,through):
    if original is None:return []
    seen=set();out=[]
    for i,row in enumerate(original['observations']['observations']):
        day=date.fromisoformat(row['date'])
        if str(day)!=row['date'] or day in seen:raise ValueError('Duplicate or noncanonical source date')
        seen.add(day)
        if sid in WEEKDAY and day.weekday()!=WEEKDAY[sid]:raise ValueError('Weekly period identity differs: '+sid)
        if sid=='GDPNOW' and (day.day!=1 or day.month not in (1,4,7,10)):raise ValueError('Quarter target identity differs')
        value=report_observations.decimal(row.get('value'))
        if row.get('value') not in (None,'.','') and value is None:raise ValueError('Invalid source number')
        if value is not None and sid in ('ICSA','CCSA') and value<0:raise ValueError('Negative claims count')
        if str(day)<=through:out.append({'date':str(day),'value':value,'original_row_index':i})
    return sorted(out,key=lambda r:r['date'])

def observed(packet,originals,generated_at):
    at=clock(generated_at);source=clock(packet['generated_at'])
    if source>at:raise ValueError('Future canonical publication')
    measurements={};histories={}
    for sid in SERIES:
        m=packet.get('measurements',{}).get(sid) or {};original=originals.get(sid)
        label,unit,freq,adj=SPECS[sid]
        if m and not original:raise ValueError('Originals required for measured identity')
        if m and (m.get('series_id')!=sid or m.get('unit')!=unit or m.get('frequency')!=freq or m.get('definition',{}).get('seasonal_adjustment_short')!=adj):raise ValueError('Source definition differs: '+sid)
        h=history(original,sid,str(at.date()));histories[sid]=h
        status=row_status(m,at,(at-source).total_seconds());day=m.get('date')
        value=report_observations.decimal(m.get('current_decimal')) if status=='fresh' else None
        due=[source+timedelta(hours=26)]
        if m.get('acquired_at'):due.append(clock(m['acquired_at'])+timedelta(hours=26))
        if day:due.append(datetime.combine(date.fromisoformat(day)+timedelta(days=AGE_LIMITS[freq]+1),time.min,timezone.utc))
        measurements[sid]={'series_id':sid,'label':label,'value':shown(value),'exact_value':str(value) if value is not None else None,
            'unit':unit,'frequency':freq,'seasonal_adjustment':adj,'period_end_weekday':WEEKDAY.get(sid),
            'observation_date':day,'current_row_index':m.get('current_row_index'),'acquired_at':m.get('acquired_at'),
            'source_generated_at':packet['generated_at'],'source_valid_until':min(due).isoformat(),'evaluated_at':generated_at,
            'definition':deepcopy(m.get('definition')),'evidence':deepcopy(m.get('evidence',{})),
            'source_url':'https://fred.stlouisfed.org/series/'+sid,'interpretation':NOTES[sid],
            'quality':{'status':'within_age_ceiling' if value is not None else status,'release_calendar_verified':False},
            'history_coverage':{'returned_rows':len(h),'numeric_rows':sum(r['value'] is not None for r in h),
                'first_date':h[0]['date'] if h else None,'last_date':h[-1]['date'] if h else None,
                'historical_first_availability_verified':False,'current_vintage_only':True},**PERMISSIONS}
    return measurements,histories

def endpoint(rows,target,max_lag):
    """Latest returned row at or before target, never jumping over a missing row."""
    eligible=[r for r in rows if r['date']<=target]
    row=eligible[-1] if eligible else None
    return row if row and (date.fromisoformat(target)-date.fromisoformat(row['date'])).days<=max_lag else None

def changes(rows,sid,row):
    out={};by={r['date']:r for r in rows}
    for weeks in (1,4,13):
        target=(date.fromisoformat(row['date'])-timedelta(weeks=weeks)).isoformat()
        old=by.get(target) if sid in WEEKDAY else endpoint(rows,target,4)
        valid=old is not None and old['value'] is not None and row['value'] is not None
        delta=row['value']-old['value'] if valid else None
        out[str(weeks)]={'calendar_weeks':weeks,'requested_baseline_date':target,'baseline':point(old),'current':point(row),
            'value_change':shown(delta),'basis_point_change':shown(delta*100) if delta is not None and sid in ('BAA10Y','T10Y3M') else None,
            'unit':SPECS[sid][1],'available':valid,'baseline_lag_days':(date.fromisoformat(target)-date.fromisoformat(old['date'])).days if old else None,
            'basis':'Exact weekly reference or explicitly dated daily endpoint within four calendar days; a missing returned row is not skipped.'}
    return out

def standardize(rows,sid,row,include_members=False):
    weekly=sid in WEEKDAY;span=1092 if weekly else 730
    day=date.fromisoformat(row['date']);start=(day-timedelta(days=span)).isoformat()
    chosen=[r for r in rows if start<=r['date']<row['date']];numeric=[r for r in chosen if r['value'] is not None]
    missing=[r['date'] for r in chosen if r['value'] is None];expected=156 if weekly else None
    if weekly:
        by={r['date']:r for r in chosen}
        missing=[(day-timedelta(weeks=i)).isoformat() for i in range(156,0,-1) if by.get((day-timedelta(weeks=i)).isoformat(),{}).get('value') is None]
    minimum=125 if weekly else 400
    covered=bool(rows and rows[0]['date']<=start)
    out={'value':None,'mean':None,'sample_stddev':None,'window_start':start,'window_end_exclusive':row['date'],
        'calendar_days':span,'requested_weekly_periods':expected,'numeric_observations':len(numeric),'minimum_numeric':minimum,
        'returned_observations':len(chosen),'missing_dates':missing,'full_returned_span':covered,
        'calendar_completeness_verified':weekly,'current_in_window':False,'status':'insufficient_history',
        'basis':'Prior 156 calendar weeks or prior 730 calendar days, excluding target; sample standard deviation. Native sign, no inversion or clipping.'}
    if include_members:out['members']=[point(r) for r in chosen]
    if row['value'] is None or not covered or len(numeric)<minimum:return out
    mean=sum(r['value'] for r in numeric)/len(numeric)
    variance=sum((r['value']-mean)**2 for r in numeric)/(len(numeric)-1);sd=variance.sqrt()
    out.update(mean=shown(mean),sample_stddev=shown(sd),status='zero_variance' if sd==0 else 'available')
    if sd:out['value']=shown((row['value']-mean)/sd)
    return out

def weekly_row(rows,sid,week):
    day=date.fromisoformat(week)
    if sid in WEEKDAY:
        expected=(day-timedelta(days=5-WEEKDAY[sid])).isoformat()
        return next((r for r in rows if r['date']==expected),None)
    return endpoint(rows,week,4)

def compile_week(histories,week,expanded=False):
    components={}
    for sid in CORE:
        rows=histories[sid];row=weekly_row(rows,sid,week)
        components[sid]={'reference_week_ending':week,'observation':point(row),'available':bool(row and row['value'] is not None),
            'mapping':'Saturday reference week' if WEEKDAY.get(sid)==5 else 'Friday source reference mapped to following Saturday' if sid in WEEKDAY else 'Latest returned daily endpoint no more than four calendar days before Saturday',
            'changes':changes(rows,sid,row) if row else {},'prior_standardization':standardize(rows,sid,row,expanded) if row else None}
    return {'reference_week_ending':week,'available_components':sum(c['available'] for c in components.values()),
        'components':components,'index':None,'regime':None,'basis':'Common calendar reference; not historical information availability or independent evidence.'}

def build(packet,originals,generated_at,legacy=None,refs=None):
    with localcontext() as arithmetic:
        arithmetic.prec=34;arithmetic.rounding=ROUND_HALF_EVEN
        measurements,histories=observed(packet,originals,generated_at)
        at=clock(generated_at);last=at.date()-timedelta(days=(at.date().weekday()-5)%7)
        if last==at.date():last-=timedelta(weeks=1)
        trail=[compile_week(histories,str(last-timedelta(weeks=i))) for i in range(155,-1,-1)]
        complete=[r for r in trail if r['available_components']==len(CORE)]
        chosen=complete[-1] if complete else None
        fresh=all(measurements[sid]['value'] is not None for sid in CORE)
        current=compile_week(histories,chosen['reference_week_ending'],True) if chosen and fresh and (at.date()-date.fromisoformat(chosen['reference_week_ending'])).days<=21 else None
        latest=compile_week(histories,str(last),True)
        latest_sources={}
        for sid in (*CORE,'T10Y3M'):
            rows=histories[sid];row=rows[-1] if rows else None
            latest_sources[sid]={'observation':point(row),'changes':changes(rows,sid,row) if row else {},
                'prior_standardization':standardize(rows,sid,row,True) if row else None,'current_use':measurements[sid]['quality']['status']}
        gd=measurements['GDPNOW'];gdp_rows=histories['GDPNOW']
        contexts=[]
        for key,ref in sorted((refs or {}).items()):
            doc=(legacy or {}).get(key) or {}
            contexts.append({'source_key':key,'retained_original':ref,'source_generated_at':doc.get('generated_at') or doc.get('as_of'),
                'contract':doc.get('contract'),'status':'retained_unqualified_context' if ref else 'unavailable','independent_votes':0})
        due=min([clock(packet['generated_at'])+timedelta(hours=26),*[clock(m['source_valid_until']) for m in measurements.values() if m['value'] is not None]])
        return {'contract':CONTRACT,'generated_at':generated_at,'source_generated_at':packet['generated_at'],'source_valid_until':due.isoformat(),
            'measurements':measurements,'weekly_context':{'current':current,'latest_closed_week':latest,'trail':trail},'latest_sources':latest_sources,
            'regional_context':{'GDPNOW':{'measurement':gd,'history':[point(r) for r in gdp_rows[-40:]],'forecast_release_time_verified':False,'vintage_path_available':False},
                'T10Y3M':{'measurement':measurements['T10Y3M'],'cleveland_recession_probability':None}},
            'dependency_graph':{'declared':list(CORE),'known_overlap':[{'source':'WEI','contains':['ICSA','CCSA'],'basis':'Source methodology: initial and continued unemployment claims are two of ten WEI components.'},
                {'family':'financial_conditions','members':['NFCI','STLFSI4','BAA10Y'],'basis':'Overlapping rates, credit and financial conditions; complete component-level lineage and independent information are unverified.'}],
                'independent_votes':0,'complete_root_lineage_verified':False,'retained_contexts':contexts},
            'quality':{'status':'complete_within_age_ceilings' if all(m['value'] is not None for m in measurements.values()) else 'partial',
                'declared_series':len(SERIES),'available_histories':sum(bool(v) for v in histories.values()),'within_age_ceiling':sum(m['value'] is not None for m in measurements.values()),
                'release_calendar_verified':False,'historical_first_availability_verified':False,'independent_investment_votes':0},
            'activity_index':None,'activity_z':None,'regime':None,'momentum':None,'call':None,'portfolio_action':'WAIT',
            'divergence':{'available':False,'gap':None,'reason':'No calibrated comparison between the legacy 0-100 activity index and standardized monthly macro indicator.'},
            'qualification':{'gdp_forecast':False,'economic_regime':False,'leading_indicator_edge':False,'portfolio_recommendation':False},
            'methodology':{'weekly_reference':'Saturday grid. Friday financial indices keep their original Friday date; daily spread endpoints keep actual dates and maximum four-day lag.',
                'standardization':'156 prior calendar weeks with at least 125 numeric values, or 730 prior calendar days with at least 400 numeric values. Target excluded; full returned span required; sample SD; zero variance unavailable.',
                'changes':'Exact 1/4/13-calendar-week baselines for weekly data; explicitly lagged daily endpoints. Never replace missing values or shift to earlier numeric rows.',
                'legacy':'Whole predecessor, source context and legacy snapshots retained. Legacy regime weights and 0-100 rescaling are not an independently calibrated activity forecast.',
                'limitations':'Current-vintage descriptive research; reference period is not release time. No identified causal GDP contribution, return forecast, sizing or execution authority.'},**PERMISSIONS}
