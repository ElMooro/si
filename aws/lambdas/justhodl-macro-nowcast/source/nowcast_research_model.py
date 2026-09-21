"""Reproducible current-vintage nowcast measurements, not investment-clock forecasts.

The compilation clock and every input are explicit. No provider, account, AI,
notification or portfolio access. Missing calendar observations stay missing.
"""
from copy import deepcopy
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
from urllib.parse import urlsplit,parse_qs
import hashlib,json,math,re
import report_observations
from research_brief_model import clock,row_status,AGE_LIMITS
from nowcast_research_catalog import SPECS,SERIES,WEIGHTS,YOY

CONTRACT='nowcast-native-research.v1';PREFIX='data/nowcast-research/'
CURRENT='data/macro-nowcast.json';PRIVATE='audit-private/20260909-originals/nowcast-research/'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
WINDOW=60;MIN_WINDOW=48


def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def shown(value):
    if value is None:return None
    out=float(round(value,8))
    if not math.isfinite(out):raise ValueError('Nonfinite derived number')
    return out


def month(day,offset=0):
    d=date.fromisoformat(day)
    if d.day!=1:raise ValueError('Monthly observation must identify its reference month')
    n=d.year*12+d.month-1+offset
    return str(date(n//12,n%12+1,1))


def history(original,through,frequency):
    if original is None:return []
    seen=set();out=[]
    for i,row in enumerate(original['observations']['observations']):
        day=date.fromisoformat(row['date'])
        if str(day)!=row['date'] or day in seen:raise ValueError('Duplicate or noncanonical observation date')
        seen.add(day)
        if frequency=='M':month(str(day))
        value=report_observations.decimal(row.get('value'))
        if row.get('value') not in (None,'.','') and value is None:raise ValueError('Invalid source number')
        if str(day)<=through:out.append({'date':str(day),'value':value,'original_row_index':i})
    return sorted(out,key=lambda r:r['date'])


def observed(packet,originals,generated_at):
    at=clock(generated_at);source=clock(packet['generated_at'])
    if source>at:raise ValueError('Future canonical publication')
    measurements={};histories={}
    for sid in SERIES:
        m=packet.get('measurements',{}).get(sid) or {};original=originals.get(sid)
        if m and not original:raise ValueError('Reconstructed source originals required')
        label,unit,freq,adj=SPECS[sid]
        if m and (m.get('series_id')!=sid or m.get('unit')!=unit or m.get('frequency')!=freq
                  or m.get('definition',{}).get('seasonal_adjustment_short')!=adj):raise ValueError('Source definition differs: '+sid)
        status=row_status(m,at,(at-source).total_seconds());day=m.get('date')
        h=history(original,str(at.date()),freq) if original else [];histories[sid]=h
        for row in h:
            value=row['value']
            if value is not None and ((sid!='T10Y2Y' and value<0)
                or (sid in ('INDPRO','PAYEMS','RSAFS','RRSFS','SP500','UMCSENT') and value<=0)
                or (sid in ('UNRATE',) and value>100)):
                raise ValueError('Value outside source domain: '+sid)
        value=report_observations.decimal(m.get('current_decimal')) if status=='fresh' else None
        due=[source+timedelta(hours=26)]
        if m.get('acquired_at'):due.append(clock(m['acquired_at'])+timedelta(hours=26))
        if day:due.append(datetime.combine(date.fromisoformat(day)+timedelta(days=AGE_LIMITS[freq]+1),time.min,timezone.utc))
        measurements[sid]={'series_id':sid,'label':label,'source_url':'https://fred.stlouisfed.org/series/'+sid,
            'value':shown(value),'exact_value':str(value) if value is not None else None,'unit':unit,'frequency':freq,
            'seasonal_adjustment':adj,'observation_date':day,'current_row_index':m.get('current_row_index'),
            'acquired_at':m.get('acquired_at'),'source_generated_at':packet['generated_at'],'source_valid_until':min(due).isoformat(),
            'evaluated_at':generated_at,'definition':deepcopy(m.get('definition')),'evidence':deepcopy(m.get('evidence',{})),
            'quality':{'status':'within_age_ceiling' if value is not None else status,'release_calendar_verified':False},
            'history_coverage':{'returned_rows':len(h),'numeric_rows':sum(r['value'] is not None for r in h),
                'first_date':h[0]['date'] if h else None,'last_date':h[-1]['date'] if h else None,
                'current_vintage_only':True,'historical_first_availability_verified':False},**PERMISSIONS}
    return measurements,histories


def point(row):
    return {'date':row['date'],'exact_value':str(row['value']) if row['value'] is not None else None,
        'value':shown(row['value']),'original_row_index':row['original_row_index']}


def monthly_yoy(rows):
    by={r['date']:r for r in rows};out={}
    for day,row in by.items():
        baseline=by.get(month(day,-12));value=None
        if row['value'] is not None and baseline and baseline['value'] is not None and baseline['value']>0:
            value=100*(row['value']/baseline['value']-1)
        out[day]={'date':day,'value':value,'current':row,'baseline':baseline,'baseline_date':month(day,-12)}
    return out


def monthly_inputs(rows,sid,generated_at):
    """Build closed reference months without pretending daily dates are monthly prints."""
    current_month=clock(generated_at).date().replace(day=1).isoformat();out={}
    if SPECS[sid][2]=='M':
        for row in rows:
            if row['date']>=current_month:continue
            out[row['date']]={'reference_month':row['date'],'value':row['value'],
                'aggregation':'native_monthly_reference','members':[point(row)],'missing_rows':[],
                'numeric_rows':int(row['value'] is not None),'returned_rows':1,'span_covered':True}
        return out
    groups={}
    for row in rows:
        d=row['date'][:7]+'-01'
        if d<current_month:groups.setdefault(d,[]).append(row)
    for day,group in sorted(groups.items()):
        last=(date.fromisoformat(month(day,1))-timedelta(days=1)).isoformat()
        span=bool(rows and rows[0]['date']<=day and rows[-1]['date']>=last)
        numeric=[r for r in group if r['value'] is not None]
        lag=(date.fromisoformat(last)-date.fromisoformat(numeric[-1]['date'])).days if numeric else None
        available=span and len(numeric)>=15 and (sid!='SP500' or lag<=4)
        value=(numeric[-1]['value'] if sid=='SP500' else sum(r['value'] for r in numeric)/len(numeric)) if available else None
        out[day]={'reference_month':day,'value':value,'aggregation':'last_numeric_daily_observation' if sid=='SP500' else 'mean_of_numeric_daily_observations',
            'members':[point(r) for r in group],'numeric_rows':len(numeric),'returned_rows':len(group),
            'missing_rows':[point(r) for r in group if r['value'] is None],'span_covered':span,
            'minimum_numeric_rows':15,'month_end':last,'last_numeric_date':numeric[-1]['date'] if numeric else None,
            'endpoint':point(numeric[-1]) if numeric else None,'endpoint_lag_calendar_days':lag,
            'calendar_completeness_verified':False,'status':'descriptive' if available else 'incomplete_returned_month'}
    return out


def monthly_point(row):
    if row is None:return None
    return {**row,'value':shown(row['value']),'exact_value':str(row['value']) if row['value'] is not None else None}


def transformed(months,sid):
    out={}
    for day,row in months.items():
        base=months.get(month(day,-12)) if sid in YOY else None
        value=row['value']
        if sid in YOY:value=100*(value/base['value']-1) if value is not None and base and base['value'] is not None and base['value']>0 else None
        out[day]={'value':value,'reference_month':day,'current':row,'baseline':base}
    return out


def component(series,sid,day,details=False):
    dates=[month(day,-i) for i in range(WINDOW,0,-1)];target=series.get(day)
    sample=[series[d] for d in dates if d in series and series[d]['value'] is not None]
    full=bool(series and min(series)<=month(dates[0],-12 if sid in YOY else 0))
    mean=sd=z=None
    if full and len(sample)>=MIN_WINDOW:
        values=[r['value'] for r in sample];mean=sum(values)/len(values)
        sd=(sum((v-mean)**2 for v in values)/Decimal(len(values)-1)).sqrt()
        if target and target['value'] is not None and sd:z=(target['value']-mean)/sd
    weight=Decimal(WEIGHTS[sid]);contribution=weight*z if z is not None else None
    out={'series_id':sid,'reference_month':day,'weight':shown(weight),'exact_weight':str(weight),
        'transform':'exact_calendar_yoy_percent' if sid in YOY else 'monthly_level',
        'transformed_unit':'percent_change' if sid in YOY else SPECS[sid][1],
        'transformed_value':shown(target['value']) if target else None,'z_score':shown(z),
        'exact_z_score':str(z) if z is not None else None,'weighted_contribution':shown(contribution),
        'exact_contribution':str(contribution) if contribution is not None else None,
        'mean':shown(mean),'sample_standard_deviation':shown(sd),'window_start':dates[0],'window_end':dates[-1],
        'numeric_months':len(sample),'requested_calendar_months':WINDOW,'minimum_numeric_months':MIN_WINDOW,
        'full_requested_span_returned':full,'excluded_months':[d for d in dates if d not in series or series[d]['value'] is None],
        'current':monthly_point(target['current']) if target else None,'baseline':monthly_point(target['baseline']) if target else None,
        'status':'descriptive' if z is not None else 'constant_window' if sd==0 else 'incomplete_calendar_history'}
    if details:out['sample_inputs']=[{'reference_month':r['reference_month'],'transformed_value':shown(r['value']),
        'current':monthly_point(r['current']),'baseline':monthly_point(r['baseline'])} for r in sample]
    return out


def index(months,measurements,generated_at):
    series={s:transformed(months[s],s) for s in WEIGHTS}
    latest=max((d for values in months.values() for d in values),default=None);trail=[]
    def one(day,details=False):
        comps={s:component(series[s],s,day,details) for s in WEIGHTS}
        valid=all(c['exact_contribution'] is not None for c in comps.values())
        value=sum(Decimal(c['exact_contribution']) for c in comps.values()) if valid else None
        return {'reference_month':day,'value':shown(value),'exact_value':str(value) if value is not None else None,
            'available':valid,'components':comps,'complete_components':sum(c['exact_contribution'] is not None for c in comps.values())}
    if latest:trail=[one(month(latest,-i)) for i in range(119,-1,-1)]
    complete=[row for row in trail if row['available']];last=complete[-1] if complete else None
    current=one(last['reference_month'],True) if last and all(measurements[s]['value'] is not None for s in WEIGHTS) and (clock(generated_at).date()-date.fromisoformat(last['reference_month'])).days<=100 else None
    return {'current':current,'latest_closed_reference':one(latest,True) if latest else None,'trail':trail,
        'latest_component_months':{s:max(months[s],default=None) for s in WEIGHTS},
        'formula':'For one common closed reference month, transform four level series using exact-calendar YoY. Standardize all seven against the preceding 60 calendar months excluding the target, sample standard deviation, at least 48 numeric months and a full returned span. Sum exact z-scores times the declared legacy weights; do not renormalize missing components.',
        'weight_origin':'Fixed predecessor judgmental weights, not estimated GDP coefficients or validated investment weights.',
        'unit':'weighted_standardized_indicator_index','meaning':'Descriptive relative macro indicator. Not GDP growth, a recession probability, an identified economic regime or an investment signal.',
        'historical_first_availability_verified':False,**PERMISSIONS}


def price_outcomes(months,trail):
    horizons=(1,3,6,12);rows=[];pairs={}
    def endpoint(row):
        if row is None:return None
        return {k:v for k,v in monthly_point(row).items() if k not in ('members','missing_rows')}
    for origin in trail:
        day=origin['reference_month'];start=months.get(day)
        for horizon in horizons:
            target=month(day,horizon);end=months.get(target)
            available=bool(start and end and start['value'] is not None and end['value'] is not None)
            value=100*(end['value']/start['value']-1) if available else None
            rows.append({'reference_month':day,'horizon_calendar_months':horizon,'target_reference_month':target,
                'start':endpoint(start),'end':endpoint(end),'price_return_percent':shown(value),'available':available,
                'contemporaneous_descriptive_index':origin['value'],'macro_available_at_entry_verified':False})
    for horizon in horizons:
        sample=[r for r in rows if r['horizon_calendar_months']==horizon and r['available']]
        pairs[str(horizon)]=[{'first_origin':a['reference_month'],'second_origin':b['reference_month']}
            for i,a in enumerate(sample) for b in sample[i+1:] if b['start']['endpoint']['date']<a['end']['endpoint']['date']]
    return {'series_id':'SP500','instrument':'S&P 500 price index','horizons_calendar_months':list(horizons),
        'rows':rows,'overlapping_pairs':pairs,'dividends_included':False,'effective_independent_sample_size':None,
        'hit_rate':None,'qualified_forecast_samples':0,
        'meaning':'Exact subsequent calendar-month price observations alongside revised macro measurements. The macro observation month is not the time its data became available. These are retrospective outcomes, not an executable backtest, forecast probability or net return.',**PERMISSIONS}


def retail(months):
    ids=('RSAFS','RRSFS');trans={s:transformed(months[s],s) for s in ids}
    latest=max((d for s in ids for d in months[s]),default=None);rows=[]
    if latest:
        for i in range(23,-1,-1):
            day=month(latest,-i);items={}
            for sid in ids:
                r=trans[sid].get(day);items[sid]={'yoy_percent':shown(r['value']) if r else None,
                    'current':monthly_point(r['current']) if r else None,'baseline':monthly_point(r['baseline']) if r else None}
            rows.append({'reference_month':day,'series':items})
    return {'rows':rows,'meaning':'RSAFS is nominal sales in millions of dollars. RRSFS is the published CPI-deflated series in millions of 1982-84 CPI-adjusted dollars, and may be absent. Nominal growth is not volume growth; no substitute deflator or inferred real value.',
        'official_definition':'https://fred.stlouisfed.org/series/RRSFS','roots':['RSAFS','CPIAUCSL'],**PERMISSIONS}


def build(packet,originals,generated_at):
    with localcontext() as arithmetic:
        arithmetic.prec=28;arithmetic.rounding=ROUND_HALF_EVEN
        measurements,histories=observed(packet,originals,generated_at)
        months={s:monthly_inputs(histories[s],s,generated_at) for s in SERIES}
        research=index(months,measurements,generated_at)
        return {'contract':CONTRACT,'generated_at':generated_at,'source_generated_at':packet['generated_at'],
            'source_valid_until':min(m['source_valid_until'] for m in measurements.values()),'canonical_replay':packet['replay'],
            'measurements':measurements,'research_index':research,'price_outcomes':price_outcomes(months['SP500'],research['trail']),
            'nominal_real_retail':retail(months),'call':None,'score':None,'normalized_score':None,'raw_score':None,
            'regime':None,'confidence':None,'regime_color':None,'portfolio_action':'WAIT',**PERMISSIONS,
            'components':[],'historical_scores':[],'regime_spy_performance':{},
            'global_confidence':{'status':'PRESERVED_UNVALIDATED_PREDECESSOR_CONTEXT','composite_z':None,'composite_n':0,'series':{},'score_eligible':False},
            'quality':{'status':'complete_descriptive_sources' if all(originals.get(s) is not None for s in SERIES) else 'partial',
                'available_histories':sum(originals.get(s) is not None for s in SERIES),'declared_series':len(SERIES),
                'within_age_ceiling':sum(m['value'] is not None for m in measurements.values()),'release_calendar_verified':False,
                'historical_first_availability_verified':False,'independent_investment_votes':0},
            'qualification':{'gdp_nowcast_target':None,'gdp_forecast':None,'recession_probability':None,'forecast_errors':None,
                'remaining':['Historical release vintages and first availability','A specified GDP target and horizon','Rolling out-of-sample evaluation with benchmark and costs','Independent predictive information and portfolio suitability'],
                'note':'Complete predecessor source and packet retained. No paid AI, notification or portfolio action is produced.'}}
