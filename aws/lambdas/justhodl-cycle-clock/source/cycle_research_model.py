"""Reproducible current-vintage cycle measurements, not investment-clock forecasts.

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
from cycle_research_catalog import SPECS,SERIES,DEPENDENCIES

CONTRACT='cycle-native-research.v1';PREFIX='data/cycle-research/'
CURRENT='data/cycle-clock.json';PRIVATE='audit-private/20260909-originals/cycle-research/'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
AXES={'growth':('INDPRO','PAYEMS'),'inflation':('CPIAUCSL','PCEPILFE')}
WINDOW=120;MIN_WINDOW=96


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
        h=history(original,day,freq) if day else [];histories[sid]=h
        for row in h:
            value=row['value']
            if value is not None and ((sid not in ('SAHMREALTIME','SAHMCURRENT') and value<0)
                or (sid in ('INDPRO','PAYEMS','CPIAUCSL','PCEPILFE','SP500') and value<=0)
                or (sid in ('UNRATE','MCUMFN') and value>100)):
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


def coordinate_component(yoy,day):
    target=yoy.get(day);dates=[month(day,-i) for i in range(WINDOW-1,-1,-1)]
    sample=[yoy[d] for d in dates if d in yoy and yoy[d]['value'] is not None]
    full_span=bool(yoy and min(yoy)<=month(dates[0],-12))
    z=None;mean=None;sd=None
    if full_span and len(sample)>=MIN_WINDOW:
        values=[r['value'] for r in sample];mean=sum(values)/len(values)
        sd=(sum((v-mean)**2 for v in values)/Decimal(len(values))).sqrt()
        if target and target['value'] is not None and sd:z=(target['value']-mean)/sd
    return {'series_id':None,'reference_month':day,'yoy_percent':shown(target['value']) if target else None,
        'z_score':shown(z),'exact_z_score':str(z) if z is not None else None,'mean_yoy_percent':shown(mean),'population_sd_pp':shown(sd),
        'window_start':dates[0],'window_end':day,'requested_calendar_months':WINDOW,'minimum_numeric_months':MIN_WINDOW,
        'numeric_months':len(sample),'full_requested_span_returned':full_span,
        'excluded_months':[d for d in dates if d not in yoy or yoy[d]['value'] is None],
        'current':point(target['current']) if target else None,'baseline':point(target['baseline']) if target and target['baseline'] else None,
        'sample_inputs':[{'reference_month':r['date'],'yoy_percent':shown(r['value']),
            'current_row_index':r['current']['original_row_index'],'baseline_row_index':r['baseline']['original_row_index']} for r in sample],
        'status':'descriptive' if z is not None else 'constant_window' if sd==0 else 'insufficient_exact_calendar_history'}


def coordinates(measurements,histories):
    ids=tuple(s for pair in AXES.values() for s in pair);yoys={s:monthly_yoy(histories[s]) for s in ids}
    latest=max((m['observation_date'] for s,m in measurements.items() if s in ids and m['observation_date']),default=None)
    trail=[]
    if latest:
        for i in range(83,-1,-1):
            day=month(latest,-i);components={}
            for sid in ids:
                c=coordinate_component(yoys[sid],day);c['series_id']=sid;components[sid]=c
            axes={a:shown(sum(Decimal(components[s]['exact_z_score']) for s in pair)/2)
                if all(components[s]['z_score'] is not None for s in pair) else None for a,pair in AXES.items()}
            trail.append({'reference_month':day,'growth_relative_z':axes['growth'],'inflation_relative_z':axes['inflation'],
                'components':components,'available':all(v is not None for v in axes.values())})
    available=[t for t in trail if t['available']];latest_complete=deepcopy(available[-1]) if available else None
    age=(clock(next(iter(measurements.values()))['evaluated_at']).date()-date.fromisoformat(latest_complete['reference_month'])).days if latest_complete else None
    usable=latest_complete is not None and age<=100 and all(measurements[s]['value'] is not None for s in ids)
    current=deepcopy(latest_complete) if usable else None
    if current:
        current['growth_relative_label']='above its trailing reference mean' if current['growth_relative_z']>0 else 'below its trailing reference mean' if current['growth_relative_z']<0 else 'at its trailing reference mean'
        current['inflation_relative_label']='above its trailing reference mean' if current['inflation_relative_z']>0 else 'below its trailing reference mean' if current['inflation_relative_z']<0 else 'at its trailing reference mean'
    for entry in trail:
        for component in entry['components'].values():component.pop('sample_inputs',None)
    return {'current':current,'last_complete_historical':latest_complete,'trail':trail,
        'latest_component_months':{s:measurements[s]['observation_date'] for s in ids},'current_reference_age_days':age,
        'roots':list(ids),'formula':'Exact same-month YoY: 100*(level(t)/level(t-12 months)-1). Standardize each against its prior 120 calendar months inclusive, population SD; minimum 96 valid YoY pairs with full returned span. Equally average two standardized components per axis.',
        'meaning':'Relative current-vintage year-over-year rates, not growth acceleration, a certified business-cycle phase, recession odds or asset leadership. Missing reference months are excluded and counted, never shifted or interpolated.',
        'historical_first_availability_verified':False,'qualified_forecast_samples':0,**PERMISSIONS}


def sahm(measurements,histories):
    sid='UNRATE';day=measurements[sid]['observation_date'];by={r['date']:r for r in histories[sid]}
    windows=[]
    if day:
        for i in range(12,-1,-1):
            end=month(day,-i);dates=[month(end,-j) for j in (2,1,0)];rows=[by.get(d) for d in dates]
            v=sum(r['value'] for r in rows)/3 if all(r and r['value'] is not None for r in rows) else None
            windows.append({'end_month':end,'mean':v,'inputs':[point(r) for r in rows if r],
                'missing_months':[d for d,r in zip(dates,rows) if not r or r['value'] is None]})
    valid=len(windows)==13 and all(w['mean'] is not None for w in windows) and measurements[sid]['value'] is not None
    baseline=min((w['mean'] for w in windows[:-1]),default=None) if valid else None
    value=windows[-1]['mean']-baseline if valid else None
    return {'label':'Reconstruction from currently retained unemployment history','value':shown(value),'unit':'percentage_points',
        'observation_date':day,'available':valid,'current_three_month_mean_percent':shown(windows[-1]['mean']) if windows else None,
        'minimum_previous_twelve_three_month_means_percent':shown(baseline),
        'baseline_end_months':[w['end_month'] for w in windows[:-1] if valid and w['mean']==baseline],
        'windows':[{**w,'mean':shown(w['mean'])} for w in windows],
        'formula':'Mean U3(t,t-1,t-2) minus minimum of the twelve three-month means ending t-12 through t-1; exact consecutive reference months.',
        'threshold_pp':0.5,'threshold_met':value>=Decimal('0.5') if value is not None else None,
        'official_realtime_series':deepcopy(measurements['SAHMREALTIME']),'official_current_vintage_series':deepcopy(measurements['SAHMCURRENT']),
        'meaning':'A descriptive unemployment rule. The reconstruction uses the current UNRATE vintage, not the unemployment data available in each past month. The official real-time series can differ. Threshold crossing does not establish a recession probability or portfolio trade.',
        'roots':['UNRATE','SAHMREALTIME','SAHMCURRENT'],**PERMISSIONS}


def liquidity_point(histories,day):
    inputs={};missing=[]
    for sid,lag in (('WALCL',0),('WTREGEN',0),('RRPONTSYD',7)):
        candidates=[r for r in histories[sid] if r['date']<=day]
        row=candidates[-1] if candidates else None
        age=(date.fromisoformat(day)-date.fromisoformat(row['date'])).days if row else None
        if not row or row['value'] is None or age>lag:missing.append(sid)
        inputs[sid]={'observation':point(row) if row else None,'backward_join_lag_days':age,'maximum_join_lag_days':lag}
    v=None;legs={}
    if not missing:
        for sid,scale,sign in (('WALCL',1000,1),('WTREGEN',1000,-1),('RRPONTSYD',1,-1)):
            legs[sid]=Decimal(inputs[sid]['observation']['exact_value'])/scale*sign
        v=sum(legs.values())
    return {'reference_date':day,'available':v is not None,'value':shown(v),'unit':'USD_bn',
        'signed_legs_usd_bn':{s:shown(v) for s,v in legs.items()},'inputs':inputs,'missing_or_unmatched':missing}


def liquidity(measurements,histories):
    roots=('WALCL','WTREGEN','RRPONTSYD');days=[r['date'] for r in histories['WALCL']][-104:]
    series=[liquidity_point(histories,d) for d in days]
    current=series[-1] if series and all(measurements[s]['value'] is not None for s in roots) else None
    baseline_day=str(date.fromisoformat(current['reference_date'])-timedelta(days=91)) if current else None
    baseline=liquidity_point(histories,baseline_day) if baseline_day else None
    ok=bool(current and current['available'] and baseline and baseline['available'])
    changes={s:shown(Decimal(str(current['signed_legs_usd_bn'][s]))-Decimal(str(baseline['signed_legs_usd_bn'][s]))) for s in roots} if ok else {}
    return {'current':current,'baseline_13_weeks':baseline,'change_13_weeks_usd_bn':shown(sum(Decimal(str(v)) for v in changes.values())) if ok else None,
        'signed_leg_changes_usd_bn':changes,'series':series,'roots':list(roots),
        'formula':'WALCL/1000 - WTREGEN/1000 - RRPONTSYD, USD billions; change compares exactly 91 calendar days.',
        'temporal_basis':'WALCL is a Wednesday level; WTREGEN is a week average with matching reported date; RRP is the latest reported date on or before that date, maximum 7 calendar days. These are mixed period measures, not synchronized stocks.',
        'meaning':'A named balance-sheet proxy, not total market liquidity, reserves, money available for equities, easing or a portfolio forecast. No missing leg is zero; a future first observation is never carried backward.',**PERMISSIONS}


def calendar_change(measurements,histories,sid,months=None,days=None,difference=False):
    row=measurements[sid];day=row['observation_date'];baseline=None
    if day:baseline=month(day,-months) if months else str(date.fromisoformat(day)-timedelta(days=days))
    past=next((r for r in histories[sid] if r['date']==baseline),None);current=report_observations.decimal(row['exact_value'])
    old=past['value'] if past else None;value=None
    if current is not None and old is not None and (difference or old>0):value=current-old if difference else 100*(current/old-1)
    return {'series_id':sid,'available':value is not None,'value':shown(value),'unit':'percentage_points' if difference else 'percent_change',
        'observation_date':day,'current_original_row_index':row['current_row_index'],'baseline_date':baseline,
        'baseline':point(past) if past else None,'current_exact_value':row['exact_value'],
        'requested_calendar_months':months,'requested_calendar_days':days,'roots':[sid],
        'formula':'current minus exact baseline' if difference else '100*(current/exact baseline-1)',**PERMISSIONS}


def declared_roots(doc,known):
    """Inventory identifiers only. This does not verify an upstream transformation."""
    found=set();pending=[doc];visited=0
    while pending:
        value=pending.pop();visited+=1
        if visited>600000:raise ValueError('Dependency inventory exceeds reviewed bound')
        if isinstance(value,dict):
            sid=value.get('series_id')
            if isinstance(sid,str) and sid in known:found.add(sid)
            u=value.get('source_url')
            if isinstance(u,str):
                parsed=urlsplit(u)
                if parsed.scheme=='https' and parsed.netloc in ('fred.stlouisfed.org','api.stlouisfed.org'):
                    ids=parse_qs(parsed.query).get('series_id',[]) if parsed.netloc=='api.stlouisfed.org' else [parsed.path.rsplit('/',1)[-1]]
                    found.update(s for s in ids if s in known)
            pending.extend(v for v in value.values() if isinstance(v,(dict,list)))
        elif isinstance(value,list):pending.extend(v for v in value if isinstance(v,(dict,list)))
    return sorted(found)


def dependencies(packets,refs,canonical,generated_at):
    if set(packets)!=set(DEPENDENCIES) or set(refs)!=set(DEPENDENCIES):raise ValueError('Exact predecessor dependency inventory required')
    nodes=[];roots={};at=clock(generated_at)
    for key,label in DEPENDENCIES.items():
        p=packets[key];ref=refs[key];is_object=isinstance(p,dict)
        stamp=p.get('generated_at') if is_object else None;age=None;clock_state='not_declared'
        if stamp:
            try:
                age=(at-clock(stamp)).total_seconds()/3600;clock_state='future_invalid' if age<0 else 'publication_clock_only'
            except (ValueError,TypeError,AttributeError):clock_state='invalid'
        declared=declared_roots(p,canonical.get('measurements',{})) if p else []
        for sid in declared:roots.setdefault(sid,[]).append(key)
        nodes.append({'id':label,'key':key,'readable':is_object,'retained_source':deepcopy(ref),
            'contract':p.get('contract') if is_object else None,'source_generated_at':stamp,
            'publication_age_hours':shown(age),'clock_status':clock_state,'declared_fred_roots':declared,
            'upstream_transformations_replayed_here':False,'qualification':'retained_context_only',
            'additional_independent_votes':0,**PERMISSIONS})
    return {'inputs':nodes,'declared_inputs':len(DEPENDENCIES),'retained_inputs':sum(n['readable'] for n in nodes),
        'root_overlap':[{'series_id':s,'input_keys':keys,'input_count':len(keys)} for s,keys in sorted(roots.items()) if len(keys)>1],
        'unique_declared_fred_roots':len(roots),'verified_core_roots':[s for s in SERIES if s in canonical.get('measurements',{})],'independent_investment_votes':0,
        'interpretation':'Shared explicitly declared FRED identities expose possible repeated evidence. Root discovery is incomplete for legacy packets and does not prove source independence, upstream calculation validity or synchronized vintages. Publication age is not observation freshness.',
        'edges':[{'from':n['key'],'to':CURRENT,'role':'context_not_vote'} for n in nodes]}


def build(packet,originals,packets,refs,generated_at):
    with localcontext() as arithmetic:
        arithmetic.prec=28;arithmetic.rounding=ROUND_HALF_EVEN
        measurements,histories=observed(packet,originals,generated_at)
        coordinate=coordinates(measurements,histories);labor=sahm(measurements,histories);liq=liquidity(measurements,histories)
        graph=dependencies(packets,refs,packet,generated_at)
        changes={s:calendar_change(measurements,histories,s,months=3 if s=='JPNASSETS' else None,days=91 if s!='JPNASSETS' else None)
            for s in ('WALCL','ECBASSETSW','JPNASSETS')}
        capacity=calendar_change(measurements,histories,'MCUMFN',months=12,difference=True)
        return {'contract':CONTRACT,'engine':'cycle-clock','version':'4.0.0','generated_at':generated_at,'source_generated_at':packet['generated_at'],
            'source_valid_until':min(m['source_valid_until'] for m in measurements.values()),'canonical_replay':deepcopy(packet['replay']),
            'quality':{'status':'partial' if any(m['value'] is None for m in measurements.values()) else 'descriptive',
                'declared_series':len(SERIES),'available_histories':sum(bool(h) for h in histories.values()),
                'within_age_ceiling':sum(m['value'] is not None for m in measurements.values()),'retained_engine_inputs':graph['retained_inputs'],
                'independent_investment_votes':0,'historical_first_availability_verified':False,'release_calendar_verified':False},
            'measurements':measurements,'coordinates':coordinate,'unemployment_rule':labor,'net_liquidity_proxy':liq,
            'central_bank_changes':changes,'capacity_change_12_months':capacity,'dependency_graph':graph,
            'cycle':{'phase':None,'headline_phase':None,'quadrant':None,'recession_prob_pct':None,'confidence':None,'asset_leadership':None},
            'risk':{'squeeze_risk':None,'level':None},'synthesis':{'posture':'WAIT','score':None,'bottom_line':'Dated descriptive research; investment direction and size are unqualified.'},
            'verdict':None,'ai':None,'call':None,'portfolio_action':'WAIT','score':None,'regime':None,
            'track_record':{'qualified_samples':0,'hit_rate':None,'status':'unqualified','reason':'Current-vintage retrospective coordinates and overlapping posture outcomes do not establish prospective, cost-adjusted forecast performance.'},
            'legacy':{'whole_source_preserved':True,'history_key':'data/cycle-clock-history.json','history_modified':False,
                'all_declared_inputs_retained':True,'unretained_external_price_and_CFTC_reads_not_substituted':True},
            'portfolio_consequences':{'mode':'user_entered_scenarios_only','equity_formula':'signed USD equity exposure * entered price return percent / 100',
                'bond_formula':'-signed DV01 USD per basis point * entered yield change basis points','recommended_positions':[],
                'excluded':['convexity','nonparallel yield curves','credit spread','dividends','carry','FX','fees','funding']},
            'qualification_gaps':['Historical release vintages and availability times','Independent predictive information beyond shared inputs',
                'Prospective out-of-sample forecasts after costs','Complete asset-price originals and total returns','Account constraints and portfolio risk model'],**PERMISSIONS}
