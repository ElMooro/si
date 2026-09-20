"""Source-defined rates, coincident model estimates and volatility-index research.

No Treasury-yield proxy for Fed meeting probabilities, no relabelled recession
forecast, no ETF/index substitution and no inferred physical return distribution.
"""
from copy import deepcopy
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,math
import report_observations
from research_brief_model import clock,row_status,AGE_LIMITS
from implied_research_catalog import SERIES
import tail_research
CONTRACT='implied-native-research.v1';PREFIX='data/implied-research/';CURRENT='data/implied-prob.json'
PRIVATE='audit-private/20260909-originals/implied-research/'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
SPECS={
 'DFEDTARU':('Federal funds target range upper limit','Percent','D','NSA'),
 'DFEDTARL':('Federal funds target range lower limit','Percent','D','NSA'),
 'FEDFUNDS':('Federal funds effective rate, monthly average','Percent','M','NSA'),
 'DFF':('Federal funds effective rate, daily','Percent','D','NSA'),
 'DGS1MO':('US Treasury one-month constant-maturity yield, investment basis','Percent','D','NSA'),
 'DGS3MO':('US Treasury three-month constant-maturity yield, investment basis','Percent','D','NSA'),
 'DGS6MO':('US Treasury six-month constant-maturity yield, investment basis','Percent','D','NSA'),
 'DGS1':('US Treasury one-year constant-maturity yield, investment basis','Percent','D','NSA'),
 'RECPROUSM156N':('Chauvet/Piger smoothed US recession probability, coincident model','Percent','M','NSA'),
 'T10Y3M':('US ten-year minus three-month Treasury yield spread','Percent','D','NSA'),
 'BAMLH0A0HYM2':('ICE BofA US high-yield index option-adjusted spread','Percent','D','NSA'),
 'VIXCLS':('Cboe VIX: S&P 500 index options, 30 days','Index','D','NSA'),
 'VXVCLS':('Cboe S&P 500 three-month volatility index','Index','D','NSA'),
 'VXNCLS':('Cboe Nasdaq-100 volatility index, 30 days','Index','D','NSA'),
 'RVXCLS':('Cboe Russell 2000 volatility index, 30 days','Index','D','NSA'),
 'VXDCLS':('Cboe DJIA volatility index, 30 days','Index','D','NSA'),
 'GVZCLS':('Cboe gold ETF volatility index: GLD options, 30 days','Index','D','NSA'),
 'OVXCLS':('Cboe crude oil ETF volatility index: USO options, 30 days','Index','D','NSA')}
VOLATILITY={'VIXCLS':('SPX',30),'VXVCLS':('SPX',90),'VXNCLS':('NDX',30),'RVXCLS':('RUT',30),
            'VXDCLS':('DJIA',30),'GVZCLS':('GLD',30),'OVXCLS':('USO',30)}
UNQUALIFIED={}

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def shown(value):
    if value is None:return None
    result=float(round(value,8))
    if not math.isfinite(result):raise ValueError('Nonfinite derived value')
    return result

def history(original,through):
    if original is None:return []
    out=[];seen=set()
    for i,row in enumerate(original['observations']['observations']):
        day=date.fromisoformat(row['date'])
        if str(day)!=row['date'] or day in seen:raise ValueError('Duplicate or noncanonical source date')
        seen.add(day)
        value=report_observations.decimal(row.get('value'))
        if row.get('value') not in (None,'.','') and value is None:raise ValueError('Invalid source number')
        if day>date.fromisoformat(through):continue
        out.append({'date':str(day),'value':value,'original_row_index':i})
    return sorted(out,key=lambda r:r['date'])

def statistics(rows,current,n):
    sample=[r for r in rows if r['value'] is not None][-n:]
    first=sample[0]['date'] if sample else None;last=sample[-1]['date'] if sample else None
    out={'requested_observations':n,'numeric_observations':len(sample),'first_date':first,'last_date':last,
        'missing_rows_inside_span':sum(r['value'] is None and first<=r['date']<=last for r in rows) if sample else 0,
        'mean':None,'sample_sd':None,'difference_from_mean':None,'relative_difference_percent':None,
        'midrank_percentile':None,'z_score':None,'status':'insufficient_history',
        'scope':'Last N numeric observations inclusive of the current dated observation; actual dates, not N trading days. Current vintage, not fair value, a probability or a backtest.',**PERMISSIONS}
    if len(sample)!=n or n<2:return out
    values=[r['value'] for r in sample];mean=sum(values)/n;sd=(sum((v-mean)**2 for v in values)/Decimal(n-1)).sqrt()
    out.update(mean=shown(mean),sample_sd=shown(sd),status='historical_only' if current is None else 'constant_window' if sd==0 else 'available',
        original_row_indices=[r['original_row_index'] for r in sample])
    if current is not None:
        diff=current-mean
        out.update(difference_from_mean=shown(diff),relative_difference_percent=shown(100*diff/mean) if mean>0 else None,
            midrank_percentile=shown(100*(Decimal(sum(v<current for v in values))+Decimal('0.5')*sum(v==current for v in values))/n),
            z_score=shown(diff/sd) if sd else None)
    return out

def observed(packet,originals,generated_at):
    at=clock(generated_at);source=clock(packet['generated_at'])
    if source>at:raise ValueError('Future canonical publication')
    rows={};histories={}
    for sid in SERIES:
        m=packet.get('measurements',{}).get(sid) or {};original=originals.get(sid)
        if m and not original:raise ValueError('Pinned original reconstruction required')
        if sid in UNQUALIFIED:
            rows[sid]={'series_id':sid,'label':sid,'value':None,'exact_value':None,'unit':None,'frequency':None,
                'observation_date':None,'quality':{'status':'source_unqualified'},'original_available':original is not None,
                **deepcopy(UNQUALIFIED[sid]),**PERMISSIONS};histories[sid]=[];continue
        label,unit,freq,adj=SPECS[sid]
        if m and (m.get('series_id')!=sid or m.get('unit')!=unit or m.get('frequency')!=freq
                  or m.get('definition',{}).get('seasonal_adjustment_short')!=adj):raise ValueError('Source definition differs: '+sid)
        status=row_status(m,at,(at-source).total_seconds());day=m.get('date')
        h=history(original,day) if day else [];histories[sid]=h
        value=report_observations.decimal(m.get('current_decimal')) if status=='fresh' else None
        due=[source+timedelta(hours=26)]
        if m.get('acquired_at'):due.append(clock(m['acquired_at'])+timedelta(hours=26))
        if day:due.append(datetime.combine(date.fromisoformat(day)+timedelta(days=AGE_LIMITS[freq]+1),time.min,timezone.utc))
        # Zero rates and negative yield spreads remain valid observations.
        if value is not None and ((sid in VOLATILITY and value<0) or (sid=='RECPROUSM156N' and not 0<=value<=100)):
            raise ValueError('Observation outside the defined index or probability range')
        n={'D':252,'W':52,'M':60,'Q':40}[freq]
        rows[sid]={'series_id':sid,'label':label,'source_url':'https://fred.stlouisfed.org/series/'+sid,
            'value':float(value) if value is not None else None,'exact_value':str(value) if value is not None else None,
            'unit':unit,'source_unit':m.get('unit'),'frequency':freq,'seasonal_adjustment':adj,'observation_date':day,
            'current_row_index':m.get('current_row_index'),'source_generated_at':packet['generated_at'],
            'acquired_at':m.get('acquired_at'),'source_valid_until':min(due).isoformat(),
            'definition':deepcopy(m.get('definition')),'evidence':deepcopy(m.get('evidence',{})),
            'evaluated_at':generated_at,
            'quality':{'status':'within_age_ceiling' if value is not None else status,'release_calendar_verified':False},
            'history_coverage':{'returned_rows':len(h),'numeric_rows':sum(r['value'] is not None for r in h),
                'first_date':h[0]['date'] if h else None,'last_date':h[-1]['date'] if h else None,
                'current_vintage_only':True,'point_in_time_history':False},
            'statistics':statistics(h,value,n),'changes':deepcopy(m.get('changes',{})) if value is not None else {},**PERMISSIONS}
    return rows,histories


def matched(rows,histories,ids):
    """Latest common actual observation; never interpolate a missing source date."""
    if any(rows[s]['quality']['status']!='within_age_ceiling' for s in ids):return None,{}
    by={s:{r['date']:r for r in histories[s] if r['value'] is not None} for s in ids}
    common=set.intersection(*(set(by[s]) for s in ids))
    if not common:return None,{}
    day=max(common)
    # The matched day must itself remain inside every source's observation age ceiling.
    if any((clock(rows[s]['evaluated_at']).date()-date.fromisoformat(day)).days>AGE_LIMITS[rows[s]['frequency']] for s in ids):return None,{}
    return day,{s:by[s][day] for s in ids}

def comparison(rows,histories,ids,label,formula,calculate,unit):
    day,values=matched(rows,histories,ids)
    result=calculate({s:r['value'] for s,r in values.items()}) if day else None
    return {'label':label,'available':result is not None,'value':shown(result),'unit':unit,'roots':list(ids),
        'observation_date':day,'formula':formula,
        'inputs':{s:{'date':day,'exact_value':str(r['value']),'original_row_index':r['original_row_index']} for s,r in values.items()},
        'latest_input_dates':{s:rows[s]['observation_date'] for s in ids},
        'status':'most_recent_common_reported_date' if day else 'matching_dated_observations_unavailable',
        'meaning':'Dated descriptive arithmetic. No intraday synchronization, futures quote or meeting probability.',**PERMISSIONS}

def target_midpoint(v):
    if v['DFEDTARL']>v['DFEDTARU']:raise ValueError('Target range bounds are crossed')
    return (v['DFEDTARL']+v['DFEDTARU'])/2

def recession_change(rows,histories):
    sid='RECPROUSM156N';m=rows[sid];day=date.fromisoformat(m['observation_date']) if m['observation_date'] else None
    if day and day.day!=1:raise ValueError('Monthly recession observation must identify its reference month')
    baseline=str(day.replace(year=day.year-1)) if day else None
    previous=next((r for r in histories[sid] if r['date']==baseline),None)
    value=previous['value'] if previous else None
    if value is not None and not 0<=value<=100:raise ValueError('Historical source probability outside range')
    ok=m['value'] is not None and value is not None
    return {'label':'Smoothed recession estimate change over twelve matched reference months','available':ok,
        'value':shown(Decimal(m['exact_value'])-value) if ok else None,'unit':'percentage_points',
        'observation_date':m['observation_date'],'baseline_date':baseline,'baseline_value':shown(value),
        'baseline_original_row_index':previous['original_row_index'] if previous else None,
        'current_original_row_index':m['current_row_index'],'roots':[sid],
        'formula':'Current model percent minus model percent for exactly twelve months earlier',
        'meaning':'Change in a revisable coincident model estimate; not a forecast for the next twelve months.',**PERMISSIONS}

def inversion(rows,histories):
    sid='T10Y3M';m=rows[sid];available=m['value'] is not None;streak=[];left=False
    if available:
        for r in reversed(histories[sid]):
            if r['value'] is None or r['value']>=0:break
            streak.append(r)
        else:left=bool(streak)
    return {'available':available,'inverted':m['value']<0 if available else None,
        'spread_basis_points':shown(Decimal(m['exact_value'])*100) if available else None,
        'unit':'basis_points','observation_date':m['observation_date'],
        'consecutive_reported_negative_observations':len(streak) if available else None,
        'first_negative_observation_in_window':streak[-1]['date'] if streak else None,'history_left_censored':left,
        'original_row_indices':[r['original_row_index'] for r in streak],
        'meaning':'Counts consecutive reported negative observations ending at this source date; missing reported values break the count. Not calendar days or a recession probability.',**PERMISSIONS}

def _build(packet,originals,legacy,generated_at):
    rows,histories=observed(packet,originals,generated_at)
    bounds=('DFEDTARL','DFEDTARU')
    derived={'target_midpoint':comparison(rows,histories,bounds,'Federal funds target range midpoint',
        '(DFEDTARL + DFEDTARU) / 2',target_midpoint,'percent')}
    for sid in ('DFF','DGS1MO','DGS3MO','DGS6MO','DGS1'):
        derived[sid.lower()+'_minus_target_midpoint']=comparison(rows,histories,(*bounds,sid),SPECS[sid][0]+' minus target midpoint',
            f'100 * ({sid} - (DFEDTARL + DFEDTARU) / 2)',lambda v,s=sid:100*(v[s]-target_midpoint(v)),'basis_points')
    derived['vix30_minus_vix3m']=comparison(rows,histories,('VIXCLS','VXVCLS'),'30-day minus three-month SPX volatility index',
        'VIXCLS - VXVCLS',lambda v:v['VIXCLS']-v['VXVCLS'],'index_points')
    derived['recession_estimate_12m_change']=recession_change(rows,histories)
    n=sum(m['quality']['status']=='within_age_ceiling' for m in rows.values())
    option_context=tail_research.context(legacy.get('data/tail-risk.json'),clock(generated_at))
    # The exact retained upstream output is context; original contract reconstruction
    # stays with the linked native Tail run, not a second unreviewed density model.
    if option_context.get('available'):
        option_context={k:option_context[k] for k in ('available','reason','generated_at','source_valid_until','replay','note',*PERMISSIONS)}
    else:option_context={'available':False,'reason':'native_option_context_unavailable',**PERMISSIONS}
    midpoint=derived['target_midpoint']
    return {'contract':CONTRACT,'schema_version':'3.0','engine':'justhodl-implied-prob','generated_at':generated_at,'as_of':None,
        'source_generated_at':packet['generated_at'],'measurements':rows,'descriptive_comparisons':derived,'yield_curve':inversion(rows,histories),
        'volatility_identities':{sid:{'underlying':u,'index_horizon_calendar_days':d,'unit':'index_points',
            'meaning':'Source-defined volatility index; not the IV of an ETF option, realized volatility, future return probability or earnings-event price.'} for sid,(u,d) in VOLATILITY.items()},
        'option_snapshot_context':option_context,
        'freshness':{'pipeline_check_due_at':(clock(packet['generated_at'])+timedelta(hours=26)).isoformat(),
            'basis':'Canonical publication/acquisition ceiling 26 hours and frequency-specific observation age ceilings; not a source release calendar.'},
        'quality':{'status':'partial' if n else 'unavailable','within_age_ceiling':n,'declared_series':len(SERIES),
            'release_calendar_verified':False,'independent_investment_votes':0},
        'fed':{'current_rate':midpoint['value'],'current_rate_date':midpoint['observation_date'],
            'current_rate_definition':'Target range midpoint on the most recent matching source date, not the effective federal funds rate.',
            **{k:None for k in ('target_lower','target_upper','target_upper_date','ffer_recent','ffer_date','bill_3m','bill_6m','bill_1y','implied_3m_change_bp','implied_6m_change_bp','implied_1y_change_bp','near_term_stance','next_meeting')},
            'meeting_probabilities':None,'official_calendar_url':'https://www.federalreserve.gov/monetarypolicy/fomccalendars.htm'},
        'recession':{**{k:None for k in ('ny_fed_12m_prob_pct','ny_fed_date','ny_fed_12m_delta_pct','composite_score_0_100','composite_label','hy_spread_percentile_10y','hy_spread_z','days_inverted_current','yield_curve_10y3m_bp','yield_curve_inverted','hy_spread_bp')},
            'source_series':'RECPROUSM156N','meaning':'Chauvet/Piger smoothed coincident US recession estimate, revisable; not the New York Fed yield-curve forecast.'},
        **{symbol:{'ticker':ticker,**{k:None for k in ('spot','iv_30d','iv_90d','moves_30d','moves_90d','density_moves_30d')}} for symbol,ticker in (('spy','SPY'),('qqq','QQQ'),('btc','IBIT'))},
        'tail_risk':{'system_tail_gauge':None,'tail_regime':None,'tail_valuation':None,'density_qualified':False},'earnings_implied':[],
        'legacy_context':{k:{'retained':v is not None,'measurement_eligible':False} for k,v in legacy.items()},
        'unqualified_claims':{'fed_meeting_probabilities':'No retained futures/OIS contract, monthly settlement calculation, effective-date convention or reviewed branching model.',
            'ny_fed_forecast':'This source is a distinct coincident recession model. An official forward yield-curve probability is not supplied here.',
            'return_probabilities':'No calibrated physical distribution or synchronized risk-neutral surface; historical/IV index levels cannot identify either.',
            'btc':'IBIT ETF shares, spot BTC and their realized/implied volatility are distinct identities; none is silently substituted.',
            'earnings':'A diffusion standard deviation through an earnings date is not a priced event straddle or a probability distribution.',
            'calendar':'Predecessor hardcoded meeting dates are unqualified. Use the official calendar; no stale date is promoted.'},
        'score':None,'regime':None,'call':None,'portfolio_action':'WAIT',**PERMISSIONS,
        'dependency_graph':{'canonical_run':deepcopy(packet['replay']),'roots':['FRED:'+s for s in SERIES],
            'features':{k:v['roots'] for k,v in derived.items()},'independent_investment_votes':0},
        'portfolio_consequences':{'status':'EXPLICIT_SCENARIOS_ONLY','meaning':'User-entered mutually exclusive terminal prices and probabilities can define a hypothetical discrete payoff distribution. They are not engine forecasts.'},
        'methodology':{'replay':'Pinned canonical definitions, original observations and exact source-row references. Current-vintage history is not a point-in-time backtest.',
            'rates':'Target bounds, daily effective rates, monthly averages and Treasury constant-maturity yields remain different series. Basis-point differences do not imply Fed meeting outcomes.',
            'recession':'A revisable coincident smoothed estimate keeps its reference month and publisher attribution. A 12-month change is percentage points on exactly matched months.',
            'volatility':'Distinct source index underlyings and horizons remain visible. No extrapolation to 90 days, ETF substitution or physical probability inference.',
            'statistics':'Sample counts/dates accompany midrank percentiles and sample SD. A partial three-year credit history is never called ten-year history.',
            'decisions':'WAIT means abstention, not that an existing portfolio is safe. Any hypothetical scenario assumptions are explicitly supplied by the user.'}}

def build(packet,originals,legacy,generated_at):
    with localcontext() as arithmetic:
        arithmetic.prec=40;arithmetic.rounding=ROUND_HALF_EVEN
        return _build(packet,originals,legacy,generated_at)
