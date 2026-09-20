"""USD funding context from canonical observations, with no fitted stress authority."""
from copy import deepcopy
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,math
from research_brief_model import clock,row_status
import report_observations

CONTRACT='eurodollar-native-research.v1'
PREFIX='data/eurodollar-research/'
CURRENT='data/eurodollar-stress.json'
PRIVATE='audit-private/20260909-originals/eurodollar-research/'
PERMISSIONS={'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}
CATALOG={
 'STLFSI4':('Index','W','st_louis_fed_conditions','Official financial-conditions index; not OFR FSI or an offshore dollar quote.'),
 'BAMLH0A0HYM2':('Percent','D','ice_credit_indices','High-yield index option-adjusted spread; not an issuer funding quote.'),
 'BAMLC0A0CM':('Percent','D','ice_credit_indices','Investment-grade index option-adjusted spread; shares methodology and risk drivers with HY.'),
 'VIXCLS':('Index','D','cboe_volatility_indices','30-day option-implied index; not a funding rate or observed crisis probability.'),
 'DTWEXBGS':('Index Jan 2006=100','D','federal_reserve_broad_dollar','Broad trade-weighted nominal dollar index, distinct from DXY; a move alone does not establish a funding squeeze.'),
 'DTB3':('Percent','D','treasury_rates','Three-month bill secondary-market rate on a bank-discount basis; lower yields can reflect several causes, not only flight to safety.'),
 'DGS10':('Percent','D','treasury_rates','Ten-year constant-maturity Treasury yield; changes in this yield are not bond total returns.'),
 'SOFR':('Percent','D','nyfed_money_markets','Secured overnight Treasury financing reference rate.'),
 'DFF':('Percent','D','nyfed_money_markets','Effective federal funds rate from a different unsecured market and participant set.'),
}
SERIES=tuple(CATALOG)
def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def native(value):
    if value is None:return None
    out=float(value)
    if not math.isfinite(out):raise ValueError('Nonfinite native arithmetic')
    return out
def shown(value):return None if value is None else float(round(value,6))

def records(original,through):
    if original is None:return []
    rows=[]
    for n,r in enumerate(original['observations']['observations']):
        if date.fromisoformat(r['date'])>date.fromisoformat(through):continue
        value=report_observations.decimal(r.get('value'))
        rows.append({'date':r['date'],'value':value,'original_row_index':n})
    return sorted(rows,key=lambda r:r['date'])

def measurements(packet,originals,generated_at):
    now=clock(generated_at);source_clock=clock(packet['generated_at'])
    if source_clock>now:raise ValueError('Canonical source is future')
    rows={};histories={}
    for sid,(unit,freq,family,meaning) in CATALOG.items():
        source=packet.get('measurements',{}).get(sid) or {};original=originals.get(sid)
        status=row_status(source,now,(now-source_clock).total_seconds())
        if source and (source.get('unit')!=unit or source.get('frequency')!=freq):status='definition_changed'
        if source and not original:raise ValueError('Originals must be reconstructed before measurement use')
        history=records(original,source['date']) if source.get('date') else [];histories[sid]=history
        valid=status=='fresh';value=report_observations.decimal(source.get('current_decimal')) if valid else None
        previous=history[-2] if len(history)>1 else None
        change=value-previous['value'] if value is not None and previous and previous['value'] is not None else None
        deadlines=[source_clock+timedelta(hours=26)]
        if source.get('acquired_at'):deadlines.append(clock(source['acquired_at'])+timedelta(hours=26))
        if source.get('date'):
            deadlines.append(datetime.combine(date.fromisoformat(source['date'])+timedelta(days=report_observations.AGE_LIMITS[freq]+1),datetime.min.time(),timezone.utc))
        rows[sid]={'series_id':sid,'label':source.get('name') or sid,'source_url':'https://fred.stlouisfed.org/series/'+sid,
            'value':native(value),'exact_value':str(value) if value is not None else None,'unit':unit,
            'value_bps':native(value*100) if value is not None and unit=='Percent' else None,
            'exact_value_bps':str(value*100) if value is not None and unit=='Percent' else None,
            'observation_date':source.get('date'),'previous_observation_date':previous['date'] if previous else None,
            'change_native_units':native(change),'change_bps':native(change*100) if change is not None and unit=='Percent' else None,
            'exact_change_native_units':str(change) if change is not None else None,'source_valid_until':min(deadlines).isoformat(),
            'last_observed_value':source.get('current_decimal'),'source_unit':source.get('unit'),
            'frequency':source.get('frequency'),'source_generated_at':packet['generated_at'],'acquired_at':source.get('acquired_at'),
            'published_at':None,'family':family,'interpretation':meaning,'definition':deepcopy(source.get('definition')),
            'quality':{'status':'within_age_ceiling' if valid else status,'observation_age_days':(now.date()-date.fromisoformat(source['date'])).days if source.get('date') else None,
                'source_age_ceiling_hours':26,'observation_age_ceiling_days':report_observations.AGE_LIMITS[freq],
                'release_calendar_verified':False},
            'history_coverage':{**deepcopy(source.get('coverage',{})),'retained_rows':len(history),
                'numeric_rows':sum(r['value'] is not None for r in history),'missing_rows':sum(r['value'] is None for r in history),
                'first_date':history[0]['date'] if history else None,'last_date':history[-1]['date'] if history else None,
                'point_in_time_history':False},'evidence':deepcopy(source.get('evidence',{})),**PERMISSIONS}
    return rows,histories

def repo_spread(rows,histories):
    a,b=rows['SOFR'],rows['DFF'];left={r['date']:r for r in histories['SOFR']};right={r['date']:r for r in histories['DFF']}
    common=sorted(left.keys()&right.keys());day=common[-1] if common else None
    out={'labels':['SOFR','DFF'],'observation_date':day,'latest_dates':{sid:rows[sid]['observation_date'] for sid in ('SOFR','DFF')},
        'current_comparison_available':False,'difference_bps':None,'exact_difference_bps':None,'unit':'basis_points',
        'interpretation':'SOFR minus effective federal funds on one matching date. Different secured/unsecured markets; not a diagnosis of reserve scarcity or a covered-interest basis.',**PERMISSIONS}
    if not day:return out
    av,bv=left[day]['value'],right[day]['value']
    out['original_row_indices']={'SOFR':left[day]['original_row_index'],'DFF':right[day]['original_row_index']}
    valid=all(r['quality']['status']=='within_age_ceiling' for r in (a,b)) and a['observation_date']==b['observation_date']==day
    if valid and av is not None and bv is not None:
        value=(av-bv)*100;out.update(current_comparison_available=True,difference_bps=native(value),exact_difference_bps=str(value))
    return out

def yield_change_dispersion(row,history):
    pairs=[];skipped=0
    for left,right in zip(history,history[1:]):
        if left['value'] is None or right['value'] is None:skipped+=1;continue
        pairs.append({'from':left['date'],'to':right['date'],'change':(right['value']-left['value'])*100,
            'gap_days':(date.fromisoformat(right['date'])-date.fromisoformat(left['date'])).days})
    chosen=pairs[-60:]
    out={'series_id':'DGS10','requested_differences':60,'valid_differences':len(chosen),'sample_stddev_bps':None,
        'annualized_reference_bps':None,'annualization_assumption':'Sample SD * sqrt(252 intervals/year); a convention, not an estimated future volatility.',
        'first_from_date':chosen[0]['from'] if chosen else None,'last_to_date':chosen[-1]['to'] if chosen else None,
        'maximum_calendar_gap_days':max((r['gap_days'] for r in chosen),default=None),
        'excluded_missing_pairs_in_returned_history':skipped,'current_comparison_available':False,
        'interpretation':'Last 60 valid adjacent provider-row yield changes. Missing rows are not bridged. Calendar gaps are shown; not 60 assumed trading days, a bond-return volatility, MOVE, or a stress probability.',**PERMISSIONS}
    if len(chosen)!=60 or row['quality']['status']!='within_age_ceiling' or chosen[-1]['to']!=row['observation_date']:return out
    with localcontext() as ctx:
        ctx.prec=40;values=[r['change'] for r in chosen];mean=sum(values,Decimal(0))/Decimal(60)
        sd=(sum(((v-mean)**2 for v in values),Decimal(0))/Decimal(59)).sqrt()
        out.update(sample_stddev_bps=shown(sd),annualized_reference_bps=shown(sd*Decimal(252).sqrt()),current_comparison_available=True)
    return out

def _build(packet,originals,fx,generated_at):
    rows,histories=measurements(packet,originals,generated_at);spread=repo_spread(rows,histories)
    dispersion=yield_change_dispersion(rows['DGS10'],histories['DGS10'])
    count=sum(m['quality']['status']=='within_age_ceiling' for m in rows.values())
    fx=fx if isinstance(fx,dict) else {};metrics=fx.get('regime_metrics')
    raw=metrics.get('usd_synthetic_20d_pct') if isinstance(metrics,dict) else None
    fx_value=float(raw) if type(raw) in (int,float) and math.isfinite(raw) else None
    fx_context={'source_key':'data/polygon-fx-regime.json','source_generated_at':fx.get('generated_at'),
        'reported_usd_synthetic_20d_pct':fx_value,'unit':'percent_return_as_reported','measurement_verified':False,
        'interpretation':'Preserved upstream context. Pair weights, price identity, dates and return window have not been reconciled; no funding-stress vote.',**PERMISSIONS}
    signals=[]
    for old,sid in (('ofr_fsi','STLFSI4'),('hy_oas','BAMLH0A0HYM2'),('ig_oas','BAMLC0A0CM'),('vix','VIXCLS'),('broad_dollar','DTWEXBGS'),('t_bill_3m','DTB3')):
        m=rows[sid];signals.append({'id':old,'canonical_id':'st_louis_fsi' if old=='ofr_fsi' else old,'fred_series':sid,'label':m['label'],
            'value':m['value'],'unit':m['unit'],'as_of':m['observation_date'],'score_0_100':None,'polarity':None,'description':m['interpretation'],**PERMISSIONS})
    signals.extend(({'id':'repo_spread','fred_series':'SOFR_minus_DFF','value':spread['difference_bps'],'unit':'basis_points','score_0_100':None,**PERMISSIONS},
        {'id':'rate_vol_10y','fred_series':'DGS10','value':dispersion['annualized_reference_bps'],'unit':'bps_per_sqrt_year_assuming_252_intervals','score_0_100':None,**PERMISSIONS},
        {'id':'usd_momentum_rt','fred_series':'MASSIVE:polygon-fx-regime','value':None,'unit':'percent_return_unverified','score_0_100':None,'context':fx_context,**PERMISSIONS}))
    due=(clock(packet['generated_at'])+timedelta(hours=26)).isoformat()
    return {'engine':'justhodl-eurodollar-stress','contract':CONTRACT,'version':'2.0.0','v':'2.0',
        'generated_at':generated_at,'as_of':generated_at,'source_generated_at':packet['generated_at'],'measurements':rows,
        'repo_comparison':spread,'yield_change_dispersion':dispersion,'fx_context':fx_context,
        'quality':{'status':'fresh' if count==9 else 'partial' if count else 'unavailable','within_age_ceiling':count,'reviewed_series':9,
            'release_calendar_verified':False,'independent_investment_votes':0},
        'freshness':{'pipeline_check_due_at':due,'basis':'Canonical observation ceilings (10 daily / 21 weekly calendar days) and separate 26-hour acquisition/packet ceiling. Compilation cannot renew the source clock.'},
        'signals':signals,'composite_score':None,'composite_stress_score':None,'severity':None,'regime':None,
        'n_signals_used':0,'n_signals_total':9,'hot_signals':[],'cold_signals':[],
        'n_failures':9-count,'failures':[{'series_id':sid,'reason':m['quality']['status']} for sid,m in rows.items() if m['quality']['status']!='within_age_ceiling'],
        'thresholds':{k:None for k in ('calm_max','moderate_min','elevated_min','critical_min')},'duration_s':None,
        'call':None,'portfolio_action':'WAIT',**PERMISSIONS,
        'dependency_graph':{'source_run':deepcopy(packet['replay']),'series_to_features':{
            **{sid:[sid] for sid in SERIES},'SOFR_minus_DFF':['SOFR','DFF'],'yield_change_dispersion':['DGS10']},
            'families':{family:[sid for sid,m in rows.items() if m['family']==family] for family in sorted({m['family'] for m in rows.values()})},
            'independence_status':'Not established: reused credit, volatility, dollar and official-condition inputs overlap other desks and one another. Displayed measures are not independent votes.'},
        'portfolio_consequences':{'status':'EXPLICIT_SCENARIOS_ONLY','scenario_url':'/eurodollar.html#scenario-form',
            'formula':'Entered floating-rate USD liability * entered rate shock in bp / 10000 * entered days / selected day basis.',
            'limits':'No inferred position, cost, hedge, target weight or expected return. Index movements do not supply a position-specific funding shock.'},
        'compatibility':{'as_of':'Legacy generation-clock alias; use each measurement observation_date for its market date.',
            'ofr_fsi':'Legacy identifier preserved; actual series is St. Louis Fed STLFSI4, not OFR FSI.',
            'scores':'Unvalidated equal-weight scores, polarity rules and thresholds are withheld.',
            'duration_s':'Legacy elapsed collection field retained as null; this model reconstructs existing canonical originals without a provider collection.'},
        'methodology':{'originals':'Selected canonical source responses are reconstructed under the pinned report compiler; no new duplicate provider collector.',
            'history':'Actual bounded current-vintage response windows. Revisions/backfills and historical first availability are not qualified.',
            'decisions':'WAIT is research abstention, not an instruction to liquidate an existing position.'}}


def build(packet,originals,fx,generated_at):
    with localcontext() as arithmetic:
        arithmetic.prec=40;arithmetic.rounding=ROUND_HALF_EVEN
        return _build(packet,originals,fx,generated_at)
