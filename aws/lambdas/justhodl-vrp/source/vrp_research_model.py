"""Reproducible S&P 500 close-sampled volatility research; no option P&L claim."""
from copy import deepcopy
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,math
import report_observations
from research_brief_model import clock,row_status

CONTRACT='vrp-native-research.v1';PREFIX='data/vrp-research/';CURRENT='data/vrp.json'
PRIVATE='audit-private/20260909-originals/vrp-research/'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}
SERIES=('SP500','VIXCLS','VXVCLS')
LABELS={'SP500':'S&P 500 price index close','VIXCLS':'Cboe 30-calendar-day implied volatility index','VXVCLS':'Cboe three-month implied volatility index'}
TENORS={'VIXCLS':30,'VXVCLS':93}

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def shown(v):return None if v is None else float(round(v,8))

def records(original,through):
    if original is None:return []
    out=[];seen=set()
    for i,r in enumerate(original['observations']['observations']):
        day=date.fromisoformat(r['date'])
        if day in seen:raise ValueError('Duplicate source date')
        seen.add(day)
        if day>date.fromisoformat(through):continue
        value=report_observations.decimal(r.get('value'))
        if value is not None and value<=0:raise ValueError('Nonpositive price or volatility index')
        out.append({'date':str(day),'value':value,'original_row_index':i})
    return sorted(out,key=lambda r:r['date'])

def observed(packet,originals,generated_at):
    at=clock(generated_at);source=clock(packet['generated_at'])
    if source>at:raise ValueError('Future canonical source')
    rows={};histories={}
    for sid in SERIES:
        m=packet.get('measurements',{}).get(sid) or {};original=originals.get(sid)
        if m and not original:raise ValueError('Original reconstruction required')
        status=row_status(m,at,(at-source).total_seconds())
        if m and (m.get('series_id')!=sid or m.get('unit')!='Index' or m.get('frequency')!='D'
                  or m.get('definition',{}).get('seasonal_adjustment_short')!='NSA'):raise ValueError('Canonical definition changed')
        history=records(original,m['date']) if m.get('date') else [];histories[sid]=history
        value=report_observations.decimal(m.get('current_decimal')) if status=='fresh' else None
        if value is not None and value<=0:raise ValueError('Nonpositive canonical index')
        due=[source+timedelta(hours=26)]
        if m.get('acquired_at'):due.append(clock(m['acquired_at'])+timedelta(hours=26))
        if m.get('date'):due.append(datetime.combine(date.fromisoformat(m['date'])+timedelta(days=11),time.min,timezone.utc))
        rows[sid]={'series_id':sid,'label':LABELS[sid],'source_url':'https://fred.stlouisfed.org/series/'+sid,
            'value':float(value) if value is not None else None,'exact_value':str(value) if value is not None else None,
            'unit':'Index','source_unit':m.get('unit'),'frequency':m.get('frequency'),'observation_date':m.get('date'),
            'source_generated_at':packet['generated_at'],'acquired_at':m.get('acquired_at'),'source_valid_until':min(due).isoformat(),
            'definition':deepcopy(m.get('definition')),'evidence':deepcopy(m.get('evidence',{})),
            'quality':{'status':'within_age_ceiling' if value is not None else status,'release_calendar_verified':False},
            'history_coverage':{'returned_rows':len(history),'numeric_rows':sum(r['value'] is not None for r in history),
                'missing_rows':sum(r['value'] is None for r in history),'first_date':history[0]['date'] if history else None,
                'last_date':history[-1]['date'] if history else None,'current_vintage_only':True,'point_in_time_history':False},**PERMISSIONS}
    return rows,histories

def intervals(history):
    """Adjacent positive provider closes; missing markers remain explicit metadata."""
    valid=[r for r in history if r['value'] is not None];out=[]
    for left,right in zip(valid,valid[1:]):
        gap=(date.fromisoformat(right['date'])-date.fromisoformat(left['date'])).days
        out.append({'from_date':left['date'],'to_date':right['date'],'calendar_days':gap,
            'log_return':(right['value']/left['value']).ln(),
            'original_row_indices':[left['original_row_index'],right['original_row_index']],
            'missing_markers_between':sum(left['date']<r['date']<right['date'] and r['value'] is None for r in history)})
    return out

def window(selected,n,endpoint):
    base={'requested_intervals':n,'observed_intervals':len(selected),'value':None,'unit':'annualized_volatility_percent',
        'first_close_date':selected[0]['from_date'] if selected else None,'last_close_date':selected[-1]['to_date'] if selected else None,
        'calendar_span_days':sum(r['calendar_days'] for r in selected),'maximum_calendar_gap_days':max((r['calendar_days'] for r in selected),default=None),
        'missing_markers_between_closes':sum(r['missing_markers_between'] for r in selected),'available':False,
        'method':'Sample standard deviation of adjacent positive reported-close log returns, ddof=1, multiplied by sqrt(252)*100.',
        'limits':'252 intervals/year is a convention. No verified exchange-session completeness, intraday path or dividend return. Null provider rows are not filled; multi-calendar-day intervals are reported. Gaps above four calendar days are refused.',**PERMISSIONS}
    if len(selected)!=n or n<2 or selected[-1]['to_date']!=endpoint or any(r['calendar_days']>4 for r in selected):return base
    values=[r['log_return'] for r in selected];mean=sum(values)/Decimal(n)
    variance=sum((v-mean)**2 for v in values)/Decimal(n-1)
    base.update(value=shown((variance*Decimal(252)).sqrt()*100),available=True,
        original_row_indices=[selected[0]['original_row_indices'][0]]+[r['original_row_indices'][1] for r in selected])
    return base

def trailing(rows,histories,pairs):
    price=rows['SP500'];end=price['observation_date'];out={}
    for n in (10,21,63):
        w=window(pairs[-n:],n,end)
        if price['quality']['status']!='within_age_ceiling':w.update(value=None,available=False)
        out[str(n)]=w
    return out

def gap(rows,rv,sid,n):
    m=rows[sid];w=rv[str(n)]
    available=m['quality']['status']=='within_age_ceiling' and w['available'] and m['observation_date']==w['last_close_date']
    return {'implied_series_id':sid,'implied_tenor_calendar_days':TENORS[sid],'trailing_intervals':n,
        'observation_date':m['observation_date'],'realized_end_date':w['last_close_date'],'available':available,
        'volatility_points':shown(Decimal(m['exact_value'])-Decimal(str(w['value']))) if available else None,
        'implied_volatility_percent':m['value'] if available else None,'trailing_volatility_percent':w['value'] if available else None,
        'unit':'volatility_percentage_points','same_underlying':'SPX price index','intraday_clocks_synchronized':False,
        'interpretation':'Same-date implied-minus-trailing-realized descriptive gap. Forward implied tenor and backward observed window differ; not expected premium, fair option value or profit.',**PERMISSIONS}

def gap_history(histories,pairs):
    implied={r['date']:r for r in histories['VIXCLS'] if r['value'] is not None};out=[]
    for i,pair in enumerate(pairs):
        if i<20 or pair['to_date'] not in implied:continue
        w=window(pairs[i-20:i+1],21,pair['to_date'])
        if not w['available']:continue
        row=implied[pair['to_date']]
        out.append({'date':pair['to_date'],'volatility_points':shown(row['value']-Decimal(str(w['value']))),
            'trailing_intervals':21,'realized_first_close_date':w['first_close_date'],'implied_original_row_index':row['original_row_index']})
    sample=out[-252:];values=[Decimal(str(r['volatility_points'])) for r in sample]
    stats={'observations':len(sample),'first_date':sample[0]['date'] if sample else None,'last_date':sample[-1]['date'] if sample else None,
        'empirical_percentile':None,'mean_volatility_points':None,'sample_sd_volatility_points':None,
        'interpretation':'At most 252 dated, overlapping current-vintage gaps. Not a guaranteed year, independent trials, a forecast score or a strategy backtest.'}
    if len(values)>=2:
        mean=sum(values)/len(values);sd=(sum((x-mean)**2 for x in values)/Decimal(len(values)-1)).sqrt()
        stats.update(mean_volatility_points=shown(mean),sample_sd_volatility_points=shown(sd),
            empirical_percentile=shown(Decimal(100)*sum(v<=values[-1] for v in values)/Decimal(len(values))))
    return {'rows':sample,'statistics':stats,'independent_samples':None,'current_vintage_only':True}

def expost(histories,pairs):
    """Exact 30-calendar-day endpoint close sampling; no maturity substitution."""
    implied={r['date']:r for r in histories['VIXCLS'] if r['value'] is not None}
    price={r['date']:r for r in histories['SP500'] if r['value'] is not None};out=[];excluded={}
    latest=max(price,default='');byend={r['to_date']:i for i,r in enumerate(pairs)}
    for day,vix in sorted(implied.items()):
        if day not in price:continue
        end=str(date.fromisoformat(day)+timedelta(days=30))
        reason=None
        if end>latest:reason='not_matured'
        elif end not in price:reason='exact_calendar_endpoint_unavailable'
        if reason:excluded[reason]=excluded.get(reason,0)+1;continue
        selected=[r for r in pairs[max(0,byend.get(day,-1)+1):byend[end]+1]]
        if not selected or selected[0]['from_date']!=day or any(r['calendar_days']>4 for r in selected):
            excluded['unqualified_close_path']=excluded.get('unqualified_close_path',0)+1;continue
        variance=sum(r['log_return']**2 for r in selected)*Decimal(365)/Decimal(30)*Decimal(10000)
        realized=variance.sqrt()
        out.append({'origin_date':day,'maturity_date':end,'calendar_days':30,'observed_intervals':len(selected),
            'maximum_calendar_gap_days':max(r['calendar_days'] for r in selected),
            'missing_markers_between_closes':sum(r['missing_markers_between'] for r in selected),
            'implied_volatility_percent':float(vix['value']),'realized_close_sampled_volatility_percent':shown(realized),
            'volatility_difference_points':shown(vix['value']-realized),
            'variance_difference_percent_squared':shown(vix['value']**2-variance),
            'implied_original_row_index':vix['original_row_index'],
            'price_original_row_indices':[selected[0]['original_row_indices'][0]]+[r['original_row_indices'][1] for r in selected]})
    sample=out[-252:];nonoverlap=[]
    for row in sample:
        if not nonoverlap or row['origin_date']>=nonoverlap[-1]['maturity_date']:nonoverlap.append(row)
    return {'rows':sample,'observations':len(sample),'available_matured_windows':len(out),'exclusions':excluded,
        'nonoverlapping_window_origins':[r['origin_date'] for r in nonoverlap],
        'independence_established':False,'unit_volatility':'volatility_percentage_points','unit_variance':'percent_squared',
        'formula':'Realized variance = (365/30) * sum(log(SP500_close_i / SP500_close_previous)^2) * 10000; no sample-mean subtraction.',
        'interpretation':'Retrospective 30-calendar-day close-sampled outcomes, exact dated endpoints only. Current vintages and daily closing times, not original first availability or synchronized timestamps. Overlapping windows are dependent. VIX squared is a reference, not a replicating strategy. No option P&L, hit rate, seller profitability, hedge price or expected premium is estimated.',**PERMISSIONS}

def _build(packet,originals,legacy,generated_at):
    rows,histories=observed(packet,originals,generated_at);pairs=intervals(histories['SP500']);rv=trailing(rows,histories,pairs)
    gaps={'30_calendar_vs_21_intervals':gap(rows,rv,'VIXCLS',21),'93_calendar_vs_63_intervals':gap(rows,rv,'VXVCLS',63)}
    count=sum(m['quality']['status']=='within_age_ceiling' for m in rows.values())
    history=gap_history(histories,pairs);ex=expost(histories,pairs)
    return {'contract':CONTRACT,'engine':'justhodl-vrp','schema_version':'2.0','generated_at':generated_at,
        'as_of':rows['SP500']['observation_date'],'source_generated_at':packet['generated_at'],'measurements':rows,
        'trailing_realized':rv,'descriptive_gaps':gaps,'gap_history':history,'expost_research':ex,
        'freshness':{'pipeline_check_due_at':(clock(packet['generated_at'])+timedelta(hours=26)).isoformat(),
            'basis':'Canonical packet/acquisition ceiling of 26 hours and daily observation ceiling of ten calendar days. Source clocks cannot be renewed by compilation.'},
        'quality':{'status':'fresh' if count==3 and all(g['available'] for g in gaps.values()) else 'partial' if count else 'unavailable',
            'within_age_ceiling':count,'reviewed_series':3,'release_calendar_verified':False,'independent_investment_votes':0},
        'legacy_context':{k:{'retained':v is not None,'generated_at':v.get('generated_at') if isinstance(v,dict) else None,
            'qualified':False,'reason':'Unreconciled legacy input preserved; not used to derive native research.'} for k,v in legacy.items()},
        'regime':None,'regime_color':None,'score':None,'call':None,'portfolio_action':'WAIT',**PERMISSIONS,
        'implied':{k:None for k in ('vix9d','vix','vix3m','vix6m','source_date')},
        'realized':{**{k:None for k in ('rv_10d','rv_21d','rv_63d','rv_garman_klass_21d')},'underlying':'Legacy SPY fields withheld; see typed SP500 research.'},
        'vrp':{k:None for k in ('vrp_9d','vrp_30d','vrp_3m','vrp_30d_percentile_1y','vrp_30d_zscore_1y','vrp_30d_mean_1y','vrp_30d_min_1y','vrp_30d_max_1y','term_structure','expost_vrp_mean','expost_positive_pct')},
        'headline':'S&P 500 implied and realized volatility research','desk_note':'Descriptive gaps and retrospective outcomes; no qualified volatility trading instruction.',
        'interpretation':{'for_vol_sellers':None,'for_hedgers':None},'series':{'dates':[],'vrp_30d':[]},
        'history_reference':{'key':'data/vrp-history.json','status':'legacy_unverified','used_for_forecasting':False},
        'dependency_graph':{'canonical_run':deepcopy(packet['replay']),'roots':['FRED:'+s for s in SERIES],
            'features':{'trailing_realized':['SP500'],'descriptive_gaps':['SP500','VIXCLS','VXVCLS'],'expost_research':['SP500','VIXCLS']},
            'independence_status':'Shared index roots and overlapping windows; not independent confirmations.'},
        'portfolio_consequences':{'status':'EXPLICIT_SCENARIOS_ONLY','formula':'Entered signed option vega in USD per volatility point * entered implied-volatility shock in points.',
            'limits':'First-order local vega approximation only. No forecast, option valuation, gamma, theta, skew, term effects, financing or costs.'},
        'methodology':{'underlying':'SP500 price index from FRED, excludes dividends; VIX complex is based on SPX options. No SPY ETF substitution.',
            'sampling':'Reported closing dates; intraday timestamps and official exchange-session completeness are not reconciled.',
            'replay':'Canonical definitions and complete bounded original observation responses reconstructed under the pinned compiler.',
            'nine_day':'Legacy VIX9D input retained but no native original source contract; no synthetic 9-day comparison.',
            'range_estimator':'No native SPX OHLC source; Garman-Klass is unavailable, never substituted from another underlying.',
            'decisions':'WAIT is abstention; no implied instruction to retain or reduce existing portfolio exposure.'}}

def build(packet,originals,legacy,generated_at):
    with localcontext() as arithmetic:
        arithmetic.prec=40;arithmetic.rounding=ROUND_HALF_EVEN
        return _build(packet,originals,legacy,generated_at)
