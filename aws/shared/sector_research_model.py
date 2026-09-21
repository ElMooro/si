"""Matched-date ETF price research and descriptive risk; no investment mandate.

All horizons count observed SPY intervals, never calendar months or a verified
exchange calendar. Returns exclude dividends and costs. A missing row is a gap,
not a zero return or an invitation to shift an endpoint.
"""
from copy import deepcopy
from datetime import datetime,timedelta
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,math,statistics
import daily_market_model
from sector_research_catalog import SECTORS,SUBSECTORS,SYMBOLS,RATIOS,HORIZONS

CONTRACT='sector-native-research.v1';PREFIX='data/sector-research/'
CURRENT='data/sector-rotation.json';PRIVATE='audit-private/20260909-originals/sector-research/'
PERMISSIONS={k:False for k in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified')}
clock=daily_market_model.clock

def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def shown(value):
    if value is None:return None
    out=float(round(value,10))
    if not math.isfinite(out):raise ValueError('Nonfinite derived sector measurement')
    return 0.0 if out==0 else out

def decimal(value):return Decimal(str(value))

def point(row):
    if row is None:return None
    return {k:row[k] for k in ('date','close','original_row_index')}


def restore_rows(source,generated_at):
    if source is None:return None,{}
    verified=daily_market_model.equity(source,generated_at)
    out={}
    for row in reversed(verified['history']):
        raw=source['response']['results'][row['source_row']]
        out[row['d']]={'date':row['d'],'close':row['c'],'original_row_index':row['source_row'],
            'open':raw['o'],'high':raw['h'],'low':raw['l'],'volume':raw.get('v')}
    return verified,out


def price_return(first,last):
    with localcontext() as ctx:
        ctx.prec=28;ctx.rounding=ROUND_HALF_EVEN
        return shown((decimal(last)/decimal(first)-1)*100)


def comparison(left,right,dates,end_index,horizon):
    start=end_index-horizon
    if start<0:return {'status':'insufficient_benchmark_history','observed_benchmark_intervals':horizon,'value':None,'excess_percentage_points':None,'ratio_return_percent':None,'end_date':dates[end_index]}
    a,b=dates[start],dates[end_index];la,lb,ra,rb=left.get(a),left.get(b),right.get(a),right.get(b)
    missing=[{'leg':leg,'date':day} for leg,day,row in (('numerator',a,la),('numerator',b,lb),('denominator',a,ra),('denominator',b,rb)) if row is None]
    out={'status':'missing_endpoint' if missing else 'available','observed_benchmark_intervals':horizon,
        'start_date':a,'end_date':b,'numerator_start':point(la),'numerator_end':point(lb),'denominator_start':point(ra),'denominator_end':point(rb),
        'missing_endpoints':missing,'value':None,'excess_percentage_points':None,'denominator_return_percent':None,'ratio_return_percent':None,
        'unit':'percent price return; excess in percentage points','calendar_completeness_verified':False}
    if missing:return out
    with localcontext() as ctx:
        ctx.prec=28;ctx.rounding=ROUND_HALF_EVEN
        x=decimal(lb['close'])/decimal(la['close']);y=decimal(rb['close'])/decimal(ra['close'])
        out.update(value=shown((x-1)*100),denominator_return_percent=shown((y-1)*100),excess_percentage_points=shown((x-y)*100),ratio_return_percent=shown((x/y-1)*100))
    return out


def technical(rows,dates,end_index):
    current=rows.get(dates[end_index]);out={}
    for n in (20,50,200):
        wanted=dates[max(0,end_index-n+1):end_index+1];missing=[d for d in wanted if d not in rows]
        members=[point(rows[d]) for d in wanted if d in rows];complete=len(wanted)==n and not missing
        value=statistics.fmean(x['close'] for x in members) if complete else None
        out['sma'+str(n)]={'value':shown(value),'unit':'USD_per_share','required_observations':n,'observed_dates':wanted,'missing_dates':missing,
            'status':'available' if complete else 'incomplete','members':members,
            'price_distance_percent':price_return(value,current['close']) if value is not None and current else None}
    # Descriptive close-location/volume ratio. It cannot identify investor flows.
    wanted=dates[max(0,end_index-19):end_index+1];members=[rows[d] for d in wanted if d in rows]
    missing=[d for d in wanted if d not in rows or rows[d]['volume'] is None]
    volume=math.fsum(x['volume'] for x in members if x['volume'] is not None)
    valid=len(wanted)==20 and not missing and volume>0
    cmf=math.fsum((0.0 if x['high']==x['low'] else (2*x['close']-x['low']-x['high'])/(x['high']-x['low']))*x['volume'] for x in members) / volume if valid else None
    out['close_location_volume']={'value':shown(cmf),'unit':'ratio','status':'available' if valid else 'incomplete_or_zero_volume','observed_dates':wanted,'missing_dates':missing,
        'definition':'Sum(close-location multiplier × shares) / sum(shares), over exactly 20 observed benchmark dates; zero-range bars contribute zero numerator and keep their denominator volume. No net capital flow is measured.'}
    return out


def risk_panel(all_rows,dates,end_index,count=63):
    symbols=tuple(SECTORS)+('SPY',);wanted=dates[max(0,end_index-count):end_index+1]
    gaps={s:[d for d in wanted if d not in all_rows[s]] for s in symbols}
    out={'status':'unavailable','symbols':list(symbols),'observed_benchmark_intervals':count,'required_prices':count+1,'dates':wanted,'missing_dates':gaps,
        'covariance':None,'correlation':None,'statistics':{},'return_rows':[],
        'definition':'Sample covariance of percent price returns across exactly matched consecutive observed SPY intervals. No pairwise deletion. Dividends, costs and exchange calendar completeness are not included.',
        'annualization_assumption':252,'annualization_scope':'Conventional scaling only; observed interval gaps need not be equal elapsed time. Not a forward volatility or loss forecast.'}
    if len(wanted)!=count+1 or any(gaps.values()):return out
    rows=[]
    for a,b in zip(wanted,wanted[1:]):
        rows.append({'start_date':a,'end_date':b,'returns_percent':{s:price_return(all_rows[s][a]['close'],all_rows[s][b]['close']) for s in symbols},
            'endpoints':{s:[point(all_rows[s][a]),point(all_rows[s][b])] for s in symbols}})
    means={s:statistics.fmean(r['returns_percent'][s] for r in rows) for s in symbols}
    covariance={a:{b:math.fsum((r['returns_percent'][a]-means[a])*(r['returns_percent'][b]-means[b]) for r in rows)/(count-1) for b in symbols} for a in symbols}
    correlations={a:{b:shown(covariance[a][b]/math.sqrt(covariance[a][a]*covariance[b][b])) if covariance[a][a]>0 and covariance[b][b]>0 else None for b in symbols} for a in symbols}
    summaries={}
    for s in symbols:
        sd=math.sqrt(max(0,covariance[s][s]));diffs=[r['returns_percent'][s]-r['returns_percent']['SPY'] for r in rows]
        summaries[s]={'sample_mean_percent':shown(means[s]),'sample_stddev_percent':shown(sd),
            'annualized_price_volatility_percent':shown(sd*math.sqrt(252)),'beta_to_SPY':shown(covariance[s]['SPY']/covariance['SPY']['SPY']) if covariance['SPY']['SPY']>0 else None,
            'annualized_tracking_error_percent':shown(statistics.stdev(diffs)*math.sqrt(252)),
            'scope':'Retrospective matched price sample; no predicted loss, independent alpha or position size.'}
    out.update(status='available',covariance={a:{b:shown(x) for b,x in row.items()} for a,row in covariance.items()},correlation=correlations,statistics=summaries,return_rows=rows,covariance_unit='percent_squared')
    return out


def build(packet,sources,generated_at,legacy=None,refs=None):
    if set(sources)!=set(SYMBOLS):raise ValueError('Exact declared sector source inventory required')
    if clock(packet['generated_at'])>clock(generated_at):raise ValueError('Future daily source publication')
    legacy=legacy or {};refs=refs or {};all_rows={};observations={};expiry=[]
    for symbol in SYMBOLS:
        verified,rows=restore_rows(sources[symbol],generated_at);all_rows[symbol]=rows
        if verified is None:
            observations[symbol]={'symbol':symbol,'status':'missing_source','value':None,'last_observed_price':None,'history':[],**PERMISSIONS};continue
        until=min(clock(verified['acquired_at'])+timedelta(hours=26),clock(verified['observed_at'])+timedelta(hours=96));expiry.append(until)
        observations[symbol]={'symbol':symbol,'name':SECTORS.get(symbol,verified['name']),'status':verified['quality']['status'],
            'value':verified['price'],'last_observed_price':verified['last_observed_price'],'unit':'USD_per_share','currency':'USD',
            'observation_date':verified['date'],'acquired_at':verified['acquired_at'],'observed_at':verified['observed_at'],'source_valid_until':until.isoformat(),
            'adjustment':verified['adjustment'],'source':deepcopy(sources[symbol]['evidence']),'request':deepcopy(sources[symbol]['request']),
            'history_scope':verified['history_scope'],'history':list(rows.values()),**PERMISSIONS}
    dates=sorted(all_rows['SPY']);last=len(dates)-1;date=dates[-1] if dates else None;sectors=[]
    for symbol,name in SECTORS.items():
        row={'symbol':symbol,'ticker':symbol,'name':name,'reference_date':date,'price':point(all_rows[symbol].get(date)),
            'comparisons':{str(n):comparison(all_rows[symbol],all_rows['SPY'],dates,last,n) for n in HORIZONS} if dates else {},
            'technical':technical(all_rows[symbol],dates,last) if dates else {},'rotation_score':None,'rank':None,'regime':None,'implication':'WAIT','call':None,**PERMISSIONS}
        row['benchmark_overlap']='Same SPY benchmark is reused across all sectors; these are correlated observations, not independent evidence votes.'
        if symbol in SUBSECTORS:
            sub=SUBSECTORS[symbol];row['subsector']={'symbol':sub,'comparison':comparison(all_rows[sub],all_rows[symbol],dates,last,21) if dates else None,'status':observations[sub]['status'],'independent_confirmation':False}
        sectors.append(row)
    ratios=[{'numerator':a,'denominator':b,'label':label,'comparisons':{str(n):comparison(all_rows[a],all_rows[b],dates,last,n) for n in HORIZONS} if dates else {},'call':None,**PERMISSIONS} for a,b,label in RATIOS]
    trail=[{'reference_date':day,'sectors':{s:comparison(all_rows[s],all_rows['SPY'],dates,i,21) for s in SECTORS}} for i,day in enumerate(dates)]
    risk=risk_panel(all_rows,dates,last) if dates else {'status':'missing_benchmark','covariance':None,'correlation':None,'statistics':{}}
    contexts=[{'source_key':key,'source_generated_at':(legacy.get(key) or {}).get('generated_at') or (legacy.get(key) or {}).get('as_of'),
        'status':'retained_unqualified_context' if ref else 'missing','retained':ref} for key,ref in sorted(refs.items())]
    valid_until=min(expiry).isoformat() if expiry else generated_at
    return {'contract':CONTRACT,'generated_at':generated_at,'source_generated_at':packet['generated_at'],'source_valid_until':valid_until,
        'observations':observations,'reference_date':date,'benchmark_dates':dates,'sectors':sectors,'ratios':ratios,'relative_performance_history':trail,'risk_sample':risk,
        'quality':{'status':'complete_sources' if len(expiry)==len(SYMBOLS) else 'partial_sources','declared_sources':len(SYMBOLS),'replayed_sources':len(expiry),
            'within_age_ceiling':sum(v['status']=='fresh' for v in observations.values()),'core_reference_coverage':sum(date in all_rows[s] for s in SECTORS) if date else 0,
            'calendar_completeness_verified':False,'historical_first_availability_verified':False,'independent_investment_votes':0},
        'methodology':{'horizons':'1, 5, 21, 63 and 126 observed SPY intervals, exact identical endpoint dates for both legs; no missing-date substitution.',
            'excess':'Sector percent price return minus benchmark percent price return, in percentage points.',
            'ratio':'Change in the numerator/denominator price ratio, in percent. Distinct from excess percentage points.',
            'adjustment':'Current retrieved split-adjusted price vintage, without dividends. Not total return or historical point-in-time availability.',
            'identity':'Current US ETF wrappers; holdings, historic constituents, currency hedges, dividend reinvestment and corporate-action first availability unverified.',
            'technical':'Dated descriptive moving averages and close-location/volume arithmetic. Price-volume metrics do not identify capital flows or institutional buying.',
            'authority':'No validated regime mapping, future return model, causal risk appetite, trade trigger or portfolio mandate.'},
        'dependency_graph':{'benchmark':'SPY','family':'US ETF price aggregates','independent_votes':0,'retained_contexts':contexts},
        'macro_context':{'cycle_phase':None,'regime_label':None,'macro_stress_score':None},'risk_appetite':None,
        'summary':{'n_sectors_analyzed':len(SECTORS),'top_3_leaders':[],'bottom_3_laggards':[],'n_rotating_in':0,'n_rotating_out':0},
        'rotation_alerts':{'rotating_in':[],'rotating_out':[]},'call':None,'portfolio_action':'WAIT',**PERMISSIONS}
