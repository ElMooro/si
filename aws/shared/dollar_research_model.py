"""Original-source dollar measurements, reciprocal FX arithmetic and dated context.

Pure reconstruction only. No provider request, credential, forecast, notification
or account path. Descriptive changes cannot vote or authorize portfolio actions.
"""
from copy import deepcopy
from datetime import date,datetime,time,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_HALF_EVEN
import hashlib,json,math,re
import report_observations as observations
from research_brief_model import clock,row_status
from dollar_research_catalog import INDICES,FX,CONTEXT,SPECS,SERIES,CONTEXT_KEYS,METHOD_SOURCES

CONTRACT='dollar-original-research.v1';PREFIX='data/dollar-research/'
CURRENT='data/dollar-radar.json';HISTORY='data/dollar-radar-history.json'
PRIVATE='audit-private/20260909-originals/dollar-research/'
PERMISSIONS={k:False for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')}


def encoded(doc):return json.dumps(doc,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def sha(raw):return hashlib.sha256(raw).hexdigest()
def shown(value):
    if value is None:return None
    out=float(round(value,10))
    if not math.isfinite(out):raise ValueError('Nonfinite derived value')
    return out
def scalar(value):return {'value':shown(value),'exact_value':str(value) if value is not None else None}
def point(row):return None if row is None else {'date':row['date'],'original_row_index':row['original_row_index'],**scalar(row['value'])}
def history(original,sid,through):
    if original is None:return []
    seen=set();rows=[]
    for index,row in enumerate(original['observations']['observations']):
        day=date.fromisoformat(row['date']);value=observations.decimal(row.get('value'))
        if str(day)!=row['date'] or day in seen:raise ValueError('Duplicate or noncanonical source date')
        seen.add(day)
        if row.get('value') not in (None,'.','') and value is None:raise ValueError('Invalid source number')
        if value is not None and sid in (*INDICES,*FX) and value<=0:raise ValueError('Nonpositive currency/index observation')
        if SPECS[sid][2]=='M' and day.day!=1:raise ValueError('Monthly period identity differs')
        if str(day)<=through:rows.append({'date':str(day),'value':value,'original_row_index':index})
    return sorted(rows,key=lambda r:r['date'])
def endpoint(rows,target,lag):
    selected=next((r for r in reversed(rows) if r['date']<=target),None)
    return selected if selected and (date.fromisoformat(target)-date.fromisoformat(selected['date'])).days<=lag else None
def inverse(value):return Decimal(1)/value if value is not None and value>0 else None
def valid_original(ref):
    return (isinstance(ref,dict) and bool(re.fullmatch('[a-f0-9]{64}',ref.get('sha256','')))
        and ref.get('key')==PRIVATE+ref['sha256']+'.bin' and type(ref.get('bytes')) is int and 0<ref['bytes']<=32*1024*1024)
def change(current,baseline,unit,invert=False):
    a=(current or {}).get('value');b=(baseline or {}).get('value')
    if invert:a,b=inverse(a),inverse(b)
    delta=a-b if a is not None and b is not None else None
    relative=100*(a/b-1) if delta is not None and b>0 else None
    return {'current':point(current),'baseline':point(baseline),'inverted':invert,'change_unit':unit,
        'difference':scalar(delta),'relative_percent':scalar(relative),'available':delta is not None,
        'formula':'100 * ((1/current) / (1/baseline) - 1)' if invert else '100 * (current / baseline - 1) for positive baseline',
        'missing_policy':'A returned missing observation is not replaced by an earlier numeric value.'}
def comparisons(rows,sid):
    if not rows:return {}
    current=rows[-1];freq=SPECS[sid][2];day=date.fromisoformat(current['date']);out={}
    for label,months,days in (('week',0,7),('month',1,0),('quarter',3,0),('year',12,0)):
        target=(day-timedelta(days=days)) if days else observations.months_before(day,months)
        if freq=='M':baseline=next((r for r in rows if r['date']==str(target)),None) if not days else None
        else:baseline=endpoint(rows,str(target),4 if freq=='D' else 6)
        raw=change(current,baseline,'percentage_points' if SPECS[sid][1]=='Percent' else SPECS[sid][1])
        raw.update(target_date=str(target),baseline_lag_days=(target-date.fromisoformat(baseline['date'])).days if baseline else None,
            unavailable_reason='horizon_shorter_than_published_frequency' if freq=='M' and days else 'baseline_or_current_missing' if not raw['available'] else None)
        if sid in FX:
            currency,num,den,unit=FX[sid]
            raw['dollar_strength']=change(current,baseline,currency+' per USD',invert=num=='USD')
        out[label]=raw
    return out
def measure(packet,originals,generated_at):
    if (packet.get('contract')!=observations.CONTRACT or packet.get('calls_eligible') is not False
        or packet.get('sizing_eligible') is not False):raise ValueError('Descriptive canonical measurement contract required')
    ref=packet.get('replay') or {}
    if (not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json',ref.get('manifest_key',''))
        or observations.digest({k:v for k,v in packet.items() if k!='replay'})!=ref.get('output_sha256')):
        raise ValueError('Exact canonical output identity required')
    at=clock(generated_at);source=clock(packet['generated_at']);rows={};histories={};qualified={}
    if source>at:raise ValueError('Future canonical source publication')
    for sid in SERIES:
        m=packet.get('measurements',{}).get(sid);original=originals.get(sid);label,unit,freq,family=SPECS[sid]
        if (m is None)!=(original is None):raise ValueError('Canonical measurement and original availability differ')
        if m is not None:
            with localcontext() as canonical_precision:
                canonical_precision.prec=28;canonical_precision.rounding=ROUND_HALF_EVEN
                rebuilt=observations.measurement(sid,original['definition'],original['observations'],original['evidence'],packet['generated_at'],original['acquired_at'])
            if rebuilt!=m:raise ValueError('Canonical original reconstruction differs: '+sid)
            if (m['unit'],m['frequency'],m['definition'].get('seasonal_adjustment_short'))!=(unit,freq,'NSA'):
                raise ValueError('Reviewed source definition differs: '+sid)
        # A row excluded as future-dated by the captured canonical publication
        # cannot become an observation merely because this compiler runs later.
        m=m or {};h=history(original,sid,str(source.date()));histories[sid]=h;latest=h[-1] if h else None
        state=row_status(m,at,(at-source).total_seconds());available=state=='fresh';due=[source+timedelta(hours=26)]
        if m.get('acquired_at'):due.append(clock(m['acquired_at'])+timedelta(hours=26))
        if m.get('date'):due.append(datetime.combine(date.fromisoformat(m['date'])+timedelta(days=observations.AGE_LIMITS[freq]+1),time.min,timezone.utc))
        cutoff=observations.months_before(date.fromisoformat(latest['date']),13 if freq=='D' else 60) if latest else None
        chart=[point(r) for r in h if cutoff is not None and r['date']>=str(cutoff)]
        row={'series_id':sid,'label':label,'source_title':m.get('name'),'unit':unit,'frequency':freq,'family':family,
            'definition':deepcopy(m.get('definition')),'evidence':deepcopy(m.get('evidence',{})),'source_url':'https://fred.stlouisfed.org/series/'+sid,
            'source_generated_at':packet['generated_at'],'acquired_at':m.get('acquired_at'),'provider_updated_at':m.get('provider_updated_at'),
            'source_review_due_at':min(due).isoformat(),'published_at':None,'latest_observation':point(latest),
            'current_value':shown(latest['value']) if latest and available else None,
            'quality':{'status':'within_age_ceiling' if available else state,'release_calendar_verified':False},
            'comparisons':comparisons(h,sid),'history':chart,'history_scope':{'calendar_months':13 if freq=='D' else 60,
                'start_cutoff':str(cutoff) if cutoff else None,'eligible_original_rows':len(h),'displayed_rows':len(chart),
                'source_coverage':deepcopy(m.get('coverage')),
                'current_vintage_only':True,'historical_first_availability_verified':False},**PERMISSIONS}
        if sid in FX:
            currency,num,den,unit=FX[sid];v=latest['value'] if latest else None
            row['quote']={'currency':currency,'numerator':num,'denominator':den,'source_quote_unit':unit,
                'foreign_units_per_usd':scalar(inverse(v) if num=='USD' else v),'usd_per_foreign_unit':scalar(v if num=='USD' else inverse(v)),
                'observation_date':latest['date'] if latest else None,'cnh_substitution_performed':False}
        rows[sid]=row;qualified[sid]=deepcopy(m)
        if qualified[sid]:qualified[sid]['quality']['status']='fresh' if available else state
    return rows,histories,qualified
def monthly_rate_comparison(histories,generated_at):
    """Closed calendar month; returned US daily mean and German monthly observation."""
    us=histories['DGS10'];de=histories['IRLTLT01DEM156N'];cutoff=clock(generated_at).date().replace(day=1);out=[]
    for german in de[-36:]:
        start=date.fromisoformat(german['date'])
        if start>=cutoff:continue
        following=(start.replace(day=28)+timedelta(days=4)).replace(day=1);end=following-timedelta(days=1)
        group=[r for r in us if str(start)<=r['date']<=str(end)];numeric=[r for r in group if r['value'] is not None]
        covered=bool(us and us[0]['date']<=str(start) and us[-1]['date']>=str(end))
        mean=sum(r['value'] for r in numeric)/len(numeric) if covered and len(numeric)>=15 else None
        delta=(mean-german['value'])*100 if mean is not None and german['value'] is not None else None
        out.append({'reference_month':str(start),'month_end':str(end),'german':point(german),
            'us_daily_members':[point(r) for r in group],'us_daily_mean':scalar(mean),'difference_bps':scalar(delta),
            'returned_us_rows':len(group),'numeric_us_rows':len(numeric),'minimum_numeric_rows':15,'returned_span_covered':covered,
            'calendar_completeness_verified':False,'available':delta is not None})
    return {'trail':out,'latest':out[-1] if out else None,
        'formula':'100 * (mean of numeric US daily constant-maturity yields in the month - German monthly yield)',
        'limitation':'Different sovereign instruments and yield methodologies; no currency-hedged carry, executable spread or daily German quote.'}
def matched_curve(rows):
    left=rows['DGS10'];right=rows['DGS2'];a=left['latest_observation'];b=right['latest_observation'];value=None
    if a and b and a['date']==b['date'] and all(r['quality']['status']=='within_age_ceiling' for r in (left,right)):
        if a['exact_value'] is not None and b['exact_value'] is not None:value=100*(Decimal(a['exact_value'])-Decimal(b['exact_value']))
    return {'left':a,'right':b,'series_ids':['DGS10','DGS2'],'difference_bps':scalar(value),'available':value is not None,
        'formula':'100 * (DGS10 - DGS2), identical observation date required','interpretation':'Constant-maturity curve slope; no recession probability.'}
def build(packet,originals,generated_at,contexts=None,predecessors=None):
    with localcontext() as arithmetic:
        arithmetic.prec=34;arithmetic.rounding=ROUND_HALF_EVEN
        rows,histories,qualified=measure(packet,originals,generated_at)
        if set(contexts or {})!=set(CONTEXT_KEYS):raise ValueError('Complete declared context inventory required')
        if set(predecessors or {})!={CURRENT,HISTORY}:raise ValueError('Whole Dollar predecessor and history references required')
        for key,ref in predecessors.items():
            if not valid_original(ref):raise ValueError('Exact complete predecessor reference required: '+key)
        for key,row in contexts.items():
            if (not isinstance(row,dict) or row.get('independent_votes')!=0
                or row.get('status') not in ('retained_unqualified_context','missing')
                or (not valid_original(row.get('original')) if row.get('status')=='retained_unqualified_context' else row.get('original') is not None)):
                raise ValueError('Retained descriptive context identity required: '+key)
        family={}
        for sid,row in rows.items():family.setdefault(row['family'],[]).append(sid)
        return {'contract':CONTRACT,'engine':'justhodl-dollar-radar','version':'4.0.0','generated_at':generated_at,
            'source_generated_at':packet['generated_at'],'source_replay':packet['replay'],'series':rows,
            'indices':list(INDICES),'bilaterals':list(FX),'context_series':list(CONTEXT),
            'derived':{'treasury_curve':matched_curve(rows),'us_germany_monthly':monthly_rate_comparison(histories,generated_at),
                'net_liquidity':observations.liquidity(qualified)},
            'dependency_graph':{'series_families':family,'known_overlap':[
                {'members':['WALCL','WRESBAL','WTREGEN','SWPT'],'basis':'Overlapping Federal Reserve balance-sheet stocks and averages; do not add as independent liquidity.'},
                {'members':['DGS10','DFII10','T10YIE'],'basis':'Nominal and inflation-indexed Treasury yields relate to the breakeven series; no independent votes.'},
                {'members':list(FX),'basis':'All bilateral quotes share USD; the Fed indices also reuse currency exposures with their own weights.'}],
                'complete_component_lineage_verified':False,'independent_votes':0},
            'retained_contexts':deepcopy(contexts),'retained_predecessors':deepcopy(predecessors),
            'quality':{'status':'complete_descriptive' if all(row['quality']['status']=='within_age_ceiling' for row in rows.values()) else 'partial_descriptive' if any(histories.values()) else 'unavailable',
                'declared_series':len(SERIES),'available_original_histories':sum(bool(h) for h in histories.values()),
                'within_age_ceilings':sum(row['quality']['status']=='within_age_ceiling' for row in rows.values()),
                'release_calendar_verified':False,'historical_first_availability_verified':False,'independent_investment_votes':0},
            'benchmark_identity':{'fed_indices':'Official source-provided indices, January 2006 average = 100; nominal and real frequencies stay separate.',
                'ice_dxy':'No official ICE DXY observation is supplied by these FRED series.',
                'bloomberg_dollar_spot':'The old fixed-weight CNY-substituted basket is preserved in the predecessor; it is not promoted as official BBDXY.',
                'benchmark_replication_qualified':False,'methodology_sources':list(METHOD_SOURCES)},
            'dollar_pressure':None,'regime':None,'score':None,'call':None,'portfolio_action':'WAIT',
            'risk_transmission':{'score':None,'verdict':None,'forecast_qualified':False},
            'canaries_pump':None,'canaries_dump':None,'independent_investment_votes':0,**PERMISSIONS,
            'methodology':{'quote_direction':'Original numerator and denominator remain explicit. Reciprocal USD-strength changes are calculated as reciprocals, never by negating the original percent change.',
                'calendar_comparisons':'Actual week/month/quarter/year targets; daily baseline at most four calendar days early, weekly at most six, monthly exact month. Missing returned rows are never skipped.',
                'liquidity':'WALCL - WTREGEN - 1000 * RRPONTSYD in USD millions; component dates and stock/weekly-average scopes stay explicit. No causal dollar forecast.',
                'history':'Bounded current provider vintage, not the information known to investors at historical dates.',
                'authority':'Recorded observations support research and explicit assumed exposures. No calibrated dollar forecast, score, recommendation, hedge ratio or size is established.'}}
