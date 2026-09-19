"""Reproducible yen measurements; no inferred carry allocation or unwind score."""
import calendar, math, statistics
from collections import Counter
from datetime import date, timedelta
from decimal import Decimal
import yen_original as native

CONTRACT='yen-original-research.v1'
PREFIX='data/yen-research/'
CURRENT='data/yen-carry.json'
AUTHORITY={'call':None,'decisive_call':None,'portfolio_action':'WAIT','calls_eligible':False,
 'sizing_eligible':False,'execution_eligible':False,'allocation_pct':None,'forecast_eligible':False}
encoded=native.encoded
digest=native.digest

def month_change(item,months):
    if not item:return None
    latest=item['rows'][-1];day=date.fromisoformat(latest['date']);year,month=divmod(day.year*12+day.month-1-months,12)
    target=f'{year:04}-{month+1:02}-01';base=next((r for r in item['rows'] if r['date']==target),None)
    value=Decimal(latest['value_decimal']) if latest['value_decimal'] is not None else None
    prior=Decimal(base['value_decimal']) if base and base['value_decimal'] is not None else None
    difference=value-prior if value is not None and prior is not None else None
    return {'target':target,'from':base['date'] if base else None,'to':latest['date'],
      'difference_decimal':str(difference) if difference is not None else None,
      'relative_pct_decimal':str(100*difference/prior) if difference is not None and prior>0 else None,
      'current_ref':{'segment':latest['segment'],'row_index':latest['row_index']},
      'baseline_ref':{'segment':base['segment'],'row_index':base['row_index']} if base else None,
      'status':'available' if difference is not None else 'calendar_endpoint_missing',
      'unit':'percentage_points' if item['unit']=='Percent' else item['unit']}

def monthly_pair(measured,identifier,usd,jpy,at=None):
    us,jp=measured.get(usd),measured.get(jpy)
    result={'id':identifier,'name':'US monthly mean minus Japan reported monthly indicator','unit':'percentage_points','frequency':'M',
      'left':usd,'right':jpy,'as_of':jp['as_of'] if jp else None,'value':None,'value_decimal':None,'status':'unavailable','rows':[],
      'limitation':'Same reported month, differing instruments and possibly differing averaging calendars. Not executable carry or a forward-implied return.',**AUTHORITY}
    if not us or not jp:return result
    groups={}
    for r in us['rows']:groups.setdefault(r['date'][:7],[]).append(r)
    for j in jp['rows']:
        records=groups.get(j['date'][:7],[]);valid=[r for r in records if r['value_decimal'] is not None]
        y,m=map(int,j['date'][:7].split('-'));required=calendar.monthrange(y,m)[1] if usd=='DFF' else 15
        complete=len(valid)>=required and j['value_decimal'] is not None
        if at and date(y,m,calendar.monthrange(y,m)[1])>=native.clock(at).date():complete=False
        mean=sum(Decimal(r['value_decimal']) for r in valid)/len(valid) if complete else None
        gap=mean-Decimal(j['value_decimal']) if complete else None
        result['rows'].append({'date':j['date'],'value_decimal':str(gap) if gap is not None else None,
          'us_mean_decimal':str(mean) if mean is not None else None,'japan_value_decimal':j['value_decimal'],
          'us_numeric_observations':len(valid),'us_missing_records':len(records)-len(valid),'minimum_us_observations':required,
          'us_calendar_completeness_verified':usd=='DFF' and len(valid)==required,
          'japan_segment':j['segment'],'japan_row_index':j['row_index'],
          'us_refs':[{'date':r['date'],'segment':r['segment'],'row_index':r['row_index']} for r in valid],
          'status':'dated_comparison' if gap is not None else 'monthly_input_incomplete'})
    latest=result['rows'][-1]
    result['components']=latest
    if jp['quality']['status']!='fresh' or us['quality']['status']!='fresh':result['status']='source_not_current'
    elif latest['value_decimal'] is not None:result.update(status='dated_comparison',value_decimal=latest['value_decimal'],value=float(latest['value_decimal']))
    else:result['status']='latest_month_incomplete'
    return result

def fx_measurement(item):
    result={'quote':'JPY per USD; a decline means yen appreciation','changes':{},'volatility':{},'call':None}
    if not item:return result
    rows=item['rows'];last=rows[-1]
    for label,days in (('1m',30),('3m',91),('6m',182)):
        target=date.fromisoformat(last['date'])-timedelta(days=days)
        base=next((r for r in reversed(rows) if r['date']<=target.isoformat()),None)
        valid=base and 0<=(target-date.fromisoformat(base['date'])).days<=4 and base['value_decimal'] is not None and last['value_decimal'] is not None
        value=Decimal(last['value_decimal']) if last['value_decimal'] is not None else None
        previous=Decimal(base['value_decimal']) if valid else None
        delta=100*(value/previous-1) if valid and previous>0 and value>0 else None
        result['changes'][label]={'target':target.isoformat(),'from':base['date'] if base else None,'to':last['date'],
          'percent_decimal':str(delta) if delta is not None else None,'max_calendar_tolerance_days':4,
          'baseline_ref':{'segment':base['segment'],'row_index':base['row_index']} if base else None,
          'status':'available' if delta is not None else 'endpoint_missing'}
    for n in (20,60):
        sample=[r for r in rows if r['value_decimal'] is not None][-n-1:]
        valid=(last['value_decimal'] is not None and len(sample)==n+1 and all(Decimal(r['value_decimal'])>0 for r in sample)
            and all(1<=(date.fromisoformat(b['date'])-date.fromisoformat(a['date'])).days<=6 for a,b in zip(sample,sample[1:])))
        returns=[math.log(float(b['value_decimal'])/float(a['value_decimal'])) for a,b in zip(sample,sample[1:])] if valid else []
        result['volatility'][str(n)]={'returns':len(returns),'annualized_pct':statistics.stdev(returns)*math.sqrt(252)*100 if valid else None,
          'from':sample[0]['date'] if sample else None,'to':sample[-1]['date'] if sample else None,
          'formula':'Sample standard deviation of consecutive available log returns * sqrt(252) * 100',
          'excluded_missing_records':sum(r['value_decimal'] is None and sample[0]['date']<=r['date']<=sample[-1]['date'] for r in rows) if sample else 0,
          'imputation':False,'max_gap_days':6,'limitation':'Available fixing returns; gaps span holidays or missing fixings. Not intraday volatility or a forward forecast.'}
    return result

def shard(item,artifacts):
    rows=item.pop('rows',[])
    if not rows:item['history']=None;return
    history={'contract':'yen-history.v1','id':item['id'],'unit':item['unit'],'frequency':item['frequency'],'rows':rows}
    body=encoded(history);sha=digest(history);key=PREFIX+'histories/'+sha+'.json';artifacts[key]=body
    item['history']={'key':key,'sha256':sha,'bytes':len(body),'observations':len(rows),'from':rows[0]['date'],'to':rows[-1]['date']}

def build(inputs,read,at):
    if inputs.get('contract')!='yen-original-inputs.v1':raise ValueError('yen input contract differs')
    measured={};errors=dict(inputs.get('acquisition_errors',{}));clocks={}
    for sid in native.SERIES:
        refs=inputs.get('fred',{}).get(sid)
        if refs is None:errors[sid]='original_source_unavailable';continue
        try:measured[sid]=native.fred(sid,refs,read,at);clocks[sid]=measured[sid]['quality']['acquired_at']
        except (ValueError,TypeError,KeyError,ArithmeticError) as exc:errors[sid]='validation_'+type(exc).__name__
    positioning=None
    if inputs.get('cftc'):
        try:positioning=native.cftc(inputs['cftc'],read,at);clocks['CFTC']=positioning['quality']['acquired_at']
        except (ValueError,TypeError,KeyError,ArithmeticError) as exc:errors['CFTC']='validation_'+type(exc).__name__
    else:errors['CFTC']='original_source_unavailable'
    comparisons={k:monthly_pair(measured,k,usd,jpy,at) for k,usd,jpy in (
        ('front_end_proxy','DFF','IR3TIB01JPM156N'),('ten_year_yield_gap','DGS10','IRLTLT01JPM156N'))}
    assets=measured.get('JPNASSETS');fx=fx_measurement(measured.get('DEXJPUS'))
    changes={sid:{str(n):month_change(item,n) for n in (6,12)} for sid,item in measured.items() if item['frequency']=='M'}
    states=Counter(v['quality']['status'] for v in measured.values())
    if positioning:states[positioning['quality']['status']]+=1
    fresh=states['fresh'];quality={'status':'fresh' if fresh==8 and not errors else 'partial' if measured or positioning else 'unavailable',
        'states':dict(states),'required_source_families':8,'available_source_families':len(measured)+bool(positioning),
        'basis':'Per-series observation and acquisition clocks. Daily H.10 fixings use nominal weekly release cadence; no verified holiday calendar.'}
    output={'contract':CONTRACT,'schema_version':'3.0','generated_at':at,'source_clocks':clocks,'quality':quality,
        'headline':'Dated yen funding, FX and futures positioning; no calibrated unwind forecast.',
        'measurements':measured,'comparisons':comparisons,'monthly_changes':changes,'fx_measurement':fx,'positioning':positioning,
        'boj_assets_trillion_jpy_decimal':str(Decimal(assets['value_decimal'])/10000) if assets and assets['value_decimal'] is not None else None,
        'boj_asset_conversion':'Native 100 million JPY / 10000 = trillion JPY; total assets are not an attributed easing impulse.',
        'carry_regime':'NOT_CALIBRATED','unwind_risk_score':None,'unwind_risk_label':'NOT_CALIBRATED','unwind_risk_components':{},
        'boj_injection_score':None,'boj_stance_label':'NOT_ATTRIBUTED','carry_attractiveness':'NOT_CALIBRATED','triggers':[],
        'carry_width':{'front_end_carry_pp':None,'duration_carry_pp':None,'executable_carry':None,'hedged_carry_return':None,
            'missing':['executable funding and investment quotes','FX forward points and cross-currency basis','fees and margin','credit and duration exposures']},
        'legacy':inputs['legacy'],'errors':errors,'history_basis':'Complete bounded current-vintage FRED queries since 2000; complete queried CFTC JPY futures-only history. No backfilled publication-time knowledge.',
        'decision_boundary':'Original-source measurements can inform research. No independent vote, position size, unwind probability or automated recommendation is qualified.',
        'portfolio_consequence':'User-entered unhedged JPY borrowing / USD investment scenario on the page. Rates, FX, fees and day-count assumptions are explicit; no account read or order.',
        **AUTHORITY}
    artifacts={}
    for item in [*measured.values(),*comparisons.values(),*([positioning] if positioning else [])]:shard(item,artifacts)
    return output,artifacts
