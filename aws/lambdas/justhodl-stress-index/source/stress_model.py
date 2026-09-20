"""Pure original-source stress research. Descriptive measurements confer no trading authority."""
import calendar
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from urllib.parse import parse_qs, urlsplit

CONTRACT='stress-research.v1'
PREFIX='data/stress-research/'
CURRENT='data/jsi.json'
HISTORY_START='1990-01-01'
PERMISSIONS=dict(calls_eligible=False,sizing_eligible=False,execution_eligible=False)
# Official unit and native frequency are part of the reviewed definition, not inferred from magnitude.
SERIES={
    'VIXCLS': ('Index','D','Market prices and spreads'),
    'NFCI': ('Index','W','Published financial-condition indices'),
    'KCFSI': ('Index','M','Published financial-condition indices'),
    'STLFSI4': ('Index','W','Published financial-condition indices'),
    'BAMLH0A0HYM2': ('Percent','D','Market prices and spreads'),
    'T10Y2Y': ('Percent','D','Market prices and spreads'),
    'BAMLC0A0CM': ('Percent','D','Market prices and spreads'),
    'WRESBAL': ('Millions of U.S. Dollars','W','Central-bank balance sheet and funding'),
    'WALCL': ('Millions of U.S. Dollars','W','Central-bank balance sheet and funding'),
    'RRPONTSYD': ('Billions of US Dollars','D','Central-bank balance sheet and funding'),
    'SOFR': ('Percent','D','Central-bank balance sheet and funding'),
    'IORB': ('Percent','D','Central-bank balance sheet and funding'),
    'NASDAQCOM': ('Index Feb 5, 1971=100','D','Market benchmark'),
}
NOTES={
    'NFCI':'Positive means tighter than its historical average; negative means looser. History is revised. Underlying market inputs overlap other indices.',
    'KCFSI':'Monthly reference period, not a daily signal. Positive means stress above its long-run average; negative means below.',
    'STLFSI4':'Published version 4 stress index. Positive means above-average stress. Historical values may be revised.',
    'WRESBAL':'Week-average reserve balances. This is a stock in USD millions, not a daily cash flow or a forecast.',
    'WALCL':'Wednesday balance-sheet level. A change is not automatically easing or risk-asset inflow.',
    'RRPONTSYD':'Daily reverse-repo stock in USD billions. Its change alone does not establish market liquidity or a funding crisis.',
    'T10Y2Y':'10-year minus 2-year Treasury yield spread. Inversion is not itself a calibrated short-horizon stress or trade probability.',
    'NASDAQCOM':'Price index only; not QQQ total return, investable execution prices, or evidence of strategy performance.',
}
METHOD_URLS={
    'NFCI':'https://www.chicagofed.org/research/data/nfci/current-data',
    'KCFSI':'https://www.kansascityfed.org/data-and-trends/kansas-city-financial-stress-index/',
    'STLFSI4':'https://fredblog.stlouisfed.org/2022/11/the-st-louis-feds-financial-stress-index-version-4/',
}


def encoded(value):return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
def digest(value):return hashlib.sha256(encoded(value)).hexdigest()


def clock(value):
    at=datetime.fromisoformat(value.replace('Z','+00:00'))
    if at.tzinfo is None:raise ValueError('timezone required')
    return at.astimezone(timezone.utc)


def decimal(value):
    if value in (None,'','.') or isinstance(value,bool):return None
    try:
        n=Decimal(str(value))
        return n if n.is_finite() and abs(n)<Decimal('1e100') else None
    except (InvalidOperation,ValueError):return None


def safe_url(value,path):
    u=urlsplit(value)
    if u.scheme!='https' or u.netloc!='api.stlouisfed.org' or u.path!=path or u.fragment:raise ValueError('provider request identity differs')
    q=parse_qs(u.query,keep_blank_values=True)
    if any(len(v)!=1 or k.lower() in ('apikey','api_key','token','authorization') for k,v in q.items()):raise ValueError('ambiguous or credential-bearing request')
    return {k:v[0] for k,v in q.items()}


def original(inputs,bodies,name):
    ref=inputs['sources'].get(name) or {}
    if ref.get('status')!='captured':raise ValueError('source unavailable')
    raw=bodies[name]
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('source bytes differ')
    if not 0<=(clock(inputs['generated_at'])-clock(ref['acquired_at'])).total_seconds()<=3600:raise ValueError('source acquisition outside run')
    if hashlib.sha256(ref['request_url'].encode()).hexdigest()!=ref['request_sha256']:raise ValueError('request hash differs')
    doc=json.loads(raw)
    if not isinstance(doc,dict):raise ValueError('provider object required')
    return doc,ref


def percentile(rows,years,frequency):
    latest=rows[-1];end=date.fromisoformat(latest['date'])
    start=end.replace(year=end.year-years,day=min(end.day,calendar.monthrange(end.year-years,end.month)[1]))
    window=[r for r in rows if r['date']>=str(start)];finite=[r for r in window if r['decimal'] is not None]
    first=next((r for r in rows if r['decimal'] is not None),None)
    grace={'D':7,'W':14,'M':40}[frequency];minimum={'D':200,'W':45,'M':10}[frequency]*years
    span=first is not None and first['date']<=str(start+timedelta(days=grace))
    qualified=latest['decimal'] is not None and span and len(finite)>=minimum
    rank=None
    if qualified:
        value=Decimal(latest['decimal']);values=[Decimal(r['decimal']) for r in finite]
        rank=100*(sum(v<value for v in values)+.5*sum(v==value for v in values))/len(values)
    return {'value':rank,'unit':'percentile_0_100','window_start':str(start),'window_end':str(end),
        'n_finite':len(finite),'n_missing':len(window)-len(finite),'minimum_finite':minimum,'complete_span':span,
        'reason':None if qualified else 'missing_latest_or_insufficient_observed_history',
        'formula':'100 * (count_below + 0.5 * count_equal) / n_finite; includes latest',
        'vintage_basis':'current_vintage','predictive_probability':False}


def change(rows,frequency,unit):
    latest=rows[-1];day=date.fromisoformat(latest['date'])
    if frequency=='M':
        month=day.year*12+day.month-1-3;y,mo=divmod(month,12)
        target=date(y,mo+1,min(day.day,calendar.monthrange(y,mo+1)[1]));label='3 calendar months'
    else:target=day-timedelta(weeks=13);label='13 calendar weeks'
    baseline=next((r for r in rows if r['date']==str(target)),None)
    value=Decimal(latest['decimal'])-Decimal(baseline['decimal']) if baseline and baseline['decimal'] is not None and latest['decimal'] is not None else None
    return {'value':float(value) if value is not None else None,'decimal':str(value) if value is not None else None,
        'unit':'percentage_points' if unit=='Percent' else unit,'label':label,'current_date':str(day),
        'target_date':str(target),'baseline_date':baseline['date'] if baseline else None,
        'current_row_index':latest['row_index'],'baseline_row_index':baseline['row_index'] if baseline else None,
        'reason':None if value is not None else 'missing_exact_dated_comparison',
        'formula':'Latest minus exact dated baseline; no row-count substitution or forward fill'}


def fred(inputs,bodies,sid):
    definition,dr=original(inputs,bodies,'definition:'+sid);document,ref=original(inputs,bodies,'observations:'+sid)
    end=date.fromisoformat(inputs['evaluation_date']);base=dict(series_id=sid,file_type='json',realtime_start=str(end),realtime_end=str(end))
    if safe_url(dr['request_url'],'/fred/series')!=base:raise ValueError('definition query differs')
    query={**base,'observation_start':HISTORY_START,'observation_end':str(end),'units':'lin','sort_order':'asc','limit':'20000','offset':'0','output_type':'1'}
    if safe_url(ref['request_url'],'/fred/series/observations')!=query:raise ValueError('observation query differs')
    unit,frequency,group=SERIES[sid];metas=definition.get('seriess') or []
    if len(metas)!=1 or metas[0].get('id')!=sid or metas[0].get('units')!=unit or metas[0].get('frequency_short')!=frequency:raise ValueError('official definition differs')
    meta=metas[0]
    if meta.get('seasonal_adjustment')!='Not Seasonally Adjusted':raise ValueError('seasonal adjustment differs')
    for k,v in [('units','lin'),('sort_order','asc'),('output_type',1),('offset',0),('limit',20000),('realtime_start',str(end)),('realtime_end',str(end))]:
        if document.get(k)!=v:raise ValueError('provider response differs: '+k)
    source_rows=document.get('observations')
    if not isinstance(source_rows,list) or not source_rows or type(document.get('count')) is not int or len(source_rows)!=document['count'] or len(source_rows)>20000:raise ValueError('incomplete provider history')
    rows=[]
    for i,r in enumerate(source_rows):
        day=date.fromisoformat(r['date'])
        if not HISTORY_START<=str(day)<=str(end) or (rows and r['date']<=rows[-1]['date']):raise ValueError('invalid observation ordering or bounds')
        if r.get('realtime_start')!=str(end) or r.get('realtime_end')!=str(end):raise ValueError('mixed source vintages')
        value=decimal(r.get('value'))
        if r.get('value') not in ('.','',None) and value is None:raise ValueError('invalid numeric source observation')
        if value is not None and sid not in ('NFCI','KCFSI','STLFSI4','T10Y2Y','SOFR','IORB') and value<0:raise ValueError('negative source level')
        if value is not None and not meta['observation_start']<=str(day)<=meta['observation_end']:raise ValueError('finite observation outside official span')
        rows.append({'date':str(day),'value':float(value) if value is not None else None,'decimal':str(value) if value is not None else None,'row_index':i})
    latest=rows[-1];age=(end-date.fromisoformat(latest['date'])).days;ceiling={'D':7,'W':16,'M':75}[frequency]
    status='unavailable' if latest['value'] is None else 'stale' if age>ceiling else 'fresh'
    finite=[r for r in rows if r['value'] is not None]
    return {'series_id':sid,'title':meta['title'],'group':group,'value':latest['value'],'value_decimal':latest['decimal'],
        'unit':unit,'frequency':meta['frequency'],'frequency_short':frequency,'seasonal_adjustment':meta['seasonal_adjustment'],
        'observation_date':latest['date'],'observation_period':latest['date'][:7] if frequency=='M' else latest['date'],
        'date_semantics':'Monthly reference period represented by first day; not publication date' if frequency=='M' else 'Native provider observation date; not publication date',
        'source_row_index':latest['row_index'],'provider_updated_at':meta.get('last_updated'),'first_publication_at':None,
        'acquired_at':ref['acquired_at'],'current_vintage_date':str(end),
        'quality':{'status':status,'age_days':age,'max_age_days':ceiling,'basis':'observation_date',
            'note':'Conservative native-frequency calendar-age ceiling; no release-calendar or point-in-time availability assertion.'},
        'history_span':{'first_finite':finite[0]['date'] if finite else None,'last_finite':finite[-1]['date'] if finite else None,
            'n_finite':len(finite),'n_missing':len(rows)-len(finite),'requested_start':HISTORY_START,'provider_start':meta['observation_start']},
        'change':change(rows,frequency,unit),'percentiles':{str(y)+'y':percentile(rows,y,frequency) for y in (2,5,10)},
        'history':rows,'originals':{'definition':dr,'observations':ref},'method_url':METHOD_URLS.get(sid,'https://fred.stlouisfed.org/series/'+sid),
        'interpretation':NOTES.get(sid,'Descriptive native provider measurement; no calibrated probability or trading threshold.'),**PERMISSIONS}


def spread(measurements,left,right,label):
    a,b=measurements.get(left,{}),measurements.get(right,{})
    day=a.get('observation_date');target=next((r for r in b.get('history',[]) if r['date']==day),None)
    value=decimal(a.get('value_decimal'));other=decimal(target.get('decimal')) if target else None
    valid=value is not None and other is not None
    return {'label':label,'value':float((value-other)*100) if valid else None,'unit':'basis_points',
        'observation_date':day,'left':left,'right':right,'right_latest_date':b.get('observation_date'),
        'left_row_index':a.get('source_row_index'),'right_row_index':target.get('row_index') if target else None,
        'formula':'('+left+' - '+right+') * 100 at the latest '+left+' observation date; both native units Percent',
        'reason':None if valid else 'missing_same_date_observation','quality':a.get('quality',{'status':'unavailable'}),**PERMISSIONS}


def build(inputs,bodies):
    if inputs.get('contract')!='stress-inputs.v1' or str(clock(inputs['generated_at']).date())!=inputs['evaluation_date']:raise ValueError('input contract or date differs')
    measurements={};failures={}
    for sid in SERIES:
        try:measurements[sid]=fred(inputs,bodies,sid)
        except (KeyError,ValueError,TypeError,AttributeError,IndexError) as exc:
            failures[sid]=str(exc) if isinstance(exc,ValueError) else type(exc).__name__
            measurements[sid]={'series_id':sid,'value':None,'unit':SERIES[sid][0],'group':SERIES[sid][2],
                'quality':{'status':'unavailable'},'history':[],**PERMISSIONS}
    return {'contract':CONTRACT,'version':'2.0.0','generated_at':inputs['generated_at'],'ok':not failures,
        'measurements':measurements,'spreads':{'sofr_iorb':spread(measurements,'SOFR','IORB','Secured funding minus reserve remuneration'),
            'hy_ig':spread(measurements,'BAMLH0A0HYM2','BAMLC0A0CM','High-yield minus investment-grade OAS')},
        'context':inputs.get('context',{}),'source_failures':failures,
        'quality':{'status':'research_only','fresh_native_series':sum(r['quality']['status']=='fresh' for r in measurements.values()),
            'native_series_expected':len(SERIES),'history_basis':'current-vintage descriptive observations, not as-known-then data'},
        'dependency_note':'NFCI, KCFSI, STLFSI4 and market measures share financial inputs. No independence count, composite vote or forecast weight is assigned.',
        'decision':{'verb':'WAIT','meaning':'abstain','reason':'No independently qualified JSI forecasting or sizing model.'},
        'decision_qualification':{'status':'research_only','model_id':None,'scorecard_manifest_key':None},
        # Compatibility fields cannot silently preserve the old unqualified model authority.
        'jsi':None,'jsi_spine':None,'overlay_score':None,'regime':None,'percentile_since_1990':None,
        'history_span':None,'historical_extremes':None,'spine_components':{},'spine_sparks':{},'spine_meta':[],
        'spine_weight_mode':'unqualified','overlay_components':[],'n_overlay_live':0,'crisis_markers':[],
        'series_weekly':[],'v2':None,'v2_error':'Legacy full-sample return atlas and fixed-confidence signals are unqualified.',
        'methodology':{'native_frequency':'No forward-filled synthetic daily grid. Each observation keeps its own date and unit.',
            'history':'Requested from 1990; actual finite span is per series. Limited history cannot earn a longer-window percentile.',
            'portfolio':'No learned weights, automatic signals or position scaling. Explicit portfolio scenarios remain separate.'},**PERMISSIONS}
