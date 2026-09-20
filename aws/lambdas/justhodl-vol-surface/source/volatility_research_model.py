"""Source-defined volatility indices and matched-date descriptive research.

Daily index observations are not an options surface, futures curve, hedge quote,
real-world tail probability, covariance estimate or a qualified investment vote.
Whole source rows remain available to the protected original-replay path.
"""
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,InvalidOperation,localcontext
import csv,hashlib,io,json,math,re,urllib.parse

CONTRACT='volatility-native-research.v1'
PREFIX='data/volatility-research/'
PRIVATE='audit-private/20260909-originals/volatility-research/'
PERMISSIONS={'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False}
MAX_BYTES=4*1024*1024
MAX_ROWS=15000
HISTORY_DAYS=3660
SOURCE_AGE_CEILING_DAYS=5
PIPELINE_HOURS=36
CATALOG={'VIX_30D': {'fred': 'VIXCLS', 'for': 'SPX', 'tenor_d': 30, 'tier': 'index'}, 'VIX_3M': {'fred': 'VXVCLS', 'for': 'SPX', 'tenor_d': 90, 'tier': 'index'}, 'NDX_VOL': {'fred': 'VXNCLS', 'for': 'NDX', 'tenor_d': 30, 'tier': 'index'}, 'RUT_VOL': {'fred': 'RVXCLS', 'for': 'RUT', 'tenor_d': 30, 'tier': 'index'}, 'DJIA_VOL': {'fred': 'VXDCLS', 'for': 'DJIA', 'tenor_d': 30, 'tier': 'index'}, 'EEM_VOL': {'fred': 'VXEEMCLS', 'for': 'EEM', 'tenor_d': 30, 'tier': 'intl'}, 'EWZ_VOL': {'fred': 'VXEWZCLS', 'for': 'EWZ', 'tenor_d': 30, 'tier': 'intl'}, 'OIL_VOL': {'fred': 'OVXCLS', 'for': 'USO', 'tenor_d': 30, 'tier': 'commodity'}, 'GOLD_VOL': {'fred': 'GVZCLS', 'for': 'GLD', 'tenor_d': 30, 'tier': 'commodity'}, 'AAPL_VOL': {'fred': 'VXAPLCLS', 'for': 'AAPL', 'tenor_d': 30, 'tier': 'single_name'}, 'GOOG_VOL': {'fred': 'VXGOGCLS', 'for': 'GOOG', 'tenor_d': 30, 'tier': 'single_name'}, 'AMZN_VOL': {'fred': 'VXAZNCLS', 'for': 'AMZN', 'tenor_d': 30, 'tier': 'single_name'}, 'GS_VOL': {'fred': 'VXGSCLS', 'for': 'GS', 'tenor_d': 30, 'tier': 'single_name'}, 'IBM_VOL': {'fred': 'VXIBMCLS', 'for': 'IBM', 'tenor_d': 30, 'tier': 'single_name'}}
PUBLISHER=('VVIX','SKEW')
LABELS=tuple(sorted((*CATALOG,*PUBLISHER)))
SINGLE_NAMES=('AAPL_VOL','GOOG_VOL','AMZN_VOL','GS_VOL','IBM_VOL')

def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode('utf-8')


def sha(raw): return hashlib.sha256(raw).hexdigest()


def stamp(value):
    if not isinstance(value,str): raise ValueError('Explicit timestamp required')
    result=datetime.fromisoformat(value.replace('Z','+00:00'))
    if result.tzinfo is None: raise ValueError('Timezone required')
    return result.astimezone(timezone.utc)


def number(value):
    if value=='.': return None
    if not isinstance(value,str) or len(value)>32 or not re.fullmatch(r'-?\d{1,6}(?:\.\d{1,12})?',value):
        raise ValueError('Canonical finite provider decimal required')
    try: out=Decimal(value)
    except InvalidOperation: raise ValueError('Invalid decimal') from None
    if not out.is_finite() or not math.isfinite(float(out)): raise ValueError('Nonfinite observation')
    return out


def shown(value):
    if value is None: return None
    if not value.is_finite() or not math.isfinite(float(value)): raise ValueError('Arithmetic overflow')
    rounded=value.quantize(Decimal('0.000001'))
    return float(value if rounded==0 and value!=0 else rounded)


def json_object(raw):
    if not isinstance(raw,bytes) or len(raw)>MAX_BYTES: raise ValueError('Bounded original bytes required')
    def pairs(items):
        result={}
        for key,value in items:
            if key in result: raise ValueError('Duplicate JSON member')
            result[key]=value
        return result
    value=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,
                     parse_constant=lambda _: (_ for _ in ()).throw(ValueError('Nonfinite JSON')))
    if not isinstance(value,dict): raise ValueError('Original object required')
    return value


def statistics(rows,value,count):
    """Last N reported numeric observations, with actual span and missingness.

    This is not N trading days. Null records inside the span are counted and
    left unfilled. The current value may not jump over a missing latest row.
    """
    numeric=[r for r in rows if r['value'] is not None]
    chosen=numeric[-count:]
    out={'requested_observations':count,'numeric_observations':len(chosen),
         'first_date':chosen[0]['date'] if chosen else None,'last_date':chosen[-1]['date'] if chosen else None,
         'mean_index_points':None,'sample_stddev_points':None,'z_score':None,'missing_rows_inside_span':0,
         'basis':'Last N numeric provider observations, inclusive; actual dates, not assumed trading-day counts.',
         'status':'insufficient_history',**PERMISSIONS}
    if chosen: out['missing_rows_inside_span']=sum(r['value'] is None and chosen[0]['date']<=r['date']<=chosen[-1]['date'] for r in rows)
    if len(chosen)!=count: return out
    with localcontext() as arithmetic:
        arithmetic.prec=40
        mean=sum((r['value'] for r in chosen),Decimal(0))/Decimal(count)
        sd=(sum(((r['value']-mean)**2 for r in chosen),Decimal(0))/Decimal(count-1)).sqrt()
        out.update(mean_index_points=shown(mean),sample_stddev_points=shown(sd),
                   z_score=shown((value-mean)/sd) if value is not None and sd else None,
                   status='constant_window' if not sd else 'latest_unavailable' if value is None else 'available')
    return out


def percentile(rows,value,start):
    """Midrank empirical percentile over an explicit dated reference window."""
    selected=[r for r in rows if r['date']>=start]
    values=[r['value'] for r in selected if r['value'] is not None]
    complete=bool(rows and rows[0]['date']<=start)
    out={'requested_start':start,'first_returned_date':selected[0]['date'] if selected else None,
         'last_returned_date':selected[-1]['date'] if selected else None,'numeric_observations':len(values),
         'missing_observations':len(selected)-len(values),'full_requested_span_available':complete,
         'percentile_pct':None,'formula':'100 * (count below + 0.5 * count equal) / numeric observations',
         'predictive_probability':False,'status':'insufficient_history'}
    if not complete or not values or value is None: return out
    with localcontext() as arithmetic:
        arithmetic.prec=40
        rank=Decimal(sum(v<value for v in values))+Decimal('0.5')*sum(v==value for v in values)
        out.update(percentile_pct=shown(100*rank/Decimal(len(values))),status='available')
    return out




def source_url(label,kind,evaluation):
    day=date.fromisoformat(evaluation)
    if label in PUBLISHER:
        if kind!='observations':raise ValueError('Publisher observations required')
        return 'https://cdn-api.cboe.com/api/global/us_indices/daily_prices/'+label+'_History.csv'
    if label not in CATALOG or kind not in ('definition','observations'):raise ValueError('Unreviewed source')
    q={'series_id':CATALOG[label]['fred'],'file_type':'json','realtime_start':str(day),'realtime_end':str(day)}
    if kind=='observations':q.update(observation_start=str(day-timedelta(days=HISTORY_DAYS)),observation_end=str(day),units='lin',output_type=1,sort_order='asc',limit=100000,offset=0)
    return 'https://api.stlouisfed.org/fred/'+('series' if kind=='definition' else 'series/observations')+'?'+urllib.parse.urlencode(q)


def parse_fred(label,definition_raw,observations_raw,evaluation):
    sid=CATALOG[label]['fred'];at=date.fromisoformat(evaluation)
    ds=json_object(definition_raw).get('seriess');doc=json_object(observations_raw)
    if not isinstance(ds,list) or len(ds)!=1 or ds[0].get('id')!=sid:raise ValueError('Definition identity differs')
    definition=ds[0]
    if any(definition.get(k)!=evaluation or doc.get(k)!=evaluation for k in ('realtime_start','realtime_end')):raise ValueError('Current vintage differs')
    if definition.get('units')!='Index' or definition.get('frequency_short')!='D' or definition.get('seasonal_adjustment_short')!='NSA':raise ValueError('Native index definition differs')
    title=definition.get('title')
    if not isinstance(title,str) or len(title)>300 or not title.startswith('CBOE ') or not ('Volatility Index' in title or 'Equity VIX' in title):raise ValueError('Publisher index title differs')
    expected={'units':'lin','output_type':1,'offset':0,'sort_order':'asc','limit':100000,'observation_start':str(at-timedelta(days=HISTORY_DAYS)),'observation_end':evaluation}
    if any(doc.get(k)!=v or type(doc.get(k)) is not type(v) for k,v in expected.items()):raise ValueError('Requested response window differs')
    source=doc.get('observations')
    if not isinstance(source,list) or not 1<=len(source)<=MAX_ROWS or type(doc.get('count')) is not int or doc['count']!=len(source):raise ValueError('Complete response required')
    rows=[];previous=None
    for index,row in enumerate(source):
        if not isinstance(row,dict):raise ValueError('Observation object required')
        day=date.fromisoformat(row['date'])
        if str(day)!=row['date'] or not at-timedelta(days=HISTORY_DAYS)<=day<=at or previous is not None and day<=previous:raise ValueError('Duplicate, unordered or out-of-window row')
        if any(row.get(k)!=evaluation for k in ('realtime_start','realtime_end')):raise ValueError('Mixed row vintage')
        value=number(row.get('value'))
        if value is not None and not Decimal(0)<=value<=Decimal(10000):raise ValueError('Index value outside reviewed range')
        rows.append({'date':str(day),'value':value,'row_index':index,'raw_value':row['value']});previous=day
    return definition,rows


def parse_publisher(symbol,raw,evaluation):
    if symbol not in PUBLISHER or not isinstance(raw,bytes) or len(raw)>MAX_BYTES:raise ValueError('Bounded publisher original required')
    records=list(csv.reader(io.StringIO(raw.decode('utf-8-sig')),strict=True))
    if not records or records[0]!=['DATE',symbol] or not 1<=len(records)-1<=MAX_ROWS:raise ValueError('Publisher CSV header or complete rows differ')
    at=date.fromisoformat(evaluation);previous=None;rows=[]
    for index,row in enumerate(records[1:]):
        if len(row)!=2 or not re.fullmatch(r'\d{2}/\d{2}/\d{4}',row[0]):raise ValueError('Two labelled CSV fields required')
        day=datetime.strptime(row[0],'%m/%d/%Y').date()
        if not date(1980,1,1)<=day<=at or previous is not None and day<=previous:raise ValueError('Duplicate, unordered or future publisher date')
        value=number(row[1])
        if value is not None and not Decimal(0)<=value<=Decimal(10000):raise ValueError('Publisher index outside reviewed range')
        rows.append({'date':str(day),'value':value,'row_index':index+1,'raw_value':row[1]});previous=day
    return {'id':symbol,'title':'Cboe VIX of VIX Index' if symbol=='VVIX' else 'Cboe SKEW Index','units':'Index','frequency':'Daily, close','seasonal_adjustment':'Not applicable','last_updated':None},rows


def native(value):
    # Native values retain provider precision. Only displayed statistics round.
    if value is None:return None
    if not value.is_finite() or not math.isfinite(float(value)):raise ValueError('Nonfinite arithmetic')
    return float(value)


def identity(label):
    if label in CATALOG:
        m=CATALOG[label]
        return {'label':label,'series_id':m['fred'],'provider':'Cboe via FRED','source_url':'https://fred.stlouisfed.org/series/'+m['fred'],
            'underlying':m['for'],'tenor_days':m['tenor_d'],'tier':m['tier'],'unit':'index_points',**PERMISSIONS}
    return {'label':label,'series_id':label,'provider':'Cboe','source_url':'https://www.cboe.com/us/indices/dashboard/'+label.lower()+'/',
        'underlying':'VIX options' if label=='VVIX' else 'SPX options','tenor_days':30,'tier':'vol_of_vol' if label=='VVIX' else 'skew_index','unit':'index_points',**PERMISSIONS}


def metric(label,definition,rows,collected_at):
    at=stamp(collected_at);last=rows[-1];previous=rows[-2] if len(rows)>1 else None
    value=last['value'];prior=previous['value'] if previous else None;delta=value-prior if value is not None and prior is not None else None
    numeric=[r for r in rows if r['value'] is not None]
    age=(at.date()-date.fromisoformat(last['date'])).days
    status='unavailable' if value is None else 'stale' if age>SOURCE_AGE_CEILING_DAYS else 'within_age_ceiling'
    source_deadline=datetime.combine(date.fromisoformat(last['date'])+timedelta(days=SOURCE_AGE_CEILING_DAYS+1),datetime.min.time(),timezone.utc)
    return {**identity(label),'definition':definition['title'],'source_unit':definition['units'],'frequency':definition['frequency'],
        'seasonal_adjustment':definition['seasonal_adjustment'],'provider_updated_at':definition.get('last_updated'),'published_at':None,
        'value':native(value),'observation_date':last['date'],'collected_at':collected_at,'original_row_index':last['row_index'],
        'previous_value':native(prior),'previous_observation_date':previous['date'] if previous else None,'change_points':native(delta),
        'change_relative_pct':native(100*delta/prior) if delta is not None and prior else None,
        'comparison_gap_days':(date.fromisoformat(last['date'])-date.fromisoformat(previous['date'])).days if previous else None,
        'comparison_basis':'Previous provider row; missing rows are not skipped and calendar gaps are explicit.',
        'exact':{'value':str(value) if value is not None else None,'previous':str(prior) if prior is not None else None,'change_points':str(delta) if delta is not None else None},
        'latest_numeric_context':{'value':native(numeric[-1]['value']),'observation_date':numeric[-1]['date']} if numeric else None,
        'quality':{'status':status,'observation_age_days':age,'max_observation_age_days':SOURCE_AGE_CEILING_DAYS,'release_calendar_verified':False,'historical_availability_verified':False},
        'source_valid_until':source_deadline.isoformat(),
        'history_coverage':{'first_date':rows[0]['date'],'last_date':last['date'],'provider_rows':len(rows),'numeric_rows':len(numeric),'null_rows':len(rows)-len(numeric),
            'weekend_numeric_rows':sum(date.fromisoformat(r['date']).weekday()>=5 for r in numeric),'current_response_vintage':at.date().isoformat(),'point_in_time_history':False},
        'descriptive_statistics':{'observation_windows':{str(n):statistics(rows,value,n) for n in (60,252)},
            'calendar_365_day_percentile':percentile(rows,value,str(date.fromisoformat(last['date'])-timedelta(days=365)))}}


def matched(labels,measurements,all_rows):
    ledgers=[{r['date']:r for r in all_rows.get(label,[])} for label in labels]
    days=set.intersection(*(set(ledger) for ledger in ledgers)) if ledgers else set()
    day=max(days) if days else None
    rows=[ledger.get(day) for ledger in ledgers]
    values=[row['value'] if row else None for row in rows]
    same_latest=bool(day and all(measurements[l].get('observation_date')==day for l in labels))
    available=same_latest and all(measurements[l]['quality']['status']=='within_age_ceiling' for l in labels) and all(v is not None for v in values)
    return {'observation_date':day,'latest_dates':{l:measurements[l].get('observation_date') for l in labels},
        'both_latest_dates_match':same_latest,'current_comparison_available':available,
        'original_row_indices':{l:r['row_index'] if r else None for l,r in zip(labels,rows)},
        'status':'available' if available else 'unavailable' if any(v is None for v in values) else 'lagged_context',**PERMISSIONS},values


def comparisons(measurements,all_rows):
    term,values=matched(('VIX_30D','VIX_3M'),measurements,all_rows);a,b=values
    delta=a-b if a is not None and b is not None else None
    term.update(left_label='VIX_30D',right_label='VIX_3M',difference_points=native(delta),ratio=native(a/b) if a is not None and b else None,
        relation=None if delta is None else '30d_above_3m' if delta>0 else '30d_below_3m' if delta<0 else 'equal',
        formula='30-day index minus 3-month index; ratio divides the two indices on one common observation date.',
        interpretation='Constant-maturity implied-volatility indices, not VIX futures prices, roll yield, option quotes or a panic classification.')
    ratios={}
    for label in ('NDX_VOL','RUT_VOL','DJIA_VOL','EEM_VOL','EWZ_VOL','OIL_VOL','GOLD_VOL'):
        row,values=matched((label,'VIX_30D'),measurements,all_rows);a,b=values
        row.update(numerator_label=label,denominator_label='VIX_30D',ratio=native(a/b) if a is not None and b else None,
            interpretation='Different underlying assets and option baskets. A ratio is descriptive and does not establish stress, relative hedge cost or an executable trade.')
        ratios[label]=row
    dispersion,values=matched(SINGLE_NAMES,measurements,all_rows)
    valid=all(v is not None for v in values)
    mean=sum(values)/len(values) if valid else None
    with localcontext() as arithmetic:
        arithmetic.prec=40
        sd=(sum((v-mean)**2 for v in values)/len(values)).sqrt() if valid else None
    dispersion.update(labels=list(SINGLE_NAMES),mean_index_points=native(mean),population_stddev_points=shown(sd),
        coefficient_of_variation=shown(sd/mean) if sd is not None and mean else None,
        spread_points=native(max(values)-min(values)) if valid else None,
        formula='Unweighted cross-sectional mean and population standard deviation over all five named indices at the same provider date.',
        interpretation='Dispersion of five implied-volatility levels; not implied correlation, portfolio diversification, stock-return dispersion or evidence of idiosyncratic/systemic causality.')
    return term,ratios,dispersion


def compute(sources,collected_at):
    if set(sources)!=set(LABELS):raise ValueError('Complete reviewed 16-index inventory required')
    at=stamp(collected_at);evaluation=str(at.date());measurements={};all_rows={};evidence=[];errors=[]
    for label in LABELS:
        source=sources[label]
        try:
            if source.get('error'):raise ValueError('Source unavailable')
            if label in PUBLISHER:definition,rows=parse_publisher(label,source['observations']['raw'],evaluation)
            else:definition,rows=parse_fred(label,source['definition']['raw'],source['observations']['raw'],evaluation)
            measurements[label]=metric(label,definition,rows,collected_at);all_rows[label]=rows
        except (KeyError,ValueError,TypeError,OverflowError,RecursionError,csv.Error):
            measurements[label]={**identity(label),'value':None,'observation_date':None,'quality':{'status':'unavailable'},'reason':'source_definition_observations_or_arithmetic_unavailable'}
            errors.append(label)
        for kind in ('definition','observations'):
            item=source.get(kind,{})
            if item.get('evidence'):evidence.append({'label':label,'series_id':identity(label)['series_id'],'kind':kind,'acquired_at':item.get('acquired_at'),**item['evidence']})
    term,ratios,dispersion=comparisons(measurements,all_rows)
    fresh=sum(m['quality']['status']=='within_age_ceiling' for m in measurements.values())
    status='fresh' if fresh==len(LABELS) else 'partial' if fresh else 'unavailable'
    pipeline=at+timedelta(hours=PIPELINE_HOURS)
    source_deadlines=[stamp(m['source_valid_until']) for m in measurements.values() if m.get('source_valid_until')]
    underlyings={l:{'for':m['for'],'tenor_d':m['tenor_d'],'tier':m['tier'],'value':measurements[l]['value'],
        'date':measurements[l].get('observation_date'),'pctile_252d':None} for l,m in CATALOG.items()}
    return {'contract':CONTRACT,'version':'4.0.0','generated_at':collected_at,'as_of':measurements['VIX_30D'].get('observation_date'),
        'measurements':measurements,'tenor_comparison':term,'cross_asset_comparisons':ratios,'single_name_dispersion':dispersion,
        'quality':{'status':status,'within_age_ceiling':fresh,'reviewed_series':len(LABELS),'unavailable_series':errors,
            'release_calendar_verified':False,'provider_root':'Cboe index methodology and option markets; FRED is a distribution path, not an independent confirming model.',
            'independence_note':'Index families overlap underlyings and methodology. Sixteen measures do not constitute sixteen independent investment votes.'},
        'freshness':{'pipeline_check_due_at':pipeline.isoformat(),'valid_until':min([pipeline,*source_deadlines]).isoformat(),
            'basis':'Per-observation five-calendar-day ceiling plus separate 36-hour collection deadline; release calendar is unverified.'},
        'source_evidence':evidence,'forecast_qualified':False,'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,'call':None,'portfolio_action':'WAIT',
        'composite_stress_score':None,'regime':None,'stress_components':{},'alerts':[],
        'data_source':'Source-defined Cboe index research: 14 FRED distribution identities and direct publisher VVIX/SKEW histories.',
        'fred_alive':sum(measurements[l]['quality']['status']=='within_age_ceiling' for l in CATALOG),'fred_failed':[l for l in CATALOG if l in errors],
        'underlyings':underlyings,'skew':{'value':measurements['SKEW']['value'],'date':measurements['SKEW'].get('observation_date'),'pctile_252d':None,'tail_mult':None,'regime':None,'source':'Cboe'},
        'vvix':{'value':measurements['VVIX']['value'],'date':measurements['VVIX'].get('observation_date'),'pctile_252d':None,'regime':None,'source':'Cboe'},
        'term_structure':{'spot':measurements['VIX_30D']['value'],'back':measurements['VIX_3M']['value'],
            'ratio_30d_3m':term['ratio'] if term['current_comparison_available'] else None,'slope_30d_3m_yearized':None,'inverted':None,'regime':None},
        'cross_asset':{'spots':{l:measurements[l]['value'] for l in CATALOG},'smallcap_stress':None,'oil_dominant':None},
        'equity_dispersion':{'available':False,'regime':None,'reason':'Use explicitly dated descriptive single_name_dispersion; no systemic/idiosyncratic classifier.'},
        'data_freshness':{'latest_fred_date':measurements['VIX_30D'].get('observation_date'),'latest_skew_date':measurements['SKEW'].get('observation_date'),'latest_vvix_date':measurements['VVIX'].get('observation_date')},
        'definitions':{'volatility_indices':'Option-price-implied target volatility expressed as index points; not realized volatility or the full strike/expiry surface.',
            'vvix':'Volatility implied by VIX options, distinct from single-stock implied volatility.',
            'skew':'Publisher SKEW index. It does not provide a calibrated real-world crash probability or an exponential tail multiplier.',
            'term_structure':'Matching-date constant-maturity volatility indices; not a VIX futures curve or futures roll yield.',
            'dispersion':'Cross-sectional variation among five chosen index levels; not correlation or portfolio diversification.',
            'statistics':'Explicit current-vintage descriptive reference windows; their percentiles are sample ranks, not forecast probabilities.',
            'vintage':'Downloaded historical rows may be revised/backfilled. Acquisition time is known; historical first availability is not.'},
        'portfolio_context':{'scenario_url':'/position-sizer.html','role':'explicit_hypothetical_sensitivity_only',
            'formula':'First-order option P&L = signed position vega in currency per volatility point * explicitly entered option-IV shock in volatility points.',
            'limits':'An index move does not determine a position-specific IV shock or vega. Delta, gamma, time decay, volatility smile, rates, FX and full repricing remain separate inputs.'},
        'data_rights':{'originals':'Complete new provider histories remain in the protected AWS IAM archive. No anonymous historical redistribution added.','historical_redistribution_permission_verified':False},
        'compatibility':{'pctile_252d':'Retired ambiguous field. Explicit 252 numeric-observation statistics have their actual dates and counts.',
            'scores':'Legacy composite, panic/contango and cost classifications do not establish investment authority; these fields are null.'}}
