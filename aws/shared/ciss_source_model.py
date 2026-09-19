"""ECB stress measurements from exact CSVs; descriptive, never a trading model."""
import calendar
from collections import Counter
import csv
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import io
import json
import math
import re
import statistics

CONTRACT = 'ciss-research.v1'
PREFIX = 'data/ciss-research/'
HEAD = 'CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX'
CONTRIBUTIONS = ('SS_BMN', 'SS_EMN', 'SS_FIN', 'SS_FXN', 'SS_MMN', 'SS_CON')
COUNTRIES = {'U2':'Euro Area','AT':'Austria','BE':'Belgium','BG':'Bulgaria','CY':'Cyprus',
    'CZ':'Czechia','DE':'Germany','DK':'Denmark','EE':'Estonia','ES':'Spain','FI':'Finland',
    'FR':'France','GB':'United Kingdom','GR':'Greece','HR':'Croatia','HU':'Hungary','IE':'Ireland',
    'IT':'Italy','LT':'Lithuania','LU':'Luxembourg','LV':'Latvia','MT':'Malta','NL':'Netherlands',
    'PL':'Poland','PT':'Portugal','RO':'Romania','SE':'Sweden','SI':'Slovenia','SK':'Slovakia',
    'US':'United States','CN':'China'}
DIMENSIONS = ('FREQ','REF_AREA','CURRENCY','PROVIDER_FM','INSTRUMENT_FM','PROVIDER_FM_ID','DATA_TYPE_FM')


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def digest(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def clock(value):
    out = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if out.tzinfo is None: raise ValueError('aware clock required')
    return out.astimezone(timezone.utc)


def period_end(period, frequency):
    if frequency == 'D': return date.fromisoformat(period)
    if frequency == 'M' and re.fullmatch(r'\d{4}-\d{2}', period):
        year, month = map(int, period.split('-'))
        return date(year, month, calendar.monthrange(year, month)[1])
    raise ValueError('unsupported observation period')


def csv_series(raw, expected_key=None):
    """Missing observations are records, not permission to shift current dates."""
    grouped = {}
    for index, row in enumerate(csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))):
        key = row.get('KEY', '')
        if not re.fullmatch(r'(CISS|CLIFS)\.[A-Z0-9_.]+', key) or len(key.split('.')) != 8:
            raise ValueError('invalid ECB series identity')
        if expected_key and key != expected_key: raise ValueError('response contains another series')
        flow = key.split('.')[0]
        dims = {name: row.get(name, '').strip() for name in DIMENSIONS}
        if key != flow+'.'+'.'.join(dims.values()): raise ValueError('series dimensions differ from key')
        if row.get('UNIT') != 'PURE_NUMB' or row.get('UNIT_MULT') != '0':
            raise ValueError('stress index unit or scale differs')
        freq = dims['FREQ'];period = row.get('TIME_PERIOD', '')
        end = period_end(period, freq)
        value_text = row.get('OBS_VALUE', '').strip()
        try: value = Decimal(value_text) if value_text else None
        except InvalidOperation as exc: raise ValueError('invalid source decimal') from exc
        if value is not None and not value.is_finite(): raise ValueError('nonfinite source observation')
        if value is not None and dims['DATA_TYPE_FM'] == 'IDX' and flow == 'CISS' and not 0 <= value <= 1:
            raise ValueError('CISS index outside defined range')
        if dims['DATA_TYPE_FM'] not in ('IDX', 'CON'): raise ValueError('unsupported stress measure type')
        metadata = {**dims, 'UNIT':row['UNIT'], 'UNIT_MULT':row['UNIT_MULT'],
                    'TITLE':row.get('TITLE'), 'TITLE_COMPL':row.get('TITLE_COMPL'),
                    'COLLECTION':row.get('COLLECTION'), 'DECIMALS':row.get('DECIMALS')}
        entry = grouped.setdefault(key, {'metadata':metadata, 'rows':{}, 'duplicate_rows':0})
        if any(entry['metadata'][name] != metadata[name] for name in DIMENSIONS+('UNIT','UNIT_MULT')):
            raise ValueError('mixed series definitions')
        point = {'period':period, 'period_end':end.isoformat(), 'decimal':str(value) if value is not None else None,
                 'value':float(value) if value is not None else None, 'status':row.get('OBS_STATUS', '').strip(),
                 'source_row':index, 'comment':row.get('OBS_COM') or None}
        if point['value'] is not None and not math.isfinite(point['value']): raise ValueError('decimal exceeds numeric range')
        if period in entry['rows']:
            previous = entry['rows'][period]
            if any(previous[k] != point[k] for k in ('decimal','status','period_end')):
                raise ValueError('conflicting duplicate observation')
            entry['duplicate_rows'] += 1
        else: entry['rows'][period] = point
    if not grouped: raise ValueError('empty ECB CSV response')
    for entry in grouped.values(): entry['rows'] = sorted(entry['rows'].values(), key=lambda p:p['period_end'])
    return grouped


def usable(row):
    return row['value'] is not None and row['status'] in ('A', 'E', 'P')


def downsample(rows, frequency):
    """Last source row per ISO week, including a missing last row and ISO year."""
    if frequency != 'D': return [[p['period'],p['value'] if usable(p) else None] for p in rows]
    groups = {}
    for row in rows:
        iso = date.fromisoformat(row['period_end']).isocalendar()
        groups[(iso.year, iso.week)] = row
    return [[p['period'],p['value'] if usable(p) else None] for p in groups.values()]


def category(key):
    flow, _, area, _, _, _, indicator, _ = key.split('.')
    if flow == 'CLIFS': return 'clifs'
    if key == HEAD: return 'ea_headline'
    if area == 'U2' and indicator in CONTRIBUTIONS: return 'ea_subindex'
    if indicator.startswith('SOV'): return 'sovereign_ea' if area == 'U2' else 'sovereign_country'
    if indicator in ('SS_CIN', 'SS_CI'): return 'country_ciss'
    return 'other'


def calendar_comparison(rows, current, frequency, months=0, days=0):
    """Exact dated baselines from full source rows, including missing prints."""
    end=date.fromisoformat(current['period_end'])
    if months:
        year,month0=divmod(end.year*12+end.month-1-months,12)
        target=date(year,month0+1,min(end.day,calendar.monthrange(year,month0+1)[1]))
    else:target=end-timedelta(days=days)
    out={'value':None,'baseline_value':None,'baseline_decimal':None,'baseline_period':None,
         'baseline_source_row':None,'target_date':target.isoformat(),'unit':'index_points'}
    if frequency=='M':
        candidates=[p for p in rows if months and p['period']==f'{target.year:04d}-{target.month:02d}']
    else:
        candidates=[p for p in rows if 0<=(target-date.fromisoformat(p['period_end'])).days<=7]
    baseline=candidates[-1] if candidates else None
    if baseline:
        out.update(baseline_period=baseline['period'],baseline_source_row=baseline['source_row'])
        if usable(baseline):out.update(baseline_value=baseline['value'],baseline_decimal=baseline['decimal'])
        if usable(current) and usable(baseline):out['value']=float(Decimal(current['decimal'])-Decimal(baseline['decimal']))
    return out


def summarize(key, parsed, evidence, acquired_at, generated_at):
    acquired, now = clock(acquired_at), clock(generated_at)
    if acquired > now or clock(evidence['first_received_at']) > acquired: raise ValueError('future acquisition')
    rows = [p for p in parsed['rows'] if p['period_end'] <= now.date().isoformat()]
    future_count = len(parsed['rows'])-len(rows)
    if not rows: raise ValueError('no completed observation periods')
    current, meta = rows[-1], parsed['metadata']
    freq = meta['FREQ'];age = (now.date()-date.fromisoformat(current['period_end'])).days
    ceiling = 14 if freq == 'D' else 95
    good = [p for p in rows if usable(p)]
    values = [p['value'] for p in good]
    acquisition_age = (now-acquired).total_seconds()
    status = 'missing' if not usable(current) else 'stale' if age>ceiling or acquisition_age>72*3600 else 'fresh'
    live = status == 'fresh'
    point = current['value'] if usable(current) else None
    pctile = 100*sum(v <= point for v in values)/len(values) if live and values else None
    deviation = statistics.pstdev(values) if len(values)>1 else None
    zscore = (point-statistics.mean(values))/deviation if live and deviation else None
    end = date.fromisoformat(current['period_end'])
    target = date(end.year-1,end.month,min(end.day,calendar.monthrange(end.year-1,end.month)[1]))
    tolerance = 7 if freq == 'D' else 0
    candidates = [p for p in rows if p['period_end'] <= target.isoformat()
                  and (target-date.fromisoformat(p['period_end'])).days <= tolerance]
    baseline = candidates[-1] if candidates else None
    # For monthly series, compare the same month, including February leap days.
    if freq == 'M': baseline = next((p for p in rows if p['period']==f'{end.year-1:04d}-{end.month:02d}'),None)
    change = float(Decimal(current['decimal'])-Decimal(baseline['decimal'])) if live and baseline and usable(baseline) else None
    prior5 = date(now.year-5,now.month,min(now.day,calendar.monthrange(now.year-5,now.month)[1])).isoformat()
    retained = [p for p in rows if p['period_end'] >= prior5]
    if not retained: retained = rows[-1:]
    series = {'id':key.replace('.', '_'), 'key':key, 'flow':key.split('.')[0], 'category':category(key),
        'area':meta['REF_AREA'], 'country':COUNTRIES.get(meta['REF_AREA'],meta['REF_AREA']), 'freq':freq,
        'label':meta['TITLE_COMPL'] or meta['TITLE'] or key, 'indicator':meta['TITLE'] or meta['PROVIDER_FM_ID'],
        'source_metadata':meta, 'unit':'dimensionless_contribution' if meta['DATA_TYPE_FM']=='CON' else 'dimensionless_index',
        'observation_status':current['status'], 'latest':point if live else None,
        'latest_decimal':current['decimal'] if live else None, 'latest_date':current['period'],
        'observation_period_end':current['period_end'], 'last_observed_value':good[-1]['value'] if good else None,
        'last_observed_date':good[-1]['period'] if good else None, 'source_row':current['source_row'],
        'source_published_at':None, 'acquired_at':acquired_at, 'evidence':evidence,
        'quality':{'status':status, 'observation_date':current['period'], 'period_end':current['period_end'],
            'source_acquired_at':acquired_at, 'evaluated_at':generated_at, 'original_source_verified':True,
            'observation_age_days':age,'maximum_observation_age_days':ceiling,
            'acquisition_age_seconds':acquisition_age,'maximum_acquisition_age_seconds':72*3600,
            'frequency':'daily' if freq=='D' else 'monthly',
            'basis':'observation-period end and acquisition ceilings; exact release calendar not verified',
            'excluded_future_rows':future_count, 'missing':[] if live else ['current_qualified_observation']},
        'start_date':rows[0]['period'], 'n_obs':len(rows), 'n_numeric':len(values), 'duplicate_rows':parsed['duplicate_rows'],
        'points':[[p['period'],p['value']] for p in retained if usable(p)],
        'points_scope':'up to five calendar years of usable observations; original CSV retains the entire retrieved history',
        'chart_points':downsample(rows,freq), 'chart_aggregation':'last observation per ISO week' if freq=='D' else 'monthly source rows',
        'min':min(values) if values else None, 'max':max(values) if values else None,
        'mean':statistics.mean(values) if values else None, 'pctile':pctile, 'zscore':zscore,
        'chg_1y':change, 'yoy_pct':None, 'change_unit':'index_points',
        'annual_comparison':{'target_date':target.isoformat(), 'baseline_period':baseline['period'] if baseline else None,
            'baseline_value':baseline['value'] if baseline and usable(baseline) else None,
            'baseline_source_row':baseline['source_row'] if baseline else None, 'value':change, 'unit':'index_points'},
        'pct_of_peak':100*point/max(values) if live and values and min(values)>=0 and max(values)>0 else None,
        'distribution_definition':'empirical fraction <= latest; full current retrieved-vintage history; not a probability of crisis',
        'discontinued':False, 'maintenance_status':'not independently established',
        'ranking_eligible':live, 'ranking_scope':'historical distribution within this exact series, not a cross-country crisis ranking',
        'call':None,'calls_eligible':False,'sizing_eligible':False,'historical_point_in_time':False}
    series['comparisons']={name:calendar_comparison(rows,current,freq,**kwargs) for name,kwargs in
        [('1w',{'days':7}),('1m',{'months':1}),('3m',{'months':3}),('12m',{'months':12})]}
    if not live:
        for value in series['comparisons'].values():value['value']=None
    for years in (3,5):
        cutoff=date(now.year-years,now.month,min(now.day,calendar.monthrange(now.year-years,now.month)[1])).isoformat()
        window=[p['value'] for p in good if p['period_end']>=cutoff]
        series['percentile_'+str(years)+'y']=100*sum(v<=point for v in window)/len(window) if live and window else None
        series['percentile_'+str(years)+'y_observations']=len(window)
    return series


def build(discoveries, histories, generated_at, errors=None):
    """Each input holds exact bytes plus its retained receipt/acquisition clock."""
    now=clock(generated_at);catalog={};issues=dict(errors or {})
    for flow,item in discoveries.items():
        if flow not in ('CISS','CLIFS'): raise ValueError('unsupported flow')
        if clock(item['acquired_at']) > now or clock(item['evidence']['first_received_at']) > clock(item['acquired_at']):
            raise ValueError('future discovery acquisition')
        for key, parsed in csv_series(item['raw']).items():
            if not key.startswith(flow+'.'): raise ValueError('discovery flow differs')
            catalog[key]={'metadata':parsed['metadata'],'latest_discovered_period':parsed['rows'][-1]['period']}
    series=[]
    for key in sorted(catalog):
        if key not in histories:
            issues.setdefault(key,'HISTORY_UNAVAILABLE');continue
        item=histories[key]
        try:
            parsed=csv_series(item['raw'],key)[key]
            row=summarize(key,parsed,item['evidence'],item['acquired_at'],generated_at)
            # A history response cannot conceal a newer missing discovery print.
            discovered=catalog[key]['latest_discovered_period']
            if row['latest_date']<discovered:
                row['quality'].update(status='incomplete',missing=['latest_discovered_period'])
                row.update(latest=None,latest_decimal=None,ranking_eligible=False,pctile=None,zscore=None,
                           chg_1y=None,percentile_3y=None,percentile_5y=None,pct_of_peak=None)
                row['annual_comparison']['value']=None
                for value in row['comparisons'].values():value['value']=None
            series.append(row)
        except (ValueError,KeyError,TypeError,OverflowError):issues[key]='INVALID_HISTORY'
    by_key={row['key']:row for row in series};head=by_key.get(HEAD)
    components=[by_key.get('CISS.D.U2.Z0Z.4F.EC.'+code+'.CON') for code in CONTRIBUTIONS]
    reconciled=None;residual=None;total=None
    if head and head['quality']['status']=='fresh' and all(r and r['quality']['status']=='fresh' and r['latest_date']==head['latest_date'] for r in components):
        total=sum(Decimal(row['latest_decimal']) for row in components)
        residual=total-Decimal(head['latest_decimal']);reconciled=abs(residual)<=Decimal('0.0000000001')
    if reconciled is False: issues['headline_reconciliation']='COMPONENT_SUM_DIFFERS'
    output={'engine':'ciss-stress','version':'2.0.0','contract':CONTRACT,'generated_at':generated_at,
        'n_series':len(series),'discovered_series':len(catalog),'categories':dict(Counter(r['category'] for r in series)),
        'catalog':catalog,'series':series,'errors':issues,'canonical_warehouse':'data/ciss-stress.json',
        'ea_composite':head['latest'] if head else None,'ea_composite_date':head['latest_date'] if head else None,
        'ea_regime':None,'call':None,'calls_eligible':False,'sizing_eligible':False,
        'quality':head['quality'] if head else {'status':'unavailable','missing':['current_headline']},
        'coverage':{'compiled':len(series),'discovered':len(catalog),'fresh':sum(r['quality']['status']=='fresh' for r in series),
                    'errors':len(issues),'status':'degraded' if issues or len(series)!=len(catalog) else 'measured'},
        'headline_reconciliation':{'status':'matched' if reconciled else 'mismatch' if reconciled is False else 'unavailable',
            'date':head['latest_date'] if head else None,'component_sum_decimal':str(total) if total is not None else None,
            'residual_decimal':str(residual) if residual is not None else None,'tolerance_decimal':'0.0000000001',
            'definition':'Five reported market contributions plus reported correlation contribution = composite index',
            'component_keys':[r['key'] for r in components if r]},
        'frequency_note':'Frequency belongs to each exact ECB key. Current daily and legacy monthly methodologies are not spliced.',
        'provenance':'ECB original CSV observations and metadata; current retrieved vintages, not historical as-known-at releases.',
        'field_units':{'ea_composite':'dimensionless_index','series.*.chg_1y':'index_points','series.*.pctile':'percent'},
        'decision':{'verb':'WAIT','meaning':'abstain','reason':'Dated stress measurements do not establish a validated forecast or portfolio size.'}}
    if reconciled is False:
        output['quality']={**output['quality'],'status':'invalid','missing':['headline_component_reconciliation']}
        output['ea_composite']=None
        head['quality']=dict(output['quality'])
        head.update(latest=None,latest_decimal=None,ranking_eligible=False,pctile=None,zscore=None,
                    chg_1y=None,percentile_3y=None,percentile_5y=None,pct_of_peak=None)
        head['annual_comparison']['value']=None
        for value in head['comparisons'].values():value['value']=None
        output['coverage']['fresh']=sum(r['quality']['status']=='fresh' for r in series)
    return output


def row_is_current(row, generated_at):
    """A downstream render must re-evaluate both clocks, not copy a green badge."""
    try:
        now=clock(generated_at);q=row['quality']
        age=(now.date()-date.fromisoformat(row['observation_period_end'])).days
        acquired_age=(now-clock(row['acquired_at'])).total_seconds()
        return (q['status']=='fresh' and row['latest'] is not None
                and 0<=age<=q['maximum_observation_age_days']
                and 0<=acquired_age<=q['maximum_acquisition_age_seconds'])
    except (KeyError,ValueError,TypeError):return False


def commentary(source, generated_at):
    if source.get('contract')!=CONTRACT: raise ValueError('canonical CISS source required')
    age=(clock(generated_at)-clock(source['generated_at'])).total_seconds()
    available=0<=age<=72*3600 and source.get('quality',{}).get('status')=='fresh'
    by_key={r['key']:r for r in source['series']}
    head=by_key.get(HEAD);claims=[]
    available=available and bool(head and row_is_current(head,generated_at))
    if available and head and head['latest'] is not None:
        selected=[head]+[by_key.get('CISS.D.U2.Z0Z.4F.EC.'+code+'.CON') for code in CONTRIBUTIONS]
        for row in selected:
            if not row or not row_is_current(row,generated_at) or row['latest_date']!=head['latest_date']:continue
            claims.append({'series_id':row['key'],'value':row['latest'],'value_decimal':row['latest_decimal'],
                'unit':row['unit'],'observation_date':row['latest_date'],'acquired_at':row['acquired_at'],
                'source_row':row['source_row'],'evidence':row['evidence']})
    text=f"ECB euro-area CISS measured {head['latest_decimal']} on {head['latest_date']}." if claims else 'A current qualified ECB CISS headline is unavailable.'
    reconciliation=source['headline_reconciliation']
    read=f"The six published contributions reconcile to the composite on {reconciliation['date']}." if len(claims)==7 and reconciliation['status']=='matched' else 'Same-date contribution reconciliation is unavailable or failed.'
    return {'engine':'ciss-ai','version':'2.0.0','contract':'ciss-commentary.v1','generated_at':generated_at,
        'model':'deterministic source observations; no AI API','source_generated_at':source['generated_at'],
        'based_on':source.get('ea_composite_date'),'source_replay':source['replay'],'claims':claims,
        'ea_composite':source.get('ea_composite') if available else None,'ea_regime':None,
        'interpretation':{'headline':text,'regime':None,'regime_read':read,
            'stress_source':'Market contributions and the signed correlation contribution are reported separately; they are not standalone index levels or cross-currency basis spreads.',
            'sovereign':'Daily and legacy monthly SovCISS definitions retain their own dates and methodologies. Missing latest observations cannot identify a most-stressed country.',
            'cross_country':'Distribution percentiles describe each exact series in its retrieved vintage. They are not cross-country crisis probabilities.',
            'risk_assets':'No asset-return forecast or position weight is established by these measurements.',
            'liquidity':'CISS measures financial stress; it does not identify central-bank injections or dollar funding quantities.',
            'watch':['Inspect missing latest observations and acquisition gaps.','Check same-date contribution reconciliation.','Review exact series definitions before comparing countries or vintages.']},
        'quality':{'status':'fresh' if available else 'unavailable','source_age_seconds':age,'source_contract':CONTRACT},
        'ok':bool(claims),'call':None,'calls_eligible':False,'sizing_eligible':False,
        'decision':{'verb':'WAIT','meaning':'abstain'}}
