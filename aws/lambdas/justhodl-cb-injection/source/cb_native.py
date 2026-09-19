"""Native CB measurements: exact source identities, missing rows and accounting dates."""
import calendar
import csv
from datetime import date, datetime, timezone
from decimal import Decimal
import hashlib
import io
import math
import re
from report_observations import decimal, measurement, months_before
from research_brief_model import clock, digest, SOURCE_CONTRACT

POLICY = {
    'WALCL': ('Millions of U.S. Dollars', 'W', 'USD_bn', '0.001'),
    'WSHOSHO': ('Millions of U.S. Dollars', 'W', 'USD_bn', '0.001'),
    'WLCFLPCL': ('Millions of U.S. Dollars', 'W', 'USD_bn', '0.001'),
    'SWPT': ('Millions of U.S. Dollars', 'W', 'USD_bn', '0.001'),
    'ECBASSETSW': ('Millions of Euros', 'W', 'EUR_bn', '0.001'),
    'JPNASSETS': ('100 Million Yen', 'M', 'JPY_bn', '0.1'),
    'ECBDFR': ('Percent', 'D', 'percent_per_annum', '1'),
    'DFEDTARU': ('Percent', 'D', 'percent_per_annum', '1'),
    'IR3TIB01JPM156N': ('Percent', 'M', 'percent_per_annum', '1'),
    'IR3TIB01CHM156N': ('Percent', 'M', 'percent_per_annum', '1'),
    'DEXJPUS': ('Japanese Yen to One U.S. Dollar', 'D', 'JPY_per_USD', '1'),
    'DEXSZUS': ('Swiss Francs to One U.S. Dollar', 'D', 'CHF_per_USD', '1'),
    'DEXUSEU': ('U.S. Dollars to One Euro', 'D', 'USD_per_EUR', '1'),
}
ECB = {
    'total_assets': 'ILM/W.U2.C.T000000.Z5.Z01',
    'monetary_policy_securities': 'ILM/W.U2.C.A070100.U2.EUR',
    'monetary_policy_lending': 'ILM/W.U2.C.A050000.U2.EUR',
}
AGE = {'D': 10, 'W': 21, 'M': 100}
MAX_SOURCE_AGE = 26 * 3600


def effective(period, frequency):
    day = date.fromisoformat(period)
    return day.replace(day=calendar.monthrange(day.year, day.month)[1]) if frequency == 'M' else day


def fred_inputs(source, originals):
    if source.get('contract') != SOURCE_CONTRACT:
        raise ValueError('canonical source contract required')
    if digest({k:v for k,v in source.items() if k != 'replay'}) != source.get('replay',{}).get('output_sha256'):
        raise ValueError('canonical source content differs')
    out = {}
    for sid, (unit, frequency, target, factor) in POLICY.items():
        observed = source.get('measurements',{}).get(sid)
        if not observed:
            continue
        original = originals.get(sid)
        if not original:
            raise ValueError('original source missing: '+sid)
        rebuilt = measurement(sid, original['definition'], original['observations'], original['evidence'],
                              source['generated_at'], original['acquired_at'])
        if observed != rebuilt:
            raise ValueError('original source reconstruction differs: '+sid)
        meta = original['definition']['seriess'][0]
        valid = (observed['unit'] == unit and observed['frequency'] == frequency
                 and meta.get('seasonal_adjustment') == 'Not Seasonally Adjusted')
        if sid == 'JPNASSETS':
            valid = valid and meta.get('frequency') == 'Monthly, End of Period'
        rows = []
        for index, raw in enumerate(original['observations']['observations']):
            value = decimal(raw.get('value'))
            rows.append({'period':raw['date'], 'date':effective(raw['date'],frequency).isoformat(),
                         'native_decimal':str(value) if value is not None else None,
                         'value_decimal':str(value*Decimal(factor)) if value is not None and valid else None,
                         'original_row_index':index, 'source_status':'missing' if value is None else 'observed',
                         'original':original['evidence']['observations']})
        rows.sort(key=lambda row:row['date'], reverse=True)
        out[sid] = {'source_id':sid, 'frequency':frequency, 'unit':target, 'source_unit':observed['unit'],
                    'scale_decimal':factor, 'definition':original['evidence']['definition'],
                    'metadata_valid':valid, 'name':observed['name'], 'rows':rows,
                    'acquired_at':original['acquired_at'], 'source_generated_at':source['generated_at'],
                    'period_basis':'month-end period boundary; monthly rate need not be an end-of-month fixing' if frequency=='M' else 'provider observation date',
                    'canonical_measurement':observed}
    return out


def ecb_input(name, raw, receipt, acquired_at):
    flow = ECB[name]
    url = 'https://data-api.ecb.europa.eu/service/data/'+flow
    request_sha = hashlib.sha256(url.encode()).hexdigest()
    sha = hashlib.sha256(raw).hexdigest()
    if (receipt.get('contract') != 'source-evidence.v1' or receipt.get('provider') != 'ecb'
            or receipt.get('captured') is not True or receipt.get('source_url') != url
            or receipt.get('bytes') != len(raw) or receipt.get('sha256') != sha
            or receipt.get('key') != 'data/evidence/ecb/'+request_sha+'/'+sha+'.bin.gz'):
        raise ValueError('ECB original identity differs')
    seen = set(); rows = []
    for index, row in enumerate(csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))):
        if row.get('KEY') != flow.replace('/','.',1) or row.get('FREQ') != 'W' or row.get('UNIT') != 'EUR':
            raise ValueError('ECB series identity or unit differs')
        period = row.get('TIME_PERIOD','')
        if not re.fullmatch(r'\d{4}-W\d{2}',period) or period in seen:
            raise ValueError('ECB duplicate or invalid period')
        seen.add(period); year, week = map(int,period.split('-W'))
        day = date.fromisocalendar(year, week, 5)
        if row.get('UNIT_MULT') not in ('6','9'):
            raise ValueError('unsupported ECB scale')
        factor = Decimal(10) ** (int(row['UNIT_MULT']) - 9)
        value = decimal(row.get('OBS_VALUE'))
        status = row.get('OBS_STATUS','')
        usable = value is not None and status in ('A','E','P')
        rows.append({'period':period,'date':day.isoformat(),'native_decimal':str(value) if value is not None else None,
                     'value_decimal':str(value*factor) if usable else None,'source_status':status,
                     'source_unit':'EUR','source_unit_multiplier':int(row['UNIT_MULT']),
                     'scale_decimal':str(factor),'original_row_index':index,'original':receipt})
    if not rows:
        raise ValueError('empty ECB observations')
    rows.sort(key=lambda row:row['date'],reverse=True)
    return {'source_id':flow.replace('/','.',1),'frequency':'W','unit':'EUR_bn','source_unit':'EUR',
            'metadata_valid':True,'name':name,'rows':rows,'acquired_at':acquired_at,
            'source_generated_at':acquired_at,'period_basis':'ECB weekly financial statement, Friday period end'}


def quality(series, stamp):
    now = clock(stamp); rows = series.get('rows') or []; row = rows[0] if rows else None
    frequency = series.get('frequency'); limit = AGE.get(frequency)
    observed = date.fromisoformat(row['date']) if row else None
    age = (now.date()-observed).days if observed else None
    source_age = (now-clock(series['source_generated_at'])).total_seconds() if series.get('source_generated_at') else None
    acquired_age = (now-clock(series['acquired_at'])).total_seconds() if series.get('acquired_at') else None
    status = 'fresh'
    if not row or limit is None:status = 'unavailable'
    elif series.get('metadata_valid') is not True:status = 'invalid_definition'
    elif age < 0:status = 'incomplete_measurement_period'
    elif source_age is None or acquired_age is None or min(source_age,acquired_age)<0:status = 'invalid_source_clock'
    elif max(source_age,acquired_age)>MAX_SOURCE_AGE:status = 'stale_source'
    elif decimal(row.get('value_decimal')) is None:status = 'missing_observation'
    elif not math.isfinite(float(decimal(row['value_decimal']))):status = 'invalid_numeric_range'
    elif series.get('unit','').endswith('_bn') and decimal(row['value_decimal'])<0:status = 'invalid_negative_stock'
    elif series.get('unit') in ('JPY_per_USD','CHF_per_USD','USD_per_EUR') and decimal(row['value_decimal'])<=0:status = 'invalid_fx'
    elif age > limit:status = 'stale_observation'
    return {'status':status,'observation_date':row['date'] if row else None,'provider_period':row['period'] if row else None,
            'frequency':frequency,'age_days':age,'max_age_days':limit,'evaluated_at':stamp,
            'acquired_at':series.get('acquired_at'),'acquisition_age_seconds':acquired_age,
            'source_generated_at':series.get('source_generated_at'),'source_age_seconds':source_age,
            'max_source_age_seconds':MAX_SOURCE_AGE}


def previous(rows, months, frequency):
    if not rows:return None,None
    latest = date.fromisoformat(rows[0]['date']); target = months_before(latest, months)
    if frequency == 'M':
        chosen = next((r for r in rows if r['date'][:7] == target.isoformat()[:7]),None)
    else:
        chosen = next((r for r in rows if r['date']<=target.isoformat()),None)
        if chosen and (target-date.fromisoformat(chosen['date'])).days > (6 if frequency=='W' else 4):chosen=None
    return chosen,target.isoformat()


def measure(series, stamp):
    q = quality(series,stamp); rows = series.get('rows') or []; row = rows[0] if rows else None
    usable = q['status']=='fresh'; value = decimal(row.get('value_decimal')) if usable else None
    changes = {}; unit = series.get('unit')
    for months in (1,6,12):
        old,target = previous(rows,months,series.get('frequency'))
        baseline = decimal(old.get('value_decimal')) if old else None
        delta = value-baseline if value is not None and baseline is not None else None
        relative = delta/baseline*100 if delta is not None and baseline>0 and unit!='percent_per_annum' else None
        changes[str(months)] = {'start_date':old['date'] if old else None,'end_date':row['date'] if row else None,
            'target_date':target,'level_change':float(delta) if delta is not None else None,
            'level_change_decimal':str(delta) if delta is not None else None,
            'change_unit':'percentage_points' if unit=='percent_per_annum' else unit,
            'percent_change':float(relative) if relative is not None else None,
            'baseline':old,'current':row,'reason':None if delta is not None else 'current_or_calendar_baseline_unavailable'}
    return {'latest':float(value) if value is not None else None,'latest_decimal':str(value) if value is not None else None,
            'unit':unit,'source_id':series.get('source_id'),'quality':q,'changes':changes,'selected':row,
            'history_start':rows[-1]['date'] if rows else None,'history_observations':len(rows),
            'source_unit':series.get('source_unit'),'scale_decimal':series.get('scale_decimal'),'definition':series.get('definition'),
            'period_basis':series.get('period_basis'),'calls_eligible':False,'sizing_eligible':False}
