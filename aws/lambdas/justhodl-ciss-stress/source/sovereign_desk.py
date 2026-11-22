"""Sovereign-stress desk document, compiled only from retained ECB originals.

The CISS warehouse (data/ciss-stress.json) keeps every CISS, SovCISS and CLIFS series with its original CSV.  This
module reads those originals again, adds the ECB's own sovereign measurements that the warehouse does not hold, and
publishes two documents for the sovereign-stress desk:

  data/ciss-sovereign.json           every measure with its date, changes, place in its own history, crisis peaks
  data/ciss-sovereign-history.json   the history of every measure (daily series as the last observation of each week)

Added from the ECB Data Portal, each response retained through evidence_store before anything is computed:
  * the euro-area government yield curve for AAA issuers and for all issuers (daily)  -> all issuers minus AAA
  * 10-year government yields by country (monthly, convergence statistic)           -> spread to Germany
  * TARGET balances by national central bank (monthly, end of month)
  * general government debt as a percentage of GDP (quarterly)
  * the euro reference rate and the Eurosystem's foreign-currency claims (the CISS page's auxiliaries)

Nothing here is a forecast, a probability or a position size.  A number is a dated measurement or it is absent.
compile_desk() is pure: the same receipts and the same clock give the same bytes, which is what publish() checks
by compiling twice from two separate reads of the stored originals.
"""
import calendar
import csv
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import io
import math
from pathlib import Path
import re
import statistics
import time

import ciss_source_model as model
from ciss_source_model import encoded, digest, clock, PREFIX

VERSION = '1.0.0'
CONTRACT = 'ciss-sovereign.v1'
HISTORY_CONTRACT = 'ciss-sovereign-history.v1'
CURRENT = 'data/ciss-sovereign.json'
HISTORY = 'data/ciss-sovereign-history.json'
WAREHOUSE = 'data/ciss-stress.json'
RUNS = PREFIX + 'sovereign/runs/'
BASE = 'https://data-api.ecb.europa.eu/service/data/'

# Full histories, one request each.  The path alone identifies the request (the evidence store drops the query).
AUX = {
    'curve': 'YC/B.U2.EUR.4F.G_N_A+G_N_C.SV_C_YM.SR_2Y+SR_10Y',
    'yields': 'IRS/M..L.L40.CI.0000..N.Z',
    'target': 'TGB/M...A094T.U2.EUR.E',
    'debt': 'GFS/Q.N..W0.S13.S1.C.L.LE.GD.T._Z.XDC_R_B1GQ_CY._T.F.V.N._T',
    'eurusd': 'EXR/D.USD.EUR.SP00.A',
    'fx_claims_ea': 'ILM/W.U2.C.A030000.U2.Z06',
    'fx_claims_nonea': 'ILM/W.U2.C.A020000.U4.Z06',
}
AUX_LABEL = {
    'curve': 'Euro-area government yield curve, AAA issuers and all issuers (daily)',
    'yields': '10-year government yields by country, convergence statistic (monthly)',
    'target': 'TARGET balances of the national central banks, end of month',
    'debt': 'General government debt, percent of GDP (quarterly)',
    'eurusd': 'Euro reference rate against the US dollar (daily)',
    'fx_claims_ea': 'Eurosystem claims on euro-area residents in foreign currency (weekly)',
    'fx_claims_nonea': 'Eurosystem claims on non-euro-area residents in foreign currency (weekly)',
}

# Named windows of euro-area sovereign history.  A cell on the desk is the highest reading inside the window.
EPISODES = [
    {'id': 'gfc2008', 'from': '2008-09-15', 'to': '2009-03-31', 'name': 'Lehman and the global crisis'},
    {'id': 'greece2010', 'from': '2010-04-23', 'to': '2010-06-30', 'name': 'Greece asks for a rescue'},
    {'id': 'ireland2010', 'from': '2010-11-01', 'to': '2010-12-31', 'name': 'Ireland rescue'},
    {'id': 'italy2011', 'from': '2011-07-01', 'to': '2011-12-31', 'name': 'Italy and Spain drawn in'},
    {'id': 'spain2012', 'from': '2012-05-01', 'to': '2012-07-31', 'name': "Spain's banks, \"whatever it takes\""},
    {'id': 'cyprus2013', 'from': '2013-03-15', 'to': '2013-04-15', 'name': 'Cyprus bail-in'},
    {'id': 'greece2015', 'from': '2015-06-26', 'to': '2015-07-20', 'name': 'Greek referendum'},
    {'id': 'brexit2016', 'from': '2016-06-23', 'to': '2016-07-08', 'name': 'Brexit vote'},
    {'id': 'italy2018', 'from': '2018-05-15', 'to': '2018-11-30', 'name': 'Italy: government and budget'},
    {'id': 'covid2020', 'from': '2020-02-24', 'to': '2020-04-30', 'name': 'COVID'},
    {'id': 'tpi2022', 'from': '2022-06-01', 'to': '2022-07-31', 'name': 'ECB lift-off and the TPI'},
    {'id': 'gilts2022', 'from': '2022-09-23', 'to': '2022-10-31', 'name': 'UK gilt crisis'},
    {'id': 'banks2023', 'from': '2023-03-08', 'to': '2023-03-31', 'name': 'SVB and Credit Suisse'},
    {'id': 'france2024', 'from': '2024-06-10', 'to': '2024-07-10', 'name': 'French snap election'},
    {'id': 'france2024b', 'from': '2024-11-20', 'to': '2024-12-20', 'name': 'French budget, government falls'},
    {'id': 'tariffs2025', 'from': '2025-04-02', 'to': '2025-04-30', 'name': 'US tariff shock'},
]

AREAS = {**model.COUNTRIES, 'U2': 'Euro area', '4F': 'ECB', 'I9': 'Euro area', 'I8': 'Euro area', 'B6': 'European Union', 'D0': 'European Union', 'V5': 'European Union'}
CONTRIBUTION = {'SS_BMN': 'Bond market', 'SS_EMN': 'Equity market', 'SS_FIN': 'Financial intermediaries', 'SS_FXN': 'Foreign exchange',
                'SS_MMN': 'Money market', 'SS_CON': 'Correlation between the markets'}
ALLOW = {'D': 10, 'W': 21, 'M': 100, 'Q': 230}        # days after the period's end before a reading is called late
ENDED = 400                                           # days with no observation before a series is treated as ended at source
SPARK = {'D': 260, 'W': 104, 'M': 60, 'Q': 40}
PLACES = {'index': 5, 'pct': 4, 'bp': 2, 'eur_bn': 3, 'pct_gdp': 2, 'usd': 4}
USABLE = ('A', 'E', 'P', 'B')
HORIZONS = (('1w', 7, 0), ('1m', 0, 1), ('3m', 0, 3), ('12m', 0, 12))


def period_end(period):
    """The last day an ECB period covers: a day, a month, a quarter, or the Friday of an ISO week."""
    if re.fullmatch(r'\d{4}-\d{2}-\d{2}', period):
        return date.fromisoformat(period)
    m = re.fullmatch(r'(\d{4})-(\d{2})', period)
    if m:
        return date(int(m[1]), int(m[2]), calendar.monthrange(int(m[1]), int(m[2]))[1])
    m = re.fullmatch(r'(\d{4})-Q([1-4])', period)
    if m:
        return date(int(m[1]), 3 * int(m[2]), calendar.monthrange(int(m[1]), 3 * int(m[2]))[1])
    m = re.fullmatch(r'(\d{4})-W(\d{2})', period)
    if m:
        return date.fromisocalendar(int(m[1]), int(m[2]), 5)
    raise ValueError('unsupported observation period')


def flow_series(raw):
    """Every series of one ECB CSV response: {key: {'meta', 'rows'}}.  A blank observation stays a row."""
    out = {}
    for index, row in enumerate(csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))):
        key = row.get('KEY', '')
        if not re.fullmatch(r'[A-Z0-9]+\.[A-Za-z0-9_.]+', key):
            raise ValueError('invalid ECB series identity')
        text = (row.get('OBS_VALUE') or '').strip()
        try:
            value = Decimal(text) if text else None
        except InvalidOperation as exc:
            raise ValueError('invalid source decimal') from exc
        if value is not None and not value.is_finite():
            raise ValueError('nonfinite source observation')
        period = row.get('TIME_PERIOD', '')
        meta = {'unit': (row.get('UNIT') or row.get('UNIT_MEASURE') or '').strip(), 'unit_mult': (row.get('UNIT_MULT') or '0').strip() or '0',
                'title': (row.get('TITLE_COMPL') or row.get('TITLE') or '').strip(), 'area': (row.get('REF_AREA') or '').strip()}
        entry = out.setdefault(key, {'meta': meta, 'rows': {}})
        if (entry['meta']['unit'], entry['meta']['unit_mult']) != (meta['unit'], meta['unit_mult']):
            raise ValueError('mixed units inside one series')
        point = {'period': period, 'end': period_end(period).isoformat(), 'value': value, 'status': (row.get('OBS_STATUS') or '').strip(), 'row': index}
        if period in entry['rows']:
            if entry['rows'][period]['value'] != value:
                raise ValueError('conflicting duplicate observation')
        else:
            entry['rows'][period] = point
    if not out:
        raise ValueError('empty ECB CSV response')
    for entry in out.values():
        entry['rows'] = sorted(entry['rows'].values(), key=lambda p: p['end'])
    return out


def observations(entry, today, scale=None):
    """Usable observations up to today as (iso date, Decimal), plus what was left out and whether the newest row is blank."""
    rows = [p for p in entry['rows'] if p['end'] <= today]
    good = [p for p in rows if p['value'] is not None and p['status'] in USABLE]
    left = {}
    for p in rows:
        if p['value'] is not None and p['status'] not in USABLE:
            left[p['status'] or '?'] = left.get(p['status'] or '?', 0) + 1
    factor = scale if scale is not None else Decimal(1)
    obs = [(p['end'], p['value'] * factor) for p in good]
    blank = rows[-1]['period'] if rows and good and rows[-1]['end'] > good[-1]['end'] else None
    return obs, {'last_row': good[-1]['row'] if good else None, 'last_period': good[-1]['period'] if good else None,
                 'exact': str(good[-1]['value']) if good else None, 'blank_after': blank, 'left_out': left}


def months_back(day, months):
    year, month0 = divmod(day.year * 12 + day.month - 1 - months, 12)
    return year, month0 + 1


def change_over(obs, index, freq, days, months):
    """Latest minus the reading one horizon earlier; None when the source has no reading there."""
    last_day = date.fromisoformat(obs[-1][0])
    if freq in ('M', 'Q'):
        if days or (freq == 'Q' and months % 3):
            return None
        year, month = months_back(last_day, months)
        base = index.get(date(year, month, calendar.monthrange(year, month)[1]).isoformat())
        return None if base is None else obs[-1][1] - base
    if months:
        year, month = months_back(last_day, months)
        target = date(year, month, min(last_day.day, calendar.monthrange(year, month)[1]))
    else:
        target = last_day - timedelta(days=days)
    reach = 7 if freq == 'D' else 10
    for step in range(reach + 1):
        base = index.get((target - timedelta(days=step)).isoformat())
        if base is not None:
            return obs[-1][1] - base
    return None


def tops(obs, count=8, apart=120):
    """The highest readings the series itself shows, each at least `apart` days from a higher one already taken."""
    taken = []
    for day, value in sorted(obs, key=lambda p: (-p[1], p[0])):
        d = date.fromisoformat(day)
        if all(abs((d - date.fromisoformat(t[0])).days) >= apart for t in taken):
            taken.append((day, value))
            if len(taken) == count:
                break
    return taken


def weekly(obs):
    """Last observation of each ISO week; the final observation is always kept."""
    groups = {}
    for day, value in obs:
        iso = date.fromisoformat(day).isocalendar()
        groups[(iso[0], iso[1])] = (day, value)
    return list(groups.values())


def number(value, unit):
    return None if value is None else round(float(value), PLACES[unit])


def measure(mid, label, unit, freq, obs, today, info=None, key=None, derived=None, evidence=None, group=None, area=None):
    """One measure of the desk from its usable observations (ascending, Decimal)."""
    if not obs:
        return None
    info = info or {}
    index = dict(obs)
    last_day, last = obs[-1]
    age = (date.fromisoformat(today) - date.fromisoformat(last_day)).days
    values = [v for _, v in obs]
    last_date = date.fromisoformat(last_day)
    five = date(last_date.year - 5, last_date.month, min(last_date.day, calendar.monthrange(last_date.year - 5, last_date.month)[1])).isoformat()
    recent = [v for d, v in obs if d >= five]
    high = max(obs, key=lambda p: (p[1], -date.fromisoformat(p[0]).toordinal()))
    low = min(obs, key=lambda p: (p[1], date.fromisoformat(p[0]).toordinal()))
    peaks = {}
    for ep in EPISODES:
        inside = [p for p in obs if ep['from'] <= p[0] <= ep['to']]
        if inside:
            hi = max(inside, key=lambda p: (p[1], -date.fromisoformat(p[0]).toordinal()))
            lo = min(inside, key=lambda p: (p[1], date.fromisoformat(p[0]).toordinal()))
            peaks[ep['id']] = {'hi': [number(hi[1], unit), hi[0]], 'lo': [number(lo[1], unit), lo[0]]}
    out = {'id': mid, 'label': label, 'unit': unit, 'freq': freq, 'group': group, 'area': area, 'key': key, 'derived': derived,
           'value': number(last, unit), 'date': last_day, 'period': info.get('last_period') or last_day, 'exact': info.get('exact'),
           'status': 'current' if age <= ALLOW[freq] else 'ended' if age > ENDED else 'late', 'age_days': age,
           'blank_after': info.get('blank_after'), 'left_out': info.get('left_out') or {},
           'change': {name: number(change_over(obs, index, freq, days, months), unit) for name, days, months in HORIZONS},
           'pctile': round(100 * sum(v <= last for v in values) / len(values), 1),
           'pctile_5y': round(100 * sum(v <= last for v in recent) / len(recent), 1) if recent else None,
           'n': len(obs), 'start': obs[0][0], 'high': [number(high[1], unit), high[0]], 'low': [number(low[1], unit), low[0]],
           'mean': number(sum(values) / len(values), unit), 'peaks': peaks, 'tops': [[number(v, unit), d] for d, v in tops(obs)],
           'spark': [number(v, unit) for _, v in obs[-SPARK[freq]:]], 'spark_from': obs[-SPARK[freq]:][0][0],
           'evidence': evidence, 'source_row': info.get('last_row')}
    return out


def history_of(obs, unit, freq):
    kept = weekly(obs) if freq == 'D' else obs
    return {'d': [d for d, _ in kept], 'v': [number(v, unit) for _, v in kept]}


def aligned(left, right, factor=Decimal(1)):
    """left minus right on the dates both have."""
    other = dict(right)
    return [(d, (v - other[d]) * factor) for d, v in left if d in other]


def freq_of(key):
    code = key.split('.')[1]
    return 'D' if code in ('D', 'B') else code


def ciss_id(row):
    """The desk's id and label for a warehouse series, or None for one the desk does not show."""
    key, area, name = row['key'], row['area'], AREAS.get(row['area'], row['area'])
    parts = key.split('.')
    flow, indicator, kind = parts[0], parts[6], parts[7]
    if flow == 'CLIFS':
        return 'CLIFS_' + area, name + ' CLIFS', 'clifs'
    if indicator == 'SOV_CIN':
        return 'SOV_' + area, name + ' SovCISS', 'sovciss'
    if indicator == 'SOV_GDPWN':
        return 'EA_SOV_GDP', 'Euro area SovCISS, GDP-weighted', 'euro_area'
    if indicator == 'SOV_EWN':
        return 'EA_SOV_EQ', 'Euro area SovCISS, equal-weighted', 'euro_area'
    if indicator == 'SS_CIN' and kind == 'IDX':
        return ('EA_CISS', 'Euro area CISS (systemic stress)', 'euro_area') if area == 'U2' else ('CISS_' + area, name + ' CISS', 'ciss')
    if area == 'U2' and kind == 'CON' and indicator in CONTRIBUTION:
        return 'EA_CON_' + indicator[3:], CONTRIBUTION[indicator] + ' contribution to the euro-area CISS', 'contribution'
    return None


def compile_desk(binding, receipts, aux, read, generated_at, errors=None):
    """(desk document, history document) from stored originals only.

    binding   the warehouse run this document stands on: {'generated_at', 'manifest_key', 'output_sha256'}
    receipts  {ECB key: evidence receipt} for the warehouse's CISS / SovCISS / CLIFS series
    aux       {name: {'evidence', 'acquired_at', 'request_url', optional 'carried'}} for the added ECB responses
    read      receipt -> the exact original bytes
    """
    now = clock(generated_at)
    today = now.date().isoformat()
    issues = dict(errors or {})
    measures, history, sources, ended = {}, {}, [], []

    def add(m, obs):
        if m is None:
            return
        if m['status'] == 'ended':
            ended.append({'id': m['id'], 'key': m['key'], 'last': m['date']})
            return
        if m['id'] in measures:
            issues[m['id']] = 'DUPLICATE_MEASURE'
            return
        measures[m['id']] = m
        history[m['id']] = history_of(obs, m['unit'], m['freq'])

    # 1. the warehouse's own series, each from its original CSV
    sov = {}
    for key in sorted(receipts):
        named = ciss_id({'key': key, 'area': key.split('.')[2]})
        if not named:
            continue
        try:
            entry = flow_series(read(receipts[key])).get(key)
            if entry is None:
                raise ValueError('response holds another series')
            obs, info = observations(entry, today)
            if not obs or (now.date() - date.fromisoformat(obs[-1][0])).days > ENDED:
                ended.append({'id': named[0], 'key': key, 'last': obs[-1][0] if obs else None})
                continue
            add(measure(named[0], named[1], 'index', freq_of(key), obs, today, info, key=key, evidence=receipts[key], group=named[2], area=key.split('.')[2]), obs)
            if named[2] == 'sovciss':
                sov[key.split('.')[2]] = obs
        except (ValueError, KeyError, TypeError, OverflowError) as exc:
            issues[key] = 'INVALID_ORIGINAL:' + type(exc).__name__
    sources.append({'name': 'ciss', 'label': 'CISS, SovCISS and CLIFS (the warehouse)', 'series': len(receipts), 'warehouse': binding})

    # the cross-section of country SovCISS, on days every country has a reading
    cross = None
    if len(sov) >= 3:
        days = set.intersection(*(set(d for d, _ in obs) for obs in sov.values()))
        lookup = {a: dict(obs) for a, obs in sov.items()}
        spread = [(d, max(lookup[a][d] for a in lookup) - min(lookup[a][d] for a in lookup)) for d in sorted(days)]
        if spread:
            add(measure('EA_SOV_RANGE', 'Highest minus lowest country SovCISS', 'index', 'D', spread, today, group='euro_area',
                        derived='max minus min of the %d country SovCISS readings of the same day' % len(sov)), spread)
            day = spread[-1][0]
            ranked = sorted(((a, lookup[a][day]) for a in lookup), key=lambda p: (-p[1], p[0]))
            cross = {'date': day, 'n': len(ranked), 'ranking': [{'area': a, 'value': number(v, 'index')} for a, v in ranked],
                     'median': number(statistics.median(v for _, v in ranked), 'index'), 'range': number(ranked[0][1] - ranked[-1][1], 'index')}

    # 2. the added ECB responses
    parsed = {}
    for name in AUX:
        item = aux.get(name)
        if not item:
            issues.setdefault(name, 'UNAVAILABLE')
            continue
        try:
            parsed[name] = flow_series(read(item['evidence']))
            newest = max(p['period'] for e in parsed[name].values() for p in e['rows'])
            sources.append({'name': name, 'label': AUX_LABEL[name], 'path': AUX[name], 'evidence': item['evidence'], 'acquired_at': item['acquired_at'],
                            'carried': bool(item.get('carried')), 'series': len(parsed[name]), 'rows': sum(len(e['rows']) for e in parsed[name].values()), 'newest': newest})
        except (ValueError, KeyError, TypeError, OverflowError) as exc:
            issues[name] = 'INVALID_ORIGINAL:' + type(exc).__name__

    def aux_obs(name, key, scale=None):
        return observations(parsed[name][key], today, scale)

    def receipt(name):
        return aux[name]['evidence']

    # the curve: all issuers minus AAA issuers
    if 'curve' in parsed:
        for tenor in ('10Y', '2Y'):
            pair = {}
            for code, who in (('G_N_A', 'AAA'), ('G_N_C', 'ALL')):
                key = 'YC.B.U2.EUR.4F.%s.SV_C_YM.SR_%s' % (code, tenor)
                if key in parsed['curve']:
                    obs, info = aux_obs('curve', key)
                    pair[who] = obs
                    label = 'Euro-area government %s-year spot rate, %s' % (tenor[:-1], 'AAA issuers' if who == 'AAA' else 'all issuers')
                    add(measure('EA_%s_%s' % (who, tenor), label, 'pct', 'D', obs, today, info, key=key, evidence=receipt('curve'), group='curve'), obs)
            if len(pair) == 2:
                gap = aligned(pair['ALL'], pair['AAA'], Decimal(100))
                add(measure('EA_CURVE_' + tenor, 'All euro-area governments minus AAA governments, %s-year' % tenor[:-1], 'bp', 'D', gap, today, group='euro_area',
                            evidence=receipt('curve'), derived='all-issuer spot rate minus AAA-issuer spot rate of the same day, in basis points'), gap)

    # 10-year yields and the spread to Germany
    members = set()
    if 'yields' in parsed:
        best = {}
        for key, entry in parsed['yields'].items():
            area = key.split('.')[2]
            if area not in model.COUNTRIES:
                continue                                        # EU and fixed-composition aggregates are not on the desk
            obs, info = aux_obs('yields', key)
            if obs and (area not in best or obs[-1][0] > best[area][1][-1][0]):
                best[area] = (key, obs, info)
        german = best.get('DE', (None, None, None))[1]
        for area in sorted(best):
            key, obs, info = best[area]
            name = AREAS.get(area, area)
            if key.split('.')[7] == 'EUR' and area in model.COUNTRIES and area != 'U2':
                members.add(area)
            add(measure('Y10_' + area, name + ' 10-year government yield', 'pct', 'M', obs, today, info, key=key, evidence=receipt('yields'), group='yield', area=area), obs)
            if german and area != 'DE' and 'Y10_' + area in measures:
                gap = aligned(obs, german, Decimal(100))
                add(measure('S10_' + area, name + ' 10-year spread to Germany', 'bp', 'M', gap, today, group='spread', area=area, evidence=receipt('yields'),
                            derived='10-year yield minus Germany\'s of the same month, in basis points'), gap)

    # TARGET balances, EUR billions
    if 'target' in parsed:
        for key in sorted(parsed['target']):
            area = key.split('.')[2]
            if area not in model.COUNTRIES and area != '4F':
                continue
            entry = parsed['target'][key]
            scale = Decimal(10) ** (int(entry['meta']['unit_mult']) - 9)
            obs, info = aux_obs('target', key, scale)
            add(measure('TGT_' + area, AREAS.get(area, area) + ' TARGET balance', 'eur_bn', 'M', obs, today, info, key=key, evidence=receipt('target'), group='target', area=area), obs)

    # government debt, percent of GDP
    if 'debt' in parsed:
        for key in sorted(parsed['debt']):
            area = key.split('.')[3]
            if area not in model.COUNTRIES and area != 'I9':
                continue
            obs, info = aux_obs('debt', key)
            add(measure('DEBT_' + area, AREAS.get(area, area) + ' government debt', 'pct_gdp', 'Q', obs, today, info, key=key, evidence=receipt('debt'), group='debt', area=area), obs)

    # the CISS page's auxiliaries
    auxiliaries = []
    for name, mid, label, unit, freq, billions in (('eurusd', 'AUX_EURUSD', 'EUR/USD reference rate', 'usd', 'D', False),
                                                    ('fx_claims_ea', 'AUX_FX_EA', 'Claims on euro-area residents in foreign currency', 'eur_bn', 'W', True),
                                                    ('fx_claims_nonea', 'AUX_FX_NONEA', 'Claims on non-euro-area residents in foreign currency', 'eur_bn', 'W', True)):
        if name not in parsed:
            continue
        for key in sorted(parsed[name]):
            entry = parsed[name][key]
            scale = Decimal(10) ** (int(entry['meta']['unit_mult']) - 9) if billions else None
            obs, info = aux_obs(name, key, scale)
            before = len(measures)
            add(measure(mid, label, unit, freq, obs, today, info, key=key, evidence=receipt(name), group='auxiliary'), obs)
            if len(measures) > before:
                auxiliaries.append(mid)

    # one row per country: which measures exist for it
    countries = []
    for area in sorted({m['area'] for m in measures.values() if m.get('area') and m['area'] in model.COUNTRIES and m['area'] != 'U2'}):
        ids = {kind: prefix + area for kind, prefix in (('sovciss', 'SOV_'), ('ciss', 'CISS_'), ('clifs', 'CLIFS_'), ('yield', 'Y10_'), ('spread', 'S10_'),
                                                         ('target', 'TGT_'), ('debt', 'DEBT_')) if prefix + area in measures}
        countries.append({'area': area, 'country': AREAS[area], 'euro': area in members, 'ids': ids})

    doc = {'engine': 'ciss-stress', 'document': 'sovereign', 'version': VERSION, 'contract': CONTRACT, 'generated_at': generated_at,
           'warehouse': binding, 'episodes': EPISODES, 'measures': measures, 'countries': countries, 'cross_section': cross,
           'euro_area': [m for m in ('EA_SOV_GDP', 'EA_SOV_EQ', 'EA_SOV_RANGE', 'EA_CURVE_10Y', 'EA_CURVE_2Y', 'EA_CISS', 'EA_CON_BMN', 'Y10_U2', 'EA_ALL_10Y', 'EA_AAA_10Y', 'TGT_4F')
                         if m in measures],
           'contributions': [m for m in ('EA_CON_BMN', 'EA_CON_EMN', 'EA_CON_FIN', 'EA_CON_FXN', 'EA_CON_MMN', 'EA_CON_CON') if m in measures],
           'auxiliaries': auxiliaries, 'sources': sources, 'ended_at_source': sorted(ended, key=lambda e: (e['key'] or '', e['id'])), 'errors': issues,
           'coverage': {'measures': len(measures), 'countries': len(countries), 'errors': len(issues), 'status': 'degraded' if issues else 'measured'},
           'units': {'index': 'ECB composite indicator, 0 to 1', 'pct': 'percent per annum', 'bp': 'basis points', 'eur_bn': 'EUR billions',
                     'pct_gdp': 'percent of GDP', 'usd': 'US dollars per euro'},
           'definitions': {'pctile': 'share of the series\' own observations at or below the latest, percent; not a probability of crisis',
                           'change': 'latest minus the source\'s reading one horizon earlier, in the measure\'s own unit',
                           'peaks': 'highest and lowest reading inside each named window', 'status': 'current, late (older than the source\'s rhythm allows) or ended'},
           'calls_eligible': False, 'sizing_eligible': False,
           'decision': {'verb': 'WAIT', 'meaning': 'abstain', 'reason': 'Dated stress measurements do not establish a validated forecast or portfolio size.'}}
    hist = {'engine': 'ciss-stress', 'document': 'sovereign-history', 'contract': HISTORY_CONTRACT, 'version': VERSION, 'generated_at': generated_at,
            'aggregation': 'daily series: last observation of each ISO week; monthly, weekly and quarterly series: every observation', 'series': history}
    return doc, hist


# ----------------------------------------------------------------------------------------------- storage and publication

def gather(client, bucket, store, deadline, previous=None, pause=1.0, sleep=time.sleep):
    """Ask the ECB for each added response once, retain it, and fall back to the last retained one if the ECB refuses."""
    from evidence_store import capture
    aux, errors = {}, {}
    kept = {s['name']: s for s in (previous or {}).get('sources') or [] if s.get('evidence') and s.get('path')}
    for position, (name, path) in enumerate(AUX.items()):
        url = BASE + path + '?format=csvdata'
        try:
            if position:
                sleep(pause)
            raw, received = store.request(url, deadline)
            flow_series(raw)                                    # an error page is not evidence
            receipt = capture(client, bucket, 'ecb', url, raw, received)
            if store.original(client, bucket, receipt) != raw:
                raise ValueError('source readback differs')
            aux[name] = {'evidence': receipt, 'acquired_at': received.isoformat(), 'request_url': url}
        except Exception as exc:                                # noqa: BLE001 - one refused response must not cost the others
            errors[name] = store.safe_error(exc)
            old = kept.get(name)
            if old and old.get('path') == path:
                aux[name] = {'evidence': old['evidence'], 'acquired_at': old['acquired_at'], 'request_url': url, 'carried': True}
                errors[name] += ' - the last retained response of %s is used' % old['acquired_at'][:10]
    return aux, errors


def run(client, bucket, store, budget_seconds=240, now=None):
    """Compile from the current warehouse and publish.  Never raises into the warehouse run that called it."""
    deadline = time.monotonic() + budget_seconds
    warehouse, _, _ = store.read(client, bucket, WAREHOUSE)
    if warehouse.get('contract') != model.CONTRACT or not (warehouse.get('replay') or {}).get('manifest_key'):
        raise ValueError('canonical CISS warehouse required')
    binding = {'generated_at': warehouse['generated_at'], 'manifest_key': warehouse['replay']['manifest_key'], 'output_sha256': warehouse['replay']['output_sha256']}
    receipts = {row['key']: row['evidence'] for row in warehouse['series'] if ciss_id(row)}
    try:
        previous, _, _ = store.read(client, bucket, CURRENT)
    except Exception as exc:                                    # noqa: BLE001
        if not store.missing(exc):
            raise
        previous = None
    aux, errors = gather(client, bucket, store, deadline, previous)
    generated = (now or datetime.now(timezone.utc)).isoformat()

    def read(receipt):
        return store.original(client, bucket, receipt)

    doc, hist = compile_desk(binding, receipts, aux, read, generated, errors)
    body = Path(__file__).read_bytes()
    sha = hashlib.sha256(body).hexdigest()
    compiler = {'key': PREFIX + 'compilers/' + sha + '.py', 'sha256': sha}
    store.immutable(client, bucket, compiler['key'], body, 'text/plain')
    manifest = {'contract': 'ciss-sovereign-replay.v1', 'generated_at': generated, 'warehouse': binding, 'receipts': receipts,
                'aux': aux, 'errors': errors, 'compiler': compiler, 'support': {'ciss_source_model.py': hashlib.sha256(Path(model.__file__).read_bytes()).hexdigest()},
                'output_sha256': digest(doc), 'history_sha256': digest(hist), 'output_excludes': ['replay', 'history']}
    manifest_key = RUNS + digest(manifest) + '.json'
    store.immutable(client, bucket, manifest_key, encoded(manifest))
    again, again_hist = compile_desk(binding, receipts, aux, read, generated, errors)      # a second, separate read of every original
    if digest(again) != manifest['output_sha256'] or digest(again_hist) != manifest['history_sha256']:
        raise ValueError('sovereign desk replay differs before publication')
    doc['replay'] = {'manifest_key': manifest_key, 'output_sha256': manifest['output_sha256'], 'compiler': compiler}
    doc['history'] = {'key': HISTORY, 'sha256': manifest['history_sha256']}
    client.put_object(Bucket=bucket, Key=HISTORY, Body=encoded(hist), ContentType='application/json', CacheControl='no-cache')
    client.put_object(Bucket=bucket, Key=CURRENT, Body=encoded(doc), ContentType='application/json', CacheControl='no-cache')
    return {'published': True, 'generated_at': generated, 'coverage': doc['coverage'], 'replay': doc['replay']}
