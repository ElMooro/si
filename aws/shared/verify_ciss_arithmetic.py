"""Independent ECB row/calendar/sum checks. No production compiler imports.

Fractions preserve original decimals. This checks measurement arithmetic, not
percentiles, z scores, historical information availability or predictive edge.
"""
import calendar
import csv
from datetime import date, datetime, timedelta, timezone
from fractions import Fraction
import io
import math
import re

HEAD = 'CISS.D.U2.Z0Z.4F.EC.SS_CIN.IDX'
COMPONENTS = tuple('CISS.D.U2.Z0Z.4F.EC.'+code+'.CON' for code in
                   ('SS_BMN', 'SS_EMN', 'SS_FIN', 'SS_FXN', 'SS_MMN', 'SS_CON'))
DIMENSIONS = ('FREQ', 'REF_AREA', 'CURRENCY', 'PROVIDER_FM', 'INSTRUMENT_FM', 'PROVIDER_FM_ID', 'DATA_TYPE_FM')
TOLERANCE = Fraction(1, 10**10)


def require(condition, message):
    if not condition: raise ValueError(message)


def clock(value):
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    require(stamp.tzinfo is not None, 'Aware source clock required')
    return stamp.astimezone(timezone.utc)


def number(text):
    require(isinstance(text, str) and len(text) <= 128, 'Bounded source decimal required')
    require(re.fullmatch(r'[+-]?(?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?', text), 'Invalid source decimal')
    exponent = re.search(r'[eE]([+-]?\d+)$', text)
    require(not exponent or abs(int(exponent[1])) <= 300, 'Source exponent exceeds bound')
    value = Fraction(text)
    require(math.isfinite(float(value)), 'Nonfinite source decimal')
    return value


def parse(raw, expected=None):
    require(isinstance(raw, bytes), 'Whole original CSV bytes required')
    reader = csv.DictReader(io.StringIO(raw.decode('utf-8-sig')))
    names = reader.fieldnames or []
    require(len(names) == len(set(names)), 'Duplicate CSV columns')
    require(set(('KEY', *DIMENSIONS, 'TIME_PERIOD', 'OBS_VALUE', 'OBS_STATUS', 'UNIT', 'UNIT_MULT')) <= set(names), 'Missing CSV columns')
    groups = {}; count = 0
    for index, row in enumerate(reader):
        require(None not in row and all(v is not None for v in row.values()), 'Incomplete CSV row')
        key = row['KEY']; pieces = key.split('.')
        require(len(pieces) == 8 and pieces[0] in ('CISS', 'CLIFS') and all(re.fullmatch(r'[A-Z0-9_]+', s) for s in pieces), 'Invalid ECB key')
        require(expected is None or key == expected, 'Different history series')
        require(pieces[1:] == [row[d].strip() for d in DIMENSIONS], 'ECB dimensions differ')
        require(row['UNIT'] == 'PURE_NUMB' and row['UNIT_MULT'] == '0', 'ECB unit or scale differs')
        require(pieces[1] in ('D', 'M') and pieces[-1] in ('IDX', 'CON'), 'Unsupported ECB measurement')
        period = row['TIME_PERIOD']
        if pieces[1] == 'D':
            require(re.fullmatch(r'\d{4}-\d{2}-\d{2}', period), 'Invalid daily period')
            end = date.fromisoformat(period)
        else:
            require(re.fullmatch(r'\d{4}-\d{2}', period), 'Invalid monthly period')
            year, month = map(int, period.split('-')); end = date(year, month, calendar.monthrange(year, month)[1])
        text = row['OBS_VALUE'].strip(); value = number(text) if text else None
        require(value is None or pieces[0] != 'CISS' or pieces[-1] != 'IDX' or 0 <= value <= 1, 'Index outside defined range')
        point = {'period': period, 'end': end, 'text': text or None, 'value': value,
                 'status': row['OBS_STATUS'].strip(), 'source_row': index}
        entry = groups.setdefault(key, {'rows': {}, 'duplicates': 0, 'source_rows': 0})
        entry['source_rows'] += 1; count += 1
        if period in entry['rows']:
            old = entry['rows'][period]
            require((old['value'], old['status']) == (value, point['status']), 'Conflicting duplicate observation')
            entry['duplicates'] += 1
        else: entry['rows'][period] = point
    require(groups, 'Empty original CSV')
    for entry in groups.values(): entry['rows'] = sorted(entry['rows'].values(), key=lambda row: row['end'])
    return groups, count


def usable(row): return row['value'] is not None and row['status'] in ('A', 'E', 'P')


def scalar(actual, exact, label):
    if exact is None: require(actual is None, label+' must be null'); return
    require(type(actual) in (int, float) and math.isfinite(actual) and actual == float(exact), label+' differs')


def decimal(actual, exact, label):
    if exact is None: require(actual is None, label+' must be null'); return
    require(number(actual) == exact, label+' differs')


def target_day(end, months=0, days=0):
    if not months: return end-timedelta(days=days)
    year, offset = divmod(end.year*12+end.month-1-months, 12)
    return date(year, offset+1, min(end.day, calendar.monthrange(year, offset+1)[1]))


def comparison(rows, current, frequency, months=0, days=0):
    target = target_day(current['end'], months, days)
    candidates = [row for row in rows if
        (months and row['period'] == target.strftime('%Y-%m'))] if frequency == 'M' else [
        row for row in rows if timedelta(0) <= target-row['end'] <= timedelta(days=7)]
    return target, candidates[-1] if candidates else None


def verify(packet, histories, discoveries):
    """Check every compiled series and the entire retrieved contribution panel.

    Full native replay must separately bind these bytes and all output fields.
    Historical sum differences are measured, not concealed or relabeled errors
    in the current packet. Only its same-date latest sum controls current use.
    """
    now = clock(packet['generated_at']); catalog = {}; discovery_count = 0
    for flow, raw in discoveries.items():
        groups, count = parse(raw); discovery_count += count
        require(all(key.startswith(flow+'.') for key in groups), 'Discovery flow differs')
        for key, group in groups.items():
            require(key not in catalog, 'Duplicate discovered series')
            catalog[key] = group['rows'][-1]['period']
    require(set(packet['catalog']) == set(catalog), 'Complete discovery catalog differs')
    require(set(histories) == set(catalog), 'Complete discovered histories required')
    series = {row['key']: row for row in packet['series']}
    require(len(series) == len(packet['series']) and set(series) == set(histories), 'Whole compiled series population differs')
    selected = {}; proofs = {}; original_count = 0; comparisons = 0; charts = 0
    for key in sorted(histories):
        groups, count = parse(histories[key], key); original_count += count
        group = groups[key]; rows = [row for row in group['rows'] if row['end'] <= now.date()]
        require(rows, 'No completed periods'); current = rows[-1]; out = series[key]; frequency = key.split('.')[1]
        require(packet['catalog'][key]['latest_discovered_period'] == catalog[key], 'Discovered latest period differs')
        require(out['source_row'] == current['source_row'] and type(out['source_row']) is int, 'Latest original coordinate differs')
        require(out['latest_date'] == current['period'] and out['observation_period_end'] == current['end'].isoformat(), 'Latest period differs')
        require(out['observation_status'] == current['status'], 'Observation status differs')
        require(out['unit'] == ('dimensionless_index' if key.endswith('.IDX') else 'dimensionless_contribution'), 'Published unit differs')
        require(type(out['n_obs']) is int and type(out['n_numeric']) is int and out['n_obs'] == len(rows) and out['n_numeric'] == sum(usable(row) for row in rows), 'History population differs')
        require(type(out['duplicate_rows']) is int and out['duplicate_rows'] == group['duplicates'], 'Duplicate count differs')
        acquired = clock(out['acquired_at']); received = clock(out['evidence']['first_received_at'])
        require(received <= acquired <= now, 'Source acquisition clocks differ')
        age = (now.date()-current['end']).days; maximum = 14 if frequency == 'D' else 95
        status = 'missing' if not usable(current) else 'stale' if age > maximum or now-acquired > timedelta(hours=72) else 'fresh'
        if current['period'] < catalog[key]: status = 'incomplete'
        # A mismatched same-date headline sum also invalidates the headline.
        expected_status = 'invalid' if key == HEAD and packet['headline_reconciliation']['status'] == 'mismatch' else status
        require(out['quality']['status'] == expected_status, 'Source quality differs')
        q = out['quality']
        require(q['maximum_observation_age_days'] == maximum and q['maximum_acquisition_age_seconds'] == 72*3600,
                'Source clock policy differs')
        require(q['observation_age_days'] == age and q['acquisition_age_seconds'] == (now-acquired).total_seconds(), 'Source age differs')
        require(q['excluded_future_rows'] == len(group['rows'])-len(rows), 'Future row count differs')
        live = expected_status == 'fresh'; exact = current['value'] if live else None
        scalar(out['latest'], exact, 'Latest'); decimal(out['latest_decimal'], exact, 'Latest decimal')
        good = [row for row in rows if usable(row)]
        scalar(out['last_observed_value'], good[-1]['value'] if good else None, 'Last historical value')
        require(out['last_observed_date'] == (good[-1]['period'] if good else None), 'Last historical date differs')
        chart_rows = {}
        for row in rows:
            identity = tuple(row['end'].isocalendar()[:2]) if frequency == 'D' else row['period']
            chart_rows[identity] = row
        expected_chart = [[row['period'], float(row['value']) if usable(row) else None] for row in chart_rows.values()]
        require(isinstance(out['chart_points'], list) and len(out['chart_points']) == len(expected_chart), 'Complete source chart population differs')
        for actual, expected in zip(out['chart_points'], expected_chart):
            require(isinstance(actual, list) and len(actual) == 2 and actual[0] == expected[0], 'Source chart coordinate differs')
            scalar(actual[1], expected[1], 'Source chart value')
        charts += len(expected_chart)
        references = {}
        for name, months, days in (('1w', 0, 7), ('1m', 1, 0), ('3m', 3, 0), ('12m', 12, 0)):
            target, base = comparison(rows, current, frequency, months, days); entry = out['comparisons'][name]
            require(entry['unit'] == 'index_points' and entry['target_date'] == target.isoformat(), 'Comparison definition differs')
            require(entry['baseline_period'] == (base['period'] if base else None), 'Baseline period differs')
            require(entry['baseline_source_row'] == (base['source_row'] if base else None) and
                    (not base or type(entry['baseline_source_row']) is int), 'Baseline original coordinate differs')
            baseline = base['value'] if base and usable(base) else None
            scalar(entry['baseline_value'], baseline, 'Baseline'); decimal(entry['baseline_decimal'], baseline, 'Baseline decimal')
            delta = current['value']-baseline if live and baseline is not None else None
            scalar(entry['value'], delta, 'Calendar change'); comparisons += 1
            references[name] = {'target_date': target.isoformat(), 'baseline_period': base['period'] if base else None,
                                'baseline_source_row': base['source_row'] if base else None, 'value': entry['value'], 'unit': 'index_points'}
        annual = out['annual_comparison']; twelve = out['comparisons']['12m']
        require(all(annual[name] == twelve[name] for name in ('target_date', 'baseline_period', 'baseline_source_row', 'unit')),
                'Legacy annual comparison coordinates differ')
        require(annual['baseline_source_row'] is None or type(annual['baseline_source_row']) is int, 'Legacy annual coordinate type differs')
        scalar(annual['baseline_value'], twelve['baseline_value'], 'Annual baseline')
        scalar(annual['value'], twelve['value'], 'Annual change'); scalar(out['chg_1y'], twelve['value'], 'Annual alias')
        if key in (HEAD, *COMPONENTS):
            selected[key] = {row['period']: row for row in rows}
            proofs[key] = {'latest_period': current['period'], 'original_row': current['source_row'],
                          'reported_decimal': current['text'], 'status': current['status'], 'source_quality': status,
                          'source_rows': count, 'comparisons': references}
    dates = sorted(set().union(*(set(rows) for rows in selected.values())))
    panel = {'matched': 0, 'mismatch': 0, 'unavailable': 0}; last = None
    for period in dates:
        points = [selected.get(key, {}).get(period) for key in (HEAD, *COMPONENTS)]
        complete = all(point is not None and usable(point) for point in points)
        total = sum(point['value'] for point in points[1:]) if complete else None
        residual = total-points[0]['value'] if complete else None
        verdict = 'unavailable' if not complete else 'matched' if abs(residual) <= TOLERANCE else 'mismatch'
        panel[verdict] += 1
        if HEAD in proofs and period == proofs[HEAD]['latest_period']:
            last = (total, residual, verdict)
    rec = packet['headline_reconciliation']
    ready = all(key in proofs and proofs[key]['source_quality'] == 'fresh' and proofs[key]['latest_period'] == proofs[HEAD]['latest_period'] for key in (HEAD, *COMPONENTS))
    expected = last[2] if ready and last else 'unavailable'
    require(rec['status'] == expected, 'Headline reconciliation state differs')
    require(rec['date'] == (proofs[HEAD]['latest_period'] if HEAD in proofs else None), 'Headline reconciliation date differs')
    require(len(rec['component_keys']) == len(set(rec['component_keys'])) and set(rec['component_keys']) == set(COMPONENTS) & set(series), 'Headline contribution identities differ')
    decimal(rec['component_sum_decimal'], last[0] if ready else None, 'Contribution sum')
    decimal(rec['residual_decimal'], last[1] if ready else None, 'Contribution residual')
    decimal(rec['tolerance_decimal'], TOLERANCE, 'Contribution tolerance')
    scalar(packet['ea_composite'], series[HEAD]['latest'] if HEAD in series else None, 'Headline composite')
    return {'contract': 'ciss-independent-arithmetic.v1', 'series_checked': len(series),
            'original_history_rows_checked': original_count, 'original_discovery_rows_checked': discovery_count,
            'calendar_comparisons_checked': comparisons, 'chart_points_checked': charts,
            'annual_comparison_aliases_checked': len(series),
            'headline_panel_periods_checked': len(dates), 'headline_panel': panel, 'latest_reconciliation': expected,
            'selected_series': proofs, 'statistics_independently_verified': False,
            'historical_point_in_time_verified': False, 'predictive_edge_verified': False}
