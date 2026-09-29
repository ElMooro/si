"""Check FR2004 source rows and every published scope with integer arithmetic.

No production compiler imports or IO. The caller authenticates complete source
bytes, definitions and the compiled packet before this separate calculation.
This does not certify the reported positions, unique fails or forecasting skill.
"""
from datetime import date
from fractions import Fraction
import math
import re

SUFFIXES = ('USTET', 'UST', 'CS', 'FGM', 'FGEM', 'OM')
CLASSES = ('ust_ex_tips', 'tips', 'corporate', 'agency_mbs', 'agency_debt', 'other_mbs')
PAIRS = {name: ('PDFTD-'+suffix, 'PDFTR-'+suffix) for name, suffix in zip(CLASSES, SUFFIXES)}
PERIODS = (('2013-04-01', '2014-12-31', 'SBN2013'), ('2015-01-01', '2022-01-04', 'SBN2015'),
           ('2022-01-05', '2024-07-02', 'SBN2022'), ('2024-07-03', '9999-12-31', 'SBN2024'))


def require(ok, reason):
    if not ok: raise ValueError('Independent fails arithmetic: '+reason)


def integer(value, expected, reason):
    require(value is None if expected is None else type(value) is int and value == expected, reason)


def billion(value, expected):
    require(value is None if expected is None else type(value) in (int, float) and math.isfinite(value)
            and value == float(Fraction(expected, 1000)), 'USD billion conversion differs')


def verify(packet, original):
    require(packet.get('contract') == 'fr2004-fails-research.v1', 'source contract differs')
    rows = original.get('pd', {}).get('timeseries')
    require(isinstance(rows, list) and 0 < len(rows) <= 50000, 'complete original rows required')
    expected_keys = {key for pair in PAIRS.values() for key in pair}
    series = {key: {} for key in expected_keys}
    for index, row in enumerate(rows):
        require(isinstance(row, dict) and row.get('keyid') in series, 'unexpected series')
        key, observed, text = row['keyid'], row.get('asofdate'), row.get('value')
        require(isinstance(observed, str) and re.fullmatch(r'\d{4}-\d{2}-\d{2}', observed), 'exact period required')
        date.fromisoformat(observed)
        require(observed not in series[key], 'duplicate report')
        require(text is None or type(text) is str, 'original number must be a string')
        if text in (None, '', '*'): amount = None
        else:
            require(re.fullmatch(r'(?:0|[1-9]\d{0,14})', text), 'whole nonnegative reported million required')
            amount = int(text)
            require(amount <= (2**53-1)//12, 'integer limit')
        period = [p for start, end, p in PERIODS if start <= observed <= end]
        require(len(period) == 1, 'unreviewed reporting definition')
        series[key][observed] = {'row_index': index, 'value_usd_mn': amount,
                                'status': 'suppressed' if text == '*' else 'missing' if amount is None else 'observed'}
    require(all(series.values()), 'missing requested series')
    require(set(packet['series_coverage']) == expected_keys, 'coverage series differ')
    for key, observations in series.items():
        require(packet['series_coverage'][key] == {'observations': len(observations),
            'suppressed': sum(r['status'] == 'suppressed' for r in observations.values()),
            'missing': sum(r['status'] == 'missing' for r in observations.values())}, 'coverage differs')

    classes = packet['classes']
    require(isinstance(classes, list) and [r.get('key') for r in classes] == list(CLASSES), 'six distinct classes required')
    scopes = [(r, [PAIRS[r['key']]]) for r in classes]
    scopes += [(packet['headline'], [PAIRS['ust_ex_tips']]),
               (packet['treasury'], [PAIRS['ust_ex_tips'], PAIRS['tips']]),
               (packet['totals'], list(PAIRS.values()))]
    history_count = sums = incomplete = 0
    for scope, pairs in scopes:
        keys = [key for pair in pairs for key in pair]
        require(scope['source_series'] == keys, 'scope membership differs')
        require(scope['unit'] == 'usd_bn' and scope['native_unit'] == 'usd_mn', 'scope units differ')
        require(scope['period_measure'] == 'cumulative_reported_fails', 'period meaning differs')
        dates = sorted(set().union(*(set(series[key]) for key in keys)))
        history = scope['history']
        require(len(history) == len(dates), 'history truncated or extended')
        reconstructed = []
        for observed, row in zip(dates, history):
            require(row['date'] == observed, 'history period differs')
            expected_period = next(p for start, end, p in PERIODS if start <= observed <= end)
            require(row['seriesbreak'] == expected_period, 'definition period differs')
            components = {key: series[key].get(observed, {'row_index': None, 'value_usd_mn': None,
                                                        'status': 'missing_observation'}) for key in keys}
            require(row['components'] == components, 'original row coordinates or missingness differ')
            totals = []
            for side in (0, 1):
                values = [series[pair[side]].get(observed, {}).get('value_usd_mn') for pair in pairs]
                totals.append(sum(values) if all(v is not None for v in values) else None)
            totals.append(sum(totals) if all(v is not None for v in totals) else None)
            for field, expected in zip(('ftd_usd_mn', 'ftr_usd_mn', 'gross_usd_mn'), totals):
                integer(row[field], expected, 'scope sum differs'); sums += 1
            require(row['complete'] is (totals[2] is not None), 'completeness differs')
            incomplete += totals[2] is None
            reconstructed.append(totals); history_count += 1
        latest = reconstructed[-1]
        require(scope['as_of'] == dates[-1] and scope['complete'] is (latest[2] is not None), 'latest reporting period differs')
        for index, side in enumerate(('ftd', 'ftr', 'gross')):
            expected = latest[index]
            integer(scope[side+'_usd_mn'], expected, 'latest native amount differs')
            billion(scope[side+'_bn'], expected)
            exact = scope['exact_usd_bn'][side]
            require(exact is None if expected is None else type(exact) is str and Fraction(exact) == Fraction(expected, 1000),
                    'exact decimal conversion differs')
            require(len(scope[side]) == len(dates), 'compatibility history truncated')
            for observed, values, point in zip(dates, reconstructed, scope[side]):
                require(point[0] == observed, 'compatibility period differs'); billion(point[1], values[index])
        billion(scope['combined_bn'], latest[2])
        require(scope['combined'] == scope['gross'], 'gross alias differs')
        require(all(scope[field] is False for field in ('calls_eligible', 'sizing_eligible', 'execution_eligible')),
                'scope received investment authority')
        require(scope['call'] is None and scope['allocation_pct'] is None, 'scope action differs')
    require(all(packet[field] is False for field in ('calls_eligible', 'sizing_eligible', 'execution_eligible')),
            'packet received investment authority')
    return {'original_series': len(series), 'original_rows_checked': len(rows), 'scopes_checked': len(scopes),
            'scope_history_rows_checked': history_count, 'integer_sums_checked': sums,
            'incomplete_scope_rows_preserved': incomplete, 'production_compiler_imported': False,
            'statistics_independently_verified': False, 'forecast_qualified': False, 'sizing_qualified': False}
