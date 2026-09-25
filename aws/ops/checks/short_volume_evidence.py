"""Independent original-row reconciliation and disclosed sampled window arithmetic."""
from collections import Counter
from decimal import Decimal, localcontext
from fractions import Fraction
import hashlib

EPSILON = Fraction(1, 2 * 10**12)


def near(actual, expected):
    if expected is None:
        assert actual is None, 'Missing evidence became a number'
    else:
        assert actual is not None and abs(Fraction(actual) - expected) <= EPSILON, 'Independent arithmetic differs'


def qualify(inputs, compiled, original):
    """Check every compact point against its complete original, then sample math.

Every record's window dates, counts, absence lists and zero-volume lists are
checked. Fraction means/pooled ratios/variance and high-precision square roots
are checked for a deterministic sample, explicitly reported rather than called
a full-population arithmetic audit.
"""
    packet = compiled['packet']
    dates = packet['dates']
    records = {name: record for shard in compiled['shards'].values() for name, record in shard['records'].items()}
    cursors = Counter()
    rows_checked = 0
    for i, stamp in enumerate(dates):
        capture = inputs['captures']['daily:' + stamp]
        raw = original(capture['original'])
        lines = raw.decode('utf-8-sig').splitlines()
        assert lines[0] == 'Date|Symbol|ShortVolume|ShortExemptVolume|TotalVolume|Market'
        assert int(lines[-1]) == len(lines) - 2 == packet['daily_sources'][i]['rows']
        assert packet['daily_sources'][i]['original'] == capture['original']
        seen = set()
        for line_number, line in enumerate(lines[1:-1], start=2):
            fields = line.split('|')
            assert len(fields) == 6 and fields[0] == stamp.replace('-', '')
            name = fields[1]
            assert name not in seen
            seen.add(name)
            p = records[name]['points'][cursors[name]]
            assert p[:2] == [i, line_number] and p[-1] == fields[-1]
            assert all(Decimal(p[j + 2]) == Decimal(fields[j + 2]) for j in range(3))
            assert 0 <= Decimal(p[3]) <= Decimal(p[2]) <= Decimal(p[4])
            cursors[name] += 1
            rows_checked += 1
    assert sum(len(v['points']) for v in records.values()) == rows_checked == packet['counts']['source_rows']
    assert set(cursors) == set(records)
    sample = set(sorted(records, key=lambda name: hashlib.sha256(name.encode()).hexdigest())[:128])
    sample.update(name for name in ('AAPL', 'SPY', 'GME', 'AMC', 'BRK.A', 'TSLA', 'NVA') if name in records)
    signatures = set()
    latest_checks = 0
    for name, record in sorted(records.items()):
        assert cursors[name] == len(record['points'])
        points = {p[0]: p for p in record['points']}
        latest = points.get(60)
        expected_latest = Fraction(latest[2]) * 100 / Fraction(latest[4]) if latest and Decimal(latest[4]) else None
        near(record['latest_short_volume_pct'], expected_latest)
        latest_checks += 1
        signature = (60 in points, tuple(len(set(range(60 - n, 60)) & set(points)) for n in (5, 20, 60)))
        if signature not in signatures:
            signatures.add(signature)
            sample.add(name)
        for size in (5, 20, 60):
            window = record['windows'][str(size)]
            wanted = range(60 - size, 60)
            absent = [dates[i] for i in wanted if i not in points]
            zeros = [dates[i] for i in wanted if i in points and Decimal(points[i][4]) == 0]
            assert window['first_date'] == dates[60 - size] and window['last_date'] == dates[59]
            assert window['prior_published_files'] == size and window['excludes_latest'] is True
            assert window['missing_dates'] == absent and window['zero_volume_dates'] == zeros
            assert window['observed_rows'] == size - len(absent)
            assert window['positive_volume_rows'] == size - len(absent) - len(zeros)
            assert window['rows_complete'] == (not absent)
            assert window['daily_ratios_complete'] == (not absent and not zeros)
    sampled_windows = 0
    with localcontext() as ctx:
        ctx.prec = 192
        def decimal(value):
            return Decimal(value.numerator) / Decimal(value.denominator)
        for name in sorted(sample):
            record = records[name]
            points = {p[0]: p for p in record['points']}
            latest = points.get(60)
            last_ratio = Fraction(latest[2]) * 100 / Fraction(latest[4]) if latest and Decimal(latest[4]) else None
            for size in (5, 20, 60):
                w = record['windows'][str(size)]
                previous = [points[i] for i in range(60 - size, 60) if i in points]
                complete = len(previous) == size
                totals = [sum((Fraction(p[j]) for p in previous), Fraction(0)) for j in (2, 3, 4)]
                for key, expected in zip(('short_volume_shares', 'short_exempt_volume_shares', 'total_volume_shares'), totals):
                    assert (Fraction(w[key]) == expected) if complete else w[key] is None
                near(w['pooled_short_volume_pct'], totals[0] * 100 / totals[2] if complete and totals[2] else None)
                ratios = [Fraction(p[2]) * 100 / Fraction(p[4]) for p in previous] if complete and all(Decimal(p[4]) for p in previous) else []
                mean = sum(ratios) / size if ratios else None
                variance = sum((v - mean) ** 2 for v in ratios) / (size - 1) if ratios else None
                near(w['mean_daily_short_volume_pct'], mean)
                difference = last_ratio - mean if last_ratio is not None and mean is not None else None
                near(w['latest_minus_mean_percentage_points'], difference)
                if variance is None:
                    assert w['sample_sd_percentage_points'] is None
                else:
                    sd = decimal(variance).sqrt()
                    near(w['sample_sd_percentage_points'], Fraction(sd))
                near(w['descriptive_z_score'], Fraction(decimal(difference) / decimal(variance).sqrt()) if difference is not None and variance else None)
                sampled_windows += 1
    return {'original_rows_reconciled': rows_checked, 'latest_ratio_checks': latest_checks,
            'all_record_window_coverage_checks': len(records) * 3,
            'sampled_window_arithmetic_checks': sampled_windows, 'sampled_window_symbols': sorted(sample),
            'sample_basis': '128 lowest SHA256 literal symbols, named controls, and every observed row-count signature',
            'full_population_window_arithmetic_checked': False,
            'short_interest_available': False, 'historical_availability_verified': False}
