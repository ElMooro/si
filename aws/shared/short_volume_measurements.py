"""Dated, literal-symbol comparisons of FINRA trade-reporting measurements.

Daily ShortVolume includes ShortExemptVolume. It is a flow, not an outstanding
position. Windows contain the preceding N published files, excluding latest;
missing symbol rows and zero denominators are never filled or shortened away.
"""
from decimal import Decimal, localcontext, ROUND_HALF_EVEN
import offexchange_measurements as source

POINT_FIELDS = ('date_index', 'source_line', 'short_volume_shares',
                'short_exempt_volume_shares', 'total_volume_shares', 'facilities')
WINDOWS = (5, 20, 60)
ZERO = Decimal(0)
QUANTUM = Decimal('0.000000000001')


def rounded(value):
    if value is None:
        return None
    with localcontext() as ctx:
        ctx.prec = 64
        result = value.quantize(QUANTUM, rounding=ROUND_HALF_EVEN)
        return format(abs(result) if result == 0 else result, 'f')


def point(row, date_index):
    if type(date_index) is not int or not 0 <= date_index <= 60:
        raise ValueError('Bounded source-date index required')
    return [date_index, row['source_line'], row['short_volume_shares'],
            row['short_exempt_volume_shares'], row['total_volume_shares'],
            row['source_fields']['Market']]


def inspect(points, dates):
    if not isinstance(dates, list) or len(dates) != 61 or dates != sorted(set(dates)):
        raise ValueError('Exactly 61 ordered published-file dates required')
    for stamp in dates:
        source.day(stamp)
    indexed = {}
    previous = -1
    for p in points:
        if not isinstance(p, list) or len(p) != len(POINT_FIELDS):
            raise ValueError('Complete compact source point required')
        i, line, short, exempt, total, markets = p
        if type(i) is not int or not previous < i < 61 or type(line) is not int or line < 2:
            raise ValueError('Unique ordered source point and exact line required')
        short, exempt, total = map(source.scalar, (short, exempt, total))
        if not 0 <= exempt <= short <= total:
            raise ValueError('Inclusive volume reconciliation failed')
        facilities = markets.split(',') if isinstance(markets, str) else []
        if not facilities or len(set(facilities)) != len(facilities) or not set(facilities) <= set('BQND'):
            raise ValueError('Reviewed facility scope required')
        indexed[i] = (short, exempt, total, markets, line)
        previous = i
    if not indexed:
        raise ValueError('At least one reported observation required')
    return indexed


def comparisons(symbol, points, dates):
    """Retain all rows; calculate statistics only for their declared coverage."""
    source.symbol(symbol)
    indexed = inspect(points, dates)
    with localcontext() as ctx:
        ctx.prec = 64
        latest = indexed.get(60)
        latest_ratio = latest[0] * 100 / latest[2] if latest and latest[2] else None
        result = {
            'symbol': symbol, 'points': points,
            'security_identity_continuity_verified': False,
            'identity_basis': 'literal_source_symbol_only',
            'latest_date': dates[-1],
            'latest_point_present': latest is not None,
            'latest_short_volume_pct': rounded(latest_ratio),
            'latest_missing_reason': 'symbol_absent_from_latest_file' if latest is None else
                                     'zero_reported_volume' if not latest[2] else None,
            'observations': len(points), 'first_observation_date': dates[points[0][0]],
            'last_observation_date': dates[points[-1][0]], 'windows': {},
            'short_interest_shares': None, 'days_to_cover': None, 'squeeze_score': None,
            'covering_inferred': False, 'direction_inferred': False,
            'calls_eligible': False, 'sizing_eligible': False,
            'execution_eligible': False, 'forecast_qualified': False,
        }
        for size in WINDOWS:
            wanted = range(60 - size, 60)
            missing = [dates[i] for i in wanted if i not in indexed]
            present = [indexed[i] for i in wanted if i in indexed]
            zero_dates = [dates[i] for i in wanted if i in indexed and not indexed[i][2]]
            complete = not missing
            valid_ratios = complete and not zero_dates
            short_sum, exempt_sum, total_sum = (sum((p[j] for p in present), ZERO) for j in range(3))
            ratios = [p[0] * 100 / p[2] for p in present] if valid_ratios else []
            mean = sum(ratios, ZERO) / size if valid_ratios else None
            variance = sum(((v - mean) ** 2 for v in ratios), ZERO) / (size - 1) if valid_ratios else None
            deviation = variance.sqrt() if variance is not None else None
            difference = latest_ratio - mean if latest_ratio is not None and mean is not None else None
            scope_sets = {tuple(sorted(p[3].split(','))) for p in present}
            if latest:
                scope_sets.add(tuple(sorted(latest[3].split(','))))
            result['windows'][str(size)] = {
                'prior_published_files': size, 'excludes_latest': True,
                'first_date': dates[60 - size], 'last_date': dates[59],
                'observed_rows': len(present), 'positive_volume_rows': sum(p[2] > 0 for p in present),
                'missing_dates': missing, 'zero_volume_dates': zero_dates,
                'rows_complete': complete, 'daily_ratios_complete': valid_ratios,
                'missing_reason': 'symbol_absent_from_prior_files' if missing else
                                  'zero_reported_volume_in_prior_files' if zero_dates else None,
                'short_volume_shares': source.number(short_sum) if complete else None,
                'short_exempt_volume_shares': source.number(exempt_sum) if complete else None,
                'total_volume_shares': source.number(total_sum) if complete else None,
                'pooled_short_volume_pct': rounded(short_sum * 100 / total_sum) if complete and total_sum else None,
                'mean_daily_short_volume_pct': rounded(mean),
                'sample_sd_percentage_points': rounded(deviation),
                'latest_minus_mean_percentage_points': rounded(difference),
                'descriptive_z_score': rounded(difference / deviation) if difference is not None and deviation else None,
                'z_score_missing_reason': 'latest_ratio_unavailable' if latest_ratio is None else
                                          'incomplete_prior_ratios' if not valid_ratios else
                                          'zero_prior_variance' if deviation == 0 else None,
                'reported_facility_scope_consistent': len(scope_sets) == 1,
                'security_identity_continuity_verified': False,
                'probability_interpretation': None,
            }
        return result
