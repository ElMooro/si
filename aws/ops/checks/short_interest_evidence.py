"""Independent Fraction checks against every retained settlement source row.

No provider, account or publishing operations. Does not call the production
measurement or compilation functions to obtain expected arithmetic.
"""
from collections import Counter
from decimal import Decimal
from fractions import Fraction
import hashlib, json

GRAIN = ('symbolCode', 'issueName', 'issuerServicesGroupExchangeCode', 'marketClassCode')
NUMERIC = ('accountingYearMonthNumber', 'currentShortPositionQuantity', 'previousShortPositionQuantity',
           'averageDailyVolumeQuantity', 'daysToCoverQuantity', 'changePreviousNumber', 'changePercent')
TOLERANCE = Fraction(1, 2 * 10**12)


def rounded_hundredths(value):
    negative = value < 0
    scaled = abs(value) * 100
    result = (scaled.numerator * 2 + scaled.denominator) // (scaled.denominator * 2)
    return Fraction(-result if negative else result, 100)


def close(actual, expected):
    if expected is None:
        assert actual is None, 'Missing denominator became a number'
    else:
        assert isinstance(actual, str) and abs(Fraction(actual) - expected) <= TOLERANCE, 'Independent ratio differs'


def qualify(inputs, compiled, original):
    packet = compiled['packet']
    assert packet['call'] is None and packet['score'] is None and packet['signal'] is None
    assert all(packet.get(k) is False for k in ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified'))
    assert packet['decision']['eligible_votes'] == 0 and packet['by_ticker'] == {}
    fields = packet['point_fields']
    records = {}
    observed = set()
    for shard in compiled['shards'].values():
        for identity, record in shard['records'].items():
            assert identity not in records
            records[identity] = record
    checks, arithmetic, missing_prior, differing_prior = 0, 0, 0, 0
    populations = Counter()
    for source_index, source in enumerate(packet['sources']):
        raw = original(source['original'])
        rows = json.loads(raw, parse_float=Decimal)
        assert len(rows) == source['rows']
        assert source['reported_total'] == int(source['source_headers']['record-total'])
        assert source['offset'] == int(source['source_headers']['record-offset'])
        key = (source['settlement_date'], source['pass'])
        assert source['offset'] == populations[key]
        populations[key] += len(rows)
        for index, row in enumerate(rows):
            identity = [row[k] for k in GRAIN]
            identity_hash = hashlib.sha256(json.dumps(identity, separators=(',', ':'), ensure_ascii=False).encode()).hexdigest()
            record = records[identity_hash]
            matches = [dict(zip(fields, point)) for point in record['observations'] if point[fields.index('settlementDate')] == row['settlementDate']]
            assert len(matches) == 1
            value = matches[0]
            for name in row:
                expected = format(Decimal(row[name]), 'f') if name in NUMERIC else row[name]
                assert value[name] == expected, 'Source row value differs'
            if source['pass'] == 1:
                assert value['source_index'] == source_index and value['source_row'] == index
                identity_key = (identity_hash, row['settlementDate'])
                assert identity_key not in observed
                observed.add(identity_key)
            checks += 1
            current, previous, adv, dtc, delta, pct = (Fraction(row[k]) for k in ('currentShortPositionQuantity', 'previousShortPositionQuantity',
                'averageDailyVolumeQuantity', 'daysToCoverQuantity', 'changePreviousNumber', 'changePercent'))
            assert Fraction(value['computed_change_shares']) == current - previous
            assert value['change_shares_reconciled'] == (delta == current - previous)
            close(value['computed_change_pct'], (current - previous) * 100 / previous if previous else None)
            close(value['reconstructed_position_to_reported_adv_days'], current / adv if adv else None)
            assert (Fraction(value['position_to_adv_exact_fraction']) if adv else value['position_to_adv_exact_fraction']) == (current / adv if adv else None)
            expected_dtc = ('unavailable_zero_reported_adv' if not adv else
                'matches_reconstructed_rounded_ratio' if dtc == rounded_hundredths(current / adv) else
                'provider_display_floor_one' if current <= adv and dtc == 1 else
                'provider_999_99_convention_unconfirmed' if dtc == Fraction(99999, 100) else 'provider_differs_from_reconstructed_ratio')
            assert value['days_to_cover_status'] == expected_dtc
            expected_pct = ('undefined_previous_zero' if not previous else 'matches_rounded_formula' if pct == rounded_hundredths((current - previous) * 100 / previous) else 'provider_differs_from_rounded_formula')
            assert value['change_pct_reconciliation'] == expected_pct
            arithmetic += 1
    for record in records.values():
        by_date = {point[fields.index('settlementDate')]: dict(zip(fields, point)) for point in record['observations']}
        assert record['missing_settlements'] == [stamp for stamp in packet['dates'] if stamp not in by_date]
        assert record['latest_settlement_present'] == (packet['dates'][-1] in by_date)
        assert record['security_identity_continuity_verified'] is False
        for stamp, value in by_date.items():
            idx = packet['dates'].index(stamp)
            prior = by_date.get(packet['dates'][idx - 1]) if idx else None
            if prior is None:
                missing_prior += 1
                assert value['matched_prior_record_shares'] is None and value['matches_reported_previous_quantity'] is None
            else:
                assert value['matched_prior_record_shares'] == prior['currentShortPositionQuantity']
                expected = Fraction(prior['currentShortPositionQuantity']) == Fraction(value['previousShortPositionQuantity'])
                assert value['matches_reported_previous_quantity'] is expected
                differing_prior += not expected
    assert len(observed) == sum(len(r['observations']) for r in records.values())
    for item in packet['settlement_populations']:
        assert populations[(item['settlement_date'], 1)] == populations[(item['settlement_date'], 2)] == item['reported_rows']
    assert len(observed) == packet['counts']['source_rows_each_scan']
    return {'original_rows_checked_both_scans': checks, 'unique_observations_checked': len(observed),
            'independent_fraction_arithmetic_checks': arithmetic, 'reported_issues_checked': len(records),
            'missing_prior_issue_rows': missing_prior, 'prior_quantity_discrepancies': differing_prior,
            'provider_originals_verified': True, 'population_arithmetic_qualified': True,
            'atomic_provider_snapshot_verified': False, 'security_continuity_verified': False,
            'historical_availability_verified': False, 'forecast_qualified': False, 'sizing_qualified': False}
