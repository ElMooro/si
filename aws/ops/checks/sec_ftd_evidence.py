"""Independent all-row and integer-rational verification of SEC CNS research.

Reads retained ZIP bytes directly. Does not call the production source parser,
measurement functions, or compiler to obtain expected rows or arithmetic.
"""
from collections import Counter, defaultdict
from fractions import Fraction
from io import BytesIO
import hashlib, json, re, zipfile

FIELDS = ('source_index', 'source_line', 'settlement_date', 'reported_symbol', 'reported_description',
          'fail_balance_shares', 'reported_previous_day_price', 'previous_reported_date',
          'previous_record_present', 'previous_fail_balance_shares',
          'adjacent_balance_change_shares', 'adjacent_balance_change_pct', 'comparison_status')
FLAGS = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')
HEADER = 'SETTLEMENT DATE|CUSIP|SYMBOL|QUANTITY (FAILS)|DESCRIPTION|PRICE'


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def percentage(value):
    """Exact integer half-even rounding, independent of Decimal production code."""
    scaled = abs(value) * 10**12
    integer, remainder = divmod(scaled.numerator, scaled.denominator)
    if remainder * 2 > scaled.denominator or remainder * 2 == scaled.denominator and integer % 2:
        integer += 1
    whole, fraction = divmod(integer, 10**12)
    return ('-' if value < 0 and integer else '') + str(whole) + '.' + str(fraction).zfill(12)


def qualify(inputs, compiled, original):
    packet = compiled['packet']
    assert packet['contract'] == 'sec-ftd-original-research.v1'
    assert packet['generated_at'] == inputs['generated_at']
    assert tuple(packet['point_fields']) == FIELDS
    assert all(packet.get(k) is False for k in FLAGS)
    assert all(packet[k] is None for k in ('call', 'score', 'signal', 'fail_age_days', 'short_sale_origin',
                                          'forced_buy_in_probability', 'borrow_availability', 'borrow_fee',
                                          'short_interest', 'short_float_pct'))
    assert packet['decision']['eligible_votes'] == 0 and packet['decision']['verb'] == 'WAIT'
    assert packet['decision']['abstain'] is True and packet['by_ticker'] == {}
    assert all(packet[k] == [] for k in ('rows', 'board', 'top_picks', 'items', 'names', 'stocks', 'tickets', 'squeeze_candidates'))
    assert packet['quality'] == {'status': 'dated_observations', 'source_originals_reconstructed': True,
        'selected_archives_complete': True, 'all_file_controls_reconciled': True, 'market_coverage_complete': False,
        'historical_availability_verified': False, 'security_identity_continuity_verified': False,
        'full_trading_calendar_coverage_verified': False}
    assert packet['evidence_dependencies'] == {'family': 'sec_cns_outstanding_equity_fails',
        'finra_short_positions_combined': False, 'daily_short_sale_volume_combined': False, 'price_or_float_overlay_combined': False}
    records, by_cusip = {}, {}
    assert set(compiled['shards']) == set(packet['record_shards'])
    for prefix, shard in compiled['shards'].items():
        body = encoded(shard)
        digest = hashlib.sha256(body).hexdigest()
        assert packet['record_shards'][prefix] == {'key': 'data/sec-ftd-research/records/' + digest + '.json',
                                                  'sha256': digest, 'bytes': len(body)}
        assert shard['contract'] == 'sec-ftd-record-shard.v1'
        for identity, record in shard['records'].items():
            cusip = record['reported_cusip']
            assert identity == hashlib.sha256(encoded(['SEC_CNS_REPORTED_CUSIP', cusip])).hexdigest()
            assert identity.startswith(prefix) and len(prefix) == 2
            assert identity not in records and cusip not in by_cusip
            assert all(record.get(k) is False for k in FLAGS)
            assert all(record[k] is None for k in ('call', 'signal', 'score'))
            assert record['security_identity_continuity_verified'] is False
            assert record['missing_records_imputed_zero'] is False and record['balances_summed_across_dates'] is False
            points = record['observations']
            assert points and all(len(point) == len(FIELDS) for point in points)
            dates = [point[2] for point in points]
            assert dates == sorted(set(dates))
            records[identity] = record
            by_cusip[cusip] = {point[2]: point for point in points}
    sources = packet['sources']
    assert [source['url'] for source in sources] == list(reversed(inputs['selected_archives']))
    total, missing_prices, missing_symbols, controls_checked = 0, 0, 0, 0
    reported_dates = set()
    for index, source in enumerate(sources):
        capture = inputs['captures'][source['url']]
        assert source['original'] == capture['original']
        assert source['source_requested_at'] == capture['requested_at']
        assert source['source_received_at'] == capture['received_at']
        assert source['source_headers'] == capture['headers']
        assert source['historical_availability_verified'] is False and source['fixed_half_month_day_boundary_assumed'] is False
        raw = original(source['original'])
        assert len(raw) == source['original']['bytes'] and hashlib.sha256(raw).hexdigest() == source['original']['sha256']
        with zipfile.ZipFile(BytesIO(raw)) as archive:
            assert len(archive.infolist()) == 1
            member = archive.infolist()[0]
            body = archive.read(member)
        assert source['member'] == {'name': member.filename, 'bytes': len(body), 'sha256': hashlib.sha256(body).hexdigest()}
        lines = body.decode('utf-8-sig').splitlines()
        while lines and not lines[-1].strip():
            lines.pop()
        assert lines[0].strip() == HEADER
        count = re.fullmatch(r'Trailer record count ([0-9]+)', lines[-2].strip())
        checksum = re.fullmatch(r'Trailer total quantity of shares ([0-9]+)', lines[-1].strip())
        assert count and checksum
        expected_controls = {'reported_record_count': int(count[1]), 'reported_quantity_sum': checksum[1],
            'record_count_source_line': len(lines) - 1, 'quantity_sum_source_line': len(lines),
            'quantity_sum_is_file_integrity_control_not_economic_flow': True}
        assert source['integrity_controls'] == expected_controls
        period = re.search(r'cnsfails([0-9]{4})([0-9]{2})([ab])\.zip$', source['url'])
        assert period and source['archive_label'] == {'year': int(period[1]), 'month': int(period[2]), 'file': period[3]}
        seen, dates, quantity_sum, n = set(), Counter(), 0, 0
        for line_number, line in enumerate(lines[1:-2], 2):
            if not line.strip():
                continue
            stamp, cusip, symbol, quantity, description, price = line.split('|')
            day = stamp[:4] + '-' + stamp[4:6] + '-' + stamp[6:]
            assert (day, cusip) not in seen
            seen.add((day, cusip))
            actual = by_cusip[cusip][day]
            assert actual[:7] == [index, line_number, day, symbol, description, quantity, price], 'Original row differs'
            dates[day] += 1
            quantity_sum += int(quantity)
            n += 1
            missing_prices += price == '.'
            missing_symbols += not symbol
        assert n == int(count[1]) == source['rows']
        assert quantity_sum == int(checksum[1])
        assert dict(sorted(dates.items())) == source['reported_dates']
        controls_checked += 2
        total += n
        reported_dates.update(dates)
    dates = sorted(reported_dates)
    assert packet['dates'] == dates and packet['settlement_date'] == dates[-1]
    assert total == sum(len(record['observations']) for record in records.values())
    preceding = {stamp: dates[i - 1] if i else None for i, stamp in enumerate(dates)}
    statuses, symbol_map, issues, arithmetic = Counter(), defaultdict(set), [], 0
    missing_prior, label_changes, zero_denominators = 0, 0, 0
    for identity, record in sorted(records.items(), key=lambda item: item[1]['reported_cusip']):
        cusip = record['reported_cusip']
        history = by_cusip[cusip]
        assert record['dates'] == dates
        assert record['missing_reported_dates'] == [day for day in dates if day not in history]
        assert record['latest_settlement_present'] is (dates[-1] in history)
        labels = sorted({(point[3], point[4]) for point in history.values()})
        assert record['reported_labels'] == [{'symbol': symbol, 'description': description} for symbol, description in labels]
        for symbol, _ in labels:
            if symbol:
                symbol_map[symbol].add(identity)
        issues.append({'record_id': identity, 'cusip': cusip, 'reported_labels': record['reported_labels'],
                       'observation_count': len(history), 'latest_settlement_present': dates[-1] in history})
        for day, point in history.items():
            prior_date = preceding[day]
            prior = history.get(prior_date)
            delta, percent = None, None
            if prior_date is None:
                status = 'no_preceding_date_in_selected_archives'
            elif prior is None:
                status = 'prior_cusip_record_not_reported'
                missing_prior += 1
            elif point[3:5] != prior[3:5]:
                status = 'reported_label_changed_comparison_unqualified'
                label_changes += 1
            else:
                current, previous = int(point[5]), int(prior[5])
                delta = str(current - previous)
                percent = percentage(Fraction((current - previous) * 100, previous)) if previous else None
                status = 'adjacent_reported_balances_only' if previous else 'reported_previous_balance_zero'
                arithmetic += 1
                zero_denominators += not previous
            assert point[7:] == [prior_date, prior is not None, prior[5] if prior is not None else None, delta, percent, status], 'Independent adjacent-balance comparison differs'
            statuses[status] += 1
    assert packet['issues'] == issues
    assert packet['symbols'] == [{'symbol': symbol, 'record_ids': sorted(ids)} for symbol, ids in sorted(symbol_map.items())]
    assert packet['archive_count'] == len(sources) and packet['selection_cutoff'] == inputs['selection_cutoff']
    assert packet['counts'] == {'source_archives': len(sources), 'original_rows': total, 'reported_dates': len(dates),
        'cusips': len(records), 'symbols': len(symbol_map), 'latest_reported_cusips': sum(dates[-1] in v for v in by_cusip.values()),
        'missing_previous_day_prices': missing_prices, 'missing_reported_symbols': missing_symbols,
        'symbols_with_multiple_reported_cusips': sum(len(v) > 1 for v in symbol_map.values()),
        'cusips_with_multiple_reported_labels': sum(len(v['reported_labels']) > 1 for v in records.values()),
        'comparison_statuses': dict(sorted(statuses.items()))}
    return {'original_rows_checked': total, 'unique_cusips_checked': len(records), 'file_controls_checked': controls_checked,
            'independent_integer_fraction_comparisons': arithmetic, 'missing_adjacent_cusip_rows': missing_prior,
            'label_change_comparisons_withheld': label_changes, 'zero_denominators': zero_denominators,
            'missing_reported_symbols_retained': missing_symbols, 'provider_originals_verified': True,
            'population_arithmetic_qualified': True, 'security_continuity_verified': False,
            'historical_availability_verified': False, 'forecast_qualified': False, 'sizing_qualified': False}
