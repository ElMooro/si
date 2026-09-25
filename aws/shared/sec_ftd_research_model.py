"""Reconstruct complete SEC CNS archives as dated descriptive research."""
from collections import Counter, defaultdict
from datetime import date, datetime, timezone
import hashlib, json, re
import sec_ftd_source as source
import sec_ftd_measurements as measurements

CONTRACT = 'sec-ftd-original-research.v1'
PREFIX = 'data/sec-ftd-research/'
PRIVATE = 'audit-private/20260909-originals/sec-ftd-research/'
CURRENT = 'data/squeeze-fuel.json'
MAX = 8 * 1024 * 1024
FLAGS = measurements.FLAGS
encoded = measurements.encoded


def sha(body):
    return hashlib.sha256(body).hexdigest()


def digest(value):
    return sha(encoded(value))


def strict(body):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON field')
            result[key] = value
        return result
    def invalid(_):
        raise ValueError('Nonfinite JSON')
    return json.loads(body, object_pairs_hook=pairs, parse_constant=invalid)


def clock(value):
    if not isinstance(value, str):
        raise ValueError('Explicit acquisition time required')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('Explicit timezone required')
    return stamp.astimezone(timezone.utc)


def original(ref, read):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'}
            or not isinstance(ref.get('sha256'), str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256'])
            or ref['key'] != PRIVATE + ref['sha256'] + '.bin'
            or type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX):
        raise ValueError('Complete protected source identity required')
    body = read(ref['key'])
    if not isinstance(body, bytes) or len(body) != ref['bytes'] or sha(body) != ref['sha256']:
        raise ValueError('Original source bytes differ')
    return body


def capture(row, url, generated, read):
    if (not isinstance(row, dict) or row.get('url') != url or row.get('http_status') != 200
            or row.get('status') != 'response_retained' or row.get('body') is not None):
        raise ValueError('Exact successful public GET required')
    if not clock(row.get('requested_at')) <= clock(row.get('received_at')) <= clock(generated):
        raise ValueError('Source acquisition chronology differs')
    data = original(row['original'], read)
    headers = row.get('headers')
    if not isinstance(headers, dict):
        raise ValueError('Retained response headers required')
    length = headers.get('content-length')
    if length is not None and (not isinstance(length, str) or not length.isdigit() or int(length) != len(data)):
        raise ValueError('Source HTTP length differs')
    return data


def plan(inputs, read):
    if not isinstance(inputs, dict) or inputs.get('contract') != 'sec-ftd-original-inputs.v1':
        raise ValueError('Reviewed original input contract required')
    count = inputs.get('archive_count')
    if type(count) is not int or count not in (2, 12):
        raise ValueError('Explicit one-month or six-month archive selection required')
    index = capture(inputs['index'], source.INDEX, inputs['generated_at'], read)
    cutoff = date.fromisoformat(inputs['selection_cutoff'])
    if cutoff != clock(inputs['index']['received_at']).date():
        raise ValueError('Archive selection must use the retained index acquisition date')
    selected = source.advertised_archives(index, cutoff.isoformat(), count)
    if inputs.get('selected_archives') != selected or set(inputs.get('captures', {})) != set(selected):
        raise ValueError('Exactly the complete advertised archive selection required')
    source_manifest = strict(original(inputs['source_manifest'], read))
    if (source_manifest.get('contract') != 'sec-settlement-complete-sources.v1'
            or source_manifest.get('selected_archives') != selected or source_manifest.get('index') != inputs['index']
            or source_manifest.get('captures') != inputs['captures']
            or source_manifest.get('selection_cutoff') != inputs['selection_cutoff']):
        raise ValueError('Source manifest differs from the exact input inventory')
    return selected


def record_identity(value):
    body = encoded(value)
    if len(body) > MAX:
        raise ValueError('Complete record shard exceeds bound')
    return {'key': PREFIX + 'records/' + sha(body) + '.json', 'sha256': sha(body), 'bytes': len(body)}


def compile_output(inputs, read):
    selected = plan(inputs, read)
    histories, dates, sources = defaultdict(list), set(), []
    observed = set()
    for url in reversed(selected):
        cap = inputs['captures'][url]
        data = capture(cap, url, inputs['generated_at'], read)
        if clock(cap['requested_at']) < clock(inputs['index']['received_at']):
            raise ValueError('Archive request predates the retained index')
        member, text = source.text_member(data)
        rows = source.rows(text, url, clock(cap['received_at']).date().isoformat())
        _, controls = source.source_lines(text)
        index = len(sources)
        period = source.archive_period(url)
        calendar = Counter(row['settlement_date'] for row in rows)
        sources.append({'url': url, 'original': cap['original'], 'member': member,
                        'source_requested_at': cap['requested_at'], 'source_received_at': cap['received_at'],
                        'source_headers': cap['headers'], 'rows': len(rows),
                        'archive_label': {'year': period[0], 'month': period[1], 'file': period[2]},
                        'reported_dates': dict(sorted(calendar.items())), 'integrity_controls': controls,
                        'historical_availability_verified': False, 'fixed_half_month_day_boundary_assumed': False})
        for row in rows:
            key = (row['settlement_date'], row['cusip'])
            if key in observed:
                raise ValueError('Overlapping archive observations require explicit reconciliation; never overwrite')
            observed.add(key)
            dates.add(row['settlement_date'])
            histories[row['cusip']].append(measurements.compact_point(row, index))
        del rows, text, data
    original_rows = len(observed)
    del observed
    calendar = sorted(dates)
    shards, issues, symbols = {}, [], defaultdict(set)
    statuses, latest, missing_prices, missing_symbols = Counter(), 0, 0, 0
    for cusip in sorted(histories):
        record = measurements.history(cusip, histories.pop(cusip), calendar)
        identity = measurements.record_id(cusip)
        shards.setdefault(identity[:2], {'contract': 'sec-ftd-record-shard.v1', 'records': {}})['records'][identity] = record
        issues.append({'record_id': identity, 'cusip': cusip, 'reported_labels': record['reported_labels'],
                       'observation_count': len(record['observations']), 'latest_settlement_present': record['latest_settlement_present']})
        for label in record['reported_labels']:
            if label['symbol']:
                symbols[label['symbol']].add(identity)
        latest += record['latest_settlement_present']
        for point in record['observations']:
            statuses[point[-1]] += 1
            missing_prices += point[6] == '.'
            missing_symbols += not point[3]
    packet = {'contract': CONTRACT, 'engine': 'justhodl-squeeze-fuel', 'version': '2.0.0', 'ok': True,
              'generated_at': inputs['generated_at'], 'settlement_date': calendar[-1], 'dates': calendar,
              'selection_cutoff': inputs['selection_cutoff'], 'archive_count': len(selected),
              'quality': {'status': 'dated_observations', 'source_originals_reconstructed': True,
                          'selected_archives_complete': True, 'all_file_controls_reconciled': True,
                          'market_coverage_complete': False, 'historical_availability_verified': False,
                          'security_identity_continuity_verified': False, 'full_trading_calendar_coverage_verified': False},
              'scope': 'SEC-published aggregate outstanding CNS equity fails by reported CUSIP and settlement date.',
              'point_fields': list(measurements.POINT_FIELDS), 'sources': sources,
              'record_shards': {key: record_identity(value) for key, value in sorted(shards.items())},
              'issues': issues, 'symbols': [{'symbol': symbol, 'record_ids': sorted(ids)} for symbol, ids in sorted(symbols.items())],
              'counts': {'source_archives': len(sources), 'original_rows': original_rows, 'reported_dates': len(calendar),
                         'cusips': len(issues), 'symbols': len(symbols), 'latest_reported_cusips': latest,
                         'missing_previous_day_prices': missing_prices, 'missing_reported_symbols': missing_symbols,
                         'symbols_with_multiple_reported_cusips': sum(len(v) > 1 for v in symbols.values()),
                         'cusips_with_multiple_reported_labels': sum(len(v['reported_labels']) > 1 for v in issues),
                         'comparison_statuses': dict(sorted(statuses.items()))},
              'methodology': {
                  'balance': 'aggregate_net_outstanding_fail_balance_on_each_reported_settlement_date',
                  'adjacent_difference': 'difference between balances on adjacent dates reported across the selected archives, only with both records and unchanged reported labels; not a new-fail or settlement flow',
                  'percent_difference': '(current - previous) / previous * 100; null when previous is zero, missing, or the reported label changes; half-even to 12 decimals',
                  'missing_row': 'not reported; SEC omits zero balances, but eligibility and security continuity are unknown, so no zero is imputed',
                  'source_price': 'reported prior-day price; dot means unavailable; currency and exact price observation date are not explicit in the file',
                  'file_controls': 'record count and sum of reported quantities verify archive integrity; the quantity checksum is not an economic aggregate',
                  'identity': 'literal reported CUSIP with every original symbol and description retained; no verified security-master or FINRA join',
                  'source_clock': 'actual request and acquisition times; original release or revision availability is unverified'},
              'evidence_dependencies': {'family': 'sec_cns_outstanding_equity_fails', 'finra_short_positions_combined': False,
                                        'daily_short_sale_volume_combined': False, 'price_or_float_overlay_combined': False},
              'fail_age_days': None, 'short_sale_origin': None, 'forced_buy_in_probability': None,
              'borrow_availability': None, 'borrow_fee': None, 'short_interest': None, 'short_float_pct': None,
              'call': None, 'signal': None, 'score': None, **dict.fromkeys(FLAGS, False),
              'decision': {'verb': 'WAIT', 'abstain': True, 'eligible_votes': 0,
                           'reason': 'Outstanding settlement balances do not establish short-sale origin, forced buying, a directional forecast or a position size.'},
              'rows': [], 'board': [], 'top_picks': [], 'items': [], 'names': [], 'stocks': [], 'by_ticker': {},
              'tickets': [], 'squeeze_candidates': []}
    if len(encoded(packet)) > MAX:
        raise ValueError('Complete research index exceeds bound')
    return {'packet': packet, 'shards': shards}
