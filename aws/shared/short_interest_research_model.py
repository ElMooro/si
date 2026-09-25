"""Source-bound FINRA settlement research; no covering or squeeze inference.

Acquisition now does not prove when a historical revision became available.
Identical issue labels do not establish security-master continuity.
"""
from collections import defaultdict, Counter
from datetime import datetime, timezone, date
import hashlib, json, re
import offexchange_measurements as exact
import short_interest_measurements as measurements

CONTRACT = 'short-interest-original-research.v1'
PREFIX = 'data/short-interest-research/'
PRIVATE = 'audit-private/20260909-originals/short-interest-research/'
CURRENT = 'data/short-interest.json'
MAX = 8 * 1024 * 1024
URLS = {kind: 'https://api.finra.org/' + kind + '/group/otcMarket/name/consolidatedShortInterest'
        for kind in ('metadata', 'partitions', 'data')}
FLAGS = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')
PERMISSIONS = dict.fromkeys(FLAGS, False)


def sha(raw):
    return hashlib.sha256(raw).hexdigest()


encoded = measurements.encoded


def digest(value):
    return sha(encoded(value))


def strict(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise ValueError('Duplicate JSON field')
            result[key] = value
        return result
    def invalid(_):
        raise ValueError('Nonfinite JSON')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def clock(value):
    if not isinstance(value, str):
        raise ValueError('Explicit acquisition time required')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None:
        raise ValueError('Timezone required')
    return stamp.astimezone(timezone.utc)


def original(ref, read):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'}
            or not isinstance(ref.get('sha256'), str) or not re.fullmatch('[a-f0-9]{64}', ref['sha256'])
            or ref['key'] != PRIVATE + ref['sha256'] + '.bin'
            or type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX):
        raise ValueError('Complete protected original required')
    body = read(ref['key'])
    if not isinstance(body, bytes) or len(body) != ref['bytes'] or sha(body) != ref['sha256']:
        raise ValueError('Original source identity differs')
    return body


def capture(row, url, body, generated, read):
    if (not isinstance(row, dict) or row.get('url') != url or row.get('body') != body
            or row.get('http_status') != 200 or row.get('status') != 'response_retained'):
        raise ValueError('Exact successful source request required')
    if not clock(row.get('requested_at')) <= clock(row.get('received_at')) <= clock(generated):
        raise ValueError('Acquisition chronology differs')
    data = original(row['original'], read)
    headers = row.get('headers')
    if not isinstance(headers, dict):
        raise ValueError('Captured response headers required')
    length = headers.get('content-length')
    if length is not None and (not isinstance(length, str) or not length.isdigit() or int(length) != len(data)):
        raise ValueError('Source HTTP length differs')
    return data


def plan(inputs, read):
    if not isinstance(inputs, dict) or inputs.get('contract') != 'short-interest-original-inputs.v1':
        raise ValueError('Reviewed original input contract required')
    generated = inputs['generated_at']
    cutoff = date.fromisoformat(exact.day(inputs['selection_cutoff']))
    if cutoff > clock(generated).date():
        raise ValueError('Selection cutoff is after generation')
    captured = inputs['captures']
    metadata = strict(capture(captured['metadata'], URLS['metadata'], None, generated, read))
    if str(metadata.get('datasetName', '')).lower() != measurements.DATASET or str(metadata.get('datasetGroup', '')).lower() != 'otcmarket' or metadata.get('partitionFields') != ['settlementDate']:
        raise ValueError('Reviewed provider schema required')
    fields = metadata.get('fields')
    if not isinstance(fields, list) or len(fields) != len(measurements.FIELDS) or {v['name'] for v in fields} != set(measurements.FIELDS):
        raise ValueError('Exact reported field set required')
    for field in fields:
        expected_type = 'Date' if field['name'] == 'settlementDate' else 'String' if field['name'] in (*measurements.GRAIN, 'stockSplitFlag', 'revisionFlag') else 'Number'
        if field.get('type') != expected_type:
            raise ValueError('Provider field type changed')
    partition_bytes = capture(captured['partitions'], URLS['partitions'], None, generated, read)
    selected = measurements.partitions(partition_bytes, cutoff)
    if inputs.get('settlement_plan') != selected:
        raise ValueError('Latest four published settlement selections differ')
    return selected


def record_id(identity):
    return digest(list(identity))


def record_identity(value):
    body = encoded(value)
    if len(body) > MAX:
        raise ValueError('Whole record shard exceeds bound')
    return {'key': PREFIX + 'records/' + sha(body) + '.json', 'sha256': sha(body), 'bytes': len(body)}


def compile_output(inputs, read):
    selected = plan(inputs, read)
    generated, captures = inputs['generated_at'], inputs['captures']
    histories, populations, sources = defaultdict(list), [], []
    expected = {'metadata', 'partitions'}
    for spec in reversed(selected):
        stamp = spec['settlement_date']
        scans = []
        previous_response = clock(captures['partitions']['received_at'])
        for pass_number in (1, 2):
            offset, total, population, pages = 0, None, [], []
            while True:
                if len(pages) >= 40:
                    raise ValueError('Whole-population page bound exceeded')
                label = f'settlement:{stamp}:pass:{pass_number}:offset:{offset}'
                expected.add(label)
                cap = captures[label]
                body = measurements.request(stamp, offset)
                data = capture(cap, URLS['data'], body, generated, read)
                if clock(cap['requested_at']) < previous_response or stamp > clock(cap['received_at']).date().isoformat():
                    raise ValueError('Source observation or selection chronology differs')
                previous_response = clock(cap['received_at'])
                summary = exact.page(data, cap['headers'], offset, measurements.LIMIT)
                if total is not None and total != summary['reported_total']:
                    raise ValueError('Population total changed while paging')
                total = summary['reported_total']
                records = measurements.rows(data, stamp)
                source_index = len(sources)
                sources.append({'settlement_date': stamp, 'pass': pass_number, 'offset': offset, 'rows': len(records),
                    'reported_total': total, 'original': cap['original'], 'url': cap['url'], 'request_body': body,
                    'source_requested_at': cap['requested_at'], 'source_received_at': cap['received_at'], 'source_headers': cap['headers'],
                    'historical_availability_verified': False})
                if pass_number == 1:
                    for index, row in enumerate(records):
                        histories[measurements.record_key(row)].append(measurements.compact_point(row, spec['previous_settlement_date'], source_index, index))
                population.extend(records)
                pages.append(source_index)
                offset = summary['next_offset']
                if summary['reported_end_reached']:
                    break
            if not total or len(population) != total:
                raise ValueError('Nonempty complete reported settlement required')
            scans.append({'rows': total, 'population_sha256': measurements.fingerprint(population), 'source_indexes': pages})
            del population, records, data
        if (scans[0]['rows'], scans[0]['population_sha256']) != (scans[1]['rows'], scans[1]['population_sha256']):
            raise ValueError('Two complete provider scans disagree')
        populations.append({**spec, 'reported_rows': scans[0]['rows'], 'population_sha256': scans[0]['population_sha256'],
                            'scans': scans, 'two_complete_scans_equal': True, 'snapshot_atomic': False})
    if set(captures) != expected:
        raise ValueError('Exactly the complete selected captures required')
    dates = [v['settlement_date'] for v in populations]
    shards, symbol_index, latest, zero_adv = {}, defaultdict(list), 0, 0
    dtc_counts, pct_counts, revisions, prior_differences, delta_discrepancies = Counter(), Counter(), 0, 0, 0
    for identity in sorted(histories):
        record = measurements.history(identity, histories[identity], dates)
        identity_hash = record_id(identity)
        shards.setdefault(identity_hash[:2], {'contract': 'short-interest-record-shard.v1', 'records': {}})['records'][identity_hash] = record
        symbol_index[identity[0]].append({'record_id': identity_hash, 'reported_issue_name': identity[1], 'exchange_code': identity[2], 'market_class': identity[3]})
        latest += record['latest_settlement_present']
        for point in record['observations']:
            fields = dict(zip(measurements.POINT_FIELDS, point))
            dtc_counts[fields['days_to_cover_status']] += 1
            pct_counts[fields['change_pct_reconciliation']] += 1
            delta_discrepancies += fields['change_shares_reconciled'] is False
            zero_adv += fields['averageDailyVolumeQuantity'] == '0'
            revisions += fields['revisionFlag'] == 'R'
            prior_differences += fields['matches_reported_previous_quantity'] is False
    refs = {key: record_identity(value) for key, value in sorted(shards.items())}
    packet = {'contract': CONTRACT, 'engine': 'justhodl-short-interest', 'version': '2.0.0', 'generated_at': generated,
        'settlement_date': dates[-1], 'dates': dates, 'selection_cutoff': inputs['selection_cutoff'],
        'quality': {'status': 'dated_observations', 'source_originals_reconstructed': True, 'selected_api_populations_complete': True,
                    'snapshot_atomic': False, 'market_coverage_complete': False, 'historical_availability_verified': False,
                    'security_identity_continuity_verified': False},
        'scope': 'FINRA consolidated reported short positions across returned market classes; API population, not every security or all economic short exposure.',
        'point_fields': list(measurements.POINT_FIELDS), 'sources': sources, 'settlement_populations': populations,
        'record_shards': refs, 'symbols': [{'symbol': name, 'issues': issues} for name, issues in sorted(symbol_index.items())],
        'counts': {'source_pages': len(sources), 'source_rows_each_scan': sum(p['reported_rows'] for p in populations),
                   'issues': len(histories), 'symbols': len(symbol_index), 'latest_issues': latest,
                   'zero_reported_adv_observations': zero_adv, 'reported_revision_observations': revisions,
                   'reported_prior_quantity_differences': prior_differences, 'dtc_statuses': dict(sorted(dtc_counts.items())),
                   'change_pct_statuses': dict(sorted(pct_counts.items())), 'reported_change_share_discrepancies': delta_discrepancies},
        'methodology': {'source': 'FINRA otcMarket/consolidatedShortInterest', 'cadence': 'twice_monthly_settlement_positions',
            'prior_quantity': 'provider_previous_unadjusted_position; separately reconciled with retained prior settlement when the complete reported issue key matches',
            'percent_change': '(current - reported_previous) / reported_previous * 100; null for previous zero, provider convention retained separately',
            'average_volume_window': 'previous advertised settlement plus one calendar day through current settlement; provider trading-day average with provider split adjustments',
            'days_to_cover': 'provider display and independently reconstructed current / reported ADV are separate; zero ADV yields null reconstructed ratio',
            'reported_999_99': 'preserved provider convention; never interpreted as an exact ratio or proof of difficulty covering',
            'rounding': '128-digit Decimal, half-even to 12 decimal places; exact rational for position/ADV',
            'split_and_revision_flags': 'retained; no security-master or split adjustment invented',
            'historical_availability': 'acquisition clocks only; no original release or revision timestamp is available in these source rows'},
        'evidence_dependencies': {'family': 'finra_reported_short_positions', 'daily_short_volume_combined': False},
        'call': None, 'signal': None, 'score': None, 'tickets': [], **PERMISSIONS,
        'decision': {'verb': 'WAIT', 'abstain': True, 'eligible_votes': 0,
                     'reason': 'Reported positions and descriptive ratios have no qualified covering, squeeze, directional or sizing authority.'},
        'by_ticker': {}, 'items': [], 'top_picks': [], 'board': [], 'squeeze_candidates': [],
        'short_float_pct': None, 'daily_short_volume_pct': None, 'price_change_pct': None}
    if len(encoded(packet)) > MAX:
        raise ValueError('Whole research index exceeds bound')
    return {'packet': packet, 'shards': shards}
