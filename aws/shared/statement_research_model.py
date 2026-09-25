"""Whole-population accounting research from immutable provider originals.

Never join by array position, pick a convenient duplicate, synthesize TTM, or
discard a row with unavailable identities. Every original coordinate belongs
to exactly one retained record. Calculations are descriptive, not an audit.
"""
from collections import Counter, defaultdict
from datetime import date, timedelta
import statement_research_source as source
import statement_measurements as measurements

CONTRACT = 'financial-statement-original-research.v1'
PREFIX = 'data/statement-research/'
PRIVATE = source.PRIVATE
MAX = source.MAX
encoded, sha = source.encoded, source.sha
FLAGS = {'calls_eligible': False, 'sizing_eligible': False, 'execution_eligible': False,
         'forecast_qualified': False, 'independent_investment_votes': 0}


def record_ref(body):
    if not 0 < len(body) <= MAX:
        raise ValueError('Complete issuer history exceeds artifact bound')
    return {'key': PREFIX + 'records/' + sha(body) + '.json', 'sha256': sha(body), 'bytes': len(body)}


def coordinate(capsule_ref, cap, number):
    return {'capture_id': capsule_ref['sha256'], 'source_id': cap['original']['sha256'], 'source_row': number,
        'endpoint': cap['spec']['endpoint'], 'request_period': cap['spec']['period']}


def row_problem(row, request, cap):
    entry = {'values': row, 'source_id': cap['original']['sha256'], 'source_row': 0}
    ident, problem = measurements.identity({request['endpoint']: entry})
    if problem:
        return None, problem
    if ident['symbol'] != request['symbol']:
        return ident, 'requested_and_reported_symbol_mismatch'
    if ident['period'] not in (('FY',) if request['period'] == 'annual' else ('Q1', 'Q2', 'Q3', 'Q4')):
        return ident, 'requested_and_reported_period_mismatch'
    # Accepted dates lack a provider timezone. Only a calendar date more than
    # one day after UTC acquisition is unambiguously in the future worldwide.
    latest = source.clock(cap['received_at']).date() + timedelta(days=1)
    if any(date.fromisoformat(ident[field][:10]) > latest for field in ('date', 'filingDate', 'acceptedDate')):
        return ident, 'reported_date_after_source_acquisition'
    return ident, None


def compile_output(manifest_ref, read):
    manifest, specs = source.plan(manifest_ref, read)
    groups = defaultdict(dict)
    sources, roots = {}, defaultdict(set)
    retained_rows, empty = 0, 0
    acquisition = []
    for request in specs:
        ref = manifest['captures'][request['url']]
        cap, rows = source.response(ref, request, manifest['completed_at'], read)
        sources[ref['sha256']] = {'request': request, 'original_sha256': cap['original']['sha256'],
            'original_bytes': cap['original']['bytes'], 'rows': len(rows),
            'requested_at': cap['requested_at'], 'received_at': cap['received_at'],
            'provider': 'FMP normalized statements', 'original_is_protected': True,
            'provider_reported_metadata_independently_verified': False}
        acquisition.extend((cap['requested_at'], cap['received_at']))
        retained_rows += len(rows)
        empty += not rows
        for i, row in enumerate(rows):
            coord = coordinate(ref, cap, i)
            ident, problem = row_problem(row, request, cap)
            # Invalid rows stay as distinct research records with an explicit
            # reason. Valid rows join only at the exact eight-field identity.
            key = {'request_period': request['period'], 'identity': ident}
            if problem:
                key.update(unqualified_source_row=coord, problem=problem)
            record_id = sha(encoded(key))
            group = groups[request['symbol']].setdefault(record_id, {**key, 'source_rows': [], 'entries': defaultdict(list)})
            group['source_rows'].append(coord)
            group['entries'][request['endpoint']].append({'values': row, 'source_id': coord['source_id'], 'source_row': i})
            if ident:
                roots['sec-cik:' + ident['cik'].zfill(10)].add(request['symbol'])
    shards, summaries = {}, []
    statuses, metric_statuses = Counter(), Counter()
    count_records = complete = with_duplicates = issue_rows = 0
    for symbol in manifest['reported_symbols']:
        records = []
        for record_id, group in sorted(groups[symbol].items()):
            entries = group.pop('entries')
            duplicates = sorted(endpoint for endpoint, rows in entries.items() if len(rows) != 1)
            # A repeated endpoint is unavailable, even if copies are identical.
            # Other unique statements may still support their own calculations.
            bundle = {endpoint: rows[0] for endpoint, rows in entries.items() if endpoint not in duplicates}
            result = None if group.get('problem') else measurements.compute(bundle)
            status = group.get('problem') or ('repeated_statement_endpoint' if duplicates else
                ('complete_aligned_statements' if result['complete_three_statement_bundle'] else 'partial_aligned_statements'))
            record = {'record_id': record_id, **group, 'duplicate_endpoints': duplicates, 'status': status,
                'measurements': result, 'source_rows': sorted(group['source_rows'], key=lambda v: (v['capture_id'], v['source_row'])),
                'historical_first_availability_verified': False, **FLAGS}
            records.append(record)
            statuses[status] += 1
            complete += status == 'complete_aligned_statements'
            with_duplicates += bool(duplicates)
            issue_rows += len(group['source_rows']) if group.get('problem') else 0
            if result:
                metric_statuses.update(v['status'] for v in result['metrics'].values())
        count_records += len(records)
        records.sort(key=lambda v: ((v.get('identity') or {}).get('date', ''), v['request_period'], v['record_id']))
        shard = {'contract': 'financial-statement-issuer-records.v1', 'requested_symbol': symbol,
            'records': records, 'source_row_count': sum(len(v['source_rows']) for v in records), **FLAGS}
        ref = record_ref(encoded(shard))
        shards[symbol] = shard
        reported_roots = sorted(root for root, labels in roots.items() if symbol in labels)
        summaries.append({'symbol': symbol, 'record': ref, 'records': len(records),
            'original_rows': shard['source_row_count'], 'reported_issuer_roots': reported_roots,
            'latest_reported_period_end': max((v['identity']['date'] for v in records if v.get('identity')), default=None),
            'empty_responses': sum(v['rows'] == 0 for v in sources.values() if v['request']['symbol'] == symbol),
            'status': 'no_provider_statements' if not records else 'descriptive_history', **FLAGS})
    if sum(v['original_rows'] for v in summaries) != retained_rows:
        raise ValueError('Original source row conservation failed')
    counts = manifest.get('counts', {})
    if counts.get('provider_rows') != retained_rows or counts.get('unavailable_empty_responses') != empty:
        raise ValueError('Reconstructed source population differs from completed acquisition')
    packet = {'contract': CONTRACT, 'generated_at': manifest['completed_at'], 'source_manifest_sha256': manifest_ref['sha256'],
        'source_acquisition_started_at': min(acquisition, key=source.clock),
        'source_acquisition_completed_at': max(acquisition, key=source.clock),
        'snapshot_atomic': False, 'index_membership_verified': False, 'calls_eligible': False,
        'reported_names': len(summaries), 'provider_responses': len(specs), 'provider_rows': retained_rows,
        'empty_responses': empty, 'records': count_records, 'complete_aligned_records': complete,
        'records_with_repeated_endpoints': with_duplicates, 'rows_with_identity_problems': issue_rows,
        'record_statuses': dict(sorted(statuses.items())), 'metric_statuses': dict(sorted(metric_statuses.items())),
        'reported_issuer_roots': {root: sorted(labels) for root, labels in sorted(roots.items())},
        'multiple_labels_per_issuer': {root: sorted(labels) for root, labels in sorted(roots.items()) if len(labels) > 1},
        'issuer_roots_independently_verified': False, 'sources': sources, 'issuers': summaries,
        'quality': {'status': 'source_reconstructed', 'original_source_bytes_replayed': True,
            'original_sec_filings_replayed': False, 'historical_first_availability_verified': False,
            'statement_duration_verified': False, 'provider_scale_independently_verified': False,
            'accounting_audit': False, 'provider_fields_may_be_derived': True},
        'methodology': {'identity_fields': list(measurements.IDENTITY), 'join': 'exact_identity_and_filing_vintage',
            'duplicate_policy': 'retain_every_row_withhold_repeated_endpoint',
            'missing_policy': 'explicit_unavailable_never_zero', 'annualization': 'none',
            'rounding': 'half_even_12_decimal_places', 'sorting': 'reported_period_end_then_stable_identity',
            'provider_methodology': 'https://site.financialmodelingprep.com/developer/docs/parsing-statements',
            'reconciliation_interpretation': 'Internal consistency of normalized vendor fields; not independent filing confirmation.',
            'quarterly_duration': 'Provider period label only; no synthetic trailing twelve months.',
            'signal_interpretation': 'Descriptive measurements only; no fraud finding, grade, long/short call or sizing.'},
        'call': None, 'score': None, 'm_score': None, 'grade': None, **FLAGS}
    return {'packet': packet, 'shards': shards}
