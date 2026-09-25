"""Conserve every source row; separate cash flows, EPS shares and snapshots."""
from collections import Counter, defaultdict
from datetime import date
from fractions import Fraction
import capital_structure_measurements as measures
import capital_structure_source as source
import statement_research_identity as sec_identity

CONTRACT = 'capital-structure-original-research.v1'
PREFIX, PRIVATE, MAX = source.PREFIX, source.PRIVATE, source.MAX
FLAGS = measures.FLAGS
encoded, sha = source.encoded, source.sha
FACTS = {
    'income-statement': ('weightedAverageShsOut', 'weightedAverageShsOutDil', 'netIncome', 'eps', 'epsDiluted', 'revenue'),
    'cash-flow-statement': ('commonStockIssuance', 'commonStockIssued', 'commonStockRepurchased',
        'netCommonStockIssuance', 'netStockIssuance', 'commonDividendsPaid', 'netDividendsPaid',
        'stockBasedCompensation', 'operatingCashFlow', 'freeCashFlow'),
    'quote': ('price', 'marketCap', 'currency', 'exchange', 'timestamp', 'sharesOutstanding'),
    'shares-float': ('date', 'floatShares', 'outstandingShares', 'freeFloat'),
    'splits': ('date', 'numerator', 'denominator', 'splitType'),
}


def record_ref(body):
    if not 0 < len(body) <= MAX:
        raise ValueError('Complete capital-structure history exceeds artifact bound')
    return {'key': PREFIX+'records/'+sha(body)+'.json', 'sha256': sha(body), 'bytes': len(body)}


def coordinate(ref, cap, number):
    return {'capture_id': ref['sha256'], 'source_id': cap['original']['sha256'], 'source_row': number,
        'endpoint': cap['spec']['endpoint'], 'request_period': cap['spec']['period']}


def split_fact(entry, symbol):
    row = entry['values']; n, d = (measures.number(row.get(k)) for k in ('numerator', 'denominator'))
    status = ('reported_symbol_differs_from_request' if row.get('symbol') != symbol else
              'reported_split_date_invalid' if not measures.iso_day(row.get('date')) else
              'reported_split_ratio_unavailable' if n is None or d is None or n <= 0 or d <= 0 else
              'descriptive_reported_split_ratio')
    return {'status': status, 'reported_event_date': measures.typed(row.get('date')),
        'reported_numerator_over_denominator': measures.exact_result(Fraction(n)/Fraction(d)) if status == 'descriptive_reported_split_ratio' else None,
        'provider_history_completeness_verified': False, 'statement_split_basis_verified': False,
        'applied_to_statement_shares': False, **FLAGS}


def compile_output(manifest_ref, identity_ref, read):
    identity_cap, index_raw, mapping = sec_identity.capture(identity_ref, read)
    manifest, specs, population, completed_at = source.plan(manifest_ref, read)
    source.prefetch(manifest, read)
    groups, snapshots, sources = defaultdict(dict), defaultdict(list), {}
    counts = dict.fromkeys(('complete_sources', 'provider_requests', 'reused_sources', 'provider_rows', 'provider_bytes', 'empty_arrays'), 0)
    acquisition, roots = [], defaultdict(set)
    for request in specs:
        ref = manifest['captures'][request['url']]
        cap, rows = source.response(ref, request, completed_at, read)
        symbol, endpoint = request['symbol'], request['endpoint']
        sources[ref['sha256']] = {'request': request, 'original_sha256': cap['original']['sha256'],
            'original_bytes': cap['original']['bytes'], 'rows': len(rows),
            'requested_at': cap['requested_at'], 'received_at': cap['received_at'],
            'original_is_protected': True, 'reused_original': cap.get('reused_original') is True,
            'provider_reported_metadata_independently_verified': False}
        acquisition.extend((cap['requested_at'], cap['received_at']))
        counts['complete_sources'] += 1; counts['provider_rows'] += len(rows)
        counts['provider_bytes'] += cap['original']['bytes']; counts['empty_arrays'] += not rows
        counts['reused_sources' if cap.get('reused_original') else 'provider_requests'] += 1
        for number, row in enumerate(rows):
            coord = coordinate(ref, cap, number)
            entry = {'values': row, 'source_id': coord['source_id'], 'source_row': number}
            evidence = {'coordinate': coord, 'reported_identity': {k: measures.typed(row.get(k)) for k in measures.IDENTITY},
                'identity_fields_present': [k for k in measures.IDENTITY if k in row],
                'facts': [measures.reported_input(entry, endpoint, field) for field in FACTS[endpoint]],
                'source_received_at': cap['received_at']}
            if endpoint not in source.STATEMENTS:
                status = 'reported_provider_snapshot' if row.get('symbol') == symbol else 'reported_symbol_differs_from_request'
                result = (measures.float_snapshot(entry, symbol) if endpoint == 'shares-float' else
                          split_fact(entry, symbol) if endpoint == 'splits' else None)
                snapshots[symbol].append({'record_id': sha(encoded(coord)), 'source_rows': [coord],
                    'reported': evidence, 'status': status, 'measurements': result,
                    'snapshot_currency_verified': False, 'joined_to_cash_flows': False, **FLAGS})
                continue
            ident, problem = measures.statement_identity(entry, symbol, mapping.get(symbol, []), cap['received_at'])
            if not problem and ident['period'] not in (('FY',) if request['period'] == 'annual' else ('Q1', 'Q2', 'Q3', 'Q4')):
                problem = 'requested_and_reported_period_mismatch'
            key = {'request_period': request['period'], 'identity': ident}
            if problem:
                key.update(problem=problem, unqualified_source_row=coord)
            record_id = sha(encoded(key))
            group = groups[symbol].setdefault(record_id, {**key, 'source_rows': [], 'reported_rows': [], 'entries': defaultdict(list)})
            group['source_rows'].append(coord); group['reported_rows'].append(evidence)
            group['entries'][endpoint].append(entry)
            if ident and not problem:
                roots['sec-cik:'+ident['cik']].add(symbol)
    if counts != manifest['counts']:
        raise ValueError('Reparsed complete original population differs from retained source counts')
    shards, summaries, statuses, metric_statuses = {}, [], Counter(), Counter()
    index_ref = {'key': PREFIX+'inputs/'+sha(index_raw)+'.json', 'sha256': sha(index_raw), 'bytes': len(index_raw)}
    index_source = {'url': sec_identity.URL, 'original': index_ref, 'requested_at': identity_cap['requested_at'],
        'received_at': identity_cap['received_at'], 'scope': 'dated_current_ticker_cik_pairs_only',
        'historical_security_continuity_verified': False}
    for symbol in manifest['reported_symbols']:
        records = []
        for record_id, group in sorted(groups[symbol].items()):
            entries = group.pop('entries')
            duplicates = sorted(k for k, values in entries.items() if len(values) != 1)
            bundle = {k: v[0] for k, v in entries.items() if k not in duplicates}
            acquired_at = max((v['source_received_at'] for v in group['reported_rows']), key=source.clock)
            result = None if group.get('problem') else measures.compute(bundle, symbol, mapping.get(symbol, []), acquired_at)
            status = group.get('problem') or ('duplicate_statement_endpoint_withheld' if duplicates else
                'complete_exact_filing_pair' if set(bundle) == set(source.STATEMENTS) else 'single_original_statement')
            records.append({'record_id': record_id, **group, 'duplicate_endpoints': duplicates,
                'status': status, 'measurements': result, **FLAGS})
            statuses[status] += 1
            if result:
                metric_statuses.update(v['status'] for v in result['metrics'].values())
        records.sort(key=lambda v: ((v.get('identity') or {}).get('date', ''), v['request_period'], v['record_id']))
        shard = {'contract': 'capital-structure-issuer-records.v1', 'requested_symbol': symbol,
            'current_sec_ciks': mapping.get(symbol, []), 'identity_index': index_source,
            'population_origins': population['complete_reported_label_occurrences'][symbol],
            'records': records, 'snapshots': snapshots[symbol],
            'source_row_count': sum(len(v['source_rows']) for v in records+snapshots[symbol]), **FLAGS}
        shards[symbol] = shard
        summaries.append({'symbol': symbol, 'record': record_ref(encoded(shard)), 'records': len(records),
            'snapshots': len(snapshots[symbol]), 'original_rows': shard['source_row_count'],
            'current_sec_ciks': mapping.get(symbol, []),
            'latest_corroborated_period_end': max((v['identity']['date'] for v in records if not v.get('problem')), default=None),
            'source_acquisition_started_at': min((v['requested_at'] for v in sources.values() if v['request']['symbol'] == symbol), key=source.clock),
            'source_acquisition_completed_at': max((v['received_at'] for v in sources.values() if v['request']['symbol'] == symbol), key=source.clock),
            'empty_responses': sum(v['rows'] == 0 for v in sources.values() if v['request']['symbol'] == symbol),
            'status': 'no_provider_rows' if not shard['source_row_count'] else 'descriptive_original_history', **FLAGS})
    if sum(v['original_rows'] for v in summaries) != counts['provider_rows']:
        raise ValueError('Every original source row must survive in one issuer record')
    packet = {'contract': CONTRACT, 'generated_at': max(acquisition+[identity_cap['received_at']], key=source.clock),
        'source_manifest_sha256': manifest_ref['sha256'], 'identity_index': index_source,
        'source_acquisition_started_at': min(acquisition, key=source.clock),
        'source_acquisition_completed_at': max(acquisition, key=source.clock), 'snapshot_atomic': False,
        'reported_names': len(summaries), 'provider_responses': len(specs), 'provider_rows': counts['provider_rows'],
        'empty_responses': counts['empty_arrays'], 'record_statuses': dict(sorted(statuses.items())),
        'metric_statuses': dict(sorted(metric_statuses.items())), 'sources': sources, 'issuers': summaries,
        'corroborated_current_issuer_roots': {k: sorted(v) for k, v in sorted(roots.items())},
        'unrequestable_reported_labels': population['reported_labels_not_provider_request_eligible'],
        'unrequestable_label_origins': {v: population['complete_reported_label_occurrences'][v]
            for v in population['reported_labels_not_provider_request_eligible']},
        'quality': {'status': 'source_reconstructed', 'original_source_bytes_replayed': True,
            'complete_planned_population': True, 'every_original_row_conserved': True,
            'current_sec_index_replayed': True, 'original_sec_filings_replayed': False,
            'historical_security_continuity_verified': False, 'statement_duration_verified': False,
            'split_basis_verified': False, 'historical_availability_verified': False,
            'provider_scale_independently_verified': False},
        'methodology': {'join': 'exact_reported_period_currency_and_filing_vintage',
            'duplicate_policy': 'retain_all_rows_withhold_ambiguous_endpoint', 'missing_policy': 'unavailable_never_zero',
            'cash_scope': 'Provider-reported cash flows; no shares retired or ownership change inferred.',
            'share_scope': 'Weighted-average basic/diluted EPS denominators, outstanding shares and free float stay separate.',
            'split_scope': 'Reported events retained; historical completeness and statement adjustment basis unverified.',
            'sbc_scope': 'Expense amount, not issued shares or shareholder dilution.',
            'yield_scope': 'No cash-flow/quote join without currency, security class and clock reconciliation.',
            'annualization': 'none; quarterly labels do not establish statement duration or TTM',
            'recommendation_scope': 'Descriptive research; no qualified investment vote or position size.'}, **FLAGS}
    return {'packet': packet, 'shards': shards, 'identity_original': index_raw}
