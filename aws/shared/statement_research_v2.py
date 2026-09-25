"""Versioned accounting research with original metadata and issuer quarantine.

The frozen v1 compiler remains replayable. This layer checks every original
row against a separately retained current SEC ticker index before exposing
company-level calculations. A current match never verifies past continuity.
"""
from collections import Counter, defaultdict
import statement_research_source as source
import statement_research_model as previous
import statement_research_identity as identity

CONTRACT = 'financial-statement-original-research.v2'
PREFIX, PRIVATE, MAX = previous.PREFIX, previous.PRIVATE, previous.MAX
FLAGS = previous.FLAGS
encoded, sha, record_ref = source.encoded, source.sha, previous.record_ref


def prepare(manifest, read):
    prefetch = getattr(read, 'prefetch', None)
    if callable(prefetch):
        prefetch([v['key'] for v in manifest['captures'].values()])
        refs = [source.strict(source.original(ref, read))['original'] for ref in manifest['captures'].values()]
        prefetch([ref['key'] for ref in refs])


def compile_output(manifest_ref, identity_ref, read):
    cap, raw_index, mapping = identity.capture(identity_ref, read)
    manifest, specs = source.plan(manifest_ref, read)
    prepare(manifest, read)
    compiled = previous.compile_output(manifest_ref, read)
    packet, shards = compiled['packet'], compiled['shards']
    raw_ref = {'key': PREFIX + 'inputs/' + sha(raw_index) + '.json', 'sha256': sha(raw_index), 'bytes': len(raw_index)}
    index_source = {'url': identity.URL, 'original': raw_ref, 'requested_at': cap['requested_at'],
        'received_at': cap['received_at'], 'last_modified': cap['headers'].get('last-modified'),
        'historical_security_continuity_verified': False, 'scope': 'current_ticker_cik_pairs_only'}
    evidence = {}
    counts, clocks, roots = Counter(), Counter(), defaultdict(set)
    for request in specs:
        ref = manifest['captures'][request['url']]
        statement, rows = source.response(ref, request, manifest['completed_at'], read)
        for number, row in enumerate(rows):
            coord = previous.coordinate(ref, statement, number)
            item = {**coord, **identity.evidence(row, request['symbol'], mapping)}
            evidence[(coord['capture_id'], number)] = item
            counts[item['current_identity_status']] += 1
            clocks.update(item['clock_issues'])
            if item['current_identity_status'] == identity.MATCH:
                roots['sec-cik:' + item['current_sec_ciks'][0]].add(request['symbol'])
    universe = source.strict(source.original(manifest['universe'], read))
    universe_clock = universe.get('generated_at') if isinstance(universe, dict) else None
    if isinstance(universe, dict):
        universe = next(v for k, v in universe.items() if k in ('rows', 'stocks', 'data') and isinstance(v, list))
    labels = {row.get('symbol') or row.get('ticker'): (i, row) for i, row in enumerate(universe)}
    statuses, metrics = Counter(), Counter()
    quarantined_rows = blocked_records = 0
    for summary in packet['issuers']:
        symbol = summary['symbol']; shard = shards[symbol]
        shard.update(contract='financial-statement-issuer-records.v2', identity_index=index_source)
        for record in shard['records']:
            record['identity_evidence'] = [evidence[(c['capture_id'], c['source_row'])] for c in record['source_rows']]
            all_matched = all(v['current_identity_status'] == identity.MATCH for v in record['identity_evidence'])
            record['current_ticker_cik_corroborated'] = all_matched
            record['historical_security_continuity_verified'] = False
            record['provider_validation_status'] = record['status']
            if not all_matched:
                quarantined_rows += len(record['source_rows'])
                if record['measurements'] is not None:
                    record['status'] = 'current_issuer_identity_not_corroborated'
                    record['measurements'] = None
            blocked_records += record['measurements'] is None
            statuses[record['status']] += 1
            if record['measurements']:
                metrics.update(v['status'] for v in record['measurements']['metrics'].values())
        number, universe_row = labels[symbol]
        summary.update(record=record_ref(encoded(shard)), current_sec_ciks=mapping.get(symbol, []),
            current_ticker_identity_scope='current_index_only_not_historical_security_master',
            calculated_records=sum(v['measurements'] is not None for v in shard['records']),
            latest_calculable_period_end=max((v['identity']['date'] for v in shard['records'] if v['measurements']), default=None),
            universe_record={'source_id': manifest['universe']['sha256'], 'source_row': number,
                'source_generated_at': universe_clock, 'reported_classification': {
                    k: identity.public_value(universe_row.get(k)) for k in ('sector', 'industry')},
                'classification_fields_present': [k for k in ('sector', 'industry') if k in universe_row],
                'classification_independently_verified': False})
    packet.update(contract=CONTRACT, identity_index=index_source,
        statement_campaign_completed_at=packet['generated_at'],
        generated_at=max((packet['generated_at'], cap['received_at']), key=source.clock),
        financial_statement_acquisition_started_at=packet['source_acquisition_started_at'],
        financial_statement_acquisition_completed_at=packet['source_acquisition_completed_at'],
        source_acquisition_started_at=min((packet['source_acquisition_started_at'], cap['requested_at']), key=source.clock),
        source_acquisition_completed_at=max((packet['source_acquisition_completed_at'], cap['received_at']), key=source.clock),
        provider_validation_statuses=packet['record_statuses'], record_statuses=dict(sorted(statuses.items())),
        metric_statuses=dict(sorted(metrics.items())), current_identity_statuses=dict(sorted(counts.items())),
        clock_issues=dict(sorted(clocks.items())), current_identity_quarantined_rows=quarantined_rows,
        records_without_calculations=blocked_records,
        complete_aligned_records=statuses['complete_aligned_statements'],
        provider_reported_cik_groups=packet['reported_issuer_roots'],
        corroborated_current_issuer_roots={root: sorted(symbols) for root, symbols in sorted(roots.items())},
        multiple_labels_per_issuer={root: sorted(symbols) for root, symbols in sorted(roots.items()) if len(symbols) > 1})
    packet['quality'].update(current_sec_index_replayed=True, historical_security_continuity_verified=False,
        original_reported_identity_metadata_preserved=True, issuer_conflicts_withheld=True)
    packet['methodology'].update(identity_correspondence='Exact requested ticker / reported CIK versus dated SEC current ticker index.',
        identity_conflict_policy='Retain every original row and reported metadata; withhold company calculations on unmapped, ambiguous or conflicting identities.',
        record_id_scope='Stable original provider-row grouping from frozen v1; content-addressed shard includes current identity evidence.',
        universe_classification='Retained screener labels and row coordinates; no independent sector verification.',
        current_identity_limit='Current corroboration does not verify historical security continuity or individual filing values.')
    return compiled
