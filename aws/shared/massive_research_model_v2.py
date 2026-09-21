"""Versioned cross-asset composition; the accepted v1 compiler stays unchanged.

Parent hashes establish exactly which publication was used. They do not certify
provider originals, establish statistical independence, or authorize investment.
"""
from collections import defaultdict
import re
import massive_research_model as legacy

CONTRACT = 'massive-composite-research.v2'
CURRENT, PREFIX, PRIVATE, MAX = legacy.CURRENT, legacy.PREFIX, legacy.PRIVATE, legacy.MAX
FLAGS, PERMISSIONS = legacy.FLAGS, legacy.PERMISSIONS
encoded, sha, strict, clock = legacy.encoded, legacy.sha, legacy.strict, legacy.clock
digest, ref, exact, original = legacy.digest, legacy.ref, legacy.exact, legacy.original
permissions, source_capture, count = legacy.permissions, legacy.source_capture, legacy.count
EXTRA = {
    'fx': ('data/fx-quote-research.json', 'fx-original-quote-research.v1',
        'data/fx-quote-research/', 'fx-original-replay.v1', None),
    'futures': ('data/futures-research.json', 'futures-original-research.v1',
        'data/futures-research/', 'futures-original-replay.v1', None),
}
SOURCES = {**legacy.SOURCES, **EXTRA}
CONTEXTS, PREDECESSORS = legacy.CONTEXTS, legacy.PREDECESSORS
CAPTURE_KEYS = tuple(v[0] for v in SOURCES.values()) + CONTEXTS
FAMILIES = {
    'fx_quotes': {'provider': 'Massive/Polygon currency quote aggregates',
        'meaning': 'Reported spot quote bars; common currency legs are related exposures, not independent votes.'},
    'futures_prices': {'provider': 'Massive futures definitions, calendars and reported bars',
        'meaning': 'Dated contracts retain product, venue, units and calendar qualifications; no continuous roll series.'},
}


def parent(kind, identity, read, cutoff):
    if kind in legacy.SOURCES: return legacy.parent(kind, identity, read, cutoff)
    if kind not in EXTRA: raise ValueError('Reviewed cross-asset parent required')
    _, contract, prefix, run_contract, _ = EXTRA[kind]
    if (not isinstance(identity, dict) or set(identity) != {'manifest_key', 'output_sha256'}
            or not digest(identity.get('output_sha256')) or not isinstance(identity.get('manifest_key'), str)
            or not re.fullmatch(re.escape(prefix) + r'runs/[a-f0-9]{64}\.json', identity['manifest_key'])):
        raise ValueError('Reviewed cross-asset run identity required')
    raw = read(identity['manifest_key']); run = strict(raw)
    if (identity['manifest_key'] != prefix + 'runs/' + sha(raw) + '.json'
            or run.get('contract') != run_contract or run.get('output_sha256') != identity['output_sha256']):
        raise ValueError('Cross-asset parent run differs')
    raw = exact(run['output'], read, prefix, 'outputs'); body = strict(raw)
    if (body.get('contract') != contract or body.get('generated_at') != run.get('generated_at')
            or sha(raw) != identity['output_sha256'] or encoded(body) != raw):
        raise ValueError('Cross-asset parent output differs')
    permissions(body)
    if clock(body['generated_at']) > cutoff: raise ValueError('Parent is later than composition cutoff')
    return body, {'kind': kind, 'replay': identity, 'output': run['output'],
        'generated_at': body['generated_at'], 'verification': 'parent_run_and_output_hashes',
        'original_provider_replay_performed_by_composite': False}


def names(rows, expression, limit):
    if (not isinstance(rows, dict) or len(rows) > limit
            or any(not isinstance(k, str) or not re.fullmatch(expression, k) for k in rows)):
        raise ValueError('Bounded instrument namespace required')
    return sorted(rows)


def source_clock(value, upper):
    if clock(value) > clock(upper): raise ValueError('Source acquisition is later than its publication')
    return value


def prior_publication(packet, read, cutoff):
    """Bind previous composition bytes without recursively reinterpreting history."""
    identity = packet.get('replay')
    if (not isinstance(identity, dict) or set(identity) != {'manifest_key', 'output_sha256'}
            or not digest(identity.get('output_sha256')) or not isinstance(identity.get('manifest_key'), str)
            or not re.fullmatch(re.escape(PREFIX) + r'runs/[a-f0-9]{64}\.json', identity['manifest_key'])):
        raise ValueError('Exact prior composition run required')
    raw = read(identity['manifest_key']); run = strict(raw)
    version = 'v1' if packet['contract'] == legacy.CONTRACT else 'v2'
    if (identity['manifest_key'] != PREFIX + 'runs/' + sha(raw) + '.json'
            or run.get('contract') != 'massive-composite-replay.' + version
            or run.get('output_sha256') != identity['output_sha256']):
        raise ValueError('Prior composition run differs')
    output_raw = exact(run['output'], read, PREFIX, 'outputs'); output = strict(output_raw)
    if (sha(output_raw) != identity['output_sha256'] or encoded(output) != output_raw
            or {k: v for k, v in packet.items() if k != 'replay'} != output
            or output['generated_at'] != run['generated_at'] or clock(output['generated_at']) > cutoff):
        raise ValueError('Prior composition publication differs')


def entries(kind, body):
    """Exact pointers and typed identities, without cross-instrument equivalence."""
    completed = source_clock(body['source_capture_completed_at'], body['generated_at'])
    result = []
    if kind == 'fx':
        pairs = names(body['pairs'], r'[A-Z]{3}_[A-Z]{3}', 256)
        if count(body['configured_pairs']) != len(pairs): raise ValueError('FX pair count differs')
        total = 0
        for pair in pairs:
            row = body['pairs'][pair]; base, quote = pair.split('_'); permissions(row)
            if (row.get('pair') != pair or row.get('base_code') != base or row.get('quote_code') != quote
                    or row.get('provider_ticker') != 'C:' + base + quote):
                raise ValueError('FX pair identity differs')
            acquired = source_clock(row['source_capture_completed_at'], completed)
            due = row.get('source_review_due_at')
            if due is not None and clock(due) <= clock(acquired): raise ValueError('Invalid FX review interval')
            total += count(row['coverage']['returned_rows'])
            result.append(('FX:MASSIVE:' + pair, '/pairs/' + pair,
                {'asset_class': 'currency_quote', 'provider': 'Massive/Polygon', 'pair': pair,
                    'base_code': base, 'quote_code': quote, 'provider_ticker': row['provider_ticker'],
                    'price_unit': row['price_unit'], 'metal_base_quantity_unit_verified': row.get('metal_base_quantity_unit_verified')},
                {'source_capture_completed_at': acquired, 'source_review_due_at': due,
                    'observation_clock_role': 'reported_aggregate_window_start_not_close_time',
                    'latest_reported_window_start_utc': (row.get('latest_reported_row') or {}).get('window_start_utc')}))
        if total != count(body['returned_rows']): raise ValueError('FX returned-row count differs')
    elif kind == 'futures':
        products = names(body['products'], r'[A-Z][A-Z0-9]{0,11}', 64)
        if count(body['quality']['products']) != len(products): raise ValueError('Futures product count differs')
        definition_date = body['definition_date']
        if not isinstance(definition_date, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', definition_date):
            raise ValueError('Explicit futures definition date required')
        clock(definition_date + 'T00:00:00Z')
        if definition_date > clock(completed).date().isoformat(): raise ValueError('Future futures definition date')
        contracts = 0
        for product in products:
            row = body['products'][product]; permissions(row); venue = row.get('venue')
            if row.get('product_code') != product or not isinstance(venue, str) or not re.fullmatch(r'[A-Z][A-Z0-9_]{0,19}', venue):
                raise ValueError('Futures product/venue identity differs')
            series = row['contracts']
            if not isinstance(series, list) or len(series) > 256: raise ValueError('Bounded dated contract inventory required')
            seen = set()
            for index, item in enumerate(series):
                permissions(item); ticker = item.get('ticker')
                if (not isinstance(ticker, str) or not re.fullmatch(r'[A-Z][A-Z0-9]{0,23}', ticker) or ticker in seen):
                    raise ValueError('Unique dated futures contract required')
                seen.add(ticker); dataset_name = product + ':bars:' + ticker
                if item.get('dataset') != dataset_name: raise ValueError('Futures bar dataset identity differs')
                dataset = body['datasets'][dataset_name]; scope = dataset['scope']
                if scope.get('kind') != 'bars' or scope.get('product') != product or scope.get('ticker') != ticker:
                    raise ValueError('Futures bar scope differs')
                acquired = source_clock(dataset['source_capture_completed_at'], completed)
                if count(dataset['returned_rows']) != count(item['coverage']['returned_rows']):
                    raise ValueError('Futures bar coverage differs')
                result.append(('FUTURE:' + venue + ':' + product + ':' + ticker,
                    '/products/' + product + '/contracts/' + str(index),
                    {'asset_class': 'dated_future', 'provider': 'Massive', 'venue': venue,
                        'product': product, 'ticker': ticker, 'definition_date': definition_date,
                        'definition_source': item['definition_source'], 'dataset': dataset_name},
                    {'source_capture_completed_at': acquired, 'source_review_due_at': None,
                        'observation_clock_role': 'reported_session_label_not_synchronized_price',
                        'latest_reported_session_end_date': (item.get('latest_reported_row') or {}).get('session_end_date'),
                        'bar_finality_independently_verified': False}))
                contracts += 1
        if contracts != count(body['quality']['selected_contracts']): raise ValueError('Futures contract count differs')
    else: raise ValueError('Reviewed cross-asset collection required')
    return result


def build(inputs, read):
    if inputs.get('contract') != 'massive-composite-inputs.v2': raise ValueError('Typed v2 composition inputs required')
    cutoff = clock(inputs['generated_at']); captures = inputs['sources']
    if set(captures) != set(CAPTURE_KEYS): raise ValueError('Whole cross-asset source inventory required')
    prior = source_capture(inputs['prior_composite'], CURRENT, read, cutoff)
    if prior is None or prior.get('contract') not in (legacy.CONTRACT, CONTRACT):
        raise ValueError('Retained prior native composite required')
    permissions(prior)
    if clock(prior['generated_at']) > cutoff: raise ValueError('Prior composite is later than cutoff')
    prior_publication(prior, read, cutoff)
    baseline = {'contract': 'massive-composite-inputs.v1', 'generated_at': inputs['generated_at'],
        'sources': {k: captures[k] for k in legacy.CAPTURE_KEYS}, 'predecessors': inputs['predecessors']}
    out = legacy.build(baseline, read)
    out.update(contract=CONTRACT, version='3.1.0', prior_composite=inputs['prior_composite'])
    identities = {key: {'asset_class': 'provider_security_symbol', 'symbol': key,
        'meaning': 'Existing option/fund symbol scope; exchange, share class and economic equivalence are not inferred.'}
        for key in out['instruments']}
    graph = out['dependency_graph']; shared = defaultdict(list)
    for kind, (key, contract, _, _, _) in EXTRA.items():
        capture = captures[key]; packet = source_capture(capture, key, read, cutoff)
        source = {'source_key': key, 'capture': capture, 'status': capture['status'], 'node': None,
            'independent_investment_votes': 0, **PERMISSIONS}
        if capture['status'] == 'retained' and packet is None: source['status'] = 'invalid_source_document'
        elif packet is not None and packet.get('contract') != contract: source['status'] = 'unqualified_source_contract'
        elif packet is not None:
            body, node = parent(kind, packet['replay'], read, cutoff)
            if {k: v for k, v in packet.items() if k != 'replay'} != body:
                raise ValueError('Captured cross-asset publication differs from parent output')
            node_id = kind + ':' + packet['replay']['output_sha256']; graph['nodes'][node_id] = node
            rows = entries(kind, body); due = [clock(v[3]['source_review_due_at']) for v in rows if v[3]['source_review_due_at'] is not None]
            deadline = min(due, default=None)
            source.update(status='descriptive_parent_bound', node=node_id, source_generated_at=body['generated_at'],
                source_capture_completed_at=body['source_capture_completed_at'], source_quality=body.get('quality'),
                source_review_due_at=deadline.isoformat() if deadline else None,
                source_review_overdue=cutoff >= deadline if deadline else None,
                freshness_scope='Acquisition review only; original window/session clocks and finality remain in each exact parent row.',
                instrument_count=len(rows), instrument_names=sorted(v[0] for v in rows))
            clocks = body['pairs'] if kind == 'fx' else body['datasets']
            source['measurement_capture_clocks'] = {name: source_clock(value['source_capture_completed_at'], body['source_capture_completed_at'])
                for name, value in sorted(clocks.items())}
            source['source_definition_date'] = body.get('definition_date')
            for identity, pointer, definition, dates in rows:
                if identity in identities: raise ValueError('Instrument identity collision')
                identities[identity] = definition
                out['instruments'][identity] = [{'source': kind, 'node': node_id, 'pointer': pointer,
                    'source_generated_at': body['generated_at'], **dates,
                    'source_review_overdue': cutoff >= clock(dates['source_review_due_at']) if dates['source_review_due_at'] else None,
                    'meaning': 'Exact parent row; neither a new observation nor an independent investment vote.'}]
                if kind == 'fx':
                    for leg in (definition['base_code'], definition['quote_code']): shared['currency:' + leg].append(identity)
                else: shared['futures_product:' + definition['venue'] + ':' + definition['product']].append(identity)
            family = 'fx_quotes' if kind == 'fx' else 'futures_prices'
            graph['measurement_families'].append({'family': family, **FAMILIES[family], 'nodes': [node_id],
                'retained_node_count': 1, 'independent_investment_votes': 0})
        out['sources'][kind] = source
    if len(graph['nodes']) > 14: raise ValueError('Cross-asset graph bound exceeded')
    out['instruments'] = dict(sorted(out['instruments'].items())); out['instrument_identities'] = dict(sorted(identities.items()))
    graph['nodes'] = dict(sorted(graph['nodes'].items())); graph['measurement_families'].sort(key=lambda row: row['family'])
    graph['shared_exposures'] = [{'key': key, 'instruments': sorted(values), 'independent_investment_votes': 0,
        'meaning': 'Shared quoted currency leg or futures product; no correlation, hedge ratio or portfolio netting is inferred.'}
        for key, values in sorted(shared.items())]
    bound = sum(row['node'] is not None for row in out['sources'].values())
    out['quality'].update(status='descriptive' if bound == len(SOURCES) else 'partial' if bound else 'unavailable',
        declared_native_sources=len(SOURCES), bound_native_sources=bound,
        overdue_source_reviews=sum(row.get('source_review_overdue') is True for row in out['sources'].values()))
    out['identity_policy'] = 'FX:MASSIVE:<base_quote>; FUTURE:<venue>:<product>:<dated ticker>. Existing security symbols retain their original scope. No automatic asset equivalence.'
    return out
