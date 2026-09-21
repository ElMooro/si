"""Reproducible composition of research publications, without investment votes.

This model checks parent run/output identities and preserves their definitions.
It replays the composition, not every parent's original provider reconstruction.
Those distinct assurance levels must remain visible to clients.
"""
from collections import defaultdict
from datetime import datetime, timezone
import hashlib, json, re

CONTRACT = 'massive-composite-research.v1'
CURRENT = 'data/massive-research.json'
PREFIX = 'data/massive-research/'
PRIVATE = 'audit-private/20260909-originals/massive-research/'
MAX = 16 * 1024 * 1024
FLAGS = ('forecast_qualified', 'calls_eligible', 'sizing_eligible', 'execution_eligible')
PERMISSIONS = {key: False for key in FLAGS}
# Public heads, contract, immutable prefix, run contract, optional run kind.
SOURCES = {
    'options': ('data/option-flow-research.json', 'option-flow-original-research.v1',
        'data/option-flow-research/', 'option-flow-replay.v1', None),
    'populations': ('data/option-population-research.json', 'option-population-desk.v1',
        'data/option-population-research/', 'option-population-replay.v1', None),
    'etf_desk': ('data/etf-desk-research.json', 'etf-desk-original-research.v1',
        'data/etf-desk-research/', 'etf-desk-replay.v1', None),
    'fund_flows': ('data/provider-fund-flow-research.json', 'provider-fund-flow-research.v1',
        'data/provider-flow-research/', 'provider-flow-replay.v1', 'flow'),
    'holdings': ('data/etf-holdings-research.json', 'etf-holdings-original-research.v1',
        'data/etf-holdings-research/', 'etf-holdings-replay.v1', 'holdings'),
}
CONTEXTS = ('data/polygon-fx-regime.json', 'data/polygon-futures-curves.json', 'flow-data.json')
PREDECESSORS = ('data/massive-signals.json', 'data/massive-capability.json',
    'data/polygon-options.json', 'data/polygon-ratios.json')
CAPTURE_KEYS = tuple(v[0] for v in SOURCES.values()) + CONTEXTS
FAMILIES = {
    'option_chain': {'provider': 'Massive/Polygon option snapshots',
        'meaning': 'Captured option contracts; no observed dealer ownership or independent OI/Greek dates.'},
    'fund_flow': {'provider': 'ETF Global via Massive/Polygon',
        'meaning': 'Dated fund flows; no inferred underlying-security purchases.'},
    'constituents': {'provider': 'ETF Global via Massive/Polygon',
        'meaning': 'Returned fund constituents; date, identity and raw weight qualifications stay local.'},
    'fund_profile': {'provider': 'ETF Global via Massive/Polygon',
        'meaning': 'Fund profiles on their own effective dates; unspecified fee/exposure scales remain unqualified.'},
}


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()


def sha(raw): return hashlib.sha256(raw).hexdigest()


def pairs(items):
    out = {}
    for key, value in items:
        if key in out: raise ValueError('Duplicate JSON member')
        out[key] = value
    return out


def strict(raw):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX: raise ValueError('Bounded original bytes required')
    def reject(_): raise ValueError('Non-finite JSON value')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=reject)


def clock(value):
    if not isinstance(value, str): raise ValueError('Aware source clock required')
    out = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if out.tzinfo is None: raise ValueError('Aware source clock required')
    return out.astimezone(timezone.utc)


def digest(value):
    return isinstance(value, str) and bool(re.fullmatch('[a-f0-9]{64}', value))


def ref(raw, kind):
    if kind not in ('inputs', 'outputs', 'runs', 'compilers', 'originals'): raise ValueError('Reviewed composite artifact kind required')
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX: raise ValueError('Bounded composite artifact required')
    d = sha(raw)
    key = PRIVATE + d + '.bin' if kind == 'originals' else PREFIX + kind + '/' + d + ('.py' if kind == 'compilers' else '.json')
    return {'key': key, 'sha256': d, 'bytes': len(raw)}


def exact(identity, read, prefix, kind, extension='.json'):
    if (not isinstance(identity, dict) or set(identity) != {'key', 'sha256', 'bytes'}
            or not digest(identity.get('sha256')) or type(identity.get('bytes')) is not int
            or not 0 < identity['bytes'] <= MAX
            or identity.get('key') != prefix + (kind + '/' if kind else '') + identity['sha256'] + extension):
        raise ValueError('Exact bounded content-addressed reference required')
    raw = read(identity['key'])
    if not isinstance(raw, bytes) or len(raw) != identity['bytes'] or sha(raw) != identity['sha256']:
        raise ValueError('Retained evidence bytes differ')
    return raw


def original(identity, read): return exact(identity, read, PRIVATE, '', '.bin')


def permissions(packet):
    if not isinstance(packet, dict) or any(packet.get(k) is not False for k in FLAGS):
        raise ValueError('Explicit descriptive authority boundary required')


def parent(kind, identity, read, cutoff):
    """Verify a published run/output pair; never execute retained source code."""
    _, contract, prefix, run_contract, run_kind = SOURCES[kind]
    if (not isinstance(identity, dict) or set(identity) != {'manifest_key', 'output_sha256'}
            or not digest(identity.get('output_sha256')) or not isinstance(identity.get('manifest_key'), str)
            or not re.fullmatch(re.escape(prefix) + r'runs/[a-f0-9]{64}\.json', identity['manifest_key'])):
        raise ValueError('Reviewed parent run identity required')
    raw = read(identity['manifest_key']); run = strict(raw)
    if (identity['manifest_key'] != prefix + 'runs/' + sha(raw) + '.json'
            or run.get('contract') != run_contract or run.get('output_sha256') != identity['output_sha256']
            or run_kind is not None and run.get('kind') != run_kind):
        raise ValueError('Parent run bytes or contract differ')
    output_raw = exact(run['output'], read, prefix, 'outputs'); output = strict(output_raw)
    if (output.get('contract') != contract or output.get('generated_at') != run.get('generated_at')
            or sha(output_raw) != identity['output_sha256'] or encoded(output) != output_raw):
        raise ValueError('Parent output identity differs')
    permissions(output)
    if clock(output['generated_at']) > cutoff: raise ValueError('Parent is later than composite cutoff')
    return output, {'kind': kind, 'replay': identity, 'output': run['output'],
        'generated_at': output['generated_at'], 'verification': 'parent_run_and_output_hashes',
        'original_provider_replay_performed_by_composite': False}


def source_capture(capture, key, read, cutoff):
    if (not isinstance(capture, dict) or capture.get('source_key') != key
            or capture.get('status') not in ('retained', 'missing', 'read_failed')):
        raise ValueError('Complete source capture status required')
    if clock(capture['acquired_at']) > cutoff: raise ValueError('Capture is later than composite cutoff')
    if capture['status'] != 'retained':
        if capture.get('original') is not None: raise ValueError('Unavailable capture cannot carry an original')
        return None
    raw = original(capture['original'], read)
    # Malformed upstream JSON is retained whole, but cannot become evidence.
    try: packet = strict(raw)
    except (ValueError, UnicodeError): return None
    return packet if isinstance(packet, dict) else None


def count(value):
    if type(value) is not int or not 0 <= value <= 100000000: raise ValueError('Bounded exact row count required')
    return value


def symbols(rows):
    if not isinstance(rows, dict) or len(rows) > 2000: raise ValueError('Bounded symbol inventory required')
    if any(not isinstance(k, str) or not re.fullmatch(r'[A-Z][A-Z0-9.\-]{0,14}', k) for k in rows):
        raise ValueError('Qualified symbol identity required')
    return sorted(rows)


def deadlines(kind, packet):
    """Keep source review deadlines; publication time is never a substitute."""
    if kind in ('options', 'populations'):
        value = packet.get('quality', {}).get('acquisition_review_due_at')
        return [value] if value is not None else []
    if kind != 'etf_desk':
        value = packet.get('source_valid_until'); return [value] if value is not None else []
    values = []
    for row in packet['funds'].values():
        values.extend(v for v in (row['flows'].get('source_valid_until'),
            row['holdings']['current'].get('source_valid_until'),
            row['profiles']['current'].get('source_valid_until')) if v is not None)
    return sorted(set(values))


def build(inputs, read):
    if inputs.get('contract') != 'massive-composite-inputs.v1': raise ValueError('Typed composite inputs required')
    cutoff = clock(inputs['generated_at']); captures = inputs['sources']; predecessors = inputs['predecessors']
    if set(captures) != set(CAPTURE_KEYS) or set(predecessors) != set(PREDECESSORS):
        raise ValueError('Whole declared source/predecessor inventory required')
    for key, capture in predecessors.items(): source_capture(capture, key, read, cutoff)
    nodes = {}; bodies = {}; edges = set(); family_members = defaultdict(set)

    def add(kind, identity):
        node_id = kind + ':' + identity.get('output_sha256', '')
        if node_id in nodes:
            if nodes[node_id]['replay'] != identity: raise ValueError('Conflicting runs for one output')
            return node_id
        packet, node = parent(kind, identity, read, cutoff)
        nodes[node_id] = node; bodies[node_id] = packet
        if len(nodes) > 12: raise ValueError('Parent graph bound exceeded')
        if kind == 'populations':
            upstream = add('options', packet['source_run']); edges.add((node_id, upstream))
            if clock(packet['source_capture_completed_at']) != clock(bodies[upstream]['generated_at']):
                raise ValueError('Population source capture clock differs')
            if clock(bodies[upstream]['generated_at']) > clock(packet['generated_at']):
                raise ValueError('Population parent is later than its view')
        elif kind == 'etf_desk':
            for role, upstream_kind in (('flows', 'fund_flows'), ('holdings', 'holdings')):
                declared = packet['canonical_sources'][role]; upstream = add(upstream_kind, declared['replay'])
                if clock(declared['generated_at']) != clock(bodies[upstream]['generated_at']):
                    raise ValueError('ETF canonical source clock differs')
                if clock(bodies[upstream]['generated_at']) > clock(packet['generated_at']):
                    raise ValueError('ETF parent is later than its view')
                edges.add((node_id, upstream))
            family_members['fund_profile'].add(node_id)
            if packet.get('quality', {}).get('additional_funds'):
                family_members['fund_flow'].add(node_id); family_members['constituents'].add(node_id)
        else: family_members[{'options': 'option_chain', 'fund_flows': 'fund_flow', 'holdings': 'constituents'}[kind]].add(node_id)
        return node_id

    source_rows = {}; instruments = defaultdict(list)
    for kind, (key, contract, _, _, _) in SOURCES.items():
        capture = captures[key]; packet = source_capture(capture, key, read, cutoff)
        row = {'source_key': key, 'capture': capture, 'status': capture['status'],
            'node': None, 'independent_investment_votes': 0, **PERMISSIONS}
        if capture['status'] == 'retained' and packet is None: row['status'] = 'invalid_source_document'
        elif packet is not None and packet.get('contract') != contract: row['status'] = 'unqualified_source_contract'
        elif packet is not None:
            node_id = add(kind, packet['replay']); body = bodies[node_id]
            if {k: v for k, v in packet.items() if k != 'replay'} != body:
                raise ValueError('Captured publication differs from retained parent output')
            collection = 'chains' if kind == 'options' else 'underlyings' if kind == 'populations' else 'funds'
            names = symbols(body[collection]); due = deadlines(kind, body)
            due_at = min((clock(v) for v in due), default=None)
            source_stamp = body.get('source_capture_completed_at') if kind == 'populations' else body['generated_at'] if kind == 'options' else None
            row.update(status='descriptive_parent_bound', node=node_id, source_generated_at=body['generated_at'],
                source_capture_completed_at=source_stamp, source_quality=body.get('quality'),
                source_review_due_at=due_at.isoformat() if due_at else None,
                source_review_overdue=cutoff >= due_at if due_at else None,
                freshness_scope='Acquisition review only; individual observation dates and field units remain in the source row.',
                instrument_count=len(names), instrument_names=names)
            for ticker in names:
                pointer = '/' + collection + '/' + ticker
                instruments[ticker].append({'source': kind, 'node': node_id, 'pointer': pointer,
                    'source_generated_at': body['generated_at'], 'source_review_overdue': row['source_review_overdue'],
                    'meaning': 'Exact parent row; neither a new observation nor an independent investment vote.'})
            if kind == 'options':
                total = sum(count(r['coverage']['returned_rows']) for r in body['chains'].values())
                eligible = sum(count(r['coverage']['eligible_identity_rows']) for r in body['chains'].values())
                if (total != count(body['quality']['returned_rows']) or eligible != count(body['quality']['identity_eligible_rows'])
                        or any(r['coverage']['eligible_identity_rows'] > r['coverage']['returned_rows'] for r in body['chains'].values())):
                    raise ValueError('Option coverage totals differ')
                row['coverage'] = {'returned_rows': total, 'identity_eligible_rows': eligible}
            elif kind == 'populations':
                total = sum(count(r['totals']['counts']['identity_eligible_rows']) for r in body['underlyings'].values())
                if total != body['quality']['counts']['identity_eligible_rows']: raise ValueError('Population coverage totals differ')
                row['coverage'] = {'identity_eligible_rows': total,
                    'expiry_strike_groups': sum(count(r['expiry_strike_groups']) for r in body['underlyings'].values())}
        source_rows[kind] = row

    legacy = {}
    for key in CONTEXTS:
        capture = captures[key]; packet = source_capture(capture, key, read, cutoff)
        legacy[key] = {'capture': capture, 'status': 'unqualified_retained_context' if packet is not None else capture['status'] if capture['status'] != 'retained' else 'invalid_source_document',
            'reported_generated_at': packet.get('generated_at') if packet else None,
            'reported_as_of': packet.get('as_of') if packet else None,
            'reported_status': packet.get('status') if packet else None,
            'reported_identity_ok': packet.get('identity_ok') if packet else None,
            'reported_products_with_data': packet.get('n_products_with_data') if packet else None,
            'source_observation_clocks_verified': False, 'independent_investment_votes': 0, **PERMISSIONS}
    bound = sum(row['node'] is not None for row in source_rows.values())
    families = [{'family': family, **FAMILIES[family], 'nodes': sorted(members),
        'retained_node_count': len(members), 'independent_investment_votes': 0}
        for family, members in sorted(family_members.items())]
    return {'contract': CONTRACT, 'engine': 'justhodl-massive-signals', 'version': '3.0.0',
        'generated_at': inputs['generated_at'], 'clock_role': 'composition_completed; source dates are preserved per parent',
        'sources': source_rows, 'contexts': legacy, 'predecessors': predecessors,
        'instruments': dict(sorted(instruments.items())),
        'dependency_graph': {'nodes': dict(sorted(nodes.items())),
            'edges': [{'view': a, 'source': b} for a, b in sorted(edges)], 'measurement_families': families,
            'statistical_independence_established': False, 'independent_investment_votes': 0,
            'rule': 'Views of the same source family are not separate confirmations. Different retained vintages stay separate; no temporal join or vote sum is performed.'},
        'quality': {'status': 'descriptive' if bound == len(SOURCES) else 'partial' if bound else 'unavailable',
            'declared_native_sources': len(SOURCES), 'bound_native_sources': bound,
            'overdue_source_reviews': sum(row.get('source_review_overdue') is True for row in source_rows.values()),
            'unqualified_legacy_contexts': len(legacy)},
        'verification': {'composition_replay': True, 'parent_run_output_hashes': True,
            'original_provider_replay_performed_by_composite': False,
            'scope': 'Deterministic composition of exact recorded parent outputs. Original-source reconstruction is supplied by the separate parent research engines and their acceptance evidence.'},
        'call': None, 'score': None, 'portfolio_action': 'WAIT', 'independent_investment_votes': 0, **PERMISSIONS,
        'portfolio_consequences': {'status': 'explicit_assumptions_required',
            'forecast_or_size_supplied': False,
            'meaning': 'Use the pinned parent measurements with explicit positions, prices and costs. Source presence, aggregate agreement and liquidity context do not establish expected returns.'},
        'provider_requests': 0, 'private_account_reads': 0, 'paid_ai_calls': 0, 'notifications_sent': 0,
        'signals_emitted': 0, 'portfolio_writes': 0}
