"""Independent conservation verifier for a derived-packet inventory.

Does not import the compiler, execute the predecessor's normalizers, follow
replay declarations, or establish original-provider / predictive qualification.
"""
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
import hashlib
import json
import re

FLAGS = ('calls_eligible', 'sizing_eligible', 'execution_eligible', 'forecast_qualified')
EXCLUDED = {'data/pm-decision.json', 'data/sizing.json'}
dump = lambda v: json.dumps(v, sort_keys=True, separators=(',', ':'), allow_nan=False).encode()
digest = lambda b: hashlib.sha256(b).hexdigest()


def require(condition, message):
    if not condition:
        raise ValueError(message)


def clock(value):
    if type(value) is not str or re.fullmatch(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,6})?(?:Z|[+-]\d{2}:\d{2})', value) is None:
        return None
    try:
        return datetime.fromisoformat(value.replace('Z', '+00:00')).astimezone(timezone.utc)
    except ValueError:
        return None


def decode(body):
    def unique(pairs):
        keys = [key for key, _ in pairs]
        if len(set(keys)) != len(keys):
            raise ValueError('Ambiguous members')
        return dict(pairs)
    def finite(token):
        raise ValueError('Invalid numeric token: ' + token)
    return json.loads(body, parse_float=Decimal, object_pairs_hook=unique, parse_constant=finite)


def verify(output, rows, captures, read):
    require(isinstance(output, dict) and output.get('contract') == 'signal-board-research.v1', 'Wrong contract')
    require(set(output) == set(FLAGS) | {'contract', 'schema_version', 'method', 'generated_at', 'registry_sha256',
        'capture_receipts_sha256', 'composite_signal', 'composite_posture', 'deep_read', 'n_engines', 'n_live',
        'n_stale', 'elapsed_s', 'categories', 'engines', 'sources', 'source_status_counts', 'dependency_graph',
        'snapshot_atomic', 'original_provider_verified', 'point_in_time_qualified', 'decision', 'note'}, 'Unreviewed output fields')
    require(output['schema_version'] == '2.0' and output['method'] == 'complete_derived_source_inventory' and output['elapsed_s'] is None, 'Unreviewed method')
    require(len(rows) == 99 and len({r['engine'] for r in rows}) == 99, 'Registry population lost')
    keys = {r['source_key'] for r in rows}
    require(len(keys) == 98 and set(captures) == keys and set(output['sources']) == keys, 'Source population differs')
    require(output['registry_sha256'] == digest(dump(rows)) and output['capture_receipts_sha256'] == digest(dump(captures)), 'Inputs not bound')
    now = clock(output['generated_at'])
    require(now is not None, 'Compilation clock invalid')
    for flag in (*FLAGS, 'original_provider_verified', 'point_in_time_qualified', 'snapshot_atomic'):
        require(output.get(flag) is False, 'Unsupported authority or snapshot claim')
    require(output['composite_signal'] is None and output['deep_read'] is None and output['composite_posture'] == 'WAIT', 'Unsupported composite or narrative')
    require(output['n_live'] == 0 and output['n_engines'] == 99 and output['n_stale'] is None, 'Counts create false freshness or votes')
    require(output['decision'] == {'verb': 'WAIT', 'meaning': 'abstain', 'qualified_votes': 0,
        'portfolio_consequences': {'status': 'unavailable', 'target_weights': None,
        'reason': 'No qualified forecast, holdings, covariance or cost model bound to this inventory'}}, 'Decision authority changed')
    total_bytes = 0
    whole = 0
    for key in sorted(keys):
        actual, receipt = output['sources'][key], captures[key]
        require(set(actual) == set(FLAGS) | {'source_key', 'capture_receipt_sha256', 'original', 'original_provider_verified',
            'observation_freshness', 'publication', 'contract_declared', 'replay_declared', 'permission_declarations', 'status', 'http_status'}, 'Unreviewed source fields')
        require(actual['source_key'] == key and receipt['source_key'] == key, 'Source identity differs')
        require(actual['capture_receipt_sha256'] == digest(dump(receipt)), 'Capture receipt differs')
        for flag in (*FLAGS, 'original_provider_verified'):
            require(actual.get(flag) is False, 'Source declaration became authority')
        require(actual['observation_freshness'] == 'unverified', 'Observation freshness inferred from publication')
        publication = {'reported_at': None, 'parsed_at': None, 'status': 'not_reported'}
        permissions = dict.fromkeys(FLAGS, 'not_reported')
        contract = None
        replay = False
        if key in EXCLUDED:
            require(receipt == {'source_key': key, 'status': 'excluded_private_account_input', 'requested': False}, 'Private input accessed')
            state, http = 'private_input_excluded', None
            require(actual['original'] is None, 'Private bytes exposed')
        elif receipt['status'] == 'transport_or_size_unavailable':
            require('original' not in receipt and actual['original'] is None, 'Incomplete response presented as whole')
            state, http = 'transport_unavailable', None
        else:
            http = receipt['http_status']
            require(type(http) is int and 100 <= http <= 599, 'Invalid HTTP status')
            require(receipt['status'] == ('public_sidecar_retained' if http == 200 else 'whole_http_error_retained'), 'Transport status inconsistent')
            ref = receipt['original']
            require(actual['original'] == ref, 'Whole original reference differs')
            body = read(ref)
            require(len(body) == ref['bytes'] and digest(body) == ref['sha256'], 'Source bytes differ')
            total_bytes += len(body)
            whole += 1
            if http != 200:
                state = 'http_error'
            else:
                try:
                    packet = decode(body)
                except (ValueError, UnicodeError, RecursionError):
                    state = 'invalid_json'
                else:
                    state = 'derived_packet_retained' if type(packet) is dict else 'non_object_json'
                    if type(packet) is dict:
                        raw_date = packet.get('generated_at')
                        dated = clock(raw_date)
                        publication = {'reported_at': raw_date if type(raw_date) is str else None,
                            'parsed_at': dated.isoformat() if dated else None,
                            'status': ('future' if dated > now else 'reported_past') if dated else
                            'not_reported' if raw_date is None else 'invalid'}
                        contract = packet.get('contract') if type(packet.get('contract')) is str else None
                        replay = type(packet.get('replay')) is dict
                        permissions = {flag: 'not_reported' if flag not in packet else
                            'declared_true' if packet[flag] is True else 'declared_false' if packet[flag] is False else 'invalid' for flag in FLAGS}
        require(actual['status'] == state and actual['http_status'] == http, 'Availability classification differs')
        require(actual['publication'] == publication, 'Publication evidence differs')
        require(actual['permission_declarations'] == permissions and actual['contract_declared'] == contract and actual['replay_declared'] is replay, 'Declared metadata differs')
    counts = dict(sorted(Counter(s['status'] for s in output['sources'].values()).items()))
    require(output['source_status_counts'] == counts, 'Status counts differ')
    graph = output['dependency_graph']
    expected_groups = {key: [r['engine'] for r in rows if r['source_key'] == key] for key in sorted(keys)}
    require(graph == {'basis': 'shared derived-packet keys only; original-provider ancestry unverified',
        'source_groups': expected_groups, 'registered_views': 99, 'distinct_derived_sources': 98,
        'independent_original_roots': None, 'independent_votes_qualified': 0}, 'Derived source groups mistaken for independent evidence')
    require(len(output['engines']) == 99, 'Engine rows lost')
    for index, (registered, actual) in enumerate(zip(rows, output['engines'])):
        require(set(actual) == set(FLAGS) | {'row_id', 'engine', 'category', 'source_key', 'source_views', 'source_status',
            'signal', 'signal_label', 'read', 'as_of', 'stale', 'stale_basis', 'reported_generated_at'}, 'Unreviewed engine fields')
        item = output['sources'][registered['source_key']]
        require(actual['row_id'] == index + 1 and all(actual[k] == registered[k] for k in ('engine', 'category', 'source_key')), 'Engine mapping differs')
        require(actual['source_views'] == len(expected_groups[registered['source_key']]) and actual['source_status'] == item['status'], 'Shared-source mapping differs')
        require(actual['signal'] is None and actual['signal_label'] == 'ABSTAIN' and actual['as_of'] is None and actual['stale'] is None, 'Unqualified score or age created')
        require(actual['reported_generated_at'] == item['publication']['reported_at'], 'Reported date differs')
        require(actual['read'] == 'Derived-source inventory; no qualified investment vote' and actual['stale_basis'] == 'observation freshness unverified', 'Unqualified narrative')
        require(all(actual.get(k) is False for k in FLAGS), 'Engine vote authorized')
    expected_categories = {c: {'signal': None, 'n': 0, 'registered_rows': sum(r['category'] == c for r in rows)} for c in sorted({r['category'] for r in rows})}
    require(output['categories'] == expected_categories, 'Category rollup creates a vote')
    return {'contract': 'signal-board-inventory-proof.v1', 'registered_rows': 99, 'distinct_derived_sources': 98,
            'whole_responses_checked': whole, 'whole_response_bytes': total_bytes, 'source_status_counts': counts,
            'private_inputs_excluded': 2, 'derived_inventory_reproduced': True,
            'original_provider_verified': False, 'predictive_validation_performed': False,
            'independent_original_roots': None, **dict.fromkeys(FLAGS, False)}
