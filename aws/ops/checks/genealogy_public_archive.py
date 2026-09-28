"""Complete public-journal reconciliation candidate; no acquisition or writes.

The inventory is a fixed, previously retained listing, not the latest summary's
preview. Every body is read exactly once and streams are always closed. Old
capture policies and incomplete scans remain visible. A registration is not a
signal onset, an independent model, or a qualified forecast.
"""
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import ast
import hashlib
import json
import math
import re

from instrument_identity import resolve_instrument
from private_artifact import public_source_allowed
from prospective_journal import canonical, digest, protocol_document, validate_record
from research_identity import record_identity_issue

BUCKET = 'justhodl-dashboard-live'
PREFIX = 'data/research-forecasts/'
CONTRACT = 'genealogy-public-archive-audit.v1'
MAX_OBJECT_BYTES = 32 * 1024 * 1024


def require(ok, code):
    if not ok:
        raise ValueError(code)


def clock(value):
    require(type(value) is str, 'clock_type')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
    except ValueError:
        raise ValueError('clock_format') from None
    require(parsed.tzinfo is not None, 'clock_timezone')
    return parsed.astimezone(timezone.utc)


def hex64(value):
    return type(value) is str and re.fullmatch('[0-9a-f]{64}', value) is not None


def public_key(value):
    return (type(value) is str and re.fullmatch(r'data/[A-Za-z0-9_.-]+\.json', value)
            and public_source_allowed(value))


def strict_json(raw):
    def pairs(values):
        result = {}
        for key, value in values:
            require(key not in result, 'duplicate_json_key')
            result[key] = value
        return result

    def number(value):
        result = float(value)
        require(math.isfinite(result), 'nonfinite_json')
        return result

    def constant(_):
        raise ValueError('nonfinite_json')

    return json.loads(raw.decode('utf-8'), object_pairs_hook=pairs,
                      parse_float=number, parse_constant=constant)


def load_inventory(path):
    lines = [line for line in path.read_text(encoding='utf-8').splitlines() if line.startswith('|')]
    require(len(lines) == 3, 'inventory_report_shape')
    headers = [x.strip() for x in lines[0].strip('|').split('|')]
    cells = [x.strip() for x in lines[2].strip('|').split('|')]
    require(len(headers) == len(cells), 'inventory_report_columns')
    inventories = ast.literal_eval(dict(zip(headers, cells))['inventories'])
    return validate_inventory(inventories)


def validate_inventory(inventories):
    require(set(inventories) == {PREFIX+'captures/', PREFIX+'records/'}, 'inventory_prefixes')
    cutoffs = set()
    for prefix, inventory in inventories.items():
        rows = inventory['objects']
        cutoff = clock(inventory['cutoff'])
        cutoffs.add(cutoff)
        require(type(rows) is list and inventory['listing_complete'] is True, 'listing_incomplete')
        require(inventory['prefix'] == prefix, 'inventory_prefix_mismatch')
        require(type(inventory['objects_at_cutoff']) is int and len(rows) == inventory['objects_at_cutoff'], 'inventory_count')
        require(hashlib.sha256(canonical(rows)).hexdigest() == inventory['inventory_sha256'], 'inventory_hash')
        previous = ''
        for row in rows:
            require(set(row) == {'key', 'bytes', 'last_modified'}, 'inventory_row_schema')
            key = row['key']
            require(type(key) is str and re.fullmatch(re.escape(prefix)+r'[0-9a-f]{64}\.json', key), 'inventory_key')
            require(key > previous, 'inventory_duplicate_or_order')
            require(type(row['bytes']) is int and 0 < row['bytes'] <= MAX_OBJECT_BYTES, 'object_size_budget')
            require(clock(row['last_modified']) <= cutoff, 'inventory_after_cutoff')
            previous = key
        require(type(inventory['total_bytes']) is int and sum(r['bytes'] for r in rows) == inventory['total_bytes'], 'inventory_bytes')
    require(len(cutoffs) == 1, 'inventory_cutoffs_differ')
    return inventories


def read_object(client, key, expected=None):
    require(type(key) is str and re.fullmatch(re.escape(PREFIX)+r'(captures|records|protocols)/[0-9a-f]{64}\.json', key), 'unreviewed_read')
    obj = client.get_object(Bucket=BUCKET, Key=key)
    stream = obj['Body']
    limit = expected['bytes'] if expected else MAX_OBJECT_BYTES
    try:
        chunks, size = [], 0
        while True:
            chunk = stream.read(min(65536, limit + 1 - size))
            if not chunk:
                break
            chunks.append(chunk)
            size += len(chunk)
            require(size <= limit, 'object_exceeds_expected_size')
        raw = b''.join(chunks)
    finally:
        stream.close()
    require(type(obj.get('ContentLength')) is int and obj['ContentLength'] == len(raw), 'object_length')
    stored = obj.get('LastModified')
    require(isinstance(stored, datetime) and stored.tzinfo is not None, 'object_storage_clock')
    if expected:
        require(len(raw) == expected['bytes'] and stored == clock(expected['last_modified']), 'object_changed_after_inventory')
    doc = strict_json(raw)
    require(type(doc) is dict, 'object_schema')
    sha = hashlib.sha256(raw).hexdigest()
    if '/records/' not in key:
        require(key.rsplit('/', 1)[1] == sha+'.json', 'content_address_mismatch')
    return doc, {'key': key, 'sha256': sha, 'bytes': len(raw), 'last_modified': stored.astimezone(timezone.utc).isoformat()}


def validate_protocol_ref(ref):
    require(type(ref) is dict and set(ref) == {'key', 'sha256', 'first_stored_at'}, 'protocol_ref_schema')
    expected = digest(protocol_document())
    require(ref['sha256'] == expected and ref['key'] == PREFIX+'protocols/'+expected+'.json', 'protocol_ref_identity')
    clock(ref['first_stored_at'])


def validate_capture(doc, evidence):
    base = {'contract', 'generated_at', 'started_at', 'protocol_ref', 'sources', 'records',
            'coverage', 'sizing_eligible', 'promotion_eligible'}
    require(base <= set(doc) <= base | {'identity_policy', 'source_read_policy'}, 'capture_schema')
    require(doc['contract'] == 'prospective-research-capture.v1', 'capture_contract')
    require(doc['sizing_eligible'] is False and doc['promotion_eligible'] is False, 'capture_authority')
    generated, started, stored = [clock(v) for v in (doc['generated_at'], doc['started_at'], evidence['last_modified'])]
    require(started <= generated and abs((stored-generated).total_seconds()) <= 300, 'capture_storage_clock')
    validate_protocol_ref(doc['protocol_ref'])
    # The first protocol may be created during the first capture. Each actual
    # registration separately has to follow its verified protocol storage time.
    require(clock(doc['protocol_ref']['first_stored_at']) <= generated, 'capture_protocol_clock')
    coverage = doc['coverage']
    require(type(coverage) is dict and set(coverage) == {'candidate_sources', 'sources_scanned', 'source_read_failures', 'candidate_scan_complete'}, 'capture_coverage_schema')
    total, scanned, errors = [coverage[k] for k in ('candidate_sources', 'sources_scanned', 'source_read_failures')]
    require(type(total) is int and type(scanned) is int and 0 <= scanned <= total and type(errors) is list, 'capture_coverage_counts')
    require(coverage['candidate_scan_complete'] is (scanned == total and not errors), 'capture_coverage_flag')
    error_keys = []
    for error in errors:
        # Earlier captures retained key-only failures. Do not read those sources.
        require(type(error) is str or type(error) is dict and set(error) == {'source_key', 'reason'}, 'capture_error_schema')
        key = error if type(error) is str else error['source_key']
        require(type(key) is str and re.fullmatch(r'data/[A-Za-z0-9_.-]+\.json', key), 'capture_error_key')
        error_keys.append(key)
    require(len(set(error_keys)) == len(error_keys), 'capture_duplicate_errors')
    require(type(doc['sources']) is list and type(doc['records']) is list, 'capture_arrays')
    sources = {}
    for source in doc['sources']:
        require(type(source) is dict and set(source) == {'source_key', 'source_bytes_sha256', 'source_generated_at', 'source_received_at', 'quality_status', 'eligibility_reasons', 'observations', 'unsupported_identity_count', 'scope'}, 'projection_schema')
        key = source['source_key']
        require(public_key(key) and key not in sources and key not in error_keys, 'projection_key')
        require(hex64(source['source_bytes_sha256']), 'projection_hash')
        receipt = clock(source['source_received_at'])
        require(started <= receipt <= generated, 'projection_receipt_clock')
        state = source['quality_status']
        require(state in ('fresh', 'stale', 'degraded', 'error', 'unknown', 'missing', 'unreported'), 'projection_quality')
        reasons = []
        if source['source_generated_at'] is None:
            reasons.append('source_publication_clock_missing')
        elif not 0 <= (receipt-clock(source['source_generated_at'])).total_seconds() <= 86400:
            reasons.append('source_publication_stale_or_future')
        if state in ('stale', 'error', 'missing'):
            reasons.append('source_quality_ineligible')
        require(source['eligibility_reasons'] == reasons, 'projection_eligibility')
        require(type(source['unsupported_identity_count']) is int and source['unsupported_identity_count'] >= 0, 'projection_identity_count')
        require(type(source['observations']) is list, 'projection_observations')
        for observed in source['observations']:
            require(type(observed) is dict and set(observed) == {'instrument', 'direction', 'origin'}, 'observation_schema')
            identity = observed['instrument']
            require(type(identity) is dict and identity.get('asset_class') == 'equity' and canonical(resolve_instrument(identity.get('symbol'), identity.get('asset_class'))) == canonical(identity), 'observation_identity')
            require(observed['origin'] == 'rank_observation' and observed['direction'] == 'NEUTRAL'
                    or observed['origin'] == 'explicit_direction' and observed['direction'] in ('UP', 'DOWN'), 'observation_direction')
        sources[key] = source
    require(len(sources) + len(errors) <= scanned, 'projection_coverage_exceeds_scan')
    expected_refs = {}
    for source in sources.values():
        if source['eligibility_reasons']:
            continue
        identity = {k: source[k] for k in ('source_key', 'source_bytes_sha256', 'source_generated_at')}
        for observed in source['observations']:
            if observed['origin'] != 'explicit_direction':
                continue
            fid = digest({'source': identity, 'observation': observed, 'protocol_sha256': doc['protocol_ref']['sha256']})
            expected_refs[fid] = (source, observed)
    refs = {}
    for ref in doc['records']:
        require(type(ref) is dict and set(ref) == {'forecast_id', 'key', 'sha256', 'created', 'registered_at', 'symbol', 'direction', 'source_key'}, 'record_ref_schema')
        fid = ref['forecast_id']
        require(hex64(fid) and hex64(ref['sha256']) and ref['key'] == PREFIX+'records/'+fid+'.json', 'record_ref_identity')
        require(fid not in refs and type(ref['created']) is bool, 'record_ref_duplicate_or_created')
        registered = clock(ref['registered_at'])
        require(registered <= generated and (not ref['created'] or registered >= started), 'record_ref_clock')
        source = sources.get(ref['source_key'])
        require(source is not None and not source['eligibility_reasons'], 'record_ref_source')
        require(any(o['origin'] == 'explicit_direction' and o['direction'] == ref['direction'] and o['instrument']['symbol'] == ref['symbol'] for o in source['observations']), 'record_ref_observation')
        require(fid in expected_refs and expected_refs[fid][0]['source_key'] == ref['source_key'], 'record_ref_source_identity')
        refs[fid] = ref
    require(set(refs) == set(expected_refs), 'eligible_observations_missing_references')
    return {'key': evidence['key'], 'sha256': evidence['sha256'], 'generated_at': doc['generated_at'],
            'started_at': doc['started_at'], 'coverage': coverage, 'sources_with_observations': len(sources),
            'ineligible_sources': sum(bool(s['eligibility_reasons']) for s in sources.values()),
            'rank_occurrences': sum(o['origin'] == 'rank_observation' for s in sources.values() for o in s['observations']),
            'explicit_occurrences': sum(o['origin'] == 'explicit_direction' for s in sources.values() for o in s['observations']),
            'record_references': len(refs), 'new_record_references': sum(r['created'] for r in refs.values()),
            'identity_policy': doc.get('identity_policy'), 'source_read_policy': doc.get('source_read_policy')}, refs


def validate_registered_record(doc, evidence):
    validate_record(doc)
    require(doc['source']['quality_status'] in ('fresh', 'degraded', 'unknown', 'unreported'), 'record_quality_state')
    # Python's True == 1 is not a typed permission or protocol equality check.
    require(canonical(doc['protocol']) == canonical(protocol_document()), 'record_protocol_types')
    require(canonical(doc['eligibility']) == canonical({'measurement_only': True, 'sizing_eligible': False,
            'promotion_eligible': False, 'original_model_replay_verified': False, 'out_of_sample_validation': False}), 'record_authority_types')
    validate_protocol_ref(doc['protocol_ref'])
    require(evidence['key'] == PREFIX+'records/'+doc['forecast_id']+'.json', 'record_path_identity')
    require(abs((clock(evidence['last_modified'])-clock(doc['registered_at'])).total_seconds()) <= 300, 'record_storage_clock')
    issue = record_identity_issue(doc)
    return {'forecast_id': doc['forecast_id'], 'key': evidence['key'], 'sha256': evidence['sha256'],
            'registered_at': doc['registered_at'], 'registration_date_et': doc['registration_date_et'],
            'source_key': doc['source']['source_key'], 'source_generated_at': doc['source']['source_generated_at'],
            'source_bytes_sha256': doc['source']['source_bytes_sha256'], 'instrument': doc['observation']['instrument'],
            'direction': doc['observation']['direction'], 'identity_issue': issue,
            'protocol_ref': doc['protocol_ref'], 'collector': doc['collector']}


def audit(client, inventories, workers=8):
    validate_inventory(inventories)
    require(type(workers) is int and 1 <= workers <= 8, 'worker_budget')
    captures, records, failures, protocols, evidence = [], {}, [], {}, []
    references, reference_counts = {}, Counter()
    reference_conflicts = []

    def read(row):
        try:
            doc, receipt = read_object(client, row['key'], row)
            if '/captures/' in row['key']:
                value = validate_capture(doc, receipt)
            else:
                value = validate_registered_record(doc, receipt)
            return value, receipt, doc['protocol_ref'], None
        except Exception as exc:
            # No arbitrary exception text or source body may enter the report.
            code = str(exc) if type(exc) is ValueError and re.fullmatch('[a-z_]+', str(exc)) else 'object_validation_failed'
            return None, None, None, {'key': row['key'], 'reason': code}

    for prefix, inventory in inventories.items():
        with ThreadPoolExecutor(max_workers=workers) as pool:
            for value, receipt, protocol, failure in pool.map(read, inventory['objects']):
                if failure:
                    failures.append(failure)
                    continue
                evidence.append(receipt)
                if protocol['key'] in protocols and canonical(protocols[protocol['key']]) != canonical(protocol):
                    failures.append({'key': receipt['key'], 'reason': 'protocol_reference_conflict'})
                protocols[protocol['key']] = protocol
                if prefix.endswith('captures/'):
                    summary, refs = value
                    captures.append(summary)
                    for fid, ref in refs.items():
                        reference_counts[fid] += 1
                        stable = {k: v for k, v in ref.items() if k != 'created'}
                        if fid in references and canonical(references[fid]) != canonical(stable):
                            reference_conflicts.append({'forecast_id': fid, 'capture': receipt['key']})
                        references[fid] = stable
                else:
                    records[value['forecast_id']] = value
    for key, ref in sorted(protocols.items()):
        try:
            doc, receipt = read_object(client, key)
            require(canonical(doc) == canonical(protocol_document()), 'protocol_body')
            require(receipt['sha256'] == ref['sha256'] and clock(receipt['last_modified']) == clock(ref['first_stored_at']), 'protocol_storage_clock')
            evidence.append(receipt)
        except Exception:
            failures.append({'key': key, 'reason': 'protocol_verification_failed'})
    ref_errors = []
    for fid, ref in sorted(references.items()):
        record = records.get(fid)
        if record is None:
            ref_errors.append({'forecast_id': fid, 'reason': 'referenced_record_unavailable'})
            continue
        fields = ('key', 'sha256', 'registered_at', 'direction', 'source_key')
        if any(ref[k] != record[k] for k in fields) or ref['symbol'] != record['instrument']['symbol']:
            ref_errors.append({'forecast_id': fid, 'reason': 'referenced_record_mismatch'})
    unreferenced = sorted(set(records) - set(references))
    ordered_records = sorted(records.values(), key=lambda r: (clock(r['registered_at']), r['forecast_id']))
    days = Counter(clock(r['registered_at']).date().isoformat() for r in ordered_records)
    sources = Counter(r['source_key'] for r in ordered_records)
    issues = Counter(r['identity_issue'] for r in ordered_records if r['identity_issue'])
    complete = not failures and not ref_errors and not reference_conflicts
    return {'contract': CONTRACT, 'cutoff': next(iter(inventories.values()))['cutoff'],
            'inventory_hashes': {p: i['inventory_sha256'] for p, i in inventories.items()},
            'listed_objects': sum(i['objects_at_cutoff'] for i in inventories.values()),
            'validated_captures': len(captures), 'validated_records': len(records),
            'validated_original_bytes': sum(e['bytes'] for e in evidence),
            'captures': sorted(captures, key=lambda r: (clock(r['generated_at']), r['key'])),
            'record_body_and_storage_checks_complete': complete,
            'capture_scans_complete': sum(c['coverage']['candidate_scan_complete'] for c in captures),
            'failures': sorted(failures, key=lambda r: r['key']), 'reference_errors': ref_errors,
            'reference_conflicts': reference_conflicts, 'unreferenced_record_ids': unreferenced,
            'unique_referenced_records': len(references), 'total_record_reference_occurrences': sum(reference_counts.values()),
            'registration_counts_by_utc_day': dict(sorted(days.items())), 'registration_counts_by_source': dict(sorted(sources.items())),
            'identity_issue_counts': dict(sorted(issues.items())), 'records': ordered_records,
            'evidence': sorted(evidence, key=lambda r: r['key']),
            'forecast_qualified': False, 'calls_eligible': False, 'sizing_eligible': False,
            'scope': 'Complete retained public registration archive at cutoff, including incomplete capture scans and identity issues. First stored registration is not signal onset. Unreferenced records may come from an interrupted capture. No private learning ledger, original engine reasoning or market prices are read.'}
