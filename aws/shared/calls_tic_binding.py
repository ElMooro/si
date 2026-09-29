"""Bind whole TIC originals to frozen Calls inputs and recheck source clocks."""
from copy import deepcopy
from datetime import timedelta
import hashlib
from pathlib import Path
import re
import calls_tic_originals as lineage

CONTRACT = 'calls-tic-binding.v1'
KEY = 'data/capital-inflows.json'
BASIS = 'whole parsed public TIC packet, canonical JSON; not transport bytes'
FAILURES = ('source_unavailable', 'unsupported_source_contract', 'invalid_original_reference', 'original_replay_failed')


def reference(value):
    if (not isinstance(value, dict) or set(value) != {'manifest_key', 'output_sha256'} or
        not isinstance(value['manifest_key'], str) or not re.fullmatch(r'data/tic-research/runs/[a-f0-9]{64}\.json', value['manifest_key']) or
        not isinstance(value['output_sha256'], str) or not re.fullmatch('[a-f0-9]{64}', value['output_sha256'])):
        raise ValueError('Exact immutable TIC replay reference required')
    return dict(value)


def unavailable(reason):
    if reason not in FAILURES: raise ValueError('Unsupported TIC capture failure')
    return {'contract': CONTRACT, 'status': 'unavailable', 'reason': reason}


def snapshot(ref, read):
    if read is None: raise ValueError('Original artifact reader required for frozen TIC run')
    ref = reference(ref)
    modules = (lineage, lineage.store, lineage.arithmetic, *lineage.store.COMPILERS)
    identity = lineage.digest({'contract': CONTRACT, 'reference': ref, 'compilers': {
        Path(module.__file__).name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() for module in modules}})
    def build():
        retained = lineage.ImmutableReader(read)
        manifest = lineage.strict(retained(ref['manifest_key']))
        if manifest.get('output_sha256') != ref['output_sha256']: raise ValueError('Frozen TIC output identity differs')
        packet = {**lineage.store.replay(manifest, retained), 'replay': ref}
        result = lineage.inspect(lineage.encoded(packet), retained, packet['generated_at'])
        return lineage.encoded({'packet': packet, 'originals': result})
    from calls_original_reader import ImmutableReader
    raw = read.verified_snapshot(identity, build) if isinstance(read, ImmutableReader) else build()
    return lineage.strict(raw)


def at_time(verified, as_of):
    result = deepcopy(verified['originals']); packet = verified['packet']; native = lineage.store.native
    now = native.clock(as_of); generated = native.clock(packet['generated_at']); issues = []
    if now < generated: raise ValueError('Future TIC source packet')
    if now-generated > timedelta(hours=26): issues.append('native_packet:publication_age')
    if packet['quality']['status'] != 'fresh': issues.append('native_packet:not_fresh')
    for m in packet['measurements'].values():
        quality = native.quality(m['as_of'], m['original']['acquired_at'], packet['release_calendar'], as_of,
                                 m['value_decimal'] is None, packet['cross_source_checks'][m['id']]['status'] == 'source_disagreement')
        if quality['status'] != 'fresh': issues.append(m['role']+':'+quality['status'])
    for name, stamp in packet['source_clocks'].items():
        age = now-native.clock(stamp)
        if age < timedelta(0): raise ValueError('Future TIC original acquisition')
        if age > timedelta(hours=26): issues.append(name+':acquisition_age')
    result.update(as_of=now.isoformat(), status='verified', current_use={'eligible': not issues, 'issues': sorted(set(issues))},
                  current_research=deepcopy(result['reported_headline']) if not issues else None,
                  canonical_packet_sha256=result.pop('packet_sha256'), hash_basis=BASIS)
    result.pop('publication_eligible', None); result.pop('candidate_only', None)
    return result


def capture(document, read, as_of):
    if not isinstance(document, dict) or not document: return unavailable('source_unavailable')
    if document.get('contract') != lineage.store.model.CONTRACT: return unavailable('unsupported_source_contract')
    try: ref = reference(document.get('replay'))
    except ValueError: return unavailable('invalid_original_reference')
    try:
        raw = lineage.encoded(document)
        if len(raw) > lineage.store.MAX_BYTES: raise ValueError('Complete bounded TIC packet required')
        verified = snapshot(ref, read)
        if lineage.encoded(verified['packet']) != raw: raise ValueError('Whole TIC packet differs')
        at_time(verified, as_of)
        return {'contract': CONTRACT, 'status': 'verified', 'reference': ref,
                'canonical_packet_sha256': hashlib.sha256(raw).hexdigest(), 'hash_basis': BASIS}
    except Exception: return unavailable('original_replay_failed')


def resolve(binding, read, as_of):
    if not isinstance(binding, dict) or binding.get('contract') != CONTRACT: raise ValueError('TIC binding contract required')
    if binding.get('status') == 'unavailable':
        if binding != unavailable(binding.get('reason')): raise ValueError('Unexpected failed TIC fields')
        return {'status': 'unavailable', 'reason': binding['reason'], 'calls_eligible': False, 'sizing_eligible': False}, None
    if (set(binding) != {'contract', 'status', 'reference', 'canonical_packet_sha256', 'hash_basis'} or
        binding.get('status') != 'verified' or binding.get('hash_basis') != BASIS or
        not isinstance(binding['canonical_packet_sha256'], str) or not re.fullmatch('[a-f0-9]{64}', binding['canonical_packet_sha256'])):
        raise ValueError('Complete frozen TIC original identity required')
    verified = snapshot(reference(binding['reference']), read)
    if lineage.digest(verified['packet']) != binding['canonical_packet_sha256']: raise ValueError('Whole frozen TIC packet differs')
    return at_time(verified, as_of), verified['packet']
