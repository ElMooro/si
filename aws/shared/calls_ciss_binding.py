"""Frozen ECB originals, independent arithmetic and per-decision clock checks."""
from copy import deepcopy
from datetime import date, timedelta
import hashlib
from pathlib import Path
import re
import calls_ciss_originals as lineage

CONTRACT = 'calls-ciss-binding.v1'
KEY = 'data/ciss-stress.json'
BASIS = 'whole parsed public CISS packet, canonical JSON; not transport bytes'
FAILURES = ('source_unavailable', 'unsupported_source_contract', 'invalid_original_reference', 'original_replay_failed')


def reference(value):
    if not isinstance(value, dict) or set(value) != {'manifest_key', 'output_sha256', 'compiler'}:
        raise ValueError('Exact public CISS replay reference required')
    compiler = value['compiler']
    if (not isinstance(value['manifest_key'], str) or not re.fullmatch(lineage.IMMUTABLE[0], value['manifest_key']) or
        not isinstance(value['output_sha256'], str) or not re.fullmatch('[a-f0-9]{64}', value['output_sha256']) or
        not isinstance(compiler, dict) or set(compiler) != {'key', 'sha256'} or
        not isinstance(compiler['sha256'], str) or not re.fullmatch('[a-f0-9]{64}', compiler['sha256']) or
        compiler['key'] != 'data/ciss-research/compilers/'+compiler['sha256']+'.py'):
        raise ValueError('Exact immutable CISS reference required')
    return deepcopy(value)


def unavailable(reason):
    if reason not in FAILURES: raise ValueError('Unsupported CISS capture failure')
    return {'contract': CONTRACT, 'status': 'unavailable', 'reason': reason}


def snapshot(ref, read):
    if read is None: raise ValueError('Original artifact reader required for frozen CISS run')
    ref = reference(ref)
    # The cache is local to a reviewed reader and binds the complete compiler
    # closure, not an arbitrary upstream supplied verification result.
    modules = (lineage, lineage.replay, lineage.replay.model, lineage.arithmetic)
    identity = lineage.digest({'reference': ref, 'compilers': {
        Path(module.__file__).name: hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest() for module in modules}})
    def build():
        retained = lineage.ImmutableReader(read)
        manifest = lineage.strict_json(retained(ref['manifest_key']))
        if manifest.get('output_sha256') != ref['output_sha256'] or manifest.get('compiler') != ref['compiler']:
            raise ValueError('Frozen CISS source reference differs')
        packet = {**lineage.replay.replay(manifest, retained), 'replay': ref}
        raw = lineage.encoded(packet)
        result = lineage.inspect(raw, retained, packet['generated_at'])
        return lineage.encoded({'packet': packet, 'originals': result})
    from calls_original_reader import ImmutableReader
    raw = read.ciss_snapshot(identity, build) if isinstance(read, ImmutableReader) else build()
    return lineage.strict_json(raw)


def at_time(result, as_of):
    """A verified historical snapshot cannot renew its own source clocks."""
    result = deepcopy(result); now = lineage.arithmetic.clock(as_of)
    generated = lineage.arithmetic.clock(result['source_generated_at'])
    if now < generated: raise ValueError('Future CISS source packet')
    proof = result['independent_arithmetic']; views = result['series']
    for key, view in views.items():
        source = result['original_sources'][key]; row = proof['selected_series'][key]; issues = []
        if now-generated > timedelta(hours=72): issues.append('native_packet:publication_age')
        if now-lineage.arithmetic.clock(source['acquired_at']) > timedelta(hours=72): issues.append('original:acquisition_age')
        if (now.date()-date.fromisoformat(row['latest_period'])).days > 14: issues.append('original:observation_age')
        if row['source_quality'] != 'fresh': issues.append('native_series:not_fresh')
        if proof['latest_reconciliation'] != 'matched': issues.append('headline:same_date_reconciliation_unavailable')
        view['current_use'] = {'eligible': not issues, 'issues': issues}
        view['current_research'] = view['reported']['decimal'] if not issues else None
    joint = len(views) == 7 and all(view['current_use']['eligible'] for view in views.values())
    for view in views.values():
        if not joint and view['current_use']['eligible']:
            view['current_use'] = {'eligible': False, 'issues': ['headline:another_required_leg_unavailable']}
            view['current_research'] = None
    result.update(as_of=now.isoformat(), current_headline_research_eligible=joint, status='verified',
                  canonical_packet_sha256=result.pop('packet_sha256'), hash_basis=BASIS)
    result.pop('publication_eligible', None); result.pop('candidate_only', None)
    return result


def capture(document, read, as_of):
    if not isinstance(document, dict) or not document: return unavailable('source_unavailable')
    if document.get('contract') != lineage.replay.model.CONTRACT: return unavailable('unsupported_source_contract')
    try: ref = reference(document.get('replay'))
    except ValueError: return unavailable('invalid_original_reference')
    try:
        raw = lineage.encoded(document)
        if len(raw) > lineage.MAX_OBJECT: raise ValueError('Complete bounded packet required')
        verified = snapshot(ref, read)
        if lineage.encoded(verified['packet']) != raw: raise ValueError('Whole CISS source packet differs')
        at_time(verified['originals'], as_of)
        return {'contract': CONTRACT, 'status': 'verified', 'reference': ref,
                'canonical_packet_sha256': hashlib.sha256(raw).hexdigest(), 'hash_basis': BASIS}
    except Exception: return unavailable('original_replay_failed')


def resolve(binding, read, as_of):
    if not isinstance(binding, dict) or binding.get('contract') != CONTRACT: raise ValueError('CISS binding contract required')
    if binding.get('status') == 'unavailable':
        if binding != unavailable(binding.get('reason')): raise ValueError('Unexpected failed CISS fields')
        return {'status': 'unavailable', 'reason': binding['reason'], 'calls_eligible': False, 'sizing_eligible': False}, None
    if (set(binding) != {'contract', 'status', 'reference', 'canonical_packet_sha256', 'hash_basis'} or
        binding.get('status') != 'verified' or binding.get('hash_basis') != BASIS or
        not isinstance(binding['canonical_packet_sha256'], str) or not re.fullmatch('[a-f0-9]{64}', binding['canonical_packet_sha256'])):
        raise ValueError('Complete frozen CISS original identity required')
    verified = snapshot(reference(binding['reference']), read)
    if lineage.digest(verified['packet']) != binding['canonical_packet_sha256']: raise ValueError('Whole frozen CISS packet differs')
    return at_time(verified['originals'], as_of), verified['packet']
