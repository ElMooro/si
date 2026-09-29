"""Freeze complete FR2004 originals; publish a linked view of the Calls scopes.

Every original row and scope is replayed. The compact view links the immutable
full input/output archive and keeps the exact row identities behind the six
displayed fields. No history is sampled or interpreted as an independent vote.
"""
from copy import deepcopy
import hashlib
import re
import calls_fails_originals as lineage

CONTRACT = 'calls-fails-binding.v1'
KEY = 'data/settlement-fails.json'
BASIS = 'whole parsed public FR2004 packet, canonical JSON; not transport bytes'
FAILURES = ('source_unavailable', 'unsupported_source_contract', 'invalid_original_reference', 'original_replay_failed')


def reference(value):
    if (not isinstance(value, dict) or set(value) != {'manifest_key', 'output_sha256'} or
        not isinstance(value['manifest_key'], str) or not re.fullmatch(r'data/fails-research/runs/[a-f0-9]{64}\.json', value['manifest_key']) or
        not isinstance(value['output_sha256'], str) or not re.fullmatch('[a-f0-9]{64}', value['output_sha256'])):
        raise ValueError('Exact immutable FR2004 replay reference required')
    return dict(value)


def unavailable(reason):
    if reason not in FAILURES: raise ValueError('Unsupported FR2004 capture failure')
    return {'contract': CONTRACT, 'status': 'unavailable', 'reason': reason}


def capture(document, read, as_of):
    if not isinstance(document, dict) or not document: return unavailable('source_unavailable')
    if document.get('contract') != lineage.store.model.CONTRACT: return unavailable('unsupported_source_contract')
    try: ref = reference(document.get('replay'))
    except ValueError: return unavailable('invalid_original_reference')
    try:
        raw = lineage.encoded(document)
        lineage.inspect(raw, lineage.ImmutableReader(read), as_of)
        return {'contract': CONTRACT, 'status': 'verified', 'reference': ref,
                'canonical_packet_sha256': hashlib.sha256(raw).hexdigest(), 'hash_basis': BASIS}
    except Exception: return unavailable('original_replay_failed')


def compact(result):
    """Linked full history, with exact current-row occurrences; no hidden sample."""
    scopes = {key: {field: deepcopy(value) for field, value in result['scopes'][key].items() if field != 'history'}
              for key in ('ust_ex_tips', 'treasury_incl_tips')}
    identities = {identity for scope in scopes.values() for identity in scope['latest_calculation']['original_observations'].values()
                  if identity is not None}
    sources = {name: {'url': source['url'], 'acquired_at': source['acquired_at'],
                     'evidence': {key: source['evidence'][key] for key in
                         ('contract', 'captured', 'provider', 'source_url', 'sha256', 'key', 'bytes', 'first_received_at')}}
               for name, source in result['original_sources'].items()}
    return {**{key: deepcopy(value) for key, value in result.items() if key not in ('scopes', 'observations', 'packet_sha256', 'original_sources')},
            'status': 'verified', 'canonical_packet_sha256': result['packet_sha256'], 'hash_basis': BASIS,
            'scopes': scopes, 'original_sources': sources,
            'observations': {key: deepcopy(result['observations'][key]) for key in sorted(identities)},
            'display_scope': 'Two overlapping Calls scopes and exact latest original rows. All 12 series and all scope histories were replayed; complete histories remain in the linked immutable source output.',
            'complete_source_output_key': 'data/fails-research/outputs/'+result['source_replay']['output_sha256']+'.json'}


def resolve(binding, read, as_of):
    if not isinstance(binding, dict) or binding.get('contract') != CONTRACT: raise ValueError('FR2004 binding contract required')
    if binding.get('status') == 'unavailable':
        if binding != unavailable(binding.get('reason')): raise ValueError('Unexpected failed FR2004 capture fields')
        return {'status': 'unavailable', 'reason': binding['reason'], 'calls_eligible': False, 'sizing_eligible': False}, None
    if (set(binding) != {'contract', 'status', 'reference', 'canonical_packet_sha256', 'hash_basis'} or
        binding.get('status') != 'verified' or binding.get('hash_basis') != BASIS or
        not isinstance(binding['canonical_packet_sha256'], str) or not re.fullmatch('[a-f0-9]{64}', binding['canonical_packet_sha256'])):
        raise ValueError('Complete frozen FR2004 original identity required')
    ref = reference(binding['reference'])
    if read is None: raise ValueError('Original artifact reader required for this frozen FR2004 run')
    retained = lineage.ImmutableReader(read)
    manifest = lineage.store.native.strict_json(retained(ref['manifest_key']))
    if manifest.get('output_sha256') != ref['output_sha256']: raise ValueError('Frozen output reference differs')
    packet = {**lineage.store.replay(manifest, retained), 'replay': ref}
    raw = lineage.encoded(packet)
    if hashlib.sha256(raw).hexdigest() != binding['canonical_packet_sha256']: raise ValueError('Whole frozen FR2004 packet differs')
    return compact(lineage.inspect(raw, retained, as_of)), packet
