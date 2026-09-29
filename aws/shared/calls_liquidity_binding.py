"""Freeze original-source identity for Calls; failures never recover retroactively.

Only the reviewed public liquidity archive is read. No provider acquisition,
current-key fallback, account access, publication or investment action occurs.
"""
import hashlib,re
import calls_liquidity_originals as lineage

CONTRACT='calls-liquidity-binding.v1'
KEY='data/liquidity-flow.json'
FAILURES=('source_unavailable','unsupported_source_contract','invalid_original_reference','original_replay_failed')


def reader(client,bucket):
    # The outer reader rejects mutable/current/private paths before S3 access.
    return lineage.ImmutableReader(lineage.store.reader(client,bucket))


def reference(value):
    if not isinstance(value,dict) or set(value)!={'manifest_key','output_sha256'}:
        raise ValueError('Exact public liquidity replay reference required')
    if (not isinstance(value['manifest_key'],str) or not re.fullmatch(
            r'data/liquidity-flow-research/runs/[a-f0-9]{64}\.json',value['manifest_key'])
        or not isinstance(value['output_sha256'],str) or not re.fullmatch(r'[a-f0-9]{64}',value['output_sha256'])):
        raise ValueError('Exact immutable liquidity identity required')
    return dict(value)


def unavailable(reason):
    if reason not in FAILURES:raise ValueError('Unsupported original-source failure')
    return {'contract':CONTRACT,'status':'unavailable','reason':reason}


def capture(document,read,as_of):
    if not isinstance(document,dict) or not document:return unavailable('source_unavailable')
    if document.get('contract')!=lineage.store.model.CONTRACT:return unavailable('unsupported_source_contract')
    try:ref=reference(document.get('replay'))
    except ValueError:return unavailable('invalid_original_reference')
    try:
        raw=lineage.encoded(document)
        if len(raw)>lineage.store.MAX:raise ValueError('Complete bounded packet required')
        lineage.inspect(raw,lineage.ImmutableReader(read),as_of)
        return {'contract':CONTRACT,'status':'verified','reference':ref,
                'canonical_packet_sha256':hashlib.sha256(raw).hexdigest(),
                'hash_basis':'whole parsed public liquidity packet, canonical JSON; not transport bytes'}
    except Exception:return unavailable('original_replay_failed')


def resolve(binding,read,as_of):
    if not isinstance(binding,dict) or binding.get('contract')!=CONTRACT:raise ValueError('Original binding contract required')
    if binding.get('status')=='unavailable':
        if binding!=unavailable(binding.get('reason')):raise ValueError('Unexpected failed-capture fields')
        return {'status':'unavailable','reason':binding['reason'],'current_use':{'eligible':False},
                'calls_eligible':False,'sizing_eligible':False},None
    if (set(binding)!={'contract','status','reference','canonical_packet_sha256','hash_basis'}
        or binding.get('status')!='verified' or binding.get('hash_basis')!=
            'whole parsed public liquidity packet, canonical JSON; not transport bytes'
        or not isinstance(binding['canonical_packet_sha256'],str)
        or not re.fullmatch('[a-f0-9]{64}',binding['canonical_packet_sha256'])):
        raise ValueError('Complete frozen original-source identity required')
    ref=reference(binding['reference'])
    if read is None:raise ValueError('Original artifact reader required for this frozen run')
    retained=lineage.ImmutableReader(read)
    packet={**lineage.store.replay(ref,retained),'replay':ref}
    raw=lineage.encoded(packet)
    if hashlib.sha256(raw).hexdigest()!=binding['canonical_packet_sha256']:
        raise ValueError('Whole reconstructed source packet differs')
    result=lineage.inspect(raw,retained,as_of)
    # This digest explicitly describes canonical JSON, not an observed HTTP body.
    result['canonical_packet_sha256']=result.pop('packet_sha256')
    result['hash_basis']=binding['hash_basis']
    return {'status':'verified',**result},packet
