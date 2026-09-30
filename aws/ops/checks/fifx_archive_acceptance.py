"""Replay complete retained public FI/FX originals without a current pointer."""
from copy import deepcopy
import gzip
import hashlib
import io
import re
import threading
from term_premium_archive_acceptance import inventory, utc

MAX = 64 * 1024 * 1024


def complete(stream, expected=None, limit=MAX):
    """A short stream read is not EOF; a declared size must match all bytes."""
    try:
        if type(limit) is not int or limit <= 0:
            raise ValueError('Positive whole-byte bound required')
        if expected is not None and (type(expected) is not int or not 0 < expected <= limit):
            raise ValueError('Exact positive whole-byte length required')
        chunks, size = [], 0
        while True:
            chunk = stream.read(min(1024 * 1024, limit + 1 - size))
            if not isinstance(chunk, bytes):
                raise ValueError('Binary archive stream required')
            if not chunk:
                break
            chunks.append(chunk); size += len(chunk)
            if size > limit:
                raise ValueError('Whole archive exceeds reviewed bound')
        if size == 0 or expected is not None and size != expected:
            raise ValueError('Complete archive length differs')
        return b''.join(chunks)
    finally:
        stream.close()


class PublicArchiveReader:
    def __init__(self, client, bucket):
        self.client, self.bucket = client, bucket
        self.reads, self.lock = {}, threading.Lock()

    def __call__(self, key):
        allowed = (r'data/(?:fifx-vol-research/(?:runs|inputs|outputs|compilers|snapshots|series|views|originals|receipts)/[a-f0-9]{64}\.(?:json|py|bin)'
                   r'|report-research/(?:runs|inputs|outputs|compilers)/[a-f0-9]{64}\.(?:json|py)'
                   r'|evidence/fred/[a-f0-9]{64}/[a-f0-9]{64}\.bin\.gz)')
        if not isinstance(key, str) or not re.fullmatch(allowed, key):
            raise ValueError('Only content-addressed public FI/FX originals and canonical FRED dependencies may be read')
        response = self.client.get_object(Bucket=self.bucket, Key=key)
        length = response.get('ContentLength')
        if type(length) is not int or not 0 < length <= MAX:
            response['Body'].close()
            raise ValueError('Complete declared archive size required')
        stored = complete(response['Body'], length)
        raw = complete(gzip.GzipFile(fileobj=io.BytesIO(stored))) if key.endswith('.gz') else stored
        digest = hashlib.sha256(raw).hexdigest()
        if key.rsplit('/', 1)[1].split('.', 1)[0] != digest:
            raise ValueError('Complete archive content address differs')
        identity = {'key': key, 'bytes': len(raw), 'sha256': digest,
                    'stored_bytes': len(stored), 'stored_sha256': hashlib.sha256(stored).hexdigest()}
        with self.lock:
            if key in self.reads and self.reads[key] != identity:
                raise ValueError('Archive changed during acceptance')
            self.reads[key] = identity
        return raw


def inspect(client, bucket, store, model, *, cutoff, checked_at):
    lower, upper = utc(cutoff), utc(checked_at)
    if lower > upper:
        raise ValueError('Repair cutoff lies after acceptance clock')
    before = inventory(client, bucket, model.PREFIX)
    read = PublicArchiveReader(client, bucket)
    manifests, clocks = [], set()
    for row in before['entries']:
        raw = read(row['Key'])
        if len(raw) != row['Size']:
            raise ValueError('Manifest differs from complete listed size')
        manifest = store.strict(raw)
        if not isinstance(manifest, dict) or manifest.get('contract') != 'fifx-vol-replay.v1':
            raise ValueError('Original FI/FX run contract required')
        stamp = utc(manifest.get('generated_at'))
        if stamp > upper or stamp in clocks:
            raise ValueError('Future or conflicting same-clock original run')
        clocks.add(stamp); manifests.append({'key': row['Key'], 'manifest': manifest})
    result = {'status': 'pending_original_post_repair_archive', 'manifests': manifests,
              'cutoff': cutoff, 'checked_at': checked_at, 'replayed': False,
              'current_head_read': False, 'current_head_publication_verified': False,
              'schedule_causation_verified': False, 'investment_authority': False,
              'point_in_time_qualified': False}
    eligible = [entry for entry in manifests if utc(entry['manifest']['generated_at']) >= lower]
    if eligible:
        selected = max(eligible, key=lambda entry: utc(entry['manifest']['generated_at']))
        manifest = selected['manifest']
        view = store.checked(manifest['view'], 'views', read)
        packet = {**view, 'replay': {'manifest_key': selected['key'], 'view_sha256': manifest['view']['sha256']}}
        proofs = store.replay(packet, read)
        if (any(packet.get(key) is not False for key in store.catalog.AUTHORITY)
            or packet.get('decision') != {'verb': 'WAIT', 'meaning': 'abstain'}
            or packet.get('call') is not None or packet.get('regime') is not None
            or packet.get('signals') != [] or packet.get('portfolio_consequences', {}).get('status') != 'UNAVAILABLE'
            or packet['portfolio_consequences'].get('target_weights') is not None):
            raise ValueError('Retained research cannot acquire investment authority')
        recovery = {}
        for sid, row in packet['series'].items():
            if any(row.get(key) is not False for key in store.catalog.AUTHORITY):
                raise ValueError('A descriptive source cannot acquire investment authority')
            recovery[sid] = {key: deepcopy(row.get(key)) for key in (
                'quality', 'source_identity', 'retained_original_rows', 'retained_history_rows',
                'latest_reported', 'receipt', 'current_expires_at', 'complete_source_artifact')}
            recovery[sid].update(current_available_at_run=row['current'] is not None,
                                 acquisition=deepcopy(packet['acquisition']['sources'].get(sid)))
        result.update(status='complete_original_archive_replayed', replayed=True,
                      selected_manifest=selected['key'], generated_at=packet['generated_at'],
                      view_sha256=manifest['view']['sha256'], quality=deepcopy(packet['quality']),
                      acquisition=deepcopy(packet['acquisition']), source_recovery=recovery,
                      complete_arithmetic_proofs=proofs,
                      complete_source_recovery=packet['quality']['current_sources'] == len(store.catalog.SOURCES),
                      recovery_scope='Availability at the retained run clock; not current availability, causal schedule attribution, first-release vintage or investment qualification.')
    after = inventory(client, bucket, model.PREFIX)
    if before['entries'] != after['entries']:
        raise ValueError('Complete archive inventory changed during acceptance')
    result.update(inventory_before=before, inventory_after=after,
                  complete_artifacts_read=[read.reads[key] for key in sorted(read.reads)])
    return result
