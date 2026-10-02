"""Read only complete content-addressed public funding originals; no current key."""
from datetime import datetime, timezone, timedelta
from io import BytesIO
import re
from term_premium_archive_acceptance import inventory, utc


class PublicOriginals:
    def __init__(self, client, bucket, store):
        self.client, self.bucket, self.store = client, bucket, store
        self.reads = {}

    def get_object(self, **kwargs):
        key = kwargs.get('Key')
        if set(kwargs) != {'Bucket', 'Key'} or kwargs['Bucket'] != self.bucket or not isinstance(key, str):
            raise ValueError('Exact public funding original read required')
        allowed = re.escape(self.store.PREFIX) + r'(?:(?:runs|captures)/[a-f0-9]{64}\.json|compilers/[a-f0-9]{64}\.py)'
        if not re.fullmatch(allowed, key):
            raise ValueError('Only funding original content-addressed namespace may be read')
        response = self.client.get_object(**kwargs)
        stream, size = response['Body'], response.get('ContentLength')
        try:
            if type(size) is not int or not 0 < size <= self.store.MAX_BYTES:
                raise ValueError('Complete bounded original length required')
            raw, length = [], 0
            while length <= size:
                chunk = stream.read(min(65536, size + 1 - length))
                if not isinstance(chunk, bytes):
                    raise ValueError('Binary original required')
                if not chunk:
                    break
                raw.append(chunk); length += len(chunk)
            raw = b''.join(raw)
            if len(raw) != size or self.store.sha(raw) != key.rsplit('/', 1)[1].split('.')[0]:
                raise ValueError('Complete original bytes differ')
            modified = response.get('LastModified')
            if not isinstance(modified, datetime) or modified.tzinfo is None:
                raise ValueError('Native original storage clock required')
        finally:
            stream.close()
        identity = {'key': key, 'bytes': len(raw), 'sha256': self.store.sha(raw),
                    'stored_at': modified.astimezone(timezone.utc).isoformat()}
        if key in self.reads and self.reads[key] != identity:
            raise ValueError('Original changed during acceptance')
        self.reads[key] = identity
        return {'Body': BytesIO(raw), 'ContentLength': len(raw)}


def inspect(client, bucket, store, *, cutoff, checked_at):
    lower, upper = utc(cutoff), utc(checked_at)
    if lower > upper:
        raise ValueError('Repair cutoff after acceptance clock')
    before = inventory(client, bucket, store.PREFIX)
    reader = PublicOriginals(client, bucket, store)
    current = {name: store.reference('compilers', raw) for name, raw in store.compiler_bytes().items()}
    candidates, manifests, clocks = [], [], set()
    for item in before['entries']:
        key = item['Key']; digest = key.rsplit('/', 1)[1][:-5]
        ref = {'key': key, 'bytes': item['Size'], 'sha256': digest}
        manifest = store.strict(store.complete_read(reader, bucket, ref, 'runs'))
        if not isinstance(manifest, dict) or manifest.get('contract') != store.CONTRACT:
            raise ValueError('Named funding original manifest required')
        started = utc(manifest.get('acquisition_started_at'))
        finished = utc(manifest.get('acquisition_completed_at'))
        stored = utc(reader.reads[key]['stored_at'])
        # S3 LastModified is returned at whole-second precision. Treat it as
        # a one-second interval instead of inventing subsecond chronology.
        if started > finished or finished > upper or finished >= stored + timedelta(seconds=1) or stored > upper or (started, finished) in clocks:
            raise ValueError('Reversed, future or conflicting original clocks')
        clocks.add((started, finished))
        if item.get('LastModified') != datetime.fromisoformat(reader.reads[key]['stored_at']):
            raise ValueError('Listed and retrieved original storage clocks differ')
        row = {'reference': ref, 'acquisition_started_at': manifest['acquisition_started_at'],
               'acquisition_completed_at': manifest['acquisition_completed_at'],
               'stored_at': reader.reads[key]['stored_at'], 'storage_clock_resolution_seconds': 1,
               'matching_local_compilers': manifest.get('compilers') == current}
        manifests.append(row)
        if started >= lower and row['matching_local_compilers']:
            candidates.append(row)
    result = {'status': 'pending_matching_post_repair_original', 'replayed': False,
              'cutoff': cutoff, 'checked_at': checked_at, 'manifests': manifests,
              'current_head_read': False, 'current_head_publication_verified': False,
              'source_qualified': False, 'point_in_time_qualified': False,
              'schedule_causation_verified': False, 'investment_authority': False}
    if candidates:
        chosen = max(candidates, key=lambda row: utc(row['acquisition_completed_at']))
        packet = store.replay(reader, bucket, chosen['reference'])
        if any(utc(row['stored_at']) > upper for row in reader.reads.values()):
            raise ValueError('Original storage clock is later than acceptance')
        # Replay checks every byte, row, derived field and denied permission.
        result.update(status='complete_funding_original_replayed', replayed=True,
                      selected_manifest=chosen['reference'],
                      acquisition_started_at=chosen['acquisition_started_at'],
                      acquisition_completed_at=chosen['acquisition_completed_at'],
                      retained_attempts=len(packet['source_attempts']),
                      complete_responses=sum(a['response_complete'] for a in packet['source_attempts']),
                      observed_instruments=packet['observed_instruments'],
                      unavailable_instruments=packet['unavailable_instruments'],
                      selected_provider=packet['selected_provider'],
                      complete_replayed_packet_sha256=store.sha(store.encode(packet)),
                      recovery_scope='Retained source computation only; not current publication, authenticity, freshness, schedule causation or investment qualification.')
    after = inventory(client, bucket, store.PREFIX)
    if before['entries'] != after['entries']:
        raise ValueError('Complete original inventory changed during acceptance')
    result.update(inventory_before=before, inventory_after=after,
                  complete_artifacts_read=[reader.reads[key] for key in sorted(reader.reads)])
    return result
