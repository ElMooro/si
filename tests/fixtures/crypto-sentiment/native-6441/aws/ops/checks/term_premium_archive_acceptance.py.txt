"""Read-only Term Premium original archive replay. No current-pointer access."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import hashlib
import re

MAX_RUNS = 4096


def utc(value):
    if not isinstance(value, str):
        raise ValueError('Explicit UTC archive clock required')
    stamp = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset() != timedelta(0):
        raise ValueError('Explicit UTC archive clock required')
    return stamp


class PublicArchiveReader:
    def __init__(self, client, bucket, store, model):
        self.client, self.bucket, self.store, self.model = client, bucket, store, model
        self.reads = {}

    def __call__(self, key):
        suffix = r'(?:runs|inputs|outputs|tables|views|sources|compilers|originals)/[a-f0-9]{64}\.(?:json|py|xls)'
        if not isinstance(key, str) or not re.fullmatch(re.escape(self.model.PREFIX) + suffix, key):
            raise ValueError('Only content-addressed public Term Premium originals may be read')
        raw = self.store.stored(self.client.get_object(Bucket=self.bucket, Key=key))
        digest = hashlib.sha256(raw).hexdigest()
        if key.rsplit('/', 1)[1].split('.', 1)[0] != digest:
            raise ValueError('Complete archive content address differs')
        identity = {'key': key, 'bytes': len(raw), 'sha256': digest}
        if key in self.reads and self.reads[key] != identity:
            raise ValueError('Archive changed during acceptance')
        self.reads[key] = identity
        return raw


def inventory(client, bucket, prefix):
    """Retain every SDK listing page and row; never accept a truncated sample."""
    rows, pages, seen = [], [], set()
    for page in client.get_paginator('list_objects_v2').paginate(Bucket=bucket, Prefix=prefix + 'runs/'):
        if pages and pages[-1]['IsTruncated'] is False:
            raise ValueError('Archive listing continued past its declared end')
        if (page.get('Name') != bucket or page.get('Prefix') != prefix + 'runs/'
            or type(page.get('IsTruncated')) is not bool or page.get('CommonPrefixes')
            or type(page.get('KeyCount')) is not int or page['KeyCount'] != len(page.get('Contents', []))):
            raise ValueError('Complete exact-prefix archive listing required')
        if page['IsTruncated'] and not page.get('NextContinuationToken'):
            raise ValueError('Truncated listing lacks continuation evidence')
        pages.append(deepcopy(page))
        for item in page.get('Contents', []):
            key = item.get('Key')
            if not isinstance(key, str) or not re.fullmatch(re.escape(prefix) + r'runs/[a-f0-9]{64}\.json', key):
                raise ValueError('Unexpected archive inventory member')
            if key in seen:
                raise ValueError('Duplicate archive inventory member')
            if type(item.get('Size')) is not int or item['Size'] <= 0:
                raise ValueError('Complete nonempty archive size required')
            seen.add(key); rows.append(deepcopy(item))
            if len(rows) > MAX_RUNS:
                raise ValueError('Complete archive inventory exceeds reviewed bound; no partial acceptance')
    if not pages or pages[-1]['IsTruncated']:
        raise ValueError('Archive listing has no complete final page')
    return {'pages': pages, 'entries': sorted(rows, key=lambda row: row['Key'])}


def inspect(client, bucket, store, model, *, cutoff, checked_at):
    """Replay latest complete post-repair run using current reviewed compilers.

    A retained run proves retained computation, not current head publication.
    A conflicting same-clock manifest or changing inventory refuses acceptance.
    """
    lower, upper = utc(cutoff), utc(checked_at)
    if lower > upper:
        raise ValueError('Repair cutoff lies after acceptance clock')
    before = inventory(client, bucket, model.PREFIX)
    read = PublicArchiveReader(client, bucket, store, model)
    manifests, clocks = [], {}
    for row in before['entries']:
        raw = read(row['Key'])
        if len(raw) != row['Size']:
            raise ValueError('Manifest differs from complete listed size')
        manifest = store.strict(raw)
        if not isinstance(manifest, dict) or manifest.get('contract') != 'term-premium-replay.v1':
            raise ValueError('Original run contract required')
        stamp = utc(manifest.get('generated_at'))
        if stamp > upper:
            raise ValueError('Future native run cannot receive acceptance')
        if stamp in clocks:
            raise ValueError('Distinct native manifests share the same acquisition clock')
        clocks[stamp] = row['Key']
        manifests.append({'key': row['Key'], 'manifest': manifest})
    result = {'status': 'pending_original_post_repair_archive', 'manifests': manifests,
              'cutoff': cutoff, 'checked_at': checked_at, 'replayed': False,
              'current_head_read': False, 'current_head_publication_verified': False,
              'investment_authority': False, 'point_in_time_qualified': False}
    eligible = [row for row in manifests if utc(row['manifest']['generated_at']) >= lower]
    if eligible:
        selected = max(eligible, key=lambda row: utc(row['manifest']['generated_at']))
        manifest = selected['manifest']
        view = store.checked(manifest['view'], 'views', read)
        packet = {**view, 'replay': {'manifest_key': selected['key'], 'output_sha256': manifest['output_sha256']}}
        output = store.replay(packet, read)
        if any(output.get(key) is not False for key in model.arithmetic.AUTHORITY):
            raise ValueError('Research replay cannot acquire investment authority')
        tables = {name: {'rows': len(table['rows']), 'columns': len(table['headers']),
                        'first_observation': min(row['observation_date'] for row in table['rows']),
                        'last_observation': max(row['observation_date'] for row in table['rows'])}
                  for name, table in output['tables'].items()}
        result.update(status='complete_original_archive_replayed', replayed=True,
                      selected_manifest=selected['key'], generated_at=manifest['generated_at'],
                      output_sha256=model.digest(output), tables=tables, series=len(output['series']),
                      original_arithmetic_checks=output['original_arithmetic_checks'])
    after = inventory(client, bucket, model.PREFIX)
    if before['entries'] != after['entries']:
        raise ValueError('Complete archive inventory changed during acceptance')
    result.update(inventory_before=before, inventory_after=after,
                  complete_artifacts_read=[read.reads[key] for key in sorted(read.reads)])
    return result
