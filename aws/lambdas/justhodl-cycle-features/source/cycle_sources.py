"""Complete native cycle acquisitions and offline compiler replay.

Stored caches are identified as stored caches, not first-publication originals.
Retention and deterministic replay grant no model, quote-clock or sizing authority.
"""
from datetime import datetime, timezone
from pathlib import Path
import urllib.error
import urllib.request
import time
import cycle_publication as pub

COMPILERS = ('lambda_function.py', 'cycle_publication.py', 'cycle_sources.py')
KEYS = frozenset([
    'data/warm/oecd/cycle/' + name + '.csv.gz' for name in ('DF_CLI', 'DF_KEI', 'DF_BTS', 'DF_CS', 'DF_FINMARK')
] + [
    'data/warm/bis/cycle/' + name + '.csv.gz' for name in ('WS_EER_M', 'WS_CBPOL_M')
] + [
    'data/warm/oecd/data/' + name + '.dat.gz' for name in ('DSD_KEI@DF_KEI', 'DSD_LFS@DF_IALFS_UNE_M')
] + [
    'data/warm/bis/data/' + name + '.dat.gz' for name in ('WS_TC', 'WS_SPP', 'WS_EER', 'WS_CREDIT_GAP', 'WS_DSR')
] + [
    'data/warm/eurostat/data/' + name + '.dat.gz' for name in ('EI_BSCO_M', 'EI_BSSI_M_R2', 'EI_BSIN_M_R2', 'EI_LMHR_M')
] + ['data/asia-leads.json', 'data/global-sovereign.json'])


class EvidenceError(RuntimeError):
    """A retention failure must escape every optional-source fallback."""


class InputError(ValueError):
    pass


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def compiler_hashes():
    return {name: pub.sha((Path(__file__).parent / name).read_bytes()) for name in COMPILERS}


def projection(packet):
    # Only operational timing/logging and the subsequently added self-references
    # are excluded. Every country, feature, value, date, source and permission stays.
    return {k: v for k, v in packet.items() if k not in (
        'elapsed_s', 'log', 'source_evidence', 'publication_context', 'feature_snapshot')}


def error_code(exc):
    code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
    return code if code in ('NoSuchKey', '404', 'AccessDenied', '403', 'SlowDown', 'InternalError') else type(exc).__name__


def response_bytes(response):
    headers = response.headers
    raw = pub.bounded(response)
    length = headers.get('Content-Length')
    if length is not None and (not str(length).isdigit() or int(length) != len(raw)):
        raise InputError('Incomplete HTTP representation')
    encoding = (headers.get('Content-Encoding') or 'identity').strip().lower()
    metadata = {'content_length': int(length) if length is not None else None,
                'content_encoding': encoding, 'content_type': headers.get('Content-Type'),
                'date': headers.get('Date'), 'last_modified': headers.get('Last-Modified')}
    return raw, metadata


def decode_http(raw, metadata):
    encoding = metadata['content_encoding']
    if encoding not in ('identity', 'gzip'):
        raise InputError('Unsupported HTTP content encoding')
    if encoding == 'gzip' and raw[:2] != b'\x1f\x8b':
        raise InputError('Declared gzip body is not gzip')
    # A compressed file can also be delivered as an identity HTTP entity.
    return pub.decoded(raw)


class Capture:
    def __init__(self, client, bucket, started_at, urls, opener=None, sleep=time.sleep):
        pub.clock(started_at)
        self.client, self.bucket, self.started_at = client, bucket, started_at
        self.urls = frozenset(urls)
        self.opener = opener or urllib.request.build_opener(NoRedirect()).open
        self.sleep = sleep
        self.events, self.clocks = [], []

    def retain(self, raw):
        try:
            return pub.retain(self.client, self.bucket, raw)
        except Exception as exc:
            raise EvidenceError('Complete cycle input retention failed') from exc

    def now(self):
        value = datetime.now(timezone.utc)
        self.clocks.append(value.isoformat())
        return value

    def get_bytes(self, key):
        if key not in KEYS:
            raise EvidenceError('Undeclared cycle input key')
        item = {'kind': 's3', 'key': key, 'requested_at': datetime.now(timezone.utc).isoformat()}
        self.events.append(item)
        try:
            obj = self.client.get_object(Bucket=self.bucket, Key=key)
            raw = pub.bounded(obj['Body'])
            if type(obj.get('ContentLength')) is not int or len(raw) != obj['ContentLength'] or not obj.get('ETag'):
                raise InputError('Incomplete or unversioned S3 representation')
            item['original'] = self.retain(raw)
            item.update(etag=obj['ETag'],
                        last_modified=pub.clock(obj['LastModified'].isoformat()).isoformat(),
                        received_at=datetime.now(timezone.utc).isoformat())
            if key.endswith('.gz') and raw[:2] != b'\x1f\x8b':
                raise InputError('Declared stored gzip is not gzip')
            body = pub.decoded(raw)
            item.update(status='complete', decoded_sha256=pub.sha(body), decoded_bytes=len(body))
            return body, pub.clock(item['last_modified'])
        except EvidenceError:
            raise
        except Exception as exc:
            item.update(status='unavailable', error='Cycle S3 input: ' + error_code(exc))
            raise InputError(item['error']) from exc

    def http_get(self, url, headers=None, timeout=90, retries=3, backoff=(15, 45, 90)):
        if url not in self.urls or retries != 3 or tuple(backoff) != (15, 45, 90):
            raise EvidenceError('Undeclared cycle request or retry policy')
        item = {'kind': 'http', 'url': url, 'attempts': [], 'timeout': timeout}
        self.events.append(item)
        last = None
        for i in range(retries):
            attempt = {'requested_at': datetime.now(timezone.utc).isoformat()}
            item['attempts'].append(attempt)
            try:
                request = urllib.request.Request(url, headers=headers or {
                    'User-Agent': 'JustHodl.AI cycle-features/1.0 (+https://justhodl.ai)'})
                try:
                    response = self.opener(request, timeout=timeout)
                except urllib.error.HTTPError as exc:
                    response = exc
                status = response.code if isinstance(response, urllib.error.HTTPError) else response.status
                raw, metadata = response_bytes(response)
                attempt.update(status_code=status, metadata=metadata,
                               original=self.retain(raw) if raw else None,
                               empty_body=not raw, received_at=datetime.now(timezone.utc).isoformat())
                if status != 200:
                    last = 'HTTP ' + str(status)
                    attempt['error'] = last
                    if status not in (429, 500, 502, 503, 504):
                        break
                else:
                    body = decode_http(raw, metadata)
                    attempt.update(status='complete', decoded_sha256=pub.sha(body), decoded_bytes=len(body))
                    item.update(status='complete', selected_attempt=i, result_status=status)
                    return body, status
            except EvidenceError:
                raise
            except Exception as exc:
                last = 'Cycle HTTP input: ' + type(exc).__name__
                attempt['error'] = last
            if i < retries - 1:
                self.sleep(backoff[min(i, len(backoff)-1)])
        item.update(status='unavailable', result_status=last)
        return None, last

    def finish(self, doc, manifest):
        if any(packet.get(k) is not False for packet in (doc, manifest) for k in pub.PERMISSIONS):
            raise EvidenceError('Source capture cannot grant cycle authority')
        value = {'contract': 'cycle-source-acquisition.v1', 'started_at': self.started_at,
                 'completed_at': datetime.now(timezone.utc).isoformat(),
                 'compiler_sha256': compiler_hashes(), 'events': self.events, 'clocks': self.clocks,
                 'output_projection_sha256': pub.sha(pub.encode(projection(doc))),
                 'manifest_projection_sha256': pub.sha(pub.encode(projection(manifest))),
                 'source_definitions_verified': False, 'first_publication_availability_verified': False,
                 'original_source_replay_verified': False}
        ref = self.retain(pub.encode(value))
        return {'contract': value['contract'], 'manifest': ref,
                'started_at': self.started_at, 'completed_at': value['completed_at'],
                'input_operations': len(self.events),
                'http_attempts': sum(len(e.get('attempts', [])) for e in self.events),
                'unavailable_operations': sum(e['status'] != 'complete' for e in self.events),
                'original_source_replay_verified': False, 'first_publication_availability_verified': False,
                'scope': 'Complete successful acquisitions and explicit failed attempts; stored caches are not original provider publication vintages.'}


class Replay:
    def __init__(self, manifest, read, urls):
        if manifest.get('contract') != 'cycle-source-acquisition.v1' or manifest.get('compiler_sha256') != compiler_hashes():
            raise EvidenceError('Exact checked-in cycle compiler required for replay')
        self.manifest, self.read, self.urls = manifest, read, frozenset(urls)
        self.position = self.clock_position = 0
        if 'clocks' in manifest:
            start, end = pub.clock(manifest['started_at']), pub.clock(manifest['completed_at'])
            clocks = [pub.clock(v) for v in manifest['clocks']]
            if not 0 <= (end-start).total_seconds() <= 600 or clocks != sorted(clocks) or any(v < start or v > end for v in clocks):
                raise EvidenceError('Cycle acquisition clocks differ')

    def original(self, ref):
        if ref['key'] != pub.PRIVATE + ref['sha256'] + '.bin' or not 0 < ref['bytes'] <= pub.LIMIT:
            raise EvidenceError('Protected complete cycle reference required')
        try:
            raw = self.read(ref['key'])
        except Exception as exc:
            raise EvidenceError('Retained cycle representation unavailable') from exc
        if len(raw) != ref['bytes'] or pub.sha(raw) != ref['sha256']:
            raise EvidenceError('Retained cycle representation differs')
        return raw

    def event(self, kind, field, target):
        events = self.manifest['events']
        if self.position >= len(events):
            raise EvidenceError('Unrecorded cycle input')
        event = events[self.position];self.position += 1
        if event['kind'] != kind or event[field] != target:
            raise EvidenceError('Cycle acquisition order or identity differs')
        return event

    def now(self):
        clocks = self.manifest['clocks']
        if self.clock_position >= len(clocks):
            raise EvidenceError('Unrecorded cycle processing clock')
        result = pub.clock(clocks[self.clock_position]);self.clock_position += 1
        if result < pub.clock(self.manifest['started_at']) or result > pub.clock(self.manifest['completed_at']):
            raise EvidenceError('Cycle processing clock outside acquisition interval')
        return result

    def get_bytes(self, key):
        if key not in KEYS:
            raise EvidenceError('Undeclared replay source')
        event = self.event('s3', 'key', key)
        raw = self.original(event['original']) if event.get('original') else None
        if event['status'] == 'unavailable':
            raise InputError(event['error'])
        try:
            body = pub.decoded(raw)
        except Exception as exc:
            raise EvidenceError('Retained complete cycle encoding differs') from exc
        if pub.sha(body) != event['decoded_sha256'] or len(body) != event['decoded_bytes'] or not event.get('etag'):
            raise EvidenceError('Stored cycle source differs')
        return body, pub.clock(event['last_modified'])

    def http_get(self, url, headers=None, timeout=90, retries=3, backoff=(15, 45, 90)):
        if url not in self.urls:
            raise EvidenceError('Undeclared replay URL')
        event = self.event('http', 'url', url)
        if timeout != event['timeout'] or retries != 3 or tuple(backoff) != (15, 45, 90) or not 1 <= len(event['attempts']) <= 3:
            raise EvidenceError('Cycle request policy differs')
        selected = None
        for i, attempt in enumerate(event['attempts']):
            raw = self.original(attempt['original']) if attempt.get('original') else b''
            if attempt.get('original') or attempt.get('empty_body'):
                size = attempt['metadata']['content_length']
                if size is not None and (type(size) is not int or size != len(raw)):
                    raise EvidenceError('HTTP representation length differs')
                if attempt.get('empty_body') != (not raw):
                    raise EvidenceError('HTTP empty-body classification differs')
            if attempt.get('status') == 'complete':
                body = decode_http(raw, attempt['metadata'])
                if attempt['status_code'] != 200 or pub.sha(body) != attempt['decoded_sha256'] or len(body) != attempt['decoded_bytes']:
                    raise EvidenceError('Complete HTTP cycle source differs')
                if selected is not None or i != len(event['attempts'])-1 or event.get('selected_attempt') != i:
                    raise EvidenceError('Cycle HTTP selected attempt differs')
                selected = body
        if event['status'] == 'complete':
            if selected is None or event['result_status'] != 200:
                raise EvidenceError('Cycle HTTP result differs')
            return selected, 200
        if selected is not None or not event['result_status']:
            raise EvidenceError('Unavailable cycle HTTP result differs')
        return None, event['result_status']

    def verify(self, doc, manifest):
        if self.position != len(self.manifest['events']) or self.clock_position != len(self.manifest['clocks']):
            raise EvidenceError('Unconsumed cycle acquisitions or clocks')
        for packet, field in ((doc, 'output_projection_sha256'), (manifest, 'manifest_projection_sha256')):
            if pub.sha(pub.encode(projection(packet))) != self.manifest[field]:
                raise EvidenceError('Complete cycle compiler projection differs')
        return {'countries': len(doc['countries']),
                'feature_series': sum(len(c['features']) for c in doc['countries'].values()),
                'observations': sum(sum(v is not None for v in f['values']) for c in doc['countries'].values() for f in c['features'].values()),
                'input_operations': self.position, 'processing_clocks': self.clock_position,
                'original_acquisition_arithmetic_replayed': True,
                'first_publication_availability_verified': False, 'model_qualified': False}


def replay_publication(env, doc, manifest, read):
    """Use the checked-in pure compiler with recorded I/O, never the handler."""
    evidence = doc.get('source_evidence')
    if not evidence or manifest.get('source_evidence') != evidence:
        raise EvidenceError('Matching cycle source evidence required')
    ref = evidence['manifest']
    if ref['key'] != pub.PRIVATE + ref['sha256'] + '.bin':
        raise EvidenceError('Protected acquisition manifest required')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or pub.sha(raw) != ref['sha256']:
        raise EvidenceError('Complete acquisition manifest differs')
    capture = pub.strict(raw)
    replay = Replay(capture, read, [env['CLI_URL']] + [v[0] for v in env['LANES'].values()])
    if (evidence['started_at'], evidence['completed_at'], evidence['input_operations'], evidence['http_attempts'], evidence['unavailable_operations']) != (
        capture['started_at'], capture['completed_at'], len(capture['events']),
        sum(len(e.get('attempts', [])) for e in capture['events']), sum(e['status'] != 'complete' for e in capture['events'])):
        raise EvidenceError('Public acquisition counts or clocks differ')
    if evidence.get('original_source_replay_verified') is not False or evidence.get('first_publication_availability_verified') is not False:
        raise EvidenceError('Producer cannot self-certify replay or publication history')
    for packet, field in ((doc, 'output_projection_sha256'), (manifest, 'manifest_projection_sha256')):
        if pub.sha(pub.encode(projection(packet))) != capture[field]:
            raise EvidenceError('Public cycle projection differs from capture')
    class CacheSink:
        def put_object(self, **kwargs):
            if kwargs['Key'] not in KEYS or '/cycle/' not in kwargs['Key']:
                raise EvidenceError('Replay attempted undeclared write')
            # No writes; validate the would-be cache body only.
            pub.decoded(kwargs['Body'])
    prior_audit, prior_s3 = env.get('AUDIT'), env['S3']
    env['AUDIT'], env['S3'] = replay, CacheSink()
    try:
        result, meta = env['compile_features'](pub.clock(capture['started_at']))
        return replay.verify(result, meta)
    finally:
        env['AUDIT'], env['S3'] = prior_audit, prior_s3
