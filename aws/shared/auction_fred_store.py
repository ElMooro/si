"""Immutable public FRED originals and exact reviewed local replay; no HTTP.

Only the objects written/referenced by this source archive are read. No live
consumer packets, private data, invokes or provider requests belong here.
"""
from pathlib import Path
import gzip
import io
import re
import auction_fred_originals as model
import auction_cross_observations
import auction_benchmarks
import auction_calendar
import auction_originals
import treasury_instruments
import funding_research_catalog

MAX_ARTIFACT = 48 * 1024 * 1024
PROVIDER = 'fred'


def compiler_bytes():
    modules = (model, auction_cross_observations, auction_benchmarks, auction_calendar, auction_originals,
               treasury_instruments, funding_research_catalog)
    return {m.__name__: Path(m.__file__).read_bytes() for m in modules} | {'auction_fred_store': Path(__file__).read_bytes()}


def read_object(client, bucket, key):
    if not key.startswith((model.PREFIX, 'data/evidence/fred/')):
        raise ValueError('Public FRED source namespace required')
    limit = model.MAX_BODY + 65536 if key.startswith('data/evidence/fred/') else MAX_ARTIFACT
    stream = client.get_object(Bucket=bucket, Key=key)['Body']
    try: raw = stream.read(limit + 1)
    finally: stream.close()
    if not isinstance(raw, bytes) or not 0 < len(raw) <= limit:
        raise ValueError('Stored FRED artifact exceeds bound')
    return raw


def immutable(client, bucket, key, raw, kind='application/json'):
    if not isinstance(raw, bytes) or not 0 < len(raw) <= MAX_ARTIFACT:
        raise ValueError('Immutable FRED artifact size invalid')
    try: client.put_object(Bucket=bucket, Key=key, Body=raw, ContentType=kind, IfNoneMatch='*')
    except Exception as exc:
        code = str(getattr(exc, 'response', {}).get('Error', {}).get('Code', ''))
        if code not in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'): raise
    if read_object(client, bucket, key) != raw:
        raise ValueError('Immutable FRED artifact readback differs')


def reference(kind, raw):
    return {'key': model.PREFIX + kind + '/' + model.sha(raw) + '.json', 'sha256': model.sha(raw), 'bytes': len(raw)}


def load_reference(ref, kind, read):
    if (not isinstance(ref, dict) or set(ref) != {'key', 'sha256', 'bytes'}
        or not re.fullmatch('[a-f0-9]{64}', str(ref.get('sha256', '')))
        or ref['key'] != model.PREFIX + kind + '/' + ref['sha256'] + '.json'
        or type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX_ARTIFACT):
        raise ValueError('Exact typed FRED artifact reference required')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or model.sha(raw) != ref['sha256']:
        raise ValueError('FRED artifact bytes differ')
    return model.strict_json(raw)


def evidence_ref(frame, raw):
    transport = frame['adapter_transport']; url = model.public_url(frame['series_id'], frame['requested_limit'])
    digest = model.sha(raw)
    return {'key': f'data/evidence/{PROVIDER}/{model.sha(url.encode())}/{digest}.bin.gz',
            'sha256': digest, 'bytes': len(raw), 'source_url': url,
            'response_received_at': transport['response_received_at']}


def original(record, read):
    if not isinstance(record, dict) or set(record) != {'frame', 'evidence'}:
        raise ValueError('Exact FRED source descriptor required')
    frame, receipt = record['frame'], record['evidence']
    if frame.get('read_status') == 'unavailable':
        if receipt is not None: raise ValueError('Unavailable request cannot have an original')
        return {'frame': frame, 'raw': None}
    if not isinstance(receipt, dict) or type(receipt.get('bytes')) is not int or not 0 < receipt['bytes'] <= model.MAX_BODY:
        raise ValueError('Bounded original FRED reference required')
    transport = frame.get('adapter_transport', {})
    sid, limit = frame.get('series_id'), frame.get('requested_limit')
    url = model.public_url(sid, limit); digest = transport.get('body_sha256')
    if not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest):
        raise ValueError('Original FRED body identity absent')
    expected = {'key': f'data/evidence/{PROVIDER}/{model.sha(url.encode())}/{digest}.bin.gz',
                'sha256': digest, 'bytes': transport.get('body_bytes'), 'source_url': url,
                'response_received_at': transport.get('response_received_at')}
    if receipt != expected: raise ValueError('Original FRED receipt identity differs')
    zipped = read(expected['key'])
    if len(zipped) > model.MAX_BODY + 65536: raise ValueError('Compressed FRED body exceeds bound')
    try:
        with gzip.GzipFile(fileobj=io.BytesIO(zipped)) as stream: raw = stream.read(model.MAX_BODY + 1)
    except (OSError, EOFError) as exc: raise ValueError('FRED gzip body corrupt') from exc
    if len(raw) != receipt['bytes'] or model.sha(raw) != digest: raise ValueError('Original FRED bytes differ')
    return {'frame': frame, 'raw': raw}


def replay(ref, read):
    manifest = load_reference(ref, 'runs', read)
    if manifest.get('contract') != model.CONTRACT: raise ValueError('FRED replay contract differs')
    sources = compiler_bytes()
    if set(manifest.get('compilers', {})) != set(sources): raise ValueError('Complete FRED compiler closure required')
    for name, raw in sources.items():
        expected = {'key': model.PREFIX + 'compilers/' + model.sha(raw) + '.py', 'sha256': model.sha(raw), 'bytes': len(raw)}
        if manifest['compilers'][name] != expected or read(expected['key']) != raw:
            raise ValueError('Reviewed FRED compiler differs; use matching checkout')
    sources = manifest['sources']
    if not isinstance(sources, dict) or not set(sources) <= set(model.LIMITS): raise ValueError('FRED request set differs')
    records = {sid: original(record, read) for sid, record in sources.items()}
    output = model.build(records, manifest['calculation_as_of'], manifest['generated_at'])
    if model.encoded(output) != model.encoded(load_reference(manifest['output'], 'measurements', read)):
        raise ValueError('Complete FRED measurement replay differs')
    return output


def retain(client, bucket, records, calculation_as_of, generated_at, native_inputs):
    output = model.build(records, calculation_as_of, generated_at)
    expected = {'fed_funds_rate': output['fed_funds']['value'], 'cross_signals': output['cross_signals'],
                'benchmark_histories': output['benchmark_histories']}
    if model.encoded(native_inputs) != model.encoded(expected):
        raise ValueError('Native FRED inputs differ from complete original replay')
    descriptors = {}
    for sid, record in records.items():
        raw, frame = record['raw'], record['frame']; receipt = None
        if raw is not None:
            receipt = evidence_ref(frame, raw); compressed = gzip.compress(raw, mtime=0)
            # Existing content-addressed originals may have equivalent gzip bytes
            # from another writer. Validate the bounded original, not its codec.
            try:
                client.put_object(Bucket=bucket, Key=receipt['key'], Body=compressed,
                                  ContentType='application/gzip', IfNoneMatch='*', Metadata={'sha256': receipt['sha256'], 'received_at': receipt['response_received_at'], 'provider': PROVIDER, 'source_url': receipt['source_url'], 'contract': 'source-evidence.v1'})
            except Exception as exc:
                if str(getattr(exc, 'response', {}).get('Error', {}).get('Code', '')) not in ('PreconditionFailed', '412', 'ConditionalRequestConflict', '409'): raise
            original({'frame': frame, 'evidence': receipt}, lambda key: read_object(client, bucket, key))
        descriptors[sid] = {'frame': frame, 'evidence': receipt}
    compilers = {}
    for name, raw in compiler_bytes().items():
        key = model.PREFIX + 'compilers/' + model.sha(raw) + '.py'
        immutable(client, bucket, key, raw, 'text/plain')
        compilers[name] = {'key': key, 'sha256': model.sha(raw), 'bytes': len(raw)}
    raw = model.encoded(output); output_ref = reference('measurements', raw)
    immutable(client, bucket, output_ref['key'], raw)
    manifest = {'contract': model.CONTRACT, 'generated_at': generated_at, 'calculation_as_of': calculation_as_of,
                'sources': descriptors, 'compilers': compilers, 'output': output_ref}
    raw = model.encoded(manifest); manifest_ref = reference('runs', raw)
    immutable(client, bucket, manifest_ref['key'], raw)
    replayed = replay(manifest_ref, lambda key: read_object(client, bucket, key))
    if model.encoded(replayed) != model.encoded(output): raise ValueError('Before-publication FRED replay differs')
    status = ('complete' if output['coverage']['complete_request_set'] else
              'partial' if output['coverage']['received_series'] else 'unavailable')
    return {'contract': model.CONTRACT, 'status': status,
            'generated_at': generated_at, 'calculation_as_of': calculation_as_of, 'manifest': manifest_ref,
            'measurements': output_ref, 'coverage': output['coverage'], 'fed_funds': output['fed_funds'],
            'measurement_coverage': output['measurement_coverage'],
            'original_bytes_replayed': bool(output['coverage']['received_series']), 'native_input_selection_replayed': True,
            'whole_auction_model_replayed': False, **model.PERMISSIONS}
