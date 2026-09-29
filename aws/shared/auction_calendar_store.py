"""Immutable upcoming-auction source capture; exact reviewed compiler replay.

Native source writes only. No provider requests, current consumer reads or invokes.
"""
from datetime import datetime, timezone
from pathlib import Path
import gzip
import io
import re
import auction_calendar as model
import auction_benchmarks
import auction_originals
import auction_original_store
import treasury_instruments
import evidence_store

PROVIDER = 'treasury-upcoming'
MAX_ARTIFACT = auction_original_store.MAX_ARTIFACT
storage_read = auction_original_store.storage_read
immutable = auction_original_store.immutable


def compiler_bytes():
    modules = (model, auction_benchmarks, auction_originals, auction_original_store, treasury_instruments, evidence_store)
    return {module.__name__:Path(module.__file__).read_bytes() for module in modules} | {
        'auction_calendar_store':Path(__file__).read_bytes()}


def reference(kind, raw):
    return {'key':model.PREFIX+kind+'/'+model.sha(raw)+'.json','sha256':model.sha(raw),'bytes':len(raw)}


def load_reference(ref, kind, read):
    if not isinstance(ref, dict) or set(ref) != {'key','sha256','bytes'} or not re.fullmatch('[a-f0-9]{64}', str(ref.get('sha256',''))):
        raise ValueError('Exact calendar reference required')
    if ref['key'] != model.PREFIX+kind+'/'+ref['sha256']+'.json' or type(ref['bytes']) is not int or not 0 < ref['bytes'] <= MAX_ARTIFACT:
        raise ValueError('Calendar artifact identity differs')
    raw = read(ref['key'])
    if len(raw) != ref['bytes'] or model.sha(raw) != ref['sha256']:
        raise ValueError('Calendar artifact bytes differ')
    return model.strict_json(raw)


def original(frame, generated_at, read):
    receipt = frame.get('evidence') or {}
    if frame.get('request_url') != model.URL or frame.get('response_url') != model.URL:
        raise ValueError('Exact calendar response identity required')
    source_url = evidence_store.public_source_url(model.URL)
    if receipt.get('source_url') != source_url or receipt.get('contract') != 'source-evidence.v1' or receipt.get('captured') is not True or receipt.get('provider') != PROVIDER:
        raise ValueError('Calendar receipt identity differs')
    digest = receipt.get('sha256')
    if not isinstance(digest,str) or not re.fullmatch('[a-f0-9]{64}',digest):
        raise ValueError('Calendar original hash missing')
    expected = f'data/evidence/{PROVIDER}/{model.sha(source_url.encode())}/{digest}.bin.gz'
    if receipt.get('key') != expected or type(receipt.get('bytes')) is not int or not 0 < receipt['bytes'] <= model.MAX_BYTES:
        raise ValueError('Calendar original namespace or size differs')
    if not model.clock(receipt['first_received_at']) <= model.clock(frame['acquired_at']) <= model.clock(generated_at):
        raise ValueError('Calendar original receipt clocks differ')
    compressed = read(expected)
    if len(compressed) > model.MAX_BYTES+65536:
        raise ValueError('Compressed calendar exceeds bound')
    with gzip.GzipFile(fileobj=io.BytesIO(compressed)) as stream:
        raw = stream.read(model.MAX_BYTES+1)
    if len(raw) != receipt['bytes'] or model.sha(raw) != digest:
        raise ValueError('Calendar original bytes differ')
    return {key:value for key,value in frame.items() if key!='evidence'} | {'raw':raw}


def replay(ref, read):
    manifest = load_reference(ref, 'runs', read)
    if manifest.get('contract') != model.REPLAY_CONTRACT:
        raise ValueError('Calendar replay contract differs')
    sources = compiler_bytes()
    if set(manifest.get('compilers',{})) != set(sources):
        raise ValueError('Whole calendar compiler closure required')
    for name, raw in sources.items():
        expected = {'key':model.PREFIX+'compilers/'+model.sha(raw)+'.py','sha256':model.sha(raw),'bytes':len(raw)}
        if manifest['compilers'][name] != expected or read(expected['key']) != raw:
            raise ValueError('Reviewed calendar compiler differs; use matching checkout')
    frame = original(manifest['source'], manifest['generated_at'], read)
    window = manifest['request_window']
    output = model.build(frame, window['start'], window['days_ahead'], manifest['generated_at'])
    if window != output['request_window'] or model.encoded(output) != model.encoded(load_reference(manifest['output'],'measurements',read)):
        raise ValueError('Complete calendar source replay differs')
    return output


def retain(client, bucket, frame, days_ahead=30, generated_at=None):
    generated_at = generated_at or datetime.now(timezone.utc).isoformat()
    output = model.build(frame, model.clock(generated_at).date().isoformat(), days_ahead, generated_at)
    receipt = evidence_store.capture(client, bucket, PROVIDER, frame['request_url'], frame['raw'], model.clock(frame['acquired_at']))
    compilers = {}
    for name, raw in compiler_bytes().items():
        key = model.PREFIX+'compilers/'+model.sha(raw)+'.py'
        immutable(client,bucket,key,raw,'text/plain')
        compilers[name] = {'key':key,'sha256':model.sha(raw),'bytes':len(raw)}
    raw = model.encoded(output);output_ref = reference('measurements',raw)
    immutable(client,bucket,output_ref['key'],raw)
    manifest = {'contract':model.REPLAY_CONTRACT, 'generated_at':generated_at,
                'request_window':output['request_window'], 'output':output_ref, 'compilers':compilers,
                'source':{key:value for key,value in frame.items() if key!='raw'} | {'evidence':receipt}}
    raw = model.encoded(manifest);manifest_ref = reference('runs',raw)
    immutable(client,bucket,manifest_ref['key'],raw)
    replayed = replay(manifest_ref, lambda key:storage_read(client,bucket,key))
    if model.encoded(output) != model.encoded(replayed):
        raise ValueError('Calendar changed before publication')
    return {'contract':model.REPLAY_CONTRACT,'generated_at':generated_at,'status':output['status'],
            'request_window':output['request_window'],'coverage':output['coverage'],
            'manifest':manifest_ref,'measurements':output_ref,'original_bytes_replayed':True,
            'selection_replayed':True, **model.PERMISSIONS}, output['selected_auctions']
