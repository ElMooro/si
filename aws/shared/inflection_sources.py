"""Original replay loaders shared by the Lambda and independent CLI."""
import hashlib
import json
from pathlib import Path
import re
import report_observations
import fred_vintage_model as vintage
from inflection_research_catalog import SERIES, ARCHIVES
from research_brief_model import digest


def compiler(ref, module, prefix, read):
    body = Path(module.__file__).read_bytes(); sha = hashlib.sha256(body).hexdigest()
    if ref != {'key': prefix + sha + '.py', 'sha256': sha} or read(ref['key']) != body:
        raise ValueError('original compiler differs from reviewed code')


def macro_originals(source, read):
    ref = source.get('replay') or {}; key = ref.get('manifest_key', '')
    if not re.fullmatch(r'data/report-research/runs/[a-f0-9]{64}\.json', key):
        raise ValueError('canonical source replay missing')
    manifest = json.loads(read(key))
    if key != 'data/report-research/runs/' + digest(manifest) + '.json': raise ValueError('macro manifest differs')
    compiler(manifest['compiler'], report_observations, 'data/report-research/compilers/', read)
    if ref.get('compiler_sha256') != manifest['compiler']['sha256'] or ref.get('output_sha256') != manifest['output_sha256']:
        raise ValueError('source reference differs')
    if digest({k: v for k, v in source.items() if k != 'replay'}) != manifest['output_sha256']:
        raise ValueError('source content differs')
    originals = {}
    for sid in SERIES:
        if sid not in source.get('measurements', {}): continue
        entry = manifest['inputs'][sid]
        item = {'evidence': entry['evidence'], 'acquired_at': entry['acquired_at']}
        for part, receipt in entry['evidence'].items():
            raw = read(receipt['key'])
            if len(raw) != receipt['bytes'] or hashlib.sha256(raw).hexdigest() != receipt['sha256']:
                raise ValueError('native original response differs')
            item[part] = json.loads(raw)
        originals[sid] = item
    return originals


def archive_originals(collection, read):
    if collection.get('contract') != 'fred-vintage-index.v1': raise ValueError('archive collection required')
    outputs = {}
    for sid in ARCHIVES:
        entry = collection.get('detail', {}).get(sid) or {}
        if entry.get('status') != 'source_replayed': continue
        if entry.get('key') != vintage.PREFIX + 'outputs/' + entry['sha256'] + '.json':
            raise ValueError('archive output path differs')
        doc = json.loads(read(entry['key'])); vintage.validate_packet(doc)
        if digest(doc) != entry['sha256'] or doc['series'] != sid or doc['collection_id'] != collection['collection_id']:
            raise ValueError('archive identity differs')
        key = doc['replay']['manifest_key']; manifest = json.loads(read(key))
        if key != vintage.PREFIX + 'runs/' + digest(manifest) + '.json' or manifest.get('contract') != 'fred-vintage-replay.v1':
            raise ValueError('complete nonsegmented balance-sheet replay required')
        compiler(manifest['compiler'], vintage, vintage.PREFIX + 'compilers/', read)
        def original(descriptor):
            return {**descriptor, 'raw': read(descriptor['evidence']['key'])}
        rebuilt = vintage.compile_series(sid, original(manifest['definition']), [original(d) for d in manifest['pages']],
            manifest['generated_at'], manifest['collection_id'], manifest['archive_end'], manifest['collection_started_at'],
            manifest.get('archive_start'))
        if rebuilt != {k: v for k, v in doc.items() if k != 'replay'} or digest(rebuilt) != manifest['output_sha256']:
            raise ValueError('archive original replay differs')
        outputs[sid] = doc
    return outputs
