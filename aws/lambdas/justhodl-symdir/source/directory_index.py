"""Coherent directory generations and disk-backed refresh rollback.

No network or credentials are created here. Callers supply the existing S3 client
and the existing token builder. Legacy docs are reindexed, never paired with an
unbound legacy postings head. This is storage consistency, not data qualification.
"""
from array import array
from contextlib import closing
from datetime import datetime, timezone
from pathlib import Path
import gc
import gzip
import hashlib
import json
import math
import pickle
import re
import shutil
import tempfile
import time

FIELDS = ('docs', 'pop', 'index', 'toklist', 'ids', 'bare')
RESERVE = 64 * 1024 * 1024
CHUNK = 1024 * 1024


class IndexIntegrityError(ValueError):
    pass


class RestrictedUnpickler(pickle.Unpickler):
    def find_class(self, module, name):
        if module == 'array' and name in ('array', '_array_reconstructor'):
            import array as arrays
            return getattr(arrays, name)
        raise IndexIntegrityError('Unsupported directory pickle class')


def unpack(path):
    with gzip.open(path, 'rb') as stream:
        value = RestrictedUnpickler(stream).load()
        # Consume the trailer so CRC/truncation or appended data cannot be hidden
        # behind a valid pickle prefix.
        if stream.read(1):
            raise IndexIntegrityError('Trailing directory pickle bytes')
    return value


def descriptor(docs_body, index_body, prefix):
    values = [hashlib.sha256(body).hexdigest() for body in (docs_body, index_body)]
    generation = hashlib.sha256((':'.join(values)).encode('ascii')).hexdigest()
    return {'schema_version': 1, 'generation': generation,
            'files': {name: {'key': prefix + 'generations/' + generation + '/' + name + '.pkl.gz',
                             'sha256': digest, 'bytes': len(body)}
                      for name, body, digest in zip(('docs', 'index'), (docs_body, index_body), values)}}


def validate_descriptor(value, prefix):
    if not isinstance(value, dict) or value.get('schema_version') != 1 or type(value.get('schema_version')) is not int:
        raise IndexIntegrityError('Unknown directory generation schema')
    generation = value.get('generation')
    if not isinstance(generation, str) or not re.fullmatch('[a-f0-9]{64}', generation):
        raise IndexIntegrityError('Invalid directory generation identity')
    files = value.get('files')
    if not isinstance(files, dict) or set(files) != {'docs', 'index'}:
        raise IndexIntegrityError('Complete directory artifact pair required')
    hashes = []
    for name in ('docs', 'index'):
        item = files[name]
        if not isinstance(item, dict) or item.get('key') != prefix + 'generations/' + generation + '/' + name + '.pkl.gz':
            raise IndexIntegrityError('Directory artifact key is not generation-bound')
        if type(item.get('bytes')) is not int or item['bytes'] <= 0:
            raise IndexIntegrityError('Positive directory artifact size required')
        digest = item.get('sha256')
        if not isinstance(digest, str) or not re.fullmatch('[a-f0-9]{64}', digest):
            raise IndexIntegrityError('Directory artifact hash required')
        hashes.append(digest)
    if hashlib.sha256((':'.join(hashes)).encode('ascii')).hexdigest() != generation:
        raise IndexIntegrityError('Directory generation digest differs')
    return files


def download(client, bucket, key, path, expected=None):
    response = client.get_object(Bucket=bucket, Key=key)
    digest = hashlib.sha256()
    length = 0
    with closing(response['Body']) as body, open(path, 'xb') as target:
        # Leave headroom for the other independent /tmp warehouse. If staging
        # cannot fit, fail while the in-memory cache is still untouched.
        declared = response.get('ContentLength')
        if declared is not None and (type(declared) is not int or declared < 0):
            raise IndexIntegrityError('Invalid directory transport length')
        if expected and declared is not None and declared != expected['bytes']:
            raise IndexIntegrityError('Directory transport length differs')
        while True:
            chunk = body.read(CHUNK)
            if not chunk:
                break
            length += len(chunk)
            if expected and length > expected['bytes']:
                raise IndexIntegrityError('Directory artifact is overlong')
            if shutil.disk_usage(path.parent).free < len(chunk) + RESERVE:
                raise IndexIntegrityError('Insufficient directory staging space')
            target.write(chunk)
            digest.update(chunk)
    if declared is not None and length != declared:
        raise IndexIntegrityError('Truncated directory transport')
    if expected and (length != expected['bytes'] or digest.hexdigest() != expected['sha256']):
        raise IndexIntegrityError('Directory artifact digest or size differs')
    return {'bytes': length, 'sha256': digest.hexdigest()}


def manifest(client, bucket, prefix):
    response = client.get_object(Bucket=bucket, Key=prefix + 'manifest.json')
    with closing(response['Body']) as body:
        raw = body.read(1024 * 1024 + 1)
    if len(raw) > 1024 * 1024:
        raise IndexIntegrityError('Directory manifest exceeds size bound')
    size = response.get('ContentLength')
    if size is not None and (type(size) is not int or size != len(raw)):
        raise IndexIntegrityError('Directory manifest transport length differs')
    def pairs(items):
        result = {}
        for key, value in items:
            if key in result:
                raise IndexIntegrityError('Duplicate directory manifest field')
            result[key] = value
        return result
    def invalid_constant(value):
        raise IndexIntegrityError('Nonfinite directory manifest number')
    value = json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid_constant)
    if not isinstance(value, dict):
        raise IndexIntegrityError('Directory manifest object required')
    return value


def clock(value):
    if type(value) is not str:
        raise IndexIntegrityError('Directory generation clock required')
    try:
        parsed = datetime.fromisoformat(value.replace('Z', '+00:00'))
        if parsed.tzinfo is None:
            raise ValueError('Timezone required')
        return parsed.astimezone(timezone.utc)
    except (ValueError, OverflowError):
        raise IndexIntegrityError('Invalid directory generation clock') from None


def refresh_needed(head, cache, prefix):
    """Check a whole head without making its timestamp a generation identity."""
    incoming_clock = clock(head.get('built_at'))
    current_clock = clock(cache.get('built_at'))
    if incoming_clock < current_clock:
        raise IndexIntegrityError('Directory manifest clock regressed')
    generation = head.get('index_generation')
    current_generation = (cache.get('index_integrity') or {}).get('generation')
    if generation is not None:
        validate_descriptor(generation, prefix)
        return generation['generation'] != current_generation or incoming_clock != current_clock
    if current_generation is not None:
        raise IndexIntegrityError('Bound directory manifest replaced by an unbound head')
    return incoming_clock > current_clock


def validate_docs(value):
    if not isinstance(value, dict) or not isinstance(value.get('docs'), list):
        raise IndexIntegrityError('Directory document population required')
    docs, pop = value['docs'], value.get('pop')
    if not isinstance(pop, array) or pop.typecode != 'f' or len(pop) != len(docs):
        raise IndexIntegrityError('Directory popularity population differs')
    clock(value.get('built_at'))
    for row, score in zip(docs, pop):
        if not isinstance(row, (list, tuple)) or len(row) != 12:
            raise IndexIntegrityError('Malformed directory document')
        if not all(type(row[i]) is str for i in (0, 1, 2, 3)) or not row[0]:
            raise IndexIntegrityError('Directory document identity required')
        if row[9] is not None and type(row[9]) is not str:
            raise IndexIntegrityError('Invalid directory source key')
        if row[11] is not None and not isinstance(row[11], dict):
            raise IndexIntegrityError('Invalid directory descriptive fields')
        if type(row[4]) not in (int, float) or not math.isfinite(row[4]) or not 0 <= row[4] <= 1:
            raise IndexIntegrityError('Invalid directory popularity')
        if not math.isfinite(score) or array('f', [row[4]])[0] != score:
            raise IndexIntegrityError('Directory popularity differs from document')


def validate_index(docs, value):
    if not isinstance(value, dict) or not isinstance(value.get('index'), dict):
        raise IndexIntegrityError('Directory postings required')
    n = len(docs)
    index = value['index']
    if value.get('toklist') != sorted(index):
        raise IndexIntegrityError('Directory token dictionary differs')
    for key, rows in index.items():
        if type(key) is not str or not isinstance(rows, array) or rows.typecode != 'I':
            raise IndexIntegrityError('Typed directory postings required')
        last = -1
        for row in rows:
            if not last < row < n:
                raise IndexIntegrityError('Invalid directory posting ordinal')
            last = row
    # Avoid allocating another complete sorted id population while loading.
    for field, qualifying in (('ids', False), ('bare', True)):
        rows = value.get(field)
        if not isinstance(rows, list):
            raise IndexIntegrityError('Directory identity mappings required')
        expected_count = sum(1 for d in docs if ':' in d[0]) if qualifying else n
        if len(rows) != expected_count:
            raise IndexIntegrityError('Directory identity mapping coverage differs')
        seen = bytearray(n)
        last = None
        for pair in rows:
            if not isinstance(pair, tuple) or len(pair) != 2 or type(pair[0]) is not str or type(pair[1]) is not int:
                raise IndexIntegrityError('Typed directory identity mapping required')
            ident, ordinal = pair
            if not 0 <= ordinal < n or seen[ordinal] or (last is not None and pair <= last):
                raise IndexIntegrityError('Duplicate, unsorted or invalid directory identity mapping')
            source_id = docs[ordinal][0]
            if qualifying and ':' not in source_id:
                raise IndexIntegrityError('Unqualified directory bare identity')
            expected = source_id.rsplit(':', 1)[-1].upper() if qualifying else source_id.upper()
            if ident != expected:
                raise IndexIntegrityError('Directory identity points to another document')
            seen[ordinal] = 1
            last = pair


class SpaceCheckedWriter:
    def __init__(self, stream, path):
        self.stream, self.path = stream, path

    def write(self, value):
        if shutil.disk_usage(self.path.parent).free < len(value) + RESERVE:
            raise IndexIntegrityError('Insufficient directory rollback space')
        return self.stream.write(value)

    def flush(self):
        return self.stream.flush()


def refresh(cache, client, bucket, prefix, rebuild, iso, temp_root=None):
    started = time.monotonic()
    with tempfile.TemporaryDirectory(prefix='jh-symdir-', dir=temp_root) as temporary:
        folder = Path(temporary)
        head = manifest(client, bucket, prefix)
        generation = head.get('index_generation')
        files = validate_descriptor(generation, prefix) if generation is not None else None
        if files is None:
            # A legacy index has no provable link to its docs. Reconstruct only
            # the derived structures from that complete single docs object.
            files = {'docs': {'key': prefix + 'docs.pkl.gz'}}
        hashes = {}
        for name, metadata in files.items():
            hashes[name] = download(client, bucket, metadata['key'], folder / (name + '.gz'),
                                    metadata if generation is not None else None)
        had_cache = cache.get('docs') is not None
        rollback = folder / 'rollback.gz'
        if had_cache:
            with open(rollback, 'xb') as target:
                with gzip.GzipFile(fileobj=SpaceCheckedWriter(target, rollback), mode='wb', compresslevel=1) as stream:
                    pickle.dump(cache, stream, protocol=5)
            # Verify a complete compression stream without decoding another
            # object graph before trusting this local rollback copy.
            with gzip.open(rollback, 'rb') as stream:
                while stream.read(CHUNK):
                    pass
        # All acquisition and rollback serialization finish before releasing
        # the old graph. Never hold both decoded generations simultaneously.
        metadata = {k: v for k, v in cache.items() if k not in FIELDS}
        for key in FIELDS:
            cache[key] = None
        gc.collect()
        candidate = derived = result = None
        try:
            candidate = unpack(folder / 'docs.gz')
            validate_docs(candidate)
            loaded_at = iso()
            if clock(candidate['built_at']) > clock(loaded_at):
                raise IndexIntegrityError('Directory generation clock is in the future')
            if generation is None:
                derived = rebuild(candidate['docs'])
                binding = 'legacy_reconstructed_from_single_docs_object'
            else:
                derived = unpack(folder / 'index.gz')
                if (derived.get('docs_sha256') != hashes['docs']['sha256']
                        or derived.get('built_at') != candidate['built_at']
                        or derived.get('version') != candidate.get('version')
                        or head.get('built_at') != candidate['built_at']):
                    raise IndexIntegrityError('Directory artifacts describe different generations')
                binding = 'hash_bound_generation'
            validate_index(candidate['docs'], derived)
            if had_cache and clock(candidate['built_at']) < clock(metadata.get('built_at')):
                raise IndexIntegrityError('Directory refresh would regress its generation')
            result = {'docs': candidate['docs'], 'pop': candidate['pop'],
                      **{key: derived[key] for key in FIELDS[2:]},
                      'loaded_at': loaded_at, 'built_at': candidate['built_at'],
                      'load_s': round(time.monotonic() - started, 2),
                      'index_integrity': {'status': binding, 'generation': generation['generation'] if generation else None,
                                          'docs_sha256': hashes['docs']['sha256'], 'investment_authority': False}}
            cache.clear()
            cache.update(result)
            return cache
        except Exception as exc:
            failure = str(exc) if isinstance(exc, IndexIntegrityError) else type(exc).__name__
            # Tracebacks can retain a second whole graph through validation's
            # arguments. Exit the exception scope and drop that traceback before
            # restoring; clearing only the local variable is insufficient.
            exc.__traceback__ = None
            candidate = derived = result = None
        gc.collect()
        if had_cache:
            restored = unpack(rollback)
            cache.clear()
            cache.update(restored)
        else:
            cache.clear()
            cache.update(metadata)
            cache.update({key: None for key in FIELDS})
        raise IndexIntegrityError('Directory refresh rejected: ' + failure) from None
