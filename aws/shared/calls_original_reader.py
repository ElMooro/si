"""Bounded reader for the two reviewed public original-source archive families."""
import gzip
import hashlib
import io
import re
import calls_liquidity_originals as liquidity
import calls_fails_originals as fails


def allowed(key):
    return isinstance(key, str) and (
        liquidity.store.allowed(key) and key not in (liquidity.store.SOURCE, liquidity.store.SETTLEMENT, liquidity.store.model.CURRENT)
        or any(re.fullmatch(pattern, key) for pattern in fails.IMMUTABLE))


class ImmutableReader:
    def __init__(self, read): self.read = read; self.cache = {}; self.bytes = 0

    def __call__(self, key):
        if not allowed(key): raise ValueError('Reviewed immutable original path required before transport')
        if key in self.cache: return self.cache[key]
        if len(self.cache) >= 256: raise ValueError('Complete original archive count exceeds bound')
        raw = self.read(key)
        if not isinstance(raw, bytes) or not 0 < len(raw) <= liquidity.store.MAX:
            raise ValueError('Complete bounded original artifact required')
        if self.bytes+len(raw) > 256*1024*1024: raise ValueError('Complete original archive bytes exceed bound')
        match = re.search(r'/([a-f0-9]{64})\.(?:json|py|bin\.gz)$', key)
        if not match or hashlib.sha256(raw).hexdigest() != match[1]: raise ValueError('Original artifact hash differs')
        self.cache[key] = raw; self.bytes += len(raw)
        return raw


def reader(client, bucket):
    def read(key):
        raw = liquidity.store.bounded(client.get_object(Bucket=bucket, Key=key)['Body'])
        return liquidity.store.bounded(gzip.GzipFile(fileobj=io.BytesIO(raw))) if key.endswith('.gz') else raw
    return ImmutableReader(read)
