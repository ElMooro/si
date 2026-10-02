"""Execute the complete catalog with invented S3 and real gzip/SQLite, no SDK/network."""
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import sqlite3
import tempfile
import time
import tracemalloc
import types
import sys
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[4]
SOURCE = ROOT / 'aws/lambdas/justhodl-provider-catalog/source/lambda_function.py'
SHARED = ROOT / 'aws/shared'
STAMP = datetime(2000, 1, 2, 12, tzinfo=timezone.utc)
TIME = STAMP.timestamp()
NEEDLE = 'kw = {"Bucket": BUCKET, "Prefix": pref,\n                      "MaxKeys": 1000}'
PREDECESSOR_HASH = 'ec8f82b9010454f8708da80279a61330f10685aba22385addee7d1a603e25684'


class Clock(datetime):
    @classmethod
    def now(cls, tz=None):
        return STAMP


def module_for(page_size):
    source = SOURCE.read_text(encoding='utf-8')
    assert source.count(NEEDLE) == 1
    if page_size == 400:
        source = source.replace(NEEDLE, NEEDLE.replace('1000', '400'))
        assert hashlib.sha256(source.encode()).hexdigest() == PREDECESSOR_HASH
    elif page_size != 1000:
        raise ValueError('Only reviewed predecessor and candidate')
    module = types.ModuleType('provider_catalog_fixture')
    boto = types.ModuleType('boto3')
    boto.client = lambda *a, **kw: None
    with patch.dict(sys.modules, {'boto3': boto}), patch.dict(os.environ, {'S3_BUCKET':'invented-bucket'}):
        sys.path.insert(0, str(SHARED))
        try:
            exec(compile(source, str(SOURCE), 'exec'), module.__dict__)
        finally:
            sys.path.remove(str(SHARED))
    module.datetime = Clock
    return module


class ListingFailure(Exception):
    pass


class Store:
    def __init__(self, objects=None, documents=None, page_plans=None, empty_final=(), fail_page=None):
        self.objects = objects or {}
        self.documents = documents or {}
        self.page_plans = page_plans or {}
        self.empty_final = set(empty_final)
        self.fail_page = fail_page
        self.list_calls, self.head_calls, self.get_calls, self.writes = [], [], [], {}
        self.token_chains = {}
        self.virtual_derived_prefixes = ('data/warm/eurostat-series/', 'data/warm/ecb-series/')

    def get_object(self, *, Bucket, Key):
        assert Bucket == 'invented-bucket'
        self.get_calls.append(Key)
        if Key not in self.documents:
            raise KeyError('Invented absent document: ' + Key)
        raw = json.dumps(self.documents[Key]).encode()
        if Key.endswith('.gz'):
            raw = gzip.compress(raw, mtime=int(TIME))
        return {'Body':io.BytesIO(raw)}

    def head_object(self, *, Bucket, Key):
        assert Bucket == 'invented-bucket'
        self.head_calls.append(Key)
        if Key not in self.objects:
            raise KeyError('Invented missing hot key: ' + Key)
        obj = self.objects[Key]
        return {'ContentLength':obj['Size'], 'LastModified':obj['LastModified']}

    def list_objects_v2(self, **kw):
        assert kw['Bucket'] == 'invented-bucket'
        pref, delimiter = kw['Prefix'], kw.get('Delimiter')
        assert not any(pref.startswith(p) for p in self.virtual_derived_prefixes), 'Derived store must never be enumerated'
        chain = (pref, delimiter)
        token = kw.get('ContinuationToken')
        if token:
            assert token in self.token_chains, 'Unknown or cross-prefix token'
            previous_chain, offset, page = self.token_chains.pop(token)
            assert previous_chain == chain, 'Token belongs to another prefix'
        else:
            offset, page = 0, 0
        self.list_calls.append(dict(kw))
        if self.fail_page == (pref, page):
            raise ListingFailure('Invented second-page failure')
        objects = [o for key,o in sorted(self.objects.items()) if key.startswith(pref)]
        common = []
        if delimiter:
            common = sorted({pref + o['Key'][len(pref):].split(delimiter,1)[0] + delimiter
                             for o in objects if delimiter in o['Key'][len(pref):]})
            objects = [o for o in objects if delimiter not in o['Key'][len(pref):]]
        limit = kw.get('MaxKeys', 1000)
        plan = self.page_plans.get(pref)
        take = min(limit, plan[page]) if plan and page < len(plan) else limit
        contents = deepcopy(objects[offset:offset + take])
        end = offset + len(contents)
        truncated = end < len(objects) or (pref in self.empty_final and page == 0 and end == len(objects))
        result = {'Contents':contents,'IsTruncated':truncated,'KeyCount':len(contents)}
        if delimiter:
            result['CommonPrefixes'] = [{'Prefix':p} for p in common[:limit]]
        if truncated:
            next_token = f'opaque-{len(self.list_calls)}-{page}'
            self.token_chains[next_token] = (chain,end,page+1)
            result['NextContinuationToken'] = next_token
        return result

    def put_object(self, *, Bucket, Key, Body, **kw):
        assert Bucket == 'invented-bucket'
        self.writes[Key] = {'body':bytes(Body), 'options':deepcopy(kw)}

    def upload_file(self, path, Bucket, Key, ExtraArgs):
        assert Bucket == 'invented-bucket'
        self.writes[Key] = {'body':self.resolve(path).read_bytes(), 'options':deepcopy(ExtraArgs)}


def object_at(key, size=100, hours=1):
    return {'Key':key,'Size':size,'LastModified':STAMP-timedelta(hours=hours)}


def full_store(count):
    objects = {f'data/warm/ofr/fixture-{i:06d}.json':object_at(f'data/warm/ofr/fixture-{i:06d}.json', 100 + i%7, i%49)
               for i in range(count)}
    for key in ('data/ofr-funding.json','data/indicator-bus.json','equity-research/INVENTED.json'):
        objects[key] = object_at(key, 700, 2)
    documents = {
        'data/audit/engine-provider-map.json':{'invented-engine':'ofr'},
        'data/audit/lambda-graph.json':{'engines':{'invented-engine':{'writes':['data/ofr-funding.json']}}},
        'data/audit/engine-writes-overrides.json':{'writes':{'invented-engine':['data/ofr-funding.json']}},
        'data/warm/ofr/state.json':{'catalog':['invented-a','invented-b']},
        'data/indicator-bus.json':{'n':1,'indicators':{'INVENTED':{'name':'Invented indicator','src':'fixture'}}},
        'data/providers/eurostat/series-manifest.json':{'pages':2500000,'pages_bytes':500000000000,
                'series_extracted':9000000,'updated_at':STAMP.isoformat()},
        'data/providers/ecb/series-manifest.json':{'pages':800,'pages_bytes':1234567,
                'series_extracted':1200,'updated_at':STAMP.isoformat()},
    }
    return Store(objects,documents)


def run(page_size, store, registry=None, measure=False):
    module = module_for(page_size)
    if registry is not None:
        module.REG = deepcopy(registry)
    with tempfile.TemporaryDirectory(prefix='catalog-fixture-') as folder:
        root = Path(folder)
        def resolve(path):
            path = Path(path)
            assert str(path).startswith('/tmp/provider-search'), 'Unexpected fixture file access'
            return root / path.name
        store.resolve = resolve
        real_open, real_gzip_open, real_connect = open, gzip.open, sqlite3.connect
        module.open = lambda path,*a,**kw: real_open(resolve(path),*a,**kw)
        module.os = types.SimpleNamespace(remove=lambda p: resolve(p).unlink(),
                                        path=types.SimpleNamespace(getsize=lambda p: resolve(p).stat().st_size))
        module.sqlite3 = types.SimpleNamespace(connect=lambda p: real_connect(resolve(p)))
        module.gzip = types.SimpleNamespace(open=lambda p,*a,**kw:real_gzip_open(resolve(p),*a,**kw),decompress=gzip.decompress)
        module.s3 = store
        if measure:
            tracemalloc.start()
        started = time.perf_counter()
        try:
            with patch('gzip.time.time', return_value=TIME), redirect_stdout(io.StringIO()):
                result = module.lambda_handler({},None)
            elapsed = time.perf_counter() - started
            peak = tracemalloc.get_traced_memory()[1] if measure else None
        finally:
            if measure:
                tracemalloc.stop()
        index = json.loads(store.writes['data/search/provider-shards.json']['body'])['index']
        packed = store.writes[index['key']]['body']
        assert hashlib.sha256(packed).hexdigest() == index['sha256']
        assert len(packed) == index['bytes']
        expanded = gzip.decompress(packed)
        assert len(expanded) == index['uncompressed_bytes']
        database = root / 'inspect.sqlite'
        database.write_bytes(expanded)
        con = sqlite3.connect(database)
        rows = con.execute('SELECT id,provider,provider_name,title,key,kind,nbytes,age_h,hot FROM docs ORDER BY rowid').fetchall()
        con.close()
        return {'result':result,'writes':deepcopy(store.writes),'rows':rows,
                'list_calls':deepcopy(store.list_calls),'head_calls':store.head_calls[:],'get_calls':store.get_calls[:],
                'elapsed_seconds':elapsed,'peak_python_bytes':peak}
