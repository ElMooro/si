"""Legacy migration cannot overwrite its source or bless a truncated/corrupt copy."""
from pathlib import Path
import hashlib
import io
import runpy
import sys
import types
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2]
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace()}):
    SCOPE=runpy.run_path(str(ROOT/'aws/ops/STAGED/ops_5881_preserve_legacy_holdings.py'))

class Failure(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}

class Storage:
    def __init__(self,raw=b'complete original',corrupt=False,changed=False):
        self.raw=raw;self.corrupt=corrupt;self.changed=changed;self.objects={};self.writes=[];self.conditions=[]
    def get_object(self,**kw):
        if kw['Key']=='13f-cache/FUND/original.json':
            self.conditions.append(kw.get('IfMatch'))
            if self.changed:raise Failure('PreconditionFailed')
            return {'Body':io.BytesIO(self.raw),'ETag':'"original"','VersionId':'version-original'}
        body=self.objects[kw['Key']]
        return {'Body':io.BytesIO(b'corrupt' if self.corrupt else body)}
    def put_object(self,**kw):
        self.writes.append(kw['Key']);assert kw['IfNoneMatch']=='*'
        self.objects[kw['Key']]=kw['Body'].read()


def test_whole_original_is_retained_with_conditional_source_read():
    client=Storage();row=SCOPE['preserve'](client,'13f-cache/FUND/original.json',{'etag':'"original"','bytes':len(client.raw)})
    assert row['sha256']==hashlib.sha256(client.raw).hexdigest() and row['source_version_id']=='version-original'
    assert client.conditions==['"original"'] and all(k.startswith(SCOPE['PRIVATE']) for k in client.writes)
    assert client.raw==b'complete original'


def test_changed_source_is_not_copied_or_declared_preserved():
    client=Storage(changed=True)
    try:SCOPE['preserve'](client,'13f-cache/FUND/original.json',{'etag':'"original"','bytes':len(client.raw)})
    except Failure:pass
    else:raise AssertionError('Changed source accepted')
    assert not client.writes


def test_corrupt_archive_fails_complete_copy_verification():
    client=Storage(corrupt=True)
    try:SCOPE['preserve'](client,'13f-cache/FUND/original.json',{'etag':'"original"','bytes':len(client.raw)})
    except AssertionError:pass
    else:raise AssertionError('Corrupt archived copy accepted')


def test_truncated_or_oversized_original_has_no_completed_archive():
    client=Storage()
    try:SCOPE['preserve'](client,'13f-cache/FUND/original.json',{'etag':'"original"','bytes':len(client.raw)+1})
    except AssertionError:pass
    else:raise AssertionError('Truncated source accepted')
    assert not client.writes
    fn=SCOPE['hash_stream'];old=fn.__globals__['MAX_BYTES'];fn.__globals__['MAX_BYTES']=2
    try:
        try:fn(io.BytesIO(b'abc'))
        except ValueError:pass
        else:raise AssertionError('Oversized source accepted')
    finally:fn.__globals__['MAX_BYTES']=old
