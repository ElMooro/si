"""Raw archive bytes, races, identity, bounds and credential redaction."""
from contextlib import redirect_stdout
from copy import deepcopy
from datetime import datetime,timezone,timedelta
import gzip
import hashlib
import importlib.util
import io
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

class Store:
    def __init__(self):self.docs={};self.puts=[];self.reads=[];self.error=None
    def put_object(self,**kw):
        self.puts.append(kw)
        if self.error:raise self.error
        if kw['Key'] in self.docs:
            e=ValueError('race');e.response={'Error':{'Code':'PreconditionFailed'}};raise e
        assert kw['IfNoneMatch']=='*'
        self.docs[kw['Key']]={**deepcopy(kw),'LastModified':datetime.now(timezone.utc)}
    def get_object(self,**kw):
        self.reads.append(kw['Key']);doc=self.docs[kw['Key']]
        return {**doc,'Body':io.BytesIO(doc['Body'])}

def load():
    store=Store();path=Path(__file__).resolve().parents[1]/'raw_snapshot.py'
    spec=importlib.util.spec_from_file_location('raw_snapshot_test',path);module=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:store)}):spec.loader.exec_module(module)
    return module,store

class ArchiveTests(unittest.TestCase):
    def test_complete_body_over_old_truncation_limit_and_full_hash(self):
        m,s=load();raw=b'{"series":['+b'123.45,'*100000+b'0]}'
        key=m.snapshot('fred','https://api.example.org/data?series_id=X',raw)
        self.assertIn(hashlib.sha256(raw).hexdigest(),key)
        self.assertEqual(m.read_snapshot(key),raw)
        receipt=m.snapshot_receipt(key);self.assertEqual(receipt['bytes'],len(raw));self.assertTrue(receipt['captured_bytes_verified'])
        self.assertFalse(receipt['publication_time_verified']);self.assertFalse(receipt['sizing_eligible'])

    def test_conditional_retry_does_not_replace_capture_clock_or_bytes(self):
        m,s=load();key=m.snapshot('fred','https://api.example.org/data?series_id=X',b'{}');original=deepcopy(s.docs[key])
        self.assertEqual(m.snapshot('fred','https://api.example.org/data?series_id=X',b'{}'),key)
        self.assertEqual(s.docs[key],original);self.assertEqual(s.puts[0]['Body'],s.puts[1]['Body'])

    def test_different_request_identity_does_not_share_a_capture(self):
        m,_=load();a=m.snapshot('ofr','https://example.org/data?mnemonic=one',b'{}');b=m.snapshot('ofr','https://example.org/data?mnemonic=two',b'{}')
        self.assertNotEqual(a,b)

    def test_credentials_fragments_userinfo_and_unrecognized_queries_are_not_retained(self):
        m,s=load();url='https://user:private@example.org/data?series_id=X&api_key=PRIVATE&UserID=PRIVATE&token=PRIVATE&custom=PRIVATE#PRIVATE'
        key=m.snapshot('fred',url,b'{}',meta={'url':url,'sha256':'forged','token':'PRIVATE'})
        self.assertNotIn('PRIVATE',str(s.docs));self.assertNotIn('user:',str(s.docs))
        self.assertEqual(s.docs[key]['Metadata']['source_url'],'https://example.org/data?series_id=X')
        self.assertEqual(m.read_snapshot(key),b'{}')

    def test_corrupt_body_or_metadata_cannot_be_verified(self):
        m,s=load();key=m.snapshot('fred','https://example.org/data',b'{}');original=deepcopy(s.docs[key])
        for field,value in [('sha256','0'*64),('provider','other'),('source_url','https://example.org/else'),('bytes','100')]:
            s.docs[key]=deepcopy(original);s.docs[key]['Metadata'][field]=value
            self.assertIsNone(m.read_snapshot(key))
        s.docs[key]=deepcopy(original);s.docs[key]['Body']=gzip.compress(b'{"changed":true}')
        self.assertIsNone(m.read_snapshot(key))
        self.assertIsNone(m.snapshot('fred','https://example.org/data',b'{}'))

    def test_storage_clock_must_support_capture_timestamp(self):
        m,s=load();key=m.snapshot('fred','https://example.org/data',b'{}')
        s.docs[key]['LastModified']+=timedelta(days=1)
        self.assertIsNone(m.read_snapshot(key))

    def test_legacy_or_arbitrary_keys_are_not_upgraded_to_proof(self):
        m,s=load()
        for key in ('data/raw/fred/2026-09-18/123456789abc.json.gz','portfolio/snapshot.json',None):
            self.assertIsNone(m.read_snapshot(key))
        self.assertEqual(s.reads,[])

    def test_over_bound_decoded_text_and_invalid_provider_return_no_key(self):
        m,s=load();m.MAX_BYTES=4
        for provider,raw in [('fred',b'12345'),('fred','{}'),('fred',b''),('../escape',b'{}')]:
            self.assertIsNone(m.snapshot(provider,'https://example.org/data',raw))
        self.assertEqual(s.puts,[])

    def test_archive_errors_are_nonfatal_and_do_not_leak_provider_error_text(self):
        m,s=load();s.error=RuntimeError('https://example.org/?api_key=PRIVATE')
        logs=io.StringIO()
        with redirect_stdout(logs):self.assertIsNone(m.snapshot('fred','https://example.org/data',b'{}'))
        self.assertNotIn('PRIVATE',logs.getvalue());self.assertIn('unavailable',logs.getvalue())

if __name__=='__main__':unittest.main()
