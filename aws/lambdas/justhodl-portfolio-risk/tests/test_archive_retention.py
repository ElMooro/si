"""Immutable archive collision checks on complete invented inputs only."""
from pathlib import Path
import copy
import json
import sys
import types
import unittest

ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'tests'),str(ROOT/'aws/shared'),str(Path(__file__).resolve().parents[1]/'source')]
from private_portfolio_test_support import Store,load
import portfolio_risk_model as model


class Body:
    def __init__(self,raw,chunk=97,hook=None):
        self.raw=raw;self.chunk=chunk;self.hook=hook;self.position=0;self.closed=False;self.read_sizes=[]
    def read(self,n=-1):
        if n<1:raise AssertionError('Unbounded archive read')
        self.read_sizes.append(n)
        if self.hook:self.hook(self)
        part=self.raw[self.position:self.position+min(n,self.chunk)];self.position+=len(part)
        return part
    def close(self):self.closed=True


class Precondition(Exception):
    response={'Error':{'Code':'PreconditionFailed'}}


class Collision(Store):
    def __init__(self,response):super().__init__();self.response=response;self.attempts=[]
    def put_object(self,**request):self.attempts.append(request);raise Precondition()
    def get_object(self,**request):self.reads.append(request['Key']);return self.response


class ArchiveRetention(unittest.TestCase):
    def setUp(self):
        fixture=json.loads((ROOT/'tests/fixtures/pre-portfolio-publication/complete-synthetic.json').read_bytes())
        self.bundle=fixture['old_bundle'];self.raw=model.canonical(self.bundle)
        self.clock=0.0
    def environment(self,store):
        _,_,env=load('portfolio-risk',store)
        env['time']=types.SimpleNamespace(monotonic=lambda:self.clock)
        return env
    def verify(self,body,**metadata):
        env=self.environment(Store())
        return env['verify_archive_bytes']({'Body':body,**metadata},self.raw,20)
    def test_frozen_complete_collision_is_exact_and_closed(self):
        original=copy.deepcopy(self.bundle);body=Body(self.raw)
        store=Collision({'Body':body,'ContentLength':len(self.raw)})
        out=self.environment(store)['retain_bundle'](self.bundle)
        self.assertEqual(out['bundle_sha256'],model.digest(self.bundle));self.assertEqual(self.bundle,original)
        self.assertEqual(store.reads,[out['bundle_key']]);self.assertEqual(len(store.attempts),1)
        self.assertEqual(store.attempts[0]['Body'],self.raw);self.assertEqual(store.attempts[0]['IfNoneMatch'],'*')
        self.assertTrue(body.closed);self.assertEqual(body.position,len(self.raw))
        self.assertTrue(all(0<n<=65536 for n in body.read_sizes))
        self.assertEqual(body.read_sizes[-1],1)
    def test_complete_without_declared_length_is_accepted(self):
        body=Body(self.raw,chunk=1);self.verify(body);self.assertTrue(body.closed)
    def test_truncated_extra_and_changed_bytes_never_validate(self):
        for raw in [self.raw[:-1],self.raw+b' ',b'!'+self.raw[1:],b'']:
            body=Body(raw)
            with self.assertRaises(ValueError):self.verify(body)
            self.assertTrue(body.closed)
    def test_invalid_declared_length_closes_before_any_body_read(self):
        for value in [True,-1,len(self.raw)-1,len(self.raw)+1,str(len(self.raw)),float(len(self.raw))]:
            body=Body(self.raw)
            with self.assertRaises(ValueError):self.verify(body,ContentLength=value)
            self.assertTrue(body.closed);self.assertEqual(body.read_sizes,[])
    def test_invalid_content_encoding_cannot_qualify_an_archive(self):
        for encoding in ['gzip','br',None]:
            body=Body(self.raw)
            with self.assertRaises(ValueError):self.verify(body,ContentEncoding=encoding)
            self.assertTrue(body.closed);self.assertEqual(body.read_sizes,[])
    def test_slow_headers_or_body_reject_and_close(self):
        self.clock=21;body=Body(self.raw)
        with self.assertRaises(ValueError):self.verify(body)
        self.assertTrue(body.closed);self.assertEqual(body.read_sizes,[])
        self.clock=0
        def late(_):self.clock=21
        body=Body(self.raw,hook=late)
        with self.assertRaises(ValueError):self.verify(body)
        self.assertTrue(body.closed)
    def test_midstream_io_failure_closes_without_retry(self):
        def fail(body):
            if body.position:raise OSError('Invented read failure')
        body=Body(self.raw,hook=fail)
        store=Collision({'Body':body,'ContentLength':len(self.raw)})
        with self.assertRaises(OSError):self.environment(store)['retain_bundle'](self.bundle)
        self.assertTrue(body.closed);self.assertEqual(len(store.attempts),1);self.assertEqual(len(store.reads),1)
    def test_new_archive_and_unrelated_failure_do_not_read_existing_state(self):
        store=Store();env=self.environment(store);out=env['retain_bundle'](self.bundle)
        self.assertEqual(store.reads,[]);self.assertEqual(store.docs[out['bundle_key']],self.bundle)
        class Denied(Collision):
            def put_object(self,**request):raise PermissionError('Invented denied archive write')
        denied=Denied({})
        with self.assertRaises(PermissionError):self.environment(denied)['retain_bundle'](self.bundle)
        self.assertEqual(denied.reads,[])


if __name__=='__main__':unittest.main()
