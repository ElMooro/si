"""Wholly invented transport, identity and coverage contract cases."""
from pathlib import Path
from unittest.mock import patch
import hashlib,io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
import ticker_coverage_context as model

def packet():
    return {'contract':'ticker-360.v1','tickers':{'TEST':{'coverage_count':1,'domains':{'macro':{'market_wide':True}}}}}

class Storage:
    def __init__(self,raw,length=None,stream=None):
        self.raw=raw;self.length=len(raw) if length is None else length
        self.body=stream if stream is not None else io.BytesIO(raw);self.reads=[]
    def get_object(self,**request):
        self.reads.append(request)
        return {'Body':self.body,'ContentLength':self.length}

class Context(unittest.TestCase):
    def test_only_named_body_is_read_hashed_and_closed_without_retention_claim(self):
        raw=json.dumps(packet()).encode();db=Storage(raw);out=model.load(db,'invented')
        self.assertEqual(db.reads,[{'Bucket':'invented','Key':'data/ticker-360.json'}]);self.assertTrue(db.body.closed)
        self.assertEqual(out['acquisition'],{'artifact':model.SOURCE,'read_status':'parsed','body_bytes':len(raw),'body_sha256':hashlib.sha256(raw).hexdigest()})
        self.assertIs(out['original_body_retained'],False);self.assertIs(out['observation_freshness_verified'],False)
        row=out['by_ticker']['TEST'];self.assertEqual(row['source_row'],'/tickers/TEST');self.assertEqual(row['coverage_count'],1)
        for scope in (out,row):
            self.assertEqual(scope['additional_independent_votes'],0)
            for flag in model.FLAGS:self.assertIs(scope[flag],False)
    def test_zero_length_is_hashed_but_is_not_a_valid_packet(self):
        out=model.load(Storage(b''),'invented');self.assertEqual(out['acquisition']['body_bytes'],0)
        self.assertEqual(out['acquisition']['body_sha256'],hashlib.sha256(b'').hexdigest());self.assertEqual(out['status'],'unavailable')
    def test_wrong_typed_or_incomplete_lengths_fail_closed_and_close(self):
        for length in (True,'2',-1,1,3):
            with self.subTest(length=length):
                db=Storage(b'{}',length);out=model.load(db,'invented')
                self.assertEqual(out['status'],'unavailable');self.assertIsNone(out['acquisition']['body_sha256']);self.assertTrue(db.body.closed)
    def test_bounded_reads_reject_oversize_even_when_declared_length_lies(self):
        for raw,length in ((b'a'*9,9),(b'a'*9,8)):
            db=Storage(raw,length)
            with patch.object(model,'LIMIT',8):out=model.load(db,'invented')
            self.assertEqual(out['status'],'unavailable');self.assertTrue(db.body.closed);self.assertIsNone(out['acquisition']['body_sha256'])
    def test_stream_failure_and_nonbyte_stream_never_leak_exception_text(self):
        class Broken(io.BytesIO):
            def read(self,*a):raise OSError('PRIVATE_CANARY')
        for stream in (Broken(b'{}'),io.StringIO('{}')):
            db=Storage(b'{}',stream=stream);out=model.load(db,'invented');self.assertTrue(stream.closed)
            self.assertEqual(out['status'],'unavailable');self.assertNotIn('PRIVATE_CANARY',json.dumps(out))
    def test_strict_json_rejects_nested_duplicate_utf8_nonfinite_underflow_and_overflow(self):
        for raw in (b'\xff',b'{"tickers":{"TEST":{},"TEST":{}}}',b'{"x":NaN}',b'{"x":Infinity}',b'{"x":1e309}',b'{"x":1e-999}'):
            with self.subTest(raw=raw):
                db=Storage(raw);out=model.load(db,'invented');self.assertEqual(out['status'],'unavailable');self.assertTrue(db.body.closed)
                self.assertEqual(out['acquisition']['body_sha256'],hashlib.sha256(raw).hexdigest())
    def test_domain_inventory_and_typed_count_must_reconcile(self):
        for count,domains in ((True,{}),(None,{}),(-1,{}),('1',{'macro':{}}),(2,{'macro':{}}),(1,{'macro':[]}),(1,{'/unsafe':{}}),(2**53,{})):
            p=packet();p['tickers']['TEST'].update(coverage_count=count,domains=domains);out=model.context(p)['by_ticker']['TEST']
            self.assertIsNone(out['coverage_count']);self.assertFalse(out['count_matches_domain_inventory'])
        p=packet();p['tickers']['TEST']={'coverage_count':0,'domains':{}};self.assertEqual(model.context(p)['by_ticker']['TEST']['coverage_count'],0)
    def test_case_collision_with_invalid_row_withholds_every_occurrence(self):
        for duplicate in ([],None,{'ticker':'OTHER'},{'coverage_count':0,'domains':{}}):
            p=packet();p['tickers']['test']=duplicate;out=model.context(p)
            self.assertEqual(out['by_ticker'],{});rows=out['ambiguous_symbols'][0]['occurrences']
            self.assertEqual([r['source_row'] for r in rows],['/tickers/TEST','/tickers/test'])
    def test_invalid_identity_and_reported_errors_cannot_become_evidence(self):
        for bad in ('/TEST','TEST~1','ＴＥＳＴ','TEST\n'):
            p=packet();p['tickers']={bad:{}};out=model.context(p);self.assertEqual(out['by_ticker'],{});self.assertEqual(out['unresolved_rows'],1)
        for key,value in (('error','PRIVATE_CANARY'),('_error',True),('status','unavailable'),('contract','wrong')):
            p=packet();p[key]=value;out=model.context(p);self.assertEqual(out['status'],'unavailable');self.assertNotIn('PRIVATE_CANARY',json.dumps(out))

if __name__=='__main__':unittest.main(verbosity=2)
