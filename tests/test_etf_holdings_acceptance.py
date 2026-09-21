"""Acceptance dispatches have durable identities and allow only two public producers."""
from pathlib import Path
from datetime import datetime,timezone
import ast,io,json,types,unittest
ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'aws/ops/staged/ops_5978_etf_holdings_native_acceptance.py'
KINDS={'justhodl-etf-constituents':'holdings','justhodl-flow-lookthrough':'lookthrough'}
class Conflict(Exception):pass
class Missing(Exception):pass


def actual(name,ns):
    node=next(n for n in ast.parse(PATH.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(PATH),'exec'),ns);return ns[name]


class NativeAcceptance(unittest.TestCase):
    def arrange(self,ambiguous=False):
        state=types.SimpleNamespace(objects={},calls=0,request=None)
        def get_object(**kw):
            if kw['Key'] not in state.objects:raise Missing()
            return {'Body':io.BytesIO(json.dumps(state.objects[kw['Key']]).encode())}
        def write(client,bucket,key,doc,**condition):
            if condition.get('IfNoneMatch')=='*' and key in state.objects:raise Conflict()
            state.objects[key]=dict(doc)
        def complete():state.objects[state.request]={'status':'complete','published':True}
        def invoke(**kw):
            state.calls+=1;request=json.loads(kw['Payload'])['request_id'];kind=KINDS[kw['FunctionName']];state.request=kind+'/'+request
            self.assertIn(state.request+'-dispatch',state.objects);self.assertEqual(kw['InvocationType'],'Event')
            if ambiguous:raise ConnectionError('Acknowledgment lost')
            complete();return {'StatusCode':202}
        module=types.SimpleNamespace(request_key=lambda k,x:k+'/'+x,status_write=write,conflict=lambda e:isinstance(e,Conflict),missing=lambda e:isinstance(e,Missing),bounded=lambda b:b.read())
        ns={'COMMIT':'a'*40,'BUCKET':'fixture','KINDS':KINDS,'datetime':datetime,'timezone':timezone,'json':json,
            'model':types.SimpleNamespace(encoded=lambda d:json.dumps(d).encode()),'time':types.SimpleNamespace(monotonic=lambda:0,sleep=lambda s:complete()),
            'invoke_when_available':lambda lam,kw:(lam.invoke(**kw),0)}
        return actual('invoke_public',ns),types.SimpleNamespace(invoke=invoke),types.SimpleNamespace(get_object=get_object),module,state
    def test_unreviewed_producer_never_invoked(self):
        fn,lam,s3,module,state=self.arrange()
        with self.assertRaises(AssertionError):fn(lam,s3,module,'justhodl-ai-brief')
        self.assertEqual(state.calls,0)
    def test_each_public_producer_is_individually_idempotent(self):
        fn,lam,s3,module,state=self.arrange()
        for name in KINDS:
            self.assertTrue(fn(lam,s3,module,name)['invoke_sent']);self.assertFalse(fn(lam,s3,module,name)['invoke_sent'])
        self.assertEqual(state.calls,2)
    def test_ambiguous_dispatch_is_observed_without_resending(self):
        fn,lam,s3,module,state=self.arrange(True)
        with self.assertRaises(ConnectionError):fn(lam,s3,module,'justhodl-etf-constituents')
        self.assertFalse(fn(lam,s3,module,'justhodl-etf-constituents')['invoke_sent']);self.assertEqual(state.calls,1)


class SourceArithmetic(unittest.TestCase):
    def setUp(self):
        import sys
        from unittest import mock
        from collections import Counter,defaultdict
        from decimal import Decimal,localcontext
        sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
        import etf_holdings_native as native,etf_holdings_model as model,etf_holdings_store as store
        from test_etf_holdings_store import fixture
        self.native=native;self.model=model;self.store=store
        self.db,inputs=fixture()
        with mock.patch.dict(model.catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'}},clear=True):
            self.packet=store.compile_output(inputs,store.reader(self.db,'fixture'),lambda k,b:store.immutable(self.db,'fixture',k,b))
        self.check=actual('independent_arithmetic',{'native':native,'model':model,'store':store,'json':json,
            'Counter':Counter,'defaultdict':defaultdict,'Decimal':Decimal,'localcontext':localcontext})
    def read(self,key):return self.db.objects[key]
    def changed(self,ref,mutate):
        doc=json.loads(self.read(ref['key']));mutate(doc)
        raw=self.native.encoded(doc);digest=self.native.sha(raw);key=ref['key'].rsplit('/',1)[0]+'/'+digest+'.json'
        self.db.objects[key]=raw
        return {'key':key,'sha256':digest,'bytes':len(raw)}
    def test_original_rows_comparisons_and_memberships(self):
        out=self.check(self.packet,self.read)
        self.assertEqual(out['counts'],{'original_row_positions':3,'snapshots':2,'current_rows':2,'identity_comparisons':1,'fund_memberships':1})
        self.assertTrue(out['all_source_positions_and_arithmetic_match'])
    def test_rehashed_wrong_row_measurement_is_rejected_against_original(self):
        fund=self.packet['funds']['SPY']['current'];snap=json.loads(self.read(fund['snapshot']['key']))
        changed=self.changed(snap['parts'][0],lambda d:d['rows'][0].update(shares_held_raw_decimal='999999'))
        fund['snapshot']=self.changed(fund['snapshot'],lambda d:d['parts'].__setitem__(0,changed))
        with self.assertRaises(AssertionError):self.check(self.packet,self.read)
    def test_rehashed_wrong_position_delta_is_rejected(self):
        fund=self.packet['funds']['SPY'];summary=json.loads(self.read(fund['comparison']['key']))
        changed=self.changed(summary['parts'][0],lambda d:d['rows'][0].update(shares_held_change_raw_decimal='999999'))
        fund['comparison']=self.changed(fund['comparison'],lambda d:d['parts'].__setitem__(0,changed))
        with self.assertRaises(AssertionError):self.check(self.packet,self.read)
    def test_rehashed_duplicate_membership_is_rejected(self):
        ref=self.packet['security_directory'];directory=json.loads(self.read(ref['key']));bucket=next(iter(directory['buckets']))
        changed=self.changed(directory['buckets'][bucket],lambda d:d['securities'][0]['memberships'].append(d['securities'][0]['memberships'][0]))
        self.packet['security_directory']=self.changed(ref,lambda d:d['buckets'].__setitem__(bucket,changed))
        with self.assertRaises(AssertionError):self.check(self.packet,self.read)


if __name__=='__main__':unittest.main(verbosity=2)



