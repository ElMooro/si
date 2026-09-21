"""Exercise actual acceptance dispatches without AWS, accounts or providers."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import ast,io,json,types,unittest
ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'aws/ops/staged/ops_5965_sector_native_acceptance.py'
class Conflict(Exception):pass
class Missing(Exception):pass

def actual(name,ns):
    node=next(n for n in ast.parse(PATH.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name==name)
    exec(compile(ast.Module(body=[node],type_ignores=[]),str(PATH),'exec'),ns)
    return ns[name]

class SourceAcceptance(unittest.TestCase):
    def arrange(self,already_ready=False,ambiguous=False):
        symbols=('XLK','SPY','SMH')
        state=types.SimpleNamespace(current={'generated_at':(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat(),'replay':{},'stocks':dict.fromkeys(symbols,{}) if already_ready else {}},claims={},calls=0)
        def fresh():state.current.update(generated_at=datetime.now(timezone.utc).isoformat(),stocks=dict.fromkeys(symbols,{}))
        def status_write(client,bucket,key,doc,**condition):
            if condition.get('IfNoneMatch')=='*' and key in state.claims:raise Conflict()
            state.claims[key]=dict(doc)
        def invoke(**kw):
            state.calls+=1
            self.assertEqual(kw['FunctionName'],'justhodl-daily-report-v3');self.assertEqual(kw['InvocationType'],'Event');self.assertEqual(json.loads(kw['Payload']),{})
            self.assertTrue(state.claims,'Claim must precede the only source dispatch')
            if ambiguous:raise ConnectionError('ambiguous response')
            fresh();return {'StatusCode':202}
        store=types.SimpleNamespace(reader=lambda *a:lambda key:json.dumps(state.current).encode(),request_key=lambda x:x,status_write=status_write,conflict=lambda e:isinstance(e,Conflict),bounded=lambda b:b.read())
        client=types.SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(json.dumps(state.claims[kw['Key']]).encode())})
        model=types.SimpleNamespace(SYMBOLS=symbols,encoded=lambda d:json.dumps(d).encode(),clock=lambda s:datetime.fromisoformat(s))
        ns={'COMMIT':'a'*40,'BUCKET':'test','store':store,'model':model,'datetime':datetime,'timezone':timezone,'json':json,
            'sector_market_replay':types.SimpleNamespace(restore=lambda packet,symbols,read:{s:{'acquired_at':packet['generated_at']} for s in symbols}),
            'time':types.SimpleNamespace(monotonic=lambda:0,sleep=lambda seconds:fresh()),'invoke_when_available':lambda lam,kwargs:(lam.invoke(**kwargs),0)}
        return actual('canonical_refresh',ns),types.SimpleNamespace(invoke=invoke),client,state
    def test_original_replayed_complete_existing_publication_requires_no_collection(self):
        fn,lam,s3,state=self.arrange(already_ready=True);out=fn(lam,s3);self.assertFalse(out['invoke_sent']);self.assertEqual(state.calls,0)
    def test_source_dispatch_claimed_once_then_proven_by_new_publication(self):
        fn,lam,s3,state=self.arrange();out=fn(lam,s3);self.assertTrue(out['invoke_sent']);self.assertEqual(out['status'],'source_publication_verified');self.assertEqual(state.calls,1)
        self.assertFalse(fn(lam,s3)['invoke_sent']);self.assertEqual(state.calls,1)
    def test_source_lost_ack_preserves_claim_without_resending(self):
        fn,lam,s3,state=self.arrange(ambiguous=True)
        with self.assertRaises(ConnectionError):fn(lam,s3)
        self.assertEqual(state.calls,1);self.assertEqual(next(iter(state.claims.values()))['status'],'claimed')
        self.assertFalse(fn(lam,s3)['invoke_sent']);self.assertEqual(state.calls,1)

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
            state.calls+=1;state.request=json.loads(kw['Payload'])['request_id']
            self.assertIn(state.request+'-dispatch',state.objects);self.assertEqual(kw['InvocationType'],'Event');self.assertEqual(kw['FunctionName'],'justhodl-sector-rotation')
            if ambiguous:raise ConnectionError('ack lost')
            complete();return {'StatusCode':202}
        module=types.SimpleNamespace(request_key=lambda x:x,status_write=write,conflict=lambda e:isinstance(e,Conflict),missing=lambda e:isinstance(e,Missing),bounded=lambda b:b.read())
        ns={'COMMIT':'a'*40,'BUCKET':'test','datetime':datetime,'timezone':timezone,'json':json,'model':types.SimpleNamespace(encoded=lambda d:json.dumps(d).encode()),
            'time':types.SimpleNamespace(monotonic=lambda:0,sleep=lambda seconds:complete()),'invoke_when_available':lambda lam,kw:(lam.invoke(**kw),0)}
        return actual('invoke_public',ns),types.SimpleNamespace(invoke=invoke),types.SimpleNamespace(get_object=get_object),module,state
    def test_native_public_dispatch_is_idempotent(self):
        fn,lam,s3,module,state=self.arrange();self.assertTrue(fn(lam,s3,module,'justhodl-sector-rotation')['invoke_sent']);self.assertFalse(fn(lam,s3,module,'justhodl-sector-rotation')['invoke_sent']);self.assertEqual(state.calls,1)
    def test_native_ambiguous_dispatch_is_observed_without_resend(self):
        fn,lam,s3,module,state=self.arrange(ambiguous=True)
        with self.assertRaises(ConnectionError):fn(lam,s3,module,'justhodl-sector-rotation')
        self.assertFalse(fn(lam,s3,module,'justhodl-sector-rotation')['invoke_sent']);self.assertEqual(state.calls,1)

if __name__=='__main__':unittest.main(verbosity=2)
