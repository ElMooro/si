"""Exercise the actual acceptance source dispatch boundary without AWS or providers."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
import ast,io,json,types,unittest
ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'aws/ops/staged/ops_5963_activity_native_acceptance.py'
class Conflict(Exception):pass

class SourceAcceptance(unittest.TestCase):
    def arrange(self,already_ready=False,ambiguous=False):
        series=('WEI','GDPNOW','ICSA','CCSA','NFCI','STLFSI4','BAA10Y','T10Y3M')
        storage=types.SimpleNamespace(current={'generated_at':(datetime.now(timezone.utc)-timedelta(hours=1)).isoformat(),'quality':{},'replay':{},'complete':already_ready},claims={},calls=0)
        def fresh():storage.current.update(generated_at=datetime.now(timezone.utc).isoformat(),complete=True)
        def status_write(client,bucket,key,doc,**condition):
            if condition.get('IfNoneMatch')=='*' and key in storage.claims:raise Conflict()
            storage.claims[key]=dict(doc)
        def invoke(**kw):
            storage.calls+=1
            self.assertEqual(kw['FunctionName'],'justhodl-daily-report-v3');self.assertEqual(kw['InvocationType'],'Event')
            self.assertEqual(json.loads(kw['Payload']),{'action':'research_measurements'})
            if ambiguous:raise ConnectionError('ambiguous response')
            fresh();return {'StatusCode':202}
        store=types.SimpleNamespace(reader=lambda *a:lambda key:json.dumps(storage.current).encode(),request_key=lambda x:x,
            status_write=status_write,conflict=lambda exc:isinstance(exc,Conflict),bounded=lambda body:body.read())
        client=types.SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(json.dumps(storage.claims[kw['Key']]).encode())})
        model=types.SimpleNamespace(SERIES=series,encoded=lambda d:json.dumps(d).encode(),clock=lambda s:datetime.fromisoformat(s))
        ns={'COMMIT':'a'*40,'BUCKET':'test','store':store,'model':model,'datetime':datetime,'timezone':timezone,'json':json,
            'canonical_fred_replay':types.SimpleNamespace(pinned_report=lambda packet,read:{'catalog':[*series,'RRSFS'] if packet['complete'] else ['ICSA']}),
            'time':types.SimpleNamespace(monotonic=lambda:0,sleep=lambda seconds:fresh()),
            'invoke_when_available':lambda lam,kwargs:(lam.invoke(**kwargs),0)}
        node=next(n for n in ast.parse(PATH.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='canonical_refresh')
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(PATH),'exec'),ns)
        return ns['canonical_refresh'],types.SimpleNamespace(invoke=invoke),client,storage

    def test_ready_pinned_publication_does_not_trigger_another_collection(self):
        fn,lam,s3,state=self.arrange(already_ready=True);result=fn(lam,s3)
        self.assertFalse(result['invoke_sent']);self.assertEqual(state.calls,0);self.assertEqual(state.claims,{})

    def test_dispatch_is_claimed_once_and_accepted_async_then_proven_by_publication(self):
        fn,lam,s3,state=self.arrange();result=fn(lam,s3)
        self.assertTrue(result['invoke_sent']);self.assertEqual(state.calls,1);self.assertEqual(result['lambda_http_status'],202)
        self.assertEqual(result['status'],'source_publication_verified');self.assertNotIn('response',result)
        again=fn(lam,s3);self.assertFalse(again['invoke_sent']);self.assertEqual(state.calls,1)

    def test_lost_ack_preserves_claim_and_recovery_never_resends(self):
        fn,lam,s3,state=self.arrange(ambiguous=True)
        with self.assertRaises(ConnectionError):fn(lam,s3)
        self.assertEqual(state.calls,1);self.assertEqual(next(iter(state.claims.values()))['status'],'claimed')
        result=fn(lam,s3)
        self.assertFalse(result['invoke_sent']);self.assertEqual(state.calls,1);self.assertEqual(result['status'],'source_publication_verified')

if __name__=='__main__':unittest.main(verbosity=2)
