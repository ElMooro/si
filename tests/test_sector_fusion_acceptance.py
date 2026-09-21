"""Acceptance dispatches have durable identities and allow only two public producers."""
from pathlib import Path
from datetime import datetime,timezone
import ast,io,json,types,unittest
ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'aws/ops/staged/ops_5972_sector_fusion_native_recovery_acceptance.py'
KINDS={'justhodl-sector-flow-state':'flow','justhodl-sector-capital-fusion':'capital'}
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
        with self.assertRaises(ConnectionError):fn(lam,s3,module,'justhodl-sector-flow-state')
        self.assertFalse(fn(lam,s3,module,'justhodl-sector-flow-state')['invoke_sent']);self.assertEqual(state.calls,1)


if __name__=='__main__':unittest.main(verbosity=2)
