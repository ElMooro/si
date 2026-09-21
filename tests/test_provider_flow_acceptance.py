"""Acceptance dispatches have durable identities and allow only two public producers."""
from pathlib import Path
from datetime import datetime,timezone
import ast,io,json,types,unittest
ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'aws/ops/staged/ops_5974_provider_fund_flow_native_acceptance.py'
KINDS={'justhodl-etf-fund-flows':'flow','justhodl-capital-flow-radar':'radar'}
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
        with self.assertRaises(ConnectionError):fn(lam,s3,module,'justhodl-etf-fund-flows')
        self.assertFalse(fn(lam,s3,module,'justhodl-etf-fund-flows')['invoke_sent']);self.assertEqual(state.calls,1)




class SourceArithmetic(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        import sys
        sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
        import provider_flow_store as store,provider_flow_model as model
        from test_provider_flow_store import Storage,source_fixture
        from decimal import Decimal,localcontext
        cls.model=model;cls.storage=Storage()
        inputs=source_fixture(cls.storage)
        cls.packet,histories=store.compile_output(inputs,store.reader(cls.storage,'fixture'))
        cls.storage.objects.update(histories)
        cls.read=staticmethod(store.reader(cls.storage,'fixture'))
        cls.check=staticmethod(actual('independent_arithmetic',{'store':store,'model':model,'json':json,'Decimal':Decimal,'localcontext':localcontext}))
    def test_all_original_positions_windows_and_groups(self):
        result=self.check(self.packet,self.read)
        self.assertEqual(result['counts']['normalized_rows'],7500)
        self.assertEqual(result['counts']['original_row_positions'],7500)
        self.assertEqual(result['counts']['complete_windows'],1800)
        self.assertGreater(result['counts']['reconciliations'],7000)
        self.assertGreater(result['counts']['group_windows'],400)
        self.assertEqual(set(result['samples']),{'SPY','VOO','TLT','XLC','SOXL','SQQQ'})
    def test_mutated_window_and_incomplete_as_zero_fail(self):
        import copy
        p=copy.deepcopy(self.packet);p['funds']['SPY']['aligned_windows']['5']['flow_usd_decimal']='1234567'
        with self.assertRaises(AssertionError):self.check(p,self.read)
        p=copy.deepcopy(self.packet);window=p['funds']['SPY']['aligned_windows']['5'];window.update(status='incomplete',flow_usd_decimal='0')
        with self.assertRaises(AssertionError):self.check(p,self.read)
    def test_tampered_row_identity_and_reconciliation_fail(self):
        import copy
        for field in ('identity','reconciliation'):
            p=copy.deepcopy(self.packet);ref=p['funds']['SPY']['history'];history=json.loads(self.read(ref['key']))
            if field=='identity':history['history'][0]['source_rows'][0]['row_index']=1
            else:history['reconciliation'][0]['share_change_decimal']='1234567'
            raw=self.model.encoded(history);key='data/provider-flow-research/histories/'+self.model.sha(raw)+'.json'
            p['funds']['SPY']['history']={'key':key,'sha256':self.model.sha(raw),'bytes':len(raw)}
            read=lambda k:raw if k==key else self.read(k)
            with self.assertRaises(AssertionError):self.check(p,read)
    def test_mutated_group_and_comparison_fail(self):
        import copy
        p=copy.deepcopy(self.packet);p['unique_configured_universe']['5']['flow_usd_decimal']='999999'
        with self.assertRaises(AssertionError):self.check(p,self.read)
        p=copy.deepcopy(self.packet)
        candidate=next(w for group in p['comparisons'] for w in group['windows'].values() if w['status']=='matched')
        candidate['bull_minus_bear_flow_usd_decimal']='999999'
        with self.assertRaises(AssertionError):self.check(p,self.read)


class ReadOnlyRecovery(unittest.TestCase):
    def test_recovery_has_no_dispatch_client_and_requires_completed_identity(self):
        namespace={'COMMIT':'a'*40,'BUCKET':'fixture','KINDS':KINDS,'json':json}
        fn=actual('completed_request',namespace);module=types.SimpleNamespace(request_key=lambda k,x:k+'/'+x,bounded=lambda b:b.read())
        name='justhodl-etf-fund-flows';request='chatgpt-'+name+'-'+'a'*12+'-1';key='flow/'+request
        state={key:{'request_id':request,'kind':'flow','status':'complete','published':True},
            key+'-dispatch':{'contract':'provider-flow-native-dispatch.v1','request_id':request,'status':'accepted_async'}}
        s3=types.SimpleNamespace(get_object=lambda **kw:{'Body':io.BytesIO(json.dumps(state[kw['Key']]).encode())})
        self.assertFalse(fn(s3,module,name)['invoke_sent'])
        state[key]['status']='running'
        with self.assertRaises(AssertionError):fn(s3,module,name)
        state[key]['status']='complete';state[key]['request_id']='other'
        with self.assertRaises(AssertionError):fn(s3,module,name)
        with self.assertRaises(AssertionError):fn(s3,module,'justhodl-ai-brief')

    def test_daily_alias_keeps_existing_reviewed_root_object(self):
        keys=tuple('etf-flows/'+n+'.json' for n in ('daily','measurements','composite','event-study','rotation','per-ticker-context','ai-analysis'))
        aliases=keys+tuple('data/'+k for k in keys)
        fn=actual('public_alias_target',{'store':types.SimpleNamespace(ALIASES=aliases)})
        for key in aliases:self.assertEqual(fn(key),'etf-flows/daily.json' if key=='data/etf-flows/daily.json' else key)
        with self.assertRaises(AssertionError):fn('private/account.json')


if __name__=='__main__':unittest.main(verbosity=2)
