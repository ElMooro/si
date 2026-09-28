"""Synthetic summary/privacy/replay/compatibility checks; no live consumer reads."""
from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from datetime import datetime,timezone
from unittest.mock import patch
import ast,gzip,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[2];SRC=ROOT/'aws/lambdas/justhodl-prepump-summary/source'
sys.path[:0]=[str(ROOT/'aws/shared'),str(SRC)]
import context_evidence_store as store
import summary_evidence as m
AT='2026-09-28T01:00:00Z'


def documents():
    return {'brief':{'call':'WAIT','sizing_eligible':False,'top_3_long_ideas':[{'ticker':'SYN_A','sized_position':'250%'}]},
        'positioning':{'aggressive_basket':{'positions':[{'ticker':'SYN_A','position_pct':99,'private_account':'private only'}]}},
        'catalysts':{'by_grade':{'A':[{'ticker':'SYN_A'}]}},'clusters':{'clusters':[{'quality_grade':'A','recommendation':{'action':'BOOST'}}]},
        'early':{'generated_at':'2099-01-01T00:00:00Z','n_actionable':100,'private_note':'private only'}}


def capture(docs=None):
    docs=documents() if docs is None else docs;attempts={};sources={}
    for name,key in m.INPUTS.items():
        raw=store.encode(docs[name]);ref=store.identity(raw,m.PRIVATE,'sources');sources[ref['key']]=raw
        attempts[name]={'source_key':key,'requested_at':AT,'received_at':AT,'status':'received','original_ref':ref,'content_encoding':''}
    return attempts,sources


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.data={m.INPUTS[k]:store.encode(v) for k,v in documents().items()}
        self.previous=b'{"synthetic_previous":"whole output"}'
        self.data.update({k:self.previous for k in (m.HEAD,*m.ALIASES)})
        self.reads=[];self.writes=[];self.denied=False;self.corrupt=False;self.race=None;self.fail_retention=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and key.startswith(m.PRIVATE):raw+=b'corrupt'
        return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':store.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(kw)
        if self.fail_retention and key.startswith(m.PRIVATE):raise Error('AccessDenied')
        if self.race==key:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=store.sha(self.data.get(key,b'')):raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None):
    memory=memory or Memory();configs=[]
    scope={'Path':Path,'__file__':str(SRC/'lambda_function.py'),'boto3':SimpleNamespace(client=lambda *a,**k:memory),
        'Config':lambda **kw:configs.append(kw),'ContextStore':store.ContextStore,'context_evidence_store':store,'S3_BUCKET':'synthetic',
        'CONTRACT':m.CONTRACT,'PRIVATE':m.PRIVATE,'HEAD':m.HEAD,'INPUTS':m.INPUTS,'ALIASES':m.ALIASES,
        'MAX_BYTES':store.MAX_BYTES,'encode':store.encode,'code':store.code,'build_summary_evidence':m.build,'json':json,'gzip':gzip,'time':time}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('lambda_handler','_publish_summary_alias')]
    assert len(nodes)==2;exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated synthetic summary>','exec'),scope)
    return scope,memory,configs


class Tests(unittest.TestCase):
    def test_repaired_brief_flows_through_without_reopening_fallback(self):
        sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-pump-radar-brief/source'))
        from brief_evidence import build as brief_build
        from test_pump_brief_evidence import capture as brief_capture
        a,s=brief_capture();docs=documents();docs['brief']=brief_build(a,s,AT)
        a,s=capture(docs);p=m.build(a,s,AT)
        self.assertEqual(p['top_picks'],[]);self.assertIsNone(p['basket']['total_exposure']);self.assertIsNone(p['conviction'])
        self.assertEqual(p['coverage']['eligible_votes'],0);self.assertEqual(p['sources'][0]['status'],'received_object_unqualified')
    def test_abstention_cannot_reopen_portfolio_or_model_size(self):
        a,s=capture();p=m.build(a,s,AT)
        self.assertEqual(p['top_picks'],[]);self.assertEqual(p['suggested_additions'],[]);self.assertEqual(p['clusters'],[])
        for field in ('n_positions','n_pump_confirmed','total_exposure'):self.assertIsNone(p['basket'][field])
        self.assertIsNone(p['conviction']);self.assertIsNone(p['temperature']['score']);self.assertIsNone(p['early']['n_actionable'])
        self.assertIsNone(p['catalysts']['n_a_grade']);self.assertEqual(p['call'],'WAIT');self.assertEqual(p['call_semantics'],'abstain')
        for flag in m.FLAGS:self.assertIs(p[flag],False)
        for secret in (b'SYN_A',b'250%',b'position_pct',b'private_account',b'private only',b'BOOST'):self.assertNotIn(secret,store.encode(p))
    def test_complete_originals_dates_and_unknowns_remain_separate(self):
        a,s=capture();p=m.build(a,s,AT);self.assertEqual(p,m.build(a,s,AT));self.assertEqual(len(p['sources']),5)
        self.assertIsNone(p['sources_freshness']['early_seconds']);self.assertEqual(p['sources'][-1]['source_clock_status'],'future')
        self.assertIn(b'private_account',s[a['positioning']['original_ref']['key']]);self.assertIsNone(p['coverage']['independent_roots'])
        d=documents();d['positioning']={};d['brief']={'status':'error','error':'do not leak'};a,s=capture(d);p=m.build(a,s,AT)
        self.assertEqual(p['coverage']['structured_objects'],3);self.assertIsNone(p['basket']['n_positions'])
        self.assertNotIn('do not leak',store.encode(p).decode())
    def test_no_structured_inputs_preserve_previous_and_malformed_identity_fails(self):
        a,s=capture(dict.fromkeys(m.INPUTS,{}))
        with self.assertRaises(ValueError):m.build(a,s,AT)
        for change in (lambda a,s:a.pop('brief'),lambda a,s:a['brief'].update(source_key='private/accounts.json'),
                       lambda a,s:a['brief'].update(received_at='2099-01-01T00:00:00Z'),
                       lambda a,s:a['brief']['original_ref'].update(bytes=False),
                       lambda a,s:s.update({a['brief']['original_ref']['key']:b'wrong'})):
            a,s=capture();change(a,s)
            with self.assertRaises(ValueError):m.build(a,s,AT)
    def test_native_complete_replay_and_all_three_compatibility_outputs(self):
        ns,mem,cfg=native();result=ns['lambda_handler']({'head':'private/accounts.json'},None);self.assertEqual(result['statusCode'],200)
        p=json.loads(mem.data[m.HEAD]);manifest=json.loads(mem.data[p['replay']['input_ref']['key']])
        replay=m.build(manifest['attempts'],mem.data,manifest['generated_at'])
        for k,v in replay.items():self.assertEqual(p[k],v)
        self.assertEqual(len(manifest['source_files']),3);self.assertEqual(len(manifest['attempts']),5)
        self.assertEqual(manifest['limits']['acquisition_budget_s'],30);self.assertEqual(manifest['limits']['publication_budget_s'],40)
        self.assertEqual(gzip.decompress(mem.data[m.ALIASES[0]]),mem.data[m.HEAD]);self.assertEqual(mem.data[m.ALIASES[1]],mem.data[m.HEAD])
        for key in (m.HEAD,*m.ALIASES):self.assertEqual(mem.data[store.identity(mem.data[key],m.PRIVATE,'outputs')['key']],mem.data[key])
        self.assertEqual(mem.data[p['replay']['previous_publication']['key']],mem.previous)
        self.assertFalse(json.loads(result['body'])['atomic_across_keys']);self.assertEqual(cfg[0]['retries']['max_attempts'],0)
        self.assertTrue(all(k in (*m.INPUTS.values(),m.HEAD,*m.ALIASES) or k.startswith(m.PRIVATE) for k in mem.reads))
    def test_alias_race_is_honestly_partial_and_never_claims_all_heads_preserved(self):
        ns,mem,_=native();mem.race=m.ALIASES[0];r=ns['lambda_handler']({},None)
        self.assertEqual(r['statusCode'],503);body=json.loads(r['body']);self.assertIs(body['previous_primary_preserved'],False)
        self.assertEqual(mem.data[m.ALIASES[0]],mem.previous);self.assertNotEqual(mem.data[m.HEAD],mem.previous)
        self.assertEqual(body['compatibility_outputs_published'],[])
    def test_acquisition_retention_and_head_failures_preserve_all_existing_outputs(self):
        for setting,value in (('denied',True),('corrupt',True),('race',m.HEAD),('fail_retention',True)):
            ns,mem,_=native();setattr(mem,setting,value);r=ns['lambda_handler']({},None);self.assertEqual(r['statusCode'],503)
            for key in (m.HEAD,*m.ALIASES):self.assertEqual(mem.data[key],mem.previous)
        ns,mem,_=native()
        for key in m.INPUTS.values():mem.data.pop(key)
        self.assertEqual(ns['lambda_handler']({},None)['statusCode'],503);self.assertEqual(mem.data[m.HEAD],mem.previous)
    def test_original_runtime_and_shorter_declared_budgets(self):
        cfg=json.loads((SRC.parent/'config.json').read_bytes());original=json.loads((ROOT/'tests/fixtures/pre-prepump-summary-evidence-config.json.txt').read_bytes())
        for key in ('runtime','memory','timeout','handler','role_arn','environment','eventbridge_scheduler'):self.assertEqual(cfg[key],original[key])
        self.assertEqual(cfg['timeout'],60)
        kwargs={'client':Memory(),'bucket':'synthetic','head':m.HEAD,'inputs':m.INPUTS,'prefix':m.PRIVATE,'contract':m.CONTRACT,'compiler_paths':{'summary_evidence.py':SRC/'summary_evidence.py'}}
        for a,b in ((True,40),(0,40),(50,40),(30,251)):
            with self.assertRaises(ValueError):store.ContextStore(**kwargs,acquisition_budget_s=a,publication_budget_s=b)
        instance=store.ContextStore(**kwargs,acquisition_budget_s=30,publication_budget_s=40)
        with patch.object(store.time,'monotonic',side_effect=[0,31]):
            with self.assertRaises(ValueError):instance.publish(m.build)
        ns,mem,_=native()
        with self.assertRaises(ValueError):ns['_publish_summary_alias'](mem,instance,'private/accounts.json',b'{}',0)
        with patch.object(store.time,'monotonic',return_value=46):
            with self.assertRaises(ValueError):ns['_publish_summary_alias'](mem,instance,m.ALIASES[0],b'{}',0)
        self.assertEqual(mem.data[m.ALIASES[0]],mem.previous)


if __name__=='__main__':unittest.main(verbosity=2)
