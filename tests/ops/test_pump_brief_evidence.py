"""Synthetic whole-source/replay/privacy/race regressions; no native or model I/O."""
from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
from copy import deepcopy
from types import SimpleNamespace
import ast,gzip,json,sys,time,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[2];SRC=ROOT/'aws/lambdas/justhodl-pump-radar-brief/source'
sys.path[:0]=[str(SRC),str(ROOT/'aws/shared')]
import context_evidence_store as store
import brief_evidence as m
AT='2026-09-28T01:00:00Z'


def documents():
    return {name:{'generated_at':AT,'private_note':'secret_'+name,'account':{'weight_pct':99},'score':100,
                  'measurement_contract':m.KNOWN.get(name,'unqualified_context'),'payload':['complete']*4000}
            for name in m.INPUTS}


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
        self.previous=b'{"synthetic_previous":"complete output"}'
        self.data[m.HEAD]=self.previous;self.reads=[];self.writes=[]
        self.denied=False;self.corrupt=False;self.race=False;self.bad_length=False;self.fail_retention=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and key.startswith(m.PRIVATE):raw+=b'corrupt'
        return {'Body':BytesIO(raw),'ContentLength':len(raw)+(1 if self.bad_length else 0),'ETag':store.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(kw)
        if self.fail_retention and key.startswith(m.PRIVATE):raise Error('AccessDenied')
        if self.race and key==m.HEAD:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=store.sha(self.data.get(key,b'')):raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None):
    memory=memory or Memory()
    scope={'Path':Path,'__file__':str(SRC/'lambda_function.py'),'boto3':SimpleNamespace(client=lambda *a,**k:memory),
        'Config':lambda **kw:kw,'ContextStore':store.ContextStore,'context_evidence_store':store,'S3_BUCKET':'synthetic-only',
        'CONTRACT':m.CONTRACT,'PRIVATE':m.PRIVATE,'HEAD':m.HEAD,'INPUTS':m.INPUTS,'build_brief_evidence':m.build,'json':json}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('lambda_handler','call_anthropic')]
    assert len(nodes)==2
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated synthetic native>','exec'),scope)
    return scope,memory


class Tests(unittest.TestCase):
    def test_deterministic_no_score_no_exposure_feedback_no_model_or_actions(self):
        a,s=capture();p=m.build(a,s,AT);self.assertEqual(p,m.build(a,s,AT))
        self.assertEqual(p['call'],'WAIT');self.assertEqual(p['call_semantics'],'abstain');self.assertTrue(p['brief_markdown'].endswith('**WAIT**'))
        self.assertIsNone(p['model']);self.assertEqual(p['model_requests'],0);self.assertEqual(p['notifications_sent'],0)
        self.assertIsNone(p['conviction_grade']);self.assertIsNone(p['market_temperature']['score']);self.assertIsNone(p['market_temperature']['rank'])
        self.assertEqual(p['top_3_long_ideas'],[]);self.assertEqual(p['top_2_pair_trades'],[])
        self.assertEqual(p['coverage']['eligible_votes'],0);self.assertIsNone(p['coverage']['independent_roots'])
        for record in [p,*p['sources']]:
            for flag in m.FLAGS:self.assertIs(record[flag],False)
    def test_public_projection_never_copies_private_account_text_or_provider_labels(self):
        d=documents();d['positioning']['measurement_contract']='private_contract';a,s=capture(d);p=m.build(a,s,AT)
        body=store.encode(p)
        for value in (b'secret_',b'private_contract',b'weight_pct',b'account',b'payload',b'completecomplete'):
            self.assertNotIn(value,body)
        self.assertTrue(all(len(raw)>10000 for raw in s.values()));self.assertIn(b'weight_pct',next(iter(s.values())))
    def test_clocks_are_reported_publication_age_not_observation_freshness(self):
        d=documents();d['positioning']['generated_at']='2099-01-01T00:00:00Z';d['pairs']['generated_at']='2026-09-28T00:00:00'
        a,s=capture(d);p=m.build(a,s,AT);by={r['source']:r for r in p['sources']}
        self.assertEqual(by['positioning']['source_clock_status'],'future');self.assertIsNone(by['positioning']['source_generation_age_seconds'])
        self.assertIsNone(by['pairs']['source_generated_at']);self.assertEqual(by['pairs']['source_clock_status'],'unknown')
        self.assertEqual(by['momentum']['source_generation_age_seconds'],0);self.assertEqual(by['momentum']['observation_freshness'],'unqualified')
    def test_every_original_identity_and_fixed_input_and_clock_is_checked(self):
        for mutate in (lambda a,s:a.pop('pairs'),lambda a,s:a['pairs'].update(source_key='data/other.json'),
                       lambda a,s:a['pairs'].update(received_at='2099-01-01T00:00:00Z'),
                       lambda a,s:a['pairs'].update(requested_at='no clock'),
                       lambda a,s:a['pairs']['original_ref'].update(bytes=True),
                       lambda a,s:s.update({a['pairs']['original_ref']['key']:b'wrong'})):
            a,s=capture();mutate(a,s)
            with self.assertRaises(ValueError):m.build(a,s,AT)
    def test_parse_errors_empty_objects_and_unavailable_are_not_measured_zero(self):
        d=documents();d['pairs']={};d['analytics']={'status':'error','error':'private error'};a,s=capture(d)
        raw=b'{"duplicate":1,"duplicate":2}';ref=store.identity(raw,m.PRIVATE,'sources');a['mechanics']['original_ref']=ref;s[ref['key']]=raw
        a['early']={k:v for k,v in a['early'].items() if k not in ('original_ref','content_encoding')};a['early']['status']='source_read_unavailable'
        p=m.build(a,s,AT);by={r['source']:r for r in p['sources']}
        self.assertEqual(by['pairs']['status'],'unrecognized_shape');self.assertEqual(by['analytics']['status'],'source_error')
        self.assertEqual(by['mechanics']['status'],'invalid_json');self.assertEqual(by['early']['status'],'source_read_unavailable')
        self.assertEqual(p['coverage']['structured_objects'],9);self.assertEqual(p['coverage']['whole_received'],12)
        a,s=capture(dict.fromkeys(m.INPUTS,{}))
        with self.assertRaises(ValueError):m.build(a,s,AT)
    def test_whole_bounded_gzip_and_strict_json(self):
        raw=b'{"zero":0,"false":false}';self.assertEqual(store.strict(gzip.compress(raw),'gzip'),{'zero':0,'false':False})
        for bad,encoding in [(gzip.compress(raw)[:-3],'gzip'),(gzip.compress(raw)+gzip.compress(raw),'gzip'),(raw,'gzip'),(b'{"v":NaN}',''),(b'{"v":1e999}',''),(b'{"v":1,"v":2}','')]:
            with self.assertRaises(ValueError):store.strict(bad,encoding)
        with patch.object(store,'MAX_BYTES',100):
            with self.assertRaises(ValueError):store.strict(gzip.compress(b'x'*1000))
    def test_native_retains_complete_previous_inputs_compilers_and_replays_exactly(self):
        scope,mem=native();result=scope['lambda_handler']({'source_key':'private/accounts.json'},None)
        self.assertEqual(result['statusCode'],200);p=json.loads(mem.data[m.HEAD]);manifest=json.loads(mem.data[p['replay']['input_ref']['key']])
        self.assertEqual(mem.data[p['replay']['previous_publication']['key']],mem.previous)
        self.assertEqual(len(manifest['attempts']),13);self.assertEqual(len(manifest['source_files']),3)
        replay=m.build(manifest['attempts'],mem.data,manifest['generated_at'])
        for k,v in replay.items():self.assertEqual(p[k],v)
        for name,ref in manifest['source_files'].items():
            path=SRC/name if name!='context_evidence_store.py' else ROOT/'aws/shared'/name
            self.assertEqual(mem.data[ref['key']],path.read_bytes())
        for row in manifest['attempts'].values():self.assertEqual(mem.data[row['original_ref']['key']],mem.data[row['source_key']])
        archive=store.identity(mem.data[m.HEAD],m.PRIVATE,'outputs');self.assertEqual(mem.data[archive['key']],mem.data[m.HEAD])
        self.assertFalse(any(k.startswith('private/') for k in mem.reads));self.assertFalse(any(w['Key'].startswith('data/archive/') for w in mem.writes))
    def test_retention_corruption_denial_length_race_and_budget_preserve_previous(self):
        for setting in ('corrupt','denied','bad_length','race','fail_retention'):
            scope,mem=native();setattr(mem,setting,True);result=scope['lambda_handler']({},None)
            self.assertEqual(result['statusCode'],503,setting);self.assertEqual(mem.data[m.HEAD],mem.previous,setting)
        scope,mem=native()
        with patch.object(store,'MAX_TOTAL',2):result=scope['lambda_handler']({},None)
        self.assertEqual(result['statusCode'],503);self.assertEqual(mem.data[m.HEAD],mem.previous)
        scope,mem=native()
        with patch.object(store.time,'monotonic',side_effect=[0,231]):result=scope['lambda_handler']({},None)
        self.assertEqual(result['statusCode'],503);self.assertEqual(mem.data[m.HEAD],mem.previous)
    def test_first_head_and_content_address_collision_readback(self):
        scope,mem=native();mem.data.pop(m.HEAD);self.assertEqual(scope['lambda_handler']({},None)['statusCode'],200)
        first=json.loads(mem.data[m.HEAD]);self.assertIsNone(first['replay']['previous_publication'])
        self.assertEqual(scope['lambda_handler']({},None)['statusCode'],200)
        writes=[w for w in mem.writes if w['Key']==m.HEAD];self.assertEqual(writes[0]['IfNoneMatch'],'*');self.assertIn('IfMatch',writes[1])
    def test_unacknowledged_head_may_have_committed_and_is_never_retried(self):
        for committed in (False,True):
            class LostAcknowledgement(Memory):
                def put_object(self,**kw):
                    if kw['Key']==m.HEAD and not committed:
                        self.writes.append(kw);raise Error('RequestTimeout')
                    super().put_object(**kw)
                    if kw['Key']==m.HEAD:raise Error('RequestTimeout')
            ns,mem=native(LostAcknowledgement());r=ns['lambda_handler']({},None);body=json.loads(r['body'])
            self.assertEqual(r['statusCode'],503);self.assertIsNone(body['previous_publication_preserved'])
            self.assertEqual(body['publication_status'],'acknowledgement_unknown')
            self.assertEqual(mem.data[m.HEAD]!=mem.previous,committed)
            self.assertEqual(sum(w['Key']==m.HEAD for w in mem.writes),1)
            self.assertEqual(mem.reads.count(m.HEAD),1)
            if committed:
                self.assertEqual(mem.data[store.identity(mem.data[m.HEAD],m.PRIVATE,'outputs')['key']],mem.data[m.HEAD])
    def test_prepublication_failure_and_competing_writer_are_distinct(self):
        ns,mem=native();mem.fail_retention=True;r=ns['lambda_handler']({},None);body=json.loads(r['body'])
        self.assertIs(body['previous_publication_preserved'],True)
        self.assertEqual(body['publication_status'],'head_write_not_attempted')
        self.assertEqual(body['preservation_scope'],'this_attempt_only')
        self.assertFalse(any(w['Key']==m.HEAD for w in mem.writes))
        class Competitor(Memory):
            def put_object(self,**kw):
                if kw['Key']==m.HEAD:self.data[m.HEAD]=b'{"synthetic_competitor":true}'
                super().put_object(**kw)
        ns,mem=native(Competitor());r=ns['lambda_handler']({},None);body=json.loads(r['body'])
        self.assertIsNone(body['previous_publication_preserved']);self.assertEqual(mem.data[m.HEAD],b'{"synthetic_competitor":true}')
        self.assertEqual(sum(w['Key']==m.HEAD for w in mem.writes),1)
    def test_declared_scope_rejects_arbitrary_reads_and_bad_archive_names(self):
        obj=store.ContextStore(Memory(),'synthetic',m.HEAD,m.INPUTS,m.PRIVATE,m.CONTRACT,{'brief_evidence.py':SRC/'brief_evidence.py'})
        for key in ('data/accounts.json','private/accounts.json',m.PRIVATE+'../escape'):
            with self.assertRaises(ValueError):obj.read(key)
        for prefix in ('data/public/',m.PRIVATE+'../'):
            with self.assertRaises(ValueError):store.identity(b'x',prefix,'sources')
    def test_no_paid_helper_or_env_in_active_package_and_original_runtime_preserved(self):
        cfg=json.loads((SRC.parent/'config.json').read_bytes());original=json.loads((ROOT/'tests/fixtures/pre-pump-brief-evidence-config.json.txt').read_bytes())
        self.assertNotIn('inherit_env',cfg)
        for key in ('runtime','memory','timeout','handler','role_arn','environment','eventbridge_scheduler'):self.assertEqual(cfg[key],original[key])
        tree=ast.parse((SRC/'lambda_function.py').read_bytes())
        imports={a.name for n in ast.walk(tree) if isinstance(n,ast.Import) for a in n.names}
        self.assertFalse(imports & {'anthropic_shim','llm_router','anthropic'})
        scope,mem=native()
        with self.assertRaises(RuntimeError):scope['call_anthropic']('synthetic','synthetic')
        self.assertEqual(mem.writes,[])
    def test_summary_consumer_remains_type_compatible_but_its_position_fallback_is_not_qualified(self):
        a,s=capture();brief=m.build(a,s,AT);raw={'brief':brief,'positioning':{'aggressive_basket':{'positions':[{'ticker':'SYN_A','position_pct':99}]}}}
        outputs={};tree=ast.parse((ROOT/'tests/fixtures/pre-prepump-summary-evidence.py.txt').read_bytes())
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        scope={'time':time,'datetime':datetime,'timezone':timezone,'INPUTS':{k:k for k in ('brief','positioning','catalysts','clusters','early')},'json':json,
            'load_s3_json':lambda key:raw.get(key),'freshness_seconds':lambda _:None,'S3_BUCKET':'synthetic',
            'OUTPUT_KEY':'data/pump-radar-summary.json','put_gzipped':lambda *a,**k:0,
            's3':SimpleNamespace(put_object=lambda **kw:outputs.update({kw['Key']:json.loads(kw['Body'])})),'print':lambda *a,**k:None}
        exec(compile(ast.Module(body=[node],type_ignores=[]),'<isolated summary compatibility>','exec'),scope)
        scope['lambda_handler']({},None);out=outputs['data/pump-radar-summary.json']
        self.assertIsNone(out['temperature']['score']);self.assertIsNone(out['temperature']['label']);self.assertEqual(out['conviction'],'—')
        # This is an independently unqualified consumer, not evidence of sizing acceptance.
        self.assertEqual(out['top_picks'][0]['position_pct'],99)


if __name__=='__main__':unittest.main(verbosity=2)
