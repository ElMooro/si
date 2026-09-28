"""Synthetic original retention, abstention, duplicate identity and replay only."""
from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];SRC=ROOT/'aws/lambdas/justhodl-apex-fusion/source'
sys.path[:0]=[str(ROOT/'aws/shared'),str(SRC)]
import context_evidence_store as store
import apex_evidence as m
AT='2026-09-28T01:00:00Z'


def documents():return {name:{'generated_at':AT,'private_scorecard':['original context']*2000,'score':100,'private_note':'never publish',
                             'performance_multiplier':1.5,'PROMOTED':True,'top':[{'ticker':'SYNA','private_position_pct':99}]} for name in m.INPUTS}


def capture(docs=None):
    docs=documents() if docs is None else docs;attempts={};sources={}
    for name,key in m.INPUTS.items():
        raw=store.encode(docs[name]);ref=store.identity(raw,m.PRIVATE,'sources');sources[ref['key']]=raw
        attempts[name]={'source_key':key,'status':'received','requested_at':AT,'received_at':AT,'original_ref':ref}
    return attempts,sources


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.data={m.INPUTS[k]:store.encode(v) for k,v in documents().items()};self.previous=b'{"whole_previous":"synthetic"}'
        self.data[m.HEAD]=self.previous;self.reads=[];self.writes=[];self.fail_retention=False;self.lost_ack=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key];return {'Body':BytesIO(raw),'ContentLength':len(raw),'ETag':store.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(kw)
        if self.fail_retention and key.startswith(m.PRIVATE):raise Error('AccessDenied')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=store.sha(self.data.get(key,b'')):raise Error('PreconditionFailed')
        self.data[key]=kw['Body']
        if self.lost_ack and key==m.HEAD:raise Error('RequestTimeout')


def native():
    mem=Memory();clients=[]
    def client(name,**kwargs):clients.append(name);assert name=='s3';return mem
    ns={'Path':Path,'__file__':str(SRC/'lambda_function.py'),'boto3':SimpleNamespace(client=client),'Config':lambda **kw:kw,
        'BUCKET':'synthetic','ContextStore':store.ContextStore,'context_evidence_store':store,
        'CONTRACT':m.CONTRACT,'HEAD':m.HEAD,'PRIVATE':m.PRIVATE,'INPUTS':m.INPUTS,'build_apex_evidence':m.build,'json':json}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('lambda_handler','_tg')]
    # Two _tg definitions are retained; execute only the final disabled wrapper.
    nodes=[next(n for n in reversed(nodes) if n.name=='_tg'),next(n for n in nodes if n.name=='lambda_handler')]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated synthetic Apex writer>','exec'),ns)
    return ns,mem,clients


class Tests(unittest.TestCase):
    def test_unqualified_scores_cannot_become_weights_probabilities_or_inversions(self):
        a,s=capture();p=m.build(a,s,AT);self.assertEqual(p,m.build(a,s,AT))
        self.assertEqual(p['top'],[]);self.assertEqual(p['weights_used'],{});self.assertEqual(p['weight_sources'],{})
        self.assertEqual(p['by_tier'],{});self.assertIsNone(p['n_scored']);self.assertIsNone(p['n_universe'])
        self.assertIs(p['tier_inversion']['active'],False);self.assertIsNone(p['tier_inversion']['alert_tier_hit_pct'])
        self.assertEqual(p['call'],'WAIT');self.assertEqual(p['call_semantics'],'abstain');self.assertEqual(p['coverage']['eligible_votes'],0)
        for flag in m.FLAGS:self.assertIs(p[flag],False)
        for secret in (b'private_scorecard',b'private_position_pct',b'never publish',b'PROMOTED',b'SYNA'):self.assertNotIn(secret,store.encode(p))
    def test_identical_whole_bodies_are_detected_but_distinctness_is_not_independence(self):
        a,s=capture();p=m.build(a,s,AT);self.assertEqual(p['coverage']['whole_received'],9)
        self.assertEqual(p['coverage']['distinct_received_payloads'],1);self.assertEqual(p['identical_payload_groups'][0]['sources'],list(m.INPUTS))
        self.assertIsNone(p['coverage']['independent_roots'])
        docs=documents();docs['flow']['score']=90;a,s=capture(docs);p=m.build(a,s,AT)
        self.assertEqual(p['coverage']['distinct_received_payloads'],2);self.assertEqual(p['coverage']['eligible_votes'],0)
    def test_future_and_naive_clocks_do_not_gain_freshness(self):
        docs=documents();docs['scorecard']['generated_at']='2099-01-01T00:00:00Z';docs['regime']['generated_at']='2026-09-28T00:00:00'
        a,s=capture(docs);p=m.build(a,s,AT);rows={r['source']:r for r in p['sources']}
        self.assertEqual(rows['scorecard']['source_clock_status'],'future');self.assertIsNone(rows['scorecard']['source_generation_age_seconds'])
        self.assertEqual(rows['regime']['source_clock_status'],'unknown');self.assertIsNone(rows['regime']['source_generated_at'])
        self.assertTrue(all(r['observation_freshness']=='unqualified' for r in rows.values()))
    def test_nonfinite_invalid_and_error_packets_are_not_valid_performance_evidence(self):
        a,s=capture();raw=b'{"performance_multiplier":NaN}';ref=store.identity(raw,m.PRIVATE,'sources');s[ref['key']]=raw;a['scorecard']['original_ref']=ref
        p=m.build(a,s,AT);self.assertEqual(p['sources'][0]['status'],'invalid_json');self.assertEqual(p['coverage']['structured_objects'],8)
        docs=documents();docs['scorecard']={'error':'private failure'};a,s=capture(docs);p=m.build(a,s,AT)
        self.assertEqual(p['sources'][0]['status'],'source_error');self.assertNotIn(b'private failure',store.encode(p))
    def test_missing_whole_context_or_wrong_identities_clocks_and_scope_fail(self):
        a,s=capture(dict.fromkeys(m.INPUTS,{}))
        with self.assertRaises(ValueError):m.build(a,s,AT)
        for mutate in (lambda a,s:a.pop('regime'),lambda a,s:a['regime'].update(source_key='private/accounts.json'),
                       lambda a,s:a['flow'].update(received_at='2099-01-01T00:00:00Z'),
                       lambda a,s:a['flow']['original_ref'].update(bytes=True),
                       lambda a,s:s.update({a['flow']['original_ref']['key']:b'wrong'})):
            a,s=capture();mutate(a,s)
            with self.assertRaises(ValueError):m.build(a,s,AT)
    def test_native_replays_all_nine_complete_sources_and_retains_compilers_prior_current(self):
        ns,mem,clients=native();r=ns['lambda_handler']({'head':'private/accounts.json'},None);self.assertEqual(r['statusCode'],200)
        p=json.loads(mem.data[m.HEAD]);manifest=json.loads(mem.data[p['replay']['input_ref']['key']]);replay=m.build(manifest['attempts'],mem.data,manifest['generated_at'])
        for k,v in replay.items():self.assertEqual(p[k],v)
        self.assertEqual(len(manifest['attempts']),9);self.assertEqual(len(manifest['source_files']),3)
        self.assertEqual(manifest['limits']['acquisition_budget_s'],60);self.assertEqual(manifest['limits']['publication_budget_s'],85)
        self.assertEqual(mem.data[p['replay']['previous_publication']['key']],mem.previous)
        for name,ref in manifest['source_files'].items():
            path=SRC/name if name!='context_evidence_store.py' else ROOT/'aws/shared'/name
            self.assertEqual(mem.data[ref['key']],path.read_bytes())
        self.assertEqual(mem.data[store.identity(mem.data[m.HEAD],m.PRIVATE,'outputs')['key']],mem.data[m.HEAD])
        self.assertEqual(clients,['s3']);self.assertTrue(all(k in (*m.INPUTS.values(),m.HEAD) or k.startswith(m.PRIVATE) for k in mem.reads))
    def test_retention_failure_and_lost_acknowledgement_are_distinct(self):
        for setting in ('fail_retention','lost_ack'):
            ns,mem,_=native();setattr(mem,setting,True);r=ns['lambda_handler']({},None);self.assertEqual(r['statusCode'],503);body=json.loads(r['body'])
            if setting=='lost_ack':self.assertIsNone(body['previous_publication_preserved']);self.assertNotEqual(mem.data[m.HEAD],mem.previous)
            else:self.assertIs(body['previous_publication_preserved'],True);self.assertEqual(mem.data[m.HEAD],mem.previous)
            self.assertLessEqual(sum(w['Key']==m.HEAD for w in mem.writes),1)
    def test_notification_wrapper_disabled_and_active_handler_has_no_grading_ledger_or_model_calls(self):
        ns,mem,_=native()
        with self.assertRaises(RuntimeError):ns['_tg']('synthetic')
        self.assertEqual(mem.writes,[])
        tree=ast.parse((SRC/'lambda_function.py').read_bytes());handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        names={n.func.id for n in ast.walk(handler) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)}
        self.assertFalse(names & {'_legacy_lambda_handler','learned_weights','tier_inversion','_tg','managed_secret'})
        self.assertFalse(any(isinstance(n,ast.ImportFrom) and n.module=='managed_secret' for n in tree.body))


if __name__=='__main__':unittest.main(verbosity=2)
