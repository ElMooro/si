"""Synthetic source-roster replay and writer only; no runtime consumer inspection."""
from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from copy import deepcopy
import ast,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[2];SRC=ROOT/'aws/lambdas/justhodl-theme-cascade/source'
sys.path[:0]=[str(ROOT/'aws/shared'),str(SRC)]
import context_evidence_store as store
import cascade_evidence as m
import provider_flow_research,provider_flow_catalog
AT='2026-09-28T07:00:00Z'


def documents():
    docs={name:{'generated_at':AT,'private_note':'never publish','scores':[99]*2000} for name in m.INPUTS}
    docs['theme_rotation'].update(all_themes=[{'ticker':'ETF1','multiplier':3},{'ticker':'ETF1'},{'ticker':'ETF2'},None],
        breadth_details={'ETF1':{'constituents_perf':[{'symbol':'SYNA'},{'symbol':'SYNA'},{'symbol':'OTHER'}]},
                         'ETF2':{'constituents_perf':[{'symbol':'SYNA'},{'symbol':'../BAD'}]}})
    return docs


def capture(docs=None):
    docs=documents() if docs is None else docs;a={};sources={}
    for name,key in m.INPUTS.items():
        raw=store.encode(docs[name]);ref=store.identity(raw,m.PRIVATE,'sources');sources[ref['key']]=raw
        a[name]={'source_key':key,'status':'received','requested_at':AT,'received_at':AT,'original_ref':ref}
    return a,sources


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        self.data={m.INPUTS[k]:store.encode(v) for k,v in documents().items()};self.previous=b'{"whole_previous":"synthetic"}';self.data[m.HEAD]=self.previous
        self.reads=[];self.writes=[];self.fail_retention=False;self.lost_ack=False
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
    def client(name,**kw):clients.append(name);assert name=='s3';return mem
    ns={'Path':Path,'__file__':str(SRC/'lambda_function.py'),'boto3':SimpleNamespace(client=client),'Config':lambda **kw:kw,
        'S3_BUCKET':'synthetic','ContextStore':store.ContextStore,'context_evidence_store':store,'cascade_evidence':m,
        'provider_flow_research':provider_flow_research,'provider_flow_catalog':provider_flow_catalog,
        'time':time,'json':json,'encode':store.encode,'code':store.code,'now':lambda:AT}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());names=('lambda_handler','_cascade_read_macro','_get_telegram_config','_send_telegram_html','deliver_telegram_alerts')
    nodes=[next(n for n in reversed(tree.body) if isinstance(n,ast.FunctionDef) and n.name==name) for name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<synthetic Cascade writer>','exec'),ns)
    return ns,mem,clients


class Tests(unittest.TestCase):
    def test_all_occurrences_duplicates_and_source_coordinates_survive_without_ranking(self):
        a,s=capture();before=deepcopy((a,s));p=m.build(a,s,AT);self.assertEqual((a,s),before);self.assertEqual(p,m.build(a,s,AT))
        roster=p['reported_theme_roster'];self.assertEqual(roster['theme_rows'],4);self.assertEqual(roster['distinct_reported_etfs'],2)
        self.assertEqual(roster['membership_rows'],5);self.assertEqual(roster['distinct_reported_pairs'],3)
        self.assertEqual(roster['theme_occurrences'][1]['prior_occurrence_index'],0)
        self.assertEqual(roster['membership_occurrences'][1]['status'],'duplicate_reported_pair')
        self.assertEqual(roster['membership_occurrences'][1]['source_pointer'],'/breadth_details/ETF1/constituents_perf/1')
        self.assertEqual(roster['membership_occurrences'][-1]['status'],'invalid_literal_membership')
        for flag in m.FLAGS:self.assertIs(p[flag],False)
        for key in ('all_ranked','alert_tier','medium_tier','laggards_hot_themes'):self.assertEqual(p[key],[])
        self.assertIsNone(p['n_alert_tier']);self.assertEqual(p['call'],'WAIT')
        self.assertNotIn(b'never publish',store.encode(p));self.assertNotIn(b'position_sizing',store.encode(p))
    def test_missing_and_partial_memberships_do_not_become_zero(self):
        for value in (None,{'ETF1':{}},{'ETF1':{'constituents_perf':None}}):
            d=documents();d['theme_rotation']['breadth_details']=value;a,s=capture(d);r=m.build(a,s,AT)['reported_theme_roster']
            self.assertIsNone(r['membership_rows'])
        d=documents();d['theme_rotation'].update(all_themes=[],breadth_details={});a,s=capture(d);r=m.build(a,s,AT)['reported_theme_roster']
        self.assertEqual(r['theme_rows'],0);self.assertEqual(r['membership_rows'],0)
    def test_non_roster_source_and_nonfinite_input_are_explicitly_unavailable(self):
        a,s=capture();raw=b'{"all_themes":[],"bad":NaN}';ref=store.identity(raw,m.PRIVATE,'sources');s[ref['key']]=raw;a['theme_rotation']['original_ref']=ref
        p=m.build(a,s,AT);self.assertEqual(p['sources'][1]['status'],'invalid_json');self.assertIsNone(p['reported_theme_roster']['theme_rows'])
        a,s=capture({name:{'private_context':True} for name in m.INPUTS});self.assertIsNone(m.build(a,s,AT)['reported_theme_roster']['theme_rows'])
    def test_future_unknown_clocks_and_exact_duplicate_bodies_never_qualify_independence(self):
        d=documents();d['macro']['generated_at']='2099-01-01T00:00:00Z';d['momentum']['generated_at']='2026-09-28';a,s=capture(d);p=m.build(a,s,AT)
        by={r['source']:r for r in p['sources']};self.assertEqual(by['macro']['source_clock_status'],'future')
        self.assertEqual(by['momentum']['source_clock_status'],'unknown');self.assertIsNone(p['coverage']['independent_roots'])
        self.assertGreater(len(p['identical_payload_groups']),0);self.assertEqual(p['coverage']['eligible_votes'],0)
    def test_exposure_exclusion_is_declared_but_has_no_body_or_vote(self):
        p=m.build(*capture(),AT);row=p['sources'][-1]
        self.assertEqual(row['source_key'],m.EXCLUDED);self.assertEqual(row['status'],'not_read_existing_flow_exclusion')
        self.assertIsNone(row['original_ref']);self.assertEqual(p['coverage']['source_reads_planned'],6)
    def test_wrong_source_identity_scope_clock_and_all_unavailable_context_fail(self):
        for edit in (lambda a,s:a.pop('macro'),lambda a,s:a['macro'].update(source_key='private/accounts.json'),
                     lambda a,s:a['themes'].update(received_at='2099-01-01T00:00:00Z'),
                     lambda a,s:s.update({a['themes']['original_ref']['key']:b'wrong'})):
            a,s=capture();edit(a,s)
            with self.assertRaises(ValueError):m.build(a,s,AT)
        a,s=capture(dict.fromkeys(m.INPUTS,{}))
        with self.assertRaises(ValueError):m.build(a,s,AT)
    def test_native_replay_whole_source_compiler_prior_and_current_retention(self):
        ns,mem,clients=native();r=ns['lambda_handler']({'head':'private/accounts.json','notify':True},None);self.assertEqual(r['statusCode'],200)
        p=json.loads(mem.data[m.HEAD]);manifest=json.loads(mem.data[p['replay']['input_ref']['key']]);replay=m.build(manifest['attempts'],mem.data,manifest['generated_at'])
        for key,value in replay.items():self.assertEqual(p[key],value)
        self.assertEqual(len(manifest['source_files']),5);self.assertEqual(len(manifest['attempts']),6)
        self.assertEqual(mem.data[p['replay']['previous_publication']['key']],mem.previous)
        for name,ref in manifest['source_files'].items():
            path=SRC/name if name in ('lambda_function.py','cascade_evidence.py') else ROOT/'aws/shared'/name
            self.assertEqual(mem.data[ref['key']],path.read_bytes())
        self.assertEqual(mem.data[store.identity(mem.data[m.HEAD],m.PRIVATE,'outputs')['key']],mem.data[m.HEAD])
        self.assertNotIn(m.EXCLUDED,mem.reads);self.assertEqual(clients,['s3'])
        self.assertTrue(all(k in (*m.INPUTS.values(),m.HEAD) or k.startswith(m.PRIVATE) for k in mem.reads))
        self.assertTrue(all(w['Key']==m.HEAD or w['Key'].startswith(m.PRIVATE) for w in mem.writes))
    def test_before_publication_failure_and_commit_then_timeout_are_distinct(self):
        for setting in ('fail_retention','lost_ack'):
            ns,mem,_=native();setattr(mem,setting,True);r=ns['lambda_handler']({},None);self.assertEqual(r['statusCode'],503)
            if setting=='lost_ack':self.assertIsNone(json.loads(r['body'])['previous_publication_preserved']);self.assertNotEqual(mem.data[m.HEAD],mem.previous)
            else:self.assertIs(json.loads(r['body'])['previous_publication_preserved'],True);self.assertEqual(mem.data[m.HEAD],mem.previous)
            self.assertLessEqual(sum(w['Key']==m.HEAD for w in mem.writes),1)
    def test_notification_wrappers_and_credential_resolver_are_inactive(self):
        ns,mem,clients=native()
        for name in ('_get_telegram_config','_send_telegram_html','deliver_telegram_alerts'):
            with self.assertRaises(RuntimeError):ns[name]('synthetic')
        self.assertEqual(mem.writes,[]);self.assertEqual(clients,[])
        tree=ast.parse((SRC/'lambda_function.py').read_bytes());handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        calls={n.func.id for n in ast.walk(handler) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name)}
        self.assertFalse(calls & {'_legacy_lambda_handler','compute_position_sizing','deliver_telegram_alerts','managed_secret','_load_alert_state','_save_alert_state'})
        self.assertFalse(any(isinstance(n,ast.ImportFrom) and n.module=='managed_secret' for n in tree.body))
    def test_original_runtime_and_schedule_preserved(self):
        cfg=json.loads((SRC.parent/'config.json').read_bytes());old=json.loads((ROOT/'tests/fixtures/pre-cascade-evidence-config.json.txt').read_bytes())
        self.assertEqual({k:v for k,v in cfg.items() if k!='description'},{k:v for k,v in old.items() if k!='description'})


if __name__=='__main__':unittest.main(verbosity=2)
