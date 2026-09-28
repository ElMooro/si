"""Synthetic compiler/native tests with complete originals and no provider/model I/O."""
from pathlib import Path
from datetime import datetime,timezone
from io import BytesIO
from copy import deepcopy
from typing import Dict,List
import ast,gzip,json,re,sys,time,unittest
ROOT=Path(__file__).resolve().parents[2];SRC=ROOT/'aws/lambdas/justhodl-catalyst-classifier/source'
sys.path.insert(0,str(SRC))
import catalyst_context as m
AT='2026-09-28T01:00:00Z'


def documents():
    return {'positioning':{'aggressive_basket':{'positions':[{'ticker':'SYN_A','position_pct':10,'private_account_note':'do not publish'}, {'ticker':'SYN_A','position_pct':20}]}},
        'momentum':{'leaders':[{'ticker':'SYN_B','momentum_score':99},{'ticker':'SYN_A','momentum_score':1}]},
        'research':{'research':{'SYN_A':{'ticker':'SYN_A','private_dossier':'complete original preserved'}}},
        'nlp':{'research':{'SYN_A':{'tone_trajectory':'private source text'}}},
        'earnings_cal':{'upcoming_14d':[{'ticker':'SYN_A','earnings_date':'2026-10-01','eps_consensus':999}, {'ticker':'SYN_A','earnings_date':'2026-10-02'}]},
        'themes':{'ticker_to_theme':{'SYN_A':'theme-one'},'themes':{'theme-one':{'label':'unqualified thesis'}}},
        'mechanics':{'candidates':[{'ticker':'SYN_A','arbitrary_original_field':[1,2,3]}]}}


def capture(docs=None):
    docs=documents() if docs is None else docs;sources={};attempts={}
    for name,key in m.INPUTS.items():
        raw=m.encode(docs[name]);ref=m.identity(raw,'sources');sources[ref['key']]=raw
        attempts[name]={'source_key':key,'status':'received','original_ref':ref,'content_encoding':'','requested_at':AT,'received_at':AT}
    return attempts,sources


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self,docs=None):
        docs=documents() if docs is None else docs
        self.data={m.INPUTS[name]:m.encode(doc) for name,doc in docs.items()}
        self.previous=b'{"synthetic_previous":"whole output"}'
        self.data[m.HEAD]=self.previous
        self.reads=[];self.writes=[];self.corrupt=False;self.race=False;self.denied=False;self.declared_delta=0
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if self.denied:raise Error('AccessDenied')
        if key not in self.data:raise Error('NoSuchKey')
        raw=self.data[key]
        if self.corrupt and key.startswith(m.PRIVATE):raw+=b'corrupted'
        return {'Body':BytesIO(raw),'ETag':m.sha(raw),'ContentLength':len(raw)+self.declared_delta}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key)
        if key==m.HEAD and self.race:raise Error('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and kw['IfMatch']!=m.sha(self.data.get(key,b'')):raise Error('PreconditionFailed')
        self.data[key]=kw['Body']


def native(memory=None):
    memory=memory or Memory()
    scope={'s3':memory,'S3_BUCKET':'synthetic-only','INPUT_KEYS':m.INPUTS,'time':time,'datetime':datetime,'timezone':timezone,
        'Path':Path,'__file__':str(SRC/'lambda_function.py'),'re':re,'json':json,'build_context_research':m.build,
        **{key:getattr(m,key) for key in ('CONTRACT','PRIVATE','HEAD','MAX_BYTES','MAX_TOTAL','FLAGS','INPUTS','sha','encode','decode','identity','valid_identity')}}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes())
    names=('_context_error_code','_context_clock','_context_read','_context_retain','lambda_handler','call_anthropic')
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    assert len(nodes)==len(names)
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated deterministic native>','exec'),scope)
    return scope,memory


class Tests(unittest.TestCase):
    def assert_no_authority(self,p):
        self.assertEqual(p['model_requests'],0);self.assertEqual(p['notifications_sent'],0);self.assertEqual(p['n_classified'],0)
        self.assertIsNone(p['model']);self.assertIsNone(p['call']);self.assertEqual(p['flagged'],[])
        self.assertTrue(all(value==[] for value in p['by_grade'].values()))
        for obj in [p,*p['catalysts']]:
            for flag in m.FLAGS:self.assertIs(obj[flag],False)
        for r in p['catalysts']:
            for key in ('primary_catalyst','catalyst_type','catalyst_grade','catalyst_date','days_to_catalyst','thesis_durability','invalidation','claude_reasoning'):
                self.assertIsNone(r[key])
            self.assertEqual(r['secondary_catalysts'],[])
    def test_missing_model_cannot_become_d_grade_month_horizon_or_wrong_issuer(self):
        a,s=capture();p=m.build(a,s,AT);self.assert_no_authority(p)
        self.assertEqual([r['ticker'] for r in p['catalysts']],['SYN_A','SYN_B'])
        for key,row in p['ticker_to_catalyst'].items():self.assertEqual(key,row['ticker'])
    def test_complete_private_sources_and_exact_all_occurrence_coordinates(self):
        a,s=capture();p=m.build(a,s,AT);r=p['catalysts'][0]
        pos=next(v for v in r['source_evidence'] if v['source']=='positioning')
        self.assertEqual(pos['source_pointers'],['/aggressive_basket/positions/0','/aggressive_basket/positions/1'])
        self.assertEqual(len(r['reported_calendar_observations']),2)
        wire=m.encode(p)
        for secret in (b'position_pct',b'private_account_note',b'do not publish',b'private_dossier',b'complete original preserved',b'private source text',b'eps_consensus'):
            self.assertNotIn(secret,wire)
        self.assertIn(b'private_account_note',s[a['positioning']['original_ref']['key']])
    def test_original_scope_bounds_and_duplicate_membership_retained(self):
        d=documents();d['positioning']['aggressive_basket']['positions']=[{'ticker':'P'+str(i)} for i in range(20)]
        d['momentum']['leaders']=[{'ticker':'M'+str(i)} for i in range(15)]
        u=m.universe(d);self.assertEqual(u['selected_tickers'],['P'+str(i) for i in range(15)])
        self.assertEqual(len(u['occurrences']),35);self.assertEqual(sum(r['selected'] for r in u['occurrences']),15)
        self.assertEqual(u['occurrences'][-1]['status'],'outside_original_source_window')
        with self.assertRaises(ValueError):m.universe(d,30)
    def test_empty_selection_is_distinct_from_unavailable_source(self):
        d=documents();d['positioning']={};d['momentum']={}
        a,s=capture(d)
        with self.assertRaises(ValueError):m.build(a,s,AT)
        d['momentum']={'measurement_contract':'leader-price-observations.v1','leaders':[]}
        a,s=capture(d);p=m.build(a,s,AT)
        self.assertEqual(p['universe_membership']['status'],'reported_empty_selection');self.assertEqual(p['catalysts'],[])
    def test_missing_source_cannot_borrow_another_ticker_or_default_grade(self):
        d=documents();d['research']['research']['SYN_A']['ticker']='SYN_B'
        a,s=capture(d);p=m.build(a,s,AT);r=p['catalysts'][0]
        evidence=next(v for v in r['source_evidence'] if v['source']=='research')
        self.assertEqual(evidence['status'],'conflicting_keyed_ticker');self.assertEqual(evidence['source_pointers'],[])
        self.assert_no_authority(p)
    def test_exact_literal_calendar_dates_only_and_no_causal_claim(self):
        ref=m.identity(b'{}','sources')
        for value in (None,True,1,'2099-01-01junk','2026-02-30','01/01/2026'):
            r=m.calendar_observation({'earnings_date':value},'/upcoming_14d/0',ref)
            self.assertEqual(r['status'],'unavailable_or_invalid_date');self.assertFalse(r['earnings_beat_verified'])
        r=m.calendar_observation({'earnings_date':'2026-09-28'},'/upcoming_14d/0',ref)
        self.assertEqual(r['reported_date'],'2026-09-28');self.assertFalse(r['event_occurred_verified'])
    def test_parser_preserves_whole_gzip_and_rejects_duplicate_nonfinite_or_truncated_json(self):
        raw=m.encode(documents()['positioning']);self.assertEqual(m.decode(gzip.compress(raw),'gzip'),json.loads(raw))
        for raw,encoding in [(b'{"a":1,"a":2}',''),(b'{"a":NaN}',''),(b'{"a":1e999}',''),(gzip.compress(b'{}')+gzip.compress(b'{}'),'gzip'),(b'{}','gzip'),(gzip.compress(b'{}')[:-3],'gzip')]:
            with self.assertRaises((ValueError,UnicodeError)):m.decode(raw,encoding)
    def test_tampered_missing_source_and_wrong_identity_fail(self):
        a,s=capture();a['positioning']['original_ref']['sha256']='0'*64
        with self.assertRaises(ValueError):m.build(a,s,AT)
        a,s=capture();s[a['momentum']['original_ref']['key']]+=b' '
        with self.assertRaises(ValueError):m.build(a,s,AT)
        a,s=capture();del a['nlp']
        with self.assertRaises(ValueError):m.build(a,s,AT)
    def test_future_naive_or_reversed_collection_clocks_fail(self):
        for bad in ('2099-01-01T00:00:00Z','2026-09-28T01:00:00','not a date'):
            a,s=capture();a['positioning']['requested_at']=bad
            with self.assertRaises(ValueError):m.build(a,s,AT)
    def test_real_native_publishes_only_redacted_packet_and_protected_whole_history(self):
        ns,store=native();saved=deepcopy(store.data)
        result=ns['lambda_handler']({},None);self.assertEqual(result['statusCode'],200)
        p=m.decode(store.data[m.HEAD]);self.assert_no_authority(p)
        self.assertEqual(store.data[m.identity(store.previous,'outputs')['key']],store.previous)
        self.assertEqual(store.data[m.identity(store.data[m.HEAD],'outputs')['key']],store.data[m.HEAD])
        for name,key in m.INPUTS.items():self.assertEqual(store.data[m.identity(saved[key],'sources')['key']],saved[key])
        self.assertTrue(all(key==m.HEAD or key.startswith(m.PRIVATE) for key in store.writes))
        inputs=m.decode(store.data[p['replay']['input_ref']['key']]);sources={a['original_ref']['key']:store.data[a['original_ref']['key']] for a in inputs['attempts'].values() if a['status']=='received'}
        replay=m.build(inputs['attempts'],sources,p['generated_at'])
        self.assertTrue(all(p.get(k)==v for k,v in replay.items()))
    def test_retention_corruption_access_failure_and_conditional_race_preserve_prior_head(self):
        for mode in ('corrupt','denied','race'):
            store=Memory();setattr(store,mode,True);ns,store=native(store)
            self.assertEqual(ns['lambda_handler']({},None)['statusCode'],503);self.assertEqual(store.data[m.HEAD],store.previous)
    def test_missing_primary_sources_or_incorrect_stored_length_preserves_prior_head(self):
        store=Memory();del store.data[m.INPUTS['positioning']];del store.data[m.INPUTS['momentum']]
        ns,_=native(store);self.assertEqual(ns['lambda_handler']({},None)['statusCode'],503)
        self.assertEqual(store.data[m.HEAD],store.previous)
        store=Memory();store.declared_delta=1;ns,_=native(store)
        self.assertEqual(ns['lambda_handler']({},None)['statusCode'],503);self.assertEqual(store.data[m.HEAD],store.previous)
    def test_model_path_disabled_and_no_model_import_or_inherited_key(self):
        ns,_=native()
        with self.assertRaisesRegex(RuntimeError,'disabled'):ns['call_anthropic']('synthetic','synthetic')
        tree=ast.parse((SRC/'lambda_function.py').read_bytes())
        imports=[n.module if isinstance(n,ast.ImportFrom) else name.name for n in ast.walk(tree) if isinstance(n,(ast.Import,ast.ImportFrom)) for name in (n.names if isinstance(n,ast.Import) else [None])]
        self.assertFalse(any(name and any(p in name for p in ('anthropic','llm_router','llm_cost','xai_voice')) for name in imports))
        config=json.loads((SRC.parent/'config.json').read_bytes());self.assertNotIn('inherit_env',config)
        original=json.loads((ROOT/'tests/fixtures/pre-catalyst-context-config.json.txt').read_bytes())
        for key in ('runtime','memory','timeout','handler','role_arn','eventbridge_scheduler'):self.assertEqual(config[key],original[key])
    def test_source_reader_has_no_arbitrary_private_or_remote_path(self):
        ns,store=native()
        for key in ('portfolio/holdings.json','data/user-trades.json','https://other.invalid','audit-private/other.bin'):
            with self.assertRaises(ValueError):ns['_context_read'](key)
        self.assertEqual(store.reads,[])
    def test_original_full_source_and_actual_drifted_helpers_remain_preserved(self):
        baseline=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())
        raw=(ROOT/'tests/fixtures/pre-momentum-leaders-catalyst-classifier.py.txt').read_bytes()
        self.assertEqual(m.sha(raw),baseline['source_checks']['justhodl-catalyst-classifier']['sha256'])
        self.assertIn('_legacy_lambda_handler',(SRC/'lambda_function.py').read_text(encoding='utf-8'))
    def consumer(self,fn,function,scope):
        tree=ast.parse((ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_bytes())
        nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==function]
        self.assertEqual(len(nodes),1)
        exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated synthetic consumer>', 'exec'),scope)
        return scope[function]
    def test_positioning_reader_preserves_null_grade_without_negative_or_positive_grade(self):
        a,s=capture();p=m.build(a,s,AT);memory=Memory();memory.data[m.HEAD]=m.encode(p)
        fn=self.consumer('justhodl-pump-positioning','load_catalysts_map',{'s3':memory,'S3_BUCKET':'synthetic-only','json':json,'Dict':Dict})
        result=fn();self.assertIsNone(result['SYN_A']['catalyst_grade']);self.assertIsNone(result['SYN_A']['catalyst_type'])
        self.assertEqual(memory.reads,[m.HEAD])
    def test_brief_compactor_does_not_recreate_missing_grade_or_durability(self):
        a,s=capture();p=m.build(a,s,AT)
        fn=self.consumer('justhodl-pump-radar-brief','compact_catalysts',{})
        result=fn(p);self.assertEqual(result['n_classified'],0);self.assertEqual(result['flagged'],[])
        self.assertTrue(all(r['catalyst_grade'] is None and r['thesis_durability'] is None for r in result['records']))
    def test_research_calendar_does_not_enter_legacy_notification_aliases(self):
        a,s=capture();p=m.build(a,s,AT);calls=[]
        def read(key):self.assertEqual(key,m.HEAD);return p
        def forbidden(*args,**kwargs):calls.append('unexpected');raise AssertionError('No alert permitted')
        fn=self.consumer('justhodl-prepump-alerts-router','check_earnings_imminent',{'List':List,'_read_json':read,
            'get_active_cascade':lambda:{'alert_tier':[{'ticker':'SYN_A'}]},'_is_new':forbidden,'_mark_alerted':forbidden})
        self.assertEqual(fn({}),[]);self.assertEqual(calls,[])


if __name__=='__main__':unittest.main(verbosity=2)
