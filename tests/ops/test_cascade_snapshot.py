"""Synthetic contemporaneous evidence replay; no native/provider/consumer reads."""
from pathlib import Path
from datetime import date,timedelta
from decimal import Decimal
from copy import deepcopy
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
import ast,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[2];SRC=ROOT/'aws/lambdas/justhodl-theme-cascade-backtest/source'
sys.path[:0]=[str(ROOT/'aws/shared'),str(SRC),str(ROOT/'aws/lambdas/justhodl-momentum-leaders/source')]
import context_evidence_store as store
import snapshot_evidence as m
import leader_price_observations as prices
import provider_flow_research,provider_flow_catalog
AT='2026-09-28T08:00:00Z'


def parent(changes=(('SYNA',10),('SYNB',14),('SYNC',0))):
    records=[];members=[]
    for i,(ticker,change) in enumerate(changes):
        rows=[]
        for j in range(25):
            close=Decimal(100+change if j==24 else 100)
            rows.append({'date':(date(2026,9,1)+timedelta(days=j)).isoformat(),'index':j,'open':close,'close':close,'low':close-1,'high':close+1,'volume':Decimal(1000),'issues':[]})
        member={'ticker':ticker,'request_index':i};members.append(member)
        observations={'ticker':ticker,'status':'parsed_completed_observations',**prices.FLAGS,'measurements':prices.measurements(rows),
            'selected_rows':[{k:str(v) if isinstance(v,Decimal) else v for k,v in row.items()} for row in rows]}
        records.append({**member,'observations':observations})
    return {'measurement_contract':prices.CONTRACT,'status':'RESEARCH_ONLY','call':None,**prices.FLAGS,'generated_at':AT,
        'universe_membership':{'selected':members},'request_records':records}


def documents():
    return {'momentum':parent(),'theme_rotation':{'generated_at':AT,'all_themes':[{'ticker':'ETF1'},{'ticker':'ETF1'}],
        'breadth_details':{'ETF1':{'constituents_perf':[{'symbol':'SYNA'},{'symbol':'SYNA'},{'symbol':'SYNC'}]}}}}


def capture(docs=None):
    a={};s={}
    for name,key in m.INPUTS.items():
        raw=store.encode((docs or documents())[name]);ref=store.identity(raw,m.PRIVATE,'sources');s[ref['key']]=raw
        a[name]={'source_key':key,'status':'received','original_ref':ref,'requested_at':AT,'received_at':AT,'content_encoding':''}
    return a,s


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Memory:
    def __init__(self):
        a,s=capture();self.data={row['source_key']:s[row['original_ref']['key']] for row in a.values()}
        self.previous=b'{"synthetic_previous":"whole original"}';self.data[m.HEAD]=self.previous
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
        'S3_BUCKET':'synthetic','ContextStore':store.ContextStore,'context_evidence_store':store,'snapshot_evidence':m,
        'provider_flow_research':provider_flow_research,'provider_flow_catalog':provider_flow_catalog,'json':json}
    tree=ast.parse((SRC/'lambda_function.py').read_bytes());h=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
    exec(compile(ast.Module(body=[h],type_ignores=[]),'<synthetic snapshot writer>','exec'),ns)
    return ns,mem,clients


class Tests(unittest.TestCase):
    def test_replay_reports_exact_dated_changes_and_genuine_zero_without_validation(self):
        a,s=capture();before=deepcopy((a,s));p=m.build(a,s,AT);self.assertEqual((a,s),before);self.assertEqual(p,m.build(a,s,AT))
        r=p['snapshot_observations'];self.assertEqual([v['value'] for v in r['observations']],[10,14,0])
        self.assertEqual(r['observations'][0]['start_parent_pointer'],'/request_records/0/observations/selected_rows/19/close')
        group={g['cohort']:g for g in r['cohorts_by_window'][0]['cohorts']}
        self.assertEqual(group['up_at_least_ten_percent']['median_price_change_percent'],12)
        self.assertEqual(group['up_at_least_ten_percent']['distinct_reported_issuers'],2)
        self.assertEqual(group['up_at_least_ten_percent']['with_reported_membership'],1)
        self.assertEqual(group['nonpositive_change']['median_price_change_percent'],0)
        self.assertFalse(r['root_source_revalidated']);self.assertFalse(p['evaluation']['predictive_validation'])
        self.assertEqual(p['big_pumpers_detail'],[]);self.assertEqual(p['laggards_hot_detail'],[])
        self.assertIsNone(p['validation_rate']);self.assertIsNone(p['lift_metrics']['big_pumper_pct_in_top_10'])
        self.assertTrue(p['lift_metrics']['interpretation'].startswith('UNVALIDATED'))
        for k in m.FLAGS:self.assertIs(p[k],False)
    def test_repeated_membership_is_retained_but_never_inflates_issuer_count(self):
        p=m.build(*capture(),AT);r=p['snapshot_observations']['observations'][0]
        self.assertEqual(len(r['reported_memberships']),2);self.assertEqual(r['reported_memberships'][1]['status'],'duplicate_reported_pair')
        self.assertEqual(p['snapshot_observations']['cohorts_by_window'][0]['cohorts'][0]['with_reported_membership'],1)
    def test_duplicate_wrong_or_future_parent_cannot_become_a_sample(self):
        cases=[{},parent((('SYNA',10),('SYNA',20))),dict(parent(),generated_at='2099-01-01T00:00:00Z'),dict(parent(),sizing_eligible=True)]
        d=parent();d['request_records'][0]['ticker']='OTHER';cases.append(d)
        d=parent();d['request_records'][0]['request_index']=False;cases.append(d)
        cases.append(dict(parent(),ranking_eligible=True))
        for value in cases:
            docs=documents();docs['momentum']=value;p=m.build(*capture(docs),AT)
            self.assertEqual(p['snapshot_observations']['observations'],[]);self.assertIsNone(p['snapshot_observations']['unavailable_observations'])
    def test_missing_price_is_retained_as_unknown_and_never_enters_zero_cohort(self):
        d=documents();d['momentum']['request_records'][0]['observations']['selected_rows'][-1]['close']=None
        p=m.build(*capture(d),AT);s=p['snapshot_observations'];self.assertIsNone(s['observations'][0]['value'])
        self.assertEqual(s['unavailable_observations'],1);self.assertEqual(s['cohorts_by_window'][0]['cohorts'][-1]['observation_indices'],[2])
    def test_boolean_duplicate_unsorted_and_inconsistent_price_evidence_is_unavailable(self):
        for edit in (lambda o:o['selected_rows'][-1].update(close=True),lambda o:o['selected_rows'][-1].update(index=True),
                     lambda o:o['selected_rows'][-1].update(date='2026-09-24'),lambda o:o['selected_rows'].reverse(),
                     lambda o:o['measurements']['price_change_5'].update(value=True),lambda o:o['measurements']['price_change_5'].update(exact='99'),
                     lambda o:o['measurements']['price_change_5'].update(start_date='2026-09-19')):
            d=documents();edit(d['momentum']['request_records'][0]['observations']);p=m.build(*capture(d),AT)
            self.assertIsNone(p['snapshot_observations']['observations'][0]['value'])
    def test_different_price_dates_do_not_share_cohort_or_median(self):
        d=documents();o=d['momentum']['request_records'][1]['observations']
        for row in o['selected_rows']:row['date']=(date.fromisoformat(row['date'])-timedelta(days=1)).isoformat()
        for k in ('start_date','end_date'):o['measurements']['price_change_5'][k]=(date.fromisoformat(o['measurements']['price_change_5'][k])-timedelta(days=1)).isoformat()
        p=m.build(*capture(d),AT);self.assertEqual(len(p['snapshot_observations']['cohorts_by_window']),2)
        self.assertEqual(sorted(g['cohorts'][0]['median_price_change_percent'] for g in p['snapshot_observations']['cohorts_by_window']),[10,14])
    def test_future_theme_and_missing_roster_make_membership_unknown(self):
        for edits in ({'generated_at':'2099-01-01T00:00:00Z'},{'all_themes':None}):
            d=documents();d['theme_rotation'].update(edits);p=m.build(*capture(d),AT)
            self.assertIsNone(p['snapshot_observations']['cohorts_by_window'][0]['cohorts'][0]['with_reported_membership'])
    def test_empty_research_population_has_no_weak_or_validated_percentage(self):
        d=documents();d['momentum']=parent(());p=m.build(*capture(d),AT)
        self.assertEqual(p['snapshot_observations']['observations'],[]);self.assertEqual(p['snapshot_observations']['cohorts_by_window'],[])
        self.assertIsNone(p['validation_rate']);self.assertIsNone(p['big_pumpers_5d_stats']['n'])
    def test_full_source_hash_exact_graph_and_nonfinite_context(self):
        a,s=capture();s[a['momentum']['original_ref']['key']]=b'{}'
        with self.assertRaises(ValueError):m.build(a,s,AT)
        a,s=capture();a['momentum']['source_key']='private/accounts.json'
        with self.assertRaises(ValueError):m.build(a,s,AT)
        a,s=capture();raw=b'{"bad":NaN}';ref=store.identity(raw,m.PRIVATE,'sources');a['momentum']['original_ref']=ref;s[ref['key']]=raw
        p=m.build(a,s,AT);self.assertEqual(p['sources'][0]['status'],'invalid_json');self.assertEqual(p['snapshot_observations']['observations'],[])
    def test_native_whole_context_replay_compiler_closure_and_pre_storage_exclusion(self):
        ns,mem,clients=native()
        with patch.object(store,'now',return_value=AT):r=ns['lambda_handler']({'head':'private/accounts.json','invoke':True},None)
        self.assertEqual(r['statusCode'],200);p=json.loads(mem.data[m.HEAD]);manifest=json.loads(mem.data[p['replay']['input_ref']['key']])
        replay=m.build(manifest['attempts'],mem.data,manifest['generated_at'])
        for k,v in replay.items():self.assertEqual(p[k],v)
        self.assertEqual(len(manifest['source_files']),5);self.assertEqual(mem.data[p['replay']['previous_publication']['key']],mem.previous)
        for name,ref in manifest['source_files'].items():
            path=SRC/name if name in ('lambda_function.py','snapshot_evidence.py') else ROOT/'aws/shared'/name
            self.assertEqual(mem.data[ref['key']],path.read_bytes())
        self.assertEqual(clients,['s3']);self.assertNotIn(m.EXCLUDED,mem.reads)
        self.assertTrue(all(k in (*m.INPUTS.values(),m.HEAD) or k.startswith(m.PRIVATE) for k in mem.reads))
        self.assertTrue(all(w['Key']==m.HEAD or w['Key'].startswith(m.PRIVATE) for w in mem.writes))
    def test_retention_failure_and_lost_acknowledgement_are_distinct(self):
        for setting in ('fail_retention','lost_ack'):
            ns,mem,_=native();setattr(mem,setting,True)
            with patch.object(store,'now',return_value=AT):r=ns['lambda_handler']({},None)
            self.assertEqual(r['statusCode'],503)
            if setting=='lost_ack':self.assertIsNone(json.loads(r['body'])['previous_publication_preserved']);self.assertNotEqual(mem.data[m.HEAD],mem.previous)
            else:self.assertIs(json.loads(r['body'])['previous_publication_preserved'],True);self.assertEqual(mem.data[m.HEAD],mem.previous)
            self.assertLessEqual(sum(w['Key']==m.HEAD for w in mem.writes),1)
    def test_original_runtime_and_schedule_are_preserved(self):
        cfg=json.loads((SRC.parent/'config.json').read_bytes());old=json.loads((ROOT/'tests/fixtures/pre-cascade-snapshot-config.json.txt').read_bytes())
        self.assertEqual({k:v for k,v in cfg.items() if k!='description'},{k:v for k,v in old.items() if k!='description'})


if __name__=='__main__':unittest.main(verbosity=2)
