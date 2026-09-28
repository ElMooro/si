"""Synthetic native and existing consumer regressions; no external I/O."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from collections import Counter
from io import BytesIO
from typing import Dict, List, Optional, Set, Tuple
import ast, json, sys, time, unittest

ROOT=Path(__file__).resolve().parents[2]
SRC=ROOT/'aws/lambdas/justhodl-catalyst-clusters/source'
sys.path.insert(0,str(SRC))
import cluster_evidence as evidence


class Memory:
    def __init__(self, data):
        self.data={k:json.dumps(v).encode() for k,v in data.items()};self.writes=[];self.reads=[]
    def get_object(self, **kw):
        self.reads.append(kw['Key']);return {'Body':BytesIO(self.data[kw['Key']])}
    def put_object(self, **kw):
        self.writes.append(kw['Key']);self.data[kw['Key']]=kw['Body'].encode() if isinstance(kw['Body'],str) else kw['Body']


def extracted(path,names,scope):
    tree=ast.parse(path.read_bytes())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    if len(nodes)!=len(names):raise AssertionError('Exact isolated native functions required')
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated native synthetic-only functions>','exec'),scope)
    return scope


def fixture():
    return {'catalysts':{'catalysts':[{'ticker':t,'catalyst_grade':'B','catalyst_type':'EARNINGS_BEAT','catalyst_date':'2026-09-28','unknown_original':'retained'} for t in ('SYN_A','SYN_B','SYN_C')]},
        'positioning':{'aggressive_basket':{'positions':[{'ticker':t,'position_pct':10} for t in ('SYN_A','SYN_B','SYN_C')]}},
        'themes':{},'earnings_cal':{},'macro':{},'momentum':{}}


def native(data=None):
    data=deepcopy(fixture() if data is None else data)
    inputs={key:'data/'+key+'.json' for key in data};store=Memory({inputs[k]:v for k,v in data.items()})
    store.data['data/catalyst-clusters.json']=b'{"synthetic_previous":"preserve"}'
    scope={'s3':store,'json':json,'time':time,'Counter':Counter,'datetime':datetime,'timezone':timezone,'timedelta':timedelta,
        'Dict':Dict,'List':List,'Optional':Optional,'Set':Set,'Tuple':Tuple,
        'S3_BUCKET':'synthetic-only','OUTPUT_KEY':'data/catalyst-clusters.json','INPUT_KEYS':inputs,
        'MIN_MEMBERS_FOR_CLUSTER':3,'EARNINGS_WINDOW_DAYS':5,'MACRO_EVENT_WINDOW_DAYS':7,
        'GRADE_WEIGHTS':{'A':1.0,'B':0.75,'C':0.5,'D':0.2},
        **{k:getattr(evidence,k) for k in ('CONTRACT','FLAGS','describe_cluster','abstain','momentum_scores')}}
    names=('load_s3_json','detect_temporal_earnings_clusters','detect_thematic_clusters','grade_cluster','recommend_action','lambda_handler','_write_error')
    return extracted(SRC/'lambda_function.py',names,scope),store


def run(data=None):
    scope,store=native(data);result=scope['lambda_handler']({},None)
    return result,json.loads(store.data['data/catalyst-clusters.json']),store


class Tests(unittest.TestCase):
    def assert_abstains(self,packet):
        self.assertEqual(packet['measurement_contract'],evidence.CONTRACT)
        for flag in evidence.FLAGS:self.assertIs(packet[flag],False)
        self.assertIsNone(packet['call']);self.assertEqual(packet['proposed_new_sizes'],{});self.assertEqual(packet['size_deltas'],{})
        self.assertTrue(all(value==[] for value in packet['basket_action_summary'].values()))
        for c in packet['clusters']:
            self.assertIsNone(c['quality']);self.assertIsNone(c['quality_grade']);self.assertIsNone(c['leader']);self.assertEqual(c['ranked_members'],[])
            rec=c['recommendation'];self.assertEqual(rec['action'],'WAIT');self.assertIsNone(rec['leader_new_size'])
            self.assertEqual(rec['laggard_new_sizes'],{});self.assertIsNone(rec['suggested_entry']);self.assertIsNone(rec['hedge_suggest'])
    def test_original_failing_missing_scores_now_abstain_and_preserve_members(self):
        result,p,store=run();self.assertEqual(result['statusCode'],200);self.assert_abstains(p)
        c=p['clusters'][0];self.assertIsNone(c['momentum_spread'])
        self.assertEqual(c['evidence_quality']['unavailable_score_occurrences'],3)
        self.assertEqual(c['member_records'],fixture()['catalysts']['catalysts'])
        self.assertEqual(p['current_sizes'],{'SYN_A':10,'SYN_B':10,'SYN_C':10})
        self.assertEqual(store.writes,['data/catalyst-clusters.json'])
    def test_null_bool_nan_string_range_and_enormous_scores_do_not_crash_or_vote(self):
        for value in (None,False,True,'0','',float('nan'),float('inf'),-1,101,10**500):
            self.assertIsNone(evidence.reported_score(value))
        self.assertEqual(evidence.reported_score(0),0);self.assertEqual(evidence.reported_score(100),100)
        data=fixture();data['momentum']={'all_scored':[{'ticker':'SYN_A','momentum_score':None}]}
        self.assert_abstains(run(data)[1])
    def test_real_zero_is_inspectable_without_calibrated_rank_or_size(self):
        data=fixture();data['momentum']={'all_scored':[{'ticker':t,'momentum_score':v} for t,v in [('SYN_A',0),('SYN_B',20),('SYN_C',100)]]}
        p=run(data)[1];self.assert_abstains(p);c=p['clusters'][0]
        self.assertEqual(c['momentum_spread'],100)
        self.assertEqual([v['reported_momentum_score'] for v in c['member_observations']],[0,20,100])
    def test_legacy_full_scores_and_forged_flags_still_cannot_size(self):
        for grade in ('A','B','C','D'):
            data=fixture()
            for c in data['catalysts']['catalysts']:c['catalyst_grade']=grade
            data['momentum']={'sizing_eligible':True,'all_scored':[{'ticker':t,'momentum_score':99} for t in ('SYN_A','SYN_B','SYN_C')]}
            data['macro']={'synthesis':{'global_posture':'RISK_OFF'}}
            p=run(data)[1];self.assert_abstains(p);self.assertEqual(p['reported_macro_regime'],'RISK_OFF');self.assertIsNone(p['macro_regime'])
    def test_adjacent_and_macro_groupings_never_open_or_hedge_positions(self):
        for scope in ('ADJACENT','MIXED','BASKET_LOCAL'):
            rec=evidence.abstain({'scope':scope,'cluster_type':'MACRO_EVENT','quality_grade':'A','leader':'SYN_A'})
            self.assertEqual(rec['action'],'WAIT');self.assertIsNone(rec['suggested_entry']);self.assertIsNone(rec['hedge_suggest'])
    def test_empty_all_scored_does_not_fall_back_and_new_price_contract_has_no_rank(self):
        packet={'all_scored':[],'leaders':[{'ticker':'SYN_A','momentum_score':99}]}
        self.assertEqual(evidence.momentum_scores(packet),{})
        packet['all_scored']=packet['leaders'];packet['measurement_contract']='leader-price-observations.v1'
        self.assertEqual(evidence.momentum_scores(packet),{})
    def test_duplicate_conflicting_score_never_recovers_via_last_write_wins(self):
        rows=[{'ticker':'SYN_A','momentum_score':v} for v in (99,1,99)]
        self.assertEqual(evidence.momentum_scores({'all_scored':rows}),{'SYN_A':None})
    def test_duplicate_members_keep_occurrences_without_three_independent_names(self):
        member={'ticker':'SYN_A','catalyst_grade':'A'};cluster={'member_records':[member,member,member]}
        saved=deepcopy(cluster);c=evidence.describe_cluster(cluster,{'SYN_A':99})
        self.assertEqual(cluster,saved);self.assertEqual(len(c['member_observations']),3)
        self.assertEqual(c['evidence_quality']['distinct_reported_tickers'],1)
        self.assertFalse(c['evidence_quality']['cluster_minimum_unique_members_met']);self.assertIsNone(c['momentum_spread'])
    def test_dependency_failures_preserve_prior_packet_and_known_empty_stays_empty(self):
        for key,value in [('catalysts',None),('catalysts',{'catalysts':None}),('catalysts',{'catalysts':[{}]}),('positioning',[]),('positioning',{'aggressive_basket':{'positions':[{}]}}),('themes',[1])]:
            data=fixture();data[key]=value;result,p,store=run(data)
            self.assertEqual(result['statusCode'],500);self.assertEqual(p,{'synthetic_previous':'preserve'});self.assertEqual(store.writes,[])
        data=fixture();data['catalysts']['catalysts']=[]
        p=run(data)[1];self.assert_abstains(p);self.assertEqual(p['clusters'],[])
    def test_position_null_not_zero_and_ambiguous_duplicate_preserves_head(self):
        data=fixture();del data['positioning']['aggressive_basket']['positions'][0]['position_pct']
        p=run(data)[1];self.assertIsNone(p['current_sizes']['SYN_A']);self.assert_abstains(p)
        data['positioning']['aggressive_basket']['positions'].append({'ticker':'SYN_A','position_pct':50})
        self.assertEqual(run(data)[2].writes,[])
    def test_existing_positioning_and_brief_consumers_accept_abstention_shapes(self):
        p=run()[1];memory=Memory({'data/catalyst-clusters.json':p})
        ns=extracted(ROOT/'aws/lambdas/justhodl-pump-positioning/source/lambda_function.py',('load_clusters_data',),{'s3':memory,'json':json,'S3_BUCKET':'synthetic-only'})
        c=ns['load_clusters_data']();self.assertEqual(c['boost'],set());self.assertEqual(c['trim'],set());self.assertEqual(c['exclude'],set());self.assertEqual(c['proposed_new_sizes'],{})
        ns=extracted(ROOT/'aws/lambdas/justhodl-pump-radar-brief/source/lambda_function.py',('compact_clusters',),{})
        brief=ns['compact_clusters'](p);self.assertEqual(brief['clusters'][0]['action'],'WAIT');self.assertIsNone(brief['clusters'][0]['leader'])
    def test_whole_original_remains_preserved(self):
        raw=(ROOT/'tests/fixtures/pre-momentum-leaders-catalyst-clusters.py.txt').read_bytes()
        import hashlib
        baseline=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())
        original=baseline['source_checks']['justhodl-catalyst-clusters']
        self.assertEqual(len(raw),original['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),original['sha256'])
        self.assertIn('legacy', (SRC/'lambda_function.py').read_text(encoding='utf-8'))


if __name__=='__main__':unittest.main(verbosity=2)
