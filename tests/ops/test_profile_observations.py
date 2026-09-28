"""Synthetic issuer joins, per-profile clocks and whole-source replay."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,timedelta
import gzip,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-theme-classifier/source')]
import context_evidence_store as store
import profile_observations as m
from test_velocity_evidence import parent
AT='2026-09-28T07:00:00Z'


def captures(tickers=('SYNA',)):
    inputs={};attempts=[];sources={}
    def received(raw):
        ref=store.identity(raw,m.PRIVATE,'sources');sources[ref['key']]=raw
        return {'status':'received','original_ref':ref,'requested_at':AT,'received_at':AT,'content_encoding':''}
    for name,key in m.ALL_INPUTS.items():
        raw=store.encode(parent(tickers) if name=='momentum' else {'generated_at':AT,'profiles':{'OLD':{'industry':'unqualified legacy cache'}}})
        inputs[name]={'source_key':key,**received(raw)}
    for ticker in tickers[:30]:
        raw=store.encode([{'symbol':ticker,'industry':'Semiconductors','sector':'Technology','marketCap':0}])
        attempts.append({'ticker':ticker,'endpoint':m.endpoint(ticker),'http_status':200,'network_attempted':True,'acquisition':'provider_request',**received(raw)})
    return inputs,attempts,sources


class Tests(unittest.TestCase):
    def test_reported_classification_replays_without_hot_labels_ranking_or_legacy_fields(self):
        a,b,s=captures(('SYNA','SYNB','SYNC'));before=deepcopy((a,b,s));p=m.build(a,b,s,AT)
        self.assertEqual((a,b,s),before);self.assertEqual(p,m.build(a,b,s,AT));self.assertEqual(p['coverage']['classified_issuers'],3)
        group=p['industry_memberships'][0];self.assertEqual(group['distinct_reported_issuers'],3)
        self.assertFalse(group['active_theme_qualified']);self.assertFalse(group['comovement_established'])
        self.assertEqual(group['members'][0]['source_pointer'],'/0/industry');self.assertEqual(p['ticker_to_theme'],{})
        self.assertEqual(p['themes'],{});self.assertIsNone(p['n_active_themes']);self.assertEqual(p['call'],'WAIT')
        for flag in m.FLAGS:self.assertIs(p[flag],False)
        self.assertNotIn(b'unqualified legacy cache',store.encode(p));self.assertIsNone(p['classification_observations'][0]['classification_vintage'])
    def test_exact_issuer_join_and_whole_single_row_shape(self):
        cases=[[],{},[{'symbol':'OTHER','industry':'wrong'}],[{'symbol':'SYNA','industry':'one'},{'symbol':'SYNA','industry':'two'}],
               [{'symbol':'OTHER','industry':'wrong'},{'symbol':'SYNA','industry':'correct'}],[{'symbol':True,'industry':'bad'}]]
        for rows in cases:self.assertIsNone(m.profile(store.encode(rows),'SYNA')['industry'])
        for ind in (None,False,0,'','  ','a\x00b','a'*513):
            self.assertEqual(m.profile(store.encode([{'symbol':'SYNA','industry':ind}]),'SYNA')['status'],'industry_unavailable')
        row=m.profile(store.encode([{'symbol':'SYNA','industry':'Software—Application','sector':None}]),'SYNA')
        self.assertEqual(row['industry'],'Software—Application');self.assertIsNone(row['sector'])
    def test_duplicate_and_misaligned_parent_do_not_generate_requests(self):
        at=store.clock(AT)
        for d in (parent(('SYNA','SYNA')),dict(parent(),forecast_qualified=True),dict(parent(),generated_at='2099-01-01T00:00:00Z')):
            with self.assertRaises(ValueError):m.selection(d,at)
        d=parent();d['request_records'][0]['ticker']='OTHER'
        with self.assertRaises(ValueError):m.selection(d,at)
    def test_original_thirty_scope_keeps_all_parent_occurrences(self):
        p=m.selection(parent(tuple('T'+str(i) for i in range(35))),store.clock(AT))
        self.assertEqual(len(p['selected_tickers']),30);self.assertEqual(len(p['occurrences']),35)
        self.assertEqual(p['occurrences'][-1]['status'],'outside_original_thirty_limit')
    def test_cache_age_is_per_profile_not_new_packet_clock(self):
        a,b,s=captures();p=m.build(a,b,s,AT);at=store.clock(AT)
        for age,expected in [(timedelta(days=7),True),(timedelta(days=7,seconds=1),False),(timedelta(days=7,hours=23),False),(timedelta(seconds=-1),False)]:
            now=at+age;d=deepcopy(p);d['generated_at']=now.isoformat()
            self.assertEqual('SYNA' in m.reusable(d,now),expected)
        old=deepcopy(p);old['generated_at']=(at+timedelta(days=30)).isoformat()
        self.assertEqual(m.reusable(old,at+timedelta(days=30)),{})
    def test_reuse_requires_exact_original_source_date_and_hash(self):
        a,b,s=captures();previous=m.build(a,b,s,AT);b[0].update(network_attempted=False,acquisition='retained_profile')
        p=m.build(a,b,s,AT,previous);self.assertEqual(p['coverage']['provider_requests_attempted'],0)
        self.assertEqual(p['coverage']['retained_profiles_reused'],1);self.assertEqual(p['profile_sources'][0]['received_at'],AT)
        for field,value in [('received_at','2026-09-28T06:59:00Z'),('http_status',201),('network_attempted',True),('content_encoding','gzip')]:
            bad=deepcopy(b);bad[0][field]=value
            with self.assertRaises(ValueError):m.build(a,bad,s,AT,previous)
        bad=deepcopy(previous);bad['profile_sources']*=2;self.assertEqual(m.reusable(bad,store.clock(AT)),{})
        bad=deepcopy(previous);bad['profile_sources'][0]['original_ref']['key']='private/accounts.json';self.assertEqual(m.reusable(bad,store.clock(AT)),{})
    def test_legacy_global_cache_never_qualifies_a_profile(self):
        old={'generated_at':AT,'profiles':{'SYNA':{'industry':'Semiconductors'}}}
        self.assertEqual(m.reusable(old,store.clock(AT)),{})
    def test_whole_source_nonfinite_duplicate_json_and_encoding_fail(self):
        for raw in (b'[{"symbol":"SYNA","industry":"one","industry":"two"}]',b'[{"symbol":"SYNA","industry":"x","number":NaN}]'):
            self.assertEqual(m.profile(raw,'SYNA')['status'],'invalid_json')
        self.assertEqual(m.profile(gzip.compress(b'[]')+b'trailing','SYNA','gzip')['status'],'invalid_json')
        a,b,s=captures();s[b[0]['original_ref']['key']]=b'[]'
        with self.assertRaises(ValueError):m.build(a,b,s,AT)
    def test_missing_secondary_is_unknown_and_failed_only_preserves_prior(self):
        a,b,s=captures(('SYNA','SYNB'));b[1].update(status='not_attempted_stop',network_attempted=False,http_status=None);b[1].pop('original_ref')
        p=m.build(a,b,s,AT);self.assertEqual(p['coverage']['unavailable_issuers'],1)
        self.assertIsNone(p['classification_observations'][1]['industry']);self.assertIsNone(p['classification_observations'][1]['acquisition_age_seconds'])
        a,b,s=captures();b[0].update(status='not_attempted_stop',network_attempted=False,http_status=None);b[0].pop('original_ref')
        with self.assertRaises(ValueError):m.build(a,b,s,AT)
    def test_explicit_empty_research_scope_is_not_an_active_zero_theme_count(self):
        p=m.build(*captures(()),AT);self.assertEqual(p['classification_observations'],[]);self.assertIsNone(p['n_active_themes'])


if __name__=='__main__':unittest.main(verbosity=2)
