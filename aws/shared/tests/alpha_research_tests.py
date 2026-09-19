"""Alpha projection, statistical semantics, privacy, replay and publication fault tests."""
from copy import deepcopy
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import alpha_research as m
import alpha_research_store as store

AT='2026-09-19T09:30:00+00:00'


def fixture():
    docs={n:{'generated_at':AT,'quality':{'status':'fresh'}} for n in m.SOURCES}
    docs['conviction']['setups']=[{'subject':'Crypto','direction':'RISK-ON / LONG','conviction':0,'rank':1,
        'contributing_engines':[{'engine':'Crypto Narratives','family':'crypto','signal':0}]}]
    docs['engine_signal_map']['by_family']={'crypto':['btc_signal']}
    docs['magnitude_distributions']['stacks']=[{'signals':['btc_signal'],'horizon_days':21,'n':12,
        'mean':3,'median':1,'p25':1,'p75':3,'win_rate':0.8}]
    docs['scorecard']['scorecard']=[{'signal_type':'btc_signal','n_scored':0,'n_total':40,'n_quarantined':40,
        'hit_rate':None,'wilson_lb':0,'wilson_ub':1,'avg_return_pct':None,'sizing_eligible':True}]
    docs['regime_composite']['meta_regime']='UNAVAILABLE';docs['risk_regime']['risk_regime']='MILD_RISK_ON'
    docs['settlement_fails'].update(treasury={'scope_id':'treasury_incl_tips','unit':'usd_bn','as_of':'2026-09-09',
        'ftd_bn':92.61,'ftr_bn':97.63,'gross_bn':190.24,'quality':{'status':'fresh'}},
        headline={'scope_id':'ust_ex_tips','unit':'usd_bn','as_of':'2026-09-09','ftd_bn':86,'ftr_bn':87,'combined_bn':173,'quality':{'status':'fresh'}})
    return docs


def inputs(docs=None):
    docs=docs or fixture();out={'generated_at':AT,'sources':{}}
    for n,key in m.SOURCES.items():
        p=m.project(n,docs[n]);out['sources'][n]={'source':key,'status':'captured_projection','acquired_at':AT,
            'projection':p,'projection_sha256':m.digest(p),'source_packet_sha256':m.digest(docs[n])}
    return out


class Error(Exception):
    def __init__(self,code): self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self): self.data={};self.calls=[];self.on_put=None
    def get_object(self,**kw):
        key=kw['Key'];self.calls.append(('read',key))
        if key not in self.data: raise Error('NoSuchKey')
        raw=self.data[key];return {'Body':io.BytesIO(raw),'ETag':hashlib.md5(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];raw=kw['Body'];self.calls.append(('write',key))
        if self.on_put:self.on_put(kw)
        if kw.get('IfNoneMatch')=='*' and key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kw and (key not in self.data or hashlib.md5(self.data[key]).hexdigest()!=kw['IfMatch']):raise Error('PreconditionFailed')
        self.data[key]=raw


class ModelTests(unittest.TestCase):
    def test_no_prior_or_quartile_sizing_even_when_source_claims_authority(self):
        out=m.build(inputs());card=out['research_ideas'][0]
        self.assertIsNone(card['sizing']['kelly_pct']);self.assertIsNone(card['stop_pct']);self.assertIsNone(card['target_pct'])
        self.assertIsNone(card['stats']['win_rate']);self.assertEqual(card['stats']['scorecards'][0]['wilson_lb'],0)
        self.assertEqual(card['stats']['scorecards'][0]['n_scored'],0);self.assertEqual(card['stats']['distributions'][0]['median'],1)
        self.assertIsNone(card['stats']['distributions'][0]['return_unit']);self.assertFalse(card['calls_eligible'])
        self.assertEqual(out['top_calls'],[]);self.assertEqual(out['decision']['meaning'],'abstain');self.assertIsNone(out['decision']['portfolio_change'])
        self.assertEqual(card['reported_conviction'],0);self.assertEqual(card['engines'][0]['reported_signal'],0)
    def test_missing_history_no_invented_size(self):
        docs=fixture();docs['magnitude_distributions']={};docs['scorecard']={}
        c=m.build(inputs(docs))['research_ideas'][0]
        self.assertIsNone(c['sizing']['dollar_at_100k']);self.assertIn('no_matching_descriptive_cohort',c['qualification_gaps'])
    def test_cohorts_are_not_pooled_and_matching_does_not_qualify(self):
        docs=fixture();docs['magnitude_distributions']['stacks']*=2
        c=m.build(inputs(docs))['research_ideas'][0]
        self.assertEqual(len(c['stats']['distributions']),2);self.assertIsNone(c['stats']['n'])
        self.assertTrue(all(not x['qualified'] for x in c['stats']['distributions']))
        self.assertIsNone(c['independent_evidence_count'])
    def test_private_contributor_withholds_all_aggregate_values(self):
        docs=fixture();row=docs['conviction']['setups'][0]
        row['contributing_engines'].append({'engine':'PM Decision','family':'desk-posture'})
        row.update(thesis='PRIVATE_SECRET',invalidation='PRIVATE_SECRET',book={'holdings':'PRIVATE_SECRET'})
        docs['conviction']['firm_book_context']={'note':'PRIVATE_SECRET'}
        out=m.build(inputs(docs));c=out['research_ideas'][0]
        self.assertTrue(c['aggregate_withheld']);self.assertIsNone(c['reported_conviction']);self.assertIsNone(c['source_rank']);self.assertIsNone(c['direction'])
        self.assertNotIn('PRIVATE_SECRET',m.encoded(out).decode());self.assertNotIn('PM Decision',m.encoded(out).decode())
    def test_arbitrary_notes_symbols_and_html_do_not_enter_projection(self):
        docs=fixture();docs['best_setups']['setups']=[{'ticker':'SPY','entry':100,'note':'secret'},{'ticker':'<img src=x>'}]
        p=m.project('best_setups',docs['best_setups']);self.assertEqual(p['symbols'],['SPY']);self.assertNotIn('entry',p)
        docs['conviction']['setups'].append({'subject':'<img src=x>','contributing_engines':[]})
        self.assertEqual(len(m.build(inputs(docs))['research_ideas']),1)
    def test_source_order_does_not_manufacture_research_ranking(self):
        docs=fixture();docs['conviction']['setups'].insert(0,{'subject':m.THEMES[-1],'rank':1,'contributing_engines':[]})
        out=m.build(inputs(docs));self.assertEqual(out['research_ideas'][0]['subject'],'Crypto')
    def test_bad_numeric_values_never_become_zero_or_probability(self):
        for value in (True,float('nan'),float('inf'),'0',None):self.assertIsNone(m.number(value))
        self.assertIsNone(m.count(1.5));self.assertEqual(m.number(-2),-2)
    def test_source_clocks_not_renewed_by_compilation(self):
        i=inputs();r=i['sources']['conviction'];r['projection']['generated_at']='2026-09-17T09:30:00Z';r['projection_sha256']=m.digest(r['projection'])
        o=m.build(i);self.assertEqual(o['source_feeds']['conviction']['status'],'stale')
        self.assertIn('source_packet_not_current',o['research_ideas'][0]['qualification_gaps'])
        i=inputs();i['sources']['conviction']['acquired_at']='2026-09-19T09:31:00Z'
        with self.assertRaisesRegex(ValueError,'acquired after'):m.build(i)
    def test_future_packet_and_missing_clock_unverified(self):
        docs=fixture();docs['conviction']['generated_at']='2026-09-19T10:00:00Z'
        self.assertEqual(m.build(inputs(docs))['source_feeds']['conviction']['status'],'clock_unverified')
        docs['conviction'].pop('generated_at');self.assertEqual(m.build(inputs(docs))['source_feeds']['conviction']['status'],'clock_unverified')
    def test_projection_schema_and_hash_rejected(self):
        i=inputs();r=i['sources']['conviction'];r['projection']['private_notes']='secret';r['projection_sha256']=m.digest(r['projection'])
        with self.assertRaisesRegex(ValueError,'schema'):m.build(i)
        i=inputs();i['sources']['conviction']['projection']['ideas'][0]['reported_conviction']=34
        with self.assertRaisesRegex(ValueError,'identity'):m.build(i)
    def test_duplicate_subject_and_oversized_input_are_rejected(self):
        docs=fixture();docs['conviction']['setups']*=2
        with self.assertRaisesRegex(ValueError,'duplicate'):m.project('conviction',docs['conviction'])
        with self.assertRaisesRegex(ValueError,'bounded'):m.project('magnitude_distributions',{'stacks':[{}]*4001})
    def test_regime_labels_are_separate_and_no_fused_risk_multiplier(self):
        out=m.build(inputs());self.assertTrue(out['regime']['reported_labels_differ']);self.assertIsNone(out['regime']['risk_multiplier'])
        self.assertEqual([v['reported_label'] for v in out['regime']['sources'][:2]],['UNAVAILABLE','MILD_RISK_ON'])
    def test_fails_scope_units_sum_and_observation_checks(self):
        docs=fixture();out=m.build(inputs(docs));f=out['pd_settlement_fails']
        self.assertEqual(f['display_values']['combined_bn'],190.24);self.assertEqual(f['ust_ex_tips']['display_values']['combined_bn'],173)
        docs['settlement_fails']['headline']['combined_bn']=190.24
        f=m.build(inputs(docs))['pd_settlement_fails'];self.assertIsNone(f['ust_ex_tips']['display_values']);self.assertIsNotNone(f['display_values'])
        docs['settlement_fails']['treasury'].pop('unit');self.assertIsNone(m.build(inputs(docs))['pd_settlement_fails']['display_values'])
    def test_decision_identity_changes_with_inputs_and_clock(self):
        a=m.build(inputs())['research_ideas'][0]['decision_id'];i=inputs();i['generated_at']='2026-09-19T09:31:00Z'
        b=m.build(i)['research_ideas'][0]['decision_id'];self.assertNotEqual(a,b)
        self.assertEqual(a,m.build(inputs())['research_ideas'][0]['decision_id'])
    def test_brief_cites_exact_decisions_and_stale_stays_stale(self):
        o=m.build(inputs());ref={'manifest_key':m.PREFIX+'runs/'+'a'*64+'.json','output_sha256':m.digest(o)}
        b=m.build_brief(o,AT,ref);self.assertIn(o['research_ideas'][0]['decision_id'],b['brief_markdown']);self.assertTrue(b['brief_markdown'].endswith('**WAIT**'))
        self.assertEqual(b['paid_ai_calls'],0);self.assertFalse(b['calls_eligible'])
        self.assertEqual(m.build_brief(o,'2026-09-20T09:30:00Z',ref)['quality']['status'],'stale')
        o['sizing_eligible']=True
        with self.assertRaises(ValueError):m.build_brief(o,AT,ref)


class StoreTests(unittest.TestCase):
    def setUp(self):self.s3=Storage();self.bucket='test'
    def seed(self):
        for name,doc in fixture().items():self.s3.data[m.SOURCES[name]]=m.encoded(doc)
        self.s3.data[m.CURRENT]=m.encoded({'old_private_note':'legacy-secret'})
        self.s3.data['data/alpha-compass-history.json']=m.encoded({'entries':['legacy-secret']})
    def test_full_retention_replay_and_brief_no_private_or_paid_reads(self):
        self.seed()
        with patch.object(store,'now',return_value=AT):
            a=store.run_compass(self.s3,self.bucket);b=store.run_brief(self.s3,self.bucket)
        read=store.raw_reader(self.s3,self.bucket);out=json.loads(read(m.CURRENT));brief=json.loads(read(m.BRIEF))
        self.assertTrue(a['published']);self.assertTrue(b['published'])
        self.assertEqual(store.replay(store.load_manifest(a['replay'],read),read),{k:v for k,v in out.items() if k!='replay'})
        self.assertEqual(store.replay(store.load_manifest(b['replay'],read),read),{k:v for k,v in brief.items() if k!='replay'})
        self.assertEqual(brief['source_replay'],a['replay']);self.assertIn(b'**WAIT**',self.s3.data['data/alpha-brief.md'])
        for key,raw in self.s3.data.items():
            if key.startswith(m.PREFIX):self.assertNotIn(b'legacy-secret',raw)
        self.assertTrue(any(k.startswith(store.PRIVATE_BACKUP) for k in self.s3.data))
        self.assertFalse(any(k.startswith('portfolio/') or k=='data/notes-index.json' for action,k in self.s3.calls))
    def test_snapshot_corruption_rejected(self):
        i=inputs();o=m.build(i);ref=store.retain(self.s3,self.bucket,i,o,'compass');read=store.raw_reader(self.s3,self.bucket);manifest=store.load_manifest(ref,read)
        self.s3.data[manifest['input']['key']]=b'{}'
        with self.assertRaisesRegex(ValueError,'input differs'):store.replay(manifest,read)
    def test_changed_compiler_and_false_manifest_rejected(self):
        i=inputs();ref=store.retain(self.s3,self.bucket,i,m.build(i),'compass');read=store.raw_reader(self.s3,self.bucket);manifest=store.load_manifest(ref,read)
        self.s3.data[manifest['compilers']['alpha_research']['key']]=b'print(1)'
        with self.assertRaisesRegex(ValueError,'compiler differs'):store.replay(manifest,read)
        with self.assertRaisesRegex(ValueError,'identity'):store.load_manifest({**ref,'output_sha256':'f'*64},read)
    def test_old_generation_cannot_overwrite_new_and_equal_clock_conflicts(self):
        newer={'contract':m.CONTRACT,'generated_at':'2026-09-19T09:40:00Z'};self.s3.data[m.CURRENT]=m.encoded(newer)
        self.assertFalse(store.publish(self.s3,self.bucket,m.CURRENT,{'contract':m.CONTRACT,'generated_at':AT}))
        with self.assertRaisesRegex(ValueError,'same clock'):store.publish(self.s3,self.bucket,m.CURRENT,{**newer,'x':1})
    def test_conditional_race_preserves_newer(self):
        old={'contract':m.CONTRACT,'generated_at':'2026-09-19T09:00:00Z'};self.s3.data[m.CURRENT]=m.encoded(old)
        newer={**old,'generated_at':'2026-09-19T10:00:00Z'}
        def race(kw):
            self.s3.on_put=None;self.s3.data[m.CURRENT]=m.encoded(newer)
        self.s3.on_put=race
        self.assertFalse(store.publish(self.s3,self.bucket,m.CURRENT,{**old,'generated_at':AT}))
        self.assertEqual(json.loads(self.s3.data[m.CURRENT]),newer)
    def test_capture_failure_does_not_reuse_fresh_source(self):
        def fail(key):raise OSError('secret-source-error')
        with patch.object(store,'now',return_value=AT):i=store.collect(fail)
        o=m.build(i);self.assertEqual(o['quality']['current_packets'],0);self.assertEqual(o['research_ideas'],[])
        self.assertNotIn('secret-source-error',m.encoded(i).decode())
    def test_preservation_failure_prevents_publication(self):
        self.seed()
        def fail(kw):
            if kw['Key'].startswith(store.PRIVATE_BACKUP):raise Error('AccessDenied')
        self.s3.on_put=fail
        with self.assertRaises(Error):store.run_compass(self.s3,self.bucket)
        self.assertIn(b'legacy-secret',self.s3.data[m.CURRENT])
    def test_modified_current_pointer_cannot_enter_brief(self):
        self.seed()
        with patch.object(store,'now',return_value=AT):store.run_compass(self.s3,self.bucket)
        p=json.loads(self.s3.data[m.CURRENT]);p['paid_ai_calls']=1;self.s3.data[m.CURRENT]=m.encoded(p)
        with self.assertRaisesRegex(ValueError,'pointer differs'):store.run_brief(self.s3,self.bucket)


class HandlerTests(unittest.TestCase):
    def load(self,name):
        root=Path(__file__).resolve().parents[2]/'lambdas'/name/'source/lambda_function.py'
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None),
                                    '_sentry_lite':types.SimpleNamespace(track_errors=lambda f:f)}):
            spec=importlib.util.spec_from_file_location(name.replace('-','_'),root);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);return mod
    def test_compass_active_route_cannot_select_legacy_telegram_or_private_path(self):
        mod=self.load('justhodl-alpha-compass')
        with patch.object(store,'run_compass',return_value={'published':True}) as run,patch.object(mod,'send_telegram') as tg,patch.object(mod,'safe_load') as legacy:
            self.assertEqual(mod.lambda_handler({'test_telegram':True,'use_legacy':True},None)['statusCode'],200)
        run.assert_called_once();tg.assert_not_called();legacy.assert_not_called()
    def test_daily_brief_has_no_paid_or_notification_active_route(self):
        mod=self.load('justhodl-alpha-daily-brief')
        with patch.object(store,'run_brief',return_value={'published':True}) as run,patch.object(mod,'call_claude') as paid,patch.object(mod,'send_telegram') as tg,patch.object(mod,'build_context_bundle') as old:
            self.assertEqual(mod.lambda_handler({'test_telegram':True,'force_paid':True},None)['statusCode'],200)
        run.assert_called_once();paid.assert_not_called();tg.assert_not_called();old.assert_not_called()
    def test_public_failure_is_sanitized(self):
        mod=self.load('justhodl-alpha-compass')
        with patch.object(store,'run_compass',side_effect=ValueError('private detail')):
            result=mod.lambda_handler({},None)
        self.assertEqual(result['statusCode'],503);self.assertNotIn('private detail',result['body'])


def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromModule(sys.modules[__name__]))
    if not result.wasSuccessful():raise SystemExit(1)


if __name__=='__main__':run()
