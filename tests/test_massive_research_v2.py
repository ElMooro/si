from copy import deepcopy
from pathlib import Path
import json,sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
from test_massive_research import fixture as old_fixture
import massive_research_model as old_model
import massive_research_store as old_store
import massive_research_model_v2 as model
import massive_research_store_v2 as store

STAMP='2026-09-21T21:00:00+00:00'

def fixture():
    memory,old_inputs,docs=old_fixture();read=store.reader(memory,'bucket')
    old=old_model.build(old_inputs,read);identity=old_store.retain(memory,'bucket',old_inputs,old,read,lambda **kw:None)
    current={**old,'replay':identity};memory.data[model.CURRENT]=model.encoded(current)
    for kind,filename in (('fx','fx-native.json'),('futures','futures-native.json')):
        doc=json.loads((ROOT/'tests/fixtures'/filename).read_text(encoding='utf-8'))
        memory.data.update({k:v.encode() for k,v in doc['artifacts'].items()})
        memory.data[model.SOURCES[kind][0]]=model.encoded(doc['publication'])
    with patch.object(store,'now',return_value=STAMP):inputs=store.capture_inputs(memory,'bucket',read,lambda **kw:None)
    return memory,inputs,current

class ModelTests(unittest.TestCase):
    def setUp(self):self.memory,self.inputs,self.previous=fixture();self.read=store.reader(self.memory,'bucket')
    def build(self):return model.build(self.inputs,self.read)
    def replace(self,kind,mutate):
        key=model.SOURCES[kind][0];packet=model.strict(self.memory.data[key]);body={k:v for k,v in packet.items() if k!='replay'};mutate(body)
        _,_,prefix,contract,_=model.SOURCES[kind];raw=model.encoded(body);out={'key':prefix+'outputs/'+model.sha(raw)+'.json','sha256':model.sha(raw),'bytes':len(raw)}
        self.memory.data[out['key']]=raw;run={'contract':contract,'generated_at':body['generated_at'],'output':out,'output_sha256':out['sha256']}
        raw=model.encoded(run);manifest=prefix+'runs/'+model.sha(raw)+'.json';self.memory.data[manifest]=raw
        packet={**body,'replay':{'manifest_key':manifest,'output_sha256':out['sha256']}};self.memory.data[key]=model.encoded(packet)
        self.inputs['sources'][key]['original']=store.protect(self.memory,'bucket',model.encoded(packet),self.read)
    def test_seven_sources_exact_namespaces_and_no_extra_authority(self):
        out=self.build();self.assertEqual(out['quality']['bound_native_sources'],7)
        self.assertEqual(len([k for k in out['instruments'] if k.startswith('FX:')]),19)
        self.assertEqual(len([k for k in out['instruments'] if k.startswith('FUTURE:')]),14)
        self.assertEqual(len(out['instruments']['SPY']),5)
        self.assertTrue(all(out[k] is False for k in model.FLAGS));self.assertEqual(out['independent_investment_votes'],0)
    def test_original_v1_output_and_replay_do_not_change(self):
        self.build();out=old_store.replay(self.previous['replay'],self.read)
        self.assertEqual(out,{k:v for k,v in self.previous.items() if k!='replay'})
        self.assertEqual(self.build(),model.build(model.strict(model.encoded(self.inputs)),self.read))
    def test_every_new_pointer_reaches_exact_dated_instrument(self):
        out=self.build()
        for identity,rows in out['instruments'].items():
            if ':' not in identity:continue
            row=rows[0];node=out['dependency_graph']['nodes'][row['node']];body,_=model.parent(row['source'],node['replay'],self.read,model.clock(STAMP))
            value=body
            for part in row['pointer'].split('/')[1:]:value=value[int(part)] if isinstance(value,list) else value[part]
            declared=out['instrument_identities'][identity]
            self.assertEqual(value['pair' if row['source']=='fx' else 'ticker'],declared['pair' if row['source']=='fx' else 'ticker'])
    def test_shared_legs_do_not_claim_correlations_or_independent_votes(self):
        groups=self.build()['dependency_graph']['shared_exposures'];usd=next(g for g in groups if g['key']=='currency:USD')
        self.assertIn('FX:MASSIVE:EUR_USD',usd['instruments']);self.assertGreater(len(usd['instruments']),1)
        self.assertTrue(all(g['independent_investment_votes']==0 for g in groups))
    def test_futures_deadline_unknown_and_capture_not_observation(self):
        out=self.build();row=out['sources']['futures'];self.assertIsNone(row['source_review_due_at']);self.assertIsNone(row['source_review_overdue'])
        item=next(rows[0] for key,rows in out['instruments'].items() if key.startswith('FUTURE:'))
        self.assertFalse(item['bar_finality_independently_verified']);self.assertNotEqual(item['source_capture_completed_at'],item['latest_reported_session_end_date'])
    def test_each_fx_pair_deadline_preserved_and_earliest_review_used(self):
        out=self.build();deadlines=[r[0]['source_review_due_at'] for k,r in out['instruments'].items() if k.startswith('FX:')]
        self.assertEqual(model.clock(out['sources']['fx']['source_review_due_at']),min(map(model.clock,deadlines)))
    def test_missing_parent_stays_partial_without_legacy_signal_fallback(self):
        self.inputs['sources'][model.EXTRA['fx'][0]].update(status='missing',original=None)
        out=self.build();self.assertEqual(out['quality']['bound_native_sources'],6)
        self.assertFalse(any(k.startswith('FX:') for k in out['instruments']));self.assertEqual(out['portfolio_action'],'WAIT')
    def test_pair_identity_mismatch_rejected(self):
        self.replace('fx',lambda b:b['pairs']['EUR_USD'].update(base_code='USD'))
        with self.assertRaisesRegex(ValueError,'pair identity'):self.build()
    def test_pair_count_mismatch_rejected(self):
        self.replace('fx',lambda b:b.update(configured_pairs=20))
        with self.assertRaisesRegex(ValueError,'pair count'):self.build()
    def test_pair_acquisition_cannot_be_in_the_future(self):
        self.replace('fx',lambda b:b['pairs']['EUR_USD'].update(source_capture_completed_at='2026-09-22T00:00:00Z'))
        with self.assertRaisesRegex(ValueError,'acquisition'):self.build()
    def test_contract_scope_mismatch_rejected(self):
        def change(b):
            item=b['products']['ES']['contracts'][0];b['datasets'][item['dataset']]['scope']['ticker']='NQZ6'
        self.replace('futures',change)
        with self.assertRaisesRegex(ValueError,'scope differs'):self.build()
    def test_duplicate_dated_contract_rejected(self):
        self.replace('futures',lambda b:b['products']['ES']['contracts'].append(deepcopy(b['products']['ES']['contracts'][0])))
        with self.assertRaisesRegex(ValueError,'Unique dated'):self.build()
    def test_future_definition_date_rejected(self):
        self.replace('futures',lambda b:b.update(definition_date='2026-09-22'))
        with self.assertRaisesRegex(ValueError,'definition date'):self.build()
    def test_changed_prior_head_does_not_pass_as_recorded_history(self):
        bad=deepcopy(self.previous);bad['score']=99
        self.inputs['prior_composite']['original']=store.protect(self.memory,'bucket',model.encoded(bad),self.read)
        with self.assertRaisesRegex(ValueError,'Prior composition publication'):self.build()
    def test_new_source_cannot_assert_investment_authority(self):
        self.replace('futures',lambda b:b.update(calls_eligible=True))
        with self.assertRaisesRegex(ValueError,'authority'):self.build()

class StoreTests(unittest.TestCase):
    def setUp(self):
        self.memory,self.inputs,self.previous=fixture();self.read=store.reader(self.memory,'bucket')
        frozen=patch.object(store,'now',return_value=STAMP);frozen.start();self.addCleanup(frozen.stop)
    def retain(self):
        out=model.build(self.inputs,self.read);identity=store.retain(self.memory,'bucket',self.inputs,out,self.read,lambda **kw:None)
        return {**out,'replay':identity}
    def test_new_and_old_runs_replay_with_distinct_frozen_compilers(self):
        packet=self.retain();self.assertEqual(store.replay(packet['replay'],self.read),{k:v for k,v in packet.items() if k!='replay'})
        self.assertEqual(old_store.replay(self.previous['replay'],self.read),{k:v for k,v in self.previous.items() if k!='replay'})
        with self.assertRaisesRegex(ValueError,'run differs'):store.replay(self.previous['replay'],self.read)
    def test_exact_v1_migration_retains_prior_bytes_and_empty_aliases(self):
        raw=self.memory.data[model.CURRENT];status=store.run(self.memory,'bucket','migrate','runner')
        self.assertTrue(status['published']);self.assertTrue(all(status['aliases'].values()))
        packet=model.strict(self.memory.data[model.CURRENT]);self.assertEqual(packet['contract'],model.CONTRACT)
        self.assertEqual(model.original(packet['prior_composite']['original'],self.read),raw)
        alias=model.strict(self.memory.data[model.PREDECESSORS[0]]);self.assertEqual(alias['tickers'],{});self.assertEqual(alias['top_prepump'],[])
    def test_changed_v1_head_refuses_promotion(self):
        packet=self.retain();old=deepcopy(self.previous);old['new_field']='concurrent';self.memory.data[model.CURRENT]=model.encoded(old)
        with self.assertRaisesRegex(ValueError,'predecessor changed'):store.conditional(self.memory,'bucket',model.CURRENT,packet,self.read)
        self.assertEqual(model.strict(self.memory.data[model.CURRENT]),old)
    def test_migration_cannot_drop_an_existing_native_source(self):
        packet=self.retain();del packet['sources']['options']['source_generated_at']
        with self.assertRaisesRegex(ValueError,'drop or roll back'):store.conditional(self.memory,'bucket',model.CURRENT,packet,self.read)
    def test_per_dataset_rollback_cannot_be_hidden_by_new_generation(self):
        packet=self.retain();self.memory.data[model.CURRENT]=model.encoded(packet)
        bad=deepcopy(packet);bad['generated_at']='2026-09-21T21:01:00Z';key=next(iter(bad['sources']['futures']['measurement_capture_clocks']))
        bad['sources']['futures']['measurement_capture_clocks'][key]='2026-09-20T00:00:00Z'
        with patch.object(store,'now',return_value='2026-09-21T22:00:00Z'):
            self.assertFalse(store.conditional(self.memory,'bucket',model.CURRENT,bad,self.read))
        self.assertEqual(model.strict(self.memory.data[model.CURRENT]),packet)
    def test_candidate_failure_preserves_all_heads_and_durable_identity(self):
        before={k:self.memory.data[k] for k in (model.CURRENT,*model.PREDECESSORS)}
        with patch.object(model,'build',side_effect=ValueError('invalid')):
            with self.assertRaises(RuntimeError):store.run(self.memory,'bucket','failed','runner')
        self.assertEqual(before,{k:self.memory.data[k] for k in before})
        self.assertEqual(store.run(self.memory,'bucket','failed','another')['status'],'failed')
    def test_recovery_does_not_recapture_or_advance_source_dates(self):
        packet=self.retain();self.memory.reads.clear();status=store.run(self.memory,'bucket','recover','runner',recover_run=packet['replay'],publish_current=False)
        self.assertEqual(status['generated_at'],packet['generated_at']);self.assertFalse(set(self.memory.reads)&set(model.CAPTURE_KEYS))
        self.assertEqual(status['aliases'],{});self.assertFalse(status['published'])
    def test_v2_reader_still_rejects_unreviewed_parent_paths(self):
        before=len(self.memory.reads)
        for key in ('data/trade-tickets.json','data/futures-research/records/'+'a'*64+'.json'):
            with self.assertRaises(ValueError):self.read(key)
        self.assertEqual(len(self.memory.reads),before)

if __name__=='__main__':unittest.main(verbosity=2)
