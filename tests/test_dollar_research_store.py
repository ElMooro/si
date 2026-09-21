from pathlib import Path
from copy import deepcopy
from unittest.mock import patch,Mock
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
from dollar_fixture import fixture,STAMP
from test_futures_research_store import Memory,Error
import dollar_research_store as store
model=store.model


class Tests(unittest.TestCase):
    def setUp(self):
        self.packet,self.originals,_,_,objects=fixture();self.s=Memory();self.s.data.update(objects)
        self.legacy=self.s.data[model.CURRENT];self.history=self.s.data[model.HISTORY]
    def run_(self,request='test',**kwargs):
        with patch.object(store,'now',return_value=STAMP):return store.run(self.s,'b',request,'execution-'+request,**kwargs)
    def test_original_replay_and_complete_predecessors(self):
        status=self.run_(publish_current=False);output=store.replay(status['replay'],store.reader(self.s,'b'))
        self.assertEqual(output['quality']['available_original_histories'],32)
        self.assertEqual(store.original(output['retained_predecessors'][model.CURRENT],store.reader(self.s,'b')),self.legacy)
        self.assertEqual(self.s.data[model.CURRENT],self.legacy);self.assertEqual(self.s.data[model.HISTORY],self.history)
    def test_native_publication_no_store_and_history_untouched(self):
        status=self.run_();self.assertTrue(status['published']);packet=json.loads(self.s.data[model.CURRENT])
        self.assertEqual(packet['contract'],model.CONTRACT);self.assertEqual(packet['replay'],status['replay'])
        self.assertEqual([m['CacheControl'] for key,m in self.s.writes if key==model.CURRENT],['no-store'])
        self.assertEqual(self.s.data[model.HISTORY],self.history);self.assertFalse(any(key==model.HISTORY for key,_ in self.s.writes))
    def test_repeated_request_cannot_capture_or_publish_again(self):
        status=self.run_();count=len(self.s.writes)
        with patch.object(store,'capture_inputs',side_effect=AssertionError('duplicate capture')):again=self.run_()
        self.assertEqual(again,status);self.assertEqual(len(self.s.writes),count)
    def test_recovery_preserves_original_compilation_without_recapture(self):
        candidate=self.run_(publish_current=False)
        with patch.object(store,'capture_inputs',side_effect=AssertionError('recovery recapture')):
            accepted=self.run_('recover',recover_run=candidate['replay'])
        self.assertTrue(accepted['published']);self.assertEqual(accepted['generated_at'],STAMP)
        self.assertEqual(accepted['replay'],candidate['replay'])
    def test_incomplete_and_failed_claims_cannot_be_retried(self):
        for phase in ('running','failed'):
            request='old-'+phase;key=store.request_key(request);prior={'status':phase,'phase':'compile'};self.s.data[key]=model.encoded(prior)
            with patch.object(store,'capture_inputs',side_effect=AssertionError('duplicate attempt')):self.assertEqual(self.run_(request),prior)
    def test_tampered_original_stops_publication_and_preserves_claim(self):
        key=self.originals['DEXUSEU']['evidence']['observations']['key'];self.s.data[key]=b'not-gzip'
        with self.assertRaisesRegex(RuntimeError,'inspect retained'):self.run_()
        self.assertEqual(self.s.data[model.CURRENT],self.legacy)
        failed=json.loads(self.s.data[store.request_key('test')]);self.assertEqual(failed['status'],'failed')
        self.assertIn('retained_input',failed)
    def test_unknown_private_and_foreign_paths_refused_before_read(self):
        client=Mock();read=store.reader(client,'b')
        for key in ('data/trade-tickets.json','data/portfolio.json','https://example.com/a','data/dollar-research/outputs/../current.json'):
            with self.assertRaises(ValueError):read(key)
        client.get_object.assert_not_called()
    def test_immutable_cache_rejects_wrong_bytes_and_never_caches_mutable_head(self):
        read=store.reader(self.s,'b');self.assertEqual(read(model.CURRENT),self.legacy)
        self.s.data[model.CURRENT]=b'new';self.assertEqual(read(model.CURRENT),b'new')
        with self.assertRaises(ValueError):read.remember(model.PRIVATE+'0'*64+'.bin',b'wrong')
    def test_migration_refuses_changed_predecessor(self):
        candidate=self.run_(publish_current=False);changed=model.encoded({'engine':'justhodl-dollar-radar','schema_version':'3.0','regime':'CHANGED'})
        self.s.data[model.CURRENT]=changed
        result=self.run_('recover',recover_run=candidate['replay']);self.assertFalse(result['published']);self.assertEqual(self.s.data[model.CURRENT],changed)
    def test_same_clock_conflict_and_older_source_or_observation_are_refused(self):
        status=self.run_();old=json.loads(self.s.data[model.CURRENT]);read=store.reader(self.s,'b')
        bad=deepcopy(old);bad['regime']='invented'
        with patch.object(store,'now',return_value='2026-09-22T00:00:00Z'):
            with self.assertRaisesRegex(ValueError,'same-clock'):store.conditional(self.s,'b',bad,read)
        ref=store.protect(self.s,'b',self.s.data[model.CURRENT],read)
        for field in ('source_generated_at','acquired_at','observation_date'):
            newer=deepcopy(old);newer['generated_at']='2026-09-21T22:00:00Z';newer['retained_predecessors'][model.CURRENT]=ref
            if field=='source_generated_at':newer[field]='2026-09-21T20:00:00Z'
            elif field=='acquired_at':newer['series']['DEXUSEU'][field]='2026-09-21T20:00:00Z'
            else:newer['series']['DEXUSEU']['latest_observation']['date']='2026-09-17'
            with patch.object(store,'now',return_value='2026-09-22T00:00:00Z'):self.assertFalse(store.conditional(self.s,'b',newer,read))
    def test_missing_required_history_or_storage_denial_cannot_publish(self):
        del self.s.data[model.HISTORY]
        with self.assertRaises(RuntimeError):self.run_()
        self.assertEqual(self.s.data[model.CURRENT],self.legacy)
        self.s.data[model.HISTORY]=self.history
        real=self.s.get_object
        def denied(**kw):
            if kw['Key']==model.CURRENT:raise Error('AccessDenied')
            return real(**kw)
        with patch.object(self.s,'get_object',side_effect=denied):
            with self.assertRaises(RuntimeError):self.run_('denied')
        self.assertEqual(self.s.data[model.CURRENT],self.legacy)
    def test_changed_frozen_compiler_cannot_recover(self):
        candidate=self.run_(publish_current=False);run=json.loads(self.s.data[candidate['replay']['manifest_key']])
        key=run['compilers']['dollar_research_model']['key'];self.s.data[key]+=b'\n'
        with self.assertRaises(RuntimeError):self.run_('recover',recover_run=candidate['replay'])
        self.assertEqual(self.s.data[model.CURRENT],self.legacy)
    def test_cas_competing_writer_is_preserved(self):
        real=self.s.put_object;competing=model.encoded({'engine':'justhodl-dollar-radar','schema_version':'3.0','race':'wins'})
        def write(**kw):
            if kw['Key']==model.CURRENT:
                self.s.data[model.CURRENT]=competing;raise Error('PreconditionFailed')
            return real(**kw)
        with patch.object(self.s,'put_object',side_effect=write):result=self.run_()
        self.assertFalse(result['published']);self.assertEqual(self.s.data[model.CURRENT],competing)

if __name__=='__main__':unittest.main(verbosity=2)
