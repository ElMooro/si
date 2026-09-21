from pathlib import Path
from io import BytesIO
from copy import deepcopy
from unittest.mock import patch,Mock
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import futures_research_model as model
import futures_research_store as store
from test_futures_research_model import fixture,GENERATED,change


class Error(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Memory:
    def __init__(self):self.data={};self.reads=[];self.writes=[];self.corrupt=None
    def get_object(self,Bucket,Key):
        self.reads.append(Key)
        if Key not in self.data:raise Error('NoSuchKey')
        raw=self.data[Key]
        return {'Body':BytesIO(raw+b' ' if Key==self.corrupt else raw),'ETag':model.sha(raw)}
    def put_object(self,Bucket,Key,Body,**kwargs):
        if kwargs.get('IfNoneMatch')=='*' and Key in self.data:raise Error('PreconditionFailed')
        if 'IfMatch' in kwargs and (Key not in self.data or model.sha(self.data[Key])!=kwargs['IfMatch']):raise Error('PreconditionFailed')
        self.data[Key]=Body;self.writes.append((Key,kwargs))


class Tests(unittest.TestCase):
    def setUp(self):
        self.memory=Memory();originals,self.sources=fixture();self.memory.data.update(originals)
        self.legacy=model.encoded({'engine':'justhodl-polygon-futures-curves','version':'2.0.1','product_data':{'preserve':'whole'},'identity':{'preserve':'whole'},'signals':['unqualified']})
        self.memory.data[model.LEGACY]=self.legacy;self.read=store.reader(self.memory,'bucket')
    def run_(self,request='test',**kwargs):
        with patch.object(store,'now',return_value=GENERATED),patch.object(store,'collect',return_value={
                'sources':self.sources,'provider_requests':19,'source_bytes':1234}):
            return store.run(self.memory,'bucket',request,'execution-'+request,credential='test-only',**kwargs)
    def output(self,status):return store.replay(status['replay'],self.read)
    def candidate(self):return self.run_(publish_current=False)
    def test_original_replay_every_row_and_clock(self):
        status=self.candidate();out=self.output(status)
        self.assertEqual(sum(v['returned_rows'] for v in out['datasets'].values()),343);self.assertEqual(out['generated_at'],GENERATED)
        self.assertEqual(store.model.checked_original(out['predecessors'][model.LEGACY]['original'],self.read),self.legacy)
        self.assertNotIn(model.CURRENT,self.memory.data);self.assertEqual(self.memory.data[model.LEGACY],self.legacy)
    def test_publication_and_compatibility_preserve_whole_predecessor(self):
        status=self.run_();self.assertTrue(status['published']);self.assertTrue(status['compatibility_published'])
        out=self.output(status);public=json.loads(self.memory.data[model.CURRENT]);alias=json.loads(self.memory.data[model.LEGACY])
        self.assertEqual(public,{**out,'replay':status['replay']});self.assertEqual(alias,store.compatibility(public))
        self.assertFalse(alias['identity_ok']);self.assertEqual(alias['signals'],[])
        self.assertTrue(all(alias[k] is False for k in model.FLAGS))
        self.assertEqual(self.memory.data[alias['retained_predecessor']['original']['key']],self.legacy)
        self.assertTrue(all(meta['CacheControl']=='no-store' for key,meta in self.memory.writes if key in (model.CURRENT,model.LEGACY)))
    def test_repeat_request_does_not_recollect_or_republish(self):
        status=self.candidate();before=len(self.memory.writes)
        with patch.object(store,'collect',side_effect=AssertionError('duplicate collection')):
            again=store.run(self.memory,'bucket','test','different-execution')
        self.assertEqual(again,status);self.assertEqual(len(self.memory.writes),before)
    def test_incomplete_request_is_returned_without_recollection(self):
        prior={'status':'running','phase':'collect_originals'};self.memory.data[store.request_key('test')]=model.encoded(prior)
        with patch.object(store,'collect',side_effect=AssertionError('duplicate collection')):
            result=store.run(self.memory,'bucket','test','new')
        self.assertEqual(result,prior)
    def test_recovery_preserves_clocks_and_needs_no_credential_or_capture(self):
        candidate=self.candidate()
        with patch.object(store,'collect',side_effect=AssertionError('recollection')),patch.object(store,'snapshot',side_effect=AssertionError('recapture')):
            status=store.run(self.memory,'bucket','recover','recovery',recover_run=candidate['replay'])
        self.assertTrue(status['published']);self.assertEqual(status['replay'],candidate['replay'])
        self.assertEqual(status['generated_at'],GENERATED);self.assertEqual(status['provider_requests_this_execution'],0)
    def test_changed_compiler_is_not_executed_or_accepted(self):
        candidate=self.candidate();manifest=candidate['replay']['manifest_key'];doc=json.loads(self.memory.data[manifest])
        doc['compilers']['futures_research_model']['sha256']='0'*64;raw=model.encoded(doc);ref=model.ref(raw,'runs');self.memory.data[ref['key']]=raw
        with self.assertRaises(ValueError):store.replay({'manifest_key':ref['key'],'output_sha256':candidate['replay']['output_sha256']},self.read)
    def test_tampered_original_or_bar_block_is_rejected(self):
        for kind in ('original','block'):
            self.setUp();status=self.candidate();out=self.output(status)
            key=self.sources['ES:bars:ESZ6']['pages'][0]['original']['key'] if kind=='original' else out['datasets']['ES:bars:ESZ6']['records'][0]['key']
            self.memory.corrupt=key
            with self.assertRaises(ValueError):store.replay(status['replay'],store.reader(self.memory,'bucket'))
    def test_nonresearch_reads_are_rejected_before_s3(self):
        for key in ('data/trade-tickets.json','accounts/current.json',model.CURRENT,model.PRIVATE+'requests/x.json'):
            with self.assertRaises(ValueError):self.read(key)
        self.assertEqual(self.memory.reads,[])
    def test_wrong_hash_write_is_rejected(self):
        raw=b'{}';ref=model.ref(raw,'inputs');ref['bytes']+=1
        with self.assertRaises(ValueError):store.immutable(self.memory,'bucket',ref,raw,self.read)
        self.assertEqual(self.memory.writes,[])
    def test_verified_immutable_bytes_are_not_written_again_within_one_execution(self):
        raw=b'{"retained":true}';ref=model.ref(raw,'inputs')
        store.immutable(self.memory,'bucket',ref,raw,self.read);before=len(self.memory.writes)
        store.immutable(self.memory,'bucket',ref,raw,self.read)
        self.assertEqual(len(self.memory.writes),before);self.assertEqual(self.read(ref['key']),raw)
    def test_failed_collection_retains_last_good_and_cannot_repeat(self):
        self.run_();before={k:self.memory.data[k] for k in (model.CURRENT,model.LEGACY)}
        with patch.object(store,'collect',side_effect=ValueError('fixture failure')):
            with self.assertRaises(RuntimeError):store.run(self.memory,'bucket','failed','failed',credential='test')
        self.assertEqual({k:self.memory.data[k] for k in before},before)
        status=json.loads(self.memory.data[store.request_key('failed')]);self.assertEqual(status['status'],'failed')
        with patch.object(store,'collect',side_effect=AssertionError('repeat')):
            self.assertEqual(store.run(self.memory,'bucket','failed','repeat'),status)
    def test_older_capture_cannot_replace_newer_head(self):
        status=self.run_();current=json.loads(self.memory.data[model.CURRENT]);newer=deepcopy(current)
        newer['generated_at']='2026-09-21T19:02:00+00:00';self.memory.data[model.CURRENT]=model.encoded(newer)
        again=self.run_('recover',recover_run=status['replay']);self.assertFalse(again['published'])
        self.assertEqual(json.loads(self.memory.data[model.CURRENT]),newer)
    def test_one_dataset_clock_rollback_is_rejected_even_when_max_clock_is_same(self):
        status=self.run_();current=json.loads(self.memory.data[model.CURRENT]);new=deepcopy(current)
        new['generated_at']='2026-09-21T19:02:00+00:00';new['datasets']['ES:bars:ESZ6']['source_capture_completed_at']='2026-09-21T18:00:00+00:00'
        self.assertFalse(store.conditional(self.memory,'bucket',model.CURRENT,new,self.read))
        self.assertEqual(json.loads(self.memory.data[model.CURRENT]),current)
    def test_same_clock_conflict_fails(self):
        self.run_();packet=json.loads(self.memory.data[model.CURRENT]);packet['quality']['selected_contracts']+=1
        with self.assertRaises(ValueError):store.conditional(self.memory,'bucket',model.CURRENT,packet,self.read)
    def test_newer_capture_cannot_regress_definition_date(self):
        self.run_();current=json.loads(self.memory.data[model.CURRENT]);old=deepcopy(current)
        old['generated_at']='2026-09-21T19:02:00+00:00';old['definition_date']='2026-09-20'
        self.assertFalse(store.conditional(self.memory,'bucket',model.CURRENT,old,self.read))
        self.assertEqual(json.loads(self.memory.data[model.CURRENT]),current)
    def test_contract_roll_can_change_bar_inventory_without_dropping_catalog_clocks(self):
        self.run_();current=json.loads(self.memory.data[model.CURRENT]);rolled=deepcopy(current)
        rolled['generated_at']='2026-09-21T19:02:00+00:00'
        prior=rolled['datasets'].pop('ES:bars:ESZ6');prior['scope']['ticker']='ESM7'
        rolled['datasets']['ES:bars:ESM7']=prior
        self.assertTrue(store.conditional(self.memory,'bucket',model.CURRENT,rolled,self.read))
        bad=deepcopy(rolled);bad['datasets'].pop('ES:contracts')
        with self.assertRaises(ValueError):store.conditional(self.memory,'bucket',model.CURRENT,bad,self.read)
    def test_concurrent_legacy_change_cannot_be_retired(self):
        status=self.candidate();self.memory.data[model.LEGACY]=self.legacy+b' '
        with self.assertRaises(RuntimeError):self.run_('recover',recover_run=status['replay'])
        self.assertEqual(self.memory.data[model.LEGACY],self.legacy+b' ')
        self.assertNotIn(model.CURRENT,self.memory.data)
    def test_unknown_predecessor_fails_before_any_head_write(self):
        self.memory.data[model.LEGACY]=b'{"unknown":true}'
        with self.assertRaises(RuntimeError):self.run_()
        self.assertNotIn(model.CURRENT,self.memory.data);self.assertEqual(self.memory.data[model.LEGACY],b'{"unknown":true}')
    def test_no_usable_comparison_does_not_replace_heads(self):
        originals,self.sources=fixture()
        for name,source in self.sources.items():
            if source['scope']['kind']=='bars':change(originals,self.sources,name,lambda d:d.update(results=[]))
        self.memory.data.update(originals)
        status=self.run_();self.assertFalse(status['published']);self.assertEqual(self.memory.data[model.LEGACY],self.legacy)
        self.assertNotIn(model.CURRENT,self.memory.data);self.assertTrue(all(not c['comparisons']['close']['1']['available'] for p in self.output(status)['products'].values() for c in p['contracts']))
    def test_low_time_budget_does_not_start_collection(self):
        with patch.object(store,'collect',side_effect=AssertionError('must not collect')):
            with self.assertRaises(RuntimeError):store.run(self.memory,'bucket','low','low',remaining_seconds=30)
        self.assertEqual(json.loads(self.memory.data[store.request_key('low')])['phase'],'preserve')
    def test_collection_leaves_replay_reserve(self):
        with patch.object(store,'time') as clock,patch.object(store,'collect',side_effect=ValueError('stop')) as collect:
            clock.monotonic.return_value=100
            with self.assertRaises(RuntimeError):store.run(self.memory,'bucket','time','time',credential='test',remaining_seconds=90)
        self.assertEqual(collect.call_args.args[4],145)
    def test_private_status_cannot_write_an_arbitrary_key(self):
        for key in ('data/private.json',model.CURRENT,model.PRIVATE+'requests/../../evil.json'):
            with self.assertRaises(ValueError):store.status_write(self.memory,'bucket',key,{})
        self.assertEqual(self.memory.writes,[])
    def test_publication_rejects_promoted_or_wrong_target_contract(self):
        self.run_();packet=json.loads(self.memory.data[model.CURRENT])
        for key,value in (('calls_eligible',True),('contract','futures-original-compatibility.v1')):
            bad=deepcopy(packet);bad[key]=value
            with self.assertRaises(ValueError):store.conditional(self.memory,'bucket',model.CURRENT,bad,self.read)


if __name__=='__main__':unittest.main(verbosity=2)
