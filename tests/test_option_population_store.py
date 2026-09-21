from pathlib import Path
from unittest.mock import patch
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests'),str(ROOT/'aws/ops/staged')]
import option_population_store as store
import test_option_flow_store as fixtures
from ops_5999_population_native_candidate import verify_memberships
model=store.model;desk=store.desk;upstream=store.upstream


class Tests(unittest.TestCase):
    def setUp(self):
        t=fixtures.StoreTests();t.setUp()
        try:self.s3,inputs,output,replay=t.candidate()
        finally:t.tearDown()
        self.s3.data[upstream.CURRENT]=upstream.encoded({**output,'replay':replay})
        self.legacy=upstream.encoded({'version':'1.3.0','market_composite':{},'calculation_config':{'n_underlyings':10},'underlyings':{'SPY':{'legacy':'whole source'}}})
        self.history=upstream.encoded({'history':[{'preserve':'all prior rows'}]})
        self.s3.data[desk.LEGACY]=self.legacy;self.s3.data[desk.HISTORY]=self.history
        self.patch=patch.object(model,'UNDERLYINGS',('SPY',));self.patch.start()
    def tearDown(self):self.patch.stop()
    def run_native(self,name='test'):
        return store.run(self.s3,'synthetic',name,'execution')
    def test_native_replay_publication_and_whole_history(self):
        status=self.run_native();self.assertEqual(status['status'],'complete')
        self.assertTrue(status['published']);self.assertTrue(status['compatibility_published'])
        packet=json.loads(self.s3.data[desk.CURRENT]);alias=json.loads(self.s3.data[desk.LEGACY])
        read=store.reader(self.s3,'synthetic')
        self.assertEqual(store.replay(packet['replay'],read),{k:v for k,v in packet.items() if k!='replay'})
        self.assertEqual(alias,desk.compatibility(packet));self.assertEqual(self.s3.data[desk.HISTORY],self.history)
        self.assertEqual(upstream.protected(packet['predecessors'][desk.LEGACY],read),self.legacy)
    def test_membership_drilldown_matches_exact_rows_and_rejects_wrong_contract(self):
        self.run_native();packet=json.loads(self.s3.data[desk.CURRENT]);read=store.reader(self.s3,'synthetic')
        source=json.loads(self.s3.data[upstream.CURRENT]);item=copy.deepcopy(packet['underlyings']['SPY'])
        self.assertEqual(verify_memberships(item,source,read),2)
        members=model.checked(item['source_membership'],'groups',read)
        members['groups'][0]['source_rows'][0]['contract_id']='O:SPY260925C00999000'
        raw=upstream.encoded(members);item['source_membership']=model.reference(raw,'groups')
        self.s3.data[item['source_membership']['key']]=raw
        with self.assertRaises(AssertionError):verify_memberships(item,source,store.reader(self.s3,'synthetic'))
    def test_same_request_is_not_executed_twice(self):
        original=self.run_native();writes=len(self.s3.writes)
        self.assertEqual(self.run_native(),original);self.assertEqual(writes,len(self.s3.writes))
    def test_replay_does_not_depend_on_latest_source_head(self):
        status=self.run_native();self.s3.data[upstream.CURRENT]=b'{}'
        restored=store.replay(status['replay'],store.reader(self.s3,'synthetic'))
        self.assertEqual(restored['source_run'],status['source_run'])
    def test_compile_failure_preserves_last_good(self):
        self.run_native();before=self.s3.data[desk.CURRENT]
        with patch.object(desk,'build',side_effect=ValueError('bad input')):
            with self.assertRaises(RuntimeError):self.run_native('failure')
        self.assertEqual(before,self.s3.data[desk.CURRENT])
        key=store.source_store.request_key('population:failure')
        self.assertEqual(json.loads(self.s3.data[key])['status'],'failed')
    def test_unknown_legacy_shape_fails_before_any_current_head_write(self):
        self.s3.data[desk.LEGACY]=b'{"version":"unreviewed","underlyings":{}}'
        with self.assertRaises(RuntimeError):self.run_native()
        self.assertNotIn(desk.CURRENT,self.s3.data)
        self.assertEqual(self.s3.data[desk.LEGACY],b'{"version":"unreviewed","underlyings":{}}')
    def test_private_account_read_is_rejected(self):
        with self.assertRaises(ValueError):store.reader(self.s3,'synthetic')('data/trade-tickets.json')
    def test_older_source_cannot_replace_a_newer_capture(self):
        self.run_native();packet=json.loads(self.s3.data[desk.CURRENT]);old=copy.deepcopy(packet)
        old['source_capture_completed_at']='2026-09-20T12:00:00Z'
        self.assertFalse(store.conditional(self.s3,'synthetic',desk.CURRENT,old))
        old=copy.deepcopy(packet);old['unreviewed']=True
        with self.assertRaises(ValueError):store.conditional(self.s3,'synthetic',desk.CURRENT,old)
    def test_compiler_and_group_artifact_tampering_rejected(self):
        status=self.run_native();read=store.reader(self.s3,'synthetic');run=store.verified_run(status['replay'],read)
        compiler=run['compilers']['option_population_model']['key'];self.s3.data[compiler]+=b' '
        with self.assertRaises(ValueError):store.replay(status['replay'],store.reader(self.s3,'synthetic'))
        self.s3.data[compiler]=self.s3.data[compiler][:-1]
        group=next(k for k in self.s3.data if k.startswith(model.PREFIX+'groups/'));self.s3.data[group]+=b' '
        with self.assertRaises(ValueError):store.replay(status['replay'],store.reader(self.s3,'synthetic'))
    def test_exact_recovery_does_not_refresh_source_or_compilation(self):
        first=self.run_native();packet=self.s3.data[desk.CURRENT]
        result=store.run(self.s3,'synthetic','recover','execution-2',recover_run=first['replay'])
        self.assertEqual(result['replay'],first['replay']);self.assertEqual(self.s3.data[desk.CURRENT],packet)
        self.assertEqual(result['provider_requests'],0)


if __name__=='__main__':unittest.main(verbosity=2)
