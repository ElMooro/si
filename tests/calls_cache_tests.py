"""Current native reader, deterministic concurrency and retained-run compatibility."""
from pathlib import Path
from concurrent.futures import Future,ThreadPoolExecutor
from threading import Event,Condition,Barrier
from unittest.mock import patch
import copy,hashlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import calls_original_reader as module
import calls_research_replay as replay

def reader():return module.ImmutableReader(lambda key: (_ for _ in ()).throw(AssertionError('Transport forbidden')))
def ident(n):return f'{n:064x}'


class Cache(unittest.TestCase):
    def joined(self,fail=False):
        store=reader();entered=Event();release=Event();waiting=Event();calls=[]
        class TrackedFuture(Future):
            def result(self,*a,**kw):waiting.set();return super().result(*a,**kw)
        def build():
            calls.append('first');entered.set();assert release.wait(3)
            if fail:raise ValueError('synthetic builder failed')
            return b'first complete result'
        def other():calls.append('second');return b'conflicting result'
        with patch.object(module,'Future',TrackedFuture),ThreadPoolExecutor(max_workers=2) as pool:
            first=pool.submit(store.verified_snapshot,ident(1),build);self.assertTrue(entered.wait(3))
            second=pool.submit(store.verified_snapshot,ident(1),other)
            joined=waiting.wait(3);release.set()
            if fail:
                for result in (first,second):
                    with self.assertRaisesRegex(ValueError,'synthetic builder failed'):result.result(3)
            else:self.assertEqual(first.result(3),second.result(3))
        self.assertTrue(joined);self.assertEqual(calls,['first']);self.assertEqual(store._verified_pending,{})
        return store
    def test_one_build_per_identity_and_no_double_byte_accounting(self):
        store=self.joined();self.assertEqual(len(store._verified),1)
        self.assertEqual(store._verified_bytes,len(b'first complete result'))
        self.assertEqual(store.verified_snapshot(ident(1),lambda:self.fail('rebuilt')),b'first complete result')
    def test_failure_reaches_waiters_releases_reservation_and_allows_retry(self):
        store=self.joined(True);self.assertEqual(store._verified,{});self.assertEqual(store._verified_bytes,0)
        self.assertEqual(store.verified_snapshot(ident(1),lambda:b'retry'),b'retry')
    def test_pending_builds_count_against_eight_snapshot_limit(self):
        store=reader();release=Event();condition=Condition();entered=[]
        def build():
            with condition:entered.append(1);condition.notify_all()
            assert release.wait(3);return b'complete'
        def notify(_):
            with condition:condition.notify_all()
        with ThreadPoolExecutor(max_workers=9) as pool:
            tasks=[pool.submit(store.verified_snapshot,ident(n),build) for n in range(9)]
            for task in tasks:task.add_done_callback(notify)
            with condition:self.assertTrue(condition.wait_for(lambda:len(entered)==9 or any(task.done() for task in tasks),timeout=3))
            release.set();accepted=0
            for task in tasks:
                try:task.result(3);accepted+=1
                except ValueError:pass
        self.assertEqual(accepted,8);self.assertEqual(len(entered),8);self.assertEqual(len(store._verified),8)
        self.assertEqual(store._verified_pending,{});self.assertEqual(store._verified_bytes,8*len(b'complete'))
    def test_parallel_hashing_cannot_overcommit_the_real_64_mib_cache_limit(self):
        store=reader();raw=b'x'*(40*1024*1024);barrier=Barrier(2);real_hash=hashlib.sha256
        def controlled_hash(value):barrier.wait(timeout=3);return real_hash(value)
        with patch.object(module.hashlib,'sha256',controlled_hash),ThreadPoolExecutor(max_workers=2) as pool:
            tasks=[pool.submit(store.verified_snapshot,ident(n),lambda:raw) for n in range(2)]
            accepted=[]
            for n,task in enumerate(tasks):
                try:self.assertIs(task.result(3),raw);accepted.append(n)
                except ValueError:pass
        self.assertEqual(len(accepted),1);self.assertEqual(store._verified_bytes,40*1024*1024)
        self.assertEqual(len(store._verified),1);self.assertEqual(store._verified_pending,{})
        rejected=1-accepted[0];self.assertEqual(store.verified_snapshot(ident(rejected),lambda:b'small retry'),b'small retry')
        self.assertEqual(store._verified_bytes,40*1024*1024+len(b'small retry'))
    def test_invalid_identity_or_result_never_reserves_or_charges_cache(self):
        store=reader()
        for identity in ('bad','A'*64,None,False):
            with self.assertRaises(ValueError):store.verified_snapshot(identity,lambda:self.fail('builder called'))
        for value in (b'',None,'text',bytearray(b'abc')):
            with self.assertRaises(ValueError):store.verified_snapshot(ident(1),lambda:value)
            self.assertEqual(store._verified_pending,{});self.assertEqual(store._verified_bytes,0)
        self.assertEqual(store.ciss_snapshot(ident(1),lambda:b'valid'),b'valid')
    def test_builder_may_read_originals_and_build_other_identity_without_lock_deadlock(self):
        raw=b'synthetic original';digest=hashlib.sha256(raw).hexdigest();key='data/evidence/fred/'+ident(1)+'/'+digest+'.bin.gz'
        store=module.ImmutableReader(lambda path:raw)
        def build():return store(key)+store.verified_snapshot(ident(2),lambda:b' nested')
        with ThreadPoolExecutor(max_workers=1) as pool:
            self.assertEqual(pool.submit(store.verified_snapshot,ident(1),build).result(3),raw+b' nested')
        self.assertEqual(store._verified_pending,{});self.assertEqual(len(store._verified),2)
    def test_recursive_identity_is_explicit_failure_and_next_call_recovers(self):
        store=reader()
        with self.assertRaisesRegex(ValueError,'Recursive'):
            store.verified_snapshot(ident(1),lambda:store.verified_snapshot(ident(1),lambda:b'bad'))
        self.assertEqual(store._verified_pending,{});self.assertEqual(store._verified_bytes,0)
        self.assertEqual(store.verified_snapshot(ident(1),lambda:b'good'),b'good')
    def test_cached_hash_and_type_are_rechecked_before_return(self):
        store=reader();store.verified_snapshot(ident(1),lambda:b'original')
        for value in (('0'*64,b'original'),('0'*64,'not bytes'),(),False):
            store._verified[ident(1)]=value
            with self.assertRaises(ValueError):store.verified_snapshot(ident(1),lambda:self.fail('rebuilt corruption'))
    def test_base_exception_releases_inflight_reservation(self):
        class Cancelled(BaseException):pass
        store=reader()
        def cancelled():raise Cancelled()
        with self.assertRaises(Cancelled):store.verified_snapshot(ident(1),cancelled)
        self.assertEqual(store._verified_pending,{});self.assertEqual(store._verified_bytes,0)
        self.assertEqual(store.verified_snapshot(ident(1),lambda:b'recovered'),b'recovered')


class Compatibility(unittest.TestCase):
    def test_all_three_exact_reviewed_versions_replay_without_executing_stored_code(self):
        for name in ('pre-liquidity-transport-calls-bundle.json','pre-calls-publication-bundle.json','pre-calls-cache-bundle.json'):
            with self.subTest(name=name):
                raw=(ROOT/'tests/fixtures'/name).read_bytes();bundle=json.loads(raw);before=replay.canonical(bundle)
                proof=replay.replay(bundle);self.assertEqual(proof['status'],'reproduced');self.assertEqual(proof['compiler_match'],'reviewed_storage_transport_revision')
                self.assertEqual(replay.canonical(bundle),before)
    def test_unknown_reader_revision_mixed_source_set_and_changed_output_are_rejected(self):
        bundle=json.loads((ROOT/'tests/fixtures/pre-calls-cache-bundle.json').read_bytes())
        for edit in (lambda b:b['payload']['compiler']['files'].update({'calls_original_reader.py':'0'*64}),
                     lambda b:b['payload']['compiler']['files'].update({'liquidity_flow_store.py':'4e7c99fe5eabdae3380cbdbbb7ecf6d65d25928c21b64c36b373402924336533'}),
                     lambda b:b['payload']['output'].update({'brief_md':'modified output'})):
            value=copy.deepcopy(bundle);edit(value);value['payload_sha256']=replay.digest(value['payload']);value['run_id']='calls-research-'+value['payload_sha256']
            with self.assertRaises(ValueError):replay.replay(value)


def run():
    suite=unittest.TestSuite(unittest.defaultTestLoader.loadTestsFromTestCase(cls) for cls in (Cache,Compatibility))
    result=unittest.TextTestRunner(verbosity=2).run(suite)
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
