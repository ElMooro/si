from pathlib import Path
from copy import deepcopy
from unittest.mock import patch
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6286_morning_producer_replay as op


def fixture(engine):
    spec=importlib.util.spec_from_file_location('isolated_'+engine.FN,ROOT/'aws/lambdas'/engine.FN/'tests/run_tests.py')
    native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
    return native.publication()[0]


class Tests(unittest.TestCase):
    def test_actual_writers_replay_only_whole_owned_sources_without_writes(self):
        for engine,_,_ in op.SPECS:
            with self.subTest(engine=engine.FN):
                memory=fixture(engine);writes=list(memory.writes);memory.reads.clear()
                result=op.replay(engine,memory)
                self.assertTrue(result['status'].endswith('_replayed'))
                self.assertFalse(result['investment_authority']);self.assertEqual(memory.writes,writes)
                prefix=engine.KEY[:-5]+'/'
                self.assertTrue(all(key==engine.KEY or key.startswith(prefix) for key in memory.reads))

    def test_tampered_head_cannot_borrow_an_archive(self):
        for engine,_,_ in op.SPECS:
            memory=fixture(engine);original=memory.data[engine.KEY];p=json.loads(original);p['sizing_eligible']=True
            memory.data[engine.KEY]=json.dumps(p).encode();writes=list(memory.writes)
            memory.data[engine.archive_ref(memory.data[engine.KEY])['key']]=original
            with self.assertRaises(ValueError):op.replay(engine,memory)
            self.assertEqual(memory.writes,writes)

    def test_private_previous_publication_rejected_before_read(self):
        for engine,_,_ in op.SPECS:
            memory=fixture(engine);p=json.loads(memory.data[engine.KEY]);p['previous_publication']={'key':'private/accounts.json','sha256':'a'*64,'bytes':2}
            raw=json.dumps(p).encode();memory.data[engine.KEY]=raw;memory.data[engine.archive_ref(raw)['key']]=raw;memory.reads.clear()
            with self.assertRaises(ValueError):op.replay(engine,memory)
            self.assertNotIn('private/accounts.json',memory.reads)

    def test_route_change_fails_before_any_source_replay(self):
        engine,base_name,prior_name=op.SPECS[0];directory=ROOT/'docs/audit/2026-09-28'
        baseline=json.loads((directory/base_name).read_bytes());prior=json.loads((directory/prior_name).read_bytes())
        package=prior['actual_packages'][engine.FN];route=deepcopy(prior['fanout_route']);route['routes'][0]['expression']='rate(1 minute)'
        clients={n:object() for n in ('lambda','s3','events','scheduler')}
        with patch.object(op,'expected_commit',return_value=package['receipt']['commit']),patch.object(op,'runtime',return_value=package),patch.object(engine,'fanout',return_value=route),patch.object(op,'replay') as replay:
            with self.assertRaisesRegex(ValueError,'Original scheduled producer route'):op.accept(engine,baseline,prior,clients)
            replay.assert_not_called()


if __name__=='__main__':unittest.main(verbosity=2)
