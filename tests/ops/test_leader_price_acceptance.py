from pathlib import Path
from copy import deepcopy
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared','tests/ops')]
import ops_6267_leader_price_acceptance as op
import test_leader_price_candidate as native


class Tests(unittest.TestCase):
    def publication(self):
        memory,p=native.publication();return memory.data[op.KEY],memory.previous,memory
    def test_whole_source_replay_and_exact_archives(self):
        raw,prior,m=self.publication();writes=list(m.writes);result=op.publication(raw,prior,op.read_sources(m,json.loads(raw)))
        self.assertEqual(result['status'],'published_leader_originals_replayed');self.assertEqual(result['request_occurrences'],1)
        self.assertEqual(op.read_public_archive(m,op.archive_ref(raw)),raw);self.assertEqual(op.read_public_archive(m,op.archive_ref(prior)),prior)
        self.assertEqual(m.writes,writes);self.assertFalse(result['investment_authority'])
    def test_tampered_measurements_population_ancestry_and_permissions_rejected(self):
        raw,prior,m=self.publication();source=json.loads(raw)
        edits=[lambda p:p.update(sizing_eligible=True),lambda p:p['source_files']['leader_price_observations.py'].update(sha256='wrong'),
            lambda p:p['source_files']['momentum_research_boundary.py'].update(sha256='wrong'),
            lambda p:p['request_records'][0]['observations']['measurements']['relative_volume_prior_20'].update(value=999),
            lambda p:p['request_records'].pop(),lambda p:p['acquisition_limits'].update(MAX_UNIVERSE=1500),
            lambda p:p.update(acquisition_started_at='2099-01-01T00:00:00Z'),lambda p:p.update(previous_publication=None),
            lambda p:p['quality'].update(measured_occurrences=999),lambda p:p.update(all_scored=[{'ticker':'FAKE','score':99}]),
            lambda p:p['universe_membership']['occurrences'].pop(),
            lambda p:p['request_records'][0]['observations']['benchmark_comparisons']['20'].update(value=999),
            lambda p:p['benchmark']['acquisition']['original_ref'].update(key='data/private.json'),
            lambda p:p['input_acquisitions']['data/convergence-radar.json']['original_ref'].update(key='private/accounts.json')]
        for edit in edits:
            p=deepcopy(source);edit(p)
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode(),prior,op.read_sources(m,p))
    def test_old_packet_pending_unknown_contract_refused_and_only_exact_own_public_history_allowed(self):
        self.assertEqual(op.publication(b'{"schema_version":1}')['status'],'pending_original_schedule_publication')
        with self.assertRaises(ValueError):op.publication(b'{"measurement_contract":"unknown"}')
        m=native.Memory()
        for key in ['private/accounts.json','data/pm-decision.json','data/momentum-leaders/history/../private.json']:
            with self.assertRaises(ValueError):op.read_public_archive(m,{'key':key})
        self.assertEqual(m.reads,[])
    def test_corrupt_retained_source_or_history_cannot_gain_acceptance(self):
        raw,prior,m=self.publication();p=json.loads(raw)
        key=p['input_acquisitions']['data/convergence-radar.json']['original_ref']['key'];m.data[key]+=b' '
        with self.assertRaises(ValueError):op.read_sources(m,p)
        m.data[op.archive_ref(prior)['key']]+=b' '
        with self.assertRaises(ValueError):op.read_public_archive(m,op.archive_ref(prior))


if __name__=='__main__':unittest.main(verbosity=2)
