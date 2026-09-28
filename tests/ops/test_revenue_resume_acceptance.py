from pathlib import Path
from copy import deepcopy
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6280_revenue_resume_acceptance as op
from test_revenue_resume import n


class Tests(unittest.TestCase):
    def test_current_publication_requires_literal_counters_and_flags(self):
        _,raw,prior=self.packet();base=json.loads(raw)
        for fields in ({'visited_occurrences':2.0},{'pending_occurrences':True},{'cycle_complete':0},
                       {'visited_is_not_successful_provider_response':1},{'request_order_is_rank':True},
                       {'original_population_order_preserved':1},{'schedule_accelerated':0}):
            changed=deepcopy(base);changed['acquisition_progress'].update(fields)
            with self.assertRaisesRegex(ValueError,'Literal acquisition'):op.publication(json.dumps(changed).encode(),prior)
        for version in ('1.1.0','1.2.0'):
            result=op.publication(json.dumps({'measurement_contract':op.original.compiler().CONTRACT,'version':version}).encode())
            self.assertFalse(result['new_queue_live_verified']);self.assertFalse(result['investment_authority'])

    def packet(self):
        m=n.Memory();ns=n.native(m,N_WORKERS=1);seen=[]
        def company(member):
            seen.append(member);return n.company(member) if len(seen)==1 else [{'endpoint':'income-statement','status':'rate_limited'}]
        ns['_revenue_company']=company;ns['lambda_handler']()
        return m,m.data[op.KEY],m.previous
    def test_exact_replay_of_native_resume_and_previous_immutable_publication(self):
        m,raw,prior=self.packet();writes=list(m.writes);r=op.publication(raw,prior)
        self.assertTrue(r['new_queue_live_verified']);self.assertEqual(r['visited_occurrences'],2)
        self.assertEqual(r['remaining_occurrences'],1);self.assertEqual(r['received_statements'],1)
        self.assertEqual(m.writes,writes);self.assertFalse(r['whole_universe_current_coverage_qualified'])
    def test_counts_coordinates_sources_and_budget_claims_cannot_be_forged(self):
        _,raw,prior=self.packet();p=json.loads(raw)
        for edit in [lambda p:p['acquisition_progress'].update(pending_occurrences=0),
            lambda p:p['acquisition_progress'].update(retained_provider_bytes=0),lambda p:p['acquisition_progress'].update(visited_request_indices=[0,2]),
            lambda p:p['acquisition_progress'].update(stop_reason='source_byte_budget'),lambda p:p.update(acquisition_progress=None),
            lambda p:p['source_files']['lambda_function.py'].update(bytes=1)]:
            q=deepcopy(p);edit(q)
            with self.assertRaises(ValueError):op.publication(json.dumps(q).encode(),prior)
    def test_previous_version_is_explicitly_pending_not_current_source_acceptance(self):
        r=op.publication(json.dumps({'measurement_contract':op.original.compiler().CONTRACT,'version':'1.1.0','generated_at':'2026-09-28T08:00:48Z'}).encode())
        self.assertEqual(r['status'],'pending_original_fanout_resumed_publication');self.assertFalse(r['new_queue_live_verified'])
    def test_scope_has_no_invocation_or_output_path_broadening(self):
        source=Path(op.__file__).read_text(encoding='utf-8')
        for forbidden in ('invoke(', 'put_object(', 'update_function', 'create_schedule', 'list_objects', 'get_secret_value'):
            self.assertNotIn(forbidden,source)
        self.assertEqual(op.KEY,'data/revenue-acceleration.json')


if __name__=='__main__':unittest.main(verbosity=2)
