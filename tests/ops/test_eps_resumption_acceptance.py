from pathlib import Path
from copy import deepcopy
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6285_eps_resumption_acceptance as op
import test_eps_target_acceptance as existing
n=existing.native


class Tests(unittest.TestCase):
    def cycle(self):
        memory=n.Memory();test=n.Tests();tree=ast.parse((n.SRC/'lambda_function.py').read_bytes())
        backup=next(ast.literal_eval(node.value) for node in tree.body if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='SP500_BACKUP' for t in node.targets))
        results=[]
        for value in (2,10,20,3):
            prior=memory.data[op.KEY];test.baseline_run(memory,value,MAX_TICKERS=3,SP500_BACKUP=backup)
            raw=memory.data[op.KEY];results.append(op.publication(raw,prior,op.retained.source_reader(memory)))
        return raw,prior,memory,results

    def test_complete_cycle_and_prior_comparisons_replay_with_no_new_requests(self):
        raw,prior,memory,results=self.cycle();self.assertEqual([r['pending_occurrences'] for r in results],[2,1,0,2])
        self.assertTrue(results[2]['complete_selected_visit_cycle']);self.assertFalse(results[2]['whole_source_universe_coverage_verified'])
        self.assertEqual(results[3]['comparison_sources_received'],1);self.assertEqual(results[3]['annual_arrays_received'],1)
        writes=list(memory.writes);op.publication(raw,prior,op.retained.source_reader(memory));self.assertEqual(memory.writes,writes)

    def test_false_order_complete_cycle_or_outcome_progress_fails(self):
        raw,prior,memory,_=self.cycle();source=json.loads(raw)
        edits=[lambda p:p.pop('acquisition_progress'),lambda p:p['acquisition_progress'].update(cycle_complete=True),
            lambda p:p['acquisition_progress'].update(planned_symbols=['THIRD','TEST','SECOND']),
            lambda p:p['acquisition_progress'].update(visited_occurrences=2),lambda p:p['acquisition_progress'].update(retained_provider_bytes=0),
            lambda p:p['acquisition_progress'].update(stop_reason='provider_authorization_error'),
            lambda p:p['request_records'][1]['acquisitions'][0].update(status='received')]
        for edit in edits:
            p=deepcopy(source);edit(p)
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode(),prior,op.retained.source_reader(memory))

    def test_previous_version_is_pending_and_acceptance_cannot_invoke_or_write(self):
        for version in ('1.1.0','1.1.1','1.2.0'):
            out=op.publication(json.dumps({'measurement_contract':n.CONTRACT,'version':version}).encode())
            self.assertFalse(out['current_compiler_publication_verified']);self.assertFalse(out['investment_authority'])
        source=Path(op.__file__).read_text(encoding='utf-8')
        for forbidden in ('invoke(', 'put_object(', 'update_function', 'get_secret_value', 'list_objects'):self.assertNotIn(forbidden,source)


if __name__=='__main__':unittest.main(verbosity=2)
