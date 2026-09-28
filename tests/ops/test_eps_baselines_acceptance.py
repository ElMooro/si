from pathlib import Path
from copy import deepcopy
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6284_eps_baselines_acceptance as op
import test_eps_target_acceptance as existing
n=existing.native


class Tests(unittest.TestCase):
    def three(self):
        memory=n.Memory();test=n.Tests()
        tree=ast.parse((n.SRC/'lambda_function.py').read_bytes())
        backup=next(ast.literal_eval(node.value) for node in tree.body if isinstance(node,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='SP500_BACKUP' for t in node.targets))
        for value in (2,None,3):
            prior=memory.data[op.KEY];test.baseline_run(memory,value,SP500_BACKUP=backup)
            raw=memory.data[op.KEY];result=op.publication(raw,prior,op.source_reader(memory))
        return raw,prior,memory,result

    def test_three_actual_runs_replay_from_immutable_older_original(self):
        raw,prior,memory,result=self.three();self.assertTrue(result['current_compiler_publication_verified'])
        self.assertEqual(result['comparison_sources_received'],1);self.assertEqual(result['retained_baselines'],1)
        self.assertEqual(json.loads(raw)['request_records'][0]['same_target_comparisons'][0]['eps_change'],1)
        before=list(memory.writes);op.publication(raw,prior,op.source_reader(memory));self.assertEqual(memory.writes,before)
        self.assertTrue(all(body.closed for body in memory.bodies))

    def test_catalogue_clock_reference_status_and_value_tampering_fail(self):
        raw,prior,memory,_=self.three();p=json.loads(raw)
        for edit in [lambda p:p['estimate_baselines']['entries'].clear(),lambda p:p['estimate_baselines']['entries'][0].update(received_at=p['generated_at']),
            lambda p:p['estimate_baselines']['entries'][0]['original_ref'].update(key='private/portfolio.json'),
            lambda p:p['request_records'][0]['comparison_source'].update(status='unavailable',error_type='OSError'),
            lambda p:p['request_records'][0]['comparison_source']['baseline'].update(ticker='OTHER'),
            lambda p:p['request_records'][0]['same_target_comparisons'][0].update(eps_change=99),
            lambda p:p['retained_source_io'].update(total_attempts=2),lambda p:p['retained_source_io'].update(worker_limit=True),
            lambda p:p.pop('estimate_baselines')]:
            q=deepcopy(p);edit(q)
            with self.assertRaises(ValueError):op.publication(json.dumps(q).encode(),prior,op.source_reader(memory))

    def test_missing_or_corrupt_whole_sources_never_qualify(self):
        raw,prior,memory,_=self.three();p=json.loads(raw)
        for descriptor in [p['estimate_baselines']['entries'][0],p['request_records'][0]['comparison_source']['baseline']]:
            key=descriptor['original_ref']['key'];saved=memory.data[key]
            memory.data[key]=b'[]'
            with self.assertRaises(ValueError):op.publication(raw,prior,op.source_reader(memory))
            memory.data[key]=saved
        with self.assertRaises(ValueError):op.publication(raw,prior)

    def test_only_owned_sources_and_unmodified_schedule_are_read(self):
        memory=n.Memory();reader=op.source_reader(memory)
        for key in ('private/accounts.json','data/ai-brief.json','data/eps-revision-velocity/sources/../bad.json'):
            descriptor=n.estimate_descriptor('TEST',n.capture());descriptor['original_ref']['key']=key
            with self.assertRaises(ValueError):reader(descriptor)
        self.assertEqual(memory.reads,[])
        source=Path(op.__file__).read_text(encoding='utf-8')
        for forbidden in ('invoke(', 'put_object(', 'update_function', 'get_secret_value', 'list_objects'):self.assertNotIn(forbidden,source)

    def test_older_compilers_are_pending_not_accepted(self):
        for p in ({},{'measurement_contract':n.CONTRACT,'version':'1.1.0'},{'measurement_contract':n.CONTRACT,'version':'1.1.1'}):
            out=op.publication(json.dumps(p).encode());self.assertFalse(out['current_compiler_publication_verified']);self.assertFalse(out['investment_authority'])


if __name__=='__main__':unittest.main(verbosity=2)
