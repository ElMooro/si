from pathlib import Path
from copy import deepcopy
from types import SimpleNamespace
from unittest.mock import patch
import importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6260_activist_filings_acceptance as op
spec=importlib.util.spec_from_file_location('isolated_activist_native_tests',ROOT/'aws/lambdas/justhodl-activist-filings-scanner/tests/run_tests.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)


class Tests(unittest.TestCase):
    def publication(self):
        memory,p=native.publication()
        return memory.data['data/activist-filings.json'],memory.previous,memory
    def test_whole_source_replay_and_exact_immutable_archives(self):
        raw,prior,m=self.publication();result=op.publication(raw,prior,op.read_sources(m,json.loads(raw)))
        self.assertEqual(result['status'],'published_ownership_originals_replayed');self.assertEqual(result['entry_occurrences'],2)
        self.assertEqual(op.read_public_archive(m,op.archive_ref(raw)),raw);self.assertEqual(op.read_public_archive(m,op.archive_ref(prior)),prior)
        writes=list(m.writes);op.publication(raw,prior,op.read_sources(m,json.loads(raw)));self.assertEqual(m.writes,writes)
    def test_altered_sources_roles_population_permissions_and_clocks_fail(self):
        raw,prior,m=self.publication();source=json.loads(raw)
        edits=[lambda p:p.update(sizing_eligible=True),lambda p:p['source_files']['filing_observations.py'].update(sha256='wrong'),
            lambda p:p['entry_occurrences'][0].update(role='Subject'),lambda p:p['entry_occurrences'].pop(),
            lambda p:p['filing_groups'][0].update(reported_beneficial_ownership_percent=90),
            lambda p:p.update(acquisition_started_at='2099-01-01T00:00:00Z'),lambda p:p.update(previous_publication=None),
            lambda p:p['feed_coverage'][0].update(returned_entries=999),
            lambda p:p['acquisitions']['universe']['original_ref'].update(key='private/account.json')]
        for edit in edits:
            p=deepcopy(source);edit(p)
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode(),prior,op.read_sources(m,p))
    def test_old_packet_pending_and_only_exact_public_history_can_be_read(self):
        self.assertEqual(op.publication(b'{"schema_version":1}')['status'],'pending_original_fanout_publication')
        m=native.Memory()
        for key in ['private/accounts.json','data/pm-decision.json','data/activist-filings/history/../private.json']:
            with self.assertRaises(ValueError):op.read_public_archive(m,{'key':key})
        self.assertEqual(m.reads,[])
    def test_actual_fanout_scope_enabled_payload_and_no_invocation(self):
        m=native.Memory();m.data['config/fanout-manifest.json']=json.dumps({'ticks':{'daily-eve':[op.FN],'other':['unrelated']},'disabled':[]}).encode()
        arn='arn:aws:lambda:us-east-1:fixture:function:justhodl-scheduler';state={'enabled':'ENABLED','tick':'daily-eve'}
        def paginator(kind):
            return SimpleNamespace(paginate=lambda **kw:[{'RuleNames':['morning']}] if kind=='list_rule_names_by_target' else [{'Targets':[{'Arn':arn,'Input':json.dumps({'tick':state['tick']})}]}])
        clients={'s3':m,'lambda':SimpleNamespace(get_function_configuration=lambda **kw:{'FunctionArn':arn,'Environment':{'Variables':{'FANOUT_MANIFEST_KEY':'config/fanout-manifest.json','SECRET':'never-return'}}}),
            'events':SimpleNamespace(get_paginator=paginator,describe_rule=lambda **kw:{'State':state['enabled'],'ScheduleExpression':'cron(0 22 * * ? *)'}),'scheduler':None}
        with patch.object(op,'runtime',lambda *a:{'code_sha256':'fixture-hash','source_files_checked':2}):
            out=op.fanout(clients);self.assertEqual(out['matching_ticks'],['daily-eve']);self.assertNotIn('never-return',json.dumps(out));self.assertEqual(m.writes,[])
            state['enabled']='DISABLED'
            with self.assertRaises(ValueError):op.fanout(clients)
            state.update(enabled='ENABLED',tick='other')
            with self.assertRaises(ValueError):op.fanout(clients)
        for doc in [{'ticks':{'daily-eve':[op.FN]},'disabled':[op.FN]},{'ticks':{'daily-eve':[op.FN,op.FN]}},{'ticks':{'daily-eve':['justhodl-eps-revision-velocity']}}]:
            with self.assertRaises(ValueError):op.manifest_membership(json.dumps(doc).encode())


if __name__=='__main__':unittest.main(verbosity=2)
