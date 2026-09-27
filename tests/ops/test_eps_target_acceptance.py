from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone
from types import SimpleNamespace
from unittest.mock import patch
import ast,importlib.util,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2];sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks','aws/shared')]
import ops_6250_eps_target_acceptance as op
spec=importlib.util.spec_from_file_location('isolated_eps_native_tests',ROOT/'aws/lambdas/justhodl-eps-revision-velocity/tests/run_tests.py');native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)


class Tests(unittest.TestCase):
    def publication(self):
        m=native.Memory();prior=m.previous
        tree=ast.parse((native.SRC/'lambda_function.py').read_bytes());backup=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='SP500_BACKUP' for t in n.targets))
        ns=native.native(m,SP500_BACKUP=backup)
        def company(ticker):
            return [native.envelope(json.dumps(values).encode(),endpoint,datetime.now(timezone.utc).isoformat()) for endpoint,values in [('quote',[{'symbol':ticker,'marketCap':1e9}]),('analyst-estimates',native.forecasts())]]
        ns['_eps_company']=company;ns['lambda_handler']();return m.data['data/eps-revision-velocity.json'],prior,m
    def test_native_whole_response_and_membership_replay(self):
        raw,prior,m=self.publication();out=op.publication(raw,prior)
        self.assertEqual(out['status'],'published_eps_originals_replayed');self.assertEqual(out['request_occurrences'],2)
        self.assertEqual(op.read_public_archive(m,op.archive_ref(raw)),raw);self.assertEqual(op.read_public_archive(m,op.archive_ref(prior)),prior)
        writes=list(m.writes);op.publication(raw,prior);self.assertEqual(writes,m.writes)
    def test_tampered_values_membership_clock_scope_and_permissions_fail(self):
        raw,prior,_=self.publication();source=json.loads(raw)
        edits=[lambda p:p['request_records'][0]['estimate_observations'][0]['values'].update(epsAvg=99),
               lambda p:p['universe_membership']['occurrences'].pop(),lambda p:p['legacy_static_backup'].pop(),
               lambda p:p.update(calls_eligible=True),lambda p:p.update(n_estimate_observations=999),lambda p:p.update(previous_publication=None),
               lambda p:p.update(acquisition_started_at='2099-01-01T00:00:00Z')]
        edits.append(lambda p:p['source_files']['eps_observations.py'].update(sha256='wrong'))
        for edit in edits:
            p=deepcopy(source);edit(p)
            with self.assertRaises(ValueError):op.publication(json.dumps(p).encode(),prior)
    def test_only_declared_public_history_and_old_packet_pending(self):
        self.assertEqual(op.publication(b'{"schema_version":1}')['status'],'pending_original_fanout_publication')
        m=native.Memory()
        for key in ['private/accounts.json','data/pm-decision.json','data/eps-revision-velocity/history/../private.json']:
            with self.assertRaises(ValueError):op.read_public_archive(m,{'key':key})
        self.assertEqual(m.reads,[])
    def test_live_fanout_checks_membership_payload_and_enabled_rule_without_invocation(self):
        m=native.Memory();m.data['config/fanout-manifest.json']=json.dumps({'ticks':{'daily-morn':[op.FN],'other':['unrelated']},'disabled':[]}).encode()
        arn='arn:aws:lambda:us-east-1:fixture:function:justhodl-scheduler';state={'enabled':'ENABLED','tick':'daily-morn'}
        def paginator(kind):
            return SimpleNamespace(paginate=lambda **kw:[{'RuleNames':['morning']}] if kind=='list_rule_names_by_target' else [{'Targets':[{'Arn':arn,'Input':json.dumps({'tick':state['tick']})}]}])
        clients={'s3':m,'lambda':SimpleNamespace(get_function_configuration=lambda **kw:{'FunctionArn':arn,'Environment':{'Variables':{'FANOUT_MANIFEST_KEY':'config/fanout-manifest.json','SECRET':'never-return'}}}),
                 'events':SimpleNamespace(get_paginator=paginator,describe_rule=lambda **kw:{'State':state['enabled'],'ScheduleExpression':'cron(0 11 * * ? *)'}),'scheduler':None}
        with patch.object(op,'runtime',lambda *a:{'code_sha256':'fixture-hash','source_files_checked':2}):
            out=op.fanout(clients);self.assertEqual(out['matching_ticks'],['daily-morn']);self.assertEqual(out['routes'][0]['expression'],'cron(0 11 * * ? *)');self.assertNotIn('never-return',json.dumps(out));self.assertEqual(m.writes,[])
            state['enabled']='DISABLED'
            with self.assertRaises(ValueError):op.fanout(clients)
            state.update(enabled='ENABLED',tick='other')
            with self.assertRaises(ValueError):op.fanout(clients)
        for doc in [{'ticks':{'daily-morn':[op.FN]},'disabled':[op.FN]},{'ticks':{'daily-morn':[op.FN,op.FN]}},{'ticks':{}}]:
            with self.assertRaises(ValueError):op.manifest_membership(json.dumps(doc).encode())


if __name__=='__main__':unittest.main(verbosity=2)
