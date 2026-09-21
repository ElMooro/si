from pathlib import Path
from unittest.mock import Mock, patch
import json, sys, unittest
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'tests'), str(ROOT/'aws/shared'), str(ROOT/'aws/ops/staged')]
import ops_6004_massive_native_acceptance as op
from test_option_flow_store import S3


class Tests(unittest.TestCase):
    def test_receipts_match_each_consumers_exact_commit_and_package(self):
        rows = [{'function': name, 'code_sha256': str(i)} for i, name in enumerate(op.CONSUMERS[:2])]
        commits = ['a'*40, 'b'*40]
        bodies = [json.dumps({'commit': commit, 'code_sha256': row['code_sha256']}) for commit, row in zip(commits, rows)]
        with patch.object(op, 'source_commit', side_effect=commits), patch.object(op, 'public', side_effect=bodies):
            self.assertEqual(op.receipts(rows), dict(zip(op.CONSUMERS[:2], commits)))

    def test_commit_or_package_mismatch_rejected(self):
        row = {'function': op.CONSUMERS[0], 'code_sha256': 'exact'}
        for body in ({'commit': 'b'*40, 'code_sha256': 'exact'}, {'commit': 'a'*40, 'code_sha256': 'wrong'}):
            with patch.object(op, 'source_commit', return_value='a'*40), patch.object(op, 'public', return_value=json.dumps(body)):
                with self.assertRaises(AssertionError): op.receipts([row])

    def test_source_commit_includes_configuration_and_transitive_helpers(self):
        fn = op.CONSUMERS[0]; source = 'aws/lambdas/'+fn+'/source'
        helper = ROOT/'aws/shared/massive_research_context.py'
        with patch.object(op.subprocess, 'check_output', side_effect=[source+'/lambda_function.py\n', 'a'*40+'\n']) as git, patch.object(op, 'shared_imports', return_value=[helper]):
            self.assertEqual(op.source_commit(fn), 'a'*40)
            self.assertEqual(git.call_args.args[0], ['git', 'log', '-1', '--format=%H', '--', source, 'aws/lambdas/'+fn+'/config.json', 'aws/shared/massive_research_context.py'])

    def test_existing_native_head_is_adopted_without_invocation(self):
        packet = {'contract': op.model.CONTRACT, 'replay': op.RECOVERY}
        s3 = S3({op.model.CURRENT: json.dumps(packet).encode()})
        with patch.object(op, 'completed_request', return_value={'invoke_sent': False}) as complete, patch.object(op, 'invoke_when_available') as invoke:
            self.assertFalse(op.invoke(Mock(), s3, 'a'*40)['invoke_sent'])
            complete.assert_called_once_with(s3, packet); invoke.assert_not_called()

    def test_one_explicit_recovery_has_durable_dispatch_and_alias_completion(self):
        s3 = S3()
        def accept(lam, args):
            self.assertEqual(args['FunctionName'], op.FUNCTION); self.assertEqual(args['InvocationType'], 'Event')
            event = json.loads(args['Payload']); self.assertEqual(event['recover_run'], op.RECOVERY)
            status = {'status': 'complete', 'provider_requests': 0, 'recovered_from': op.RECOVERY,
                      'replay': op.RECOVERY, 'published': True, 'aliases': dict.fromkeys(op.model.PREDECESSORS, True)}
            s3.data[op.store.request_key(event['request_id'])] = json.dumps(status).encode()
            s3.data[op.model.CURRENT] = json.dumps({'contract': op.model.CONTRACT, 'replay': op.RECOVERY}).encode()
            return {'StatusCode': 202}, 0
        with patch.object(op, 'invoke_when_available', side_effect=accept) as invoke:
            result = op.invoke(Mock(), s3, 'a'*40); self.assertTrue(result['invoke_sent']); invoke.assert_called_once()
        claim = json.loads(s3.data[result['dispatch_key']])
        self.assertEqual(claim['status'], 'accepted_async'); self.assertEqual(claim['recovery'], op.RECOVERY)

    def test_uncertain_dispatch_is_never_reinvoked(self):
        commit = 'a'*40; request = 'chatgpt-massive-native-'+commit[:12]+'-1'
        claim = {'contract': 'massive-composite-native-dispatch.v1', 'request_id': request, 'recovery': op.RECOVERY}
        status = {'status': 'complete', 'provider_requests': 0, 'recovered_from': op.RECOVERY, 'replay': op.RECOVERY}
        s3 = S3({op.store.request_key(request+'-dispatch'): json.dumps(claim).encode(), op.store.request_key(request): json.dumps(status).encode()})
        with patch.object(op, 'invoke_when_available') as invoke:
            with self.assertRaises(AssertionError): op.invoke(Mock(), s3, commit)
            invoke.assert_not_called()

    def test_ambiguous_transport_failure_leaves_claim_for_inspection(self):
        s3 = S3(); commit = 'a'*40
        with patch.object(op, 'invoke_when_available', side_effect=TimeoutError('uncertain')) as invoke:
            with self.assertRaises(TimeoutError): op.invoke(Mock(), s3, commit)
            invoke.assert_called_once()
        key = op.store.request_key('chatgpt-massive-native-'+commit[:12]+'-1-dispatch')
        self.assertEqual(json.loads(s3.data[key])['status'], 'claimed')


if __name__ == '__main__': unittest.main(verbosity=2)
