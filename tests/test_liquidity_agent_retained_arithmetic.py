from pathlib import Path
import io, sys, unittest
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops/checks','aws/ops/staged','scripts',
    'aws/lambdas/justhodl-liquidity-flow/source')]
import ops_6125_liquidity_agent_retained_arithmetic as operation


class Tests(unittest.TestCase):
    def test_retained_read_checks_path_length_and_hash_before_use(self):
        raw=b'whole original\r\n';digest=operation.baseline.sha(raw)
        ref={'key':operation.baseline.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}
        client=Mock();client.get_object.return_value={'Body':io.BytesIO(raw)}
        self.assertEqual(operation.read(client,ref),raw)
        client.reset_mock()
        with self.assertRaises(ValueError):operation.read(client,dict(ref,key='data/private-account.json'))
        client.get_object.assert_not_called()
        client.get_object.return_value={'Body':io.BytesIO(raw[:-1])}
        with self.assertRaises(ValueError):operation.read(client,ref)

    def test_claim_is_conditional_and_existing_request_cannot_be_overwritten(self):
        client=Mock();value={'status':'claimed'};raw=operation.baseline.encoded(value)
        client.get_object.return_value={'Body':io.BytesIO(raw)}
        operation.journal(client,value,True)
        self.assertEqual(client.put_object.call_args.kwargs['IfNoneMatch'],'*')
        self.assertTrue(client.put_object.call_args.kwargs['Key'].startswith(operation.baseline.PRIVATE+'requests/'))
        client.put_object.side_effect=RuntimeError('already claimed')
        with self.assertRaises(RuntimeError):operation.journal(client,value,True)

    def test_operation_preserves_both_accepted_populations_without_live_side_effects(self):
        text=Path(operation.__file__).read_text(encoding='utf-8')
        for forbidden in ('.invoke(','.update_function_code(','.update_function_configuration(','.put_rule(','.urlopen('):
            self.assertNotIn(forbidden,text)
        self.assertIn('sys.exit(1)',text)
        self.assertIn('145589',text)
        self.assertEqual(operation.BASELINE,operation.scope_operation.BASELINE)
        self.assertEqual(operation.SCOPE['sha256'],'3ccd3cd4810a9f533a47868a3b3cae2c0ff515da95ab9257be82d035ee155f87')


if __name__=='__main__':unittest.main(verbosity=2)
