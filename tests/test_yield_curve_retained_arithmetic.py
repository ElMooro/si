from pathlib import Path
import io,sys,unittest
from unittest.mock import Mock
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops/checks','aws/ops/staged','scripts')]
import ops_6128_yield_curve_retained_arithmetic as operation


class Tests(unittest.TestCase):
    def test_hash_length_and_private_path_checked(self):
        raw=b'whole original\r\n';digest=operation.baseline.sha(raw)
        ref={'key':operation.baseline.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}
        client=Mock();client.get_object.return_value={'Body':io.BytesIO(raw)}
        self.assertEqual(operation.read(client,ref),raw)
        client.reset_mock()
        with self.assertRaises(ValueError):operation.read(client,dict(ref,key='data/private-account.json'))
        client.get_object.assert_not_called()
        client.get_object.return_value={'Body':io.BytesIO(raw[:-1])}
        with self.assertRaises(ValueError):operation.read(client,ref)

    def test_existing_claim_never_overwritten(self):
        client=Mock();value={'status':'claimed'}
        client.get_object.return_value={'Body':io.BytesIO(operation.baseline.encoded(value))}
        operation.journal(client,value,True)
        self.assertEqual(client.put_object.call_args.kwargs['IfNoneMatch'],'*')
        client.put_object.side_effect=RuntimeError('existing request')
        with self.assertRaises(RuntimeError):operation.journal(client,value,True)

    def test_retained_reader_cannot_fall_back_to_live_sources(self):
        client=Mock();raw=b'{}';digest=operation.baseline.sha(raw)
        ref={'key':operation.baseline.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}
        client.get_object.return_value={'Body':io.BytesIO(raw)}
        with self.assertRaises(ValueError):operation.replay(client,ref,{})
        self.assertEqual(client.get_object.call_count,1)

    def test_only_private_retention_and_complete_scope(self):
        text=Path(operation.__file__).read_text(encoding='utf8')
        for bad in ('.invoke(','.update_function_code(','.update_schedule(','.put_rule(','.urlopen(','.get_secret_value('):
            self.assertNotIn(bad,text)
        self.assertIn('sys.exit(1)',text);self.assertIn("journal(s3,progress,True)",text)
        self.assertEqual(operation.catalog.SERIES,operation.baseline.SERIES)
        self.assertEqual(operation.BASELINE['sha256'],'c6538de44a68d0b17a21f2cd7385ad00f706ab3a063bc8401d9668b41e2675c5')

if __name__=='__main__':unittest.main(verbosity=2)
