"""Private source retention, claim and reviewed isolated replay boundaries."""
from pathlib import Path
import io,sys,unittest
from unittest.mock import Mock,patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops/checks','aws/ops/staged','scripts','aws/lambdas/justhodl-term-premium/source')]
import ops_6131_term_premium_workbook_qualification as operation


class Tests(unittest.TestCase):
    def test_complete_hash_length_and_private_path(self):
        raw=b'whole workbook\r\n';digest=operation.baseline.sha(raw)
        ref={'key':operation.baseline.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}
        client=Mock();client.get_object.return_value={'Body':io.BytesIO(raw)}
        self.assertEqual(operation.read(client,ref),raw)
        for bad in (dict(ref,key='data/account.json'),dict(ref,bytes=True),dict(ref,bytes=0),dict(ref,key=ref['key']+'/../x')):
            client.reset_mock()
            with self.assertRaises(ValueError):operation.read(client,bad)
            client.get_object.assert_not_called()
        client.get_object.return_value={'Body':io.BytesIO(raw[:-1])}
        with self.assertRaises(ValueError):operation.read(client,ref)

    def test_existing_claim_cannot_be_replaced(self):
        client=Mock();value={'status':'claimed'}
        client.get_object.return_value={'Body':io.BytesIO(operation.baseline.encoded(value))}
        operation.journal(client,value,True)
        self.assertEqual(client.put_object.call_args.kwargs['IfNoneMatch'],'*')
        client.put_object.side_effect=RuntimeError('existing request')
        with self.assertRaises(RuntimeError):operation.journal(client,value,True)

    def test_source_is_fixed_and_redirects_are_not_admitted(self):
        response=Mock();response.geturl.return_value='https://example.com/replacement.xls'
        response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False)
        with patch.object(operation.urllib.request,'urlopen',return_value=response) as fetch:
            with self.assertRaises(ValueError):operation.acquire()
            self.assertEqual(fetch.call_count,1)
            self.assertEqual(fetch.call_args.args[0].full_url,operation.candidate.URL)
        response.read.assert_not_called()

    def test_only_reviewed_complete_compiler_closure_can_be_replayed(self):
        paths=operation.compiler_paths()
        self.assertEqual(len(paths),12);self.assertIn('xlrd/book.py',paths)
        client=Mock()
        with self.assertRaises(ValueError):operation.isolated_replay(client,{}, {}, {'../x.py':{}},'2026-09-26T00:00:00Z')
        client.get_object.assert_not_called()
        compilers={name:{} for name in paths}
        with patch.object(operation,'compiler_paths',return_value=paths),patch.object(operation,'read',return_value=b'unreviewed source'),patch.object(operation.subprocess,'run') as run:
            with self.assertRaises(ValueError):operation.isolated_replay(client,{}, {},compilers,'2026-09-26T00:00:00Z')
            run.assert_not_called()

    def test_durable_attempt_precedes_source_request_and_no_native_actions(self):
        text=Path(operation.__file__).read_text(encoding='utf8')
        for bad in ('.invoke(','.update_function_code(','.update_schedule(','.put_rule(','.get_secret_value('):self.assertNotIn(bad,text)
        self.assertLess(text.index("provider_request_attempts=1);journal(s3,progress)"),text.index('raw,source=acquire()'))
        self.assertIn('sys.exit(1)',text);self.assertIn("journal(s3,progress,True)",text)
        self.assertNotIn('urlopen',operation.ISOLATED_REPLAY);self.assertNotIn('boto3',operation.ISOLATED_REPLAY)


if __name__=='__main__':unittest.main(verbosity=2)
