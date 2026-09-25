from pathlib import Path
import io, json, sys, unittest
sys.path[:0] = [str(Path(__file__).resolve().parents[1] / p) for p in ('aws/shared', 'aws/ops/checks')]
import statement_identity_capture as capture
import statement_research_identity as identity
import statement_research_store_v2 as store
import financial_statement_campaign as campaign
from test_statement_research_v2 import Fixture
from test_statement_producer import S3


class Response(io.BytesIO):
    def __init__(self, body, status=200):
        super().__init__(body); self.status=status
        self.headers={'Content-Type':'application/json','Content-Length':str(len(body))}


class Tests(unittest.TestCase):
    def test_one_sec_request_retains_entire_original_and_repeat_reuses_validated_capture(self):
        fixture=Fixture();body=fixture.files[fixture.capture['original']['key']];client=S3({});calls=[]
        def fetch(request):
            calls.append(request.full_url)
            self.assertEqual(request.full_url,identity.URL)
            self.assertNotIn('Authorization',request.headers)
            return Response(body)
        clock=lambda:'2026-03-01T14:00:00Z'
        ref=capture.capture(client,'capture-once',clock,fetch)
        cap,raw,mapping=identity.capture(ref,store.reader(client,campaign.BUCKET))
        self.assertEqual(raw,body);self.assertEqual(mapping['ABC'],['0000000123'])
        self.assertEqual(capture.capture(client,'capture-once',clock,fetch),ref)
        self.assertEqual(calls,[identity.URL])
        self.assertTrue(all(v['Key'].startswith(campaign.PRIVATE) for v in client.writes))

    def test_error_empty_and_incomplete_responses_are_retained_and_not_retried(self):
        for body,status in ((b'denied',403),(b'',200),(b'{"0":',200),(b'{}',200)):
            client=S3({});calls=[]
            def fetch(request):calls.append(request.full_url);return Response(body,status)
            clock=lambda:'2026-03-01T14:00:00Z'
            with self.assertRaises(ValueError):capture.capture(client,'bad-source',clock,fetch)
            journal=json.loads(client.files[campaign.request_key('bad-source','sec-identity')])
            self.assertEqual(journal['status'],'failed');self.assertEqual(journal['http_status'],status)
            self.assertEqual(client.files[journal['original']['key']],body)
            with self.assertRaisesRegex(ValueError,'already attempted'):capture.capture(client,'bad-source',clock,fetch)
            self.assertEqual(len(calls),1)

    def test_transport_failure_journal_omits_exception_message_and_blocks_retry(self):
        client=S3({})
        def fetch(request):raise RuntimeError('UNSAFE_EXCEPTION_CONTENT')
        with self.assertRaises(RuntimeError):capture.capture(client,'transport',lambda:'2026-03-01T14:00:00Z',fetch)
        body=client.files[campaign.request_key('transport','sec-identity')]
        self.assertNotIn(b'UNSAFE_EXCEPTION_CONTENT',body)
        self.assertEqual(json.loads(body)['error_type'],'RuntimeError')


if __name__=='__main__':unittest.main(verbosity=2)
