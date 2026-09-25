from pathlib import Path
from unittest.mock import patch
import json, sys, unittest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]/p) for p in ('aws/shared','aws/ops/checks')]
import statement_source_refresh as refresh
import statement_research_source as source
import statement_producer as producer
import financial_statement_campaign as campaign
from test_statement_research_v2 import Fixture
from test_statement_producer import S3


class Tests(unittest.TestCase):
    def test_original_universe_requires_every_name_unique_and_retains_exact_bytes(self):
        labels=['TEST'+str(n) for n in range(500)]
        body=source.encoded({'stocks':[{'symbol':s,'sector':'Provider label'} for s in labels],'generated_at':'2026-03-01T00:00:00Z'})
        s3=S3({refresh.UNIVERSE:body})
        got,ref,capture=refresh.original_universe(s3)
        self.assertEqual(got,sorted(labels));self.assertEqual(s3.files[ref['key']],body);self.assertFalse(capture['index_membership_verified'])
        for rows in ([{'symbol':s} for s in labels[:-1]], [{'symbol':'SAME'}]*500, [{'symbol':None}]*500):
            s3=S3({refresh.UNIVERSE:source.encoded({'rows':rows})})
            with self.assertRaises(ValueError):refresh.original_universe(s3)
        s3=S3({refresh.UNIVERSE:source.encoded({'rows':[],'stocks':[]})})
        with self.assertRaises(ValueError):refresh.original_universe(s3)

    def setup_campaign(self):
        f=Fixture(('ABC','XYZ'));s3=S3(f.files);calls=[]
        def fetch(spec):
            calls.append(spec['url']);cap=f.capsules[spec['url']]
            return {**cap,'inventory':{'rows':1},'retained_capture':f.manifest['captures'][spec['url']]}
        universe=(['ABC','XYZ'],f.manifest['universe'],{'original':f.manifest['universe'],'captured_at':'2026-03-01T12:00:00Z'})
        return f,s3,calls,fetch,universe

    def test_complete_source_replay_advances_only_ready_and_repeated_request_makes_no_calls(self):
        f,s3,calls,fetch,universe=self.setup_campaign()
        with patch.object(refresh,'original_universe',return_value=universe):
            result=refresh.run(s3,'test-refresh','managed-test-placeholder',clock=lambda:'2026-03-01T13:00:00Z',fetch=fetch,fetch_identity=lambda:f.identity_ref)
            self.assertTrue(result['ready_advanced']);self.assertEqual(len(calls),12)
            again=refresh.run(s3,'test-refresh','managed-test-placeholder',clock=lambda:'2026-03-01T13:00:00Z',fetch=fetch,fetch_identity=lambda:f.identity_ref)
            self.assertEqual(again,result);self.assertEqual(len(calls),12)
        ready=json.loads(s3.files[producer.READY]);self.assertEqual(ready['qualification']['original_rows_checked'],12)
        self.assertEqual(ready['qualification']['metric_comparisons'],68);self.assertEqual(ready['producer_invocations'],0)
        self.assertNotIn(producer.CURRENT,s3.files)
        self.assertTrue(all(v['Key'].startswith((source.PRIVATE,'data/statement-research/')) for v in s3.writes))

    def test_failed_source_retains_successes_and_leaves_ready_unchanged(self):
        f,s3,calls,fetch,universe=self.setup_campaign();old=source.encoded({'generated_at':'2026-02-28T00:00:00Z','status':'qualified'})
        s3.files[producer.READY]=old
        def fail(spec):
            if 'balance-sheet' in spec['endpoint']:raise RuntimeError('source unavailable')
            return fetch(spec)
        with patch.object(refresh,'original_universe',return_value=universe):
            with self.assertRaises(ValueError):refresh.run(s3,'failed-refresh','managed-test-placeholder',fetch=fail,fetch_identity=lambda:f.identity_ref)
        self.assertEqual(s3.files[producer.READY],old)
        status=json.loads(s3.files[campaign.request_key('failed-refresh','refresh')]);self.assertEqual(status['status'],'failed')
        self.assertTrue(status['captures']);self.assertLess(len(calls),12)
        with self.assertRaisesRegex(ValueError,'already attempted'):refresh.run(s3,'failed-refresh','managed-test-placeholder',fetch=fetch,fetch_identity=lambda:f.identity_ref)

    def test_ready_cas_refuses_overwriting_a_concurrent_refresh(self):
        f,s3,calls,fetch,universe=self.setup_campaign();old=source.encoded({'generated_at':'2026-02-28T00:00:00Z','status':'qualified'})
        s3.files[producer.READY]=old;s3.race=True
        with patch.object(refresh,'original_universe',return_value=universe):
            result=refresh.run(s3,'race-refresh','managed-test-placeholder',clock=lambda:'2026-03-01T13:00:00Z',fetch=fetch,fetch_identity=lambda:f.identity_ref)
        self.assertFalse(result['ready_advanced']);self.assertEqual(result['reason'],'concurrent_ready_publication');self.assertEqual(s3.files[producer.READY],old)


if __name__=='__main__':unittest.main(verbosity=2)
