import json
import runpy
import sys
import types
import unittest
from pathlib import Path
from unittest.mock import patch

import scenario_publication as p
from scenario_test_support import S3, Conditional


class Publication(unittest.TestCase):
    def test_whole_prior_retention_replay_and_no_accounts(self):
        s3 = S3(); before = s3.docs[p.CURRENT]
        result = p.run(s3,'synthetic','2026-09-20T04:00:00Z')
        out = json.loads(s3.docs[p.CURRENT]); manifest = json.loads(s3.docs[result['replay']['key']])
        self.assertEqual(p.replay(manifest,lambda k:s3.docs[k]),{k:v for k,v in out.items() if k != 'replay'})
        self.assertEqual(s3.docs[p.PRIVATE+p.digest(before)+'.bin'],before)
        self.assertEqual(out['sized_positions'],[]);self.assertIsNone(out['suggested_gross_top15_pct'])
        self.assertTrue(all(v is False for v in out['permissions'].values()))
        self.assertFalse(any('brain' in k or 'best-setups' in k or 'portfolio/' in k for k in s3.reads))

    def test_repeated_schedule_does_not_refresh_model_clock(self):
        s3=S3();p.run(s3,'synthetic','2026-09-20T04:00:00Z');before=dict(s3.docs);writes=list(s3.writes)
        result=p.run(s3,'synthetic','2026-09-21T04:00:00Z')
        self.assertFalse(result['published']);self.assertEqual(s3.docs,before);self.assertEqual(s3.writes,writes)

    def test_concurrent_pointer_is_not_overwritten(self):
        s3=S3();before=s3.docs[p.CURRENT];s3.race=True
        with self.assertRaises(Conditional):p.run(s3,'synthetic')
        self.assertEqual(s3.docs[p.CURRENT],before)

    def test_corrupt_model_and_output_fail_without_pointer_write(self):
        for kind in ('model','output','current'):
            s3=S3();p.run(s3,'synthetic');out=json.loads(s3.docs[p.CURRENT])
            manifest=json.loads(s3.docs[out['replay']['key']])
            if kind=='model':s3.docs[out['scenario_model']['key']]+=b'changed'
            elif kind=='output':s3.docs[manifest['output']['key']]=b'{}'
            else:out['sized_positions']=[{'ticker':'FORGED','suggested_size_pct':6}];s3.docs[p.CURRENT]=p.encoded(out)
            writes=list(s3.writes)
            with self.assertRaises(ValueError):p.run(s3,'synthetic')
            self.assertEqual(s3.writes,writes)

    def test_unknown_predecessor_rejected_without_any_write(self):
        s3=S3();s3.docs[p.CURRENT]=b'{"engine":"unknown"}'
        with self.assertRaises(ValueError):p.run(s3,'synthetic')
        self.assertEqual(s3.writes,[])

    def test_http_read_does_not_publish(self):
        s3=S3()
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:s3)}):
            module=runpy.run_path(str(Path(p.__file__).with_name('lambda_function.py')))
        result=module['lambda_handler']({'httpMethod':'GET'})
        self.assertEqual(result['statusCode'],200);self.assertEqual(s3.writes,[])
        self.assertEqual(s3.reads,[p.CURRENT])

    def test_model_bundle_identical_to_page_source(self):
        root=Path(__file__).resolve().parents[4]
        self.assertEqual((root/'jh-portfolio-scenario.js').read_bytes(),Path(p.__file__).with_name('scenario_model.js').read_bytes())


if __name__=='__main__':unittest.main()
