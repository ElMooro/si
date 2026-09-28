from pathlib import Path
from unittest.mock import Mock
from io import BytesIO
import sys, unittest

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/ops/staged'))
import ops_6294_china_execution_diagnostic as op


class Tests(unittest.TestCase):
    def test_fixed_producer_reports_omit_application_bodies(self):
        logs = Mock()
        event = {'timestamp': int(op.START.timestamp()*1000)+1000,
                 'message': 'REPORT RequestId: 12345678-1234-1234-1234-123456789abc\tDuration: 95000.00 ms\tMemory Size: 256 MB\tMax Memory Used: 92 MB\tStatus: timeout\n'}
        logs.get_paginator.return_value.paginate.return_value = [{'events': [event]}]
        rows = op.system_reports(logs)
        self.assertEqual(rows[0]['duration_ms'], 95000)
        self.assertEqual(rows[0]['status'], 'timeout')
        self.assertNotIn('message', rows[0])
        args = logs.get_paginator.return_value.paginate.call_args.kwargs
        self.assertEqual(args['logGroupName'], '/aws/lambda/justhodl-china-liquidity')
        self.assertEqual(args['filterPattern'], '"REPORT RequestId:"')
        self.assertEqual(args['endTime']-args['startTime'], 600000)
        event['message'] = 'Application output REPORT RequestId: 12345678-1234-1234-1234-123456789abc'
        with self.assertRaises(ValueError): op.system_reports(logs)

    def fixture(self):
        s3 = Mock(); objects = {}; rows = []
        def put(raw):
            digest=op.store.sha(raw); key=op.store.PRIVATE+digest+'.bin'
            objects[key]=raw; rows.append({'Key':key,'LastModified':op.START,'Size':len(raw)})
            return {'key':key,'sha256':digest,'bytes':len(raw)}
        body = put(b'Complete public provider body with an unknown field')
        manifest = {'request':{'source_host':'api.stlouisfed.org','parameters':{'series_id':'FIXTURE','api_key':'must-not-be-output'}},
                    'requested_at':op.START.isoformat(),'status':'http_response','http_status':200,'original':body}
        ref = put(op.store.encode(manifest))
        s3.get_paginator.return_value.paginate.return_value = [{'Contents': rows}]
        s3.get_object.side_effect = lambda **kw: {'Body':BytesIO(objects[kw['Key']]),'ContentLength':len(objects[kw['Key']]),'ETag':'"fixed"'}
        return s3,objects,rows,manifest,ref

    def test_whole_own_sources_verify_without_emitting_bodies_or_credentials(self):
        s3,objects,rows,manifest,ref=self.fixture()
        out=op.own_source_attempts(s3)
        self.assertEqual(out['objects_checked'],2)
        self.assertEqual(len(out['attempts']),1)
        self.assertEqual(out['attempts'][0]['original_sha256'],manifest['original']['sha256'])
        self.assertEqual(out['attempts'][0]['series_id'],'FIXTURE')
        self.assertNotIn('must-not-be-output',str(out))
        self.assertNotIn('unknown field',str(out))
        self.assertEqual(s3.get_paginator.return_value.paginate.call_args.kwargs,{'Bucket':op.BUCKET,'Prefix':op.store.PRIVATE})
        objects[manifest['original']['key']]=b'tampered'
        with self.assertRaises(ValueError):op.own_source_attempts(s3)

    def test_namespace_and_complete_population_bounds_are_not_bypassed(self):
        s3,objects,rows,_,_=self.fixture()
        rows[0]['Key']='portfolio/private.json'
        with self.assertRaises(ValueError):op.own_source_attempts(s3)
        self.assertFalse(s3.get_object.called)
        s3,objects,rows,_,_=self.fixture();rows.extend([dict(rows[0]) for _ in range(160)])
        with self.assertRaises(ValueError):op.own_source_attempts(s3)
        self.assertFalse(s3.get_object.called)


if __name__ == '__main__':unittest.main(verbosity=2)
