from pathlib import Path
from unittest.mock import Mock
import sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6293_term_premium_execution_diagnostic as op


class Tests(unittest.TestCase):
    def test_only_fixed_producer_system_reports_and_numeric_resources(self):
        logs=Mock();logs.get_paginator.return_value.paginate.return_value=[{'events':[{'timestamp':1000,
            'message':'REPORT RequestId: 12345678-1234-1234-1234-123456789abc\tDuration: 120000.00 ms\tBilled Duration: 120000 ms\tMemory Size: 512 MB\tMax Memory Used: 512 MB\tStatus: error\tError Type: Runtime.OutOfMemory\n'}]}]
        result=op.system_reports(logs,'2026-09-28T13:45:15.369949+00:00')
        self.assertEqual(result[0]['duration_ms'],120000);self.assertEqual(result[0]['max_memory_mb'],512)
        self.assertEqual(result[0]['error_type'],'Runtime.OutOfMemory');self.assertNotIn('message',result[0])
        args=logs.get_paginator.return_value.paginate.call_args.kwargs
        self.assertEqual(args['logGroupName'],'/aws/lambda/justhodl-term-premium');self.assertEqual(args['filterPattern'],'"REPORT RequestId:"')
        self.assertLessEqual(args['endTime']-args['startTime'],600000)

    def test_unrelated_application_body_is_never_returned(self):
        logs=Mock();logs.get_paginator.return_value.paginate.return_value=[{'events':[{'timestamp':1000,'message':'application record'}]}]
        with self.assertRaises(ValueError):op.system_reports(logs,'2026-09-28T13:45:15.369949+00:00')


if __name__=='__main__':unittest.main(verbosity=2)
