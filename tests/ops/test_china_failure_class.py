from pathlib import Path
from unittest.mock import Mock
import sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6296_china_failure_class as op


class Tests(unittest.TestCase):
    def test_only_safe_exception_class_from_fixed_producer_window(self):
        logs=Mock();event={'timestamp':int(op.scoped.START.timestamp()*1000)+1000,'message':op.PREFIX+' EvidenceError\n'}
        logs.get_paginator.return_value.paginate.return_value=[{'events':[event]}]
        self.assertEqual(op.failure_classes(logs),[{'timestamp_ms':event['timestamp'],'exception_class':'EvidenceError'}])
        args=logs.get_paginator.return_value.paginate.call_args.kwargs
        self.assertEqual(args['logGroupName'],'/aws/lambda/justhodl-china-liquidity')
        self.assertEqual(args['endTime']-args['startTime'],600000)
        self.assertEqual(args['filterPattern'],'"'+op.PREFIX+'"')
        for value in [op.PREFIX+' EvidenceError private-body',op.PREFIX+' https://example.com', 'other '+op.PREFIX+' Error',op.PREFIX+' Error\ntraceback']:
            event['message']=value
            with self.assertRaises(ValueError):op.failure_classes(logs)
        event.update(message=op.PREFIX+' EvidenceError',timestamp=0)
        with self.assertRaises(ValueError):op.failure_classes(logs)


if __name__=='__main__':unittest.main(verbosity=2)
