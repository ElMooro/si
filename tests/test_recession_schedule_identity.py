from pathlib import Path
import sys, unittest
from copy import deepcopy
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
from ops_6191_recession_schedule_verified import configuration
class Tests(unittest.TestCase):
    def test_only_transport_metadata_can_differ(self):
        first={'Name':'native','State':'ENABLED','Target':{'Arn':'exact','Input':'preserved'},'ScheduleExpression':'cron(40 12 * * ? *)','ResponseMetadata':{'RequestId':'first'}}
        second=deepcopy(first);second['ResponseMetadata']={'RequestId':'second','HTTPHeaders':{'date':'later'}}
        self.assertEqual(configuration(first),configuration(second));self.assertEqual(first['ResponseMetadata']['RequestId'],'first')
        for field,value in [('State','DISABLED'),('Target',{'Arn':'other'}),('ScheduleExpression','rate(1 hour)'),('NewUnknownConfig',False)]:
            changed=deepcopy(second);changed[field]=value
            self.assertNotEqual(configuration(first),configuration(changed))
if __name__=='__main__':unittest.main()
