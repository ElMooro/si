"""Invented source-only reproductions; does not contact any provider or AWS."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from unittest.mock import Mock,patch
import hashlib,json,sys,types,unittest
R=Path(__file__).resolve().parents[1];SOURCE=R/'tests/fixtures/feed-heartbeat/predecessor.py.txt';NOW=datetime(2020,1,10,tzinfo=timezone.utc)
class Clock(datetime):
 @classmethod
 def now(cls,tz=None):return NOW
def module():
 raw=SOURCE.read_bytes();fake=types.ModuleType('boto3');fake.client=Mock(return_value=Mock());m=types.ModuleType('invented_heartbeat')
 with patch.dict(sys.modules,{'boto3':fake}):exec(compile(raw,str(SOURCE),'exec'),m.__dict__)
 m.datetime=Clock;return m
class ExistingDefects(unittest.TestCase):
 def test_prefix_reads_first_lexicographic_member_without_complete_population(self):
  m=module();m.s3.list_objects_v2.return_value={'IsTruncated':True,'NextContinuationToken':'invented','Contents':[{'Key':'data/invented/a.json','LastModified':NOW-timedelta(days=30),'Size':10}]}
  result=m.check_artifact('data/invented/',True)
  self.assertEqual(m.s3.list_objects_v2.call_args.kwargs['MaxKeys'],1)
  self.assertEqual(result['_lm'],NOW-timedelta(days=30));self.assertNotIn('inventory_complete',result)
 def test_access_denial_is_misreported_as_absence(self):
  m=module();m.s3.head_object.side_effect=PermissionError('invented denied')
  self.assertEqual(m.check_artifact('data/invented.json',False),{'exists':False})
 def test_weekly_object_is_stale_after_three_days_despite_two_week_grace(self):
  m=module();m.FEEDS=[('invented-weekly','data/invented.json',10080,False)];m.s3.head_object.return_value={'LastModified':NOW-timedelta(days=3),'ContentLength':123}
  m.lambda_handler({},None);packet=json.loads(m.s3.put_object.call_args.kwargs['Body'])
  self.assertEqual(packet['feeds']['invented-weekly']['status'],'STALE');self.assertLess(packet['feeds']['invented-weekly']['age_minutes'],10080*2)
 def test_future_metadata_is_fresh_with_negative_age(self):
  m=module();m.FEEDS=[('invented','data/invented.json',60,False)];m.s3.head_object.return_value={'LastModified':NOW+timedelta(days=3),'ContentLength':123}
  m.lambda_handler({},None);packet=json.loads(m.s3.put_object.call_args.kwargs['Body'])
  self.assertEqual(packet['feeds']['invented']['status'],'FRESH');self.assertLess(packet['feeds']['invented']['age_minutes'],0)
 def test_scheduler_denial_is_reported_as_missing_and_no_classic_rules_are_checked(self):
  m=module();sched=Mock();sched.get_schedule.side_effect=PermissionError('invented denied');m.boto3.client.return_value=sched
  result=m.check_schedules();self.assertEqual(len(result['missing']),6);self.assertEqual(m.boto3.client.call_args.args,('scheduler',));self.assertNotIn('unknown',result)
if __name__=='__main__':unittest.main(verbosity=2)
