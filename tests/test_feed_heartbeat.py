from pathlib import Path
from datetime import datetime,timezone,timedelta
from unittest.mock import Mock,patch
import ast,copy,hashlib,json,math,sys,time,types,unittest
R=Path(__file__).resolve().parents[1];NOW=datetime(2020,1,10,tzinfo=timezone.utc)
class ClockMeta(type):
 def __instancecheck__(cls,value):return isinstance(value,datetime)
class Clock(datetime,metaclass=ClockMeta):
 @classmethod
 def now(cls,tz=None):return NOW
class Error(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}
class Always:
 def check(self):pass
def load(source_path=None):
 m=types.ModuleType('heartbeat');boto=types.ModuleType('boto3');boto.client=Mock(return_value=Mock());config=types.ModuleType('botocore.config');config.Config=Mock(side_effect=lambda **kw:kw)
 with patch.dict(sys.modules,{'boto3':boto,'botocore':types.ModuleType('botocore'),'botocore.config':config}):
  exec(compile((source_path or R/'aws/lambdas/justhodl-feed-heartbeat/source/lambda_function.py').read_bytes(),'actual-heartbeat','exec'),m.__dict__)
 m.datetime=Clock;m.BUCKET='invented';m.s3=Mock();m.scheduler=Mock();m.events=Mock();m.SCHEDULE_BINDINGS=[];m.FEED_SOURCE_BINDINGS={};m.creation_clients=boto.client;m.creation_config=config.Config
 return m

def page(rows,more=False,token=None):
 return {'Name':'invented','Prefix':'data/x/','KeyCount':len(rows),'Contents':rows,'IsTruncated':more,**({'NextContinuationToken':token} if token else {})}
def row(name='a',age=1,size=123):return {'Key':'data/x/'+name,'LastModified':NOW-timedelta(days=age),'Size':size}
def binding(kind='EventBridge rule'):
 return {'kind':kind,'name':'reviewed-name','group':'default','function':'invented','target_arn':'arn:invented:exact','expression':'rate(1 hour)','timezone':'UTC'}
class Storage(unittest.TestCase):
 def test_complete_prefix_uses_every_page_and_preserves_each_member(self):
  m=load();m.s3.list_objects_v2.side_effect=[page([row('a',30)],True,'next'),page([row('z',1)])]
  result=m.storage_assessment(m.check_artifact('data/x/',True,Always()),10080)
  self.assertTrue(result['inventory_complete']);self.assertEqual(result['status'],'STALE_STORAGE');self.assertEqual(result['object_count'],2);self.assertEqual(result['recent_storage_objects'],1);self.assertEqual(result['stale_storage_objects'],1);self.assertEqual(result['age_minutes'],1440);self.assertEqual(result['oldest_age_minutes'],43200);self.assertEqual(result['size_bytes'],246)
  self.assertEqual(m.s3.list_objects_v2.call_args.kwargs['ContinuationToken'],'next');self.assertEqual(len(result['inventory']),2)
 def test_weekly_and_quarterly_storage_use_declared_intervals(self):
  for days,expected in [(3,10080),(30,90*1440)]:
   m=load();m.s3.head_object.return_value={'LastModified':NOW-timedelta(days=days),'ContentLength':0};out=m.storage_assessment(m.check_artifact('data/a',False,Always()),expected)
   self.assertEqual(out['status'],'RECENT_STORAGE');self.assertFalse(out['observation_freshness_verified']);self.assertEqual(out['size_bytes'],0)
 def test_denial_and_timeout_do_not_mean_missing(self):
  for exc in [Error('AccessDenied'),TimeoutError(),PermissionError(),Error('NoSuchBucket')]:
   m=load();m.s3.head_object.side_effect=exc;out=m.storage_assessment(m.check_artifact('data/a',False,Always()),60)
   self.assertEqual(out['status'],'UNKNOWN');self.assertIsNone(out['exists']);self.assertIsNone(out['size_bytes']);self.assertIsNone(out['object_count'])
 def test_named_head_not_found_and_complete_empty_prefix_are_absent(self):
  m=load();m.s3.head_object.side_effect=Error('404');m.s3.list_objects_v2.return_value=page([])
  for prefix in [False,True]:
   out=m.storage_assessment(m.check_artifact('data/x/',prefix,Always()),60);self.assertEqual(out['status'],'MISSING');self.assertEqual(out['object_count'],0)
 def test_future_clock_is_invalid_not_negative_age_fresh(self):
  m=load();m.s3.head_object.return_value={'LastModified':NOW+timedelta(seconds=1),'ContentLength':12};out=m.storage_assessment(m.check_artifact('data/a',False,Always()),60)
  self.assertEqual(out['status'],'INVALID_METADATA');self.assertIsNone(out['age_minutes'])
 def test_missing_boolean_negative_size_or_naive_time_is_unknown(self):
  for changes in [{'ContentLength':None},{'ContentLength':True},{'ContentLength':-1},{'LastModified':NOW.replace(tzinfo=None)},{'LastModified':None}]:
   m=load();m.s3.head_object.return_value={'LastModified':NOW,'ContentLength':12,**changes};out=m.storage_assessment(m.check_artifact('data/a',False,Always()),60);self.assertEqual(out['status'],'UNKNOWN')
 def test_partial_later_page_failure_keeps_captured_rows_without_population_claim(self):
  m=load();m.s3.list_objects_v2.side_effect=[page([row()],True,'next'),Error('AccessDenied')];out=m.storage_assessment(m.check_artifact('data/x/',True,Always()),60)
  self.assertEqual(out['status'],'UNKNOWN');self.assertEqual(len(out['inventory']),1);self.assertFalse(out['inventory_complete']);self.assertIsNone(out['recent_storage_objects']);self.assertIsNone(out['last_modified'])
 def test_malformed_duplicated_foreign_or_cyclic_listing_is_unknown(self):
  bad=[{**page([]),'Name':'other'},{**page([]),'KeyCount':True},{**page([]),'KeyCount':1},page([row(),row()]),page([{**row(),'Key':'private/a'}]),page([],True)]
  for value in bad:
   m=load();m.s3.list_objects_v2.return_value=value;out=m.check_artifact('data/x/',True,Always());self.assertFalse(out['inventory_complete']);self.assertIsNone(out['exists'])
  m=load();m.s3.list_objects_v2.side_effect=[page([row('a')],True,'same'),page([row('b')],True,'same')];self.assertFalse(m.check_artifact('data/x/',True,Always())['inventory_complete'])
 def test_budget_refusal_stops_before_metadata_request(self):
  m=load();ctx=Mock();ctx.get_remaining_time_in_millis.return_value=500;out=m.check_artifact('data/a',False,m.Budget(ctx));self.assertEqual(out['reason'],'time_budget_exhausted');m.s3.head_object.assert_not_called()
 def test_boolean_or_nonfinite_remaining_budget_is_rejected(self):
  for value in [True,None,float('nan'),float('inf')]:
   m=load();ctx=Mock();ctx.get_remaining_time_in_millis.return_value=value
   with self.assertRaises(m.BudgetExhausted):m.Budget(ctx).check()
 def test_exact_storage_threshold_is_inclusive_without_rounding_decision(self):
  m=load()
  for seconds,status in [(7200,'RECENT_STORAGE'),(7200.001,'STALE_STORAGE')]:
   m.s3.head_object.return_value={'LastModified':NOW-timedelta(seconds=seconds),'ContentLength':1};self.assertEqual(m.storage_assessment(m.check_artifact('x',False,Always()),60)['status'],status)
 def test_malformed_exception_metadata_cannot_crash_failure_handler(self):
  for response in [None,[],{'Error':None},{'Error':{'Code':True}}]:
   m=load();exc=Error('unused');exc.response=response;m.s3.head_object.side_effect=exc;out=m.check_artifact('x',False,Always());self.assertIsNone(out['exists']);self.assertEqual(out['reason'],'metadata_request_failed')
 def test_reviewed_population_bound_never_accepts_truncated_population(self):
  rows=[row(str(i)) for i in range(50001)]
  for total in (50000,50001):
   m=load();m.s3.list_objects_v2.side_effect=[page(rows[start:min(start+1000,total)],start+1000<total,'next-'+str(start)) for start in range(0,total,1000)];out=m.check_artifact('data/x/',True,Always());self.assertEqual(out['inventory_complete'],total==50000);self.assertEqual(len(out['inventory']),total)
   if total==50001:self.assertEqual(out['reason'],'incomplete_inventory');self.assertIsNone(out['exists'])

class Schedules(unittest.TestCase):
 def fixture(self,kind='EventBridge rule',source_path=None):
  m=load(source_path);m.SCHEDULE_BINDINGS=[binding(kind)];m.events.describe_rule.return_value={'Name':'reviewed-name','State':'ENABLED','ScheduleExpression':'rate(1 hour)'};m.events.list_targets_by_rule.return_value={'Targets':[{'Id':'one','Arn':'arn:invented:exact'}]};m.scheduler.get_schedule.return_value={'Name':'reviewed-name','GroupName':'default','State':'ENABLED','ScheduleExpression':'rate(1 hour)','ScheduleExpressionTimezone':'UTC','Target':{'Arn':'arn:invented:exact'}};return m
 def test_classic_and_scheduler_verified_by_real_target_not_name_guess(self):
  for kind in ['EventBridge rule','EventBridge Scheduler']:
   m=self.fixture(kind);out=m.check_schedules(Always());self.assertEqual(out['status'],'BINDINGS_VERIFIED');self.assertFalse(out['delivery_verified'])
 def test_access_denial_is_unknown_not_missing_or_healthy(self):
  m=self.fixture();m.events.describe_rule.side_effect=Error('AccessDeniedException');out=m.check_schedules(Always());self.assertEqual(out['status'],'UNKNOWN');self.assertEqual(out['missing'],[]);self.assertEqual(out['unknown'],['reviewed-name']);self.assertFalse(out['healthy'])
 def test_wrong_target_disabled_or_drift_is_explicit(self):
  for changes,status in [({'target':'arn:invented:other'},'MISBOUND'),({'state':'DISABLED'},'DISABLED'),({'expression':'rate(2 hours)'},'DRIFT')]:
   m=self.fixture();m.events.list_targets_by_rule.return_value={'Targets':[{'Id':'one','Arn':changes.get('target','arn:invented:exact')}]};m.events.describe_rule.return_value.update(State=changes.get('state','ENABLED'),ScheduleExpression=changes.get('expression','rate(1 hour)'));out=m.check_schedules(Always());self.assertEqual(out['status'],'CRITICAL');self.assertEqual(out['checks'][0]['status'],status)
 def test_later_target_page_and_duplicate_metadata(self):
  m=self.fixture();m.events.list_targets_by_rule.side_effect=[{'Targets':[{'Id':'other','Arn':'arn:other'}],'NextToken':'next'},{'Targets':[{'Id':'one','Arn':'arn:invented:exact'}]}];self.assertTrue(m.check_schedules(Always())['healthy']);self.assertEqual(m.events.list_targets_by_rule.call_args.kwargs['NextToken'],'next')
  m=self.fixture();m.events.list_targets_by_rule.return_value={'Targets':[{'Id':'one','Arn':'arn:invented:exact'}]*2};self.assertEqual(m.check_schedules(Always())['status'],'UNKNOWN')
 def test_named_notfound_is_distinct_from_unsupported_empty_scope(self):
  m=self.fixture();m.events.describe_rule.side_effect=Error('ResourceNotFoundException');self.assertEqual(m.check_schedules(Always())['missing'],['reviewed-name']);m.SCHEDULE_BINDINGS=[];self.assertEqual(m.check_schedules(Always())['status'],'UNKNOWN')
 def test_unreviewed_service_cannot_fall_through_to_classic(self):
  m=self.fixture('invented service');self.assertEqual(m.check_schedules(Always())['status'],'UNKNOWN');m.events.describe_rule.assert_not_called();m.scheduler.get_schedule.assert_not_called()

class Handler(unittest.TestCase):
 def test_binding_count_uses_individual_bindings_not_aggregate_section(self):
  for source,expected in [(R/'tests/fixtures/feed-heartbeat/counter-draft-before.py.txt',1),(None,2)]:
   m=Schedules().fixture(source_path=source);m.SCHEDULE_BINDINGS.append(binding('EventBridge Scheduler'));m.FEEDS=[('schedules',None,60,False)];m.lambda_handler({},None);packet=json.loads(m.s3.put_object.call_args.kwargs['Body'])
   self.assertEqual(len(packet['feeds']['schedules']['detail']['checks']),2);self.assertEqual(packet['n_bindings_verified'],expected);self.assertEqual(packet['n_feeds'],1)
   if source is None:self.assertEqual(packet['n_schedule_sections_verified'],1)
 def test_whole_handler_writes_only_original_key_and_denies_data_freshness(self):
  m=load();m.FEEDS=[('weekly','data/a',10080,False),('schedules',None,60,False)];m.s3.head_object.return_value={'LastModified':NOW-timedelta(days=3),'ContentLength':123};out=m.lambda_handler({},None);kw=m.s3.put_object.call_args.kwargs;p=json.loads(kw['Body']);self.assertEqual(kw['Key'],'data/feed-heartbeat.json');self.assertEqual(p['system_status'],'UNKNOWN');self.assertEqual(p['storage_monitor_status'],'UNKNOWN');self.assertEqual(p['n_recent_storage'],1);self.assertEqual(p['n_unknown'],1);self.assertIsNone(p['n_fresh']);self.assertIsNone(p['n_stale']);self.assertFalse(p['calls_eligible']);self.assertFalse(p['execution_eligible']);self.assertEqual(out['statusCode'],200);self.assertEqual(len(p['feeds']),2)
 def test_all_access_failures_never_become_zero_unknown_or_missing(self):
  m=load();m.FEEDS=[('a','data/a',60,False)];m.s3.head_object.side_effect=Error('AccessDenied');m.lambda_handler({},None);p=json.loads(m.s3.put_object.call_args.kwargs['Body']);self.assertEqual(p['n_missing'],0);self.assertEqual(p['n_unknown'],1);self.assertEqual(p['storage_monitor_status'],'UNKNOWN')
 def test_write_failure_is_not_success(self):
  m=load();m.FEEDS=[];m.s3.put_object.side_effect=PermissionError()
  with self.assertRaises(PermissionError):m.lambda_handler({},None)
 def test_storage_created_during_collection_uses_completion_clock(self):
  m=load();clock=Mock(side_effect=[NOW,NOW+timedelta(seconds=2),NOW+timedelta(seconds=3)])
  class Advancing(Clock):
   @classmethod
   def now(cls,tz=None):return clock()
  m.datetime=Advancing;m.FEEDS=[('a','data/a',60,False)];m.s3.head_object.return_value={'LastModified':NOW+timedelta(seconds=1),'ContentLength':1};m.lambda_handler({},None);p=json.loads(m.s3.put_object.call_args.kwargs['Body']);self.assertEqual(p['storage_monitor_status'],'HEALTHY');self.assertEqual(p['feeds']['a']['status'],'RECENT_STORAGE');self.assertEqual(p['generated_at'],(NOW+timedelta(seconds=3)).isoformat());self.assertEqual(p['acquisition_started_at'],NOW.isoformat())

class PreservedScope(unittest.TestCase):
 def test_complete_legacy_feed_declarations_remain_with_three_explicit_additions(self):
  def values(path,name):
   class Products(ast.NodeTransformer):
    def visit_BinOp(self,n):
     self.generic_visit(n)
     if isinstance(n.op,ast.Mult) and isinstance(n.left,ast.Constant) and isinstance(n.right,ast.Constant) and type(n.left.value) is int and type(n.right.value) is int:return ast.Constant(n.left.value*n.right.value)
     return n
   tree=Products().visit(ast.parse(path.read_bytes()));return next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id==name for t in n.targets))
  source=R/'aws/lambdas/justhodl-feed-heartbeat/source/lambda_function.py';prior=values(R/'tests/fixtures/feed-heartbeat/predecessor.py.txt','FEEDS');current=values(source,'FEEDS');self.assertEqual(current[:len(prior)],prior);self.assertEqual(len(current),21);self.assertEqual(len({r[0] for r in current}),21)
  expected={'finra-research':'data/short-interest.json','8k-enriched':'data/8k-filings-enriched.json','xbrl-index':'data/xbrl-fundamentals-index.json'};self.assertEqual({r[0]:r[1] for r in current[len(prior):]},expected)
  declarations=values(source,'FEED_SOURCE_BINDINGS');self.assertEqual(declarations['short-interest']['function'],'justhodl-short-book');self.assertEqual(declarations['sec-8k']['function'],'justhodl-sec-filings-intel')
 def test_clients_have_one_attempt_and_bounded_transport(self):
  m=load();m.creation_config.assert_called_once_with(connect_timeout=2,read_timeout=3,retries={'total_max_attempts':1});self.assertEqual([c.args[0] for c in m.creation_clients.call_args_list],['s3','events','scheduler'])
 def test_configuration_retains_actual_controls_and_does_not_provision_absent_trigger(self):
  prior=json.loads((R/'tests/fixtures/feed-heartbeat/config-predecessor.json.txt').read_bytes());current=json.loads((R/'aws/lambdas/justhodl-feed-heartbeat/config.json').read_bytes());self.assertEqual(current['retained_unbound_schedule_declaration']['expression'],prior['schedule']);self.assertIsNone(current['schedule'])
  for key in ('memory','timeout','runtime','handler','environment'):self.assertEqual(current[key],prior[key])

if __name__=='__main__':unittest.main(verbosity=2)
