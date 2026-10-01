"""Invented responses only; no SDK/client construction or network access."""
import ast
from contextlib import contextmanager
from types import SimpleNamespace
import copy
from datetime import datetime, timedelta, timezone
import importlib.util
import json
import os
from pathlib import Path
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
FILE = ROOT / 'aws/ops/staged/ops_6411_industry_publication_readonly.py'
spec = importlib.util.spec_from_file_location('industry_probe', FILE)
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
NOW = datetime(2026, 10, 1, 13, 6, 27, tzinfo=timezone.utc)
SECRET = 'PRIVATE_CANARY_NEVER_REPORT'


class Fake:
    def __init__(self):
        self.calls = []
        self.overrides = {}

    def __getattr__(self, op):
        if op not in {op for _, op in m.ALLOWED}:
            raise AssertionError('Disallowed API: ' + op)
        def call(**kw):
            self.calls.append((op, copy.deepcopy(kw)))
            if op in self.overrides:
                v = self.overrides[op]
                if isinstance(v, Exception):
                    raise v
                return copy.deepcopy(v(kw) if callable(v) else v)
            defaults = {
                'head_object': {'LastModified': m.START, 'ContentLength': 42, 'ETag': '"'+'a'*32+'"', 'Metadata': {'private': SECRET}},
                'get_metric_data': {'MetricDataResults': [{'Id': k, 'StatusCode': 'Complete', 'Timestamps': [m.START], 'Values': [0], 'Label': SECRET} for k in ('invocations','errors','throttles','duration')]},
                'list_rule_names_by_target': {'RuleNames': ['industry-test']},
                'describe_rule': {'Name': 'industry-test', 'State': 'ENABLED', 'ScheduleExpression': 'cron(0 13 ? * MON-FRI *)', 'Description': SECRET, 'RoleArn': SECRET},
                'list_targets_by_rule': {'Targets': [{'Arn': m.ARN, 'Input': SECRET, 'RoleArn': SECRET}]},
                'list_schedules': {'Schedules': [{'Name': 'industry-test', 'GroupName': 'default', 'Target': {'Arn': m.ARN, 'Input': SECRET}}]},
                'get_schedule': {'Name': 'industry-test', 'GroupName': 'default', 'Target': {'Arn': m.ARN, 'Input': SECRET, 'RoleArn': SECRET}, 'State': 'ENABLED', 'ScheduleExpression': 'cron(0 13 ? * MON-FRI *)', 'ScheduleExpressionTimezone': 'UTC', 'Description': SECRET},
            }
            return copy.deepcopy(defaults[op])
        return call


def run(fake=None, timer=lambda: 0):
    f = fake or Fake()
    api = m.Reads({k: f for k in ('s3','cloudwatch','events','scheduler')}, NOW, timer)
    return m.observe(api), f


class ProbeTests(unittest.TestCase):
    def test_allowlisted_reads_and_output_privacy(self):
        r, f = run()
        self.assertNotIn(SECRET, json.dumps(r))
        self.assertTrue(r['bounded_reads_complete'])
        self.assertFalse(r['inventory_complete'])
        self.assertEqual(r['api_calls'], 9)
        self.assertEqual(f.calls[0], ('head_object', {'Bucket':'justhodl-dashboard-live','Key':'data/industry-case.json'}))
        self.assertEqual({k for k,_ in f.calls}, {op for _,op in m.ALLOWED})
        for op, args in f.calls:
            self.assertNotIn('Input', args)
            if op == 'list_rule_names_by_target': self.assertIn(args['TargetArn'], m.TARGETS)
        self.assertEqual(r['native_invocations'], 0)

    def test_metric_window_is_completed_minute_and_exact_function(self):
        r,f = run()
        args = next(v for k,v in f.calls if k=='get_metric_data')
        self.assertEqual(args['StartTime'], m.START)
        self.assertEqual(args['EndTime'], NOW.replace(second=0))
        self.assertEqual(args['MaxDatapoints'],3000)
        for q in args['MetricDataQueries']:
            self.assertEqual(q['MetricStat']['Metric']['Dimensions'], [{'Name':'FunctionName','Value':m.FUNCTION}])
            self.assertEqual(q['MetricStat']['Period'],60)
        self.assertEqual(r['metrics']['series']['invocations']['observed_value'],0)
        self.assertFalse(r['metrics']['series']['invocations']['all_minutes_reported'])

    def test_absent_metrics_stay_unknown(self):
        for rows in ([], [{'Id':'invocations','StatusCode':'Complete','Values':[],'Timestamps':[]}],
                     [{'Id':k,'StatusCode':'Complete','Values':[],'Timestamps':[]} for k in ('invocations','errors','throttles','duration')]):
            f=Fake();f.overrides['get_metric_data']={'MetricDataResults':rows}
            r,_=run(f)
            self.assertIsNone(r['metrics']['series']['invocations']['observed_value'])
            self.assertFalse(r['bounded_reads_complete'])

    def test_api_errors_do_not_become_zero_or_absence_and_do_not_leak(self):
        for op in {op for _,op in m.ALLOWED}:
            f=Fake();f.overrides[op]=RuntimeError(SECRET)
            r,_=run(f)
            self.assertNotIn(SECRET,json.dumps(r))
            self.assertFalse(r['bounded_reads_complete'],op)
            self.assertLessEqual(r['api_calls'],m.MAX_CALLS)
            if op=='head_object': self.assertEqual(r['output_metadata']['status'],'unavailable')
            if op=='get_metric_data': self.assertNotIn('series',r['metrics'])

    def test_rule_pagination_and_detail_caps(self):
        f=Fake();counts={}
        def rows(kw):
            a=kw['TargetArn'];counts[a]=counts.get(a,0)+1
            return {'RuleNames':['rule-'+str(i) for i in range(100)],'NextToken':str(counts[a])}
        f.overrides['list_rule_names_by_target']=rows
        f.overrides['describe_rule']=lambda kw:{'Name':kw['Name'],'State':'ENABLED','ScheduleExpression':'rate(1 day)'}
        r,_=run(f)
        self.assertEqual(list(counts.values()),[2,2,2])
        self.assertEqual(len(r['rules']['rules']),5)
        self.assertIn('pagination_limit',r['rules']['issues'])
        self.assertIn('detail_limit',r['rules']['issues'])

    def test_full_worst_case_is_33_calls(self):
        f=Fake(); counter={'r':0,'s':0}
        def rules(kw):
            counter['r']+=1
            return {'RuleNames':['r'+str(i) for i in range(6)],'NextToken':str(counter['r'])}
        def schedules(kw):
            counter['s']+=1
            return {'Schedules':[{'Name':'s'+str(i),'GroupName':'default','Target':{'Arn':m.ARN}} for i in range(6)],'NextToken':str(counter['s'])}
        f.overrides.update(list_rule_names_by_target=rules,list_schedules=schedules,
            describe_rule=lambda kw:{'Name':kw['Name'],'State':'ENABLED','ScheduleExpression':'rate(1 day)'},
            get_schedule=lambda kw:{'Name':kw['Name'],'GroupName':kw['GroupName'],'State':'ENABLED','ScheduleExpression':'rate(1 day)','ScheduleExpressionTimezone':'UTC','Target':{'Arn':m.ARN}})
        r,_=run(f)
        self.assertEqual(r['api_calls'],33)
        self.assertEqual(counter,{'r':6,'s':10})
        self.assertFalse(r['bounded_reads_complete'])

    def test_repeated_token_stops_without_printing_token(self):
        f=Fake();f.overrides['list_schedules']={'Schedules':[],'NextToken':SECRET}
        r,_=run(f)
        self.assertEqual(sum(k=='list_schedules' for k,_ in f.calls),2)
        self.assertIn('invalid_response',r['schedules']['issues'])
        self.assertNotIn(SECRET,json.dumps(r))

    def test_target_page_limit_and_target_race(self):
        for response in ({'Targets':[]},{'Targets':[{'Arn':m.ARN}],'NextToken':SECRET}):
            f=Fake();f.overrides['list_targets_by_rule']=response
            r,_=run(f)
            self.assertTrue(r['rules']['bounded_scope_incomplete'])
            self.assertFalse(r['bounded_reads_complete'])
        f=Fake();f.overrides['get_schedule']={'Name':'industry-test','GroupName':'default','Target':{'Arn':'unrelated'}}
        r,_=run(f);self.assertIn('invalid_response',r['schedules']['issues'])

    def test_partial_metrics_and_bad_numbers_are_not_success(self):
        for extra in ({'NextToken':SECRET},{'Messages':[{'Value':SECRET}]}):
            f=Fake();base=f.get_metric_data();base.update(extra);f.calls=[];f.overrides['get_metric_data']=base
            r,_=run(f);self.assertFalse(r['bounded_reads_complete']);self.assertNotIn(SECRET,json.dumps(r))
        for value in (True,None,-1,float('nan'),float('inf'),'0'):
            f=Fake();f.overrides['get_metric_data']={'MetricDataResults':[{'Id':'invocations','Values':[value],'Timestamps':[m.START],'StatusCode':'Complete'}]}
            r,_=run(f);self.assertEqual(r['metrics']['status'],'unavailable')

    def test_bad_timestamps_and_duplicate_results(self):
        for stamps in ([NOW],[m.START,m.START]):
            f=Fake();f.overrides['get_metric_data']={'MetricDataResults':[{'Id':'invocations','Values':[1]*len(stamps),'Timestamps':stamps,'StatusCode':'Complete'}]}
            r,_=run(f);self.assertEqual(r['metrics']['status'],'unavailable')
        f=Fake();row={'Id':'invocations','Values':[],'Timestamps':[],'StatusCode':'Complete'}
        f.overrides['get_metric_data']={'MetricDataResults':[row,row]}
        r,_=run(f);self.assertEqual(r['metrics']['status'],'unavailable')

    def test_deadline_and_global_cap_stop_before_sdk_call(self):
        f=Fake();api=m.Reads({'s3':f},NOW,timer=lambda:0);api.deadline=0
        with self.assertRaises(m.Unavailable):api.call('s3','head_object')
        self.assertEqual(f.calls,[])
        api.deadline=100;api.calls=m.MAX_CALLS
        with self.assertRaises(m.Unavailable):api.call('s3','head_object')
        self.assertEqual(f.calls,[])
        with self.assertRaises(m.Unavailable):api.call('lambda','invoke')

    def test_fixed_day_guard_and_no_local_main(self):
        for t in (m.START,NOW+timedelta(days=1),NOW-timedelta(days=1)):
            with self.assertRaises(m.Unavailable):m.Reads({},t)
        with patch.dict(os.environ,{},clear=True):
            with self.assertRaisesRegex(SystemExit,'reviewed_direct_runner_only'):m.main()

    def test_malformed_pages_do_not_prove_missing_binding(self):
        for op, reply in [('list_schedules',{'Schedules':[None]}),('list_rule_names_by_target',{'RuleNames':[None]}),('head_object',{})]:
            f=Fake();f.overrides[op]=reply;r,_=run(f)
            self.assertFalse(r['bounded_reads_complete'])
            self.assertFalse(r['inventory_complete'])

    def test_empty_scoped_inventory_never_claims_global_absence(self):
        f=Fake();f.overrides.update(list_schedules={'Schedules':[]},list_rule_names_by_target={'RuleNames':[]})
        r,_=run(f)
        self.assertFalse(r['rules']['inventory_complete']);self.assertFalse(r['schedules']['inventory_complete'])
        self.assertIn('Other qualifiers',r['rules']['scope'])
        self.assertIn('indirect',r['schedules']['scope'])

    def test_runner_report_sanitizes_failures_and_disables_retries(self):
        f=Fake();f.overrides['head_object']=RuntimeError(SECRET)
        recorded=[];configs=[]
        class Report:
            def kv(self,**kw):recorded.append(kw)
            def fail(self,msg):recorded.append({'failure':msg})
        @contextmanager
        def report(_):yield Report()
        class Clock:
            @staticmethod
            def now(_):return NOW
        modules={'boto3':SimpleNamespace(client=lambda *a,**k:f),
                 'botocore.config':SimpleNamespace(Config=lambda **kw:configs.append(kw)),
                 'ops_report':SimpleNamespace(report=report)}
        env={'GITHUB_ACTIONS':'true','GITHUB_REPOSITORY':'ElMooro/si','GITHUB_EVENT_NAME':'workflow_dispatch',
             'GITHUB_WORKFLOW':'Run ops script (direct)','GITHUB_SHA':'a'*40}
        with patch.dict(os.environ,env,clear=True),patch.dict('sys.modules',modules),patch.object(m,'datetime',Clock):
            # Preserve clock()'s datetime isinstance check while pinning only Reads' supplied time.
            original=m.Reads
            with patch.object(m,'Reads',side_effect=lambda clients,now: original(clients,NOW)),patch.object(m,'clock',side_effect=lambda v:v):
                with self.assertRaises(SystemExit) as e:m.main()
        self.assertEqual(e.exception.code,1)
        self.assertNotIn(SECRET,json.dumps(recorded))
        self.assertEqual(configs,[{'connect_timeout':3,'read_timeout':5,'retries':{'total_max_attempts':1}}])
        self.assertEqual(recorded[0]['probe_commit'],'a'*40)
        self.assertEqual(len(recorded[0]['probe_source_sha256']),64)

    def test_static_import_and_sdk_surface(self):
        tree=ast.parse(FILE.read_text(encoding='utf-8'))
        for node in ast.walk(tree):
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):
                self.assertNotIn(node.func.attr,{'invoke','put_object','put_rule','update_schedule','create_schedule','get_object','filter_log_events'})
        self.assertEqual(len(m.ALLOWED),7)


if __name__=='__main__':unittest.main()
