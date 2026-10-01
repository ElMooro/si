"""Invented-only read-policy, privacy, bounds and receipt checks; no AWS clients."""
import ast
import base64
import copy
from datetime import datetime, timedelta, timezone
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import tempfile
import unittest
from unittest import mock
from contextlib import redirect_stdout

ROOT = Path(__file__).resolve().parents[2]
SCRIPT = ROOT / 'aws/ops/staged/ops_6410_etf_desk_capacity_readonly.py'
spec = importlib.util.spec_from_file_location('desk_capacity_probe', SCRIPT)
m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
AT = datetime(2026, 10, 1, 5, 0, 0, tzinfo=timezone.utc)
CODE = base64.b64encode(b's' * 32).decode()
SECRET = 'DO_NOT_EMIT_PRIVATE_CANARY'
REQUEST = '12345678-1234-1234-1234-123456789abc'
LINE = 'REPORT RequestId: '+REQUEST+'\tDuration: 159623.16 ms\tBilled Duration: 160023 ms\tMemory Size: 4096 MB\tMax Memory Used: 942 MB\tInit Duration: 399.84 ms\n'


class Error(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code, 'Message': SECRET}, 'ResponseMetadata': {'RequestId': SECRET}}
        super().__init__(SECRET)


class Body(io.BytesIO):
    def read(self, size=-1):
        assert 0 < size <= m.MAX_RECEIPT_BYTES + 1
        return super().read(size)


class Fake:
    def __init__(self):
        self.calls = []; self.bodies = []; self.config_calls = 0
        self.cfg = {'FunctionName': m.FUNCTION, 'FunctionArn': m.FUNCTION_ARN, 'State': 'Active', 'LastUpdateStatus': 'Successful',
            'CodeSha256': CODE, 'Timeout': 900, 'MemorySize': 4096,
            'LastModified': '2026-10-01T03:55:00.000+0000',
            'Environment': {'Variables': {'PRIVATE_VALUE': SECRET}}, 'LastUpdateStatusReason': SECRET}
        self.receipt = {'schema': 'release-receipt.v1', 'function': m.FUNCTION,
            'commit': m.EXPECTED_COMMIT, 'verified': True, 'code_sha256': CODE,
            'zip_sha256_hex': (b's'*32).hex(), 'zip_bytes': 123456,
            'deployed_at': '2026-10-01T03:56:00Z', 'actor': SECRET, 'run_id': SECRET,
            'source': {name: {'bytes': 100, 'sha256': sha, 'ignored': SECRET} for name,sha in m.EXPECTED_SOURCES.items()},
            'unexpected_payload': {'private': SECRET}}
        self.metric_points = None
        self.log_pages = [{'events': [{'timestamp': int((AT-timedelta(hours=6)).timestamp()*1000),
                            'message': LINE, 'eventId': SECRET, 'logStreamName': SECRET}], 'searchedLogStreams': [SECRET]}]
        self.config_after = None; self.error = None; self.override_raw = None; self.get_extra = b''
        self.metadata_override = {}; self.after_call = None

    def __getattr__(self, name):
        raise AssertionError('Unexpected API including mutation: '+name)

    def record(self, operation, kw):
        self.calls.append((operation, copy.deepcopy(kw)))
        if self.error == operation:
            raise Error('AccessDeniedException')
        if self.after_call: self.after_call()

    def get_function_configuration(self, **kw):
        self.record('get_function_configuration', kw); self.config_calls += 1
        return copy.deepcopy(self.config_after if self.config_calls == 2 and self.config_after else self.cfg)

    def content(self):
        return self.override_raw if self.override_raw is not None else json.dumps(self.receipt).encode()

    def head_object(self, **kw):
        self.record('head_object', kw)
        return {'ContentLength': len(self.content()) if kw['Key'] == m.RECEIPT else 5055894,
                'ETag': '"'+'a'*32+'"', 'LastModified': AT-timedelta(hours=1),
                'Metadata': {'private': SECRET}, **self.metadata_override}

    def get_object(self, **kw):
        self.record('get_object', kw); body = Body(self.content()+self.get_extra); self.bodies.append(body)
        return {'Body': body, 'ContentLength': len(self.content()), 'ETag': '"'+'a'*32+'"', 'Metadata': {'private': SECRET}}

    def get_metric_statistics(self, **kw):
        self.record('get_metric_statistics', kw)
        point = {'Timestamp': AT-timedelta(hours=6), 'Unit': kw['Unit'], 'unknown': SECRET}
        if kw['MetricName'] == 'Duration': point.update(Average=159623.16, Maximum=159623.16, SampleCount=1)
        else: point['Sum'] = 1 if kw['MetricName'] == 'Invocations' else 0
        return {'Label': SECRET, 'Datapoints': copy.deepcopy(self.metric_points) if self.metric_points is not None else [point]}

    def filter_log_events(self, **kw):
        self.record('filter_log_events', kw)
        index = sum(name == 'filter_log_events' for name,_ in self.calls)-1
        return copy.deepcopy(self.log_pages[index])


def reader(fake, timer=lambda: 0):
    return m.Reads(dict.fromkeys(('lambda','s3','cloudwatch','logs'), fake), AT, timer)


class Probe(unittest.TestCase):
    def test_exact_scope_safe_projection_and_historical_margin_is_not_qualification(self):
        f=Fake(); result=m.observe(reader(f)); raw=json.dumps(result)
        self.assertTrue(result['receipt_and_live_code_verified'])
        self.assertTrue(result['receipt']['all_expected_source_hashes_present_and_matching'])
        self.assertEqual(result['api_calls'],10); self.assertEqual(len(f.calls),10)
        self.assertEqual(result['observed_max_memory_mb'],942)
        self.assertAlmostEqual(result['timeout_minus_observed_duration_seconds'],740.37684)
        self.assertEqual(result['memory_minus_observed_max_memory_mb'],3154)
        self.assertEqual(result['reports']['reports_ending_after_last_modified'],0)
        self.assertFalse(result['enabled_supplement_capacity_established'])
        self.assertFalse(result['reports']['code_version_attribution_verified'])
        self.assertFalse(result['public_output_body_read']); self.assertFalse(result['public_output_semantic_freshness_verified'])
        for forbidden in (SECRET,REQUEST,'Environment','message','logStreamName','RequestId','actor'):
            # Intentional counters use the word messages; no raw-key/value leakage.
            if forbidden == 'message': continue
            self.assertNotIn(forbidden,raw)
        self.assertTrue(all(b.closed for b in f.bodies))
        for name,args in f.calls:
            if name.startswith('get_function'): self.assertEqual(args,{'FunctionName':m.FUNCTION_ARN})
            if name in ('head_object','get_object'):
                self.assertEqual(args['Bucket'],m.BUCKET)
                self.assertIn(args['Key'],(m.RECEIPT,m.OUTPUT))
                if name == 'get_object': self.assertEqual(args['Key'],m.RECEIPT); self.assertIn('IfMatch',args)
            if name == 'get_metric_statistics':
                self.assertEqual(args['Dimensions'],[{'Name':'FunctionName','Value':m.FUNCTION}])
                self.assertLessEqual((args['EndTime']-args['StartTime']).total_seconds(),48*3600)
            if name == 'filter_log_events': self.assertEqual(args['filterPattern'],m.FILTER)

    def test_mutations_and_unreviewed_reads_are_blocked_before_sdk(self):
        bad=[('lambda','invoke',{'FunctionName':m.FUNCTION}),('lambda','update_function_configuration',{}),
             ('s3','put_object',{}),('s3','delete_object',{}),('s3','list_objects_v2',{}),
             ('logs','start_query',{}),('events','describe_rule',{}),('scheduler','update_schedule',{}),
             ('lambda','get_function_configuration',{'FunctionName':'other'}),
             ('s3','get_object',{'Bucket':m.BUCKET,'Key':m.OUTPUT,'IfMatch':'"'+'a'*32+'"'}),
             ('s3','head_object',{'Bucket':m.BUCKET,'Key':'private/brain.json'}),
             ('s3','head_object',{'Bucket':'other','Key':m.OUTPUT}),
             ('cloudwatch','get_metric_statistics',{'MetricName':'Duration'}),
             ('logs','filter_log_events',{'logGroupName':'/aws/lambda/other'})]
        f=Fake(); api=reader(f)
        for service,operation,args in bad:
            with self.subTest(operation=operation,args=args),self.assertRaises(m.Stop): api.call(service,operation,**args)
        self.assertEqual(f.calls,[]); self.assertEqual(api.calls,0)

    def test_three_pages_twelve_calls_and_pagination_incompleteness(self):
        f=Fake(); event=f.log_pages[0]['events'][0]
        f.log_pages=[{'events':[event]*20,'nextToken':str(i)} for i in range(3)]
        result=m.observe(reader(f))
        self.assertEqual(result['api_calls'],12); self.assertEqual(len(result['reports']['numeric_records']),60)
        self.assertFalse(result['reports']['pagination_complete'])
        f=Fake(); f.log_pages=[{'events':[],'nextToken':'same'}]*2
        result=m.observe(reader(f)); self.assertEqual(result['reports']['pages_read'],2)
        self.assertFalse(result['reports']['pagination_complete'])

    def test_budget_and_deadline_reject_before_or_after_a_single_read(self):
        f=Fake(); api=reader(f); api.calls=12
        with self.assertRaises(m.Stop): m.configuration(api)
        self.assertEqual(f.calls,[])
        clock=[0]; api=reader(f,lambda:clock[0]); clock[0]=90
        with self.assertRaises(m.Stop): m.configuration(api)
        self.assertEqual(f.calls,[])
        clock[0]=0; api=reader(f,lambda:clock[0]); f.after_call=lambda:clock.__setitem__(0,91)
        with self.assertRaises(m.Stop): m.observe(api)
        self.assertEqual(len(f.calls),1)

    def test_missing_telemetry_stays_null_but_measured_zero_stays_zero(self):
        f=Fake(); f.metric_points=[]; f.log_pages=[{'events':[]}]
        r=m.observe(reader(f))
        for key in ('observed_max_duration_ms','observed_max_memory_mb','timeout_minus_observed_duration_seconds','memory_minus_observed_max_memory_mb'):
            self.assertIsNone(r[key])
        self.assertEqual(r['metrics']['Errors']['status'],'no_returned_datapoints')
        r=m.observe(reader(Fake())); self.assertEqual(r['metrics']['Errors']['points'][0]['Sum'],0)
        f=Fake(); original=f.get_metric_statistics
        def no_samples(**kw):
            r=original(**kw)
            if kw['MetricName']=='Duration': r['Datapoints'][0].update(Average=0,Maximum=0,SampleCount=0)
            return r
        f.get_metric_statistics=no_samples; self.assertIsNone(m.observe(reader(f))['observed_max_duration_ms'])

    def test_all_known_receipt_mismatches_and_live_configuration_drift_fail_verification(self):
        for case in ('commit','code','zip','verified','source','state','drift'):
            f=Fake()
            if case=='commit': f.receipt['commit']='b'*40
            if case=='code': f.receipt['code_sha256']=base64.b64encode(b't'*32).decode()
            if case=='zip': f.receipt['zip_sha256_hex']='a'*64
            if case=='verified': f.receipt['verified']=False
            if case=='source': f.receipt['source']['etf_desk_model.py']['sha256']='a'*64
            if case=='state': f.cfg['State']='Pending'
            if case=='drift': f.config_after={**f.cfg,'MemorySize':8192}
            with self.subTest(case=case):
                r=m.observe(reader(f)); self.assertFalse(r['receipt_and_live_code_verified'])
                if case=='drift':
                    self.assertFalse(r['runtime_stable']); self.assertIsNone(r['memory_minus_observed_max_memory_mb'])

    def test_receipt_precondition_race_and_foreign_function_identity_stop(self):
        f=Fake()
        def raced(**kw):
            f.record('get_object',kw)
            raise Error('PreconditionFailed')
        f.get_object=raced
        with self.assertRaisesRegex(m.Stop,'read_changed'): m.observe(reader(f))
        self.assertEqual(len(f.calls),3)
        for field in ('FunctionName','FunctionArn','State','LastUpdateStatus','LastModified'):
            f=Fake(); f.cfg[field]=SECRET
            with self.subTest(field=field),self.assertRaises(m.Stop): m.observe(reader(f))
            self.assertEqual(len(f.calls),1)

    def test_absent_shared_hashes_do_not_become_verified_sources(self):
        f=Fake()
        for name in ('etf_desk_model.py','etf_desk_store.py'): del f.receipt['source'][name]
        r=m.observe(reader(f)); self.assertTrue(r['receipt_and_live_code_verified'])
        self.assertFalse(r['receipt']['all_expected_source_hashes_present_and_matching'])
        self.assertIsNone(r['receipt']['expected_source_matches']['etf_desk_model.py'])

    def test_receipt_body_bound_duplicate_json_and_readback_length(self):
        for case in ('oversize','duplicate','extra','nan','wrong_etag'):
            f=Fake()
            if case=='oversize': f.metadata_override={'ContentLength':m.MAX_RECEIPT_BYTES+1}
            if case=='duplicate': f.override_raw=b'{"commit":"a","commit":"b"}'
            if case=='extra': f.get_extra=b'extra'
            if case=='nan': f.override_raw=b'{"unreviewed":NaN}'
            if case=='wrong_etag': f.metadata_override={'ETag':SECRET}
            with self.subTest(case=case),self.assertRaises(m.Stop): m.observe(reader(f))
            self.assertTrue(all(b.closed for b in f.bodies))
            if case in ('oversize','wrong_etag'): self.assertFalse(any(name=='get_object' for name,_ in f.calls))

    def test_only_full_bounded_numeric_reports_are_projected(self):
        expected={'duration_ms','billed_duration_ms','memory_mb','max_memory_mb','init_duration_ms'}
        self.assertEqual(set(m.report_fields(LINE)),expected)
        for bad in (SECRET+' '+LINE,LINE+SECRET,LINE.replace('942 MB','NaN MB'),LINE.replace('942 MB','99999 MB'),
                    LINE.replace('159623.16 ms','-1 ms'),'x'*4097,None):
            self.assertIsNone(m.report_fields(bad))
        f=Fake(); f.log_pages=[{'events':[{'message':LINE+SECRET,'timestamp':int(AT.timestamp()*1000)},
                                       {'message':LINE,'timestamp':int((AT+timedelta(seconds=1)).timestamp()*1000)}]}]
        r=m.observe(reader(f)); self.assertEqual(r['reports']['ignored_records'],2)
        self.assertEqual(r['reports']['numeric_records'],[]); self.assertIsNone(r['observed_max_memory_mb'])

    def test_metrics_contract_rejects_bad_numbers_duplicates_future_and_excess(self):
        original=Fake().get_metric_statistics(MetricName='Duration',Unit='Milliseconds')['Datapoints'][0]
        for case in ('nan','inf','bool','negative','missing','duplicate','future','too_many','mean_above_max'):
            f=Fake(); point=copy.deepcopy(original)
            if case in ('nan','inf','bool','negative','missing'): point['Maximum']={'nan':float('nan'),'inf':float('inf'),'bool':True,'negative':-1,'missing':None}[case]
            if case=='future': point['Timestamp']=AT+timedelta(hours=1)
            if case=='mean_above_max': point['Average']=point['Maximum']+1
            f.metric_points=[point]*(49 if case=='too_many' else 2 if case=='duplicate' else 1)
            with self.subTest(case=case),self.assertRaises(m.Stop): m.observe(reader(f))

    def test_oversized_log_page_and_denial_stop_without_fallback(self):
        f=Fake(); f.log_pages=[{'events':[f.log_pages[0]['events'][0]]*21}]
        with self.assertRaises(m.Stop): m.observe(reader(f))
        for operation in ('get_function_configuration','head_object','get_object','get_metric_statistics','filter_log_events'):
            f=Fake(); f.error=operation
            with self.subTest(operation=operation),self.assertRaisesRegex(m.Stop,'read_denied'): m.observe(reader(f))
            self.assertEqual(f.calls[-1][0],operation)

    def test_expected_hashes_pin_reviewed_sources_and_staged_script_has_no_mutating_calls(self):
        for name,sha in m.EXPECTED_SOURCES.items():
            path=ROOT/'aws/lambdas'/m.FUNCTION/'source'/name if name=='lambda_function.py' else ROOT/'aws/shared'/name
            self.assertEqual(hashlib.sha256(path.read_bytes()).hexdigest(),sha)
        self.assertEqual(m.ALLOWED,frozenset({('lambda','get_function_configuration'),('s3','head_object'),('s3','get_object'),('cloudwatch','get_metric_statistics'),('logs','filter_log_events')}))
        tree=ast.parse(SCRIPT.read_text())
        for node in ast.walk(tree):
            if isinstance(node,ast.Call) and isinstance(node.func,ast.Attribute):
                self.assertFalse(node.func.attr.startswith(('put_','delete_','update_','create_')))
                self.assertNotIn(node.func.attr,('invoke','start_query','stop_query','list_objects_v2'))
        self.assertEqual(SCRIPT.parent.name,'staged')

    def test_repeat_observation_is_idempotent_read_only(self):
        first=Fake(); second=Fake()
        self.assertEqual(m.observe(reader(first)),m.observe(reader(second)))
        self.assertEqual(first.calls,second.calls)

    def test_runner_guard_precedes_client_creation(self):
        import boto3
        with mock.patch.dict(os.environ,{},clear=True),mock.patch.object(boto3,'client') as client,self.assertRaisesRegex(SystemExit,'reviewed_direct_runner_only'):
            m.main()
        client.assert_not_called()

    def test_report_success_and_failures_never_emit_untrusted_values(self):
        import boto3
        real_reads=m.Reads
        for case in ('success','denied','mismatch','unexpected','injected_stop','injected_stop_dict'):
            f=Fake()
            if case=='denied': f.error='get_metric_statistics'
            if case=='mismatch': f.receipt['commit']='e'*40
            if case=='unexpected': f.metadata_override={'ContentLength':None}
            with self.subTest(case=case),tempfile.TemporaryDirectory() as temp:
                env={'GITHUB_ACTIONS':'true','GITHUB_REPOSITORY':'ElMooro/si','GITHUB_EVENT_NAME':'workflow_dispatch',
                     'GITHUB_WORKFLOW':'Run ops script (direct)','GITHUB_SHA':'a'*40,
                     'GITHUB_WORKSPACE':temp,'GITHUB_STEP_SUMMARY':str(Path(temp)/'summary')}
                def client(service,**kwargs):
                    if case=='injected_stop': raise m.Stop(SECRET)
                    if case=='injected_stop_dict': raise m.Stop({'secret':SECRET})
                    self.assertEqual(kwargs['region_name'],m.REGION)
                    self.assertEqual(kwargs['config'].retries,{'total_max_attempts':1})
                    return f
                stdout=io.StringIO()
                with mock.patch.dict(os.environ,env),mock.patch.object(boto3,'client',side_effect=client),mock.patch.object(m,'Reads',side_effect=lambda clients,at:real_reads(clients,AT)),redirect_stdout(stdout):
                    if case=='success': m.main()
                    else:
                        with self.assertRaises(SystemExit) as exc: m.main()
                        self.assertEqual(exc.exception.code,1)
                report=(Path(temp)/'aws/ops/reports/latest'/ (SCRIPT.stem+'.md')).read_text()
                for text in (report,stdout.getvalue(),(Path(temp)/'summary').read_text()):
                    self.assertNotIn(SECRET,text); self.assertNotIn(REQUEST,text)
                    self.assertNotIn('Traceback',text); self.assertNotIn('Environment',text)
                self.assertIn('**Status:** '+('success' if case=='success' else 'failure'),report)


if __name__ == '__main__': unittest.main()
