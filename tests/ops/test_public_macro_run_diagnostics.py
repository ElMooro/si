"""Read-only diagnostics return only reviewed literals and runtime counters."""
from pathlib import Path
from unittest.mock import patch
import ast, importlib.util, unittest, hashlib, json, sys
from io import BytesIO

ROOT = Path(__file__).resolve().parents[2]
path = ROOT / 'aws/ops/staged/ops_6223_public_macro_run_diagnostics.py'
spec = importlib.util.spec_from_file_location('public_run_diagnostics', path)
subject = importlib.util.module_from_spec(spec); spec.loader.exec_module(subject)
END = subject.datetime(2026, 9, 27, 11, 35, tzinfo=subject.timezone.utc)
EVENTS = [
    {'eventId': '1', 'timestamp': 1790508600000, 'message': '[ERROR] AcquisitionError: Whole provider query failed https://example.invalid/?api_key=DO_NOT_RETURN_THIS\n  File "/var/task/portwatch_acquisition.py", line 32, in query'},
    {'eventId': '2', 'timestamp': 1790508601000, 'message': 'REPORT RequestId: ignore\tDuration: 122.5 ms\tBilled Duration: 123 ms\tMemory Size: 1024 MB\tMax Memory Used: 92 MB\tStatus: error'},
]


class Tests(unittest.TestCase):
    def test_public_head_paths_match_original_producers(self):
        modules = {'justhodl-portwatch': 'portwatch_store.py', 'justhodl-geopolitical-risk': 'geo_news_store.py'}
        for fn, module in modules.items():
            tree = ast.parse((ROOT / 'aws/lambdas' / fn / 'source' / module).read_bytes())
            heads = [n.value.value for n in tree.body if isinstance(n, ast.Assign) and isinstance(n.value, ast.Constant)
                     and any(isinstance(t, ast.Name) and t.id == 'HEAD' for t in n.targets)]
            self.assertEqual(heads, [subject.TARGETS[fn]])
    def test_sanitized_complete_pagination_deduplication_and_exact_target(self):
        class Logs:
            def __init__(self): self.calls = []
            def filter_log_events(self, **kw):
                self.calls.append(kw)
                return {'events': EVENTS, **({'nextToken': 'page2'} if len(self.calls) == 1 else {})}
        logs = Logs(); out = subject.diagnose(logs, 'justhodl-portwatch', END)
        self.assertEqual(out['events_examined'], 2); self.assertEqual(len(logs.calls), 2)
        self.assertEqual(out['reviewed_error_counts'], {'Whole provider query failed': 1})
        self.assertEqual(out['source_locations'], {'portwatch_acquisition.py:32': 1})
        self.assertEqual(out['execution_reports'][0]['Duration'], {'value': 122.5, 'unit': 'ms'})
        self.assertEqual(logs.calls[1]['nextToken'], 'page2')
        self.assertEqual(logs.calls[0]['logGroupName'], '/aws/lambda/justhodl-portwatch')
        for prohibited in ('DO_NOT_RETURN', 'example.invalid', 'RequestId', 'ignore'):
            self.assertNotIn(prohibited, str(out))
        self.assertEqual(out['raw_messages_returned'], 0)
    def test_unknown_error_text_and_foreign_file_paths_are_not_returned(self):
        class Logs:
            def filter_log_events(self, **kw):
                return {'events': [{'eventId': 'x', 'timestamp': 1, 'message': '[ERROR] ValueError: private text\n  File "/var/task/private_account.py", line 1, in handler'}]}
        out = subject.diagnose(Logs(), 'justhodl-portwatch', END)
        self.assertEqual(out['exception_type_counts'], {'ValueError': 1}); self.assertEqual(out['reviewed_error_counts'], {})
        self.assertEqual(out['source_locations'], {}); self.assertNotIn('private', str(out))
    def test_caught_native_failure_class_is_visible_without_raw_message(self):
        class Logs:
            def filter_log_events(self, **kw):
                return {'events': [{'eventId': 'x', 'timestamp': 1, 'message': 'PortWatch preserved publication failed: CaptureError\n'}]}
        self.assertEqual(subject.diagnose(Logs(), 'justhodl-portwatch', END)['exception_type_counts'], {'CaptureError': 1})
    def test_only_bound_complete_retained_public_attempts_are_inspected(self):
        sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-portwatch/source'))
        import portwatch_store as store
        data = {}; stamp = subject.datetime(2026, 9, 27, 11, 20, tzinfo=subject.timezone.utc)
        def put(value):
            raw = json.dumps(value).encode(); h = hashlib.sha256(raw).hexdigest(); key = store.PRIVATE+h+'.bin'; data[key] = raw
            return {'key': key, 'sha256': h, 'bytes': len(raw)}
        ref = put({'error': {'code': 400, 'message': 'Invalid query'}})
        url = 'https://services9.arcgis.com/weJ1QsnbMYJlCHdG/arcgis/rest/services/Daily_Ports_Data/FeatureServer/0/query?where=1%3D1&returnCountOnly=true&f=json'
        attempt = put({'request': {'url': url, 'method': 'GET', 'body_utf8': None, 'timeout': 25},
                       'acquired_at': stamp.isoformat(), 'status': 'http_response', 'http_status': 200, 'original': ref})
        class S3:
            def get_paginator(self, name):
                assert name == 'list_objects_v2'; return self
            def paginate(self, **kw):
                assert kw == {'Bucket': 'justhodl-dashboard-live', 'Prefix': store.PRIVATE}
                yield {'Contents': [{'Key': k, 'Size': len(v), 'LastModified': stamp} for k, v in data.items()]}
            def get_object(self, **kw):
                raw = data[kw['Key']]; return {'Body': BytesIO(raw), 'ContentLength': len(raw)}
        out = subject.portwatch_attempts(S3(), END)
        self.assertEqual(len(out['attempts']), 1); self.assertEqual(out['attempts'][0]['operation'], 'count')
        self.assertEqual(out['attempts'][0]['provider_error']['code'], 400)
        data[attempt['key']] += b' '
        with self.assertRaises(ValueError): subject.portwatch_attempts(S3(), END)
    def test_pagination_bound_cannot_be_reported_as_complete(self):
        class Logs:
            n = 0
            def filter_log_events(self, **kw):
                self.n += 1; return {'events': [], 'nextToken': str(self.n)}
        with self.assertRaisesRegex(ValueError, 'Complete run-log pagination'):
            subject.diagnose(Logs(), 'justhodl-portwatch', END)
    def test_expired_window_does_not_create_aws_clients(self):
        class Future(subject.datetime):
            @classmethod
            def now(cls, tz=None): return cls(2026, 9, 28, tzinfo=subject.timezone.utc)
        with patch.object(subject, 'datetime', Future), patch.object(subject.boto3, 'client') as client:
            with self.assertRaises(ValueError): subject.main()
            client.assert_not_called()


if __name__ == '__main__': unittest.main()
