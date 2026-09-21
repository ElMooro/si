"""Baseline retention must not turn missing clocks/ratios into invented evidence."""
from pathlib import Path
from datetime import datetime, timezone
import hashlib, io, json, sys, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/staged')]
import ops_5987_options_dependency_preflight as audit

class Store:
    def __init__(self,raw):self.raw=raw;self.saved={};self.reads=[];self.corrupt=False
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        raw=self.saved.get(key,self.raw)
        if self.corrupt and key.startswith(audit.PREFIX):raw=b'altered'
        return {'Body':io.BytesIO(raw),'ETag':'"etag"','LastModified':datetime(2026,9,21,tzinfo=timezone.utc)}
    def put_object(self,**kw):
        assert kw['Key'].startswith(audit.PREFIX) and kw['IfNoneMatch']=='*'
        self.saved[kw['Key']]=kw['Body']

class Tests(unittest.TestCase):
    def test_complete_original_not_reserialized(self):
        raw=b'{ "all_results": [], "unknown_future_field": [1,2,3] }\n'
        s=Store(raw);ref,desc=audit.capture(s,'data/polygon-options-flow.json')
        self.assertEqual(s.saved[ref['key']],raw)
        self.assertEqual(ref['sha256'],hashlib.sha256(raw).hexdigest())
        self.assertIn('unknown_future_field',desc['root_fields'])
    def test_readback_must_match(self):
        s=Store(b'{}');s.corrupt=True
        with self.assertRaises(AssertionError):audit.capture(s,'data/dealer-gex.json')
    def test_private_account_path_rejected_before_read(self):
        s=Store(b'{}')
        with self.assertRaises(AssertionError):audit.capture(s,'data/trade-tickets.json')
        self.assertEqual(s.reads,[])
    def test_missing_clock_and_zero_ratio_are_distinct(self):
        d=audit.describe('data/polygon-options-flow.json',{'all_results':[
            {'ticker':'A','cv_pv_ratio':0,'snapshot_truncated':True},
            {'ticker':'B','cv_pv_ratio':None,'snapshot_truncated':False,'generated_at':'2026-09-21T00:00:00Z'},
            {'ticker':'C','error':'no_contracts'}]})
        self.assertEqual(d['partial_rows'],1)
        self.assertEqual(d['failed_rows'],1)
        self.assertEqual(d['rows_with_zero_call_put_ratio'],1)
        self.assertEqual(d['rows']['non_null_clock_rows']['observed_at'],0)
        self.assertEqual(d['rows']['non_null_clock_rows']['generated_at'],1)
        self.assertIsNone(d['permissions']['calls_eligible'])
    def test_history_not_truncated_or_assumed_sorted(self):
        d=audit.describe('data/dealer-gex-history.json',{'history':[{'ts':3},{'ts':0},{'ts':2}]})
        self.assertEqual(d['history']['rows'],3)
        self.assertEqual(d['history']['first_ts'],0)
        self.assertEqual(d['history']['last_ts'],3)
    def test_unexpected_root_shape_rejected(self):
        with self.assertRaises(AssertionError):audit.describe('data/dealer-gex.json',[])

if __name__=='__main__':unittest.main()
