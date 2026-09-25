from pathlib import Path
from io import BytesIO
from datetime import date
from unittest.mock import Mock,patch
import sys,time,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
import ops_6038_offexchange_journalled_source_inventory as op
from test_option_flow_store import S3
class Tests(unittest.TestCase):
    def spec(self):return op.prior.plan(date(2026,9,25),'2026-09-24')['probe:ATS_W_SMBL']
    def response(self,raw,code=200,headers=None):
        response=BytesIO(raw);response.status=code;response.headers=headers or {};return response
    def test_empty_204_is_preserved_with_its_status_not_market_zero(self):
        db=S3({});opener=Mock();opener.open.return_value=self.response(b'',204,{'Record-Total':'0'})
        out=op.fetch(db,self.spec(),opener,time.monotonic()+10)
        self.assertEqual(out['status'],'empty_http_response');self.assertEqual(out['original']['bytes'],0)
        self.assertEqual(out['http_status'],204);self.assertFalse(out['coverage_complete'])
        self.assertEqual(db.data[out['original']['key']],b'')
    def test_empty_200_is_distinct_from_a_valid_empty_json_array(self):
        results=[]
        for raw in (b'',b'[]'):
            opener=Mock();opener.open.return_value=self.response(raw);results.append(op.fetch(S3({}),self.spec(),opener,time.monotonic()+10))
        self.assertNotEqual(results[0]['status'],results[1]['status']);self.assertFalse(results[1]['inventory']['coverage_complete'])
    def test_bounded_source_never_preserves_truncated_prefix(self):
        self.assertIsNone(op.read_response(BytesIO(b'12345'),4));self.assertEqual(op.read_response(BytesIO(b''),4),b'')
    def test_http_error_is_retained_and_transport_failure_is_not_invented(self):
        opener=Mock();opener.open.side_effect=urllib.error.HTTPError(self.spec()['url'],429,'Rate limited',{},BytesIO(b''))
        result=op.fetch(S3({}),self.spec(),opener,time.monotonic()+10);self.assertEqual(result['http_status'],429);self.assertEqual(result['original']['bytes'],0)
        opener.open.side_effect=TimeoutError();result=op.fetch(S3({}),self.spec(),opener,time.monotonic()+10);self.assertIsNone(result['original'])
    def test_each_completed_response_has_its_own_durable_journal(self):
        db=S3({});opener=Mock();opener.open.return_value=self.response(b'[]')
        result,key=op.task(db,'weekly-test',self.spec(),time.monotonic()+10,opener)
        saved=op.prior.strict(db.data[key]);self.assertEqual(saved['capture'],result);self.assertEqual(saved['status'],'complete')
        with self.assertRaises(Exception):op.task(db,'weekly-test',self.spec(),time.monotonic()+10,opener)
        self.assertEqual(opener.open.call_count,1)
    def test_failed_task_is_recorded_before_exception_propagates(self):
        db=S3({});opener=Mock();opener.open.side_effect=ValueError('Rejected redirect')
        with self.assertRaises(ValueError):op.task(db,'failed-test',self.spec(),time.monotonic()+10,opener)
        docs=[op.prior.strict(raw) for key,raw in db.data.items() if key.endswith('.json')]
        self.assertEqual(docs[0]['status'],'failed');self.assertEqual(docs[0]['error_type'],'ValueError')
    def test_unreviewed_destination_never_reaches_transport(self):
        opener=Mock()
        with self.assertRaises(ValueError):op.fetch(S3({}),{**self.spec(),'url':'https://example.org'},opener,time.monotonic()+10)
        opener.open.assert_not_called()
if __name__=='__main__':unittest.main(verbosity=2)
