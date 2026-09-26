"""Full response capture and request claims; all network and storage calls mocked."""
from pathlib import Path
from datetime import date,timedelta
from unittest.mock import patch
import io,json,sys,unittest
from urllib.parse import urlsplit,parse_qs
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks')]
import ops_6137_fifx_full_source_capture as operation
import fifx_source_capture as source

class Storage:
    def __init__(self):self.objects={};self.puts=[]
    def put_object(self,**kw):self.objects[kw['Key']]=kw['Body'];self.puts.append(kw)
    def get_object(self,**kw):return {'Body':io.BytesIO(self.objects[kw['Key']])}

def quote():return {'chart':{'result':[{'meta':{'symbol':'^MOVE','longName':'Wrong instrument','instrumentType':'INDEX'},
    'timestamp':[1,2,3],'indicators':{'quote':[{'close':[0,None,1.25],'open':[0,None,1.0],'volume':[0,None,10]}]}}],'error':None}}

class Tests(unittest.TestCase):
    def test_complete_fixed_request_plan_has_no_key_paid_provider_or_duplicate_move(self):
        plan=source.request_plan('2026-09-26T06:00:00Z');self.assertEqual(len(plan),17);self.assertNotIn('^MOVE',plan)
        self.assertEqual(set(plan),set(source.SERIES)|set(source.SYMBOLS[1:]))
        for key,spec in plan.items():
            url=urlsplit(spec['url']);query=parse_qs(url.query)
            self.assertEqual(url.scheme,'https');self.assertNotIn('api_key',query);self.assertNotIn('token',query)
            if key in source.SERIES:
                self.assertEqual(url.netloc,'fred.stlouisfed.org');self.assertEqual(query,{'id':[key],'cosd':['1988-01-01'],'coed':['2026-09-26']})
            else:
                self.assertEqual(url.netloc,'query1.finance.yahoo.com');self.assertEqual(query['interval'],['1d'])
                if key!='^VHSI':self.assertEqual(query['period1'],['315532800']);self.assertTrue(int(query['period2'][0])>315532800)
        with self.assertRaises(ValueError):source.request_plan('2026-09-26T06:00:00')
    def test_all_csv_rows_including_missing_zero_and_negative_are_inspected(self):
        rows=['observation_date,DGS10']+[(date(1990,1,1)+timedelta(days=i)).isoformat()+','+['.','0','-1.25','4.5'][i%4] for i in range(5000)]
        out=source.inspect_csv(('\n'.join(rows)+'\n').encode(),'DGS10');self.assertEqual(out['original_rows'],5000)
        self.assertEqual(out['missing_values'],1250);self.assertEqual(out['nonpositive_values'],2500);self.assertFalse(out['point_in_time_qualified'])
    def test_csv_wrong_identity_duplicates_nonfinite_and_truncated_rows_fail(self):
        for raw in (b'DATE,DGS2\n2020-01-01,0\n',b'DATE,DGS10\n2020-01-01,0\n2020-01-01,1\n',
            b'DATE,DGS10\n2020-01-01,NaN\n',b'DATE,DGS10\n2020-01-01\n',b'DATE,DGS10\n2020-01-02,1\n2020-01-01,2\n'):
            with self.assertRaises(ValueError):source.inspect_csv(raw,'DGS10')
    def test_quote_inspection_never_qualifies_a_matching_ticker_or_drops_null_rows(self):
        out=source.inspect_quote(json.dumps(quote()).encode(),'^MOVE');self.assertEqual(out['original_rows'],3);self.assertEqual(out['missing_closes'],1)
        self.assertEqual(out['metadata']['longName'],'Wrong instrument');self.assertFalse(out['instrument_identity_qualified']);self.assertFalse(out['official_feed_parity_verified'])
        q=quote();q['chart']['result'][0]['indicators']['quote'][0]['close'].pop()
        with self.assertRaises(ValueError):source.inspect_quote(json.dumps(q).encode(),'^MOVE')
        q=quote();q['chart']['result'][0]['timestamp']=[1,1,3]
        with self.assertRaises(ValueError):source.inspect_quote(json.dumps(q).encode(),'^MOVE')
    def test_each_request_is_durably_recorded_before_one_attempt_and_never_retried(self):
        client=Storage();progress={'sources':{},'provider_request_attempts':0};spec=source.request_plan('2026-09-26T06:00:00Z')['DGS10']
        def fail(arg):
            saved=json.loads(client.objects[operation.STATUS]);self.assertEqual(saved['sources']['DGS10']['status'],'attempt_recorded');self.assertEqual(saved['provider_request_attempts'],1);raise TimeoutError()
        with patch.object(source,'acquire',side_effect=fail) as acquire:operation.capture_one(client,progress,'DGS10',spec)
        self.assertEqual(acquire.call_count,1);self.assertEqual(progress['sources']['DGS10']['status'],'unavailable')
    def test_bad_http_response_still_retains_whole_bytes_and_receipt_privately(self):
        client=Storage();progress={'sources':{},'provider_request_attempts':0};spec=source.request_plan('2026-09-26T06:00:00Z')['DGS10']
        raw=b'complete unavailable response';receipt={'http_status':429,'bytes':len(raw),'sha256':operation.baseline.sha(raw),'source_url':spec['url']}
        with patch.object(source,'acquire',return_value=(raw,receipt)):operation.capture_one(client,progress,'DGS10',spec)
        row=progress['sources']['DGS10'];self.assertEqual(row['status'],'retained_unqualified_response');self.assertEqual(client.objects[row['original']['key']],raw)
        self.assertNotIn('inspection',row);self.assertTrue(all(p['Key'].startswith(operation.baseline.PRIVATE) for p in client.puts))
    def test_complete_response_and_receipt_have_fixed_timeout_bounded_read_and_no_redirect(self):
        class Response(io.BytesIO):
            code=200;headers={'Content-Type':'text/csv','Set-Cookie':'must not retain'}
        class Opener:
            def open(self,request,timeout):self.timeout=timeout;self.request=request;return Response(b'DATE,DGS10\n2020-01-01,0\n')
        opener=Opener();spec=source.request_plan('2026-09-26T06:00:00Z')['DGS10']
        with patch.object(source.urllib.request,'build_opener',return_value=opener):raw,receipt=source.acquire(spec)
        self.assertEqual(opener.timeout,35);self.assertEqual(receipt['bytes'],len(raw));self.assertNotIn('Set-Cookie',receipt['headers'])
        self.assertIsNone(source.NoRedirect().redirect_request(None,None,None,None,None,None))
    def test_operation_only_retains_private_sources_and_does_not_mutate_native_execution(self):
        text=Path(operation.__file__).read_text(encoding='utf-8')
        for bad in ('.invoke(','.update_function_code(','.put_rule(','.update_schedule(','.get_parameter(','.get_secret_value('):self.assertNotIn(bad,text)
        for required in ('journal(s3,progress,True)','sys.exit(1)',"privacy['all_denied']",'bond.binding(packet,read)','native_package_unchanged=True'):self.assertIn(required,text)
        client=Storage()
        with self.assertRaises(ValueError):operation.checked(client,{'key':'audit-private/wrong','sha256':'a'*64,'bytes':10})
if __name__=='__main__':unittest.main(verbosity=2)
