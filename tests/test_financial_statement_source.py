from pathlib import Path
import json, sys, unittest, importlib.util
from io import BytesIO
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/ops/checks'))
import financial_statement_source as source


def row(**changes):
    return dict(date='2025-09-27', symbol='AAPL', reportedCurrency='USD', cik='0000320193',
        filingDate='2025-10-31', acceptedDate='2025-10-31 06:01:00', fiscalYear='2025', period='FY',
        revenue=0, depreciationAndAmortization=None, **changes)


class Tests(unittest.TestCase):
    def test_whole_rows_preserve_zeros_nulls_duplicates_and_provider_identity(self):
        spec = source.request_spec('AAPL', 'income-statement', 'annual')
        item = row(); result = source.inspect(json.dumps([item, item]).encode(), spec)
        self.assertEqual(result['rows'], 2)
        self.assertEqual(result['zero_field_counts'], {'revenue': 2})
        self.assertEqual(result['null_field_counts'], {'depreciationAndAmortization': 2})
        self.assertEqual(result['repeated_statement_identities'], 1)
        self.assertEqual(result['metadata'][1]['source_row'], 1)
        self.assertEqual(result['identity_problems'], [])
        self.assertFalse(result['accounting_calculations_qualified'])

    def test_alignment_cannot_join_array_positions_different_currencies_or_dates(self):
        spec = source.request_spec('AAPL', 'income-statement', 'annual')
        a, b = row(), row(); b['date']='2024-09-28'; b['fiscalYear']='2024'
        inventories = {key: source.inspect(json.dumps([a, b]).encode(), {**spec, **source.request_spec('AAPL', key, 'annual')}) for key in source.ENDPOINTS}
        inventories['balance-sheet-statement']['metadata'].reverse()
        value = source.alignment(inventories)
        self.assertEqual(value['exact_common_identities'], 2); self.assertFalse(value['positionally_aligned'])
        inventories['cash-flow-statement']['metadata'][0]['reportedCurrency']='EUR'
        self.assertEqual(source.alignment(inventories)['exact_common_identities'], 1)

    def test_wrong_symbol_missing_dimensions_and_unreviewed_requests_remain_unqualified(self):
        spec = source.request_spec('AAPL', 'income-statement', 'annual')
        item = row(); item.update(symbol='OTHER', reportedCurrency=None, cik=None, date=None, period='Q4')
        result=source.inspect(json.dumps([item]).encode(), spec)
        self.assertEqual(len(result['identity_problems']),5)
        for raw in (b'{"error":"limit"}', b'[]', b'[{"date":1,"date":2}]', b'[NaN]'):
            with self.assertRaises(ValueError): source.inspect(raw,spec)
        for args in (('PRIVATE','income-statement','annual'),('AAPL','account','annual'),('AAPL','income-statement','custom')):
            with self.assertRaises(ValueError): source.request_spec(*args)

    def test_capture_retains_complete_errors_claims_once_and_keeps_credentials_out_of_journals(self):
        spec = importlib.util.spec_from_file_location('financial_source_probe_test', ROOT / 'aws/ops/staged/ops_6072_financial_statement_original_probe.py')
        mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
        class Conflict(Exception):
            response={'Error':{'Code':'412'}}
        class Storage:
            def __init__(self):self.data={}
            def put_object(self,**kw):
                if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.data:raise Conflict()
                assert kw['Key'].startswith(mod.PRIVATE);self.data[kw['Key']]=kw['Body']
            def get_object(self,**kw):return {'Body':BytesIO(self.data[kw['Key']])}
        for code, body in ((200,json.dumps([row()]).encode()),(503,b''),(200,b'{"error":"unavailable"}')):
            client,calls=Storage(),[]
            def transport(request,**kw):
                calls.append(request.full_url);response=BytesIO(body);response.status=code;response.headers={'Content-Type':'application/json'};return response
            if code==200 and body.startswith(b'['):
                record, inv, key=mod.capture(client,mod.SPECS[0],'example-test-value',transport)
                self.assertEqual(inv['rows'],1)
            else:
                with self.assertRaises(RuntimeError):mod.capture(client,mod.SPECS[0],'example-test-value',transport)
            self.assertIn(body,client.data.values())
            self.assertTrue(calls[0].endswith('&apikey=example-test-value'))
            with self.assertRaises(Conflict):mod.capture(client,mod.SPECS[0],'example-test-value',transport)
            self.assertEqual(len(calls),1)
            for key,value in client.data.items():
                if key.endswith('.json'):self.assertNotIn(b'example-test-value',value)


if __name__ == '__main__':
    unittest.main(verbosity=2)
