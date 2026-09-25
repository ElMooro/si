from pathlib import Path
from io import BytesIO
from unittest.mock import Mock,patch
import json,sys,time,unittest,urllib.error
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6031_gold_rotation_source_preflight as op

class Response(BytesIO):status=200

class Tests(unittest.TestCase):
    def test_requests_have_exact_identity_and_completed_date_range(self):
        for kind in op.KINDS:
            value=op.url('GLD',kind,'2020-01-01','2021-01-01')
            self.assertIn('symbol=GLD',value);self.assertNotIn('apikey',value)
            self.assertEqual('from=' in value,kind!='profile')
        for symbol,kind in (('SPY&apikey=secret','full'),('AAPL','full'),('GLD','../account')):
            with self.assertRaises(AssertionError):op.url(symbol,kind,'2020-01-01','2021-01-01')
        with self.assertRaises(AssertionError):op.url('GLD','full','2021-01-01','2020-01-01')

    def test_inventory_exposes_duplicate_dates_and_wrong_instrument(self):
        raw=op.encoded([{'date':'2021-01-05','symbol':'GLD','close':10},{'date':'2021-01-05','symbol':'SPY','close':None},
            {'date':'invalid','symbol':'GLD','close':12},{'date':'2021-01-01','symbol':'GLD','close':9}])
        row=op.describe(raw,'GLD','full')
        self.assertEqual(row['duplicate_dates'],1);self.assertEqual(row['symbol_mismatch_rows'],1)
        self.assertEqual(row['invalid_or_missing_dates'],1);self.assertEqual(row['value_coverage']['close']['null'],1)
        self.assertEqual(row['review_status'],'inventory_only_not_qualified')
        self.assertEqual(row['first_date'],'2021-01-01');self.assertEqual(row['last_date'],'2021-01-05')

    def test_profile_identity_and_adjusted_field_inventory_keep_their_definitions(self):
        profile=op.describe(op.encoded([{'symbol':'GLD','currency':'USD','isEtf':True}]),'GLD','profile')
        self.assertEqual(profile['identity'][0]['currency'],'USD')
        adjusted=op.describe(op.encoded([{'date':'2021-01-05','symbol':'GLD','adjClose':9.9,'adjVolume':100}]),'GLD','dividend-adjusted')
        self.assertIn('adjClose',adjusted['value_coverage']);self.assertNotIn('close',adjusted['value_coverage'])

    def test_complete_provider_response_is_retained_with_credential_only_in_header(self):
        opener=Mock();raw=op.encoded([{'symbol':'GLD','date':'2021-01-05','price':10}]);opener.open.return_value=Response(raw)
        with patch.object(op,'retain',return_value={'sha256':op.sha(raw)}) as retain:
            result=op.fetch(Mock(),'GLD','light','2020-01-01','2021-01-01','test-secret',opener,time.monotonic()+30)
        retain.assert_called_once();self.assertEqual(retain.call_args.args[1],raw)
        self.assertEqual(result['status'],'response_retained');self.assertNotIn('test-secret',json.dumps(result))
        request=opener.open.call_args.args[0];self.assertEqual(request.get_header('Apikey'),'test-secret');self.assertNotIn('test-secret',request.full_url)

    def test_credential_echo_and_oversized_original_cannot_be_retained(self):
        for raw in (b'{"secret":"test-secret"}',b'x'*(op.MAX+1)):
            opener=Mock();opener.open.return_value=Response(raw)
            with patch.object(op,'retain') as retain,self.assertRaises(ValueError):
                op.fetch(Mock(),'GLD','light','2020-01-01','2021-01-01','test-secret',opener,time.monotonic()+30)
            retain.assert_not_called()

    def test_transport_failure_never_becomes_a_zero_or_an_automatic_retry(self):
        opener=Mock();opener.open.side_effect=TimeoutError()
        with patch.object(op,'retain') as retain:
            value=op.fetch(Mock(),'SPY','full','2020-01-01','2021-01-01','test-secret',opener,time.monotonic()+30)
        self.assertEqual(value['status'],'transport_unavailable');self.assertIsNone(value['original'])
        self.assertEqual(opener.open.call_count,1);retain.assert_not_called()

    def test_http_error_body_is_retained_as_error_not_prices(self):
        opener=Mock();raw=b'{"Error Message":"subscription not available"}'
        opener.open.side_effect=urllib.error.HTTPError('https://financialmodelingprep.com/stable/profile',403,'Forbidden',{},BytesIO(raw))
        with patch.object(op,'retain',return_value={'sha256':op.sha(raw)}):
            value=op.fetch(Mock(),'SPY','full','2020-01-01','2021-01-01','test-secret',opener,time.monotonic()+30)
        self.assertEqual(value['http_status'],403);self.assertEqual(value['status'],'provider_error_retained')
        self.assertEqual(value['inventory']['review_status'],'unexpected_shape')

    def test_strict_original_parser_rejects_ambiguous_json(self):
        for raw in (b'{"close":1,"close":2}',b'{"close":NaN}'):
            with self.assertRaises(ValueError):op.strict(raw)
        with self.assertRaises(ValueError):op.NoRedirect().redirect_request(None,None,None,None,None,None)

if __name__=='__main__':unittest.main(verbosity=2)
