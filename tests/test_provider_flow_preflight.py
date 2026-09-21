"""The original-source audit never publishes credentials or follows a redirect."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from collections import Counter
from decimal import Decimal
from unittest import mock
import ast,io,json,urllib.request,urllib.error,urllib.parse,unittest
PATH=Path(__file__).resolve().parents[1]/'aws/ops/staged/ops_5973_provider_fund_flow_source_preflight.py'

class SourceAudit(unittest.TestCase):
    def setUp(self):
        self.retained=[]
        def retain(client,raw):self.retained.append(raw);return {'key':'protected','bytes':len(raw)}
        self.ns=dict(datetime=datetime,timezone=timezone,timedelta=timedelta,Counter=Counter,Decimal=Decimal,json=json,urllib=urllib,
            ENDPOINT='https://api.polygon.io/etf-global/v1/fund-flows',retain=retain)
        nodes=[n for n in ast.parse(PATH.read_text(encoding='utf-8')).body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in ('bounded','probe','NoRedirect')]
        exec(compile(ast.Module(body=nodes,type_ignores=[]),str(PATH),'exec'),self.ns)
    def test_whole_original_retained_and_credential_only_in_header(self):
        raw=b'{"status":"OK","results":[{"composite_ticker":"SPY","fund_flow":0,"effective_date":"2026-09-18","processed_date":"2026-09-19"}],"extension":{"future_schema":true}}'
        opener=mock.Mock();opener.open.return_value=io.BytesIO(raw)
        with mock.patch('urllib.request.build_opener',return_value=opener):r=self.ns['probe'](None,'fixture-secret','SPY')
        req=opener.open.call_args.args[0]
        self.assertNotIn('fixture-secret',req.full_url);self.assertEqual(req.get_header('Authorization'),'Bearer fixture-secret')
        self.assertEqual(self.retained,[raw]);self.assertEqual(r['rows'],1);self.assertEqual(r['sample_latest_effective_rows'][0]['fund_flow'],0)
        self.assertNotIn('fixture-secret',json.dumps(r))
    def test_response_with_credential_is_never_retained(self):
        opener=mock.Mock();opener.open.return_value=io.BytesIO(b'{"status":"OK","results":[],"debug":"fixture-secret"}')
        with mock.patch('urllib.request.build_opener',return_value=opener),self.assertRaises(AssertionError):self.ns['probe'](None,'fixture-secret','SPY')
        self.assertEqual(self.retained,[])
    def test_http_error_is_fixed_metadata_without_body(self):
        opener=mock.Mock();opener.open.side_effect=urllib.error.HTTPError('https://provider/?secret=fixture-secret',403,'private reason',{},io.BytesIO(b'private error body'))
        with mock.patch('urllib.request.build_opener',return_value=opener):r=self.ns['probe'](None,'fixture-secret','SPY')
        self.assertEqual(r['http_status'],403);self.assertEqual(self.retained,[]);self.assertNotIn('private',json.dumps(r));self.assertNotIn('fixture-secret',json.dumps(r))
    def test_no_redirects(self):
        with self.assertRaises(ValueError):self.ns['NoRedirect']().redirect_request(None,None,None,None,None,None)

if __name__=='__main__':unittest.main(verbosity=2)
