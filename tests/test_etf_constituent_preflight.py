"""Original holdings probes stay bounded, dated and credential-safe."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from decimal import Decimal
from collections import Counter
import ast,io,json,re,types,unittest,urllib.request,urllib.error,urllib.parse
ROOT=Path(__file__).resolve().parents[1]
PATH=ROOT/'aws/ops/staged/ops_5975_etf_constituent_source_preflight.py'
ENDPOINT='https://api.polygon.io/etf-global/v1/constituents'


def actual(names,ns):
    tree=ast.parse(PATH.read_text(encoding='utf-8'))
    nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef)) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),str(PATH),'exec'),ns)
    return ns


class HoldingsProbe(unittest.TestCase):
    def test_provider_destination_and_query_allowlist(self):
        ns=actual(['checked_url'],{'urllib':urllib})
        good=ENDPOINT+'?composite_ticker=SPY&processed_date=2026-09-18&sort=constituent_rank.asc&limit=5000'
        self.assertEqual(ns['checked_url'](good),good)
        for bad in ('https://foreign.invalid/etf-global/v1/constituents?cursor=x',
                    ENDPOINT+'?apiKey=credential',ENDPOINT+'?cursor=x&cursor=y',
                    'https://api.polygon.io@foreign.invalid/etf-global/v1/constituents?cursor=x',
                    ENDPOINT+'?cursor=x#fragment'):
            with self.assertRaises(AssertionError):ns['checked_url'](bad)

    def test_exact_processed_snapshot_pagination_preserves_missing_tickers(self):
        row={'composite_ticker':'SPY','processed_date':'2026-09-18','effective_date':'2026-09-17',
             'constituent_ticker':'A','weight':Decimal('.1'),'constituent_rank':1}
        zero={**row,'constituent_ticker':None,'weight':Decimal('0'),'constituent_rank':2}
        following=ENDPOINT+'?cursor=next'
        responses=[{'status':'OK','results':[row]}, {'status':'OK','results':[row],'next_url':following},
                   {'status':'OK','results':[zero]}]
        seen=[]
        def request(s3,credential,url,budget):
            seen.append(url);doc=responses.pop(0)
            return doc,{'status':'retained','original':{'key':str(len(seen))},'acquired_at':'2026-09-21T06:00:00Z'}
        ns=actual(['checked_url','snapshot_probe'],{'urllib':urllib,'datetime':datetime,'timezone':timezone,
            'Counter':Counter,'Decimal':Decimal,'re':re,'ENDPOINT':ENDPOINT,'request_original':request})
        result=ns['snapshot_probe'](None,'fixture','SPY','2026-09-21',{})
        self.assertEqual(result['rows'],2);self.assertEqual(result['missing_ticker'],1)
        self.assertEqual(result['raw_weight_sum'],'0.1');self.assertFalse(result['weight_unit_certified'])
        self.assertIn('processed_date=2026-09-18',seen[1]);self.assertEqual(seen[2],following)
        self.assertEqual(result['effective_dates'],{'2026-09-17':2})

    def test_partial_pagination_is_not_called_complete(self):
        row={'composite_ticker':'SPY','processed_date':'2026-09-18'}
        responses=[({'results':[row]},{}),({'results':[row],'next_url':ENDPOINT+'?cursor=next'},{}),
                   (None,{'status':'provider_http_error','http_status':503})]
        ns=actual(['checked_url','snapshot_probe'],{'urllib':urllib,'re':re,'ENDPOINT':ENDPOINT,
            'request_original':lambda *a:responses.pop(0)})
        result=ns['snapshot_probe'](None,'fixture','SPY','2026-09-21',{})
        self.assertEqual(result['status'],'incomplete');self.assertNotIn('raw_weight_sum',result)

    def test_error_bodies_and_credentials_do_not_enter_retained_evidence(self):
        from unittest import mock
        saved=[]
        ns=actual(['checked_url','NoRedirect','bounded','request_original'],{'urllib':urllib,'json':json,
            'Decimal':Decimal,'datetime':datetime,'timezone':timezone,'retain':lambda s,r:saved.append(r) or {'key':'fixture'}})
        class Forbidden:
            def read(self,*a):raise AssertionError('Do not read provider error bodies')
            def close(self):pass
        error=urllib.error.HTTPError(ENDPOINT,403,'private',{},Forbidden())
        opener=mock.Mock();opener.open.side_effect=error
        with mock.patch.object(urllib.request,'build_opener',return_value=opener):
            body,result=ns['request_original'](None,'fixture-secret',ENDPOINT+'?cursor=x',{'requests':0,'bytes':0})
        self.assertIsNone(body);self.assertEqual(result,{'status':'provider_http_error','http_status':403});self.assertEqual(saved,[])
        opener.open.side_effect=None;opener.open.return_value=io.BytesIO(b'{"reflected":"fixture-secret"}')
        with mock.patch.object(urllib.request,'build_opener',return_value=opener):
            with self.assertRaises(AssertionError):ns['request_original'](None,'fixture-secret',ENDPOINT+'?cursor=x',{'requests':0,'bytes':0})
        self.assertEqual(saved,[])


if __name__=='__main__':unittest.main(verbosity=2)
