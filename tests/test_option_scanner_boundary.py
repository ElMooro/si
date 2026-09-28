"""Synthetic consumer reads only; no AWS imports, invocations or private outputs."""
from pathlib import Path
from io import BytesIO
from types import SimpleNamespace
from unittest.mock import patch
import ast,json,runpy,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
from option_scanner_boundary import KEY,guard,context


class Tests(unittest.TestCase):
    def test_named_boundary_abstains_even_on_forged_permissions_and_retains_other_families(self):
        payload={'calls_eligible':True,'sizing_eligible':True,'all_qualifying':[{'symbol':'TEST','tier':'TIER_A_BULLISH_FLOW','score':100}]}
        self.assertEqual(guard(KEY,payload),{});self.assertIs(guard('data/unrelated.json',payload),payload)
        self.assertFalse(context(payload)['calls_eligible']);self.assertEqual(context(payload)['independent_investment_votes'],0)
        self.assertEqual(len(payload['all_qualifying']),1)
    def test_best_ideas_does_not_fallback_to_raw_rows_or_award_a_vote(self):
        raw=json.dumps({'request_records':[{'ticker':'TEST'}],'unusual':[{'symbol':'TEST','score':100}],'all_qualifying':[{'symbol':'TEST','score':100}]}).encode();reads=[]
        store=SimpleNamespace(get_object=lambda **kw:(reads.append(kw['Key']) or {'Body':BytesIO(raw)}))
        with patch.dict(sys.modules,{'boto3':SimpleNamespace(client=lambda *a,**kw:store)}):
            ns=runpy.run_path(str(ROOT/'aws/lambdas/justhodl-best-ideas/source/lambda_function.py'))
        spec=next(s for s in ns['SPECS'] if s[0]=='optflow');result,reason=ns['harvest'](spec)
        self.assertEqual(result,{});self.assertIn('no_vote',reason);self.assertEqual(reads,[KEY])
    def test_both_confluence_handlers_do_not_create_scanner_names_or_votes(self):
        for name in ('flow-confluence','options-confluence'):
            writes=[];reads=[];store=SimpleNamespace(put_object=lambda **kw:writes.append(json.loads(kw['Body'])))
            with patch.dict(sys.modules,{'boto3':SimpleNamespace(client=lambda *a,**kw:store)}):
                ns=runpy.run_path(str(ROOT/'aws/lambdas'/('justhodl-'+name)/'source/lambda_function.py'))
            handler=ns['lambda_handler'];env=handler.__globals__
            def read(key):
                reads.append(key)
                if key==KEY:return {'measurement_contract':'options-flow-observations.v1','calls_eligible':True,'all_qualifying':[{'symbol':'TEST','tier':'TIER_A_BULLISH_FLOW'}],'unusual':[{'symbol':'TEST','score':100,'direction':'bullish'}]}
                return {}
            env['_read']=read;handler({},None)
            self.assertEqual(len(writes),1);self.assertNotIn('TEST',json.dumps(writes[0]));self.assertNotIn('data/flow-data.json',reads)
            self.assertEqual(writes[0]['options_scanner_exclusion']['independent_investment_votes'],0)
    def test_three_whole_original_consumer_sources_remain_hash_bound(self):
        import hashlib
        for name,digest in [('best-ideas','48c041f688796c194d965fe93dcdf25183cdb7694e5a38d6d03bbde7d49b35e0'),('flow-confluence','765149695ce6a970a4846260016efcb44d5c2e65db70fb1ef8644d113b2bc6cc'),('options-confluence','3c93837656a220ebefad01353f7b9d1511b3bbb057fd5878894cf8036df366a5')]:
            raw=(ROOT/'tests/fixtures'/('pre-options-flow-consumer-'+name+'.py.txt')).read_bytes();self.assertEqual(hashlib.sha256(raw).hexdigest(),digest)


if __name__=='__main__':unittest.main(verbosity=2)
