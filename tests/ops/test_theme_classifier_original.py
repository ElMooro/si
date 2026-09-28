"""Synthetic original classification/cache defects; no provider or native I/O."""
from pathlib import Path
from datetime import datetime,timezone,timedelta
from typing import Dict,List,Optional
from types import SimpleNamespace
from io import BytesIO
import ast,json,hashlib,math,unittest
ROOT=Path(__file__).resolve().parents[2]
RAW=(ROOT/'tests/fixtures/pre-momentum-leaders-theme-classifier.py.txt').read_bytes();TREE=ast.parse(RAW)
AT=datetime(2026,9,28,7,tzinfo=timezone.utc)


class FixedDatetime(datetime):
    @classmethod
    def now(cls,tz=None):return AT


def original():
    ns={'Optional':Optional,'Dict':Dict,'List':List,'datetime':FixedDatetime,'timezone':timezone,'json':json,
        'PROFILE_CACHE_KEY':'synthetic-cache','PROFILE_CACHE_TTL_DAYS':7,'S3_BUCKET':'synthetic','FMP_KEY':'synthetic-only','MIN_TICKERS_FOR_THEME':3}
    nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in ('load_profile_cache','save_profile_cache','fetch_profile','derive_theme_label') or
           isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='THEME_ALIASES' for t in n.targets)]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated original classifier>','exec'),ns);return ns


def themes(tickers,scores,cache):
    ns=original();ns.update(leader_tickers=tickers,leader_scores=scores,cache=cache)
    h=next(n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
    start=next(i for i,n in enumerate(h.body) if isinstance(n,ast.AnnAssign) and isinstance(n.target,ast.Name) and n.target.id=='industry_buckets')
    end=next(i for i,n in enumerate(h.body) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='sorted_theme_keys' for t in n.targets))
    exec(compile(ast.Module(body=h.body[start:end],type_ignores=[]),'<synthetic grouping only>','exec'),ns)
    return ns['themes']


class Tests(unittest.TestCase):
    def test_full_original_matches_captured_actual_package(self):
        b=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['source_checks']['justhodl-theme-classifier']
        self.assertEqual(len(RAW),b['bytes']);self.assertEqual(hashlib.sha256(RAW).hexdigest(),b['sha256'])
    def test_future_cache_is_accepted(self):
        ns=original();profiles={'SYNA':{'industry':'synthetic'}}
        ns['load_s3_json']=lambda k:{'generated_at':(AT+timedelta(days=30)).isoformat(),'profiles':profiles}
        self.assertEqual(ns['load_profile_cache'](),profiles)
    def test_partial_eighth_day_is_mistaken_for_seven_day_cache(self):
        ns=original();profiles={'SYNA':{'industry':'synthetic'}}
        ns['load_s3_json']=lambda k:{'generated_at':(AT-timedelta(days=7,hours=23)).isoformat(),'profiles':profiles}
        self.assertEqual(ns['load_profile_cache'](),profiles)
    def test_rewriting_cache_rejuvenates_entries_without_per_profile_vintage(self):
        ns=original();writes=[];ns['s3']=SimpleNamespace(put_object=lambda **kw:writes.append(kw))
        profiles={'OLD':{'industry':'synthetic','fetched_at':'2020-01-01T00:00:00Z'},'NEW':{'industry':'synthetic'}}
        ns['save_profile_cache'](profiles);saved=json.loads(writes[0]['Body']);ns['load_s3_json']=lambda k:saved
        self.assertEqual(ns['load_profile_cache']()['OLD']['fetched_at'],'2020-01-01T00:00:00Z')
        self.assertEqual(saved['generated_at'],AT.isoformat())
    def test_unmatched_and_ambiguous_provider_rows_are_taken_as_requested_issuer(self):
        for data in ([{'symbol':'OTHER','industry':'wrong issuer'}],
                     [{'symbol':'OTHER','industry':'first wrong'},{'symbol':'SYNA','industry':'actual requested'}]):
            ns=original();body=json.dumps(data).encode()
            ns['urllib']=SimpleNamespace(request=SimpleNamespace(Request=lambda *a,**kw:None,urlopen=lambda *a,**kw:BytesIO(body)))
            self.assertEqual(ns['fetch_profile']('SYNA')['industry'],data[0]['industry'])
    def test_duplicate_single_issuer_forms_three_member_active_theme(self):
        result=themes(['SYNA']*3,{'SYNA':70},{'SYNA':{'industry':'Semiconductors'}})
        self.assertEqual(result['Semiconductors']['n_leaders'],3);self.assertTrue(result['Semiconductors']['is_active'])
        self.assertEqual(result['Semiconductors']['tickers'],['SYNA']*3)
    def test_nan_momentum_becomes_nonstandard_theme_score(self):
        tickers=['A','B','C'];result=themes(tickers,{t:float('nan') for t in tickers},{t:{'industry':'Semiconductors'} for t in tickers})
        self.assertTrue(math.isnan(result['Semiconductors']['avg_momentum']));self.assertIn('NaN',json.dumps(result))
    def test_shared_industry_is_called_active_without_any_comovement_observations(self):
        tickers=['A','B','C'];result=themes(tickers,{t:0 for t in tickers},{t:{'industry':'Semiconductors'} for t in tickers})
        self.assertTrue(result['Semiconductors']['is_active']);self.assertEqual(result['Semiconductors']['avg_momentum'],0)


if __name__=='__main__':unittest.main(verbosity=2)
