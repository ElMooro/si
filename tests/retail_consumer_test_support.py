"""Exercise actual retail read boundaries without invoking account-aware handlers."""
from pathlib import Path
from datetime import datetime,timezone
import ast,io,json,sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-retail-sentiment/tests')]
from native_retail_tests import packet,STAMP
import retail_research as adapter
CANARY={'market_regime':'MANIA','market_regime_signal':'BUY','top_30_by_mentions':[{'ticker':'TEST','velocity_pct':9999}],
 'ranked':{'biggest_velocity_surges':[{'ticker':'TEST','velocity_pct':9999}]},'biggest_velocity_surges':[{'ticker':'TEST','velocity_pct':9999}],'stocktwits_trending':[{'symbol':'TEST','trending_score':999}],'calls_eligible':True}
def source(fn):return (ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8')
def actual_function(fn,name,ns):
 tree=ast.parse(source(fn));node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name==name)
 exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-retail-consumer','exec'),ns);return ns[name]
class Boundaries(unittest.TestCase):
 def test_six_actual_direct_reads_refuse_legacy_directional_canaries(self):
  for fn,name,read in [('justhodl-ai-chat','rs','get_s3'),('justhodl-best-setups','retail','read_json'),('justhodl-cycle-clock','retail','load'),('justhodl-hot-stocks-digest','retail','_read'),('justhodl-prediction-snapshotter','retail','_read_json')]:
   line=next(l for l in source(fn).splitlines() if 'retail_research' in l and 'decision_view' in l)
   ns={read:lambda *a:CANARY};exec(textwrap.dedent(line),ns);self.assertIsNone(ns[name]['market_regime']);self.assertEqual(ns[name]['top_30_by_mentions'],[])
  line=next(l for l in source('justhodl-digest-trends-ai').splitlines() if 'retail_research' in l)
  got=eval('{'+line.strip()+'}',{'_read_json':lambda key:CANARY})['retail'];self.assertEqual(got['ranked'],{})
 def test_convergence_both_extractors_cannot_count_one_source_twice(self):
  class Client:
   def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(CANARY).encode()),'LastModified':datetime.now(timezone.utc)}
  fetch=actual_function('justhodl-convergence-radar','fetch_engine_raw',{'s3':Client(),'S3_BUCKET':'b','json':json,'datetime':datetime,'timezone':timezone})
  for key in ('top_30_by_mentions','stocktwits_trending'):
   got=fetch(key,{'key':'data/retail-sentiment.json','path':key});self.assertEqual(got[1],[])
 def test_regime_boundary_does_not_default_missing_attention_to_neutral(self):
  class Client:
   def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(CANARY).encode())}
  fetch=actual_function('justhodl-regime-composite','fetch_module',{'S3':Client(),'BUCKET':'b','json':json,'datetime':datetime,'timezone':timezone,'CISS_SERIES':{}})
  got=fetch({'key':'data/retail-sentiment.json','label':'Retail','emoji':'','dimension':'risk_on'})
  self.assertIsNone(got['polarity']);self.assertIs(got['vote_eligible'],False)
 def test_morning_dynamic_inventory_applies_actual_guard(self):
  line=next(l.strip()[len('return '):] for l in source('justhodl-morning-intelligence').splitlines() if 'retail_research' in l)
  got=eval(line,{'keys':{'retail':'data/retail-sentiment.json','other':'data/other.json'},'fs3':lambda key:CANARY})
  self.assertIsNone(got['retail']['market_regime']);self.assertEqual(got['other'],CANARY)
 def test_native_market_context_retains_samples_without_fabricating_observation_date(self):
  from extremes_native_test_support import synthesis_with
  _,_,p=packet();at=datetime.fromisoformat(STAMP)
  got=synthesis_with('retail',p,at,'market-extremes');self.assertTrue(got['eligibility']['retail']['research_context_available'])
  self.assertEqual(got['eligibility']['retail']['measurement_count'],0);self.assertEqual(got['decision']['eligible_votes'],0)
  self.assertEqual(got['contexts']['retail']['communities']['all-stocks']['eligible_symbols'],8)
  for bad in (CANARY,{},None):self.assertFalse(synthesis_with('retail',bad,at,'market-extremes')['eligibility']['retail']['research_context_available'])
 def test_other_feeds_untouched_by_guard(self):self.assertEqual(adapter.guard('data/other.json',CANARY),CANARY)
def run():
 result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Boundaries))
 if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
