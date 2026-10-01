from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import ast,contextlib,hashlib,importlib.util,io,json,sys,types,unittest
R=Path(__file__).resolve().parents[1]
KEYS=['data/short-interest-tickers.json','data/13f-positions.json','data/estimate-revisions-latest.json','data/rotation-chains.json']
s=importlib.util.spec_from_file_location('short_position_context',R/'aws/shared/short_position_context.py');sp=importlib.util.module_from_spec(s);s.loader.exec_module(sp)
NOW=datetime(2026,10,1,12,tzinfo=timezone.utc)
class Clock(datetime):
 @classmethod
 def now(cls,tz=None):return NOW
class Store:
 def __init__(self,name,cache):
  self.output='data/alpha-scoreboard-research.json' if name=='alpha' else 'data/opportunities-research.json';src='data/compound-signals.json' if name=='alpha' else 'data/opportunities.json'
  self.docs={src:{'compound':[{'symbol':'TEST','systems':[]}]} if name=='alpha' else {'all':[{'ticker':'TEST','verdict':'OPPORTUNITY'}]},self.output:cache,
   KEYS[0]:{'contract':'short-interest-tickers.v1','by_ticker':{'TEST':{'short_interest':0}}},KEYS[1]:{},KEYS[2]:{},KEYS[3]:{}};self.writes=[];self.reads=[]
 def get_object(self,**kw):
  self.reads.append(kw['Key']);assert kw['Key'] in self.docs;raw=json.dumps(self.docs[kw['Key']]).encode();return {'Body':io.BytesIO(raw),'ContentLength':len(raw)}
 def put_object(self,**kw):assert kw['Key']==self.output;self.writes.append(json.loads(kw['Body']));return {}

def run(name,cache):
 st=Store(name,cache);fake={'boto3':types.SimpleNamespace(client=lambda *a,**k:st,resource=lambda *a,**k:(_ for _ in ()).throw(AssertionError('No database'))),'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:None),'short_position_context':sp}
 ee=types.ModuleType('equity_enrich');m=types.ModuleType('whole_policy_'+name)
 with patch.dict(sys.modules,fake),patch('urllib.request.urlopen',side_effect=AssertionError('No provider')) as http,patch('socket.create_connection',side_effect=AssertionError('No socket')) as transport:
  exec(compile((R/'aws/shared/equity_enrich.py').read_bytes(),'ee','exec'),ee.__dict__)
  ee.fetch_peer_pe=lambda:({},{});ee.fetch_financials=lambda *a:{'financials':[]};ee.grade_track_record=lambda *a:None
  with patch.dict(sys.modules,{'equity_enrich':ee}):exec(compile((R/f'aws/lambdas/justhodl-{name}-research/source/lambda_function.py').read_bytes(),name,'exec'),m.__dict__)
  m.datetime=Clock;m.time=types.SimpleNamespace(time=lambda:100.0)
  if name=='alpha':m.compute_changes=lambda *a:{};m.log_signals=lambda *a:0
  with contextlib.redirect_stdout(io.StringIO()):out=m.lambda_handler({},None)
  http.assert_not_called();transport.assert_not_called()
 assert out['statusCode']==200 and len(st.writes)==1 and len(st.reads)==6
 return st.writes[0],ee

class Policy(unittest.TestCase):
 def test_whole_predecessors_exact_changes_and_unrelated_config_fields_preserved(self):
  manifest=json.loads((R/'tests/fixtures/no-paid-research/preservation.json').read_bytes())
  for source,entry in manifest['sources'].items():
   raw=(R/entry['file']).read_bytes();self.assertEqual(len(raw),entry['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),entry['sha256'])
   if source in manifest['edits']:
    text=(R/source).read_text(encoding='utf-8')
    for before,after in reversed(manifest['edits'][source]):self.assertEqual(text.count(after),1);text=text.replace(after,before)
    self.assertEqual(text,raw.decode('utf-8'))
   else:
    old=json.loads(raw);new=json.loads((R/source).read_bytes());self.assertEqual(new['environment_keys'],['FMP_KEY']);old['environment_keys'].remove('ANTHROPIC_API_KEY');self.assertEqual(new,old)

 def test_no_paid_or_free_transport_needed_for_status(self):
  for name in ('alpha','opportunities'):
   out,ee=run(name,{});self.assertEqual(out['narrative'],ee.narrative_status());self.assertEqual(out['new_theses'],0);self.assertEqual(out['narrative_statuses'],1);self.assertEqual(out['by_ticker']['TEST']['short_position_context']['short_interest_shares'],0)
 def test_cached_model_prose_never_republished_even_with_forged_version_or_future_time(self):
  for name in ('alpha','opportunities'):
   for stamp in (NOW.isoformat(),'2099-01-01T00:00:00Z','2000-01-01T00:00:00Z',None):
    for ver in ('alpha-1','opp-1','deterministic-unqualified-v1'):
     out,ee=run(name,{'by_ticker':{'TEST':{'thesis':'BUY_CANARY','bear':'MODEL_CANARY','thesis_at':stamp,'thesis_ver':ver}}});self.assertNotIn('CANARY',json.dumps(out));self.assertEqual(out['by_ticker']['TEST']['thesis'],ee.make_thesis(None,None,None,None,None)[0])
 def test_malformed_prior_cache_does_not_block_new_status(self):
  for name in ('alpha','opportunities'):
   for cache in (None,[],{'by_ticker':'malformed'},{'by_ticker':{'TEST':[1]}}):self.assertEqual(run(name,cache)[0]['narrative']['status'],'unqualified')
 def test_router_import_and_model_name_reference_absent_from_actual_shared_module(self):
  tree=ast.parse((R/'aws/shared/equity_enrich.py').read_text(encoding='utf-8'))
  self.assertFalse(any(isinstance(n,ast.ImportFrom) and n.module=='llm_router' for n in ast.walk(tree)));self.assertFalse(any(isinstance(n,ast.Name) and n.id=='complete' for n in ast.walk(tree)))
if __name__=='__main__':unittest.main(verbosity=2)
