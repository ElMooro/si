from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import ast,copy,io,json,runpy,sys,types,unittest
R=Path(__file__).resolve().parents[4]
def run(packet,engine='bonds',legacy=False):
 class S3:
  class exceptions:NoSuchKey=KeyError
  def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(packet).encode()),'LastModified':datetime.now(timezone.utc)}
 fake=types.ModuleType('boto3');fake.client=lambda *a,**kw:S3()
 secret=types.ModuleType('managed_secret');secret.managed_secret=lambda *a,**kw:''
 source=R/'tests/fixtures/bond-trace-contract/pre512/aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py.txt' if legacy else R/'aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py'
 with patch.dict(sys.modules,{'boto3':fake,'managed_secret':secret,'anthropic_shim':types.ModuleType('anthropic_shim'),'_sentry_lite':types.ModuleType('_sentry_lite')}),patch('urllib.request.urlopen',side_effect=AssertionError('No network or model allowed')):
  mod=runpy.run_path(str(source));result=mod['fetch_engine'](engine,{'key':'data/bond-trace.json','fields':['regime','quality','calls_eligible']});prompt=mod['build_user_prompt']({result[0]:result[1]})
 return result,prompt
class Synthesis(unittest.TestCase):
 def test_legacy_and_candidate_bond_packets_never_gain_decision_authority(self):
  for packet in [{'regime':'CALM'},{'regime':'BOND_PANIC','calls_eligible':False,'quality':{'status':'unqualified'}},{'regime':'CALM','calls_eligible':True,'quality':{'status':'fresh'}}]:
   result,prompt=run(packet);self.assertIn('_error',result[1]);self.assertIn('BONDS — UNAVAILABLE',prompt);self.assertNotIn(packet['regime'],prompt)
 def test_other_contexts_unchanged(self):
  packet={'regime':'invented description','quality':{'status':'unqualified'}};candidate,prompt=run(packet,'invented_engine');previous,_=run(packet,'invented_engine',True);candidate[1].pop('_age_min');previous[1].pop('_age_min');self.assertEqual(candidate,previous)
 def test_only_fetch_engine_executable_ast_changes(self):
  def funcs(p):return {n.name:ast.dump(n,include_attributes=False) for n in ast.parse(p.read_bytes()).body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
  old=funcs(R/'tests/fixtures/bond-trace-contract/pre512/aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py.txt');new=funcs(R/'aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py');self.assertEqual(set(old),set(new));self.assertEqual([k for k in old if old[k]!=new[k]],['fetch_engine'])
if __name__=='__main__':unittest.main()
