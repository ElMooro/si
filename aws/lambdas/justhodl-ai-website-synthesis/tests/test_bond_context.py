from pathlib import Path
import ast,json,sys,unittest
R=Path(__file__).resolve().parents[4];sys.path.insert(0,str(R/'tests'))
from synthesis_status_test_support import Memory,load

class Synthesis(unittest.TestCase):
 def test_legacy_and_candidate_bond_packets_never_gain_decision_authority(self):
  for packet in [{'regime':'CALM'},{'regime':'BOND_PANIC','calls_eligible':False,'quality':{'status':'unqualified'}},{'regime':'CALM','calls_eligible':True,'quality':{'status':'fresh'}}]:
   mem=Memory();mod,_=load(mem);mem.objects['data/bond-trace.json']=json.dumps(packet).encode();mod.s3=mem
   name,snap=mod.fetch_engine('bonds',mod.ENGINE_INPUTS['bonds']);prompt=mod.build_user_prompt({name:snap})
   self.assertIn('_error',snap);self.assertIn('BONDS — UNAVAILABLE',prompt);self.assertNotIn(packet['regime'],prompt);self.assertFalse(snap['calls_eligible'])
 def test_all_other_declared_contexts_remain_present_and_withhold_votes(self):
  mem=Memory();mod,_=load(mem);mod.s3=mem
  for name,spec in mod.ENGINE_INPUTS.items():
   mem.objects[spec['key']]=b'{"regime":"invented description","calls_eligible":true}'
   returned,snap=mod.fetch_engine(name,spec);self.assertEqual(returned,name);self.assertEqual(snap['_availability']['read_status'],'parsed');self.assertFalse(snap['calls_eligible']);self.assertNotIn('regime',snap)
 def test_predecessor_functions_preserved_as_explicit_safe_compatibility_entrypoints(self):
  old=ast.parse((R/'tests/fixtures/website-research-status/pre513/aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py.txt').read_bytes());new=ast.parse((R/'aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py').read_bytes())
  names=lambda tree:{n.name for n in tree.body if isinstance(n,(ast.FunctionDef,ast.ClassDef))}
  self.assertFalse(names(old)-names(new));mem=Memory();mod,_=load(mem)
  with self.assertRaises(RuntimeError):mod.call_anthropic('s','u')
  self.assertFalse(mod.send_telegram('invented'));self.assertEqual(mem.clients,[])
if __name__=='__main__':unittest.main()
