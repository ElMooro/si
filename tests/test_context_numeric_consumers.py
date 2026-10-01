from pathlib import Path
from unittest.mock import patch
import ast,hashlib,sys,types,unittest
R=Path(__file__).resolve().parents[1];D=R/'aws/shared'
sys.path.insert(0,str(R/'aws/shared'))
ctx=types.ModuleType('context_evidence_store');exec(compile((D/'context_evidence_store.py').read_bytes(),str(D/'context_evidence_store.py'),'exec'),ctx.__dict__)
MODULES=[('justhodl-apex-fusion','apex_evidence.py'),('justhodl-prepump-summary','summary_evidence.py'),('justhodl-pump-radar-brief','brief_evidence.py'),('justhodl-theme-cascade','cascade_evidence.py'),('justhodl-theme-cascade-backtest','snapshot_evidence.py')]
STAMP='2020-01-01T00:00:00Z'
def module(fn,file):
 p=R/'aws/lambdas'/fn/'source'/file;m=types.ModuleType('invented_context_consumer')
 with patch.dict(sys.modules,{'context_evidence_store':ctx}):exec(compile(p.read_bytes(),str(p),'exec'),m.__dict__)
 return m
def fixture(m,bad=None,all_bad=False):
 attempts={};sources={};first=next(iter(m.INPUTS))
 for name,key in m.INPUTS.items():
  raw=bad if bad is not None and (all_bad or name==first) else b'{"v":0.0}'
  ref=ctx.identity(raw,m.PRIVATE,'sources');sources[ref['key']]=raw
  attempts[name]={'source_key':key,'status':'received','requested_at':STAMP,'received_at':STAMP,'original_ref':ref,'content_encoding':''}
 return attempts,sources,first
class Consumers(unittest.TestCase):
 def test_five_actual_projections_retain_bad_original_and_do_not_count_it(self):
  for fn,file in MODULES:
   m=module(fn,file)
   for raw in (b'{"v":1e-1000}',b'{"v":0.100000000000000000001}'):
    with self.subTest(function=fn,raw=raw):
     attempts,sources,first=fixture(m,raw);before=dict(sources);out=m.build(attempts,sources,STAMP);row=next(r for r in out['sources'] if r['source']==first)
     self.assertEqual(row['status'],'invalid_json');self.assertEqual(row['original_ref'],attempts[first]['original_ref']);self.assertEqual(sources,before)
     self.assertEqual(out['coverage']['structured_objects'],len(m.INPUTS)-1);self.assertEqual(out['coverage']['eligible_votes'],0)
     for flag in ('calls_eligible','sizing_eligible','execution_eligible'):self.assertIs(out[flag],False)
 def test_all_lossy_inputs_preserve_prior_head_by_refusing_projection(self):
  for fn,file in MODULES:
   m=module(fn,file);attempts,sources,_=fixture(m,b'{"v":1e-1000}',True)
   with self.subTest(function=fn),self.assertRaises(ValueError):m.build(attempts,sources,STAMP)
 def test_genuine_zero_context_remains_a_structured_object(self):
  for fn,file in MODULES:
   m=module(fn,file);attempts,sources,_=fixture(m);out=m.build(attempts,sources,STAMP)
   self.assertEqual(out['coverage']['structured_objects'],len(m.INPUTS));self.assertEqual(out['coverage']['eligible_votes'],0)
 def test_website_metadata_retains_whole_identity_but_never_parses_lossy_packet(self):
  p=R/'aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py';tree=ast.parse(p.read_bytes());node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_metadata')
  ns={'context_evidence_store':ctx,'hashlib':hashlib};exec(compile(ast.Module(body=[node],type_ignores=[]),str(p),'exec'),ns)
  raw=b'{"v":1e-1000}';meta,packet=ns['_metadata'](raw);self.assertEqual(meta['read_status'],'malformed');self.assertEqual(meta['body_sha256'],hashlib.sha256(raw).hexdigest());self.assertIsNone(packet)
  meta,packet=ns['_metadata'](b'{"v":0.0}');self.assertEqual(meta['read_status'],'parsed');self.assertEqual(packet['v'],0.0)
if __name__=='__main__':unittest.main(verbosity=2)
