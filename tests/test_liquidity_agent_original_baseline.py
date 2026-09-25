"""Existing 72-identity coverage and byte-complete private baseline safeguards."""
from pathlib import Path
import ast,hashlib,io,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6121_liquidity_agent_original_baseline as baseline

class Store:
 def __init__(self):self.objects={};self.puts=[]
 def put_object(self,**request):self.puts.append(request);self.objects[request['Key']]=request['Body']
 def get_object(self,**request):return {'Body':io.BytesIO(self.objects[request['Key']])}

class Tests(unittest.TestCase):
 def test_every_predecessor_series_and_actual_output_path_are_preserved(self):
  path=ROOT/'aws/lambdas/justhodl-liquidity-agent/source/lambda_function.py'
  tree=ast.parse(path.read_text(encoding='utf-8'))
  values={n.targets[0].id:n.value for n in tree.body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name)}
  series=tuple(row[0] for row in ast.literal_eval(values['FRED_SERIES']))
  self.assertEqual(baseline.SERIES,series);self.assertEqual(len(series),72)
  self.assertIn(ast.literal_eval(values['S3_KEY']),baseline.INPUTS)
  self.assertEqual(len(series),len(set(series)))
 def test_retention_preserves_whole_bytes_and_never_writes_to_a_public_key(self):
  raw=b'\x00complete\r\noriginal\n';s3=Store();ref=baseline.retain(s3,raw)
  self.assertEqual(s3.objects[ref['key']],raw);self.assertEqual(ref['bytes'],len(raw))
  self.assertEqual(ref['sha256'],hashlib.sha256(raw).hexdigest())
  self.assertTrue(ref['key'].startswith('audit-private/20260909-originals/'))
  self.assertEqual(s3.puts[0]['IfNoneMatch'],'*')
  self.assertEqual(s3.puts[0]['CacheControl'],'no-store')
  with self.assertRaises(ValueError):baseline.retain(s3,b'')
  text=Path(baseline.__file__).read_text(encoding='utf-8')
  self.assertNotIn('.invoke(',text);self.assertNotIn('update_schedule(',text)
  self.assertNotIn('get_parameter(',text);self.assertIn('sys.exit(1)',text)

if __name__=='__main__':unittest.main(verbosity=2)
