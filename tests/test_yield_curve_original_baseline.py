"""Conserve the predecessor's full input inventory without executing its code."""
from pathlib import Path
import ast,hashlib,io,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6127_yield_curve_original_baseline as baseline

class Store:
    def __init__(self):self.objects={};self.puts=[]
    def put_object(self,**kw):self.puts.append(kw);self.objects[kw['Key']]=kw['Body']
    def get_object(self,**kw):return {'Body':io.BytesIO(self.objects[kw['Key']])}

class Tests(unittest.TestCase):
    def test_every_literal_requested_identity_and_donor_are_retained(self):
        path=ROOT/'aws/lambdas/justhodl-yield-curve/source/lambda_function.py'
        tree=ast.parse(path.read_text(encoding='utf8'))
        names={n.targets[0].id:n.value for n in tree.body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name)}
        series=tuple(ast.literal_eval(row.elts[0]) for name in ('NOMINAL_TENORS','REAL_TENORS','BREAKEVENS','EXTRAS') for row in names[name].elts)
        self.assertEqual(series,baseline.SERIES);self.assertEqual(len(series),23)
        self.assertIn(ast.literal_eval(names['KEY']),baseline.INPUTS)
        for call in ast.walk(tree):
            if isinstance(call,ast.Call) and isinstance(call.func,ast.Attribute) and call.func.attr=='get_object':
                key=next(k.value for k in call.keywords if k.arg=='Key')
                self.assertIn(ast.literal_eval(key),baseline.INPUTS)
        self.assertNotIn('FEDFUNDS',series);self.assertIn('DFF',series)

    def test_complete_binary_retention_is_private_conditional_and_read_back(self):
        client=Store();raw=b'whole predecessor\x00\r\n'+b'x'*50000
        ref=baseline.retain(client,raw)
        self.assertEqual(ref['bytes'],len(raw));self.assertEqual(ref['sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(client.objects[ref['key']],raw)
        self.assertTrue(ref['key'].startswith(baseline.PRIVATE));self.assertEqual(client.puts[0]['IfNoneMatch'],'*')
        self.assertEqual(client.puts[0]['CacheControl'],'no-store')
        with self.assertRaises(ValueError):baseline.retain(client,b'')

    def test_baseline_never_invokes_or_acquires_and_has_single_claim(self):
        text=Path(baseline.__file__).read_text(encoding='utf8')
        for bad in ('.invoke(','.update_function_code(','.update_schedule(','.put_rule(','.get_parameter(','.get_secret_value(','api.stlouisfed.org'):
            self.assertNotIn(bad,text)
        self.assertIn('journal(s3, progress, True)',text);self.assertIn('sys.exit(1)',text)
        self.assertIn('canonical.restore(packet, SERIES, read)',text)
        self.assertIn('whole_zip',text);self.assertIn('privacy[\'all_denied\']',text)

if __name__=='__main__':unittest.main(verbosity=2)
