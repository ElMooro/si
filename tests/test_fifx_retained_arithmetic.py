"""Private qualification is bounded, replayable and cannot mutate native code."""
from pathlib import Path
import hashlib, io, json, sys, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops/staged','aws/ops/checks','scripts')]
import ops_6138_fifx_retained_arithmetic as operation

class Store:
    def __init__(self,raw=b''):self.raw=raw;self.puts=[]
    def get_object(self,**kw):return {'Body':io.BytesIO(self.raw)}
    def put_object(self,**kw):self.puts.append(kw);self.raw=kw['Body']

class Tests(unittest.TestCase):
    def test_exact_private_original_read(self):
        raw=b'whole source\x00';digest=hashlib.sha256(raw).hexdigest()
        ref={'key':operation.baseline.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}
        self.assertEqual(operation.read(Store(raw),ref),raw)
        with self.assertRaises(ValueError):operation.read(Store(raw+b'!'),ref)
        with self.assertRaises(ValueError):operation.read(Store(raw),{**ref,'key':'data/current.json'})

    def test_request_claim_precedes_work_and_cannot_overwrite_existing_claim(self):
        store=Store();operation.journal(store,{'status':'claimed'},True)
        self.assertEqual(store.puts[0]['IfNoneMatch'],'*')
        self.assertTrue(store.puts[0]['Key'].startswith(operation.baseline.PRIVATE))
        source=Path(operation.__file__).read_text(encoding='utf8')
        self.assertLess(source.index('journal(s3,progress,True)'),source.index('old=json.loads'))

    def test_complete_five_module_closure_includes_parser_and_timezones(self):
        self.assertEqual(set(operation.compiler_paths()),{'fifx_candidate.py','fifx_catalog.py','fifx_originals.py',
            'fifx_timezones.py','verify_fifx_arithmetic.py'})
        self.assertTrue(all(p.is_file() for p in operation.compiler_paths().values()))

    def test_no_acquisition_native_invocation_or_mutable_publication(self):
        text=Path(operation.__file__).read_text(encoding='utf8')
        for bad in ('.invoke(','.update_function_code(','.update_schedule(','.put_rule(','.get_parameter(','.get_secret_value(',
                    'sendMessage','urlopen(','.acquire(','create_schedule('):self.assertNotIn(bad,text)
        self.assertIn('sys.exit(1)',text)
        self.assertIn("[sys.executable,'-I','-c',ISOLATED",text)
        self.assertIn('if rebuilt!=read(s3,proof_ref)',text)
        self.assertIn("set(captured['sources'])!=set(catalog.SOURCES)",text)
        self.assertIn("if sid not in ('^MOVE','^VHSI') and not proof['current_available']",text)

if __name__=='__main__':unittest.main(verbosity=2)
