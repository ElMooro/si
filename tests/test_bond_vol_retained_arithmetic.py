"""Private qualification boundary, exact-byte reads, one request and pure replay."""
from pathlib import Path
import hashlib,io,json,sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/p) for p in ('aws/shared','aws/ops/staged','aws/ops/checks','scripts')]
import ops_6135_bond_vol_retained_arithmetic as operation

class Store:
    def __init__(self,raw):self.raw=raw;self.puts=[]
    def get_object(self,**kw):return {'Body':io.BytesIO(self.raw)}
    def put_object(self,**kw):self.puts.append(kw);self.raw=kw['Body']

class Tests(unittest.TestCase):
    def test_pinned_original_identity_rejects_tampering_and_public_refs(self):
        raw=b'whole original\x00';digest=hashlib.sha256(raw).hexdigest()
        ref={'key':operation.baseline.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}
        self.assertEqual(operation.read(Store(raw),ref),raw)
        with self.assertRaises(ValueError):operation.read(Store(raw+b'!'),ref)
        with self.assertRaises(ValueError):operation.read(Store(raw),{**ref,'key':'data/public.json'})

    def test_conditional_claim_and_full_readback(self):
        store=Store(b'');operation.journal(store,{'status':'claimed'},True)
        self.assertEqual(store.puts[0]['IfNoneMatch'],'*')
        self.assertTrue(store.puts[0]['Key'].startswith(operation.baseline.PRIVATE))
        self.assertEqual(store.puts[0]['CacheControl'],'no-store')

    def test_closure_includes_pinned_timezone_and_independent_verifier(self):
        paths=operation.compiler_paths()
        self.assertEqual(set(paths),{'bond_vol_candidate.py','bond_vol_catalog.py','bond_vol_timezone.py','verify_bond_vol_arithmetic.py',
            'canonical_fred_replay.py','report_observations.py','research_brief_model.py','evidence_store.py'})
        self.assertTrue(all(p.is_file() for p in paths.values()))

    def test_no_native_mutation_or_duplicate_quote_request(self):
        text=Path(operation.__file__).read_text(encoding='utf8')
        for bad in ('.invoke(','.update_function_code(','.update_schedule(','.put_rule(','.get_parameter(','.get_secret_value(','sendMessage'):
            self.assertNotIn(bad,text)
        self.assertIn('journal(s3,progress,True)',text);self.assertIn('sys.exit(1)',text)
        self.assertLess(text.index("status='quote_acquisition_attempted'"),text.index('try:quote_raw,receipt=acquire_quote()'))
        self.assertEqual(text.count('try:quote_raw,receipt=acquire_quote()'),1)
        self.assertIn("[sys.executable,'-I','-c',ISOLATED_REPLAY",text)
        self.assertIn("if rebuilt!=read(s3,output_ref) or rechecked!=read(s3,proof_ref)",text)

    def test_original_quote_metadata_and_redirect_boundary(self):
        class Response(io.BytesIO):
            headers={'Content-Type':'application/json','Set-Cookie':'never retain'};status=200
            def geturl(self):return operation.catalog.MOVE_URL
        with patch.object(operation.urllib.request,'urlopen',return_value=Response(b'{"original":"whole"}')) as call:
            raw,receipt=operation.acquire_quote()
        self.assertEqual(receipt['bytes'],len(raw));self.assertEqual(receipt['sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(receipt['headers'],{'Content-Type':'application/json'});self.assertEqual(call.call_count,1)
        class Redirect(Response):
            def geturl(self):return 'https://unreviewed.invalid/'
        with patch.object(operation.urllib.request,'urlopen',return_value=Redirect(b'body')):
            with self.assertRaises(ValueError):operation.acquire_quote()

if __name__=='__main__':unittest.main(verbosity=2)
