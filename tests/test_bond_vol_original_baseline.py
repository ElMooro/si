"""The conservation probe must cover the actual full predecessor, without executing it."""
from pathlib import Path
import ast,hashlib,io,json,sys,unittest,zipfile
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6133_bond_vol_original_baseline as baseline

class Store:
    def __init__(self):self.objects={};self.puts=[]
    def put_object(self,**kw):self.puts.append(kw);self.objects[kw['Key']]=kw['Body']
    def get_object(self,**kw):return {'Body':io.BytesIO(self.objects[kw['Key']])}

class Tests(unittest.TestCase):
    def test_actual_ten_channels_history_and_funding_donor_are_preserved(self):
        path=ROOT/'aws/lambdas/justhodl-bond-vol/source/lambda_function.py'
        tree=ast.parse(path.read_text(encoding='utf8'))
        names={n.targets[0].id:n.value for n in tree.body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name)}
        channels=ast.literal_eval(names['CHANNELS'])
        self.assertEqual(tuple(ch['fred'] for ch in channels),baseline.SERIES);self.assertEqual(len(channels),10)
        self.assertIn(ast.literal_eval(names['S3_KEY']),baseline.INPUTS)
        for call in ast.walk(tree):
            if isinstance(call,ast.Call) and isinstance(call.func,ast.Attribute) and call.func.attr=='get_object':
                key=next(k.value for k in call.keywords if k.arg=='Key')
                self.assertIn(ast.literal_eval(key),baseline.INPUTS)
        self.assertIn('data/funding-plumbing.json',baseline.INPUTS)

    def test_whole_binary_bytes_conditional_readback_and_tracked_empty_markers(self):
        client=Store();raw=b'whole predecessor\x00\r\n'+b'x'*50000;ref=baseline.retain(client,raw)
        self.assertEqual(client.objects[ref['key']],raw);self.assertEqual(ref['bytes'],len(raw))
        self.assertEqual(ref['sha256'],hashlib.sha256(raw).hexdigest());self.assertTrue(ref['key'].startswith(baseline.PRIVATE))
        self.assertEqual(client.puts[0]['IfNoneMatch'],'*');self.assertEqual(client.puts[0]['CacheControl'],'no-store')
        with self.assertRaises(ValueError):baseline.retain(client,b'')
        self.assertEqual(baseline.retain(client,b'',allow_empty=True)['bytes'],0)

    def test_duplicate_null_and_unparsed_history_stays_conserved_not_qualified(self):
        rows=[{'date':'2020-01-01','z':None},{'date':'2020-01-01','z':0},None]
        result=baseline.summarize(json.dumps({'points':rows}).encode())
        self.assertEqual(result['rows'],3);self.assertEqual(result['dated_rows'],2);self.assertEqual(result['unique_dates'],1)
        self.assertFalse(result['original_quote_verified']);self.assertFalse(result['point_in_time_qualified'])
        for raw in (b'not json',b'null',b'{"move":[]}'):
            self.assertFalse(baseline.summarize(raw)['original_quote_verified'])

    def test_proxy_and_real_parsed_quotes_both_need_original_source_evidence(self):
        for proxy,value in ((True,0),(False,0),(True,None),(False,100.25)):
            out=baseline.summarize(json.dumps({'move':{'is_proxy':proxy,'value':value,'spark':[None]}}).encode())
            self.assertEqual(out['move']['is_proxy'],proxy);self.assertEqual(out['move']['value'],value)
            self.assertFalse(out['move']['original_quote_verified'])

    def test_no_native_actions_and_all_tracked_source_files_are_conserved(self):
        text=Path(baseline.__file__).read_text(encoding='utf8')
        for bad in ('.invoke(','.update_function_code(','.put_rule(','.update_schedule(','.get_parameter(','.get_secret_value(','api.stlouisfed.org','query1.finance.yahoo.com'):
            self.assertNotIn(bad,text)
        self.assertIn('journal(s3, progress, True)',text);self.assertIn('sys.exit(1)',text)
        self.assertIn("assert privacy['all_denied']",text);self.assertIn('whole_zip',text)
        self.assertIn("'git','ls-files'",text);self.assertIn('canonical.restore(packet, SERIES, read)',text)

    def test_actual_drift_is_preserved_without_a_false_release_claim(self):
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as archive:
            archive.writestr('lambda_function.py',b'real handler')
            archive.writestr('_fred_shim.py',b'old deployed shim')
            archive.writestr('extra.txt',b'')
        inventory=baseline.package_inventory(stream.getvalue(),{'lambda_function.py':b'real handler','_fred_shim.py':b'repo shim','missing.py':b'missing'})
        self.assertFalse(inventory['code_matches_repository']);self.assertFalse(inventory['release_verification'])
        self.assertEqual(inventory['source_differences']['_fred_shim.py']['status'],'different_bytes')
        self.assertEqual(inventory['source_differences']['missing.py']['status'],'missing_from_package')
        self.assertEqual(inventory['complete_zip_inventory']['extra.txt']['bytes'],0)
        self.assertEqual(inventory['additional_packaged_files'],['extra.txt'])

    def test_matching_bytes_still_only_certify_baseline_conservation(self):
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as archive:archive.writestr('lambda_function.py',b'handler')
        out=baseline.package_inventory(stream.getvalue(),{'lambda_function.py':b'handler'})
        self.assertTrue(out['code_matches_repository']);self.assertFalse(out['release_verification'])
        self.assertEqual(out['source_differences'],{})

if __name__=='__main__':unittest.main(verbosity=2)
