"""Complete FI/FX migration conservation without running the native engine."""
from pathlib import Path
import ast,hashlib,io,json,re,sys,unittest,zipfile
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6136_fifx_original_baseline as baseline
class Storage:
    def __init__(self):self.objects={};self.puts=[]
    def put_object(self,**kw):self.objects[kw['Key']]=kw['Body'];self.puts.append(kw)
    def get_object(self,**kw):return {'Body':io.BytesIO(self.objects[kw['Key']])}
class Tests(unittest.TestCase):
    def test_actual_all_rates_fx_and_global_index_sources_are_listed(self):
        original=ROOT/'aws/lambdas/justhodl-fifx-vol-migration/tests/legacy_lambda_function.py.txt'
        self.assertEqual(hashlib.sha256(original.read_bytes()).hexdigest(),'8ad0b0f88379a4cf253741dcfab9d05890f95a729bb7082ebd9b84e4bb0e78eb')
        tree=ast.parse(original.read_text(encoding='utf-8'))
        constants={n.value for n in ast.walk(tree) if isinstance(n,ast.Constant) and isinstance(n.value,str)}
        self.assertEqual(set(baseline.SERIES),{'VIXCLS','DGS10','DEXUSEU','DEXJPUS','DEXUSUK','DTWEXBGS'})
        self.assertEqual(set(baseline.QUOTE_SYMBOLS),{s for s in constants if re.fullmatch(r'\^[A-Z0-9]+',s)}|{'000001.SS'})
        self.assertEqual(len(baseline.QUOTE_SYMBOLS),12)
        for sid in baseline.SERIES:self.assertIn(sid,constants)
        native=ast.parse((ROOT/'aws/lambdas/justhodl-fifx-vol-migration/source/fifx_catalog.py').read_text(encoding='utf-8'))
        native_constants={n.value for n in ast.walk(native) if isinstance(n,ast.Constant) and isinstance(n.value,str)}
        for sid in (*baseline.SERIES,*baseline.QUOTE_SYMBOLS):self.assertIn(sid,native_constants)
        for key in ('data/fifx-vol.json','data/fifx-vol-history.json','data/bond-vol.json'):self.assertIn(key,baseline.INPUTS)
    def test_whole_originals_and_empty_tracked_files_are_retained_conditionally(self):
        client=Storage();raw=b'original\x00'+b'x'*60000;ref=baseline.retain(client,raw)
        self.assertEqual(client.objects[ref['key']],raw);self.assertEqual(ref['sha256'],hashlib.sha256(raw).hexdigest())
        self.assertTrue(ref['key'].startswith(baseline.PRIVATE));self.assertEqual(client.puts[0]['IfNoneMatch'],'*');self.assertEqual(client.puts[0]['CacheControl'],'no-store')
        with self.assertRaises(ValueError):baseline.retain(client,b'')
        self.assertEqual(baseline.retain(client,b'',allow_empty=True)['bytes'],0)
    def test_deep_original_rows_dates_duplicates_and_missing_values_are_not_pruned(self):
        rows=[{'d':'2000-01-01','fi':None},{'d':'2000-01-01','fi':0},None]
        out=baseline.summarize(json.dumps({'rows':rows}).encode());self.assertEqual(out['rows'],3);self.assertEqual(out['unique_dates'],1)
        self.assertFalse(out['original_quote_verified']);self.assertFalse(out['point_in_time_qualified'])
        out=baseline.summarize(json.dumps({'legs':{'fixed_income':{'measure':'fallback','level':0}}}).encode())
        self.assertEqual(out['current_legs']['fixed_income']['level'],0);self.assertFalse(out['original_quote_verified'])
        self.assertEqual(baseline.summarize(b'not json')['status'],'whole_unparsed_predecessor')
    def test_actual_package_drift_is_evidence_not_release_verification(self):
        stream=io.BytesIO()
        with zipfile.ZipFile(stream,'w') as z:z.writestr('lambda_function.py',b'actual')
        out=baseline.package_inventory(stream.getvalue(),{'lambda_function.py':b'repo'})
        self.assertFalse(out['code_matches_repository']);self.assertFalse(out['release_verification']);self.assertFalse(out['runtime_member_resolution_verified'])
    def test_no_provider_acquisition_or_native_changes_and_all_consumers_preserved(self):
        text=Path(baseline.__file__).read_text(encoding='utf-8')
        for bad in ('.invoke(','.update_function_code(','.put_rule(','.update_schedule(','.get_parameter(','.get_secret_value(','query1.finance.yahoo.com','api.stlouisfed.org'):self.assertNotIn(bad,text)
        for required in ('sys.exit(1)','journal(s3, progress, True)',"assert privacy['all_denied']",'canonical.restore(packet, SERIES, read)',"'signal-board.html'","'fifx-vol-history.json'",'scheduled_qualified_packages','tracked_config_present'):self.assertIn(required,text)
if __name__=='__main__':unittest.main(verbosity=2)
