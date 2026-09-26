"""Conserve whole engine/parsed archive without pretending it is the source XLS."""
from pathlib import Path
import ast,hashlib,io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_6130_term_premium_original_baseline as baseline

class Store:
    def __init__(self):self.objects={};self.puts=[]
    def put_object(self,**kw):self.puts.append(kw);self.objects[kw['Key']]=kw['Body']
    def get_object(self,**kw):return {'Body':io.BytesIO(self.objects[kw['Key']])}

class Tests(unittest.TestCase):
    def test_actual_output_and_archive_are_the_complete_explicit_baseline(self):
        path=ROOT/'aws/lambdas/justhodl-term-premium/tests/legacy_lambda_function.py.txt';tree=ast.parse(path.read_text(encoding='utf8'))
        pair=next(n for n in tree.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Tuple) and [x.id for x in n.targets[0].elts]==['OUT','HIST'])
        self.assertEqual(tuple(ast.literal_eval(pair.value)),baseline.INPUTS)
        parser=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='_parse_acm')
        want=next(n for n in ast.walk(parser) if isinstance(n,ast.Assign) and any(isinstance(x,ast.Name) and x.id=='want' for x in n.targets))
        self.assertEqual(set(ast.literal_eval(want.value).values()),{'ACMTP10','ACMTP05','ACMTP02','ACMY10','ACMRNY10'})

    def test_complete_binary_retention_is_private_conditional_and_read_back(self):
        client=Store();raw=b'whole predecessor\x00\r\n'+b'x'*50000;ref=baseline.retain(client,raw)
        self.assertEqual(client.objects[ref['key']],raw);self.assertEqual(ref['bytes'],len(raw))
        self.assertEqual(ref['sha256'],hashlib.sha256(raw).hexdigest());self.assertTrue(ref['key'].startswith(baseline.PRIVATE))
        self.assertEqual(client.puts[0]['IfNoneMatch'],'*');self.assertEqual(client.puts[0]['CacheControl'],'no-store')

    def test_real_empty_vendored_packaging_marker_is_preserved_but_missing_payload_is_rejected(self):
        raw=(ROOT/'aws/lambdas/justhodl-term-premium/source/xlrd-2.0.1.dist-info/REQUESTED').read_bytes()
        self.assertEqual(raw,b'');client=Store()
        with self.assertRaises(ValueError):baseline.retain(client,raw)
        ref=baseline.retain(client,raw,allow_empty=True)
        self.assertEqual(ref['bytes'],0);self.assertEqual(client.objects[ref['key']],raw)
        self.assertEqual(ref['sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(client.puts[-1]['IfNoneMatch'],'*')

    def test_rows_duplicates_nulls_and_unparsed_bytes_cannot_be_called_original_source(self):
        rows=[{'date':'2020-01-01','tp10':None},{'date':'2020-01-01','tp10':0},None]
        result=baseline.summarize(json.dumps(rows).encode());self.assertEqual(result['rows'],3)
        self.assertEqual(result['dated_rows'],2);self.assertEqual(result['unique_dates'],1);self.assertFalse(result['original_workbook_verified'])
        self.assertFalse(result['point_in_time_qualified'])
        for raw in (b'not json',b'null',b'{"latest":[]}'):
            self.assertFalse(baseline.summarize(raw)['original_workbook_verified'])

    def test_no_invocation_acquisition_or_schedule_mutation_and_claim_is_conditional(self):
        text=Path(baseline.__file__).read_text(encoding='utf8')
        for bad in ('.invoke(','.update_function_code(','.put_rule(','.update_schedule(','.get_parameter(','.get_secret_value(','newyorkfed.org'):
            self.assertNotIn(bad,text)
        self.assertIn('journal(s3, progress, True)',text);self.assertIn('sys.exit(1)',text)
        self.assertIn("assert privacy['all_denied']",text);self.assertIn('whole_zip',text)
        self.assertIn("'git','ls-files'",text)

if __name__=='__main__':unittest.main(verbosity=2)
