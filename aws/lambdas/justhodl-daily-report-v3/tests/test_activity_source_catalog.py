"""The actual public research route extends the catalog without touching old definitions."""
from pathlib import Path
import ast,hashlib,importlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[4];sys.path.insert(0,str(ROOT/'aws/shared'))
import activity_source_catalog

class ActivityCatalog(unittest.TestCase):
    def test_real_curve_completion_preserves_every_existing_source_and_native_route(self):
        archive=ROOT/'tests/fixtures/legacy-daily-report-before-real-curve-completion.py.txt'
        raw=archive.read_bytes()
        self.assertEqual(len(raw),119694)
        self.assertEqual(hashlib.sha256(raw).hexdigest(),'347a7170e98aacd7a16c7536c99ad7c8bd3561ffc5732388eba50fecdd12da02')
        source=ROOT/'aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py'
        def catalog(text):
            return ast.literal_eval(next(n.value for n in ast.parse(text).body if isinstance(n,ast.Assign)
                and any(isinstance(t,ast.Name) and t.id=='FRED_SERIES' for t in n.targets)))
        before,after=catalog(raw.decode()),catalog(source.read_text(encoding='utf8'))
        self.assertEqual({k:after[k] for k in before},before)
        self.assertEqual(set(after)-set(before),{'DFII7','DFII20'})
        self.assertEqual(after['DFII7'],('treasury','7Y TIPS'));self.assertEqual(after['DFII20'],('treasury','20Y TIPS'))
        addition=b'\n    # Complete real-curve tenors already requested by the Yield Curve engine.\n    \'DFII7\':(\'treasury\',\'7Y TIPS\'), \'DFII20\':(\'treasury\',\'20Y TIPS\'),'
        self.assertEqual(source.read_bytes().replace(addition,b''),raw)
    def test_actual_research_handler_preserves_catalog_and_includes_reviewed_sources(self):
        source=(ROOT/'aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py').read_text(encoding='utf-8')
        tree=ast.parse(source);node=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler'][-1]
        node.decorator_list=[];calls=[]
        declared=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='FRED_SERIES' for t in n.targets))
        original=ast.literal_eval(declared.value)
        ns={'json':json,'FRED_SERIES':{**original,'EXISTING':('old_category','original_name')},'s3':object(),'S3_BUCKET':'b','FRED_KEY':'test-placeholder',
            'run_source_research':lambda client,bucket,catalog,key,**kw:calls.append(catalog) or {'published':True}}
        for item in tree.body:
            if isinstance(item,ast.ImportFrom):
                for alias in item.names:
                    if alias.asname and alias.asname.startswith('include_'):ns[alias.asname]=getattr(importlib.import_module(item.module),alias.name)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-research-route','exec'),ns)
        self.assertEqual(ns['lambda_handler']({'action':'research_measurements'},None)['statusCode'],200)
        self.assertEqual(len(calls),1);catalog=calls[0]
        self.assertEqual(catalog['EXISTING'],{'category':'old_category','display_name':'original_name'})
        self.assertTrue({'WEI','GDPNOW','RRSFS','ICSA','DFF','DFII7','DFII20'}<=set(catalog))
        old={'WEI':{'category':'reviewed','display_name':'Specific title'}}
        expanded=activity_source_catalog.extend_catalog(old)
        self.assertEqual(old,{'WEI':{'category':'reviewed','display_name':'Specific title'}})
        self.assertEqual(expanded['WEI'],old['WEI']);self.assertEqual(set(expanded),{'WEI','GDPNOW','RRSFS'})
