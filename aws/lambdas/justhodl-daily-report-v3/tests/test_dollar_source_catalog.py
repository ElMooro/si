"""Exercise the deployed canonical route with the five formerly absent inputs."""
from pathlib import Path
import ast,importlib,json,sys,unittest
ROOT=Path(__file__).resolve().parents[4];sys.path.insert(0,str(ROOT/'aws/shared'))
import dollar_source_catalog


class DollarCatalog(unittest.TestCase):
    def test_actual_handler_adds_only_reviewed_sources_preserving_all_existing_entries(self):
        source=(ROOT/'aws/lambdas/justhodl-daily-report-v3/source/lambda_function.py').read_text(encoding='utf-8')
        tree=ast.parse(source);node=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler'][-1]
        node.decorator_list=[];declared=next(n for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='FRED_SERIES' for t in n.targets))
        original=ast.literal_eval(declared.value);calls=[]
        ns={'json':json,'FRED_SERIES':original,'s3':object(),'S3_BUCKET':'b','FRED_KEY':'test-placeholder',
            'run_source_research':lambda client,bucket,catalog,key,**kw:calls.append(catalog) or {'published':True}}
        for item in tree.body:
            if isinstance(item,ast.ImportFrom):
                for alias in item.names:
                    if alias.asname and alias.asname.startswith('include_'):ns[alias.asname]=getattr(importlib.import_module(item.module),alias.name)
        exec(compile(ast.Module(body=[node],type_ignores=[]),'actual-dollar-source-route','exec'),ns)
        self.assertEqual(ns['lambda_handler']({'action':'research_measurements'},None)['statusCode'],200)
        actual=calls.pop();ns['include_dollar_series']=lambda x:x
        self.assertEqual(ns['lambda_handler']({'action':'research_measurements'},None)['statusCode'],200)
        previous=calls.pop();self.assertEqual({k:actual[k] for k in previous},previous)
        self.assertEqual(set(actual)-set(previous),set(dollar_source_catalog.SERIES))
        self.assertEqual(len(actual),len(previous)+5)
    def test_existing_definition_wins_and_input_is_not_mutated(self):
        catalog={'DEXUSAL':{'category':'reviewed','display_name':'Actual prior title'},'OTHER':{'extra':[1,2]}}
        before=json.loads(json.dumps(catalog));result=dollar_source_catalog.extend_catalog(catalog)
        self.assertEqual(catalog,before);self.assertEqual(result['DEXUSAL'],before['DEXUSAL'])
        self.assertEqual(result['OTHER'],before['OTHER'])

if __name__=='__main__':unittest.main(verbosity=2)
