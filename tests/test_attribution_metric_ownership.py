"""Actual handlers: isolated market research publishers and private attribution projection."""
import contextlib
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[1]
class S3Error(Exception):
    def __init__(self,code): self.response={'Error':{'Code':code}};super().__init__(code)
class S3:
    def __init__(self,docs=None):self.docs=docs or {};self.reads=[];self.writes=[];self.denied=set()
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key in self.denied:raise S3Error('AccessDenied')
        if key not in self.docs:raise S3Error('NoSuchKey')
        return {'Body':io.BytesIO(json.dumps(self.docs[key]).encode())}
    def put_object(self,**kw):
        key=kw['Key'];self.writes.append(key);self.docs[key]=json.loads(kw['Body'])
def load(name,s3):
    path=ROOT/'aws/lambdas'/('justhodl-'+name)/'source/lambda_function.py'
    mods={'boto3':types.SimpleNamespace(client=lambda *a,**k:s3),'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:''),'anthropic_shim':types.ModuleType('anthropic_shim'),'_fred_shim':types.ModuleType('_fred_shim')}
    spec=importlib.util.spec_from_file_location('owned_'+name.replace('-','_'),path);mod=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,mods):spec.loader.exec_module(mod)
    mod.ANTHROPIC_KEY='offline-fixture';return mod
class Reply:
    def __enter__(self):return self
    def __exit__(self,*args):return False
    def read(self):return json.dumps({'content':[{'type':'text','text':json.dumps({'plumbing_health':{'grade':'B'},'complete':{'null':None,'zero':0}})}]}).encode()

class MetricOwnership(unittest.TestCase):
    def setUp(self):
        self.s3=S3({'data/ka-config.json':{'metrics':[],'categories':[],'version':2},'data/khalid-config.json':{'metrics':[],'categories':[],'version':99},'data/khalid-analysis.json':{'private_fixture':'untouched'},'data/khalid-metrics.json':{'original':True}})
        self.mod=load('ka-metrics',self.s3)
        self.quiet=contextlib.ExitStack();self.quiet.enter_context(contextlib.redirect_stdout(io.StringIO()));self.quiet.enter_context(contextlib.redirect_stderr(io.StringIO()))
    def tearDown(self):self.quiet.close()
    def test_actual_scheduled_handler_writes_ka_only_and_reads_own_config(self):
        with patch('urllib.request.urlopen',return_value=Reply()):result=self.mod.lambda_handler({},None)
        self.assertEqual(result['statusCode'],200);self.assertEqual(self.s3.writes,['data/ka-metrics.json','data/ka-analysis.json']);self.assertNotIn('data/khalid-config.json',self.s3.reads)
        doc=self.s3.docs['data/ka-analysis.json'];self.assertEqual(doc['engine'],'justhodl-ka-metrics');self.assertEqual(doc['input_artifact'],'data/ka-metrics.json');self.assertEqual(doc['input_generated'],self.s3.docs['data/ka-metrics.json']['generated']);self.assertEqual(doc['complete'],{'null':None,'zero':0});self.assertEqual(self.s3.docs['data/khalid-analysis.json'],{'private_fixture':'untouched'})
    def test_actual_get_endpoints_read_only_the_matching_engine(self):
        self.s3.docs['data/ka-analysis.json']={'own':True};self.s3.docs['data/ka-metrics.json']={'own_data':True}
        for route,key in [('/analysis','own'),('/data','own_data')]:
            response=self.mod.lambda_handler({'httpMethod':'GET','path':route},None);self.assertTrue(json.loads(response['body'])[key])
        self.assertEqual(self.s3.writes,[]);self.assertTrue(all('/ka-' in k for k in self.s3.reads))
    def test_actual_configuration_update_never_mutates_legacy_keys(self):
        response=self.mod.lambda_handler({'httpMethod':'PUT','body':json.dumps({'metrics':[],'categories':['test']})},None)
        self.assertEqual(response['statusCode'],200);self.assertEqual(self.s3.writes,['data/ka-config.json','data/ka-metrics.json']);self.assertEqual(self.s3.docs['data/khalid-config.json']['version'],99)
    def test_confirmed_missing_ka_config_uses_explicit_read_only_seed(self):
        del self.s3.docs['data/ka-config.json'];config=self.mod.load_config();self.assertEqual(config['version'],99);self.assertTrue(config['configuration_source']['legacy_read_only_fallback']);self.assertEqual(self.s3.writes,[])
        self.mod.save_config(config);self.assertEqual(self.s3.writes,['data/ka-config.json']);self.assertFalse(self.s3.docs['data/ka-config.json']['configuration_source']['legacy_read_only_fallback'])
    def test_access_denied_does_not_select_other_config_or_write(self):
        self.s3.denied.add('data/ka-config.json')
        with self.assertRaises(S3Error):self.mod.load_config()
        self.assertEqual(self.s3.reads,['data/ka-config.json']);self.assertEqual(self.s3.writes,[])
    def test_malformed_config_does_not_select_other_engine(self):
        self.s3.docs['data/ka-config.json']={'metrics':'invalid'}
        with self.assertRaises(ValueError):self.mod.load_config()
        self.assertEqual(self.s3.reads,['data/ka-config.json']);self.assertEqual(self.s3.writes,[])
    def test_llm_outage_preserves_original_generation_without_legacy_write(self):
        self.s3.docs['data/ka-analysis.json']={'generated':'2025-01-01T00:00:00Z','prior':'evidence'}
        with patch('urllib.request.urlopen',side_effect=RuntimeError('unavailable')):self.mod.run_ai_analysis({'metrics':[],'categories':[]},{'count':0})
        self.assertEqual(self.s3.writes,['data/ka-analysis.json']);doc=self.s3.docs['data/ka-analysis.json'];self.assertEqual(doc['generated'],'2025-01-01T00:00:00Z');self.assertEqual(doc['llm_status'],'unavailable');self.assertEqual(doc['prior'],'evidence')
    def test_actual_khalid_handler_preserves_own_namespace(self):
        mod=load('khalid-metrics',self.s3)
        with patch('urllib.request.urlopen',return_value=Reply()):response=mod.lambda_handler({},None)
        self.assertEqual(response['statusCode'],200);self.assertEqual(self.s3.writes,['data/khalid-metrics.json','data/khalid-analysis.json']);self.assertEqual(self.s3.docs['data/khalid-analysis.json']['engine'],'justhodl-khalid-metrics')

class PublicSourceOwnership(unittest.TestCase):
    def test_actual_handler_does_not_rewrite_landing_or_copy_private_prose(self):
        private='PRIVATE_CANARY_NOT_FOR_PUBLIC'
        raw={'generated_at':'2026-09-09T12:00:00Z','sources':{'FRED:DGS10':{'source':'U.S. Department of the Treasury '+private,'description':private,'updated':'2026-09-09T11:00:00Z'},'owner private symbol '+private:{'source':'Bureau of Labor Statistics '+private,'updated':private},'ECONOMICS:USCPI':{'source':'unrecognized Publisher '+private,'description':private,'updated':private}},'last_harvest_diag':{'done':0,'total':100,'rate_per_min':5,'private':private,'matched':private}}
        s3=S3({'data/tv-sources.json':raw,'data/macro-attribution.json':{'attribution':{'FRED:A':{'family':private,'publisher':private}},'by_publisher':[{'publisher':private}], 'coverage_pct':private}})
        mod=load('source-map',s3);response=mod.lambda_handler({},None)
        self.assertEqual(response['statusCode'],200);self.assertEqual(s3.writes,['data/source-map.json']);self.assertEqual(s3.docs['data/tv-sources.json'],raw)
        out=s3.docs['data/source-map.json'];self.assertNotIn(private,json.dumps(out));self.assertEqual(out['schema_version'],'public-source-map.v1');self.assertEqual(out['cleaned_sources']['FRED:DGS10']['source_family'],'US-TREASURY');self.assertEqual(out['withheld_symbol_count'],1);self.assertEqual(out['harvest_progress']['walked'],0);self.assertIsNone(out['harvest_progress']['matched']);self.assertIsNone(out['macro_coverage_pct'])
        self.assertNotIn('last_harvest_diag',out);self.assertNotIn('new_sources',out);self.assertEqual(out['publication']['contains_private_data'],False)
    def test_missing_input_explicitly_unavailable_and_only_own_output_written(self):
        s3=S3();mod=load('source-map',s3);mod.lambda_handler({},None);out=s3.docs['data/source-map.json'];self.assertEqual(out['input_status'],'UNAVAILABLE');self.assertEqual(out['errors'],['SOURCE_INPUT_UNAVAILABLE']);self.assertIsNone(out['harvest_progress']['pct']);self.assertEqual(s3.writes,['data/source-map.json'])
    def test_nonfinite_diagnostics_cannot_publish_invalid_json(self):
        s3=S3({'data/tv-sources.json':{'sources':{},'last_harvest_diag':{'done':float('nan'),'total':float('inf'),'matched':True}}});mod=load('source-map',s3);mod.lambda_handler({},None);out=s3.docs['data/source-map.json'];json.dumps(out,allow_nan=False);self.assertIsNone(out['harvest_progress']['walked']);self.assertIsNone(out['harvest_progress']['matched'])
    def test_every_public_source_retained_and_future_raw_fields_excluded(self):
        raw={'sources':{'FRED:SERIES'+str(i):{'source':'Federal Reserve','updated':'2026-09-09T00:00:00Z','private_note':'DO_NOT_COPY'} for i in range(62)}}
        s3=S3({'data/tv-sources.json':raw});load('source-map',s3).lambda_handler({},None);out=s3.docs['data/source-map.json'];self.assertEqual(len(out['cleaned_sources']),62);self.assertEqual(out['public_symbol_count'],62);self.assertNotIn('DO_NOT_COPY',json.dumps(out))

if __name__=='__main__':unittest.main()
