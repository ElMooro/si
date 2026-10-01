"""Whole actual monitor with invented packets and a source-owned overlay only."""
from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import ast,copy,importlib.util,json,sys,types,unittest
R=Path(__file__).resolve().parents[1];SOURCE=R/'aws/lambdas/justhodl-contract-gate/source';KEY='data/ai-website-synthesis.json'
sys.path[:0]=[str(SOURCE),str(R/'aws/shared')]
import reviewed_contracts as reviewed
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module);return module
kernel=load('website_status_kernel',R/'aws/lambdas/justhodl-ai-website-synthesis/source/research_status.py')
tree=ast.parse((R/'aws/lambdas/justhodl-ai-website-synthesis/source/lambda_function.py').read_text(encoding='utf-8'))
SPECS=ast.literal_eval(next(n.value for n in tree.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='ENGINE_INPUTS' for t in n.targets)))
ENTRY=reviewed.load_overlay()['entries'][KEY]
def packet():return kernel.compile_status(SPECS,{},'2026-10-01T15:00:00Z','a'*64)
def overlay():
 return reviewed.load_overlay()
class MemoryS3:
 def __init__(self):self.writes={};self.heads=[]
 def head_object(self,**kwargs):self.heads.append(kwargs['Key']);raise FileNotFoundError('invented absent artifact')
 def put_object(self,**kwargs):
  assert kwargs['Key'] in ('data/contract-violations.json','data/_state/rowcounts/20261001.json');self.writes[kwargs['Key']]=json.loads(kwargs['Body'])
class Gate(unittest.TestCase):
 def check_packet(self,p):
  client=MemoryS3();fake={'boto3':types.SimpleNamespace(client=lambda *a,**kw:client),'botocore.config':types.SimpleNamespace(Config=lambda **kw:object())}
  with patch.dict(sys.modules,fake):gate=load('whole_website_contract_gate',SOURCE/'lambda_function.py')
  registry={'contracts':{KEY:{'required_keys':['error','snapshots','generated_at','schema_version','status'],'max_age_hours':12},'data/unrelated.json':{'required_keys':['records'],'rows_path':['records'],'min_rows':3}}};prior=copy.deepcopy(registry)
  reads=[];source_overlay=overlay();at=datetime(2026,10,1,15,0,tzinfo=timezone.utc)
  def get(key):
   reads.append(key)
   if key==gate.CONTRACTS_KEY:return copy.deepcopy(registry)
   if key==KEY:
    if isinstance(p,Exception):raise p
    return copy.deepcopy(p)
   if key=='data/unrelated.json':return {'records':[]}
   raise AssertionError('Unapproved fixture read: '+key)
  gate.get_json=get;gate.get_json_raw=lambda key:b'{"exempt":{}}';gate.now=lambda:at
  gate.list_artifacts=lambda:[{'key':key,'size':100,'modified':at} for key in (KEY,'data/unrelated.json')]
  gate.load_overlay=lambda:copy.deepcopy(source_overlay)
  gate.apply_contracts=lambda reg:reviewed.apply_contracts(reg,source_overlay)
  result=gate.check();self.assertEqual(registry,prior)
  self.assertEqual(reads,[gate.CONTRACTS_KEY,KEY,'data/unrelated.json'])
  self.assertTrue(any(v['artifact']=='data/unrelated.json' and v['cls']=='ROW_COLLAPSE' for v in result['violations']))
  self.assertEqual(client.writes['data/contract-violations.json'],result)
  return [v for v in result['violations'] if v['artifact']==KEY]
 def test_zero_parsed_inputs_and_abstention_pass_structural_monitor(self):self.assertEqual(self.check_packet(packet()),[])
 def test_old_error_stub_fails_new_source_contract(self):
  violations=self.check_packet({'schema_version':'1.0','status':'error','snapshots':{},'error':'PRIVATE_CANARY','generated_at':'2026-10-01T15:00:00Z'})
  self.assertTrue(violations);self.assertNotIn('PRIVATE_CANARY',json.dumps(violations))
 def test_authority_wrong_contract_boolean_count_and_missing_null_field_fail(self):
  for key,value in [('calls_eligible',True),('sizing_eligible',True),('execution_eligible',True),('forecast_qualified',True),('contract','PRIVATE_CANARY'),('engines_total',True),('engines_loaded',False),('call','LONG')]:
   p=packet();p[key]=value
   with self.subTest(key=key):self.assertTrue(self.check_packet(p))
  p=packet();del p['call'];self.assertTrue(any(v['cls']=='MISSING_KEYS' for v in self.check_packet(p)))
 def test_fresh_publication_does_not_remove_explicit_unqualified_status(self):
  p=packet();p['input_status']['bonds']['reported_as_of']='2000-01-01';self.assertEqual(self.check_packet(p),[]);self.assertFalse(p['input_status']['bonds']['observation_freshness_verified']);self.assertEqual(p['status'],'unqualified')
 def test_read_error_stays_violation_without_error_contents(self):
  rows=self.check_packet(PermissionError('PRIVATE_CANARY'));self.assertEqual([r['cls'] for r in rows],['UNPARSEABLE']);self.assertNotIn('PRIVATE_CANARY',json.dumps(rows))
 def test_whole_registry_and_producer_overlay_change_only_named_entry(self):
  prior=json.loads((R/'tests/fixtures/website-research-status/pre-monitor-reviewed-contracts.json').read_bytes());after=overlay();entry=after['entries'].pop(KEY);self.assertEqual(after,prior);self.assertEqual(entry,ENTRY)
  original={'producers':{'data/unrelated.json':{'writers':['invented'],'readers':['keep']},KEY:{'writers':['legacy'],'readers':['widget'],'mentions':['keep']}}}
  updated=reviewed.apply_producers(original,overlay());self.assertEqual(updated['producers']['data/unrelated.json'],original['producers']['data/unrelated.json']);self.assertEqual(updated['producers'][KEY]['writers'],['justhodl-ai-website-synthesis']);self.assertEqual(updated['producers'][KEY]['readers'],['widget'])
if __name__=='__main__':unittest.main(verbosity=2)
