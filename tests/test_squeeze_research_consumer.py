"""Actual downstream source boundaries with an invented new-contract output.

This checks compatibility/no donated squeeze votes only. It does not qualify
other feed inputs, cached historical packets or the consumers' investment model.
"""
from pathlib import Path
from unittest.mock import Mock
from io import BytesIO
import ast,importlib.util,json,sys,textwrap,unittest
R=Path(__file__).resolve().parents[1];W=R;sys.path.insert(0,str(R/'aws/shared'))
import context_evidence_store as store
spec=importlib.util.spec_from_file_location('candidate',R/'aws/lambdas/justhodl-squeeze-pretrigger/source/squeeze_research_model.py');model=importlib.util.module_from_spec(spec);spec.loader.exec_module(model)
def packet():
 attempts={k:{'source_key':v,'status':'source_read_unavailable'} for k,v in model.INPUTS.items()}
 return model.project(attempts,{},'2026-10-01T18:00:00Z',store.validate_ref,'audit-private/20260909-originals/squeeze-pretrigger-research/')
def source(name):return (R/'aws/lambdas'/('justhodl-'+name)/'source/lambda_function.py').read_text(encoding='utf-8')
def block(name,start,end,scope):
 text=source(name);a=text.index(start);b=text.index(end,a+len(start));exec(textwrap.dedent(text[a:b]),scope)
def functions(name,names,scope):
 nodes=[n for n in ast.parse(source(name)).body if isinstance(n,ast.FunctionDef) and n.name in names];assert len(nodes)==len(names)
 exec(compile(ast.Module(body=nodes,type_ignores=[]),name,'exec'),scope)
class Consumers(unittest.TestCase):
 def test_flow_confluence_receives_no_squeeze_rows(self):
  add=Mock();scope={'_read':lambda key:packet(),'add':add,'_tk':lambda r:r['ticker']}
  block('flow-confluence','    sq = _read("data/squeeze-pretrigger.json")','    for it in (_read("data/insider-clusters.json")',scope)
  self.assertEqual(add.call_count,0)
 def test_options_confluence_receives_no_squeeze_rows(self):
  add=Mock();scope={'_read':lambda key:packet(),'add':add,'_tk':lambda r:r['ticker']}
  block('options-confluence','    sq = _read("data/squeeze-pretrigger.json")','    ivc = _read("data/earnings-iv-crush.json")',scope)
  self.assertEqual(add.call_count,0)
 def test_boom_radar_receives_no_squeeze_dimension(self):
  scope={'getj':lambda key:packet(),'dims':{'SQUEEZE':{}},'_tk':lambda r:r['ticker']}
  block('boom-radar','    sq = getj("data/squeeze-pretrigger.json")','    oc = getj("data/options-confluence.json")',scope)
  self.assertEqual(scope['dims'],{'SQUEEZE':{}});self.assertIsNone(scope['squeeze_regime_strength']);self.assertEqual(scope['squeeze_state'],'UNQUALIFIED')
 def test_best_ideas_cannot_harvest_nested_descriptive_context_as_votes(self):
  client=Mock();p=packet();p['short_position_context']['by_ticker']={'TEST':{'short_interest_shares':100,'calls_eligible':False}}
  client.get_object.return_value={'Body':BytesIO(json.dumps(p).encode())}
  scope={'s3':client,'S3_BUCKET':'invented','json':json,'momentum_guard':lambda key,value:value}
  functions('best-ideas',{'harvest','dig','num'},scope)
  result=scope['harvest'](('squeeze','Squeeze Pre-Trigger','data/squeeze-pretrigger.json',['imminent_setups'],'symbol','score','RISK','invented',25))
  self.assertEqual(result[0],{});self.assertEqual(client.get_object.call_args.kwargs['Key'],'data/squeeze-pretrigger.json')
 def test_alert_router_has_no_squeeze_message_or_state_mark(self):
  mark=Mock();scope={'List':list,'_read_json':lambda key:packet(),'_mark_alerted':mark,'_is_new':lambda *args:True}
  functions('prepump-alerts-router',{'check_squeeze_pretrigger'},scope)
  state={};self.assertEqual(scope['check_squeeze_pretrigger'](state),[]);self.assertEqual(state,{});self.assertEqual(mark.call_count,0)
if __name__=='__main__':unittest.main(verbosity=2)
