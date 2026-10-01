from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import ast,contextlib,copy,io,json,sys,types,unittest
from test_financial_missing_zero import fixture,run
R=Path(__file__).resolve().parents[1]
class Clock(datetime):
 @classmethod
 def now(cls,tz=None):return cls(2026,10,1,12,tzinfo=timezone.utc)
def whole(name,data,old=False):
 source='data/compound-signals.json' if name=='alpha' else 'data/opportunities.json';dest='data/alpha-scoreboard-research.json' if name=='alpha' else 'data/opportunities-research.json'
 docs={source:{'compound':[{'symbol':'TEST','systems':[]}]} if name=='alpha' else {'all':[{'ticker':'TEST','verdict':'OPPORTUNITY'}]},dest:{}}
 writes=[];reads=[]
 def get_object(**kw):
  assert kw['Key'] in docs;reads.append(kw['Key']);raw=json.dumps(docs[kw['Key']]).encode();return {'Body':io.BytesIO(raw),'ContentLength':len(raw)}
 def put_object(**kw):
  assert kw['Key']==dest;value=json.loads(kw['Body']);json.dumps(value,allow_nan=False);writes.append(value);return {}
 store=types.SimpleNamespace(get_object=get_object,put_object=put_object)
 ee=types.ModuleType('equity_enrich');tree=ast.parse((R/'aws/shared/equity_enrich.py').read_bytes());nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in ('narrative_status','make_thesis')];exec(compile(ast.Module(body=nodes,type_ignores=[]),'status','exec'),ee.__dict__)
 ee.fetch_financials=lambda *_:run(data,old);ee.fetch_peer_pe=lambda:({},{});ee.key_status=lambda:{'status':'invented'};ee.load_confirmation_feeds=lambda **kw:({},{},{},{},{});ee.grade_track_record=lambda *a,**k:None
 def deny(*a,**k):raise AssertionError('No network or other resource')
 fake={'equity_enrich':ee,'boto3':types.SimpleNamespace(client=lambda *a,**k:store,resource=deny)};module=types.ModuleType('whole_financial_'+name)
 with patch.dict(sys.modules,fake),patch('urllib.request.urlopen',deny),patch('socket.create_connection',deny):
  p=R/('aws/lambdas/justhodl-'+name+'-research/source/lambda_function.py');exec(compile(p.read_bytes(),str(p),'exec'),module.__dict__);module.datetime=Clock;module.time=types.SimpleNamespace(time=lambda:100.0)
  if name=='alpha':module.compute_changes=lambda *a:{};module.log_signals=lambda *a:0
  with contextlib.redirect_stdout(io.StringIO()):response=module.lambda_handler({},None)
 assert response['statusCode']==200 and len(writes)==1 and reads==[source,dest]
 return writes[0]
class Consumers(unittest.TestCase):
 def test_healthy_complete_published_packets_remain_identical(self):
  for name in ('alpha','opportunities'):self.assertEqual(whole(name,fixture()),whole(name,fixture(),True))
 def test_both_actual_publishers_keep_missing_financial_legs_unavailable(self):
  d=fixture();d['balance-sheet-statement'][0].pop('cashAndCashEquivalents')
  for name in ('alpha','opportunities'):
   with self.subTest(name=name):
    out=whole(name,d);row=out['by_ticker']['TEST'];self.assertIsNone(row['ev_ebitda']);self.assertIsNone(row['net_debt_ebitda']);self.assertEqual(out['narrative']['status'],'unqualified')
 def test_both_actual_publishers_retain_zero_eps_and_acquisition_share(self):
  d=fixture();d['income-statement'][0]['epsdiluted']=0;d['cash-flow-statement'][0]['acquisitionsNet']=0
  for name in ('alpha','opportunities'):
   with self.subTest(name=name):
    row=whole(name,d)['by_ticker']['TEST'];self.assertEqual(row['financials'][0]['eps'],0);self.assertEqual(row['acq_pct'],0)
if __name__=='__main__':unittest.main(verbosity=2)
