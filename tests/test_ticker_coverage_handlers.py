from pathlib import Path
from datetime import datetime,timezone
from unittest.mock import patch
import contextlib,copy,hashlib,importlib.util,io,json,sys,types,unittest
R=Path(__file__).resolve().parents[1];ARCHIVE=R/'tests/fixtures/ticker-coverage';sys.path[:0]=[str(R/'aws/shared'),str(R/'aws/shared/tests')]
s=importlib.util.spec_from_file_location('ticker_coverage_context',R/'aws/shared/ticker_coverage_context.py');helper=importlib.util.module_from_spec(s);s.loader.exec_module(helper)

class Clock(datetime):
 @classmethod
 def now(cls,tz=None):return datetime(2026,10,1,12,tzinfo=timezone.utc)

class Storage:
 def __init__(self,packet):self.packet=packet;self.writes=[];self.streams=[]
 def get_object(self,**kw):
  key=kw['Key']
  if key==helper.SOURCE:value=self.packet
  elif key=='data/insider-clusters.json':value={'clusters':[{'ticker':'KO','n_insiders':3}]}
  elif key=='data/buyback-yield-ranking.json':value={'top_20_ranked':[{'ticker':'KO','ttm_buyback_yield_net_pct':2}]}
  else:value={}
  if isinstance(value,Exception):raise value
  raw=value if isinstance(value,bytes) else json.dumps(value).encode();stream=io.BytesIO(raw)
  if key==helper.SOURCE:self.streams.append(stream)
  return {'Body':stream,'ContentLength':len(raw)}
 def put_object(self,**kw):self.writes.append(json.loads(kw['Body']));return {}

def packet(count=3):return {'contract':'ticker-360.v1','tickers':{'KO':{'coverage_count':count,'domains':{d:{'data':{'market_wide':True}} for d in ['macro-regime','dollar','fx']}}}}

def run(name,value,candidate=True):
 client=Storage(value);fake={'boto3':types.SimpleNamespace(client=lambda *a,**k:client),'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:None),'ticker_coverage_context':helper}
 path=R/f'aws/lambdas/justhodl-{name}/source/lambda_function.py' if candidate else ARCHIVE/(name+'.py.txt');m=types.ModuleType('whole_'+name)
 with patch.dict(sys.modules,fake),patch('urllib.request.urlopen',side_effect=AssertionError('No network')) as http,contextlib.redirect_stdout(io.StringIO()):
  exec(compile(path.read_bytes(),str(path),'exec'),m.__dict__)
  m.time=types.SimpleNamespace(time=lambda:100.0);m.datetime=Clock
  if name=='best-ideas':
   choices=[];families=set()
   for spec in m.SPECS:
    if spec[6] not in families:choices.append(spec);families.add(spec[6])
    if len(choices)==2:break
   m.SPECS=choices;m.harvest=lambda *a:({'KO':0.4},'invented');m.regime_haircut=lambda:(1,None,None,None);m.forensic_flags=lambda:(set(),{})
  m.lambda_handler({},None);http.assert_not_called()
 assert len(client.writes)==1
 if candidate:assert all(stream.closed for stream in client.streams)
 return client.writes[0]

class Whole(unittest.TestCase):
 def test_whole_predecessor_and_exact_allowed_edits_are_retained(self):
  manifest=json.loads((ARCHIVE/'preservation.json').read_bytes())
  for path,meta in manifest['sources'].items():
   raw=(R/meta['file']).read_bytes();self.assertEqual(len(raw),meta['bytes']);self.assertEqual(hashlib.sha256(raw).hexdigest(),meta['sha256'])
   lines=raw.decode('utf-8').splitlines(keepends=True)
   for edit in reversed(manifest['edits'][path]):
    i=edit['start_line'];n=len(edit['old'].splitlines(keepends=True));self.assertEqual(''.join(lines[i:i+n]),edit['old']);lines[i:i+n]=edit['new'].splitlines(keepends=True)
   expected=''.join(lines)
   if path=='tests/deployment/test_options_complete_closure.py':
    # The later Options release extended this gate. Retain both complete
    # predecessors and apply only its already-reviewed pair of exact edits.
    later=R/'tests/fixtures/crypto-funding-archive'
    plan=json.loads((later/'edits.json').read_bytes())[path]
    self.assertEqual(expected.encode(),(later/('before/'+path+'.txt')).read_bytes())
    self.assertEqual(hashlib.sha256(expected.encode()).hexdigest(),plan['predecessor_sha256'])
    for before,after in plan['edits']:
     self.assertEqual(expected.count(before),1,path);expected=expected.replace(before,after)
    self.assertEqual(hashlib.sha256(expected.encode()).hexdigest(),plan['candidate_sha256'])
   self.assertEqual(expected.encode(),(R/path).read_bytes(),path)

 def test_whole_healthy_output_only_changes_diagnostic_context_and_cross_validation(self):
  def clean(v):
   if isinstance(v,dict):return {k:([] if k=='cross_validated_tickers' else 0 if k=='cross_validated' else clean(x)) for k,x in v.items() if k not in ('ticker_360_context','cross_validation_status','t360_context','version','schema_version','headline','how_to_read','disclaimer')}
   if isinstance(v,list):return [clean(x) for x in v]
   return v
  for name in ('flow-confluence','best-ideas'):self.assertEqual(clean(run(name,packet())),clean(run(name,packet(),False)))
 def test_coverage_and_tier_copy_does_not_claim_independent_validation(self):
  out=run('best-ideas',packet())
  self.assertIn('Independence and predictive performance are unverified.',out['headline'])
  self.assertIn('their count does not establish independent evidence',out['how_to_read'])
  self.assertIn('not calibrated probabilities',out['how_to_read'])
  self.assertIn('not validated forecasts or portfolio sizing guidance',out['disclaimer'])
 def test_invalid_or_mismatched_duplicate_cannot_hide_ambiguity(self):
  for duplicate in ([1],{'ticker':'IBM'},{'domains':{},'coverage_count':0}):
   value=packet();value['tickers']['ko']=duplicate
   for name in ('flow-confluence','best-ideas'):
    out=run(name,value);row=out['ticker_map']['KO'] if name=='flow-confluence' else out['stack'][0]
    self.assertIsNone(row['t360_coverage']);self.assertEqual(len(out['ticker_360_context']['ambiguous_symbols'][0]['occurrences']),2)
 def test_reported_error_is_not_usable_context(self):
  value=packet();value['error']='PRIVATE_CANARY'
  for name in ('flow-confluence','best-ideas'):
   out=run(name,value);self.assertEqual(out['ticker_360_context']['status'],'unavailable');self.assertNotIn('PRIVATE_CANARY',json.dumps(out))
 def test_original_cross_validation_can_be_manufactured_by_market_context_only(self):
  old=run('flow-confluence',packet(),False);new=run('flow-confluence',packet())
  self.assertEqual(old['cross_validated_tickers'],['KO']);self.assertEqual(new['cross_validated_tickers'],[])
  self.assertEqual(new['cross_validation_status'],'unqualified');self.assertEqual(new['ticker_map']['KO']['score'],old['ticker_map']['KO']['score']);self.assertEqual(new['ticker_map']['KO']['n_engines'],2)
 def test_original_malformed_row_crashes_and_new_whole_handler_still_publishes(self):
  for name in ('flow-confluence','best-ideas'):
   for value in [{'contract':'ticker-360.v1','tickers':[1]}, {'contract':'ticker-360.v1','tickers':{'KO':[1]}}, {'contract':'ticker-360.v1','tickers':{'KO':{'domains':[1]}}}]:
    with self.subTest(name=name,value=value):
     with self.assertRaises(AttributeError):run(name,value,False)
     result=run(name,value);self.assertEqual(result.get('n_total',result.get('counts',{}).get('names')),1)
 def test_unknown_and_ambiguous_maps_cannot_select_context_winner(self):
  value=packet();value['tickers']['ko']=copy.deepcopy(value['tickers']['KO'])
  for name in ('flow-confluence','best-ideas'):
   result=run(name,value);row=result['ticker_map']['KO'] if name=='flow-confluence' else result['stack'][0];self.assertIsNone(row['t360_coverage']);self.assertEqual(row['t360_domains'],[]);self.assertEqual(len(result['ticker_360_context']['ambiguous_symbols'][0]['occurrences']),2)
 def test_missing_is_not_zero_and_valid_zero_is_preserved(self):
  for name in ('flow-confluence','best-ideas'):
   for value,count in [({},None),({'contract':'ticker-360.v1','tickers':{'KO':{'coverage_count':0,'domains':{}}}},0)]:
    result=run(name,value);row=result['ticker_map']['KO'] if name=='flow-confluence' else result['stack'][0];self.assertEqual(row['t360_coverage'],count)
 def test_unreconciled_boolean_and_nonfinite_counts_cannot_vote(self):
  for count in (True,-1,2,4,'3',float('inf')):
   for name in ('flow-confluence','best-ideas'):
    result=run(name,packet(count));row=result['ticker_map']['KO'] if name=='flow-confluence' else result['stack'][0];self.assertIsNone(row['t360_coverage']);self.assertEqual(result['ticker_360_context']['additional_independent_votes'],0)
 def test_read_denial_nonfinite_duplicate_and_underflow_are_isolated(self):
  for value in (PermissionError('PRIVATE_CANARY'),b'{"tickers":{},"tickers":{}}',b'{"contract":"ticker-360.v1","x":1e-1000}',b'{"x":NaN}'):
   for name in ('flow-confluence','best-ideas'):
    result=run(name,value);self.assertEqual(result['ticker_360_context']['status'],'unavailable');self.assertNotIn('PRIVATE_CANARY',json.dumps(result))
 def test_shared_context_never_trusts_upstream_self_qualification(self):
  value=packet();value['calls_eligible']=True;value['tickers']['KO']['forecast_qualified']=True
  result=helper.context(value);self.assertTrue(all(result[k] is False for k in helper.FLAGS));self.assertTrue(all(result['by_ticker']['KO'][k] is False for k in helper.FLAGS))

if __name__=='__main__':unittest.main(verbosity=2)
