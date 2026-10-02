"""Invented-only tests of the entire candidate hub and producer."""
from pathlib import Path
from unittest.mock import patch
import copy,importlib.util,io,json,sys,types,unittest
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/ticker-context-guard'
sys.path.insert(0,str(R/'aws/shared'))
def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod);return mod
hub=load('candidate_hub',R/'aws/shared/ticker_360.py')
old=types.ModuleType('prior_hub')
exec(compile((D/'shared-before.py.txt').read_bytes(),'<retained predecessor>','exec'),old.__dict__)
def packet():return {'generated_at':'2026-10-02T00:00:00Z','as_of':'2020-01-01','by_ticker':{'QAONLY':{'ticker':'QAONLY','score':99,'signal':'LONG','value':0}},'call':'LONG'}
class Storage:
 def __init__(self,packets):self.packets=packets;self.reads=[];self.writes=[]
 def get_object(self,**req):
  self.reads.append(req);obj=self.packets[req['Key']]
  if isinstance(obj,Exception):raise obj
  return {'Body':io.BytesIO(json.dumps(obj).encode())}
 def put_object(self,**req):self.writes.append(req);return {}
def domain(context=None,kind='packet',name='fixture',obj=None,module=hub):
 data=packet() if obj is None else obj;s3=Storage({'invented.json':data})
 return module._domain_view(s3,name,{'key':'invented.json','context':context,'kind':kind},'QAONLY',{})
class Candidate(unittest.TestCase):
 def flags(self,out):
  for key in ('calls_eligible','sizing_eligible','execution_eligible','forecast_qualified','observation_freshness_verified'):self.assertIs(out[key],False)
  self.assertEqual(out['independent_investment_votes'],0)
 def test_predecessor_demonstrates_exact_guard_bypass(self):
  before=domain('short_interest_context',module=old);self.assertEqual(before['ticker_data']['signal'],'LONG');self.assertIsNone(before['summary']['call'])
  after=domain('short_interest_context');self.assertIsNone(after['ticker_data']);self.assertFalse(after['ticker_covered']);self.assertEqual(after['context_status'],'applied');self.flags(after)
 def test_all_real_research_guards_keep_rows_withheld(self):
  for context in ('short_interest_context','short_volume_context','offexchange_context','capital_structure_context','sec_ftd_context','statement_context','pd_fails_context','dollar_research_context','futures_research_context','fx_research_context','gold_rotation_context'):
   with self.subTest(context=context):
    out=domain(context);self.assertEqual(out['context_status'],'applied');self.assertIsNone(out['ticker_data']);self.assertFalse(out['ticker_covered']);self.flags(out)
 def test_missing_context_is_unavailable_without_raw_fallback(self):
  out=domain('fixture_module_does_not_exist');self.assertTrue(out['packet_available']);self.assertFalse(out['available']);self.assertIsNone(out['ticker_data']);self.assertEqual(out['context_status'],'unavailable');self.flags(out)
 def test_context_exception_does_not_disclose_error_content(self):
  ctx=types.ModuleType('short_interest_context');ctx.decision_view=lambda _:(_ for _ in ()).throw(ValueError('PRIVATE_CANARY'))
  with patch.dict(sys.modules,short_interest_context=ctx):out=domain('short_interest_context')
  self.assertFalse(out['available']);self.assertNotIn('PRIVATE_CANARY',json.dumps(out));self.assertIsNone(out['ticker_data'])
 def test_nonobject_context_return_is_invalid(self):
  for value in (None,[],0,False,'LONG'):
   ctx=types.ModuleType('short_interest_context');ctx.decision_view=lambda _,v=value:v
   with patch.dict(sys.modules,short_interest_context=ctx):out=domain('short_interest_context')
   self.assertEqual(out['context_status'],'invalid');self.assertFalse(out['available']);self.assertIsNone(out['ticker_data'])
 def test_approved_view_wins_over_conflicting_raw_row_and_is_not_mutated(self):
  view={'by_ticker':{'QAONLY':{'ticker':'QAONLY','reported':0}},'call':None,'research_context':{'status':'invented_descriptive','calls_eligible':False}};before=copy.deepcopy(view)
  ctx=types.ModuleType('short_interest_context');ctx.decision_view=lambda _:view
  with patch.dict(sys.modules,short_interest_context=ctx):out=domain('short_interest_context')
  self.assertEqual(out['ticker_data'],view['by_ticker']['QAONLY']);self.assertEqual(view,before);self.assertEqual(out['research_context'],view['research_context']);self.flags(out)
 def test_market_wide_context_failure_cannot_create_coverage(self):
  out=domain('fixture_module_does_not_exist',name='dollar');self.assertFalse(out['available']);self.assertIsNone(out['ticker_data']);self.assertEqual(hub._confluence({'dollar':out})['coverage_count'],0)
 def test_new_news_domain_and_every_peer_registration_remain(self):
  self.assertEqual(hub.SOURCES,old.SOURCES);self.assertEqual(len(hub.SOURCES),24);self.assertIn('news-sentiment',hub.SOURCES)
  out=domain(None,name='news-sentiment');self.assertEqual(out['ticker_data'],packet()['by_ticker']['QAONLY']);self.assertEqual(out['context_status'],'unqualified_raw_inventory');self.flags(out)
 def test_generation_cannot_fill_missing_observation_clock(self):
  data=packet();data.pop('as_of');out=domain(obj=data);self.assertIsNone(out['as_of']);self.assertEqual(out['generated_at'],data['generated_at']);self.assertIsNone(out['reported_as_of'])
 def test_both_reported_clocks_are_preserved_separately(self):
  out=domain();self.assertEqual(out['as_of'],'2020-01-01');self.assertEqual(out['reported_as_of'],'2020-01-01');self.assertEqual(out['generated_at'],'2026-10-02T00:00:00Z');self.flags(out)
 def test_wrong_typed_clocks_are_not_coerced(self):
  for value in (0,False,[],{}):
   data=packet();data.update(as_of=value,generated_at=value);out=domain(obj=data);self.assertIsNone(out['as_of']);self.assertIsNone(out['reported_as_of']);self.assertIsNone(out['generated_at'])
 def test_per_ticker_aliases_retain_existing_selection_without_granting_votes(self):
  for source in ({'tickers':{'QAONLY':{'value':0}}},{'by_ticker':{'qaonly':{'value':0}}},{'qaonly':{'value':0}}):
   out=domain(kind='tkr',obj=source);self.assertEqual(out['ticker_data'],{'value':0});self.assertTrue(out['available']);self.flags(out)
 def test_cache_shared_across_tickers_does_not_trigger_more_packet_reads(self):
  s3=Storage({'invented.json':packet()})
  with patch.object(hub,'SOURCES',{'fixture':{'key':'invented.json','context':'short_interest_context','kind':'packet'}}):out=hub.enrich_many(['QAONLY','OTHER'],s3)
  self.assertEqual(len(s3.reads),1);self.assertEqual(set(out),{'QAONLY','OTHER'});self.assertTrue(all(v['domains']['fixture']['ticker_data'] is None for v in out.values()))
 def test_missing_domain_does_not_cancel_other_domains(self):
  s3=Storage({'invented.json':packet()});sources={'good':{'key':'invented.json','context':None,'kind':'packet'},'absent':{'key':'missing.json','context':None,'kind':'packet'}}
  with patch.object(hub,'SOURCES',sources):out=hub.enrich('QAONLY',s3)
  self.assertEqual(out['confluence']['coverage_count'],1);self.assertFalse(out['domains']['absent']['available']);self.assertEqual(out['domains']['good']['ticker_data']['value'],0)
 def test_actual_producer_drops_guarded_rows_and_preserves_inventory_contract(self):
  sources={'short-interest':{'key':'guarded.json','context':'short_interest_context','kind':'packet'},'news-sentiment':{'key':'news.json','context':None,'kind':'packet'}}
  s3=Storage({'guarded.json':packet(),'news.json':packet()});boto=types.ModuleType('boto3');boto.client=lambda *a,**k:s3
  with patch.dict(sys.modules,{'boto3':boto,'ticker_360':hub}),patch.object(hub,'SOURCES',sources):
   producer=load('candidate_producer',R/'aws/lambdas/justhodl-ticker-360/source/lambda_function.py');result=producer.lambda_handler({},None)
  self.assertTrue(result['ok']);self.assertEqual(len(s3.writes),1);published=json.loads(s3.writes[0]['Body']);row=published['tickers']['QAONLY'];self.assertEqual(row['coverage_count'],1);self.assertEqual(set(row['domains']),{'news-sentiment'});self.flags(published)
  data=row['domains']['news-sentiment'];self.flags(data);self.assertEqual(data['data'],packet()['by_ticker']['QAONLY']);self.assertEqual(data['as_of'],'2020-01-01');self.assertEqual(data['generated_at'],'2026-10-02T00:00:00Z')
  import ticker_coverage_context
  consumer=ticker_coverage_context.context(published);self.assertEqual(consumer['by_ticker']['QAONLY']['coverage_count'],1);self.assertEqual(consumer['additional_independent_votes'],0)

class Preservation(unittest.TestCase):
 def test_whole_predecessors_reverse_exact_reviewed_changes_only(self):
  import hashlib
  doc=json.loads((D/'transition.json').read_bytes())
  for kind,oldname in [('shared','shared-before.py.txt'),('producer','lambda-before.py.txt')]:
   t=doc[kind];raw=(R/t['path']).read_text(encoding='utf-8');self.assertEqual(hashlib.sha256(raw.encode()).hexdigest(),t['after_sha256'])
   for change in reversed(t['replacements']):
    self.assertEqual(raw.count(change['after']),1);raw=raw.replace(change['after'],change['before'])
   self.assertEqual(raw,(D/oldname).read_text(encoding='utf-8'));self.assertEqual(hashlib.sha256(raw.encode()).hexdigest(),t['before_sha256'])
 def test_every_registered_context_is_in_exact_deployment_closure(self):
  sys.path.insert(0,str(R/'aws/ops/checks'));import release_package_evidence
  sources=release_package_evidence.shared_imports(R,[R/'aws/lambdas/justhodl-ticker-360/source/lambda_function.py'])
  expected={'ticker_360'}|{v['context'] for v in hub.SOURCES.values() if v['context']}
  self.assertEqual({p.stem for p in sources},expected);self.assertEqual(set(hub.CONTEXT_LOADERS),expected-{'ticker_360'})
 def test_existing_raw_inventory_and_per_ticker_selection_match_predecessor(self):
  for kind in ('packet','tkr'):
   for obj in (packet(),{'tickers':{'QAONLY':{'reported':0}}},{'by_ticker':{'OTHER':{'value':1}}}):
    before=domain(kind=kind,obj=obj,module=old);after=domain(kind=kind,obj=obj)
    for key in ('available','ticker_data','summary'):self.assertEqual(after[key],before[key])
 def test_no_configuration_change_or_source_twin_is_hidden(self):
  baseline=json.loads((R/'docs/audit/2026-10-02/ticker-context-guard.json').read_bytes())
  import hashlib
  config=R/'aws/lambdas/justhodl-ticker-360/config.json';self.assertEqual(hashlib.sha256(config.read_bytes()).hexdigest(),baseline['source_hashes'][config.relative_to(R).as_posix()])
  native=json.loads((R/'docs/audit/2026-10-02/ticker-controls-baseline.json').read_bytes())['functions']['justhodl-ticker-360']
  self.assertEqual(baseline['original_controls']['justhodl-ticker-360']['schedules'],native['schedules'])
  self.assertEqual(baseline['original_controls']['justhodl-ticker-360']['timeout'],native['Timeout'])
  self.assertEqual(baseline['original_controls']['justhodl-ticker-360']['memory_mb'],native['MemorySize'])


class FallbackIdentity(unittest.TestCase):
 def test_cached_primary_failure_does_not_block_later_fallback(self):
  s=Storage({'primary.json':OSError('invented'),'fallback.json':packet()});cache={}
  self.assertIsNone(hub._read_packet(s,'primary.json',cache))
  self.assertEqual(hub._read_packet(s,'primary.json',cache,'fallback.json'),packet())
  self.assertIsNone(cache['primary.json']);self.assertEqual(cache['fallback.json'],packet())
  hub._read_packet(s,'primary.json',cache,'fallback.json');self.assertEqual([r['Key'] for r in s.reads],['primary.json','fallback.json'])
 def test_primary_success_keeps_precedence_without_fallback_read(self):
  s=Storage({'primary.json':packet(),'fallback.json':{'unexpected':True}});cache={}
  self.assertEqual(hub._read_packet(s,'primary.json',cache,'fallback.json'),packet());self.assertEqual([r['Key'] for r in s.reads],['primary.json'])
 def test_universe_and_domain_use_actual_fallback_source_without_raw_guard_bypass(self):
  sources={'short-interest':{'key':'primary.json','fallback_key':'fallback.json','context':'short_interest_context','kind':'packet'}}
  s=Storage({'primary.json':OSError('invented'),'fallback.json':packet()});cache={}
  with patch.object(hub,'SOURCES',sources):
   self.assertEqual(hub.universe(s,cache),['QAONLY']);view=hub.enrich('QAONLY',s,cache=cache)['domains']['short-interest']
  self.assertEqual(view['source_key'],'fallback.json');self.assertEqual(view['configured_source_key'],'primary.json');self.assertTrue(view['fallback_source_used']);self.assertIsNone(view['ticker_data']);self.assertFalse(view['raw_fallback_used']);self.assertEqual(len(s.reads),2)
 def test_self_fallback_is_bounded_and_remains_unavailable(self):
  s=Storage({'missing.json':OSError('invented')});self.assertIsNone(hub._read_packet(s,'missing.json',{},'missing.json'));self.assertEqual(len(s.reads),1)

if __name__=='__main__':unittest.main(verbosity=2)
