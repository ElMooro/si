"""Complete invented equity identity/recovery regressions. No real data access."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone, timedelta
from types import SimpleNamespace
from unittest.mock import patch
from contextlib import ExitStack
import importlib.util,io,json,sys,unittest

HERE=Path(__file__).resolve().parent
EXTERNAL=(HERE/'equity_identity.py').exists()
ROOT=HERE.parent/'si-batch-improvements' if EXTERNAL else HERE.parent
SOURCE=HERE if EXTERNAL else ROOT/'aws/lambdas/justhodl-symbology-master/source'
SHARED=ROOT/'aws/shared/openfigi.py'

def load(name,path):
 spec=importlib.util.spec_from_file_location(name,path);obj=importlib.util.module_from_spec(spec)
 with patch.dict(sys.modules,{'boto3':SimpleNamespace(client=lambda *a,**kw:SimpleNamespace()),'raw_snapshot':SimpleNamespace(snapshot=None)}):spec.loader.exec_module(obj)
 return obj
identity=load('equity_identity_candidate',SOURCE/'equity_identity.py')
bonds=load('equity_bond_store_candidate',SOURCE/'bond_symbology.py')
figi=load('equity_openfigi_candidate',SHARED)
native=load('equity_native_candidate',SOURCE/'lambda_function.py')
NOW=datetime(2026,10,1,tzinfo=timezone.utc)
SEC={'0':{'ticker':'TEST','cik_str':123,'title':'Invented issuer'}}
SECURITY={'figi':'BBG00TEST001','ticker':'TEST','name':'Invented issuer','marketSector':'Equity','exchCode':'US','retained_extra':['all',None]}

class S3Error(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}
class Store:
 def __init__(self,prior='missing'):
  self.docs={};self.puts=[];self.reads=[];self.failure=None;self.write_error=None;self.streams=[]
  if prior!='missing':self.docs[identity.MASTER_KEY]=prior
 def get_object(self,**kw):
  self.reads.append(kw['Key'])
  if self.failure:raise self.failure
  if kw['Key'] not in self.docs:raise S3Error('NoSuchKey')
  raw=json.dumps(self.docs[kw['Key']]).encode();body=io.BytesIO(raw);self.streams.append(body)
  return {'Body':body,'ContentLength':len(raw),'ETag':'"original-version"'}
 def put_object(self,**kw):
  self.puts.append(kw)
  if self.write_error:raise self.write_error
 def written(self):return json.loads(self.puts[-1]['Body'])

class Tests(unittest.TestCase):
 def setUp(self):
  self.modules=patch.dict(sys.modules,{'equity_identity':identity,'bond_symbology':bonds,'openfigi':figi});self.modules.start()
 def tearDown(self):self.modules.stop()
 def spine(self,source=SEC):return identity.spine(deepcopy(source),{'url':'https://invented.invalid/fixture','raw_snapshot_key':None})[0]
 def handler(self,store,sec=SEC,chain=False):
  with ExitStack() as stack:
   stack.enter_context(patch.object(native,'s3',store));stack.enter_context(patch.object(native,'snapshot',None))
   stack.enter_context(patch.object(native.urllib.request,'urlopen',return_value=io.BytesIO(json.dumps(sec).encode())))
   stack.enter_context(patch.object(native,'enrich_figi',return_value={}))
   stack.enter_context(patch.object(native,'enrich_bond_cusips',return_value={}))
   if not chain:stack.enter_context(patch.object(native,'enrich_cusip_chain',return_value={}))
   return native.lambda_handler({},SimpleNamespace(get_remaining_time_in_millis=lambda:60000))
 def resolve(self,rows,response):
  with patch.object(figi,'get_api_key',return_value='invented-key'),patch.object(figi,'mapping',return_value=response),patch.object(identity.time,'monotonic',return_value=100):
   return identity.enrich_figi(rows,SimpleNamespace(get_remaining_time_in_millis=lambda:80000),now=NOW)
 def test_prior_timeout_denial_null_and_bad_population_never_write_or_request_provider(self):
  for prior,exc in (({},TimeoutError()),({},S3Error('AccessDenied')),(None,None),({},None),({'by_ticker':None},None),({'by_ticker':{'TEST':[]}},None)):
   store=Store(prior);store.failure=exc
   with patch.object(native,'s3',store),patch.object(native.urllib.request,'urlopen') as url:
    with self.assertRaises(Exception):native.lambda_handler({},None)
   url.assert_not_called();self.assertEqual(store.puts,[])
 def test_genuine_absence_allows_only_conditional_create(self):
  store=Store();self.assertEqual(self.handler(store)['statusCode'],200)
  self.assertEqual(store.puts[0]['IfNoneMatch'],'*');self.assertNotIn('IfMatch',store.puts[0])
 def test_existing_master_uses_observed_version_and_preserves_unknown_fields(self):
  prior={'by_ticker':{'TEST':{'cik':'0000000123','figi':'BBG00PRIOR01','custom':[1,None]}},'opaque':{'all':['keep']}}
  store=Store(prior);self.handler(store);doc=store.written()
  self.assertEqual(doc['opaque'],prior['opaque']);self.assertEqual(doc['by_ticker']['TEST']['custom'],[1,None])
  self.assertEqual(doc['by_ticker']['TEST']['figi'],'BBG00PRIOR01');self.assertEqual(store.puts[0]['IfMatch'],'"original-version"')
 def test_concurrent_update_is_never_retried_unconditionally(self):
  store=Store({'by_ticker':{}});store.write_error=S3Error('PreconditionFailed')
  with self.assertRaises(S3Error):self.handler(store)
  self.assertEqual(len(store.puts),1);self.assertIn('IfMatch',store.puts[0])
 def test_recycled_ticker_cannot_borrow_previous_issuer_identifiers(self):
  old={'cik':'0000000456','figi':'BBG00PRIOR01','cusip':'111111111','isin':'US1111111111','lei':'invented-old','extra':{'retained':True}}
  store=Store({'by_ticker':{'TEST':old}});self.handler(store);row=store.written()['by_ticker']['TEST']
  self.assertEqual(row['cik'],'0000000123')
  for key in ('figi','cusip','isin','lei'):self.assertIsNone(row[key])
  self.assertEqual(row['quarantined_prior_identity'],old)
 def test_missing_prior_cik_is_unqualified_not_same_issuer(self):
  old={'figi':'BBG00PRIOR01'};rows=self.spine();identity.carry_previous(rows,{'by_ticker':{'TEST':old}})
  self.assertIsNone(rows['TEST']['figi']);self.assertEqual(rows['TEST']['quarantined_prior_identity'],old)
 def test_empty_present_prior_row_cannot_borrow_retained_other_row(self):
  rows=self.spine();identity.carry_previous(rows,{'by_ticker':{'TEST':{}},'retained_prior_tickers':{'TEST':{'cik':'0000000123','figi':'BBG00PRIOR01'}}})
  self.assertIsNone(rows['TEST']['figi']);self.assertEqual(rows['TEST']['quarantined_prior_identity'],{})
 def test_missing_zero_bool_invalid_and_oversized_sec_cik_never_become_zero_identity(self):
  for value in (None,0,True,False,'',-1,1.2,'1e2','12345678901'):
   source=deepcopy(SEC);source['0']['cik_str']=value
   with self.assertRaises(identity.IdentityError):self.spine(source)
  self.assertEqual(identity.cik('123'),'0000000123')
 def test_conflicting_ticker_population_is_rejected_as_a_whole(self):
  sec={**SEC,'1':{'ticker':'TEST','cik_str':456,'title':'Invented other'}};store=Store({'by_ticker':{}})
  with self.assertRaises(identity.IdentityError):self.handler(store,sec)
  self.assertFalse(store.puts)
 def test_duplicate_same_issuer_source_rows_are_all_retained(self):
  sec={**SEC,'1':deepcopy(SEC['0'])};rows,by_cik=identity.spine(sec,{'kind':'invented'})
  self.assertEqual(len(rows['TEST']['source_records']),2);self.assertEqual(by_cik,{'0000000123':['TEST']})
 def test_conflicting_labels_never_choose_last_source_name(self):
  sec={**SEC,'1':{**SEC['0'],'title':'Conflicting label'}};rows=self.spine(sec)
  self.assertIsNone(rows['TEST']['name']);self.assertEqual(len(rows['TEST']['source_records']),2)
 def test_source_population_contraction_preserves_prior(self):
  prior={'by_ticker':{str(k):{'cik':str(k+1)} for k in range(10)}};store=Store(prior)
  with self.assertRaises(identity.IdentityError):self.handler(store)
  self.assertFalse(store.puts)
 def test_departed_rows_and_preexisting_retained_population_are_kept(self):
  previous={'by_ticker':{'OLD':{'cik':'0000000045','all':[1,2,3]}},'retained_prior_tickers':{'OLDER':{'cik':'0000000046','note':'keep'}}}
  retained=identity.carry_previous(self.spine(),previous)
  self.assertEqual(retained,{**previous['retained_prior_tickers'],**previous['by_ticker']})
 def test_repeated_carry_does_not_recursively_nest_same_issuer_history(self):
  rows=self.spine();old={'by_ticker':{'TEST':{'cik':'0000000123','figi':'BBG00PRIOR01','note':'retain'}}}
  identity.carry_previous(rows,old);size=len(json.dumps(rows));identity.carry_previous(rows,{'by_ticker':deepcopy(rows)})
  self.assertEqual(len(json.dumps(rows)),size)
 def test_provider_error_is_not_terminal_no_match(self):
  rows=self.spine();stats=self.resolve(rows,[{'error':'invented temporary failure'}])
  self.assertEqual(stats['errors'],1);self.assertEqual(stats['no_match'],0);self.assertEqual(rows['TEST']['figi_status'],'error')
  self.assertFalse(rows['TEST']['figi_resolution_eligible']);self.assertEqual(stats['remaining_unqualified'],1)
 def test_missing_response_or_wrong_count_is_error_for_every_job(self):
  sec={**SEC,'1':{'ticker':'TEST2','cik_str':456,'title':'Second'}}
  for result in (None,[],[{'data':[SECURITY]}]):
   rows=self.spine(sec);stats=self.resolve(rows,result)
   self.assertEqual(stats['errors'],2);self.assertTrue(all(r['figi_status']=='error' for r in rows.values()))
 def test_legacy_negative_is_retried_and_multiple_candidates_are_kept(self):
  rows=self.spine();rows['TEST']['figi_status']='no_match';item={'data':[SECURITY,{**SECURITY,'figi':'BBG00TEST002'}]}
  stats=self.resolve(rows,[item]);self.assertEqual(stats['attempted'],1);self.assertEqual(stats['ambiguous'],1)
  self.assertEqual(rows['TEST']['figi_resolution']['response'],item);self.assertIsNone(rows['TEST']['figi'])
 def test_unique_matching_equity_maps_but_does_not_qualify_issuer_security_relationship(self):
  rows=self.spine();stats=self.resolve(rows,[{'data':[SECURITY]}]);row=rows['TEST']
  self.assertEqual(stats['enriched'],1);self.assertEqual(row['figi'],SECURITY['figi'])
  self.assertFalse(row['figi_resolution']['issuer_security_relationship_qualified'])
  self.assertEqual(row['figi_resolution']['response']['data'][0],SECURITY)
 def test_foreign_ticker_or_non_equity_result_cannot_promote(self):
  for security in ({**SECURITY,'ticker':'OTHER'},{**SECURITY,'marketSector':'Corp'}):
   rows=self.spine();stats=self.resolve(rows,[{'data':[security]}])
   self.assertEqual(stats['enriched'],0);self.assertEqual(stats['ambiguous'],1);self.assertIsNone(rows['TEST']['figi'])
 def test_wrong_or_missing_exchange_cannot_qualify_current_or_cached_resolution(self):
  for exchange in (None,'JP'):
   rows=self.spine();stats=self.resolve(rows,[{'data':[{**SECURITY,'exchCode':exchange}]}])
   self.assertEqual(stats['ambiguous'],1);self.assertFalse(rows['TEST']['figi_resolution_eligible'])
   rows=self.spine();self.resolve(rows,[{'data':[SECURITY]}]);row=rows['TEST']
   response={'data':[{**SECURITY,'exchCode':exchange}]}
   row['figi_resolution']={**figi.resolution(response),'query':{'idType':'TICKER','idValue':'TEST','exchCode':'US'},'sec_cik':row['cik']}
   self.assertFalse(identity._qualified_figi('TEST',row,NOW))
 def test_explicit_negative_expires_and_cannot_cross_issuer_identity(self):
  rows=self.spine();self.resolve(rows,[{'warning':'No identifier found.'}]);row=rows['TEST']
  self.assertTrue(identity._qualified_figi('TEST',row,NOW));self.assertFalse(identity._qualified_figi('TEST',row,NOW+timedelta(days=7)))
  row['cik']='0000000456';self.assertFalse(identity._qualified_figi('TEST',row,NOW))
 def test_optional_resolution_never_exhausts_budget_without_a_clock(self):
  for context in (None,SimpleNamespace(get_remaining_time_in_millis=lambda:20000),SimpleNamespace(get_remaining_time_in_millis=lambda:float('nan'))):
   with patch.object(figi,'get_api_key') as key:
    stats=identity.enrich_figi(self.spine(),context,now=NOW)
   self.assertEqual(stats['attempted'],0);key.assert_not_called()
 def test_stale_cached_resolution_flag_is_recomputed_even_when_work_deferred(self):
  rows=self.spine();self.resolve(rows,[{'data':[SECURITY]}]);self.assertTrue(rows['TEST']['figi_resolution_eligible'])
  identity.enrich_figi(rows,None,now=NOW+timedelta(days=7))
  self.assertFalse(rows['TEST']['figi_resolution_eligible'])
 def test_issuer_carry_does_not_inherit_unchecked_positive_permissions(self):
  old={'cik':'0000000123','figi':'BBG00PRIOR01','figi_resolution_eligible':True,'cusip_match_qualified':True,'isin_match_qualified':True,'lei_match_qualified':True}
  rows=self.spine();identity.carry_previous(rows,{'by_ticker':{'TEST':old}})
  for key in ('figi_resolution_eligible','cusip_match_qualified','isin_match_qualified','lei_match_qualified'):self.assertFalse(rows['TEST'][key])
 def test_conflicting_cusip_does_not_attach_a_new_isin_or_lei(self):
  rows=self.spine();rows['TEST']['cusip']='111111111';store=Store();store.docs['data/13f-cusip-map.json']={'123456789':{'ticker':'TEST'}}
  with patch.object(native,'s3',store):stats=native.enrich_cusip_chain(rows)
  self.assertEqual(rows['TEST']['cusip'],'111111111');self.assertIsNone(rows['TEST']['isin']);self.assertIsNone(rows['TEST']['lei'])
  self.assertEqual(stats['conflicts'],1)
 def test_us_isin_derivation_without_country_evidence_is_candidate_only(self):
  rows=self.spine();store=Store();store.docs['data/13f-cusip-map.json']={'123456789':{'ticker':'TEST'}}
  with patch.object(native,'s3',store):stats=native.enrich_cusip_chain(rows)
  self.assertEqual(rows['TEST']['cusip'],'123456789');self.assertIsNone(rows['TEST']['isin'])
  self.assertFalse(rows['TEST']['isin_derivation_candidate']['qualified']);self.assertEqual(stats['isin'],0)
 def test_ambiguous_direct_cusips_and_name_only_matches_do_not_promote(self):
  for doc in ({'123456789':{'ticker':'TEST'},'987654321':{'ticker':'TEST'}},{'123456789':'Invented issuer'}):
   rows=self.spine();store=Store();store.docs['data/13f-cusip-map.json']=doc
   with patch.object(native,'s3',store):native.enrich_cusip_chain(rows)
   self.assertIsNone(rows['TEST']['cusip']);self.assertIsNone(rows['TEST']['isin'])
 def test_optional_cusip_exception_cannot_leave_partial_row_changes(self):
  rows=self.spine();original=deepcopy(rows)
  def broken(working):
   working['TEST']['cusip']='111111111'
   raise RuntimeError('invented failure')
  with patch.object(native,'enrich_cusip_chain',side_effect=broken):stats=native.enrich_cusip_safely(rows)
  self.assertEqual(rows,original);self.assertEqual(stats['errors'],1)
 def test_optional_figi_exception_cannot_leave_partial_row_changes(self):
  rows=self.spine();original=deepcopy(rows)
  def broken(working,*args,**kw):
   working['TEST']['figi']='BBG00OTHER01'
   raise RuntimeError('invented failure')
  with patch.object(identity,'enrich_figi',side_effect=broken):stats=native.enrich_figi(rows,context=SimpleNamespace())
  self.assertEqual(rows,original);self.assertEqual(stats['errors'],1)
 def test_malformed_optional_map_cannot_stop_core_publication(self):
  store=Store();store.docs['data/13f-cusip-map.json']={'123456789':{'ticker':123}}
  result=self.handler(store,chain=True)
  self.assertEqual(result['statusCode'],200);self.assertEqual(store.written()['enrichment_status']['cusip_chain']['error'],'optional_cusip_chain_failed')
 def test_native_packet_exposes_unqualified_scope_and_complete_source_records(self):
  store=Store();self.handler(store);doc=store.written()
  self.assertEqual(doc['identity_quality']['source_records'],1)
  for key in ('source_replay_verified','investment_authority','universe_coverage_qualified','issuer_security_relationships_qualified'):self.assertFalse(doc['identity_quality'][key])
  self.assertEqual(doc['by_ticker']['TEST']['source_records'][0]['record'],SEC['0'])

class RetainedPredecessorTests(unittest.TestCase):
 def test_whole_predecessor_reproduces_all_six_repaired_failures(self):
  path=HERE/'predecessor-lambda_function.py.txt' if EXTERNAL else ROOT/'tests/fixtures/symbology/pre-equity-lambda.py.txt'
  ns={'__name__':'whole_equity_predecessor'}
  with patch.dict(sys.modules,{'boto3':SimpleNamespace(client=lambda *a,**kw:None),'raw_snapshot':SimpleNamespace(snapshot=None)}):exec(compile(path.read_bytes(),str(path),'exec'),ns)
  ns['_figi_key']=lambda:'invented-key'
  rows={'TEST':{'figi':None}}
  with patch.object(ns['urllib'].request,'urlopen',return_value=io.BytesIO(b'[{"error":"invented provider error"}]')),patch.object(ns['time'],'sleep'):
   stats=ns['enrich_figi'](rows)
  self.assertEqual(rows['TEST']['figi_status'],'no_match');self.assertEqual(stats['errors'],0)
  store=Store();store.docs['data/13f-cusip-map.json']={'123456789':{'ticker':'TEST'}};ns['s3']=store
  rows={'TEST':{'name':'Invented issuer','cusip':'111111111','isin':None,'lei':None}};ns['enrich_cusip_chain'](rows)
  self.assertEqual(rows['TEST']['cusip'],'111111111');self.assertTrue(rows['TEST']['isin'].startswith('US123456789'))
  ns['enrich_figi']=lambda *a,**kw:{};ns['enrich_cusip_chain']=lambda *a,**kw:{};ns['enrich_bond_cusips']=lambda *a,**kw:{}
  ns['datetime']=SimpleNamespace(now=lambda tz:NOW)
  def invoke(prior,sec=SEC,fail=False):
   storage=Store(prior);storage.failure=TimeoutError() if fail else None;ns['s3']=storage
   with patch.object(ns['urllib'].request,'urlopen',return_value=io.BytesIO(json.dumps(sec).encode())):ns['lambda_handler']({},None)
   return storage.written()
  doc=invoke({'by_ticker':{}},fail=True);self.assertIsNone(doc['by_ticker']['TEST']['figi'])
  doc=invoke({'by_ticker':{'TEST':{'cik':'0000000456','figi':'BBG00PRIOR01'}}});self.assertEqual(doc['by_ticker']['TEST']['cik'],'0000000123');self.assertEqual(doc['by_ticker']['TEST']['figi'],'BBG00PRIOR01')
  doc=invoke({'by_ticker':{}},{'0':{**SEC['0'],'cik_str':None}});self.assertEqual(doc['by_ticker']['TEST']['cik'],'0000000000')
  doc=invoke({'by_ticker':{}},{**SEC,'1':{**SEC['0'],'cik_str':456}});self.assertEqual(doc['n_tickers'],1);self.assertEqual(doc['n_ciks'],2)
 def test_complete_large_equity_master_without_weakening_bond_default_bound(self):
  raw=json.dumps({'by_ticker':{'TEST':{'cik':'0000000123','invented_complete_note':'A'*(16*1024*1024)}}}).encode()
  def client():
   body=io.BytesIO(raw)
   return SimpleNamespace(get_object=lambda **kw:{'Body':body,'ContentLength':len(raw),'ETag':'"whole-large"'}),body
  c,body=client()
  with self.assertRaises(ValueError):bonds._read(c,'invented',identity.MASTER_KEY)
  self.assertTrue(body.closed)
  c,body=client()
  with patch.dict(sys.modules,{'bond_symbology':bonds}):doc,etag=identity.read_prior(c,'invented')
  self.assertEqual(len(doc['by_ticker']['TEST']['invented_complete_note']),16*1024*1024);self.assertEqual(etag,'"whole-large"');self.assertTrue(body.closed)

if __name__=='__main__':unittest.main()
