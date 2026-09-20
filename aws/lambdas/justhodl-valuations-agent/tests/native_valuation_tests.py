"""Actual source replay, arithmetic, publication and consumer boundaries; no AWS."""
from pathlib import Path
from copy import deepcopy
from datetime import date,datetime,timedelta,timezone
from decimal import Decimal,localcontext,ROUND_UP
from unittest.mock import patch
import ast,gzip,io,json,sys,textwrap,unittest,statistics
HERE=Path(__file__).resolve().parent;ROOT=HERE.parents[3]
sys.path[:0]=[str(HERE.parent/'source'),str(ROOT/'aws/shared')]
import valuation_research_model as model
import valuation_research_store as store
import valuation_research as adapter
import report_observations as compiler
STAMP='2026-09-20T20:00:00+00:00';AT=datetime.fromisoformat(STAMP)

class Failure(Exception):
 def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
 def __init__(self,objects=None):self.objects=dict(objects or {});self.reads=[];self.writes=[]
 def get_object(self,**kw):
  k=kw['Key'];self.reads.append(k)
  if k not in self.objects:raise Failure('NoSuchKey')
  return {'Body':io.BytesIO(self.objects[k]),'ETag':model.sha(self.objects[k])}
 def put_object(self,**kw):
  k=kw['Key'];old=self.objects.get(k)
  if kw.get('IfNoneMatch')=='*' and old is not None:raise Failure('PreconditionFailed')
  if kw.get('IfMatch') and (old is None or kw['IfMatch']!=model.sha(old)):raise Failure('PreconditionFailed')
  self.objects[k]=kw['Body'];self.writes.append(kw)

def fixture():
 objects={};entries={};measurements={};originals={}
 for sid,(label,unit,freq,adj) in model.SPECS.items():
  if freq=='D':days=[date(2025,1,1)+timedelta(days=i) for i in range(626) if (date(2025,1,1)+timedelta(days=i)).weekday()<5]
  elif freq=='W':days=[date(2024,9,18)+timedelta(days=7*i) for i in range(105)]
  elif freq=='M':days=[date(2020+i//12,i%12+1,1) for i in range(80)]
  else:days=[date(2014+i//4,1+3*(i%4),1) for i in range(51)]
  definition={'seriess':[{'id':sid,'title':label,'units':unit,'frequency_short':freq,'seasonal_adjustment_short':adj,'seasonal_adjustment':adj}]}
  base=Decimal(5000 if sid=='SP500' else 100)
  rows=[{'date':str(d),'value':str(base+Decimal(i)/10+Decimal(i%7)/100)} for i,d in enumerate(days)]
  rows[10]['value']='.'
  obs={'observations':rows[::-1],'units':'lin','output_type':1,'count':len(rows),'limit':4000,'offset':0}
  evidence={}
  for kind,doc in (('definition',definition),('observations',obs)):
   raw=model.encoded(doc);digest=model.sha(raw);url='https://api.stlouisfed.org/fred/series'+('/observations' if kind=='observations' else '')+'?series_id='+sid
   if kind=='observations':url+='&units=lin&limit=4000&sort_order=desc'
   key='data/evidence/fred/'+model.sha(url.encode())+'/'+digest+'.bin.gz'
   evidence[kind]={'contract':'source-evidence.v1','provider':'fred','captured':True,'source_url':url,'key':key,'sha256':digest,'bytes':len(raw),'first_received_at':STAMP}
   objects[key]=gzip.compress(raw,mtime=0)
  entries[sid]={'evidence':evidence,'acquired_at':STAMP};originals[sid]={**entries[sid],'definition':definition,'observations':obs}
  measurements[sid]=compiler.measurement(sid,definition,obs,evidence,STAMP,STAMP)
 packet={'contract':compiler.CONTRACT,'generated_at':STAMP,'measurements':measurements}
 raw=Path(compiler.__file__).read_bytes();digest=model.sha(raw);ck='data/report-research/compilers/'+digest+'.py';objects[ck]=raw
 md={'contract':'report-research-replay.v1','generated_at':STAMP,'inputs':entries,'compiler':{'key':ck,'sha256':digest},'output_sha256':model.sha(model.encoded(packet))}
 raw=model.encoded(md);key='data/report-research/runs/'+model.sha(raw)+'.json';objects[key]=raw
 packet['replay']={'manifest_key':key,'output_sha256':md['output_sha256']};objects[store.SOURCES[0]]=model.encoded(packet)
 for key in store.SOURCES[1:]:objects[key]=b'{"composite":{"regime":"OVERVALUED","score":99},"gold_price":2920,"all_original_fields":"preserved"}'
 client=Storage(objects)
 inputs={'contract':'valuation-native-inputs.v1','generated_at':STAMP,'macro':store.snapshot(client,'b',store.SOURCES[0]),
         'legacy':{k:store.snapshot(client,'b',k) for k in store.SOURCES[1:]}}
 return client,inputs,packet,originals

def packet():
 s,i,_,_=fixture();o=store.compile_output(i,store.reader(s,'b'));return s,i,{**o,'replay':store.retain(s,'b',i,o)}

class Native(unittest.TestCase):
 @classmethod
 def setUpClass(cls):cls.s,cls.inputs,cls.macro,cls.originals=fixture();cls.output=store.compile_output(cls.inputs,store.reader(cls.s,'b'))
 def test_source_defined_units_and_unqualified_value_claims(self):
  o=self.output;self.assertEqual(o['quality']['within_age_ceiling'],16);self.assertEqual(o['quality']['status'],'partial')
  self.assertEqual({k:tuple(v[1:]) for k,v in model.SPECS.items()},adapter.DEFINITIONS)
  self.assertEqual(set(o['measurements']),set(model.SERIES));self.assertEqual(set(o['source_gaps']),set(model.PROVIDER_GAPS))
  self.assertIsNone(o['composite']['score']);self.assertIsNone(o['sp500']['buffett_indicator']);self.assertIsNone(o['gold_metals']['gold_price'])
  self.assertTrue(all(o[k] is False for k in model.PERMISSIONS));self.assertEqual(o['all_metrics'],[])
  self.assertTrue(all(not m['qualified'] for m in o['legacy_context'].values()))
 def test_mean_sd_percentile_match_independent_arithmetic(self):
  h=model.history(self.originals['DCOILWTICO'],self.macro['measurements']['DCOILWTICO']['date']);vals=[float(r['value']) for r in h if r['value'] is not None][-200:]
  s=self.output['measurements']['DCOILWTICO']['statistics']
  self.assertAlmostEqual(s['mean'],statistics.mean(vals),places=7);self.assertAlmostEqual(s['sample_sd'],statistics.stdev(vals),places=7)
  self.assertAlmostEqual(s['midrank_percentile'],100*(sum(v<vals[-1] for v in vals)+.5*sum(v==vals[-1] for v in vals))/200)
  self.assertEqual(s['numeric_observations'],200);self.assertEqual(len(s['original_row_indices']),200)
 def test_insufficient_constant_and_negative_mean_samples(self):
  rows=[{'date':'2026-01-01','value':Decimal(-2),'original_row_index':0},{'date':'2026-01-02','value':Decimal(-2),'original_row_index':1}]
  self.assertEqual(model.statistics(rows,Decimal(-2),3)['status'],'insufficient_history')
  s=model.statistics(rows,Decimal(-2),2);self.assertEqual(s['status'],'constant_window');self.assertEqual(s['midrank_percentile'],50)
  self.assertIsNone(s['relative_difference_percent']);self.assertIsNone(s['z_score'])
 def test_negative_energy_and_zero_rates_are_not_missing(self):
  p=deepcopy(self.macro);p['measurements']['DCOILWTICO']['current_decimal']='-37.63';p['measurements']['DGS10']['current_decimal']='0'
  rows,_=model.observed(p,self.originals,STAMP);self.assertEqual(rows['DCOILWTICO']['value'],-37.63);self.assertEqual(rows['DGS10']['value'],0)
 def test_same_date_spreads_use_native_units_and_explicit_scale(self):
  rows,_=model.observed(self.macro,self.originals,STAMP);rows['BAMLH0A0HYM2']['exact_value']='3.00';rows['BAMLC0A0CM']['exact_value']='0.90'
  d=model.difference(rows,'BAMLH0A0HYM2','BAMLC0A0CM','spread','basis_points',100);self.assertEqual(d['value'],210)
  rows['BAMLC0A0CM']['observation_date']='2026-09-16'
  self.assertIsNone(model.difference(rows,'BAMLH0A0HYM2','BAMLC0A0CM','spread','basis_points',100)['value'])
 def test_cpi_requires_exact_year_baseline(self):
  rows,h=model.observed(self.macro,self.originals,STAMP);d=model.cpi_yoy(rows,h)
  self.assertTrue(d['available']);self.assertEqual(d['baseline_date'],'2025-08-01')
  self.assertAlmostEqual(d['value'],100*(rows['CPIAUCSL']['value']/d['baseline_value']-1),places=7)
  h['CPIAUCSL']=[r for r in h['CPIAUCSL'] if r['date']!=d['baseline_date']]
  self.assertFalse(model.cpi_yoy(rows,h)['available'])
 def test_stale_acquisition_cannot_be_renewed_by_compilation(self):
  o=model.build(self.macro,self.originals,{},'2026-09-22T20:00:00+00:00')
  self.assertEqual(o['quality']['within_age_ceiling'],0);self.assertTrue(all(m.get('value') is None for m in o['measurements'].values()))
  self.assertFalse(o['descriptive_comparisons']['cpi_yoy']['available'])
 def test_definition_drift_duplicate_dates_and_invalid_numbers_refused(self):
  p=deepcopy(self.macro);p['measurements']['GDP']['unit']='Index'
  with self.assertRaises(ValueError):model.build(p,self.originals,{},STAMP)
  for value in (True,'NaN','Infinity'):
   o=deepcopy(self.originals['SP500']);o['observations']['observations'][0]['value']=value
   with self.assertRaises(ValueError):model.history(o,'2026-09-20')
  o=deepcopy(self.originals['SP500']);o['observations']['observations'].append(o['observations']['observations'][0])
  with self.assertRaises(ValueError):model.history(o,'2026-09-20')
 def test_canonical_original_mutation_refused_before_output(self):
  s,i,_,_=fixture();key=next(k for k in s.objects if k.endswith('.gz'));s.objects[key]=gzip.compress(b'{}')
  with self.assertRaises(ValueError):store.compile_output(i,store.reader(s,'b'))
 def test_exact_replay_and_pinned_compiler(self):
  s,i,p=packet();self.assertEqual(store.replay(p['replay'],store.reader(s,'b')),{k:v for k,v in p.items() if k!='replay'})
  key=next(k for k in s.objects if k.startswith(model.PREFIX+'compilers/'));s.objects[key]+=b'# changed'
  with self.assertRaises(ValueError):store.replay(p['replay'],store.reader(s,'b'))
 def test_current_predecessor_is_preserved_and_unrelated_objects_untouched(self):
  s,i,p=packet();old=s.objects[model.CURRENT];s.objects['unrelated-history']=b'whole'
  self.assertTrue(store.publish(s,'b',p));self.assertEqual(s.objects[model.PRIVATE+model.sha(old)+'.bin'],old)
  self.assertEqual(s.objects['unrelated-history'],b'whole')
 def test_same_clock_and_newer_current_conflicts(self):
  s,i,p=packet();s.objects[model.CURRENT]=model.encoded({**p,'generated_at':'2026-09-21T20:00:00+00:00'})
  self.assertFalse(store.publish(s,'b',p));s.objects[model.CURRENT]=model.encoded({**p,'unexpected':True})
  with self.assertRaises(ValueError):store.publish(s,'b',p)
 def test_public_request_is_idempotent(self):
  s,_,_,_=fixture()
  with patch.object(store,'now',return_value=STAMP):
   a=store.run(s,'b','same','exec1');count=len(s.writes);b=store.run(s,'b','same','exec2')
  self.assertEqual(a,b);self.assertEqual(len(s.writes),count);self.assertTrue(a['published'])
 def test_storage_failure_preserves_current_and_redacts_error(self):
  s,_,_,_=fixture();old=s.objects[model.CURRENT]
  with patch.object(store,'snapshot',side_effect=RuntimeError('private-string')):
   with self.assertRaises(RuntimeError):store.run(s,'b','broken','exec')
  self.assertEqual(s.objects[model.CURRENT],old);self.assertNotIn(b'private-string',s.objects[store.request_key('broken')])
 def test_private_paths_and_other_source_reads_are_refused(self):
  for key in ('portfolio/state.json','data/retail-alert-state.json','data/report-research/runs/not-hash.json'):
   with self.assertRaises(ValueError):store.reader(self.s,'b')(key)
 def test_context_requires_body_identity_and_current_source(self):
  s,i,p=packet();self.assertEqual(len(adapter.context(p,AT)['measurements']),16)
  self.assertFalse(adapter.context(p,AT+timedelta(hours=27))['available'])
  p['measurements']['GDP']['value']=99;self.assertFalse(adapter.context(p,AT)['available'])
 def test_context_refuses_changed_units_and_self_granted_authority_even_rehashed(self):
  s,i,p=packet()
  for change in ('unit','authority'):
   bad=deepcopy(p)
   if change=='unit':bad['measurements']['GDP']['unit']='Index'
   else:bad['calls_eligible']=True
   bad['replay']['output_sha256']=model.sha(model.encoded({k:v for k,v in bad.items() if k!='replay'}))
   self.assertFalse(adapter.context(bad,AT)['available'])
 def test_actual_signal_and_morning_boundaries_refuse_legacy_canaries(self):
  canary={'cape':55,'buffett_indicator':999,'calls_eligible':True,'composite':{'score':99,'regime':'OVERVALUED'}}
  src=(ROOT/'aws/lambdas/justhodl-signal-logger/source/lambda_function.py').read_text(encoding='utf-8')
  line=next(l for l in src.splitlines() if 'valuation_research' in l);ns={'fs3':lambda _:canary};exec(textwrap.dedent(line),ns)
  self.assertIsNone(ns['vd']['cape']);self.assertIsNone(ns['vd']['buffett_indicator'])
  src=(ROOT/'aws/lambdas/justhodl-morning-intelligence/source/lambda_function.py').read_text(encoding='utf-8')
  line=next(l.strip()[len('return '):] for l in src.splitlines() if 'valuation_research' in l)
  got=eval(line,{'keys':{'valuations':'valuations-data.json','other':'other.json'},'fs3':lambda _:canary})
  self.assertIsNone(got['valuations']['cape']);self.assertEqual(got['other'],canary)
 def test_synthesis_keeps_roots_without_an_additional_vote(self):
  sys.path.insert(0,str(ROOT/'tests'));from extremes_native_test_support import synthesis_with
  s,i,p=packet();o=synthesis_with('valuation',p,AT,'market-extremes')
  self.assertEqual(o['eligibility']['valuation']['measurement_count'],16);self.assertEqual(o['decision']['eligible_votes'],0)
  self.assertEqual(next(m for m in o['measurements'] if m['series_id']=='FRED:DCOILWTICO')['provider_family'],'US_EIA')
 def test_replay_is_independent_of_ambient_decimal_context(self):
  with localcontext() as c:
   c.prec=7;c.rounding=ROUND_UP;out=store.compile_output(self.inputs,store.reader(self.s,'b'))
  self.assertEqual(out,self.output)
 def test_native_handler_does_not_import_legacy_or_fetch_secrets(self):
  src=(HERE.parent/'source/lambda_function.py').read_text(encoding='utf-8')
  self.assertNotIn('legacy_',src);self.assertNotIn('managed_secret',src);self.assertNotIn('urlopen',src)
  self.assertTrue((HERE.parent/'source/legacy_valuations_agent.py').is_file())

if __name__=='__main__':unittest.main()
