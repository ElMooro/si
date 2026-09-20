"""Deterministic synthetic upstreams, actual replay/storage and economic boundaries."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime,timezone,timedelta
from unittest.mock import patch
import hashlib,io,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import extremes_native_model as model
import extremes_native_store as store
import extremes_research as adapter
STAMP='2026-09-20T18:20:00+00:00';AT=datetime.fromisoformat(STAMP)

class Failure(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self,objects=None):self.objects=dict(objects or {});self.reads=[];self.writes=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise Failure('NoSuchKey')
        raw=self.objects[key];return {'Body':io.BytesIO(raw),'ETag':model.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Failure('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or model.sha(old)!=kw['IfMatch']):raise Failure('PreconditionFailed')
        self.objects[key]=kw['Body'];self.writes.append(kw)

def crisis(value=2.7,unit='Percent'):
    return {'contract':'crisis-research.v1','generated_at':STAMP,'call':None,**model.PERMISSIONS,
        'measurements':{'BAMLH0A0HYM2':{'series_id':'BAMLH0A0HYM2','label':'Synthetic high yield spread','unit':unit,
            'definition':{'units':unit},'value':value,'value_decimal':str(value),'frequency':'D','observation_date':'2026-09-18',
            'quality':{'status':'fresh'},'source_generated_at':STAMP,'acquired_at':STAMP,**model.PERMISSIONS}}}

def funding(value=2.7):
    valid=(AT+timedelta(hours=26)).isoformat();digest='1'*64
    evidence={k:{'provider':'fred','captured':True,'key':'data/evidence/fred/'+digest+'/'+digest+'.bin.gz'} for k in ('definition','observations')}
    return {'contract':'eurodollar-native-research.v1','generated_at':STAMP,'source_generated_at':STAMP,'call':None,**model.PERMISSIONS,
        'freshness':{'pipeline_check_due_at':valid},'repo_comparison':{'current_comparison_available':False},
        'measurements':{'BAMLH0A0HYM2':{'series_id':'BAMLH0A0HYM2','label':'Synthetic high yield spread','value':value,'exact_value':str(value),
            'value_bps':float(model.decimal(value)*100),'exact_value_bps':str(model.decimal(value)*100),'unit':'Percent','source_unit':'Percent','frequency':'D',
            'quality':{'status':'within_age_ceiling'},'source_url':'https://fred.stlouisfed.org/series/BAMLH0A0HYM2',
            'source_generated_at':STAMP,'acquired_at':STAMP,'observation_date':'2026-09-18','source_valid_until':valid,
            'evidence':evidence,'interpretation':'Synthetic test source','history_coverage':{},**model.PERMISSIONS}}}

def fails():
    q={'status':'fresh','acquired_at':STAMP,'next_expected_publication_date':'2026-09-24T20:15:00+00:00'}
    def scope(name,ftd,ftr):return {'scope_id':name,'unit':'usd_bn','complete':True,'as_of':'2026-09-09','quality':q,
        'field_units':{k:'usd_bn' for k in ('ftd_bn','ftr_bn','combined_bn')},'ftd_bn':ftd,'ftr_bn':ftr,'combined_bn':float(model.decimal(ftd)+model.decimal(ftr))}
    return {'contract':'fr2004-fails-research.v1','generated_at':STAMP,'call':None,**model.PERMISSIONS,'quality':q,
        'headline':scope('ust_ex_tips',86,87),'treasury':scope('treasury_incl_tips',92.61,97.63)}

def fixture(engine='capitulation',overrides=None):
    s=Storage();docs={'crisis':crisis(),'funding':funding(),'fails':fails()};docs.update(overrides or {})
    for name in model.INPUTS[engine]:
        p=docs.get(name)
        if p is None:continue
        source_key,_,prefix=model.SOURCES[name];body=model.encoded(p);digest=model.sha(body);outkey='data/'+prefix+'/outputs/'+digest+'.json'
        run={'contract':'synthetic-upstream-replay.v1','generated_at':STAMP,'output':{'key':outkey,'sha256':digest,'bytes':len(body)},'output_sha256':digest}
        run_raw=model.encoded(run);runkey='data/'+prefix+'/runs/'+model.sha(run_raw)+'.json'
        p={**p,'replay':{'manifest_key':runkey,'output_sha256':digest}}
        s.objects.update({source_key:model.encoded(p),runkey:run_raw,outkey:body})
    with patch.object(store,'now',return_value=STAMP):entries=store.collect(s,'b',engine,time.monotonic()+30)
    inputs={'contract':'extremes-native-inputs.v1','engine':engine,'started_at':STAMP,'generated_at':STAMP,'sources':entries}
    return s,inputs

def packet(engine='capitulation',overrides=None):
    s,i=fixture(engine,overrides);out=store.compile_output(i,store.reader(s,'b'));ref=store.retain(s,'b',i,out)
    return s,i,{**out,'replay':ref}

def synthesis_with(name,p,at,engine='capitulation'):
    """Actual pure producer boundary used by the older domain regression suites."""
    packets={n:p if n==name else None for n in model.INPUTS[engine]}
    evidence={n:{'status':'retained' if n==name else 'missing','upstream_identity_verified':n==name} for n in packets}
    return model.compute(engine,packets,evidence,at.isoformat())

class Native(unittest.TestCase):
    def test_shared_series_is_one_identity_and_no_extra_votes(self):
        s,i,p=packet();g=p['dependency_graph'];self.assertEqual(g['series']['FRED:BAMLH0A0HYM2'],['crisis','funding'])
        self.assertEqual(len(g['same_series_date_overlaps']),1);self.assertEqual(g['independent_investment_votes'],0)
        self.assertIsNone(p['capitulation_score']);self.assertIsNone(p['posture']);self.assertEqual(p['decision']['verb'],'WAIT')
    def test_disagreement_is_preserved_without_averaging(self):
        s,i,p=packet(overrides={'funding':funding(3.1)});c=p['dependency_graph']['same_series_date_conflicts']
        self.assertEqual(len(c),1);self.assertEqual([v['value'] for v in c[0]['values']],[2.7,3.1]);self.assertIsNone(p['cycle_position'])
    def test_same_series_different_unit_is_definition_conflict(self):
        s,i,p=packet(overrides={'crisis':crisis(270,'Basis Points')})
        self.assertEqual(p['dependency_graph']['definition_conflicts'][0]['units'],['Basis Points','Percent'])
    def test_fails_scopes_reconcile_separately_never_sum_together(self):
        s,i,p=packet();scopes=p['pd_settlement_fails']['scopes']
        self.assertEqual(scopes['ust_ex_tips']['combined_bn'],173);self.assertEqual(scopes['treasury_incl_tips']['combined_bn'],190.24)
        self.assertNotIn('total',p['pd_settlement_fails']);self.assertEqual(len([r for r in p['measurements'] if r['source_engine']=='fails']),6)
    def test_wrong_scope_or_units_or_reconciliation_withholds_whole_fails_context(self):
        for field,value in (('scope_id','ust_ex_tips'),('unit','usd_mn'),('combined_bn',999)):
            p=fails();p['treasury'][field]=value;s,i,out=packet(overrides={'fails':p})
            self.assertEqual(out['contexts']['fails'],{});self.assertFalse(out['eligibility']['fails']['research_context_available'])
    def test_missing_inputs_do_not_become_neutral_zero_or_fresh(self):
        s,i,p=packet('market-extremes',{'fails':None})
        self.assertEqual(p['quality']['status'],'unavailable');self.assertEqual(p['measurements'],[])
        self.assertIsNone(p['scores']['top_risk']);self.assertIsNone(p['scores']['capitulation'])
    def test_no_current_observations_cannot_count_boilerplate_as_available_context(self):
        c=crisis();c['measurements']['BAMLH0A0HYM2']['observation_date']='2025-01-01'
        s,i,p=packet(overrides={'crisis':c})
        self.assertFalse(p['eligibility']['crisis']['research_context_available']);self.assertEqual(p['contexts']['crisis'],{})
    def test_both_authority_spellings_are_refused(self):
        c=crisis();c['forecast_eligible']=True
        s,i,p=packet(overrides={'crisis':c});self.assertFalse(p['eligibility']['crisis']['research_context_available'])
    def test_stale_upstream_is_not_refreshed_by_new_wrapper(self):
        s,i=fixture();later='2026-09-24T18:20:00+00:00';i['started_at']=i['generated_at']=later
        for v in i['sources'].values():v['acquired_at']=later
        p=store.compile_output(i,store.reader(s,'b'));self.assertEqual(p['measurements'],[])
    def test_future_upstream_is_excluded(self):
        s,i=fixture();earlier='2026-09-19T18:20:00+00:00';i['started_at']=i['generated_at']=earlier
        for v in i['sources'].values():v['acquired_at']=earlier
        p=store.compile_output(i,store.reader(s,'b'));self.assertEqual(p['measurements'],[])
    def test_raw_packet_run_and_output_corruption_are_refused(self):
        for field in ('packet','upstream_run','upstream_output'):
            s,i=fixture();key=i['sources']['crisis'][field]['key'];s.objects[key]+=b' '
            with self.assertRaises(ValueError):store.compile_output(i,store.reader(s,'b'))
    def test_upstream_output_lookup_refuses_private_or_account_path(self):
        s,i=fixture();p=json.loads(s.objects[model.SOURCES['crisis'][0]]);run=json.loads(s.objects[p['replay']['manifest_key']]);run['output']['key']='portfolio/risk.json'
        raw=model.encoded(run);key='data/crisis-research/runs/'+model.sha(raw)+'.json';s.objects[key]=raw;p['replay']['manifest_key']=key;s.objects[model.SOURCES['crisis'][0]]=model.encoded(p)
        with self.assertRaises(ValueError):store.collect(s,'b','capitulation',time.monotonic()+30)
        self.assertNotIn('portfolio/risk.json',s.reads)
    def test_self_authorized_upstream_with_same_rehashed_body_still_cannot_vote(self):
        c=crisis();c['calls_eligible']=True;s,i,p=packet(overrides={'crisis':c})
        self.assertFalse(p['eligibility']['crisis']['research_context_available']);self.assertEqual(p['decision']['eligible_votes'],0)
    def test_replay_is_exact_and_compiler_change_requires_reviewed_release(self):
        s,i,p=packet();self.assertEqual(store.replay(p['replay'],store.reader(s,'b')),{k:v for k,v in p.items() if k!='replay'})
        run=json.loads(s.objects[p['replay']['manifest_key']]);key=run['compilers']['extremes_native_model']['key'];s.objects[key]=b'# changed'
        with self.assertRaises(ValueError):store.replay(p['replay'],store.reader(s,'b'))
    def test_newer_current_wins_and_same_clock_conflict_refused(self):
        s,i,p=packet();key=store.current('capitulation');old={**p,'generated_at':'2026-09-21T18:20:00+00:00'};s.objects[key]=model.encoded(old)
        self.assertFalse(store.publish(s,'b',p));self.assertEqual(json.loads(s.objects[key]),old)
        s.objects[key]=model.encoded({**p,'unexpected':True})
        with self.assertRaises(ValueError):store.publish(s,'b',p)
    def test_success_archives_full_predecessor_and_does_not_touch_history(self):
        s,i,p=packet();old=b'{"generated_at":"2026-09-19T12:00:00Z","signal":"STRONG_BUY","nested":{"all":"retained"}}'
        s.objects[store.current('capitulation')]=old;hist='data/capitulation-history.json';s.objects[hist]=b'whole-history'
        self.assertTrue(store.publish(s,'b',p));self.assertEqual(s.objects[model.PRIVATE+model.sha(old)+'.bin'],old);self.assertEqual(s.objects[hist],b'whole-history')
    def test_request_is_idempotent_and_cannot_publish_twice(self):
        s,i=fixture()
        with patch.object(store,'now',return_value=STAMP):
            a=store.run(s,'b','capitulation','same','exec1');count=len(s.writes);b=store.run(s,'b','capitulation','same','exec2')
        self.assertEqual(a,b);self.assertEqual(len(s.writes),count);self.assertTrue(a['published'])
    def test_capture_or_storage_failure_preserves_current_and_reports_failure(self):
        s,i=fixture();key=store.current('capitulation');s.objects[key]=b'{"existing":true}'
        with patch.object(store,'collect',side_effect=RuntimeError('private-error-not-for-public')):
            with self.assertRaises(RuntimeError):store.run(s,'b','capitulation','failed','exec')
        self.assertEqual(s.objects[key],b'{"existing":true}');status=s.objects[store.request_key('capitulation','failed')]
        self.assertNotIn(b'private-error',status);self.assertEqual(json.loads(status)['status'],'failed')
    def test_access_denied_is_not_missing_and_capture_deadline_is_not_partial_success(self):
        s,i=fixture()
        with patch.object(s,'get_object',side_effect=Failure('AccessDenied')):
            with self.assertRaises(Failure):store.collect(s,'b','capitulation',time.monotonic()+30)
        with self.assertRaises(RuntimeError):store.collect(s,'b','capitulation',time.monotonic()-1)
    def test_native_consumer_context_expires_and_mutation_is_rejected(self):
        s,i,p=packet();self.assertTrue(adapter.context(p,AT)['available'])
        self.assertFalse(adapter.context(p,AT+timedelta(hours=4))['available'])
        p['measurements'][0]['value']=999;self.assertFalse(adapter.context(p,AT)['available'])
    def test_schedule_allowances_preserve_existing_cadences(self):
        self.assertEqual(model.deadline('capitulation',AT).isoformat(),'2026-09-20T19:15:00+00:00')
        self.assertEqual(model.deadline('market-extremes',AT).isoformat(),'2026-09-21T01:00:00+00:00')
    def test_no_paid_AI_provider_notification_or_account_dependency(self):
        s,i,p=packet();self.assertFalse(any(key.startswith(('portfolio/','account/')) for key in s.reads))
        for engine in model.INPUTS:
            source=(ROOT/'aws/lambdas'/('justhodl-'+engine)/'source/lambda_function.py').read_text(encoding='utf-8')
            self.assertNotIn('legacy_',source);self.assertNotIn('TELEGRAM',source);self.assertNotIn('anthropic',source)

def run():
    result=unittest.TextTestRunner(verbosity=2).run(unittest.defaultTestLoader.loadTestsFromTestCase(Native))
    if not result.wasSuccessful():raise SystemExit(1)
if __name__=='__main__':run()
