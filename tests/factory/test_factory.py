"""Adversarial contracts and a complete student -> isolated grader -> lake tick."""
import copy
import hashlib
import importlib.util
import io
import json
import runpy
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / 'aws/shared'))
sys.path.insert(0, str(ROOT / 'aws/lambdas/justhodl-student-rsi/source'))
# Tests use only in-memory clients; production boto3 is supplied by Lambda.
try:
    import boto3
except ImportError:
    sys.modules['boto3'] = types.ModuleType('boto3')
    sys.modules['botocore'] = types.ModuleType('botocore')
    config = types.ModuleType('botocore.config'); config.Config = lambda **kw: kw
    sys.modules['botocore.config'] = config

from factory_core import Invalid, canonical, digest, iso, make_season, promotion_decision, retain_fact, score_prediction, seal_state, select_state, validate_prediction, validate_task, week_window
from factory_repair import candidates, propose, render
from factory_store import Busy, Conflict, Store
import student_lambda as student


def module(name, path):
    spec = importlib.util.spec_from_file_location(name, str(ROOT / path))
    loaded = importlib.util.module_from_spec(spec); spec.loader.exec_module(loaded)
    return loaded

grader = module('factory_test_grader', 'aws/lambdas/justhodl-factory-grader/source/lambda_function.py')
gateway = module('factory_test_gateway', 'aws/lambdas/justhodl-ai/source/factory_gateway.py')


class CloudError(Exception):
    def __init__(self, code):
        self.response = {'Error': {'Code': code}}


class MemoryS3:
    def __init__(self):
        self.rows, self.fail_put, self.denied_get = {}, set(), set()
    def get_object(self, Bucket, Key):
        if (Bucket, Key) in self.denied_get:
            raise CloudError('AccessDenied')
        row = self.rows.get((Bucket, Key))
        if row is None:
            raise CloudError('NoSuchKey')
        return {'Body': io.BytesIO(row), 'ETag': '"' + hashlib.md5(row).hexdigest() + '"'}
    def put_object(self, Bucket, Key, Body, **kw):
        if (Bucket, Key) in self.fail_put:
            raise CloudError('InternalError')
        if isinstance(Body, str): Body = Body.encode()
        old = self.rows.get((Bucket, Key))
        if kw.get('IfNoneMatch') == '*' and old is not None:
            raise CloudError('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or kw['IfMatch'] != '"' + hashlib.md5(old).hexdigest() + '"'):
            raise CloudError('PreconditionFailed')
        self.rows[(Bucket, Key)] = Body
        return {'ETag': '"' + hashlib.md5(Body).hexdigest() + '"'}
    def list_objects_v2(self, Bucket, Prefix, MaxKeys=1000, **kwargs):
        keys = sorted(k for b, k in self.rows if b == Bucket and k.startswith(Prefix))
        keys = [k for k in keys if k > kwargs.get('StartAfter', '')]
        start = int(kwargs.get('ContinuationToken', 0)); chunk = keys[start:start + MaxKeys]
        result = {'Contents': [{'Key': key} for key in chunk]}
        if start + MaxKeys < len(keys): result['NextContinuationToken'] = str(start + MaxKeys)
        return result


class FactoryTests(unittest.TestCase):
    def setUp(self):
        self.now = datetime(2026, 9, 13, 15, tzinfo=timezone.utc)
        self.cloud = MemoryS3()
        self.store = Store(self.cloud, 'private', 'public', lambda: self.now)
        self.season = make_season(self.now)
        self.season.update(calendar_review_required=False, price_sources={s:'official-consolidated:'+s for s in ('SPY','QQQ','IWM','TLT','GLD','BTC')})
        self.season.pop('policy_hash'); self.season['policy_hash'] = digest(self.season)
        self.policy = {'enabled': True, 'max_grades_per_day': 50, 'max_experiments_per_day': 1, 'max_tick_seconds': 40,
            'max_guest_traces_per_day': 10, 'sense_interval_seconds': 900, 'objective': 'verified usefulness',
            'generative_status': 'blocked_no_verified_generative_model'}
        for key, value in {'factory/control/policy.json': self.policy, 'factory/control/season.json': self.season,
            'factory/control/invites.json': {'allowlist': [{'uid':'verified-uid','agent':'alice','enabled':True}], 'capacity':10},
            'factory/exams/code-identity-v1.json': {'id':'exam-v1','seed':'secret-fixture-should-never-publish','cases':64,'frozen':True}}.items():
            self.store.immutable('private', key, value)
    def state(self, version=1):
        s = student.initial_state(self.now, self.season, self.policy); s['state_version'] = version
        return seal_state(s)
    def prediction(self):
        return {'id':'test-prediction','week':'2026-09-14','symbol':'SPY','direction':'UP','regime':'TREND',
            'crisis_probability': .2, 'direction_probabilities':{'DOWN':.2,'FLAT':.2,'UP':.6},
            'regime_probabilities':{'RANGE':.2,'TRANSITION':.2,'TREND':.6},'data_cutoff':'2026-09-11T20:00:00Z',
            'model_revision':'research-v1','price_source':'official-consolidated:SPY'}
    def guest_event(self, role='user', uid='verified-uid'):
        return {'headers':{'x-jh-factory-role':role,'x-jh-factory-uid':uid}}
    def test_all_unapproved_workers_and_envelopes_rejected(self):
        for e in ({'task':'iam:CreateRole','task_id':'x'},{'mode':'train'},{'task':'run-coding-battery','task_id':'x','arguments':{'command':'curl'}}, {'requestContext':{}}):
            with self.assertRaises(Invalid): validate_task(e)
    def test_private_transaction_heals_partial_public_write(self):
        self.store.acquire(); state = self.state()
        self.cloud.fail_put.add(('public','data/student-state.json'))
        with self.assertRaises(Conflict): self.store.commit_state(state, None)
        recovered, etag = self.store.load_state()
        self.assertEqual(recovered['state_version'],1)
        self.cloud.fail_put.clear(); self.store.publish_mirrors(recovered)
        self.assertEqual(self.cloud.rows[('public','student-state.json')], self.cloud.rows[('public','data/student-state.json')])
        with self.assertRaises(Conflict): self.store.commit_state(self.state(), etag)
    def test_corrupt_authority_recovers_and_never_initializes_over_existing_state(self):
        self.cloud.rows[('private', Store.CURRENT)] = b'{broken'
        self.store.immutable('public','student-state.json',self.state(8))
        recovered, etag = self.store.load_state(); self.assertEqual(recovered['state_version'],8)
        self.store.acquire(); self.store.commit_state(self.state(9), etag)
        self.assertEqual(self.store.load_state()[0]['state_version'],9)
        self.cloud.rows[('private', Store.CURRENT)] = b'{}'
        for key in list(self.cloud.rows):
            if key[0]=='public' or 'snapshots' in key[1]: del self.cloud.rows[key]
        with self.assertRaises(Invalid): self.store.load_state()
    def test_access_denial_does_not_turn_existing_private_object_into_absence(self):
        self.cloud.denied_get.add(('private','factory/control/policy.json'))
        with self.assertRaises(CloudError): self.store.read('private','factory/control/policy.json')
    def test_stale_writer_cannot_overwrite_newer_state(self):
        self.store.acquire(); second = Store(self.cloud,'private','public',lambda:self.now)
        with self.assertRaises(Busy): second.acquire()
        self.now += timedelta(seconds=151); second.acquire()
        with self.assertRaises(Busy): self.store.commit_state(self.state(),None)
    def test_checksum_rejects_tampering_and_equal_version_conflicts(self):
        s=self.state(); bad=copy.deepcopy(s); bad['gen']=40
        self.assertEqual(select_state([bad,s]),s)
        other=self.state(); other['gen']=1; other=seal_state(other)
        with self.assertRaises(Invalid):select_state([s,other])
    def test_append_only_wall_retains_exact_prior_bytes_and_rejects_edit(self):
        self.store.acquire(); a=self.store.append_event('prediction','one',{'x':1},public=True)
        self.store.jsonl_view('factory/salon/wall.jsonl',[a]); first=self.cloud.rows[('public','factory/salon/wall.jsonl')]
        b=self.store.append_event('prediction','two',{'x':2},public=True)
        self.store.jsonl_view('factory/salon/wall.jsonl',[b])
        self.assertTrue(self.cloud.rows[('public','factory/salon/wall.jsonl')].startswith(first))
        with self.assertRaises(Conflict): self.store.jsonl_view('factory/salon/wall.jsonl',[{**a,'data':{'x':3}}])
    def test_missing_source_keeps_value_and_old_observation_time(self):
        prior={'value':4.1,'observed_at':'2026-09-10T00:00:00Z','source':'NY Fed','unit':'percent'}
        fact=retain_fact(prior,None,self.now,max_age_seconds=1)
        self.assertEqual(fact['value'],4.1); self.assertEqual(fact['observed_at'],prior['observed_at'])
        self.assertTrue(fact['retained'] and fact['stale'])
        for value in (float('nan'),float('inf'),True):
            result=retain_fact(None,{**prior,'value':value},self.now,max_age_seconds=1)
            self.assertIsNone(result['value'])
    def test_submission_boundary_dst_and_holiday_session(self):
        p=self.prediction(); open_at=datetime(2026,9,14,13,30,tzinfo=timezone.utc)
        self.assertEqual(validate_prediction(p,self.season,open_at,'alice')['agent'],'alice')
        for now in (open_at-timedelta(seconds=1),open_at+timedelta(minutes=5)):
            with self.assertRaises(Invalid):validate_prediction(p,self.season,now,'alice')
        self.assertTrue(week_window('2026-11-23',self.season)['closes_at'].endswith('18:00:00+00:00'))
        self.assertTrue(week_window('2026-11-02',self.season)['opens_at'].endswith('14:30:00+00:00'))
        self.assertEqual(week_window('2026-09-07',self.season)['sessions'][0],'2026-09-08')
    def test_future_cutoff_and_reward_hacking_vectors_rejected(self):
        p=self.prediction(); now=datetime(2026,9,14,13,31,tzinfo=timezone.utc)
        p['data_cutoff']='2027-01-01T00:00:00Z'
        with self.assertRaises(Invalid):validate_prediction(p,self.season,now,'alice')
        p=self.prediction(); p['direction_probabilities']['UP']=4
        with self.assertRaises(Invalid):validate_prediction(p,self.season,now,'alice')
        c={'evaluation_id':'one','independent':True,'held_out':True,'n':64,'critical_failures':0,'score':.8}
        self.assertFalse(promotion_decision(c,{**c,'score':.9})['eligible'])
        self.assertFalse(promotion_decision(c,{**c,'evaluation_id':'two','score':.5})['eligible'])
    def test_auth_identity_and_one_entry_per_week(self):
        self.now=datetime(2026,9,14,13,31,tzinfo=timezone.utc)
        p=self.prediction()
        with self.assertRaises(Invalid):gateway.handle(self.guest_event(uid='intruder'),'POST','/factory/predictions',p,self.store)
        result=gateway.handle(self.guest_event(),'POST','/factory/predictions',p,self.store)
        self.assertEqual(result['id'],'2026-09-14-alice-SPY')
        p['id']='different-id'; p['direction']='DOWN'
        with self.assertRaises(Conflict):gateway.handle(self.guest_event(),'POST','/factory/predictions',p,self.store)
        with self.assertRaises(Invalid):gateway.handle(self.guest_event(),'POST','/factory/control',{'enabled':False},self.store)
    def test_private_views_require_admission_and_cannot_select_arbitrary_keys(self):
        self.store.immutable('public','data/student-state.json',self.state())
        result=gateway.handle(self.guest_event(),'GET','/factory/view',{'kind':'state'},self.store)
        self.assertEqual(json.loads(result['raw'])['checksum'],self.state()['checksum'])
        for body in ({'kind':'state','key':'factory/exams/private.json'},{'kind':'control'},{'kind':'event','id':'../control/policy'}):
            with self.assertRaises(Invalid):gateway.handle(self.guest_event(),'GET','/factory/view',body,self.store)
        with self.assertRaises(Invalid):gateway.handle(self.guest_event(uid='not-invited'),'GET','/factory/view',{'kind':'state'},self.store)
    def test_guest_fails_are_not_training_eligible(self):
        trace={'domain':'math','task':{'operation':'add','a':'.1','b':'.2','answer':'.4'},
               'provenance':{'license':'original','source':'test'},'solution_notes':'A proposed answer'}
        receipt=gateway.handle(self.guest_event(),'POST','/factory/traces',trace,self.store)
        verdict=grader.handle({'kind':'trace','id':receipt['id']},self.store)
        self.assertFalse(verdict['ok']); self.assertFalse(verdict['training_eligible']);self.assertFalse(verdict['elo_eligible'])
        trace['task']['answer']='.3'
        receipt=gateway.handle(self.guest_event(),'POST','/factory/traces',trace,self.store)
        self.assertTrue(grader.handle({'kind':'trace','id':receipt['id']},self.store)['ok'])
        trace['solution_notes']='iam:CreateRole'
        with self.assertRaises(Invalid):gateway.handle(self.guest_event(),'POST','/factory/traces',trace,self.store)
    def test_official_grading_never_substitutes_warehouse(self):
        self.now=datetime(2026,9,14,13,31,tzinfo=timezone.utc)
        receipt=gateway.handle(self.guest_event(),'POST','/factory/predictions',self.prediction(),self.store)
        self.now=datetime(2026,9,18,20,11,tzinfo=timezone.utc)
        result=grader.grade_market(self.store,receipt['id'])
        self.assertEqual(result['status'],'pending');self.assertEqual(result['reason'],'official_prints_unavailable')
        self.assertIsNone(self.store.read('private','factory/salon/results/'+receipt['id']+'.json')[0])
    def test_complete_loop_independent_exam_pass_reject_and_retry(self):
        class LocalLambda:
            def invoke(inner, **kw):
                result=grader.handle(json.loads(kw['Payload']),self.store)
                return {'Payload':io.BytesIO(canonical(result))}
        original=student.sense
        student.sense=lambda store,previous,now: {'observed_at':iso(now),'funding':{},'tape':{},'oss':{}}
        try:
            outcome=student.tick({},self.store,LocalLambda())
            self.assertTrue(outcome['ok']);self.assertEqual(outcome['errors'],[])
            state=self.store.load_state()[0]
            self.assertEqual(state['gen'],1);self.assertEqual(len(state['skillbook']),1)
            self.assertEqual(state['fit']['n'],64);self.assertGreater(state['fit']['score'],state['fit']['baseline'])
            self.now+=timedelta(minutes=1)
            student.tick({},self.store,LocalLambda())
            self.assertEqual(self.store.load_state()[0]['gen'],1)
            published=b''.join(v for (b,k),v in self.cloud.rows.items() if b=='public')
            self.assertNotIn(b'secret-fixture-should-never-publish',published)
            self.assertIn(('private','factory/queue/deployment-create-identity-v1.json'),self.cloud.rows)
        finally:student.sense=original
    def test_candidate_renderer_is_the_actual_deployment_helper(self):
        proposal=propose()
        self.assertEqual(proposal['source'],(ROOT/'scripts/lambda_identity.py').read_text())
        identity=runpy.run_path(str(ROOT/'scripts/lambda_identity.py'))['resolve_identity']
        role='arn:aws:iam::857687956942:role/justhodl-student-rsi-role'
        self.assertEqual(identity({'role':role,'handler':'student_lambda.lambda_handler'}),{'role':role,'handler':'student_lambda.lambda_handler'})
        shell=(ROOT/'scripts/deploy_lambdas.sh').read_text()
        create=shell[shell.index('create-function --'):shell.index('create-function --')+1300] if 'create-function --' in shell else shell[shell.index('--role "$fn_role"')-200:shell.index('--role "$fn_role"')+400]
        self.assertIn('--role "$fn_role"',create);self.assertIn('--handler "$fn_handler"',create)

if __name__=='__main__':unittest.main(verbosity=2)
