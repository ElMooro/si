"""Run actual native retention, availability and conditional-publication checks."""
from pathlib import Path
from copy import deepcopy
from datetime import datetime, timezone
import hashlib, io, json, sys, unittest, urllib.error
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared'),str(ROOT/'tests')]
import fifx_store as store
import fifx_model as model
import fifx_candidate as candidate
import fifx_acquire as acquisition
import test_fifx_candidate as fixture
import lambda_function as handler

class Conflict(Exception):
    response={'Error':{'Code':'PreconditionFailed'}}

class Missing(Exception):
    response={'Error':{'Code':'NoSuchKey'}}

class Store:
    def __init__(self):self.objects={};self.puts=[];self.force_conflict=False
    def get_object(self,**kw):
        if kw['Key'] not in self.objects:raise Missing()
        raw=self.objects[kw['Key']]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        self.puts.append(kw);old=self.objects.get(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and old is not None:raise Conflict()
        if 'IfMatch' in kw and (self.force_conflict or old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise Conflict()
        self.objects[kw['Key']]=kw['Body']

def private(raw=b'whole predecessor'):
    digest=hashlib.sha256(raw).hexdigest();return {'key':model.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}

def inputs(client,n=42):
    original={};defs={}
    for sid in store.catalog.SOURCES:
        raw=fixture.csv(sid,n=n) if sid in store.catalog.FRED else json.dumps(fixture.quote(sid,n=n)).encode()
        if sid in store.catalog.FRED:defs[sid]=fixture.definition(sid)
        rec=fixture.receipt(sid,raw)
        original[sid]={'original':store.retain_bytes(client,'bucket',raw,'originals','bin'),
                       'receipt':store.retain_bytes(client,'bucket',model.encoded(rec),'receipts')}
    value={'contract':'fifx-vol-inputs.v1','generated_at':fixture.NOW,'sources':original,'macro':{},
        'predecessors':{'packet':private(),'history':private(b'whole history')},'previous_watermarks':model.empty_watermarks(),
        'context':{'source_key':store.BOND,'original':private(b'whole bond'),'status':'retained_unqualified_context','independent_votes':0},
        'acquisition':{'provider_requests':17,'sources':{}}}
    return value,defs

def packet(client):
    value,defs=inputs(client)
    with patch.object(store,'definitions',return_value=defs):result=store.retain(client,'bucket',value)
    return result,value,defs

class Tests(unittest.TestCase):
    def test_qualified_modules_are_exact_and_closed(self):
        store.qualified_arithmetic()
        self.assertEqual(set(store.qualification.QUALIFIED),{'fifx_candidate.py','fifx_catalog.py','fifx_originals.py','fifx_timezones.py','verify_fifx_arithmetic.py'})

    def test_complete_retention_and_original_replay(self):
        client=Store();out,value,defs=packet(client)
        with patch.object(store,'definitions',return_value=defs):proof=store.replay(json.loads(model.encoded(out)),store.reader(client,'bucket'))
        self.assertEqual(len(proof),18);self.assertEqual(sum(p['original_rows'] for p in proof.values()),18*42)
        self.assertEqual(out['decision'],{'verb':'WAIT','meaning':'abstain'})
        self.assertEqual(len(out['series']),18);self.assertIsNone(out['migration']['state'])
        self.assertTrue(all(out[k] is False for k in store.catalog.AUTHORITY))

    def test_original_and_compiler_tampering_fail_replay(self):
        client=Store();out,value,defs=packet(client);run=store.binding(out,store.reader(client,'bucket'))
        for ref in (value['sources']['DGS10']['original'],run['compilers']['fifx_candidate'],run['series']['DEXJPUS']):
            old=client.objects[ref['key']];client.objects[ref['key']]=old+b' '
            with patch.object(store,'definitions',return_value=defs),self.assertRaises(ValueError):store.replay(out,store.reader(client,'bucket'))
            client.objects[ref['key']]=old

    def test_json_type_substitutions_fail_binding_and_full_replay(self):
        client=Store();out,_,defs=packet(client)
        changes=(lambda p:p.update(calls_eligible=0),
                 lambda p:p['series']['DGS10'].update(retained_original_rows=42.0),
                 lambda p:p['series']['DGS10'].update(calls_eligible=0))
        for change in changes:
            changed=deepcopy(out);change(changed)
            with self.assertRaisesRegex(ValueError,'immutable view'):store.binding(changed,store.reader(client,'bucket'))
            with patch.object(store,'definitions',return_value=defs),self.assertRaises(ValueError):store.replay(changed,store.reader(client,'bucket'))

    def test_type_changed_publication_and_same_clock_head_are_rejected(self):
        client=Store();out,_,_=packet(client)
        before=b'{"generated_at":"2026-09-25T21:20:00+00:00"}';client.objects[model.CURRENT]=before
        changed=deepcopy(out);changed['series']['DGS10']['retained_original_rows']=42.0
        with self.assertRaisesRegex(ValueError,'immutable view'):store.publish(client,'bucket',changed)
        self.assertEqual(client.objects[model.CURRENT],before)
        client.objects[model.CURRENT]=model.encoded(changed)
        with self.assertRaisesRegex(ValueError,'same-clock'):store.publish(client,'bucket',out)
        self.assertEqual(client.objects[model.CURRENT],model.encoded(changed))

    def test_post_write_type_change_cannot_receive_successful_readback(self):
        class ChangedStorage(Store):
            def put_object(self,**kw):
                super().put_object(**kw)
                if kw['Key']==model.CURRENT:
                    changed=json.loads(self.objects[model.CURRENT]);changed['series']['DGS10']['retained_original_rows']=42.0
                    self.objects[model.CURRENT]=model.encoded(changed)
        client=ChangedStorage();out,_,_=packet(client)
        client.objects[model.CURRENT]=b'{"generated_at":"2026-09-25T21:20:00+00:00"}'
        with self.assertRaisesRegex(ValueError,'readback'):store.publish(client,'bucket',out)

    def test_ambiguous_or_nonfinite_retained_json_is_rejected(self):
        for raw in (b'{"x":1,"x":1}',b'{"nested":{"x":1,"x":2}}',b'{"x":NaN}',b'{"x":Infinity}',b'{"x":1e999}'):
            with self.assertRaises(ValueError):store.strict(raw)
        self.assertEqual(store.strict(b'{"empty":null,"flag":false,"zero":0}'),{'empty':None,'flag':False,'zero':0})
        client=Store();out,_,_=packet(client);read=store.reader(client,'bucket');run=store.binding(out,read)
        raw=model.encoded(run)
        raw=b'{"contract":"fifx-vol-replay.v1",'+raw[1:]
        key=model.PREFIX+'runs/'+store.sha(raw)+'.json';client.objects[key]=raw
        changed=deepcopy(out);changed['replay']['manifest_key']=key
        with self.assertRaisesRegex(ValueError,'Duplicate'):store.binding(changed,read)

    def test_both_heads_use_the_same_complete_immutable_run(self):
        client=Store();out,_,_=packet(client)
        for key in (model.CURRENT,model.HISTORY):client.objects[key]=b'{"generated_at":"2026-09-25T21:20:00+00:00","whole_legacy":[1,2,3]}'
        self.assertTrue(store.publish(client,'bucket',out));self.assertTrue(store.publish(client,'bucket',out,model.HISTORY))
        self.assertEqual(client.objects[model.CURRENT],client.objects[model.HISTORY])
        self.assertEqual(client.puts[-1]['CacheControl'],'no-store')
        self.assertTrue(store.publish(client,'bucket',out))
        copied=deepcopy(out);copied['call']='LONG'
        with self.assertRaises(ValueError):store.publish(client,'bucket',copied)

    def test_newer_and_same_clock_heads_cannot_be_overwritten(self):
        client=Store();out,_,_=packet(client)
        client.objects[model.CURRENT]=b'{"generated_at":"2026-09-27T00:00:00+00:00"}'
        before=client.objects[model.CURRENT];self.assertFalse(store.publish(client,'bucket',out));self.assertEqual(client.objects[model.CURRENT],before)
        client.objects[model.CURRENT]=model.encoded({'generated_at':out['generated_at']})
        with self.assertRaises(ValueError):store.publish(client,'bucket',out)

    def test_source_regression_and_missing_interval_keep_highwatermarks(self):
        raw=fixture.csv('DGS10',n=50);out=candidate.build_source('DGS10',raw,fixture.receipt('DGS10',raw),fixture.NOW,fixture.definition('DGS10'))
        marks=model.empty_watermarks();marks['DGS10']={'acquired_at':'2026-09-26T07:00:00+00:00','observation_date':'2026-09-26'}
        view,mark=model.compact_source(out,{},marks)
        self.assertIsNone(view['current']);self.assertEqual(view['quality']['status'],'source_regression');self.assertEqual(mark,marks['DGS10'])
        missing=candidate.build_source('DGS10',None,None,fixture.NOW)
        view,mark=model.compact_source(missing,{},marks);self.assertIsNone(view['current']);self.assertEqual(mark,marks['DGS10'])

    def test_duplicate_native_claim_stops_before_provider_or_public_write(self):
        client=Store();key=model.PRIVATE+'requests/'+store.sha(b'native:same-request')+'.json';client.objects[key]=b'{"status":"failed"}'
        with patch.object(acquisition,'acquire') as request,self.assertRaises(Conflict):store.run(client,'bucket','same-request')
        request.assert_not_called();self.assertEqual(len(client.puts),1)

    def test_source_requests_never_duplicate_move_and_retain_whole_http_error(self):
        self.assertEqual(len(acquisition.plan(fixture.NOW)),17);self.assertNotIn('^MOVE',acquisition.plan(fixture.NOW))
        original=b'{"error":"whole 404 original"}'
        response=urllib.error.HTTPError('https://example.invalid',404,'not found',{'Content-Type':'application/json','Set-Cookie':'do not retain'},io.BytesIO(original))
        opener=type('Opener',(),{'open':lambda *a,**k:(_ for _ in ()).throw(response)})()
        with patch.object(acquisition.urllib.request,'build_opener',return_value=opener):raw,rec=acquisition.acquire('https://example.invalid')
        self.assertEqual(raw,original);self.assertEqual(rec['http_status'],404);self.assertNotIn('Set-Cookie',rec['headers'])

    def test_missing_move_does_not_stop_other_sources_and_attempts_are_durable(self):
        client=Store();client.objects[store.SOURCE]=b'{}';client.objects[store.BOND]=b'{}'
        claim=model.PRIVATE+'requests/'+store.sha(b'native:bounded')+'.json'
        predecessors={'packet':private(),'history':private(b'history')};observed=[]
        def acquire(url,timeout):
            journal=json.loads(client.objects[claim]);observed.append(journal['provider_requests'])
            self.assertEqual(sum(v['status']=='attempt_recorded' for v in journal['sources'].values()),1)
            self.assertGreater(timeout,0);self.assertLessEqual(timeout,20)
            raise TimeoutError('provider unavailable')
        with patch.object(store,'previous_state',return_value=(predecessors,model.empty_watermarks())),patch.object(store,'existing_move',side_effect=Missing()),patch.object(acquisition,'acquire',side_effect=acquire),patch.object(store,'retain',return_value={'generated_at':fixture.NOW,'quality':{},'replay':{}}) as retained,patch.object(store,'publish',return_value=True):
            result=store.run(client,'bucket','bounded')
        value=retained.call_args.args[2]
        self.assertEqual(observed,list(range(1,18)));self.assertEqual(result['provider_requests'],17)
        self.assertEqual(set(value['sources']),set(store.catalog.SOURCES))
        self.assertTrue(all(v=={'original':None,'receipt':None} for v in value['sources'].values()))
        self.assertEqual(value['acquisition']['sources']['^MOVE']['status'],'bound_source_unavailable')

    def test_acquisition_budget_skips_remaining_requests_without_retry(self):
        client=Store();client.objects[store.SOURCE]=b'{}'
        predecessors={'packet':private(),'history':private(b'history')}
        with patch.object(store,'previous_state',return_value=(predecessors,model.empty_watermarks())),patch.object(store.time,'monotonic',side_effect=[0]+[101]*17),patch.object(acquisition,'acquire') as acquired,patch.object(store,'retain',return_value={'generated_at':fixture.NOW,'quality':{},'replay':{}}) as retained,patch.object(store,'publish',return_value=True):
            result=store.run(client,'bucket','exhausted')
        acquired.assert_not_called();self.assertEqual(result['provider_requests'],0)
        value=retained.call_args.args[2];self.assertEqual(len(value['sources']),18)
        self.assertTrue(all(value['acquisition']['sources'][sid]['status']=='acquisition_budget_exhausted' for sid in acquisition.plan(fixture.NOW)))
        self.assertEqual(value['context']['status'],'unavailable')

    def test_publication_conflicts_are_bounded_and_preserve_head(self):
        client=Store();out,_,_=packet(client);old=b'{"generated_at":"2026-09-25T21:20:00+00:00"}'
        client.objects[model.CURRENT]=old;client.force_conflict=True
        with self.assertRaisesRegex(RuntimeError,'conflict ceiling'):store.publish(client,'bucket',out)
        self.assertEqual(client.objects[model.CURRENT],old)
        self.assertEqual(sum('IfMatch' in kw for kw in client.puts),4)

    def test_unapproved_replay_paths_are_rejected(self):
        for key in ('audit-private/secret','data/fifx-vol-research/../secret','https://other.invalid','data/portfolio.json'):
            self.assertFalse(store.allowed(key))
        self.assertTrue(store.allowed(model.CURRENT));self.assertTrue(store.allowed(model.HISTORY))

    def test_http_and_validation_never_acquire_or_publish(self):
        with patch.object(handler,'run') as run,patch.object(handler.boto3,'client') as aws:
            self.assertEqual(handler.lambda_handler({'httpMethod':'GET'})['statusCode'],307)
            self.assertEqual(handler.lambda_handler({'requestContext':{'http':{'method':'GET'}}})['statusCode'],307)
            self.assertEqual(handler.lambda_handler({'validate_only':True})['statusCode'],200)
            with self.assertRaises(ValueError):handler.lambda_handler({})
        run.assert_not_called();aws.assert_not_called()

    def test_actual_schedule_preserved_and_capacity_budget_explicit(self):
        config=json.loads((SOURCE.parent/'config.json').read_bytes())
        self.assertEqual(config['schedule'],'cron(20 21 ? * MON-FRI *)')
        self.assertEqual(config['preserved_schedule_reference']['schedule_name'],'justhodl-fifx-vol-daily')
        self.assertEqual(config['memory'],2048);self.assertEqual(config['timeout'],900)
        self.assertEqual((SOURCE.parent/'tests/legacy_lambda_function.py.txt').stat().st_size,19283)

if __name__=='__main__':unittest.main(verbosity=2)
