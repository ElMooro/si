"""Whole predecessor preservation, original replay and exactly-once request claims."""
from pathlib import Path
from unittest import mock
import copy, io, json, sys, unittest, threading
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).parent)]
import etf_holdings_store as store
import etf_holdings_model as model
import etf_holdings_native as native
from test_etf_holdings_model import Projection, GENERATED


class ObjectError(Exception):
    def __init__(self, code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self):self.objects={};self.reads=[];self.writes=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise ObjectError('NoSuchKey')
        raw=self.objects[key];return {'Body':io.BytesIO(raw),'ETag':native.sha(raw)}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if kw.get('IfNoneMatch')=='*' and old is not None:raise ObjectError('PreconditionFailed')
        if 'IfMatch' in kw and (old is None or native.sha(old)!=kw['IfMatch']):raise ObjectError('PreconditionFailed')
        self.objects[key]=kw['Body'];self.writes.append(key)
    def get_paginator(self,name):
        if name!='list_objects_v2':raise AssertionError('Unreviewed inventory')
        storage=self
        class Paginator:
            def paginate(self,**kw):
                yield {'Contents':[{'Key':k} for k in sorted(storage.objects) if k.startswith(kw['Prefix'])]}
        return Paginator()


def fixture():
    db=Storage();inputs,raw=Projection().inputs();db.objects.update(raw)
    prior=native.encoded({'generated_at':'2026-09-20T22:00:00Z','all_previous_fields':[0,None,{'preserved':True}]})
    for key in store.CONTEXTS:db.objects[key]=prior
    db.objects['etf-flows/constituents/SPY.json']=prior
    db.objects['etf-constituents-v2/SPY.json']=prior
    db.objects['etf-flows/constituents/ZZZZ.json']=prior
    inputs['contexts']=store.preserve(db,'fixture')
    return db,inputs


class RetainedHoldings(unittest.TestCase):
    def setUp(self):
        self.catalog=mock.patch.dict(model.catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'}},clear=True)
        self.catalog.start();self.addCleanup(self.catalog.stop)

    def native(self,db,inputs):
        out=store.compile_output(inputs,store.reader(db,'fixture'),lambda k,b:store.immutable(db,'fixture',k,b))
        ref=store.retain(db,'fixture',inputs,out)
        return {**out,'replay':ref}

    def test_original_replay_and_projection_require_whole_row_parts_and_compilers(self):
        db,inputs=fixture();packet=self.native(db,inputs)
        self.assertEqual(store.replay(packet['replay'],store.reader(db,'fixture')),{k:v for k,v in packet.items() if k!='replay'})
        store.conditional(db,'fixture',model.CURRENT,packet)
        look_inputs={'contract':'etf-lookthrough-inputs.v1','kind':'lookthrough','generated_at':GENERATED,
            'canonical_source':store.snapshot(db,'fixture',model.CURRENT),'previous':store.snapshot(db,'fixture',model.LOOK_CURRENT)}
        looked=self.native(db,look_inputs)
        self.assertEqual(looked['funds'],packet['funds']);self.assertEqual(looked['provider_requests'],0)
        row_key=next(k for k in db.objects if k.startswith(model.PREFIX+'rows/'))
        original=db.objects[row_key];db.objects[row_key]+=b' '
        with self.assertRaises(ValueError):store.replay(looked['replay'],store.reader(db,'fixture'))
        db.objects[row_key]=original
        run=json.loads(db.objects[packet['replay']['manifest_key']])
        key=run['compilers']['etf_holdings_native']['key'];db.objects[key]=b'raise RuntimeError("never execute retained source")'
        with self.assertRaises(ValueError):store.replay(packet['replay'],store.reader(db,'fixture'))

    def test_whole_legacy_caches_including_unconfigured_fund_are_retained(self):
        db,inputs=fixture()
        self.assertIn('etf-flows/constituents/ZZZZ.json',inputs['contexts'])
        for key,ref in inputs['contexts'].items():self.assertEqual(store.protected(ref,store.reader(db,'fixture')),db.objects[key])
        expected=copy.deepcopy(inputs['contexts']);db.objects[store.CONTEXTS[0]]=b'{"changed_after_preservation":true}'
        self.assertEqual(store.preserve(db,'fixture'),expected)

    def test_durable_claim_and_aliases_do_not_recollect_or_emit_signals(self):
        db,inputs=fixture();fake=mock.Mock();fake.query_date=inputs['query_date']
        fake.collect.return_value=(inputs['collections'],inputs['provider_requests'],inputs['original_provider_bytes'])
        with mock.patch.object(store.collector,'Collector',return_value=fake),mock.patch.object(store,'now',return_value=GENERATED):
            result=store.run(db,'fixture','holdings','once','execution-1')
            repeated=store.run(db,'fixture','holdings','once','execution-2')
        self.assertEqual(result,repeated);self.assertEqual(fake.collect.call_count,1)
        self.assertTrue(result['published']);self.assertEqual(result['compatibility_publications'],dict.fromkeys(store.ALIASES,True))
        for field in ('signals_emitted','paid_ai_calls','notifications_sent','private_account_reads','portfolio_writes'):self.assertEqual(result[field],0)
        for key in store.ALIASES:
            alias=json.loads(db.objects[key]);self.assertEqual(alias['per_stock_exposure'],{})
            self.assertTrue(all(alias[field] is False for field in model.PERMISSIONS))

    def test_new_compilation_cannot_regress_processing_vintage_or_same_clock_body(self):
        db=Storage();base={'contract':model.CONTRACT,'generated_at':GENERATED,
            'funds':{'SPY':{'current':{'processed_date':'2026-09-18'},'prior':{'processed_date':'2026-08-21'}}}}
        self.assertTrue(store.conditional(db,'fixture',model.CURRENT,base))
        earlier={**base,'generated_at':'2026-09-21T06:59:00Z'}
        self.assertFalse(store.conditional(db,'fixture',model.CURRENT,earlier))
        changed=copy.deepcopy(base);changed['generated_at']='2026-09-21T08:00:00Z'
        changed['funds']['SPY']['current']['processed_date']='2026-09-17'
        self.assertFalse(store.conditional(db,'fixture',model.CURRENT,changed))
        with self.assertRaises(ValueError):store.conditional(db,'fixture',model.CURRENT,{**base,'altered':True})
        self.assertEqual(json.loads(db.objects[model.CURRENT]),base)

    def test_reader_never_reads_private_accounts_and_bounded_cache_evicts(self):
        db=Storage();read=store.reader(db,'fixture',capacity=15)
        for key in ('portfolio/current.json','config/api-key.json',native.PRIVATE+'../bad.bin'):
            with self.assertRaises(ValueError):read(key)
        self.assertEqual(db.reads,[])
        refs=[store.protect(db,'fixture',b'1234567890'+bytes([i])) for i in range(3)]
        db.reads=[]
        for ref in refs:read(ref['key'])
        read(refs[-1]['key']);self.assertEqual(len(db.reads),3)
        read(refs[0]['key']);self.assertEqual(len(db.reads),4)

    def test_failure_keeps_current_whole_and_records_request_without_exception_detail(self):
        db,inputs=fixture();original=b'{"previous_publication":true}';db.objects[model.CURRENT]=original
        fake=mock.Mock();fake.collect.side_effect=RuntimeError('sensitive-provider-detail')
        with mock.patch.object(store.collector,'Collector',return_value=fake),self.assertRaises(RuntimeError):
            store.run(db,'fixture','holdings','failed-once','execution')
        self.assertEqual(db.objects[model.CURRENT],original)
        status=json.loads(db.objects[store.request_key('holdings','failed-once')])
        self.assertEqual(status['status'],'failed');self.assertNotIn('sensitive-provider-detail',json.dumps(status))

    def test_failed_current_observation_retains_separate_previous_dated_snapshot(self):
        db,inputs=fixture();first=self.native(db,inputs);store.conditional(db,'fixture',model.CURRENT,first)
        inputs=copy.deepcopy(inputs);inputs['previous']=store.snapshot(db,'fixture',model.CURRENT)
        inputs['collections']['SPY']['current']={'ticker':'SPY','cutoff':'2026-09-21','pages':[],
            'selection':None,'status':'provider_http_error','http_status':503}
        next_=self.native(db,inputs)
        self.assertEqual(next_['funds']['SPY']['current']['quality']['status'],'unavailable')
        self.assertEqual(next_['funds']['SPY']['retained_previous_current'],first['funds']['SPY']['current'])
        self.assertEqual(next_['quality']['complete_returned_snapshots'],0)

    def test_parallel_writer_has_bounded_concurrency_and_verified_cache(self):
        db=Storage();read=store.reader(db,'fixture');barrier=threading.Barrier(3);lock=threading.Lock()
        active=0;peak=0;written=[]
        def write(client,bucket,key,body):
            nonlocal active,peak
            with lock:active+=1;peak=max(peak,active)
            barrier.wait(timeout=5)
            db.objects[key]=body;written.append(key)
            with lock:active-=1
        with mock.patch.object(store,'immutable',side_effect=write):
            with store.ArtifactWriter(db,'fixture',read,workers=3,pending_limit=6) as emit:
                for i in range(15):
                    raw=native.encoded({'row':i});key=model.PREFIX+'rows/'+native.sha(raw)+'.json'
                    emit(key,raw);self.assertLessEqual(len(emit.pending),6)
        self.assertEqual(len(written),15);self.assertEqual(peak,3)
        for key in written:self.assertEqual(read(key),db.objects[key])
        self.assertEqual(db.reads,[])
        with self.assertRaises(ValueError):read.remember(written[0],b'wrong content')
        with self.assertRaises(ValueError):store.ArtifactWriter(db,'fixture',read,workers=7)

    def test_failed_parallel_write_preserves_inputs_before_publication(self):
        db,inputs=fixture();old=b'{"previous_whole":true}';db.objects[model.CURRENT]=old
        fake=mock.Mock();fake.query_date=inputs['query_date'];fake.collect.return_value=(inputs['collections'],4,inputs['original_provider_bytes'])
        original=store.immutable
        def fail_rows(client,bucket,key,body,*args):
            if '/rows/' in key:raise RuntimeError('private-error-detail')
            return original(client,bucket,key,body,*args)
        with mock.patch.object(store.collector,'Collector',return_value=fake),mock.patch.object(store,'immutable',side_effect=fail_rows),mock.patch.object(store,'now',return_value=GENERATED):
            with self.assertRaises(RuntimeError):store.run(db,'fixture','holdings','write-failed','execution')
        self.assertEqual(db.objects[model.CURRENT],old)
        status=json.loads(db.objects[store.request_key('holdings','write-failed')])
        self.assertEqual((status['status'],status['phase']),('failed','compile'))
        retained=store.checked(status['retained_input'],model.PREFIX,'inputs',store.reader(db,'fixture'))
        self.assertEqual(retained['collections'],inputs['collections'])
        self.assertNotIn('private-error-detail',json.dumps(status));self.assertNotIn('candidate_replay',status)

    def old_io_run(self,db,inputs):
        packet=self.native(db,inputs);ref=packet['replay'];run=json.loads(db.objects[ref['manifest_key']])
        old=(Path(__file__).resolve().parents[3]/'tests/fixtures/etf-holdings-store-pre-io.py.txt').read_bytes()
        digest=native.sha(old);self.assertIn(digest,store.COMPATIBLE_IO_STORES)
        key=model.PREFIX+'compilers/'+digest+'.py';db.objects[key]=old
        run['compilers']['etf_holdings_store']={'key':key,'sha256':digest}
        raw=native.encoded(run);key=model.PREFIX+'runs/'+native.sha(raw)+'.json';db.objects[key]=raw
        return {**packet,'replay':{'manifest_key':key,'output_sha256':ref['output_sha256']}}

    def test_reviewed_io_predecessor_replays_and_recovers_without_collection(self):
        db,inputs=fixture();old=self.old_io_run(db,inputs);read=store.reader(db,'fixture')
        self.assertEqual(store.replay(old['replay'],read),{k:v for k,v in old.items() if k!='replay'})
        prior_request=store.request_key('holdings','timed-out');db.objects[prior_request]=b'{"status":"running","phase":"retained_replay"}'
        with mock.patch.object(store.collector,'Collector') as collect,mock.patch.object(store,'now',return_value=GENERATED):
            recovered=store.run(db,'fixture','holdings','recover-once','new-execution',recover_run=old['replay'])
            repeated=store.run(db,'fixture','holdings','recover-once','repeat-execution',recover_run=old['replay'])
        collect.assert_not_called();self.assertEqual(recovered,repeated);self.assertTrue(recovered['published'])
        self.assertEqual(recovered['provider_requests_this_execution'],0);self.assertEqual(recovered['provider_requests'],4)
        self.assertEqual(recovered['generated_at'],old['generated_at']);self.assertEqual(recovered['recovered_from'],old['replay'])
        self.assertEqual(recovered['replay']['output_sha256'],old['replay']['output_sha256'])
        self.assertNotEqual(recovered['replay']['manifest_key'],old['replay']['manifest_key'])
        self.assertEqual(recovered['candidate_replay'],recovered['replay'])
        self.assertEqual(db.objects[prior_request],b'{"status":"running","phase":"retained_replay"}')

    def test_recovery_rejects_unreviewed_compilers_expired_clocks_and_changed_output(self):
        db,inputs=fixture();old=self.old_io_run(db,inputs);ref=old['replay']
        with mock.patch.object(store,'now',return_value='2026-09-24T07:00:00Z'):
            with self.assertRaises(ValueError):store.recovery_inputs(ref,store.reader(db,'fixture'))
        run=json.loads(db.objects[ref['manifest_key']]);unknown=b'not reviewed'
        compiler=model.PREFIX+'compilers/'+native.sha(unknown)+'.py';db.objects[compiler]=unknown
        changed=copy.deepcopy(run);changed['compilers']['etf_holdings_store']={'key':compiler,'sha256':native.sha(unknown)}
        raw=native.encoded(changed);key=model.PREFIX+'runs/'+native.sha(raw)+'.json';db.objects[key]=raw
        with self.assertRaises(ValueError):store.replay({**ref,'manifest_key':key},store.reader(db,'fixture'))
        output=json.loads(db.objects[run['output']['key']]);output['quality']['exact_provider_identities']=999
        raw=native.encoded(output);key=model.PREFIX+'outputs/'+native.sha(raw)+'.json';db.objects[key]=raw
        run['output']={'key':key,'sha256':native.sha(raw),'bytes':len(raw)};run['output_sha256']=native.sha(raw)
        raw=native.encoded(run);key=model.PREFIX+'runs/'+native.sha(raw)+'.json';db.objects[key]=raw
        wrong={'manifest_key':key,'output_sha256':run['output_sha256']};current=db.objects.get(model.CURRENT)
        with mock.patch.object(store.collector,'Collector') as collect,mock.patch.object(store,'now',return_value=GENERATED):
            with self.assertRaises(RuntimeError):store.run(db,'fixture','holdings','wrong-output','execution',recover_run=wrong)
        collect.assert_not_called();self.assertEqual(db.objects.get(model.CURRENT),current)

    def test_recovery_is_not_available_to_projection_or_arbitrary_paths(self):
        db,inputs=fixture()
        with mock.patch.object(store.collector,'Collector') as collect:
            with self.assertRaises(RuntimeError):store.run(db,'fixture','lookthrough','wrong-kind','execution',recover_run={'anything':True})
            for key in ('portfolio/snapshot.json','https://unreviewed.invalid/','data/etf-holdings-research/runs/../../private.json'):
                with self.assertRaises(ValueError):store.recovery_inputs({'manifest_key':key,'output_sha256':'0'*64},store.reader(db,'fixture'))
        collect.assert_not_called()

    def test_pinned_pre_bounds_compilers_recompute_the_same_original_output(self):
        db,inputs=fixture();packet=self.native(db,inputs);ref=packet['replay']
        run=json.loads(db.objects[ref['manifest_key']])
        base=Path(__file__).resolve().parents[3]/'tests/fixtures'
        for module in ('etf_holdings_native','etf_holdings_store'):
            body=(base/(module.replace('_','-')+'-pre-bounds.py.txt')).read_bytes();digest=native.sha(body)
            self.assertIn(digest,store.COMPATIBLE_COMPILERS[module])
            key=model.PREFIX+'compilers/'+digest+'.py';db.objects[key]=body
            run['compilers'][module]={'key':key,'sha256':digest}
        raw=native.encoded(run);key=model.PREFIX+'runs/'+native.sha(raw)+'.json';db.objects[key]=raw
        previous={'manifest_key':key,'output_sha256':ref['output_sha256']}
        self.assertEqual(store.replay(previous,store.reader(db,'fixture')),{k:v for k,v in packet.items() if k!='replay'})
        # A compatible digest cannot be assigned to a different numerical compiler.
        run['compilers']['etf_holdings_model']=run['compilers']['etf_holdings_native']
        raw=native.encoded(run);key=model.PREFIX+'runs/'+native.sha(raw)+'.json';db.objects[key]=raw
        with self.assertRaises(ValueError):store.replay({**previous,'manifest_key':key},store.reader(db,'fixture'))


if __name__=='__main__':unittest.main(verbosity=2)
