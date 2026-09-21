"""Whole predecessor preservation, original replay and exactly-once request claims."""
from pathlib import Path
from unittest import mock
import copy, io, json, sys, unittest
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


if __name__=='__main__':unittest.main(verbosity=2)
