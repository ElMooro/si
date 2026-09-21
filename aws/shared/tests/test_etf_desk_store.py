"""Desk publication verifies original canonical data and protects complete predecessors."""
from pathlib import Path
from unittest import mock
import copy,json,sys,unittest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).parent)]
import etf_desk_store as store
import etf_desk_model as model
import etf_desk_catalog as catalog
import provider_flow_catalog as flow_catalog
from test_etf_holdings_store import Storage
from test_provider_flow_store import source_fixture
from test_etf_desk_model import fixture as joined_fixture,AT


def fixture():
    db=Storage();i,raw,_,_=joined_fixture(desk=('SPY','VOO','BND'),canonical=())
    db.objects.update(raw)
    old=model.encoded({'generated_at':'2026-09-20T22:20:00Z','whole_predecessor':[0,None,{'extra':'retained'}]})
    for key in set(store.CONTEXTS)|set(store.holdings_store.CONTEXTS):db.objects[key]=old
    db.objects['data/etf-flow-hist/_index.json']=old
    for ticker in ('SPY','VOO','BND','ZZZZ'):
        db.objects['data/etf-flow-hist/'+ticker+'.json']=model.encoded({'generated_at':'2026-09-20T22:20:00Z','d':['2017-04-03','2026-09-18'],'f':[0,1],'preserved':[None,True]})
    fi=source_fixture(db);fi['provider_requests']=2
    fr=store.flow_store.reader(db,'fixture');fo,fh=store.flow_store.compile_output(fi,fr)
    fref=store.flow_store.retain(db,'fixture',fi,fo,fh);db.objects[store.flow_model.CURRENT]=model.encoded({**fo,'replay':fref})
    hi={'contract':'etf-holdings-inputs.v1','kind':'holdings','generated_at':AT,'query_date':'2026-09-21',
        'collections':{t:i['extra_holdings'][t] for t in ('SPY','VOO')},'contexts':store.holdings_store.preserve(db,'fixture'),
        'previous':None,'provider_requests':8,'original_provider_bytes':0}
    hr=store.holdings_store.reader(db,'fixture')
    ho=store.holdings_store.compile_output(hi,hr,lambda k,b:store.holdings_store.immutable(db,'fixture',k,b))
    href=store.holdings_store.retain(db,'fixture',hi,ho,hr);db.objects[store.holdings_model.CURRENT]=model.encoded({**ho,'replay':href})
    read=store.reader(db,'fixture')
    i.update(contexts=store.preserve(db,'fixture',read),canonical_flows=store.snapshot(db,'fixture',store.flow_model.CURRENT,read),
        canonical_holdings=store.snapshot(db,'fixture',store.holdings_model.CURRENT,read),
        extra_flows={'BND':i['extra_flows']['BND']},extra_holdings={'BND':i['extra_holdings']['BND']})
    return db,i


class RetainedDesk(unittest.TestCase):
    def setUp(self):
        p=mock.patch.object(catalog,'DESK',('SPY','VOO','BND'));p.start();self.addCleanup(p.stop)
        p=mock.patch.dict(flow_catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True);p.start();self.addCleanup(p.stop)
    def native(self,db,inputs):
        read=store.reader(db,'fixture')
        output=store.compile_output(inputs,read,lambda k,b:store.immutable(db,'fixture',k,b,read=read))
        ref=store.retain(db,'fixture',inputs,output,read)
        return {**output,'replay':ref}
    def test_canonical_originals_profiles_and_every_artifact_replay(self):
        db,i=fixture();packet=self.native(db,i)
        self.assertEqual(store.replay(packet['replay'],store.reader(db,'fixture')),{k:v for k,v in packet.items() if k!='replay'})
        key=packet['funds']['BND']['profiles']['current']['snapshot']['key'];db.objects[key]+=b' '
        with self.assertRaises(ValueError):store.replay(packet['replay'],store.reader(db,'fixture'))
    def test_all_predecessors_and_unconfigured_histories_survive(self):
        db,i=fixture();read=store.reader(db,'fixture')
        self.assertIn('data/etf-flow-hist/ZZZZ.json',i['contexts'])
        for key,ref in i['contexts'].items():
            if ref:self.assertEqual(store.protected(ref,read),db.objects[key])
        db.objects['data/etf-flow-hist/ZZZZ.json']=b'{"changed_after_preservation":true}'
        self.assertEqual(store.preserve(db,'fixture',read),i['contexts'])
    def test_durable_request_invokes_collector_once_and_aliases_bind_same_run(self):
        db,i=fixture();payload={k:i[k] for k in ('query_date','profiles','extra_flows','extra_holdings','provider_requests','original_provider_bytes')}
        with mock.patch.object(store,'collect',return_value=payload) as collect,mock.patch.object(store,'now',return_value=AT):
            result=store.run(db,'fixture','once','execution-1');again=store.run(db,'fixture','once','execution-2')
        self.assertEqual(result,again);self.assertEqual(collect.call_count,1);self.assertTrue(result['published'])
        self.assertEqual(result['compatibility_publications'],dict.fromkeys(store.ALIASES,True))
        for target in store.ALIASES:
            alias=json.loads(db.objects[target]);self.assertEqual(alias['canonical']['replay'],result['replay'])
            self.assertEqual(alias['fund_inventory'],['BND','SPY','VOO']);self.assertEqual(alias['by_stock'],{})
            self.assertTrue(all(alias[k] is False for k in model.PERMISSIONS))
        for k in ('private_account_reads','paid_ai_calls','notifications_sent','signals_emitted','portfolio_writes'):self.assertEqual(result[k],0)
    def test_exact_compiler_required_and_retained_source_never_executed(self):
        db,i=fixture();packet=self.native(db,i);run=json.loads(db.objects[packet['replay']['manifest_key']])
        key=run['compilers']['etf_desk_model']['key'];db.objects[key]=b'raise RuntimeError("must not execute")'
        with self.assertRaises(ValueError):store.replay(packet['replay'],store.reader(db,'fixture'))
    def test_prior_publication_requires_exact_research_contract_and_authority(self):
        db,i=fixture();packet=self.native(db,i)
        for field,value in (('contract','unreviewed'),('sizing_eligible',True)):
            changed=copy.deepcopy(packet);changed[field]=value
            with self.subTest(field=field),self.assertRaisesRegex(ValueError,'prior desk contract'):
                store.recorded_output(changed,store.reader(db,'fixture'))
    def test_source_qualified_candidate_compiler_replays_only_exact_reviewed_bytes(self):
        db,i=fixture();packet=self.native(db,i);run=json.loads(db.objects[packet['replay']['manifest_key']])
        raw=(Path(__file__).resolve().parents[3]/'tests/fixtures/etf-desk-store-pre-publisher.py.txt').read_bytes()
        digest=model.sha(raw);self.assertIn(digest,store.COMPATIBLE_COMPILERS['etf_desk_store'])
        key=model.PREFIX+'compilers/'+digest+'.py';db.objects[key]=raw
        run['compilers']['etf_desk_store']={'key':key,'sha256':digest}
        body=model.encoded(run);manifest=model.PREFIX+'runs/'+model.sha(body)+'.json';db.objects[manifest]=body
        ref={**packet['replay'],'manifest_key':manifest}
        self.assertEqual(store.replay(ref,store.reader(db,'fixture')),{k:v for k,v in packet.items() if k!='replay'})
        db.objects[key]+=b' '
        with self.assertRaises(ValueError):store.replay(ref,store.reader(db,'fixture'))
    def test_explicit_publisher_keeps_conditional_write_and_private_request_record(self):
        db,i=fixture();packet=self.native(db,i);calls=[]
        def publisher(client,bucket,key,raw,condition):
            calls.append((key,condition));client.put_object(Bucket=bucket,Key=key,Body=raw,**condition)
        self.assertTrue(store.conditional(db,'fixture',model.CURRENT,packet,publisher))
        self.assertEqual(calls,[(model.CURRENT,{'IfNoneMatch':'*'})])
        self.assertTrue(store.request_key('private-request').startswith(store.PRIVATE+'requests/'))
        self.assertNotIn('private-request',store.request_key('private-request'))
    def test_publication_same_clock_and_component_regressions_are_rejected(self):
        db,i=fixture();packet=self.native(db,i)
        self.assertTrue(store.conditional(db,'fixture',model.CURRENT,packet))
        for family in ('profiles','holdings'):
            changed=copy.deepcopy(packet);changed['generated_at']='2026-09-21T11:00:00+00:00'
            changed['funds']['BND'][family]['current']['processed_date']='2026-09-17'
            self.assertFalse(store.conditional(db,'fixture',model.CURRENT,changed))
        changed=copy.deepcopy(packet);changed['quality']['status']='wrong'
        with self.assertRaises(ValueError):store.conditional(db,'fixture',model.CURRENT,changed)
    def test_prior_profile_is_preserved_separately_on_failed_current_collection(self):
        db,i=fixture();packet=self.native(db,i);store.conditional(db,'fixture',model.CURRENT,packet)
        read=store.reader(db,'fixture');i['previous']=store.snapshot(db,'fixture',model.CURRENT,read)
        i['profiles']['BND']['current'].update(status='provider_http_error',selection=None,pages=[])
        updated=self.native(db,i);profiles=updated['funds']['BND']['profiles']
        self.assertIsNone(profiles['current']['summary']);self.assertFalse(profiles['current']['quality']['current_profile_eligible'])
        self.assertEqual(profiles['retained_previous_current'],packet['funds']['BND']['profiles']['current'])
        self.assertFalse(profiles['retained_previous_eligible_as_current'])
    def test_recovery_replays_retained_run_without_collecting_or_reaging(self):
        db,i=fixture();packet=self.native(db,i)
        with mock.patch.object(store,'collect') as collect,mock.patch.object(store,'now',return_value='2026-09-21T11:00:00+00:00'):
            result=store.run(db,'fixture','recover','recovery-execution',recover_run=packet['replay'])
        collect.assert_not_called();self.assertTrue(result['published'])
        self.assertEqual(result['generated_at'],AT);self.assertEqual(result['provider_requests_this_execution'],0)
        self.assertEqual(json.loads(db.objects[model.CURRENT]),packet)
        with mock.patch.object(store,'now',return_value='2026-10-01T00:00:00Z'),self.assertRaises(ValueError):
            store.recovery_inputs(packet['replay'],store.reader(db,'fixture'))

    def test_metadata_does_not_make_same_original_conflicting_but_bytes_do(self):
        raw=b'{}';key=model.profile.PRIVATE+model.sha(raw)+'.bin';ref={'key':key,'bytes':2,'sha256':model.sha(raw)};calls=[]
        store.warm([{**ref,'source_key':'one'},{**ref,'source_key':'two'}],lambda k:calls.append(k) or raw)
        self.assertEqual(calls,[key])
        with self.assertRaises(ValueError):store.warm([ref,{**ref,'bytes':3}],lambda k:self.fail('Do not read a conflicting reference'))
    def test_private_paths_and_mutable_account_keys_cannot_enter_reader_or_writer(self):
        db=Storage();read=store.reader(db,'fixture')
        for key in ('private/account.json','data/portfolio.json','audit-private/unreviewed.bin','data/etf-desk-research/requests/'+'a'*64+'.json'):
            with self.subTest(key=key),self.assertRaises(ValueError):read(key)
        self.assertEqual(db.reads,[])
        with self.assertRaises(ValueError):store.conditional(db,'fixture','data/portfolio.json',{'generated_at':AT})
        self.assertEqual(db.writes,[])

if __name__=='__main__':unittest.main(verbosity=2)
