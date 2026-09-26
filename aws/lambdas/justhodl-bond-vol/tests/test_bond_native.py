"""Offline native replay, source continuity, conditional writes and authority."""
from pathlib import Path
from copy import deepcopy
from unittest.mock import patch
import gzip,hashlib,importlib.util,io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[4];DRAFT=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(DRAFT),str(ROOT/'aws/shared'),str(ROOT/'tests')]
import bond_vol_model as model
import bond_vol_store as store
from test_bond_vol_candidate import fixture,source,NOW,quote,receipt

class FakeError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self,objects=None):self.objects=dict(objects or {});self.reads=[];self.writes=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise FakeError('NoSuchKey')
        raw=self.objects[key];return {'Body':io.BytesIO(raw),'ETag':store.sha(raw)}
    def put_object(self,**kw):
        old=self.objects.get(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and old is not None:raise FakeError('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or store.sha(old)!=kw['IfMatch']):raise FakeError('PreconditionFailed')
        self.objects[kw['Key']]=kw['Body'];self.writes.append(kw)

def setup():
    originals=fixture(n=65);client=Storage()
    for sid,item in originals.items():
        for part in ('definition','observations'):
            url=item['evidence'][part]['source_url'];raw=model.encoded(item[part]);sha=store.sha(raw)
            key='data/evidence/fred/'+store.sha(url.encode())+'/'+sha+'.bin.gz';client.objects[key]=gzip.compress(raw,mtime=0)
            item['evidence'][part]={'contract':'source-evidence.v1','captured':True,'provider':'fred','source_url':url,
                'first_received_at':NOW,'sha256':sha,'bytes':len(raw),'key':key}
    packet=source(originals);output_sha=packet['replay']['output_sha256'];compiler=Path(store.report_observations.__file__).read_bytes()
    compiler_key='data/report-research/compilers/'+store.sha(compiler)+'.py';client.objects[compiler_key]=compiler
    manifest={'contract':'report-research-replay.v1','generated_at':NOW,'output_sha256':output_sha,
        'compiler':{'key':compiler_key,'sha256':store.sha(compiler)},
        'inputs':{s:{'evidence':o['evidence'],'acquired_at':o['acquired_at']} for s,o in originals.items()}}
    raw=model.encoded(manifest);key='data/report-research/runs/'+store.sha(raw)+'.json';client.objects[key]=raw
    packet['replay']['manifest_key']=key;client.objects[store.SOURCE]=model.encoded(packet)
    client.objects[model.CURRENT]=b'{"generated_at":"2026-09-17T00:00:00Z","complete":"legacy"}'
    client.objects['data/bond-vol-history.json']=b'{"points":[{"date":"2026-09-17","z":null}]}'
    client.objects['data/funding-plumbing.json']=b'{"whole":"funding context"}'
    predecessors,previous=store.previous_state(client,'b');raw=model.encoded(quote());quote_receipt=receipt(raw)
    inputs={'contract':'bond-vol-inputs.v1','generated_at':NOW,'macro':store.retain_bytes(client,'b',client.objects[store.SOURCE],'snapshots'),
        'context':store.context(client,'b'),'predecessors':predecessors,'previous_watermarks':previous,
        'quote':store.retain_bytes(client,'b',raw,'originals','bin'),'quote_receipt':store.retain_bytes(client,'b',model.encoded(quote_receipt),'receipts'),
        'quote_acquisition':{'status':'received'}}
    return client,inputs,packet,originals

class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        client,cls.inputs,cls.source,cls.originals=setup();cls.output=store.compile_output(cls.inputs,store.reader(client,'b'));cls.objects=client.objects
    def setUp(self):self.client=Storage(self.objects);self.read=store.reader(self.client,'b')
    def packet(self):return store.retain(self.client,'b',self.inputs,self.output)

    def test_complete_replay_and_compact_projection_keep_every_original_and_window(self):
        packet=self.packet();self.assertEqual(store.replay(packet,self.read),self.output)
        self.assertEqual(len(packet['series']),10);self.assertEqual(packet['view']['complete_original_rows'],650)
        for sid,row in self.output['series'].items():
            self.assertNotIn('original_rows',packet['series'][sid]);self.assertNotIn('rolling_history',packet['series'][sid])
            self.assertEqual(store.checked(packet['series'][sid]['complete_history_artifact'],'series',self.read),row)
        self.assertNotIn('original',packet['move']);self.assertNotIn('history',packet['move'])
        self.assertEqual(packet['move']['retained_quote_rows'],2);self.assertFalse(packet['calls_eligible'])

    def test_missing_tampered_shards_originals_and_code_cannot_replay(self):
        packet=self.packet();manifest=store.binding(packet,self.read)
        keys=[manifest['series']['DGS2']['key'],manifest['quote']['key'],manifest['compilers']['bond_vol_model']['key'],self.inputs['macro']['key'],self.inputs['quote']['key']]
        for key in keys:
            old=self.client.objects[key];self.client.objects[key]=b'{}'
            with self.assertRaises(ValueError):store.replay(packet,self.read)
            self.client.objects[key]=old

    def test_mutable_head_has_whole_predecessors_and_rejects_newer_or_same_clock_conflicts(self):
        packet=self.packet();self.assertTrue(store.publish(self.client,'b',packet));self.assertTrue(store.publish(self.client,'b',packet))
        before,marks=store.previous_state(self.client,'b');self.assertEqual(before,self.inputs['predecessors']);self.assertEqual(marks,model.watermarks(packet))
        for ref in before.values():self.assertTrue(self.client.objects[ref['key']])
        future=deepcopy(packet);future['generated_at']='2026-09-19T00:00:00Z';self.client.objects[model.CURRENT]=model.encoded(future)
        self.assertFalse(store.publish(self.client,'b',packet))
        same=deepcopy(packet);same['methodology']['scope']='different';self.client.objects[model.CURRENT]=model.encoded(same)
        with self.assertRaises(ValueError):store.publish(self.client,'b',packet)

    def test_publication_boundary_checks_authority_before_storage_access(self):
        packet=self.packet()
        for edit in (lambda p:p.update(calls_eligible=True),lambda p:p.update(regime='NORMAL'),lambda p:p.update(composite_z_score=0),
            lambda p:p['move'].update(sizing_eligible=True),lambda p:p['series']['DGS2'].update(forecast_qualified=True),
            lambda p:p['decision'].update(verb='LONG'),lambda p:p['portfolio_consequences'].update(target_weights={'SPY':1})):
            changed=deepcopy(packet);edit(changed)
            with patch.object(self.client,'get_object',side_effect=AssertionError('No storage access permitted')):
                with self.assertRaises(ValueError):store.publish(self.client,'b',changed)

    def test_replay_reference_cannot_be_borrowed_for_changed_view(self):
        packet=self.packet();packet['series']['DGS2']['current']['max_interval_days']=99
        with self.assertRaises(ValueError):store.publish(self.client,'b',packet)

    def test_acquisition_and_observation_regression_withhold_current_without_losing_history(self):
        previous=deepcopy(self.inputs['previous_watermarks']);previous['DGS2']['acquired_at']='2026-09-18T21:00:00Z'
        previous['DGS10']['observation_date']='2026-09-18'
        output=model.build(self.source,self.originals,'2026-09-18T22:00:00Z',self.inputs['context'],self.inputs['predecessors'],previous,None,None,{'status':'unavailable'})
        for sid in ('DGS2','DGS10'):
            self.assertIsNone(output['series'][sid]['current']);self.assertIsNone(output['series'][sid]['current_distribution'])
            self.assertEqual(output['series'][sid]['quality']['status'],'source_regression');self.assertTrue(output['series'][sid]['rolling_history'])
        self.assertEqual(output['source_watermarks']['DGS2']['acquired_at'],previous['DGS2']['acquired_at'])

    def test_missing_source_recovery_cannot_reset_watermarks(self):
        originals=deepcopy(self.originals);originals['DGS2']=None;packet=source(originals)
        marks=deepcopy(self.inputs['previous_watermarks']);marks['DGS2']={'acquired_at':'2026-09-18T21:00:00Z','observation_date':'2026-09-18'}
        out=model.build(packet,originals,'2026-09-18T22:00:00Z',self.inputs['context'],self.inputs['predecessors'],marks,None,None,{'status':'unavailable'})
        self.assertEqual(out['source_watermarks']['DGS2'],marks['DGS2']);self.assertIsNone(out['series']['DGS2']['current'])
        restored=model.build(self.source,self.originals,'2026-09-18T23:00:00Z',self.inputs['context'],self.inputs['predecessors'],model.watermarks(out),None,None,{'status':'unavailable'})
        self.assertEqual(restored['series']['DGS2']['quality']['status'],'source_regression')

    def test_quote_regression_cannot_promote_an_older_price(self):
        marks=deepcopy(self.inputs['previous_watermarks']);marks['MOVE']={'acquired_at':NOW,'observation_date':'2026-09-18'}
        raw=model.encoded(quote());out=model.build(self.source,self.originals,NOW,self.inputs['context'],self.inputs['predecessors'],marks,raw,receipt(raw),{'status':'received'})
        self.assertIsNone(out['move']['current']);self.assertEqual(out['move']['status'],'source_regression');self.assertEqual(len(out['move']['history']),2)

    def test_native_path_claims_once_and_quote_outage_never_substitutes_rates(self):
        with patch.object(store,'now',return_value=NOW),patch.object(store,'acquire_quote',side_effect=OSError('unavailable')) as acquire:
            result=store.run(self.client,'b','one-native-request')
            with self.assertRaises(FakeError):store.run(self.client,'b','one-native-request')
        self.assertEqual(acquire.call_count,1);self.assertTrue(result['published'])
        packet=json.loads(self.client.objects[model.CURRENT]);self.assertEqual(packet['move']['status'],'unavailable');self.assertIsNone(packet['move']['current'])
        self.assertEqual(packet['quality']['current_series'],10);self.assertEqual(result['notifications_sent'],0)
        self.assertEqual(store.replay(packet,self.read)['contract'],model.CONTRACT)

    def test_successful_native_quote_path_and_full_original_byte_retention(self):
        raw=model.encoded(quote())
        with patch.object(store,'now',return_value=NOW),patch.object(store,'acquire_quote',return_value=(raw,receipt(raw))):
            result=store.run(self.client,'b','quote-native-request')
        self.assertTrue(result['published']);packet=json.loads(self.client.objects[model.CURRENT])
        manifest=store.binding(packet,self.read);inputs=store.checked(manifest['input'],'inputs',self.read)
        self.assertEqual(store.checked(inputs['quote'],'originals',self.read,'bin'),raw)
        self.assertEqual(packet['move']['current']['reported_close'],91.25)

    def test_cas_race_never_overwrites_newer_head(self):
        packet=self.packet();future=deepcopy(packet);future['generated_at']='2026-09-19T00:00:00Z';put=self.client.put_object
        def race(**kw):
            if kw['Key']==model.CURRENT:self.client.objects[model.CURRENT]=model.encoded(future)
            return put(**kw)
        with patch.object(self.client,'put_object',side_effect=race):self.assertFalse(store.publish(self.client,'b',packet))

    def test_http_validation_and_missing_context_never_acquire_or_publish(self):
        spec=importlib.util.spec_from_file_location('bond_native_handler',DRAFT/'lambda_function.py');handler=importlib.util.module_from_spec(spec);spec.loader.exec_module(handler)
        with patch.object(handler.boto3,'client',side_effect=AssertionError('No AWS access')):
            self.assertEqual(handler.lambda_handler({'httpMethod':'GET'},None)['statusCode'],307)
            self.assertEqual(handler.lambda_handler({'httpMethod':'POST'},None)['statusCode'],307)
            self.assertFalse(json.loads(handler.lambda_handler({'validate_only':True},None)['body'])['published'])
            with self.assertRaises(ValueError):handler.lambda_handler({},None)

    def test_unapproved_paths_and_artifact_bounds_fail_before_read(self):
        for key in ('data/portfolio.json','audit-private/anything','data/bond-vol-research/../../x'):
            with self.assertRaises(ValueError):self.read(key)
        self.assertEqual(self.client.reads,[])

    def test_accepted_resources_and_conserved_runtime_schedule(self):
        store.qualified_arithmetic();old=json.loads((DRAFT.parent/'tests/legacy_config.json.txt').read_bytes());new=json.loads((DRAFT.parent/'config.json').read_bytes())
        self.assertEqual(new['preserved_schedule_reference'],old['eventbridge_scheduler']);self.assertEqual(new['schedule'],old['eventbridge_scheduler']['cron'])
        self.assertEqual(new['timeout'],180);self.assertEqual(new['memory'],1024);self.assertNotIn('inherit_env',new)
        self.assertEqual(store.sha((DRAFT.parent/'tests/legacy_lambda_function.py.txt').read_bytes()),'b60af4dcd0a542e93b53ad32b145f15d5ad43f3dfd2e44858fba34c1d5e0c419')

if __name__=='__main__':unittest.main(verbosity=2)
