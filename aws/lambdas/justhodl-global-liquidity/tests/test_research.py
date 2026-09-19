from copy import deepcopy
from datetime import datetime,timezone
import ast,gzip,hashlib,io,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(ROOT/'scripts'),str(Path(__file__).resolve().parents[1]/'source')]
import global_liquidity_research as model
import global_liquidity_store as store
from global_liquidity_calendar import POLICY
from global_liquidity_readthrough import context
from report_observations import build,encoded,digest
from evidence_store import capture
from test_report_observations import inputs,NOW
from replay_global_liquidity import replay


def fixture():
    values={'WALCL':'6746548','WTREGEN':'877028','RRPONTSYD':'0','ECBASSETSW':'5911237',
            'JPNASSETS':'6446620','DEXUSEU':'1.1604','DEXJPUS':'153.71','M2SL':'23218'}
    originals={}
    for sid in model.SERIES:
        unit,freq=POLICY[sid][:2] if sid in POLICY else ('Millions of U.S. Dollars','W') if sid=='WTREGEN' else ('Billions of US Dollars','D') if sid=='RRPONTSYD' else ('Billions of Dollars','M')
        rows=[('2026-09-18',values[sid])]
        if sid=='JPNASSETS':rows=[('2026-08-01',values[sid]),('2026-07-01','6442957')]
        if sid=='M2SL':rows=[('2026-07-01','23218'),('2025-07-01','22025.5')]
        originals[sid]=inputs(sid,freq,rows,unit)
        originals[sid]['definition']['seriess'][0]['frequency']='Monthly, End of Period' if sid=='JPNASSETS' else freq
    source=build({sid:['research',sid] for sid in model.SERIES},originals,NOW)
    source['replay']={'manifest_key':'data/report-research/runs/'+'a'*64+'.json','output_sha256':digest(source)}
    return source,originals


class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self):self.objects={};self.race=False
    def get_object(self,**kw):
        key=kw['Key']
        if key not in self.objects:raise StorageError('NoSuchKey')
        raw=self.objects[key];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        key=kw['Key'];raw=kw['Body']
        if key==store.CURRENT and self.race:
            self.race=False;self.objects[key]=encoded({'generated_at':'2099-01-01T00:00:00Z'});raise StorageError('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and key in self.objects:raise StorageError('PreconditionFailed')
        if kw.get('IfMatch') and (key not in self.objects or hashlib.sha256(self.objects[key]).hexdigest()!=kw['IfMatch']):raise StorageError('PreconditionFailed')
        self.objects[key]=raw


def prepared():
    client=Storage();_,original=fixture()
    for item in original.values():
        for part in ('definition','observations'):
            item['evidence'][part]=capture(client,'fixture','fred',item['evidence'][part]['source_url'],encoded(item[part]),received_at=datetime.fromisoformat(NOW))
    source=build({sid:['research',sid] for sid in model.SERIES},original,NOW)
    body=Path(store.report_observations.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest()
    compiler={'key':'data/report-research/compilers/'+sha+'.py','sha256':sha};client.objects[compiler['key']]=body
    manifest={'contract':'report-research-replay.v1','generated_at':NOW,
        'inputs':{sid:{'evidence':item['evidence'],'acquired_at':item['acquired_at']} for sid,item in original.items()},
        'compiler':compiler,'output_sha256':digest(source)}
    key='data/report-research/runs/'+digest(manifest)+'.json';client.objects[key]=encoded(manifest)
    source['replay']={'manifest_key':key,'output_sha256':manifest['output_sha256'],'compiler_sha256':sha}
    client.objects['data/report-measurements.json']=encoded(source)
    return client,source,original


class Frozen(datetime):
    @classmethod
    def now(cls,tz=None):return datetime.fromisoformat(NOW)


class Tests(unittest.TestCase):
    def test_unfinished_month_end_cannot_become_a_current_measurement(self):
        _,original=fixture();original['JPNASSETS']['observations']['observations'][0]['date']='2026-09-01'
        source=build({sid:['research',sid] for sid in model.SERIES},original,NOW)
        source['replay']={'manifest_key':'data/report-research/runs/'+'a'*64+'.json','output_sha256':digest(source)}
        out=model.build(source,original,NOW)
        self.assertEqual(out['series']['JPNASSETS']['quality']['status'],'incomplete_measurement_period')
        self.assertIsNone(out['series']['JPNASSETS']['current_decimal'])
        self.assertIsNone(out['three_bank_subtotal']['total_usd_millions_decimal'])

    def test_active_handler_never_routes_to_legacy_or_discloses_errors(self):
        path=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
        node=next(n for n in ast.parse(path.read_text(encoding='utf-8')).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        scope={'s3':object(),'S3_BUCKET':'fixture','json':json}
        exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
        with patch.object(store,'run',return_value={'published':True,'history_published':True}) as run:
            self.assertEqual(scope['lambda_handler']({'action':'legacy'},None)['statusCode'],200);run.assert_called_once()
        with patch.object(store,'run',return_value={'published':False,'history_published':False}):
            self.assertEqual(scope['lambda_handler']()['statusCode'],409)
        with patch.object(store,'run',side_effect=ValueError('secret query value')):
            result=scope['lambda_handler']();self.assertEqual(result['statusCode'],503);self.assertNotIn('secret',result['body'])

    def test_three_bank_scope_native_units_and_zero_rrp(self):
        source,original=fixture();out=model.build(source,original,NOW)
        self.assertEqual(out['quality']['fresh_series'],8)
        self.assertEqual(out['us_net_liquidity_proxy']['net'],6746548-877028)
        self.assertEqual(out['three_bank_subtotal']['components']['JPNASSETS']['balance']['selected']['effective_observation_date'],'2026-08-31')
        self.assertEqual(out['us_m2']['year_comparison']['pct_change'],5.414179)
        self.assertIsNone(out['us_m2']['growth_acceleration'])
        self.assertFalse(out['calls_eligible']);self.assertIsNone(out['global_impulse_13w_pct'])
        self.assertIsNone(out['global_liquidity_index']['total_usd_bn'])
        self.assertEqual(out['decision']['meaning'],'abstain')

    def test_stale_current_is_unavailable_but_dated_history_retained(self):
        source,original=fixture();out=model.build(source,original,'2026-09-20T20:00:00Z')
        self.assertIsNone(out['three_bank_subtotal']['total_usd_millions_decimal'])
        self.assertEqual(out['series']['WALCL']['last_observed_value'],'6746548')
        self.assertEqual(out['calendar_research']['status'],'CURRENT_VINTAGE_RESEARCH')
        self.assertFalse(out['calendar_research']['point_in_time'])

    def test_changed_original_or_forged_measurement_cannot_compile(self):
        source,original=fixture();bad=deepcopy(original);bad['WALCL']['observations']['observations'][0]['value']='1'
        with self.assertRaises(ValueError):model.build(source,bad,NOW)
        source['measurements']['WALCL']['current']=1
        with self.assertRaises(ValueError):model.build(source,original,NOW)

    def test_unknown_unit_blocks_conversion_instead_of_magnitude_guess(self):
        source,original=fixture();original['ECBASSETSW']['definition']['seriess'][0]['units']='Billions of Euros'
        source=build(source['catalog'],original,NOW);source['replay']={'manifest_key':'data/report-research/runs/'+'a'*64+'.json','output_sha256':digest(source)}
        out=model.build(source,original,NOW)
        self.assertEqual(out['three_bank_subtotal']['missing_components'],['ECBASSETSW'])
        self.assertIsNone(out['three_bank_subtotal']['total_usd_millions_decimal'])

    def test_store_full_original_replay_and_legacy_byte_preservation(self):
        client,_,original=prepared();old=b'{malformed old publication';history=encoded({'snapshots':[{'impulse_13w':99}]})
        client.objects[store.CURRENT]=old;client.objects[store.HISTORY]=history
        with patch.object(store,'datetime',Frozen):result=store.run(client,'fixture')
        self.assertTrue(result['published']);self.assertTrue(result['history_published'])
        read=store.raw_reader(client,'fixture');packet=json.loads(read(store.CURRENT));manifest=json.loads(read(packet['replay']['manifest_key']))
        self.assertEqual(replay(manifest,read),{k:v for k,v in packet.items() if k!='replay'})
        self.assertEqual(read(packet['legacy_context'][store.CURRENT]['key']),old)
        self.assertEqual(read(packet['legacy_context'][store.HISTORY]['key']),history)
        ref=original['WALCL']['evidence']['observations'];client.objects[ref['key']]=gzip.compress(b'{}')
        with self.assertRaises(ValueError):replay(manifest,read)

    def test_wrong_original_blocks_all_current_publication(self):
        client,_,original=prepared();ref=original['WALCL']['evidence']['definition'];client.objects[ref['key']]=gzip.compress(b'{}')
        with patch.object(store,'datetime',Frozen),self.assertRaises(ValueError):store.run(client,'fixture')
        self.assertNotIn(store.CURRENT,client.objects);self.assertNotIn(store.HISTORY,client.objects)

    def test_race_never_replaces_newer_current_or_publishes_older_history(self):
        client,_,_=prepared();client.race=True
        with patch.object(store,'datetime',Frozen):result=store.run(client,'fixture')
        self.assertFalse(result['published']);self.assertFalse(result['history_published'])
        self.assertEqual(json.loads(client.objects[store.CURRENT])['generated_at'],'2099-01-01T00:00:00Z')

    def test_readthrough_only_exposes_fresh_scoped_original_bound_data(self):
        source,original=fixture();out=model.build(source,original,NOW);out['replay']={'output_sha256':digest(out)}
        view=context(out,datetime.fromisoformat(NOW));self.assertEqual(view['status'],'descriptive')
        self.assertEqual(set(view['components_usd_bn']),{'Fed','Eurosystem','BOJ'})
        self.assertEqual(view['independent_votes'],0)
        self.assertEqual(context(out,datetime(2026,9,21,tzinfo=timezone.utc))['status'],'unavailable')
        out['three_bank_subtotal']['total_usd_millions_decimal']='1'
        self.assertEqual(context(out,datetime.fromisoformat(NOW))['components_usd_bn'],{})
        self.assertEqual(context({'components':{'Fed':1,'PBOC':99}},datetime.fromisoformat(NOW))['components_usd_bn'],{})


if __name__=='__main__':unittest.main()
