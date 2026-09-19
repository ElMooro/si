from copy import deepcopy
from datetime import datetime
import gzip,hashlib,io,json,sys,unittest
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(ROOT/'scripts'),str(Path(__file__).resolve().parents[1]/'source')]
import cb_native as native
import cb_research as model
import cb_store as store
from cb_research_catalog import extend_catalog
from report_observations import build,encoded,digest
from evidence_store import capture
from test_report_observations import inputs,NOW
from replay_cb_research import replay


class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self):self.objects={};self.metadata={}
    def get_object(self,**kw):
        key=kw['Key']
        if key not in self.objects:raise StorageError('NoSuchKey')
        raw=self.objects[key]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest(),'Metadata':self.metadata.get(key,{})}
    def put_object(self,**kw):
        key=kw['Key'];raw=kw['Body']
        if kw.get('IfNoneMatch')=='*' and key in self.objects:raise StorageError('PreconditionFailed')
        if kw.get('IfMatch') and (key not in self.objects or hashlib.sha256(self.objects[key]).hexdigest()!=kw['IfMatch']):raise StorageError('PreconditionFailed')
        self.objects[key]=raw;self.metadata[key]=kw.get('Metadata',{})


def seal(source):
    source['replay']={'manifest_key':'data/report-research/runs/'+'a'*64+'.json','output_sha256':digest({k:v for k,v in source.items() if k!='replay'})}
    return source


def fixture():
    originals={}
    for sid,(unit,freq,target,factor) in native.POLICY.items():
        if freq=='M':rows=[('2026-08-01','-0.045' if sid=='IR3TIB01CHM156N' else '6000000' if sid=='JPNASSETS' else '1.5'),('2026-07-01','-0.04' if sid=='IR3TIB01CHM156N' else '5800000' if sid=='JPNASSETS' else '1.4')]
        else:rows=[('2026-09-16','100'),('2026-08-12','90')]
        if sid in ('WALCL','WSHOSHO','WLCFLPCL','SWPT'):
            pair={'WALCL':(120000,100000),'WSHOSHO':(80000,75000),'WLCFLPCL':(10000,5000),'SWPT':(1000,1000)}[sid]
            rows=[('2026-09-16',str(pair[0])),('2026-08-12',str(pair[1]))]
        originals[sid]=inputs(sid,freq,rows,unit)
        originals[sid]['definition']['seriess'][0]['frequency']='Monthly, End of Period' if sid=='JPNASSETS' else freq
    source=build(extend_catalog({}),originals,NOW)
    return seal(source),originals


def ecb_fixture(client,name,values=None):
    flow=native.ECB[name]
    values=values or [('2026-W37','120000' if name=='total_assets' else '80000' if name=='monetary_policy_securities' else '10000','A'),
                      ('2026-W32','100000' if name=='total_assets' else '75000' if name=='monetary_policy_securities' else '5000','A')]
    raw=('KEY,FREQ,UNIT,UNIT_MULT,TIME_PERIOD,OBS_VALUE,OBS_STATUS\n'+''.join(f'{flow.replace("/",".",1)},W,EUR,6,{d},{v},{s}\n' for d,v,s in values)).encode()
    receipt=capture(client,'fixture','ecb','https://data-api.ecb.europa.eu/service/data/'+flow,raw,received_at=datetime.fromisoformat(NOW))
    return {'raw':raw,'evidence':receipt,'acquired_at':NOW}


def prepared():
    client=Storage();_,original=fixture()
    for item in original.values():
        for part in ('definition','observations'):
            item['evidence'][part]=capture(client,'fixture','fred',item['evidence'][part]['source_url'],encoded(item[part]),received_at=datetime.fromisoformat(NOW))
    source=build(extend_catalog({}),original,NOW)
    body=Path(store.report_observations.__file__).read_bytes();sha=hashlib.sha256(body).hexdigest()
    compiler={'key':'data/report-research/compilers/'+sha+'.py','sha256':sha};client.objects[compiler['key']]=body
    manifest={'contract':'report-research-replay.v1','generated_at':NOW,'inputs':{sid:{'evidence':item['evidence'],'acquired_at':item['acquired_at']} for sid,item in original.items()},'compiler':compiler,'output_sha256':digest(source)}
    key='data/report-research/runs/'+digest(manifest)+'.json';client.objects[key]=encoded(manifest)
    source['replay']={'manifest_key':key,'output_sha256':manifest['output_sha256'],'compiler_sha256':sha}
    client.objects['data/report-measurements.json']=encoded(source)
    ecb={name:ecb_fixture(client,name) for name in native.ECB}
    return client,source,original,ecb


class Research(unittest.TestCase):
    def test_exact_native_replay_stock_reconciliation_and_negative_nonpolicy_rate(self):
        client,source,original,ecb=prepared();out=model.build(source,original,ecb,{},NOW)
        fed=out['central_banks'][0]['decomposition']
        self.assertEqual(fed['stock_change_1m_decimal'],'20.000')
        self.assertEqual(fed['other_assets_and_adjustments_change_1m_decimal'],'10.000')
        self.assertEqual(fed['reconciliation_residual'],0)
        self.assertEqual(out['central_banks'][3]['interbank_proxy_pct'],-.045)
        self.assertIsNone(out['central_banks'][3]['policy_rate_pct'])
        self.assertEqual(out['central_banks'][3]['rate']['changes']['1']['change_unit'],'percentage_points')
        self.assertIsNone(out['global_injection_impulse']['score']);self.assertFalse(out['sizing_eligible'])

    def test_latest_missing_is_preserved_not_backfilled(self):
        source,original=fixture();original['WALCL']['observations']['observations'][0]['value']='.'
        source=seal(build(extend_catalog({}),original,NOW))
        rows=native.fred_inputs(source,original);m=native.measure(rows['WALCL'],NOW)
        self.assertEqual(m['quality']['status'],'missing_observation');self.assertIsNone(m['latest'])
        self.assertEqual(m['selected']['original_row_index'],0)
        self.assertIsNone(model.decompose(rows,'USD_bn',model.FED,NOW)['stock_change_1m'])

    def test_wrong_units_and_unfinished_month_end_are_withheld(self):
        source,original=fixture();original['JPNASSETS']['definition']['seriess'][0]['units']='Billions of Yen'
        source=seal(build(extend_catalog({}),original,NOW));m=native.measure(native.fred_inputs(source,original)['JPNASSETS'],NOW)
        self.assertEqual(m['quality']['status'],'invalid_definition');self.assertIsNone(m['latest'])
        source,original=fixture();original['JPNASSETS']['observations']['observations'][0]['date']='2026-09-01'
        source=seal(build(extend_catalog({}),original,NOW));m=native.measure(native.fred_inputs(source,original)['JPNASSETS'],NOW)
        self.assertEqual(m['quality']['status'],'incomplete_measurement_period');self.assertIsNone(m['latest'])

    def test_stale_acquisition_cannot_be_renewed_by_processing_time(self):
        source,original=fixture();series=native.fred_inputs(source,original)['WALCL'];series['acquired_at']='2026-09-16T00:00:00Z'
        m=native.measure(series,NOW);self.assertEqual(m['quality']['status'],'stale_source');self.assertIsNone(m['latest'])

    def test_provider_definition_drift_is_not_silently_accepted(self):
        source,original=fixture();original['WALCL']['definition']['seriess'][0]['seasonal_adjustment']='Seasonally Adjusted'
        source=seal(build(extend_catalog({}),original,NOW))
        self.assertEqual(native.measure(native.fred_inputs(source,original)['WALCL'],NOW)['quality']['status'],'invalid_definition')

    def test_ecb_last_good_retains_original_acquisition_clock_after_fetch_failure(self):
        c=Storage();item=ecb_fixture(c,'total_assets');item['acquired_at']='2026-09-16T00:00:00Z'
        descriptor={k:v for k,v in item.items() if k!='raw'}
        c.objects[store.PREFIX+'ecb-cache/total_assets.json']=encoded(descriptor)
        opener=type('Opener',(),{'open':lambda *a,**k:(_ for _ in ()).throw(TimeoutError('provider URL'))})()
        with patch.object(store,'now',return_value=NOW),patch.object(store.urllib.request,'build_opener',return_value=opener):
            returned,error=store.acquire_ecb(c,'fixture','total_assets',store.raw_reader(c,'fixture'))
        self.assertEqual(error,'TimeoutError');self.assertEqual(returned['acquired_at'],item['acquired_at'])
        self.assertEqual(native.quality(native.ecb_input('total_assets',returned['raw'],returned['evidence'],returned['acquired_at']),NOW)['status'],'stale_source')

    def test_source_tamper_and_boolean_observation(self):
        source,original=fixture();source['measurements']['WALCL']['current_decimal']='999'
        with self.assertRaises(ValueError):native.fred_inputs(source,original)
        source,original=fixture();original['WALCL']['observations']['observations'][0]['value']=True
        source=seal(build(extend_catalog({}),original,NOW));m=native.measure(native.fred_inputs(source,original)['WALCL'],NOW)
        self.assertIsNone(m['latest'])

    def test_ecb_null_status_and_calendar_baselines_do_not_compact(self):
        c=Storage();item=ecb_fixture(c,'total_assets',[('2026-W37','.','A'),('2026-W36','100','A')])
        row=native.ecb_input('total_assets',**{'raw':item['raw'],'receipt':item['evidence'],'acquired_at':NOW})
        self.assertIsNone(native.measure(row,NOW)['latest']);self.assertEqual(len(row['rows']),2)
        item=ecb_fixture(c,'total_assets',[('2026-W37','120','L'),('2026-W32','100','A')])
        row=native.ecb_input('total_assets',item['raw'],item['evidence'],NOW)
        self.assertIsNone(native.measure(row,NOW)['latest'])
        item=ecb_fixture(c,'total_assets',[('2026-W37','120','A'),('2026-W33','100','A')])
        row=native.ecb_input('total_assets',item['raw'],item['evidence'],NOW)
        self.assertIsNone(native.measure(row,NOW)['changes']['1']['level_change'])

    def test_ecb_original_identity_and_duplicate_rejection(self):
        c=Storage();item=ecb_fixture(c,'total_assets');broken={**item['evidence'],'sha256':'0'*64}
        with self.assertRaises(ValueError):native.ecb_input('total_assets',item['raw'],broken,NOW)
        with self.assertRaises(ValueError):native.ecb_input('monetary_policy_lending',item['raw'],item['evidence'],NOW)
        item=ecb_fixture(c,'total_assets',[('2026-W37','1','A'),('2026-W37','1','A')])
        with self.assertRaises(ValueError):native.ecb_input('total_assets',item['raw'],item['evidence'],NOW)

    def test_missing_or_excess_components_never_become_a_zero_residual(self):
        source,original=fixture();rows=native.fred_inputs(source,original)
        rows['WLCFLPCL']['rows'][0]['value_decimal']=None
        self.assertIsNone(model.decompose(rows,'USD_bn',model.FED,NOW)['reconciliation_residual'])
        rows=native.fred_inputs(source,original);rows['WSHOSHO']['rows'][0]['value_decimal']='200'
        self.assertEqual(model.decompose(rows,'USD_bn',model.FED,NOW)['status'],'invalid_components_exceed_total')

    def test_settlement_scopes_never_borrow_and_units_are_required(self):
        d={'treasury':{'as_of':'2026-09-09','ftd_bn':None,'ftr_bn':100,'gross_bn':190,'scope_id':'treasury_incl_tips','quality':{'status':'fresh'},'field_units':{'ftd_bn':'usd_bn','ftr_bn':'usd_bn','gross_bn':'usd_bn'}},
           'headline':{'as_of':'2026-09-09','ftd_bn':86,'ftr_bn':87,'combined_bn':173,'scope':'ust_ex_tips','quality':{'status':'fresh'},'field_units':{'ftd_bn':'usd_bn','ftr_bn':'usd_bn','combined_bn':'usd_bn'}}}
        out=model.fails_context(d,NOW);self.assertIsNone(out['ftd_bn']);self.assertEqual(out['ust_ex_tips']['combined_bn'],173)
        self.assertFalse(out['original_provider_verified']);self.assertFalse(out['reconciliation']['consistent'])
        d['headline']['field_units']['ftd_bn']='usd_millions';out=model.fails_context(d,NOW)
        self.assertIsNone(out['ust_ex_tips']['ftd_bn']);self.assertEqual(out['ust_ex_tips']['quality']['status'],'unverified_units')

    def test_store_replays_originals_preserves_legacy_and_registered_archives(self):
        c,source,original,ecb=prepared();legacy=b'{"old":true}';c.objects[store.CURRENT]=legacy
        with patch.object(store,'now',return_value=NOW),patch.object(store,'acquire_ecb',side_effect=lambda client,bucket,name,read:(ecb[name],None)):
            result=store.run(c,'fixture')
        self.assertTrue(result['published']);out=json.loads(c.objects[store.CURRENT])
        manifest=json.loads(c.objects[out['replay']['manifest_key']])
        self.assertEqual(replay(manifest,store.raw_reader(c,'fixture')),{k:v for k,v in out.items() if k!='replay'})
        self.assertEqual(c.objects[out['legacy_context'][store.CURRENT]['key']],legacy)
        for family in ('snapshots','measurements'):
            archive=json.loads(c.objects['data/cb-injection/'+family+'/'+NOW[:10]+'.json'])
            self.assertIsNone(archive['impulse']);self.assertEqual(archive['replay'],out['replay'])
        key=next(iter(original.values()))['evidence']['observations']['key'];c.objects[key]=gzip.compress(b'{}')
        with self.assertRaises(ValueError):replay(manifest,store.raw_reader(c,'fixture'))

    def test_older_generation_or_source_cannot_replace_current(self):
        c=Storage();old={'contract':model.CONTRACT,'generated_at':NOW,'source_generated_at':NOW};c.objects[store.CURRENT]=encoded(old)
        self.assertFalse(store.publish(c,'fixture',store.CURRENT,{**old,'generated_at':'2026-09-17T00:00:00Z'}))
        self.assertFalse(store.publish(c,'fixture',store.CURRENT,{**old,'generated_at':'2026-09-19T00:00:00Z','source_generated_at':'2026-09-17T00:00:00Z'}))
        self.assertEqual(json.loads(c.objects[store.CURRENT]),old)


if __name__=='__main__':unittest.main()
