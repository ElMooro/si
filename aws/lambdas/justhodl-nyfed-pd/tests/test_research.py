import ast
from copy import deepcopy
from datetime import date,datetime,timedelta
import gzip,hashlib,io,json
from pathlib import Path
import sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(Path(__file__).resolve().parents[1]/'source')]
import dealer_original as n
import dealer_research as m
import dealer_research_store as s
from evidence_store import capture
AT='2026-09-19T11:00:00+00:00'


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
        key=kw['Key']
        if kw.get('IfNoneMatch')=='*' and key in self.objects:raise StorageError('PreconditionFailed')
        if kw.get('IfMatch') and (key not in self.objects or kw['IfMatch']!=hashlib.sha256(self.objects[key]).hexdigest()):raise StorageError('PreconditionFailed')
        self.objects[key]=kw['Body'];self.metadata[key]=kw.get('Metadata',{})


def observations(count=55):
    return {'pd':{'timeseries':[{'keyid':key,'asofdate':(date(2026,9,9)-timedelta(days=7*i)).isoformat(),
                                'value':str(1000+index*10+i)} for index,key in enumerate(n.KEYS) for i in range(count)]}}


def fixture():
    defs=deepcopy(n.DEFINITIONS);raws={}
    for key in defs:
        raws[key]=b'%PDF-'+key.encode();defs[key]['sha256']=hashlib.sha256(raws[key]).hexdigest()
    reference=deepcopy(n.REGISTRY['reference_release']);raws['historical_release']=b'%PDF-archived-2022'
    reference['sha256']=hashlib.sha256(raws['historical_release']).hexdigest()
    raws.update(catalog=n.encoded({'pd':{'timeseries':[n.SERIES[k]['catalog'] for k in n.KEYS]}}),
                breaks=n.encoded({'pd':{'seriesbreaks':[{'seriesbreak':k,'startdate':v['start'],'enddate':v['end']} for k,v in defs.items()]}}),
                release_schedule=b'<p>Thursdays at approximately 4:15 with previous week statistics</p>',observations=n.encoded(observations()))
    c=Storage()
    for k in s.LEGACY_KEYS:c.objects[k]=n.encoded({'old':k,'value':123})
    sources={key:{'url':n.URLS[key],'acquired_at':AT,
                 'evidence':capture(c,'fixture','fr2004',n.URLS[key].split('?')[0],raw,datetime.fromisoformat(AT))} for key,raw in raws.items()}
    inputs={'contract':'dealer-original-inputs.v1','generated_at':AT,'sources':sources,'previous_output':None,'fails_context':None,'legacy_context':{}}
    return c,defs,reference,raws,inputs


def original_result():
    c,defs,ref,_,inputs=fixture()
    with patch.dict(n.DEFINITIONS,defs,clear=True),patch.dict(n.REGISTRY,reference_release=ref):
        return m.compile_research(inputs,s.raw_reader(c,'fixture'))


class DealerResearch(unittest.TestCase):
    def test_signed_zero_and_exact_sum_have_declared_basis(self):
        p=observations(1)
        for row in p['pd']['timeseries']:
            if row['keyid'].endswith('NSP'):row['value']='-1' if row['keyid']=='PDSI2NSP' else '0'
        parsed=n.parse(n.encoded(p),AT);key,label,ids='x','specific',['PDSI2NSP','PDSI3NSP']
        g=m.group(key,label,ids,parsed,AT,AT)
        self.assertEqual(g['usd_mn'],-1);self.assertEqual(g['exact_usd_bn'],'-0.001')
        self.assertEqual(g['valuation_basis'],'original_issuance_par')
        with self.assertRaises(ValueError):m.group('bad','mixed',['PDSI2NSP','PDPOSCSCP'],parsed,AT,AT)

    def test_latest_suppression_and_gap_never_fall_back(self):
        p=observations();row=next(r for r in p['pd']['timeseries'] if r['keyid']=='PDSIRRA-UTSETTOT');row['value']='*'
        g=m.group('x','x',['PDSIRRA-UTSETTOT','PDSIRRA-UTSTTOT'],n.parse(n.encoded(p),AT),AT,AT)
        self.assertEqual(g['as_of'],'2026-09-09');self.assertIsNone(g['usd_mn']);self.assertEqual(g['quality']['status'],'incomplete')
        self.assertEqual(g['components']['PDSIRRA-UTSETTOT']['status'],'suppressed')

    def test_catalog_conflicts_withhold_aggregate_but_keep_all_source_rows(self):
        out=original_result()
        self.assertEqual(len(out['native_series']),70);self.assertEqual(sum(len(r['history']) for r in out['native_series'].values()),70*55)
        for key in ('PDPOSGST-TOT','PDSIRRA-CDTOT','PDTRGS-EXTB'):
            self.assertEqual(out['native_series'][key]['quality']['status'],'definition_unverified')
            self.assertIsNotNone(out['native_series'][key]['reported_value'])
        self.assertIsNone(out['financing']['reverse_repo_in_b']);self.assertIsNotNone(out['financing']['treasury']['reverse_repo_in_b'])
        self.assertIsNone(out['positions_ledger']['TREASURY_EXTIPS']['latest_b'])

    def test_transaction_normalization_never_invents_daily_or_weekly_values(self):
        out=original_result()
        self.assertIn('BILLS',out['native_series']['PDTRGS-EXTB']['definition']['catalog']['description'])
        for row in out['transactions'].values():
            self.assertIsNone(row['unit']);self.assertIsNone(row['daily_average_b']);self.assertIsNone(row['weekly_b'])

    def test_old_scope_identity_is_not_inferred_from_current_catalog(self):
        p=observations(130);parsed=n.parse(n.encoded(p),AT)
        g=m.group('x','x',['PDSI2NSP','PDSI3NSP'],parsed,AT,AT)
        old=[r for r in g['history'] if r[2]!='SBN2024']
        self.assertTrue(old);self.assertTrue(all(r[1] is None for r in old))
        self.assertIsNotNone(g['usd_mn']);self.assertEqual(len(parsed['PDSI2NSP']),130)

    def test_no_sizing_votes_or_unqualified_legacy_statistics(self):
        out=original_result()
        self.assertFalse(out['calls_eligible']);self.assertFalse(out['sizing_eligible']);self.assertIsNone(out['allocation_pct'])
        self.assertIsNone(out['corporate']['squeeze_setup']);self.assertIsNone(out['corporate']['z_52w'])
        self.assertIsNone(out['net_treasury_total_b']);self.assertIsNone(out['positions_ledger']['ABS']['z_52w'])
        self.assertEqual(out['corporate']['regime'],'UNQUALIFIED')

    def test_baselines_exclude_current_and_stop_at_gap_and_period_change(self):
        hist=[[(date(2025,1,1)+timedelta(days=7*i)).isoformat(),10,'SBN2024'] for i in range(54)]
        hist[-1][1]=1000000
        stats=m.statistics(hist);self.assertEqual(stats['prior_n'],52);self.assertIsNone(stats['z']);self.assertEqual(stats['percentile'],100)
        hist[-2][1]=None;self.assertEqual(m.statistics(hist)['prior_n'],0)
        hist[-2][1]=10;hist[-2][2]='different';self.assertEqual(m.statistics(hist)['prior_n'],0)
        hist[-2][2]='SBN2024';hist.pop(-2);self.assertEqual(m.statistics(hist)['prior_n'],0)

    def test_calendar_comparison_requires_exact_date_and_same_source_period(self):
        hist=[['2026-06-10',10,'SBN2024'],['2026-09-02',20,'SBN2024'],['2026-09-09',25,'SBN2024']]
        self.assertEqual(m.comparisons(hist)['13w']['difference_usd_mn'],15)
        hist[0][0]='2026-06-09';self.assertIsNone(m.comparisons(hist)['13w']['difference_usd_mn'])
        hist[1][2]='SBN2022';self.assertIsNone(m.comparisons(hist)['1w']['difference_usd_mn'])

    def test_invalid_original_values_duplicates_and_identity_rejected(self):
        for v in ('1.2','nan',False,1,'999999999999999'):
            p=observations(1);p['pd']['timeseries'][0]['value']=v
            with self.assertRaises(ValueError):n.parse(n.encoded(p),AT)
        p=observations(1);p['pd']['timeseries'][-1]['value']='-1'
        with self.assertRaises(ValueError):n.parse(n.encoded(p),AT)
        p=observations(1);p['pd']['timeseries'].append(p['pd']['timeseries'][0])
        with self.assertRaises(ValueError):n.parse(n.encoded(p),AT)
        p=observations(1);p['pd']['timeseries'].pop()
        with self.assertRaises(ValueError):n.parse(n.encoded(p),AT)
        with self.assertRaises(ValueError):n.strict_json(b'{"x":1,"x":2}')

    def test_definition_pdf_catalog_and_source_break_drift_rejected(self):
        _,defs,ref,raws,_=fixture()
        with patch.dict(n.DEFINITIONS,defs,clear=True),patch.dict(n.REGISTRY,reference_release=ref):
            self.assertEqual(len(n.metadata(raws)),70)
            for key in ('SBN2024','historical_release','catalog','breaks','release_schedule'):
                bad=dict(raws);bad[key]=b'{}'
                with self.assertRaises(ValueError):n.metadata(bad)

    def test_original_request_hash_and_acquisition_clock_are_required(self):
        c,defs,ref,_,inputs=fixture();read=s.raw_reader(c,'fixture')
        with patch.dict(n.DEFINITIONS,defs,clear=True),patch.dict(n.REGISTRY,reference_release=ref):
            bad=deepcopy(inputs);bad['sources']['observations']['url']+='?wrong=1'
            with self.assertRaises(ValueError):n.load(bad,read)
            bad=deepcopy(inputs);bad['sources']['observations']['acquired_at']='2027-01-01T00:00:00Z'
            with self.assertRaises(ValueError):n.load(bad,read)
            key=inputs['sources']['observations']['evidence']['key'];c.objects[key]=gzip.compress(b'{}')
            with self.assertRaises(ValueError):n.load(inputs,read)

    def test_expiry_is_from_original_acquisition_and_expected_release(self):
        q=m.quality('2026-09-09',AT,AT,True);self.assertEqual(q['status'],'fresh')
        self.assertEqual(q['next_expected_publication_date'],'2026-09-24T20:15:00+00:00')
        self.assertEqual(m.quality('2026-09-09',AT,'2026-09-26T00:00:00Z',True)['status'],'stale')
        self.assertIsNone(q['publication_date']);self.assertFalse(q['holiday_adjustment_verified'])

    def test_full_store_replay_and_four_protected_legacy_backups(self):
        c,defs,ref,_,inputs=fixture();old={k:c.objects[k] for k in s.LEGACY_KEYS};read=s.raw_reader(c,'fixture')
        with patch.dict(n.DEFINITIONS,defs,clear=True),patch.dict(n.REGISTRY,reference_release=ref),patch.object(s,'acquire',side_effect=lambda c,b,k:inputs['sources'][k]),patch.object(s,'now',return_value=AT):
            result=s.run(c,'fixture');self.assertTrue(result['published']);self.assertEqual(result['original_series'],70)
            packet=n.strict_json(read(m.CURRENT));manifest=n.strict_json(read(packet['replay']['manifest_key']))
            self.assertEqual(s.replay(manifest,read),{k:v for k,v in packet.items() if k!='replay'})
            for k,raw in old.items():self.assertEqual(c.objects[s.PRIVATE+hashlib.sha256(raw).hexdigest()+'.bin'],raw)
            c.objects[manifest['registry']['key']]=b'{}'
            with self.assertRaises(ValueError):s.replay(manifest,read)

    def test_retained_revisions_and_dropped_dates(self):
        c,defs,ref,_,inputs=fixture();read=s.raw_reader(c,'fixture')
        with patch.dict(n.DEFINITIONS,defs,clear=True),patch.dict(n.REGISTRY,reference_release=ref),patch.object(s,'acquire',side_effect=lambda c,b,k:inputs['sources'][k]),patch.object(s,'now',return_value=AT):
            s.run(c,'fixture');inputs['previous_output']=s.previous(read);inputs['generated_at']='2026-09-19T11:01:00Z'
            rows=observations();rows['pd']['timeseries'][0]['value']='123'
            inputs['sources']['observations']['evidence']=capture(c,'fixture','fr2004',n.DATA_URL,n.encoded(rows),datetime.fromisoformat(inputs['generated_at']))
            inputs['sources']['observations']['acquired_at']=inputs['generated_at']
            out=m.compile_research(inputs,read);self.assertEqual(out['revisions']['known_changed_values'],1)
            rows['pd']['timeseries'].pop(1)
            inputs['sources']['observations']['evidence']=capture(c,'fixture','fr2004',n.DATA_URL,n.encoded(rows),datetime.fromisoformat(inputs['generated_at']))
            with self.assertRaisesRegex(ValueError,'dropped retained'):m.compile_research(inputs,read)

    def test_conditional_publication_rejects_concurrent_chain_and_old_clock(self):
        c=Storage();p={'contract':m.CONTRACT,'generated_at':AT,'source_generated_at':AT,'as_of':'2026-09-09','replay':{'output_sha256':'a'*64}}
        c.objects[m.CURRENT]=n.encoded(p)
        self.assertFalse(s.publish(c,'fixture',{**p,'generated_at':'2026-09-19T12:00:00Z'},{'sha256':'b'*64}))
        self.assertFalse(s.publish(c,'fixture',{**p,'generated_at':'2026-09-19T10:00:00Z'},None))
        with self.assertRaises(ValueError):s.publish(c,'fixture',{**p,'call':'LONG'},None)

    def test_actual_http_handler_cannot_collect_or_select_legacy(self):
        source=Path(s.__file__).with_name('lambda_function.py').read_text(encoding='utf-8');tree=ast.parse(source)
        handler=next(v for v in tree.body if isinstance(v,ast.FunctionDef) and v.name=='lambda_handler')
        code=ast.get_source_segment(source,handler);self.assertNotIn('_legacy_unvalidated_handler',code);self.assertNotIn('_emit_signal',code)
        scope={'json':json,'s3':object(),'BUCKET':'fixture'};exec(compile(ast.Module(body=[handler],type_ignores=[]),'handler-test','exec'),scope)
        with patch.object(s,'run',side_effect=AssertionError('HTTP collected')),patch.object(s,'raw_reader',return_value=lambda _:n.encoded({'contract':m.CONTRACT})):
            self.assertEqual(scope['lambda_handler']({'requestContext':{'http':{'method':'GET'}}})['statusCode'],200)
        with patch.object(s,'run',return_value={'published':True}) as run:
            self.assertEqual(scope['lambda_handler']({'legacy':True})['statusCode'],200);self.assertEqual(run.call_count,1)


if __name__=='__main__':unittest.main()
