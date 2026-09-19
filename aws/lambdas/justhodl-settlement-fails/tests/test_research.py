import ast
from copy import deepcopy
from datetime import date,datetime,timedelta
import hashlib
import gzip
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'scripts'),str(Path(__file__).resolve().parents[1]/'source')]
import fails_native as n
import fails_research as m
import fails_store as s
from evidence_store import capture

AT='2026-09-19T11:00:00+00:00'


class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self):self.objects={};self.reads=[];self.metadata={}
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise StorageError('NoSuchKey')
        raw=self.objects[key]
        return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest(),'Metadata':self.metadata.get(key,{})}
    def put_object(self,**kw):
        key=kw['Key']
        if kw.get('IfNoneMatch')=='*' and key in self.objects:raise StorageError('PreconditionFailed')
        if kw.get('IfMatch') and (key not in self.objects or kw['IfMatch']!=hashlib.sha256(self.objects[key]).hexdigest()):raise StorageError('PreconditionFailed')
        self.objects[key]=kw['Body'];self.metadata[key]=kw.get('Metadata',{})


def observations(count=106):
    return {'pd':{'timeseries':[{'keyid':key,'asofdate':(date(2026,9,9)-timedelta(days=7*i)).isoformat(),
                                'value':str(100000+index*1000+(i%13)*7)} for index,key in enumerate(n.KEYS) for i in range(count)]}}


def fixtures():
    defs=deepcopy(n.DEFINITIONS);raws={}
    for key in defs:
        raws[key]=b'%PDF-'+key.encode();defs[key]['sha256']=hashlib.sha256(raws[key]).hexdigest()
    catalog=[{'keyid':'PDF'+side+'-'+suffix,'seriesbreak':'SBN2024','description':prefix+' : DEALER FINANCING FAILS TO '+label}
             for _,suffix,_,prefix in n.CLASSES for side,label in [('TD','DELIVER'),('TR','RECEIVE')]]
    raws.update(catalog=n.encoded({'pd':{'timeseries':catalog}}),
                breaks=n.encoded({'pd':{'seriesbreaks':[{'seriesbreak':k,'startdate':v['start'],'enddate':v['end']} for k,v in defs.items()]}}),
                release_schedule=b'<p>Thursdays at approximately 4:15 p.m. with previous week statistics</p>',
                observations=n.encoded(observations()))
    client=Storage();client.objects[m.CURRENT]=n.encoded({'generated_at':AT,'classes':[{'key':'legacy','old_value':123}]})
    descriptors={}
    for key,raw in raws.items():
        descriptors[key]={'url':n.URLS[key],'acquired_at':AT,'evidence':capture(client,'fixture','fr2004',n.URLS[key].split('?')[0],raw,datetime.fromisoformat(AT))}
    inputs={'contract':'fr2004-original-inputs.v1','generated_at':AT,'sources':descriptors,'previous_output':None,'legacy_context':None}
    return client,defs,raws,inputs


class Research(unittest.TestCase):
    def test_exact_scope_sums_and_units(self):
        c,defs,_,inputs=fixtures()
        with patch.dict(n.DEFINITIONS,defs,clear=True):p=m.compile_research(inputs,s.raw_reader(c,'fixture'))
        self.assertEqual(p['headline']['gross_usd_mn'],201000)
        self.assertEqual(p['treasury']['gross_usd_mn'],406000)
        self.assertEqual(p['totals']['gross_usd_mn'],1266000)
        self.assertEqual(p['headline']['exact_usd_bn']['gross'],'201')
        self.assertEqual(len(p['classes']),6);self.assertEqual(p['treasury']['unit'],'usd_bn')
        self.assertEqual(p['headline']['statistics']['baseline']['sample_n'],104)
        self.assertIsNone(p['headline']['pctile']);self.assertIsNone(p['classes'][0]['stats']['spike'])
        self.assertFalse(p['calls_eligible']);self.assertFalse(p['sizing_eligible']);self.assertIsNone(p['allocation_pct'])

    def test_rounding_reconciliation_uses_integer_millions(self):
        p=observations(1);p['pd']['timeseries'][0]['value']='113827';p['pd']['timeseries'][1]['value']='119224'
        rows=n.parse(n.encoded(p),AT);scope=m.scope('x','x',[n.KEYS[:2]],rows,AT,AT)
        self.assertEqual(scope['gross_usd_mn'],233051);self.assertEqual(scope['combined_bn'],233.051)
        self.assertEqual(scope['exact_usd_bn'],{'ftd':'113.827','ftr':'119.224','gross':'233.051'})

    def test_suppressed_latest_does_not_fallback_to_previous(self):
        p=observations();p['pd']['timeseries'][0]['value']='*';rows=n.parse(n.encoded(p),AT)
        scope=m.scope('x','x',[n.KEYS[:2]],rows,AT,AT)
        self.assertEqual(scope['as_of'],'2026-09-09');self.assertIsNone(scope['gross_usd_mn'])
        self.assertIsNone(scope['ftd_bn']);self.assertIsNotNone(scope['ftr_bn']);self.assertFalse(scope['complete'])
        self.assertEqual(scope['quality']['status'],'incomplete');self.assertIsNone(scope['statistics']['z'])

    def test_zero_is_measured_not_missing(self):
        p=observations(1)
        for row in p['pd']['timeseries']:row['value']='0'
        rows=n.parse(n.encoded(p),AT);scope=m.scope('x','x',[n.KEYS[:2]],rows,AT,AT)
        self.assertTrue(scope['complete']);self.assertEqual(scope['gross_bn'],0)

    def test_invalid_duplicate_wrong_series_and_future_rows_rejected(self):
        for value in ('-1','1.2','NaN','Inf',False,100,None):
            p=observations(1)
            if value is None:p['pd']['timeseries'].append(deepcopy(p['pd']['timeseries'][0]))
            else:p['pd']['timeseries'][0]['value']=value
            with self.assertRaises(ValueError):n.parse(n.encoded(p),AT)
        for key,value in [('keyid','PDFTD-UNKNOWN'),('asofdate','2027-01-01')]:
            p=observations(1);p['pd']['timeseries'][0][key]=value
            with self.assertRaises(ValueError):n.parse(n.encoded(p),AT)
        with self.assertRaises(ValueError):n.strict_json(b'{"pd":1,"pd":2}')

    def test_missing_requested_series_is_rejected(self):
        p=observations(1);p['pd']['timeseries'].pop()
        with self.assertRaises(ValueError):n.parse(n.encoded(p),AT)

    def test_reviewed_pdf_catalog_and_break_changes_rejected(self):
        _,defs,raws,_=fixtures()
        with patch.dict(n.DEFINITIONS,defs,clear=True):
            self.assertEqual(len(n.metadata(raws)),12)
            for key in ('SBN2024','catalog','breaks','release_schedule'):
                bad=dict(raws)
                if key=='SBN2024':bad[key]+=b'changed'
                elif key=='catalog':bad[key]=bad[key].replace(b'FAILS TO DELIVER',b'NET POSITION')
                elif key=='breaks':bad[key]=bad[key].replace(b'2024-07-03',b'2024-07-04')
                else:bad[key]=b'new timetable'
                with self.assertRaises(ValueError):n.metadata(bad)

    def test_original_body_request_and_clock_corruption_rejected(self):
        c,defs,_,inputs=fixtures();read=s.raw_reader(c,'fixture')
        with patch.dict(n.DEFINITIONS,defs,clear=True):
            for field,value in [('url',n.CATALOG_URL),('acquired_at','2027-01-01T00:00:00Z')]:
                bad=deepcopy(inputs);bad['sources']['observations'][field]=value
                with self.assertRaises(ValueError):n.load(bad,read)
            key=inputs['sources']['observations']['evidence']['key'];c.objects[key]=gzip.compress(b'wrong bytes')
            with self.assertRaises(ValueError):n.load(inputs,read)

    def test_prior_baseline_excludes_current_and_constant_sd_is_null(self):
        rows=[{'date':(date(2026,9,9)-timedelta(days=7*i)).isoformat(),'gross_usd_mn':100 if i else 900,'seriesbreak':'SBN2024'} for i in range(31)][::-1]
        st=m.statistics(rows)
        self.assertEqual(st['mean_usd_mn_decimal'],'100');self.assertIsNone(st['z']);self.assertEqual(st['pctile'],100)
        self.assertEqual(st['status'],'constant_prior_sample')

    def test_gap_missing_and_period_breaks_do_not_compact_baseline(self):
        rows=[{'date':(date(2026,9,9)-timedelta(days=7*i)).isoformat(),'gross_usd_mn':i,'seriesbreak':'SBN2024'} for i in range(40)][::-1]
        for kind in ('missing','gap','break'):
            changed=deepcopy(rows)
            if kind=='missing':changed[-10]['gross_usd_mn']=None
            elif kind=='gap':changed.pop(-10)
            else:changed[-10]['seriesbreak']='SBN2022'
            self.assertIsNone(m.statistics(changed)['z'])

    def test_ties_use_midrank_not_a_probability(self):
        rows=[{'date':(date(2026,9,9)-timedelta(days=7*i)).isoformat(),'gross_usd_mn':100,'seriesbreak':'SBN2024'} for i in range(31)][::-1]
        st=m.statistics(rows);self.assertEqual(st['pctile'],50);self.assertIsNone(st['probability'])

    def test_acquisition_and_next_normal_release_expire_separately(self):
        q=m.quality('2026-09-09',AT,AT);self.assertEqual(q['status'],'fresh');self.assertIsNone(q['publication_date'])
        self.assertEqual(q['next_expected_publication_date'],'2026-09-24T20:15:00+00:00')
        self.assertEqual(m.quality('2026-09-09',AT,'2026-09-21T00:00:00Z')['status'],'stale')
        self.assertEqual(m.quality('2026-09-09','2026-09-25T23:00:00Z','2026-09-25T23:00:00Z')['status'],'stale')

    def test_run_retains_originals_legacy_and_reproduces_without_emissions(self):
        c,defs,_,inputs=fixtures();old=c.objects[m.CURRENT]
        with patch.dict(n.DEFINITIONS,defs,clear=True),patch.object(s,'now',return_value=AT),patch.object(s,'acquire',side_effect=lambda _c,_b,k:inputs['sources'][k]):
            result=s.run(c,'fixture');self.assertTrue(result['published']);packet=json.loads(c.objects[m.CURRENT])
            manifest=json.loads(c.objects[result['replay']['manifest_key']])
            output=s.replay(manifest,s.raw_reader(c,'fixture'));self.assertEqual(output,{k:v for k,v in packet.items() if k!='replay'})
            self.assertEqual(c.objects[s.PRIVATE+hashlib.sha256(old).hexdigest()+'.bin'],old)
            self.assertEqual(result['signals_emitted'],0)
            c.objects[manifest['output']['key']]+=b' '
            with self.assertRaises(ValueError):s.replay(manifest,s.raw_reader(c,'fixture'))

    def test_failed_capture_never_replaces_current(self):
        c,_,_,_=fixtures();old=c.objects[m.CURRENT]
        with patch.object(s,'acquire',side_effect=RuntimeError('provider unavailable')):
            with self.assertRaises(RuntimeError):s.run(c,'fixture')
        self.assertEqual(c.objects[m.CURRENT],old)

    def test_revision_chain_retains_changes_and_rejects_lost_dates(self):
        c,defs,_,inputs=fixtures();read=s.raw_reader(c,'fixture')
        with patch.dict(n.DEFINITIONS,defs,clear=True),patch.object(s,'now',return_value=AT),patch.object(s,'acquire',side_effect=lambda _c,_b,k:inputs['sources'][k]):
            s.run(c,'fixture');prior=s.previous(read)
            changed=deepcopy(inputs);changed['previous_output']=prior;changed['generated_at']='2026-09-19T11:01:00Z'
            rows=observations();rows['pd']['timeseries'][0]['value']='123456'
            evidence=capture(c,'fixture','fr2004',n.DATA_URL,n.encoded(rows),datetime.fromisoformat(changed['generated_at']))
            changed['sources']['observations']={'url':n.DATA_URL,'acquired_at':changed['generated_at'],'evidence':evidence}
            result=m.compile_research(changed,read)
            self.assertEqual(result['revisions']['known_changed_values'],1)
            self.assertEqual(result['revisions']['changes'][0]['previous'],100000)
            self.assertEqual(result['revisions']['changes'][0]['current'],123456)
            rows['pd']['timeseries']=[r for r in rows['pd']['timeseries'] if r['asofdate']!='2024-09-04']
            changed['sources']['observations']['evidence']=capture(c,'fixture','fr2004',n.DATA_URL,n.encoded(rows),datetime.fromisoformat(changed['generated_at']))
            with self.assertRaisesRegex(ValueError,'dropped previously retained'):m.compile_research(changed,read)

    def test_current_publication_rejects_races_and_same_clock_conflicts(self):
        c=Storage();packet={'contract':m.CONTRACT,'generated_at':AT,'source_generated_at':AT,'as_of':'2026-09-09','replay':{'output_sha256':'a'*64}}
        c.objects[m.CURRENT]=n.encoded(packet)
        with self.assertRaises(ValueError):s.publish(c,'fixture',{**packet,'call':'LONG'},None)
        later={**packet,'generated_at':'2026-09-19T12:00:00Z'}
        self.assertFalse(s.publish(c,'fixture',later,{'sha256':'b'*64}))
        self.assertTrue(s.publish(c,'fixture',later,{'sha256':'a'*64}))

    def test_active_handler_cannot_select_legacy_or_signal_emitter(self):
        source=Path(s.__file__).with_name('lambda_function.py').read_text(encoding='utf-8');tree=ast.parse(source)
        handler=next(x for x in tree.body if isinstance(x,ast.FunctionDef) and x.name=='lambda_handler')
        text=ast.get_source_segment(source,handler)
        self.assertNotIn('_legacy_unvalidated_handler',text);self.assertNotIn('_emit_signal',text)
        calls=[]
        scope={'json':json,'strict_json_dumps':json.dumps,'S3':object(),'BUCKET':'fixture'}
        exec(compile(ast.Module(body=[handler],type_ignores=[]),'handler-boundary','exec'),scope)
        with patch.object(s,'run',side_effect=lambda *args:calls.append(args) or {'published':True}):
            self.assertEqual(scope['lambda_handler']({'_legacy':True,'mode':'emit'},None)['statusCode'],200)
            self.assertEqual(len(calls),1)
        with patch.object(s,'run',side_effect=AssertionError('HTTP must not collect')),patch.object(s,'raw_reader',return_value=lambda _:n.encoded({'contract':m.CONTRACT})):
            self.assertEqual(scope['lambda_handler']({'requestContext':{'http':{'method':'GET'}}},None)['statusCode'],200)


if __name__=='__main__':unittest.main()
