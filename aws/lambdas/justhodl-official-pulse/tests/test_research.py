import copy,hashlib,importlib.util,io,json,sys,types,unittest,zipfile
from datetime import date,timedelta
from pathlib import Path
from unittest.mock import patch
import xml.etree.ElementTree as ET
ROOT=Path(__file__).resolve().parents[4];SOURCE=ROOT/'aws/lambdas/justhodl-official-pulse/source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared')]
import official_original as n,official_research as m,official_store as s
AT='2026-09-19T18:20:00+00:00'

class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self):self.objects={};self.meta={};self.fail_current=False
    def get_object(self,**kw):
        key=kw['Key']
        if key not in self.objects:raise StorageError('NoSuchKey')
        body=self.objects[key];return {'Body':io.BytesIO(body),'ETag':hashlib.sha256(body).hexdigest(),'Metadata':self.meta.get(key,{})}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if self.fail_current and key==m.CURRENT:raise StorageError('AccessDenied')
        if kw.get('IfNoneMatch')=='*' and old is not None:raise StorageError('412')
        if 'IfMatch' in kw and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise StorageError('412')
        self.objects[key]=kw['Body'];self.meta[key]=kw.get('Metadata',{})

def retain(store,url,raw,provider='h41'):
    return {'url':url,'acquired_at':AT,'evidence':n.evidence_store.capture(store,'b',provider,url,raw,n.clock(AT))}

def fixture(count=560,start=None,mutate=None):
    store=Storage();refs={};root=ET.Element('Data');by_name={}
    first=start or date(2026,9,16)-timedelta(weeks=count-1)
    values={}
    for i in range(count):
        cash=200000+i*i;ust=2000000+i*100+(i%3)*15
        values[i]={'foreign_rrp':cash,'rrp_other':100,'rrp_total':cash+100,'custody':ust,'custody_average':ust-3,
            'custody_agency':200000,'custody_other':70000,'custody_total':ust+270000}
    for name,spec in n.SERIES.items():
        attrs=dict(zip(('SERIES_NAME','CATEGORY','SUBCATEGORY','COMPONENT','SERIESTYPE'),spec[:5]));attrs.update(FREQ='19',DISTRIBUTION='TOT',UNIT='Currency',UNIT_MULT='1000000',CURRENCY='USD')
        element=ET.SubElement(root,'Series',attrs);ET.SubElement(element,'Annotations').text=spec[6];rows=[]
        for i in range(count):
            day=(first+timedelta(weeks=i)).isoformat();value=str(values[i][name]);ET.SubElement(element,'Obs',TIME_PERIOD=day,OBS_STATUS='A',OBS_VALUE=value);rows.append({'date':day,'value':value})
        by_name[name]=rows
    if mutate:mutate(root)
    structure=ET.Element('Structure');status=ET.SubElement(structure,'CodeList',id='CL_OBS_STATUS')
    for k,v in {'A':'Normal','NC':'Not calculable','NA':'Not available','ND':'No data'}.items():ET.SubElement(ET.SubElement(status,'Code',value=k),'Description').text=v
    raw=io.BytesIO()
    with zipfile.ZipFile(raw,'w',zipfile.ZIP_DEFLATED) as z:
        z.writestr('H41_data.xml',ET.tostring(root));z.writestr('H41_struct.xml',ET.tostring(structure));z.writestr('H41_H41.xsd',b'<schema/>');z.writestr('frb_common.xsd',b'<schema/>')
    refs['native_archive']=retain(store,n.XML_URL,raw.getvalue())
    html='<title>Federal Reserve H.4.1 - September 17, 2026</title><table><th id="t3h1c3">Wednesday Sep 16, 2026</th><th id="t3h2c1">Week ended Sep 16, 2026</th>'
    for name,row,label in [('custody_total',1,'Securities held in custody'),('custody',2,'Marketable U.S. Treasury'),('custody_agency',3,'Federal agency'),('custody_other',4,'Other securities')]:
        html+=f'<tr><td id="t3r{row}c1">{label}</td><td id="t3r{row}c5">{values[count-1][name]}</td></tr>'
    html+=f'<td id="t3r2c2">{values[count-1]["custody_average"]}</td></table>'
    refs['release_html']=retain(store,n.HTML_URL,html.encode());refs['scope_definition']=retain(store,n.FAQ_URL,b'Current face historical data from July 2007; current face value.')
    for name,url in n.calendar_urls(AT).items():
        doc={'releases':[{'id':20,'name':'H.4.1 Factors Affecting Reserve Balances'}]} if name=='series_release' else {'count':2,'offset':0,'release_dates':[{'release_id':20,'date':d} for d in ('2026-09-17','2026-09-24')]}
        refs[name]=retain(store,url,n.encoded(doc))
    source={'contract':'report-observations.v1','generated_at':AT,'measurements':{sid:{} for sid in ('WLRRAFOIAL','WMTSECL1')}};originals={}
    for name in ('foreign_rrp','custody'):
        sid=n.SERIES[name][5];definition={'seriess':[{'id':sid,'frequency':'Weekly, As of Wednesday','units':'Millions of U.S. Dollars','seasonal_adjustment':'Not Seasonally Adjusted'}]}
        obs={'count':count,'offset':0,'sort_order':'desc','order_by':'observation_date','observations':by_name[name][::-1]};pair={}
        for label,doc in [('definition',definition),('observations',obs)]:pair[label]=retain(store,'https://api.stlouisfed.org/fred/'+label+'?series_id='+sid,n.encoded(doc),'fred')['evidence']
        originals[sid]={'evidence':pair,'acquired_at':AT}
    code=Path(s.report_observations.__file__).read_bytes();sha=hashlib.sha256(code).hexdigest();compiler={'key':'data/report-research/compilers/'+sha+'.py','sha256':sha};store.objects[compiler['key']]=code
    manifest={'inputs':originals,'compiler':compiler,'output_sha256':m.digest(source)};key='data/report-research/runs/'+m.digest(manifest)+'.json';store.objects[key]=m.encoded(manifest)
    source['replay']={'manifest_key':key,'compiler_sha256':sha,'output_sha256':manifest['output_sha256']};store.objects['data/report-measurements.json']=m.encoded(source)
    return store,{'contract':'official-original-inputs.v1','originals':refs,'canonical':source,'tic':None,'legacy':{'fixture':True},'acquisition_errors':{}}

class Tests(unittest.TestCase):
    def test_original_full_history_and_scopes_reproduce(self):
        store,inputs=fixture();out,hist=m.build(inputs,s.raw_reader(store,'b'),AT)
        self.assertEqual(out['quality']['status'],'fresh');self.assertEqual(len(hist),12)
        self.assertEqual(out['custody']['n_obs'],560);self.assertEqual(out['distribution_checks']['custody']['matches'],560)
        self.assertEqual(out['custody']['statistics']['available_prior_changes'],520)
        self.assertIsNone(out['custody']['z_13wchg_10y']);self.assertEqual(out['dollar_leg']['available'],0)
        self.assertEqual(out['dollar_leg']['status'],'UNKNOWN');self.assertFalse(out['calls_eligible']);self.assertFalse(out['sizing_eligible'])
        self.assertEqual(out['reconciliations']['custody']['latest']['residual_usd_million_decimal'],'0')
        self.assertNotEqual(out['measurements']['custody_average']['latest_bn'],out['custody']['latest_bn'])

    def test_date_windows_do_not_compress_a_missing_or_invalid_week(self):
        rows={(date(2026,9,16)-timedelta(weeks=k)).isoformat():{'analysis_eligible':True,'value_decimal':str(100-k)} for k in range(14)}
        self.assertEqual(m.change(rows,'2026-09-16',13)['change_usd_million_decimal'],'13')
        del rows['2026-09-02'];out=m.change(rows,'2026-09-16',13)
        self.assertIsNone(out['change_usd_million_decimal']);self.assertIn('2026-09-02',out['unavailable_dates'])

    def test_prior_statistics_exclude_current_and_require_all_520_changes(self):
        rows={(date(2026,9,16)-timedelta(weeks=k)).isoformat():{'analysis_eligible':True,'value_decimal':str(k*k)} for k in range(540)}
        a=m.statistics(rows,'2026-09-16');rows['2026-09-16']['value_decimal']='999999999';b=m.statistics(rows,'2026-09-16')
        self.assertEqual(a['mean_usd_million_decimal'],b['mean_usd_million_decimal']);self.assertNotEqual(a['z_decimal'],b['z_decimal'])
        del rows['2020-01-01'];self.assertEqual(m.statistics(rows,'2026-09-16')['status'],'unavailable')

    def test_current_face_early_zero_is_retained_but_not_analytical(self):
        def zeros(root):
            for series in root:
                for row in series:
                    if row.tag=='Obs':row.set('OBS_VALUE','0')
        store,inputs=fixture(3,date(2007,6,27),zeros);result,_=n.archive(inputs['originals']['native_archive'],s.raw_reader(store,'b'),AT)
        self.assertEqual([r['value_decimal'] for r in result['custody']['rows']],['0','0','0'])
        self.assertEqual([r['analysis_eligible'] for r in result['custody']['rows']],[False,True,True])
        self.assertEqual([r['analysis_eligible'] for r in result['custody_average']['rows']],[False,False,True])

    def test_identity_units_duplicate_dates_and_status_fail_closed(self):
        for mutate in (lambda root:root[0].set('UNIT_MULT','1000'),lambda root:root[0].append(copy.deepcopy(root[0][-1])),lambda root:root[0][-1].set('OBS_STATUS','NA')):
            store,inputs=fixture(2,mutate=mutate)
            with self.assertRaises(ValueError):n.archive(inputs['originals']['native_archive'],s.raw_reader(store,'b'),AT)

    def test_original_hash_and_future_acquisition_rejected(self):
        store,inputs=fixture(2);ref=copy.deepcopy(inputs['originals']['native_archive']);ref['evidence']['bytes']+=1
        with self.assertRaises(ValueError):n.archive(ref,s.raw_reader(store,'b'),AT)
        ref=copy.deepcopy(inputs['originals']['native_archive']);ref['acquired_at']='2026-09-20T00:00:00Z'
        with self.assertRaises(ValueError):n.archive(ref,s.raw_reader(store,'b'),AT)

    def test_distribution_disagreement_is_preserved_and_units_cannot_be_substituted(self):
        store,inputs=fixture(2);read=s.raw_reader(store,'b');native,_=n.archive(inputs['originals']['native_archive'],read,AT)
        item=m.canonical_macro_sources.originals(inputs['canonical'],read,('WMTSECL1',))['WMTSECL1']
        item=copy.deepcopy(item);item['observations']['observations'][0]['value']='1'
        out=n.distribution(item,native['custody']);self.assertEqual(out['different_values'],1);self.assertEqual(out['matches'],1)
        item['definition']['seriess'][0]['units']='Billions of Dollars'
        with self.assertRaises(ValueError):n.distribution(item,native['custody'])

    def test_xml_entity_declarations_are_rejected_before_parsing(self):
        store,inputs=fixture(2);read=s.raw_reader(store,'b');raw=read(inputs['originals']['native_archive']['evidence']['key']);new=io.BytesIO()
        with zipfile.ZipFile(io.BytesIO(raw)) as original,zipfile.ZipFile(new,'w',zipfile.ZIP_DEFLATED) as out:
            for name in original.namelist():
                body=original.read(name)
                if name=='H41_data.xml':body=b'<!DOCTYPE x [<!ENTITY bomb "expand">]>'+body
                out.writestr(name,body)
        ref=retain(store,n.XML_URL,new.getvalue())
        with self.assertRaisesRegex(ValueError,'entity declarations'):n.archive(ref,read,AT)

    def test_missing_canonical_source_still_retains_a_failed_candidate(self):
        store,inputs=fixture(2);old=b'{"legacy":true}';store.objects[m.CURRENT]=old;del store.objects['data/report-measurements.json']
        with patch.object(s,'collect',return_value=(inputs['originals'],{})),patch.object(s,'now',return_value=AT),self.assertRaises(ValueError):s.run(store,'b','fixture')
        self.assertEqual(store.objects[m.CURRENT],old)
        attempts=[json.loads(raw) for k,raw in store.objects.items() if k.startswith(m.PREFIX+'attempts/')]
        self.assertEqual(attempts[0]['source_status_codes']['canonical_source'],'StorageError')

    def test_printed_date_mismatch_is_not_a_matching_value(self):
        store,inputs=fixture(2);read=s.raw_reader(store,'b');series,_=n.archive(inputs['originals']['native_archive'],read,AT)
        raw=read(inputs['originals']['release_html']['evidence']['key']).replace(b'Wednesday Sep 16',b'Wednesday Sep 9')
        ref=retain(store,n.HTML_URL,raw);out=n.release(ref,read,AT,series)
        self.assertFalse(out['checks'][0]['matches'])

    def test_stale_and_calendar_lag_are_not_fixed_by_generated_time(self):
        cal={'nominal_expected_wednesday':'2026-09-16','calendar_expires_at':'2026-09-25T04:00:00Z'}
        self.assertEqual(m.quality('2026-09-09',AT,cal,AT)['status'],'stale')
        self.assertEqual(m.quality('2026-09-16','2026-09-16T00:00:00Z',cal,AT)['status'],'stale')
        self.assertEqual(m.quality('2026-09-16',AT,cal,AT,True)['status'],'source_disagreement')

    def test_reconciliation_rounding_bound_and_missingness(self):
        def series(v):return {'rows':[{'date':'2026-09-16','value_decimal':v,'analysis_eligible':v is not None}]}
        values={'t':series('10'),'a':series('3'),'b':series('6')};self.assertEqual(m.reconciliation(values,'t',['a','b'])['rows'][0]['status'],'within_reporting_rounding')
        values['b']=series('4');self.assertEqual(m.reconciliation(values,'t',['a','b'])['rows'][0]['status'],'outside_reporting_rounding')
        values['b']=series(None);self.assertIsNone(m.reconciliation(values,'t',['a','b'])['rows'][0]['residual_usd_million_decimal'])

    def test_complete_legacy_preserved_and_all_retained_histories_replayed(self):
        store,inputs=fixture();old=b'{"legacy_inputs":[1,2,3]}';store.objects[m.CURRENT]=old
        with patch.object(s,'collect',return_value=(inputs['originals'],{})),patch.object(s,'now',return_value=AT):result=s.run(store,'b','fixture')
        self.assertTrue(result['published']);self.assertTrue(any(k.startswith(s.PRIVATE) and v==old for k,v in store.objects.items()))
        read=s.raw_reader(store,'b');manifest=json.loads(read(result['replay']['manifest_key']));output=s.replay(manifest,read)
        store.objects[output['custody']['history']['key']]=b'{}'
        with self.assertRaises(ValueError):s.replay(manifest,read)

    def test_failed_candidate_keeps_current_and_records_attempt(self):
        store,inputs=fixture(2);old=b'{"legacy":true}';store.objects[m.CURRENT]=old
        with patch.object(s,'collect',return_value=({}, {'native_archive':'HTTP_503'})),patch.object(s,'now',return_value=AT),self.assertRaises(KeyError):s.run(store,'b','fixture')
        self.assertEqual(store.objects[m.CURRENT],old);self.assertTrue(any(k.startswith(m.PREFIX+'attempts/') for k in store.objects))

    def test_newer_source_wins_and_same_clock_conflicts_fail(self):
        store=Storage();packet={'contract':m.CONTRACT,'generated_at':AT,'source_generated_at':AT,'source_clocks':{'native_archive':AT}}
        old={**packet,'source_generated_at':'2026-09-20T00:00:00Z'};store.objects[m.CURRENT]=m.encoded(old)
        self.assertFalse(s.publish(store,'b',{**packet,'generated_at':'2026-09-21T00:00:00Z'}))
        store.objects[m.CURRENT]=m.encoded({**packet,'different':True})
        with self.assertRaises(ValueError):s.publish(store,'b',packet)

    def test_http_is_read_only_and_rejects_legacy_packet(self):
        store=Storage();store.objects[m.CURRENT]=m.encoded({'contract':m.CONTRACT})
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:store)}):
            spec=importlib.util.spec_from_file_location('official_active_test',SOURCE/'lambda_function.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with patch.object(s,'run',side_effect=AssertionError('HTTP collector forbidden')):result=module.lambda_handler({'httpMethod':'GET'},None)
        self.assertEqual(result['statusCode'],200);self.assertEqual(result['headers']['Cache-Control'],'no-store')
        store.objects[m.CURRENT]=b'{"v":"1.0.0"}';self.assertEqual(module.lambda_handler({'httpMethod':'GET'},None)['statusCode'],503)

if __name__=='__main__':unittest.main(verbosity=2)
