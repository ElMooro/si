import copy,gzip,hashlib,importlib.util,io,json,sys,types,unittest,zipfile
from pathlib import Path
from unittest.mock import patch
from test_originals import ROOT,AT,series
import foreign_original as n,foreign_research as m,foreign_store as s

class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}
class Storage:
    def __init__(self):self.objects={};self.meta={};self.fail_current=False
    def get_object(self,**kw):
        key=kw['Key']
        if key not in self.objects:raise StorageError('NoSuchKey')
        body=self.objects[key]
        return {'Body':io.BytesIO(body),'ETag':hashlib.sha256(body).hexdigest(),'Metadata':self.meta.get(key,{})}
    def put_object(self,**kw):
        key=kw['Key'];old=self.objects.get(key)
        if self.fail_current and key==m.CURRENT:raise StorageError('AccessDenied')
        if kw.get('IfNoneMatch')=='*' and old is not None:raise StorageError('412')
        if 'IfMatch' in kw and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise StorageError('412')
        self.objects[key]=kw['Body'];self.meta[key]=kw.get('Metadata',{})

def retain(store,url,raw):return {'url':url,'acquired_at':AT,'evidence':n.evidence_store.capture(store,'b','tic',url,raw,n.clock(AT))}

def fixture():
    store=Storage();members=[];refs={};days=[m.month_before('2026-07-01',i) for i in range(13)]
    countries={code:alias.replace('_',' ').title() for alias,code in m.COUNTRY_ALIASES.items()}
    names={**countries,'99996':'Grand Total','99990':'Foreign Official','99991':'Foreign Private'}
    for code,name in names.items():
        for family in n.FAMILIES:
            for measure in n.MEASURES:
                value=(24000 if code=='99996' else 1000) if measure=='pos' else (100 if code=='99996' else 40 if code=='99990' else 60 if code=='99991' else 2) if measure=='net' else 0
                members.append(series(code,name,family,measure,values=[[d,str(value)] for d in days]))
    raw=io.BytesIO()
    with zipfile.ZipFile(raw,'w',zipfile.ZIP_DEFLATED) as z:z.writestr('cslt.json',n.encoded({'releaseID':'3','version':'2.0','series':members}))
    refs['bulk']=retain(store,n.CSLT_URL,raw.getvalue())
    table='Table 5: Major Foreign Holders of Treasury Securities\nBillions of dollars\nCountry\t2026-07\n'
    table+=''.join(name+'\t1.0\n' for name in list(countries.values())[:9]);table+='Grand Total\t24.0\nAll Other\t15.0\n'
    refs['table5']=retain(store,n.TABLE_URL,table.encode())
    for key,url in n.calendar_urls(AT).items():
        doc={'releases':[{'id':3,'name':'Treasury International Capital: Continuous Securities Long Term (CSLT)'}]} if key=='series_release' else {'count':2,'offset':0,'limit':1000,'release_dates':[{'release_id':3,'date':'2026-09-16'},{'release_id':3,'date':'2026-10-16'}]}
        refs[key]=retain(store,url,n.encoded(doc))
    inputs={'contract':'foreign-original-inputs.v1','originals':refs,'acquisition_errors':{'mspd':'fixture_omitted','auction_note':'fixture_omitted','auction_bond':'fixture_omitted'},'legacy':{'fixture':True}}
    return store,inputs

class Tests(unittest.TestCase):
    def test_full_replay_retains_complete_legacy_and_histories(self):
        store,inputs=fixture();old=b'{"v":"1.5.0","original_inputs":{"untouched":123}}';store.objects[m.CURRENT]=old
        with patch.object(s,'collect',return_value=(inputs['originals'],inputs['acquisition_errors'],'2026-09-19')),patch.object(s,'now',return_value=AT):result=s.run(store,'b','fixture')
        self.assertTrue(result['published']);self.assertTrue(any(k.startswith(s.PRIVATE) and v==old for k,v in store.objects.items()))
        manifest=json.loads(s.raw_reader(store,'b')(result['replay']['manifest_key']));output=s.replay(manifest,s.raw_reader(store,'b'))
        self.assertEqual(output['quality']['status'],'partial');self.assertFalse(output['calls_eligible']);self.assertIsNone(output['call'])
        self.assertEqual(output['native_observations'],27*7*3*13)
        group=json.loads(s.raw_reader(store,'b')(output['groups']['10251']['history']['key']))
        self.assertEqual(len(group['series']['lt_treas:net']['rows']),13)
        self.assertNotIn('acquired_at',group['series']['lt_treas:net']['original'])
        self.assertEqual(output['absorption']['status'],'UNAVAILABLE')
        store.objects[output['groups']['10251']['history']['key']]=b'{}'
        with self.assertRaises(ValueError):s.replay(manifest,s.raw_reader(store,'b'))

    def test_failed_candidate_retains_inputs_without_replacing_current(self):
        store=Storage();old=b'{"v":"1.5.0"}';store.objects[m.CURRENT]=old
        with patch.object(s,'collect',return_value=({}, {'bulk':'HTTP_503'},'2026-09-19')),patch.object(s,'now',return_value=AT),self.assertRaises(KeyError):s.run(store,'b','fixture')
        self.assertEqual(store.objects[m.CURRENT],old)
        attempts=[json.loads(v) for k,v in store.objects.items() if k.startswith(m.PREFIX+'attempts/')]
        self.assertEqual(len(attempts),1);self.assertIn(attempts[0]['input']['key'],store.objects)

    def test_current_write_failure_leaves_replayable_candidate(self):
        store,inputs=fixture();old=b'{"v":"1.5.0"}';store.objects[m.CURRENT]=old;store.fail_current=True
        with patch.object(s,'collect',return_value=(inputs['originals'],{},'2026-09-19')),patch.object(s,'now',return_value=AT),self.assertRaises(StorageError):s.run(store,'b','fixture')
        self.assertEqual(store.objects[m.CURRENT],old);self.assertTrue(any(k.startswith(m.PREFIX+'runs/') for k in store.objects))

    def test_newer_source_and_output_clocks_win_and_equal_clock_conflicts_fail(self):
        store=Storage();packet={'contract':m.CONTRACT,'generated_at':AT,'source_clocks':{'bulk':AT}}
        for changed in ({'generated_at':'2026-09-20T00:00:00Z'},{'source_clocks':{'bulk':'2026-09-20T00:00:00Z'}}):
            store.objects[m.CURRENT]=n.encoded({**packet,**changed})
            if 'source_clocks' in changed:
                candidate={**packet,'generated_at':'2026-09-21T00:00:00Z'}
            else:candidate=packet
            self.assertFalse(s.publish(store,'b',candidate))
        store.objects[m.CURRENT]=n.encoded({**packet,'other':'conflict'})
        with self.assertRaises(ValueError):s.publish(store,'b',packet)

    def test_http_read_never_collects_and_rejects_unreviewed_packet(self):
        store=Storage();store.objects[m.CURRENT]=n.encoded({'contract':m.CONTRACT,'generated_at':AT})
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:store)}):
            spec=importlib.util.spec_from_file_location('foreign_active_test',ROOT/'aws/lambdas/justhodl-foreign-flows/source/lambda_function.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with patch.object(s,'run',side_effect=AssertionError('HTTP must not collect')):
            response=module.lambda_handler({'requestContext':{'http':{'method':'GET'}}},None)
        self.assertEqual(response['statusCode'],200);self.assertEqual(response['headers']['Cache-Control'],'no-store')
        store.objects[m.CURRENT]=b'{"v":"1.5.0"}'
        self.assertEqual(module.lambda_handler({'httpMethod':'GET'},None)['statusCode'],503)

    def test_unknown_group_is_retained_but_never_assumed_a_country(self):
        from test_originals import archive
        ref,read=archive([series(),series('99901','Future combined area')]);groups,_=n.archive(ref,read,AT)
        self.assertEqual(groups['99901']['scope'],'unreviewed_group')

if __name__=='__main__':unittest.main(verbosity=2)
