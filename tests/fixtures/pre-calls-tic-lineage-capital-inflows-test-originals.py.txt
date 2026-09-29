import ast,copy,gzip,hashlib,importlib.util,io,json,sys,types,unittest,zipfile
from pathlib import Path
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4];SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(ROOT/'aws/shared'),str(SOURCE)]
import tic_original as n,tic_research as m,tic_store as s,evidence_store
AT='2026-09-19T15:00:00+00:00';VINTAGE='2026-09-19'


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


def retain(store,url,body):
    ref=evidence_store.capture(store,'b','tic',url,body,n.clock(AT))
    return {'url':url,'acquired_at':AT,'evidence':ref}


def fixture():
    store=Storage();members=[];refs={};days=[m.month_before('2026-07-01',i) for i in range(36)]
    values={'total':4000,'official':1000,'private':3000,'us_abroad':2500,'short_treasury':0}
    for sid,(identity,role,title) in n.SERIES.items():
        value=values.get(role,1000);rows=[[d,str(value)] for d in days]
        meta={'title':title,'frequency':'M','season':'NSA','units':'Millions of Dollars','additional':{'status':'A'}}
        members.append({'source_id':identity,'metadata':meta,'observations':rows})
        definition={'id':sid,'title':title,'units':'Millions of Dollars','frequency_short':'M','seasonal_adjustment':'Not Seasonally Adjusted','observation_start':days[-1]}
        refs[sid+':definition']=retain(store,n.definition_url(sid,VINTAGE),m.encoded({'seriess':[definition]}))
        obs={'units':'lin','count':len(rows),'limit':4000,'offset':0,'observations':[{'date':d,'value':v,'realtime_start':VINTAGE,'realtime_end':VINTAGE} for d,v in rows]}
        refs[sid+':observations']=retain(store,n.observations_url(sid,definition,VINTAGE),m.encoded(obs))
    bulk={'releaseID':'3','version':'2.0','transmissionDt':'unknown zone','series':members}
    refs['bulk']=archive(store,bulk)
    urls=n.calendar_urls(AT)
    refs['series_release']=retain(store,urls['series_release'],m.encoded({'releases':[{'id':3,'name':'Treasury International Capital: Continuous Securities Long Term (CSLT)'}]}))
    refs['release_dates']=retain(store,urls['release_dates'],m.encoded({'count':2,'offset':0,'limit':1000,'release_dates':[{'release_id':3,'date':'2026-09-16'},{'release_id':3,'date':'2026-10-16'}]}))
    inputs={'contract':'tic-original-inputs.v1','originals':refs,'requested_fred_vintage':VINTAGE,'acquisition_errors':{},'legacy':{'fixture':True}}
    return store,inputs,bulk


def archive(store,bulk):
    raw=io.BytesIO()
    with zipfile.ZipFile(raw,'w',zipfile.ZIP_DEFLATED) as z:z.writestr('cslt.json',m.encoded(bulk))
    return retain(store,n.CSLT_URL,raw.getvalue())


class Tests(unittest.TestCase):
    def test_originals_reconcile_all_history_and_preserve_zero(self):
        store,inputs,_=fixture();out,hist=m.build(inputs,s.raw_reader(store,'b'),AT)
        self.assertEqual(out['quality']['status'],'fresh');self.assertEqual(out['headline']['net_cross_border_lt_12mo_b'],18)
        self.assertEqual(out['headline']['short_term_treasury_12mo_b'],0)
        self.assertEqual(out['holder_splits']['lt_total']['official']['sum_12m'],12)
        self.assertEqual(len(hist),20);self.assertEqual(out['call'],None);self.assertIs(out['sizing_eligible'],False)
        self.assertEqual(out['regime'],'MONITOR_ONLY');self.assertIsNone(out['quality']['publication_date'])
        self.assertTrue(all(v['status']=='matching' for v in out['cross_source_checks'].values()))

    def test_window_uses_exact_calendar_not_observation_count(self):
        rows=[{'date':m.month_before('2026-07-01',i),'value_decimal':'0','row_index':i} for i in range(13)]
        self.assertEqual(m.window(rows,'2026-07-01',12)['usd_million_decimal'],'0')
        del rows[4]
        self.assertIsNone(m.window(rows,'2026-07-01',12)['usd_million_decimal'])
        self.assertEqual(m.window(rows,'2026-07-01',12)['missing_months'],['2026-03-01'])

    def test_reporting_rounding_is_bounded_per_reported_amount(self):
        def rows(v):return [{'date':'2026-07-01','value_decimal':str(v),'row_index':0}]
        self.assertEqual(m.reconcile(rows(4002),{str(i):rows(1000) for i in range(4)},'2026-07-01')['status'],'within_reporting_rounding')
        self.assertEqual(m.reconcile(rows(4003),{str(i):rows(1000) for i in range(4)},'2026-07-01')['status'],'unreconciled')
        self.assertEqual(m.reconcile(rows(4002),{'official':rows(1000),'private':rows(3000)},'2026-07-01')['status'],'unreconciled')

    def test_old_holder_disagreement_blocks_twelve_month_split(self):
        store,inputs,bulk=fixture()
        row=next(v for v in bulk['series'] if v['source_id']=='for_lt_total_net_99990')
        row['observations'][3][1]='1100';inputs['originals']['bulk']=archive(store,bulk)
        out,_=m.build(inputs,s.raw_reader(store,'b'),AT)
        self.assertEqual(out['holder_splits']['lt_total']['official']['latest'],1)
        self.assertIsNone(out['holder_splits']['lt_total']['official']['sum_12m'])
        self.assertEqual(out['cross_source_checks']['FORLTTOTALNET99990']['status'],'source_disagreement')
        self.assertEqual(out['quality']['status'],'partial')

    def test_native_missing_month_is_not_filled_from_fred(self):
        store,inputs,bulk=fixture();row=next(v for v in bulk['series'] if v['source_id']=='for_lt_total_net_99996')
        row['observations'][0][1]='.';inputs['originals']['bulk']=archive(store,bulk)
        out,_=m.build(inputs,s.raw_reader(store,'b'),AT)
        self.assertIsNone(out['headline']['latest_month_b']);self.assertIsNone(out['headline']['foreign_net_into_us_lt_12mo_b'])

    def test_original_hash_and_request_identity_are_checked(self):
        store,inputs,_=fixture();ref=inputs['originals']['bulk'];bad=copy.deepcopy(ref);bad['url']='https://example.org/not-tic.zip'
        with self.assertRaises(ValueError):n.cslt(bad,s.raw_reader(store,'b'),AT)
        store.objects[ref['evidence']['key']]=gzip.compress(b'wrong')
        with self.assertRaises(ValueError):n.cslt(ref,s.raw_reader(store,'b'),AT)

    def test_native_identity_units_duplicates_and_reporting_precision_fail(self):
        for mutation in ('duplicate','unit','precision','day','future'):
            store,inputs,bulk=fixture()
            if mutation=='duplicate':bulk['series'].append(bulk['series'][0])
            elif mutation=='unit':bulk['series'][0]['metadata']['units']='Billions of Dollars'
            elif mutation=='precision':bulk['series'][0]['observations'][0][1]='0.25'
            elif mutation=='day':bulk['series'][0]['observations'][0][0]='2026-07-02'
            else:bulk['series'][0]['observations'][0][0]='2027-07-01'
            with self.subTest(mutation=mutation),self.assertRaises(ValueError):n.cslt(archive(store,bulk),s.raw_reader(store,'b'),AT)
        for value in (True,False,'NaN','Infinity'):
            with self.assertRaises(ValueError):n.amount(value)

    def test_release_calendar_and_acquisition_age_are_distinct(self):
        store,inputs,_=fixture();cal=n.release_calendar(inputs['originals'],s.raw_reader(store,'b'),AT)
        self.assertEqual(cal['nominal_expected_observation_month'],'2026-07-01')
        self.assertEqual(n.quality('2026-06-01',AT,cal,AT)['status'],'release_due_unverified')
        later='2026-09-20T18:00:00+00:00';self.assertEqual(n.quality('2026-07-01',AT,cal,later)['status'],'stale_source')
        self.assertEqual(n.quality('2026-07-01',AT,None,AT)['status'],'release_calendar_unverified')
        at='2026-10-17T04:00:00+00:00';cal['acquired_at']=at
        self.assertEqual(n.quality('2026-08-01',at,cal,at)['status'],'release_due_unverified')

    def test_fred_future_vintage_and_truncated_response_do_not_qualify(self):
        store,inputs,_=fixture();sid='FORLTTOTALNET99996';ref=inputs['originals'][sid+':observations']
        doc=n.strict_json(s.raw_reader(store,'b')(ref['evidence']['key']));doc['count']+=1
        inputs['originals'][sid+':observations']=retain(store,ref['url'],m.encoded(doc))
        out,_=m.build(inputs,s.raw_reader(store,'b'),AT)
        self.assertEqual(out['cross_source_checks'][sid]['status'],'unavailable');self.assertEqual(out['quality']['status'],'partial')
        inputs['requested_fred_vintage']='2026-09-20'
        with self.assertRaises(ValueError):m.build(inputs,s.raw_reader(store,'b'),AT)

    def test_full_store_replay_and_protected_legacy_preservation(self):
        store,inputs,_=fixture();legacy=m.encoded({'version':'1.1.0','generated_at':'2026-09-18T00:00:00Z','all_original_fields':{'x':123}});store.objects[m.CURRENT]=legacy
        with patch.object(s,'collect',return_value=(inputs['originals'],{},VINTAGE)),patch.object(s,'now',return_value=AT):result=s.run(store,'b','fixture')
        self.assertTrue(result['published']);self.assertTrue(any(k.startswith(s.PRIVATE) and v==legacy for k,v in store.objects.items()))
        manifest=json.loads(s.raw_reader(store,'b')(result['replay']['manifest_key']));out=s.replay(manifest,s.raw_reader(store,'b'))
        self.assertEqual(m.digest(out),result['replay']['output_sha256'])
        rolling=json.loads(s.raw_reader(store,'b')(out['rolling_history']['key']))
        self.assertEqual(rolling['unit'],'mixed_explicit')
        self.assertEqual(rolling['field_units']['rows.*.rolling_12mo_b'],'usd_bn')
        self.assertEqual(rolling['field_units']['rows.*.totals.*.usd_million_decimal'],'usd_million')
        store.objects[manifest['output']['key']]=b'{}'
        with self.assertRaises(ValueError):s.replay(manifest,s.raw_reader(store,'b'))

    def test_failed_candidate_retains_inputs_and_does_not_publish(self):
        store,inputs,_=fixture();old=b'{"version":"1.1.0"}';store.objects[m.CURRENT]=old
        with patch.object(s,'collect',return_value=({}, {'bulk':'HTTP_503'},VINTAGE)),patch.object(s,'now',return_value=AT),self.assertRaises(KeyError):s.run(store,'b','fixture')
        self.assertEqual(store.objects[m.CURRENT],old)
        attempts=[json.loads(v) for k,v in store.objects.items() if k.startswith(m.PREFIX+'attempts/')]
        self.assertEqual(len(attempts),1);self.assertIn(attempts[0]['input']['key'],store.objects)
        self.assertEqual(attempts[0]['source_status_codes'],{'bulk':'HTTP_503'})

    def test_current_write_failure_preserves_replayable_candidate(self):
        store,inputs,_=fixture();old=b'{"version":"1.1.0"}';store.objects[m.CURRENT]=old;store.fail_current=True
        with patch.object(s,'collect',return_value=(inputs['originals'],{},VINTAGE)),patch.object(s,'now',return_value=AT),self.assertRaises(StorageError):s.run(store,'b','fixture')
        self.assertEqual(store.objects[m.CURRENT],old);self.assertTrue(any(k.startswith(m.PREFIX+'runs/') for k in store.objects))

    def test_publication_cannot_overwrite_newer_clock(self):
        store=Storage();packet={'contract':m.CONTRACT,'generated_at':AT,'source_clocks':{'a':AT}}
        store.objects[m.CURRENT]=m.encoded({**packet,'generated_at':'2026-09-20T00:00:00Z'})
        self.assertFalse(s.publish(store,'b',packet))
        store.objects[m.CURRENT]=m.encoded({**packet,'other':'conflict'})
        with self.assertRaises(ValueError):s.publish(store,'b',packet)

    def test_http_read_never_recollects(self):
        store=Storage();store.objects[m.CURRENT]=m.encoded({'contract':m.CONTRACT,'generated_at':AT})
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:store),'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'fixture')}):
            spec=importlib.util.spec_from_file_location('tic_active_handler_test',SOURCE/'lambda_function.py');module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with patch.object(s,'run',side_effect=AssertionError('collector must not run')):
            response=module.lambda_handler({'requestContext':{'http':{'method':'GET'}}},None)
        self.assertEqual(response['statusCode'],200);self.assertEqual(response['headers']['Cache-Control'],'no-store')

    def test_actual_risk_overlay_cannot_size_from_unqualified_transactions(self):
        source=(ROOT/'aws/lambdas/justhodl-risk-regime/source/lambda_function.py').read_text(encoding='utf-8')
        block=source[source.index('    capital_inflows = None'):source.index('    # ── secondary risk overlay')]
        import textwrap
        for authority in ({},{'calls_eligible':True},{'calls_eligible':True,'sizing_eligible':True}):
            ci={'ok':True,'quality':{'status':'fresh'},'regime':'SUDDEN_STOP',**authority}
            scope={'_read':lambda key:ci,'score':30,'posture':{'size_mult':1},'all_tells':[]}
            exec(compile(textwrap.dedent(block),'actual_risk_tic_boundary','exec'),scope)
            self.assertEqual(scope['posture'],{'size_mult':1});self.assertEqual(scope['all_tells'],[])
            self.assertEqual(scope['capital_inflows']['confirmation'],'NOT_QUALIFIED')

    def test_actual_allocator_does_not_manufacture_foreign_bond_signal(self):
        source=(ROOT/'aws/lambdas/justhodl-master-allocator/source/lambda_function.py').read_text(encoding='utf-8')
        block=source[source.index('    ci = read_json("data/capital-inflows.json")'):source.index('    # Brain macro posture')]
        import textwrap
        ci={'quality':{'status':'fresh'},'by_asset_class':{'treasuries':{'latest_month_b':100,'rolling_12mo_b':500}}}
        scope={'read_json':lambda key:ci,'out':{},'clamp':lambda x,a,b:max(a,min(b,x))}
        exec(compile(textwrap.dedent(block),'actual_allocator_tic_boundary','exec'),scope)
        self.assertEqual(scope['out'],{})

    def test_actual_cross_asset_synthesis_preserves_context_without_authority(self):
        import ast
        source=(ROOT/'aws/lambdas/justhodl-cross-asset-flow-state/source/lambda_function.py').read_text(encoding='utf-8')
        handler=next(v for v in ast.parse(source).body if isinstance(v,ast.FunctionDef) and v.name=='lambda_handler')
        assignment=next(v for v in handler.body if isinstance(v,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='foreign' for t in v.targets))
        ci={'regime':'SUDDEN_STOP','calls_eligible':True,'sizing_eligible':True,'data_asof':'2026-07-01',
            'generated_at':AT,'by_asset_class':{'treasuries':{'latest_month_b':0}},'quality':{'status':'fresh'},'replay':{'manifest_key':'fixture'}}
        scope={'ci':ci};exec(compile(ast.Module(body=[assignment],type_ignores=[]),'actual_cross_asset_tic_boundary','exec'),scope)
        result=scope['foreign'];self.assertEqual(result['regime'],'MONITOR_ONLY');self.assertEqual(result['source_regime'],'SUDDEN_STOP')
        self.assertFalse(result['calls_eligible']);self.assertFalse(result['sizing_eligible'])
        self.assertEqual(result['by_asset_class'],ci['by_asset_class']);self.assertEqual(result['quality'],ci['quality'])
        self.assertEqual(result['replay'],ci['replay']);self.assertEqual(result['observation_date'],ci['data_asof'])


if __name__=='__main__':unittest.main(verbosity=2)
