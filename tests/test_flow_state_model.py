from pathlib import Path
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared')]
import flow_state_model as m
AT='2026-09-24T23:00:00+00:00'

def fixture():
    blobs={};docs={}
    months=['2025-'+str(x).zfill(2)+'-01' for x in range(8,13)]+['2026-'+str(x).zfill(2)+'-01' for x in range(1,8)]
    def transaction(value,months):return {'usd_bn_decimal':value,'usd_million_decimal':str(m.dec(value)*1000),'months':months,'missing_months':[],'status':'complete'}
    period={'start_date':'2026-09-16','end_date':'2026-09-23'}
    def fund(ticker,value):return {'ticker':ticker,'flow_windows':{'5d':{**period,'status':'complete_descriptive_estimate','observations_required':5,'observations_available':5,'value_decimal':value}}}
    def category(name,tickers,value):return {'category':name,'configured_members':tickers+['MISSING'],'covered_members':tickers,
        'unavailable_members':['MISSING'],'n_etfs':len(tickers),'value_decimal':value,'net_flow_5d_usd':float(value),'period':period}
    extras={
        'data/etf-true-flows.json':{'by_etf':{'AAA':fund('AAA','100.125'),'BBB':fund('BBB','2.25')},
            'category_rotation':[category('ONE',['AAA'],'100.125'),category('TWO',['BBB'],'2.25'),category('OVERLAP',['AAA','BBB'],'102.375')],
            'dependency_roots':['ISSUER:One']},
        'data/capital-inflows.json':{'data_asof':'2026-07-01','holder_splits':{'official':'retained exactly'},'regime':'MONITOR_ONLY',
            'by_asset_class':{'treasuries':{'unit':'usd_bn','series_id':'FORLTTREASNET99996','data_asof':'2026-07-01',
                'latest':transaction('-3.56',['2026-07-01']),'latest_month_b':-3.56,
                'rolling_12mo':transaction('246.534',months),'rolling_12mo_b':246.534}}},
        'data/risk-regime.json':{'dependency_roots':{'volatility':['fred:VIXCLS']}},
        'data/dollar-radar.json':{'series':{'VIXCLS':{},'DGS10':{}},'dependency_graph':{'independent_votes':0}},
        'data/fx-quote-research.json':{'dependency_graph':{'independent_votes':0}},
    }
    def store(raw,key):blobs[key]=raw;return {'key':key,'sha256':m.sha(raw),'bytes':len(raw)}
    def artifact(doc,prefix,kind,ext='json'):
        raw=doc if isinstance(doc,bytes) else m.encoded(doc)
        return store(raw,prefix+kind+'/'+m.sha(raw)+'.'+ext)
    for key,(contract,namespace,run_contract) in m.PARENTS.items():
        prefix='data/'+namespace+'/'
        out={'contract':contract,'generated_at':'2026-09-24T20:00:00Z','quality':{'status':'partial'},**m.PERMISSIONS,**extras[key]}
        output=artifact(out,prefix,'outputs')
        run={'contract':run_contract,'generated_at':out['generated_at'],'output_sha256':output['sha256'],'output':output,
            'input':artifact({'complete':True},prefix,'inputs'),'compilers':{'original':artifact(b'# original\n',prefix,'compilers','py')}}
        run_ref=artifact(run,prefix,'runs');docs[key]={**out,'replay':{'manifest_key':run_ref['key'],'output_sha256':output['sha256']}}
    docs[m.CURRENT]={'engine':'cross-asset-flow-state','version':'1.0','headline':'into equities, out of bonds','generated_at':'2026-09-24T20:01:00Z'}
    for key in m.CONTEXT:docs[key]={'generated_at':'2026-09-24T20:00:00Z','state':'STRONG BUY','distribution':{'accumulation':99}}
    captures={}
    for key,doc in docs.items():
        raw=m.encoded(doc);captures[key]={'source_key':key,'acquired_at':AT,'original':store(raw,m.PRIVATE+m.sha(raw)+'.bin')}
    return {'contract':'flow-state-inputs.v1','generated_at':AT,'captures':captures},blobs,docs

class Tests(unittest.TestCase):
    def test_dated_families_stay_distinct_without_false_outflow_or_total(self):
        inputs,blobs,_=fixture();out=m.compile_output(inputs,blobs.__getitem__)
        self.assertEqual([r['direction'] for r in out['asset_class_rotation']],['net_issuance_estimate']*3)
        self.assertNotIn('out of',out['headline']);self.assertIsNone(out['category_total'])
        self.assertEqual(out['monthly_transactions'][0]['value_decimal'],'-3.56')
        self.assertEqual(out['monthly_transactions'][0]['observation_age_days'],85)
        self.assertFalse(out['monthly_transactions'][0]['month_label_is_release_date'])
        self.assertEqual(out['monthly_transactions'][0]['unit'],'usd_bn')
        self.assertEqual(out['asset_class_rotation'][0]['unit'],'usd')

    def test_exact_sums_coverage_and_overlaps_are_exposed(self):
        inputs,blobs,_=fixture();out=m.compile_output(inputs,blobs.__getitem__)
        self.assertEqual(out['asset_class_rotation'][2]['value_decimal'],'102.375')
        self.assertEqual(out['asset_class_rotation'][2]['unavailable_members'],['MISSING'])
        self.assertEqual(out['category_overlap'],[{'ticker':'AAA','categories':['ONE','OVERLAP'],'additive_across_categories':False},
            {'ticker':'BBB','categories':['TWO','OVERLAP'],'additive_across_categories':False}])

    def test_shared_root_and_legacy_claims_cannot_supply_votes(self):
        inputs,blobs,_=fixture();out=m.compile_output(inputs,blobs.__getitem__)
        self.assertEqual(out['dependency_graph']['shared_roots'],[{'source_identity':'fred:vixcls',
            'parents':['data/dollar-radar.json','data/risk-regime.json'],'independent_confirmation':False}])
        self.assertIsNone(out['hard_assets_and_dollar']['metals_state']);self.assertIsNone(out['dark_pool']['accumulation_n'])
        self.assertEqual(out['independent_investment_votes'],0);self.assertTrue(all(out[k] is False for k in m.FLAGS))
        self.assertEqual(len(out['retained_contexts']),3)
        self.assertEqual(m.original(out['retained_predecessor'],blobs.__getitem__),m.original(inputs['captures'][m.CURRENT]['original'],blobs.__getitem__))

    def test_generation_age_does_not_claim_source_freshness_or_original_replay(self):
        inputs,blobs,_=fixture();out=m.compile_output(inputs,blobs.__getitem__)
        parent=out['parents']['data/capital-inflows.json'];self.assertEqual(parent['publication_age_hours'],3)
        self.assertFalse(parent['publication_age_is_observation_freshness'])
        self.assertFalse(parent['source_original_replay_performed_by_composition'])
        self.assertEqual(out['foreign_flows']['holder_splits'],{'official':'retained exactly'})

    def test_categories_refuse_mixed_periods_wrong_sums_and_duplicate_members(self):
        _,_,docs=fixture();source=docs['data/etf-true-flows.json']
        changes=(lambda p:p['by_etf']['AAA']['flow_windows']['5d'].update(end_date='2026-09-22'),
            lambda p:p['category_rotation'][0].update(value_decimal='101.125'),
            lambda p:p['category_rotation'][0]['covered_members'].append('AAA'),
            lambda p:p['category_rotation'][0].update(period={'start_date':'2026-09-16','end_date':'2026-09-25'}))
        for change in changes:
            bad=copy.deepcopy(source);change(bad)
            with self.assertRaises(ValueError):m.categories(bad,AT)

    def test_missing_and_zero_remain_distinct(self):
        _,_,docs=fixture();source=docs['data/etf-true-flows.json'];source['category_rotation']=[]
        missing={'category':'EMPTY','configured_members':['X'],'covered_members':[],'unavailable_members':['X'],'n_etfs':0,'value_decimal':None,'net_flow_5d_usd':None,'period':None}
        source['category_rotation']=[missing];rows,_=m.categories(source,AT)
        self.assertEqual(rows[0]['direction'],'unavailable');self.assertIsNone(rows[0]['value_decimal'])
        missing['value_decimal']='0'
        with self.assertRaises(ValueError):m.categories(source,AT)

    def test_tic_unit_conversion_and_month_sequence_are_required(self):
        _,_,docs=fixture();source=docs['data/capital-inflows.json']
        for change in (lambda p:p['by_asset_class']['treasuries']['latest'].update(usd_million_decimal='-3.56'),
            lambda p:p['by_asset_class']['treasuries']['rolling_12mo']['months'].__setitem__(1,'2025-10-01')):
            bad=copy.deepcopy(source);change(bad)
            with self.assertRaises(ValueError):m.transactions(bad,AT)

    def test_changed_input_and_foreign_parent_path_fail_before_unsafe_read(self):
        inputs,blobs,docs=fixture();blobs[inputs['captures'][m.CURRENT]['original']['key']]+=b' '
        with self.assertRaises(ValueError):m.compile_output(inputs,blobs.__getitem__)
        parent=docs['data/risk-regime.json'];parent['replay']['manifest_key']='data/trade-tickets.json';calls=[]
        with self.assertRaises(ValueError):m.bind_parent('data/risk-regime.json',parent,lambda k:calls.append(k),AT)
        self.assertEqual(calls,[])

    def test_json_rejects_duplicate_keys_and_nonfinite_values(self):
        for raw in (b'{"a":1,"a":2}',b'{"a":NaN}'):
            with self.assertRaises(ValueError):m.strict(raw)

if __name__=='__main__':unittest.main(verbosity=2)
