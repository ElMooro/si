"""Crisis research regression against retained real FRED responses; offline only."""
from copy import deepcopy
from decimal import localcontext,ROUND_UP
from pathlib import Path
import ast,gzip,hashlib,io,json,sys,unittest
from datetime import datetime,timezone
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
ROOT=HERE.parents[3]
sys.path[:0]=[str(HERE.parent/'source'),str(ROOT/'aws/shared')]
import crisis_research_model as model
import crisis_research_store as store

FIXTURES=HERE/'fixtures'
STAMP='2026-09-20T09:30:00+00:00'


def fixtures():
    source=json.loads((FIXTURES/'macro.json').read_bytes())
    manifest=json.loads((FIXTURES/'manifest.json').read_bytes());originals={}
    for sid,entry in manifest['inputs'].items():
        item=deepcopy(entry)
        for part,e in entry['evidence'].items():
            raw=(FIXTURES/'originals'/(e['sha256']+'.json')).read_bytes()
            assert len(raw)==e['bytes'] and hashlib.sha256(raw).hexdigest()==e['sha256']
            item[part]=json.loads(raw)
        originals[sid]=item
    return source,originals


class Native(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.source,cls.originals=fixtures()
        cls.output=model.build(cls.source,cls.originals,{},STAMP)

    def test_real_native_histories_and_no_invented_score(self):
        out=self.output
        self.assertEqual(out['quality']['fresh_native_series'],23)
        self.assertEqual(sum(len(r['history']) for r in out['measurements'].values()),61175)
        self.assertIsNone(out['master_crisis_score']);self.assertIsNone(out['defcon_level'])
        self.assertEqual(out['decision']['verb'],'WAIT')
        self.assertTrue(all(out[k] is False for k in model.PERMISSIONS))

    def test_latest_shared_date_does_not_match_row_positions(self):
        pair=self.output['comparisons']['sofr_iorb']
        self.assertEqual(pair['value_decimal'],'-5.00')
        self.assertEqual(pair['observation_date'],'2026-09-17')
        self.assertEqual(pair['source_latest_dates']['IORB'],'2026-09-20')
        for sid,index in pair['source_rows'].items():
            self.assertEqual(self.originals[sid]['observations']['observations'][index]['date'],pair['observation_date'])

    def test_native_units_and_signed_comparisons(self):
        self.assertEqual(self.output['comparisons']['hy_ig']['value_decimal'],'192.00')
        self.assertEqual(self.output['comparisons']['vix_vix3m']['value_decimal'],'-3.11')
        self.assertEqual(self.output['comparisons']['vix_vix3m']['unit'],'index_points')
        self.assertNotEqual(self.output['measurements']['WALCL']['unit'],self.output['measurements']['RRPONTSYD']['unit'])

    def test_missing_latest_shared_value_does_not_select_older_row(self):
        rows=deepcopy(self.output['measurements'])
        next(r for r in rows['IORB']['history'] if r['date']=='2026-09-17')['value_decimal']=None
        out=model.difference(rows,'SOFR','IORB','test','test')
        self.assertIsNone(out['value']);self.assertEqual(out['observation_date'],'2026-09-17')
        self.assertIn('no older',out['reason'])

    def test_mismatched_units_and_excessively_old_shared_dates_withhold(self):
        rows=deepcopy(self.output['measurements']);rows['IORB']['unit']='Index'
        self.assertIsNone(model.difference(rows,'SOFR','IORB','test','test')['value'])
        rows['IORB']['unit']='Percent';rows['IORB']['observation_date']='2026-09-30'
        self.assertIn('seven days',model.difference(rows,'SOFR','IORB','test','test')['reason'])

    def test_rehashed_measurement_tampering_still_fails_original_rebuild(self):
        source=deepcopy(self.source);source['measurements']['SOFR']['current']=0
        source['replay']['output_sha256']=model.digest({k:v for k,v in source.items() if k!='replay'})
        with self.assertRaisesRegex(ValueError,'original macro reconstruction differs'):
            model.build(source,self.originals,{},STAMP)

    def test_original_observation_tampering_fails(self):
        originals=deepcopy(self.originals)
        originals['SOFR']['observations']['observations'][0]['value']='100'
        with self.assertRaisesRegex(ValueError,'original macro reconstruction differs'):
            model.build(self.source,originals,{},STAMP)

    def test_missing_sources_never_become_neutral_or_calm(self):
        source=deepcopy(self.source);source['measurements']={}
        source['replay']['output_sha256']=model.digest({k:v for k,v in source.items() if k!='replay'})
        out=model.build(source,{}, {},STAMP)
        self.assertEqual(out['quality']['fresh_native_series'],0)
        self.assertTrue(all(r['value'] is None for r in out['comparisons'].values()))
        self.assertIsNone(out['playbook']);self.assertIsNone(out['defcon_level'])

    def test_stale_acquisition_withholds_current_interpretations_preserves_history(self):
        out=model.build(self.source,self.originals,{},'2026-09-22T09:30:00+00:00')
        self.assertEqual(out['quality']['fresh_native_series'],0)
        self.assertTrue(all(r['value'] is None for r in out['comparisons'].values()))
        self.assertEqual(len(out['measurements']['SOFR']['history']),len(self.output['measurements']['SOFR']['history']))
        self.assertIsNone(out['measurements']['SOFR']['value'])

    def test_future_packet_cannot_enter_research(self):
        with self.assertRaisesRegex(ValueError,'dated macro source'):
            model.build(self.source,self.originals,{},'2026-09-19T09:30:00+00:00')

    def test_fixed_arithmetic_is_independent_of_ambient_decimal_context(self):
        with localcontext() as ctx:
            ctx.prec=8;ctx.rounding=ROUND_UP
            self.assertEqual(model.build(self.source,self.originals,{},STAMP),self.output)

    def test_retained_context_cannot_promote_itself_or_duplicate_votes(self):
        out=model.build(self.source,self.originals,{'global-stress':{'calls_eligible':True,'independent_votes':99}},STAMP)
        self.assertFalse(out['context']['global-stress']['calls_eligible'])
        self.assertEqual(out['context']['global-stress']['independent_votes'],0)
        self.assertEqual(len(out['dependency_graph']['roots']),23)
        self.assertEqual(out['dependency_graph']['independent_votes'],0)

    def test_history_cannot_include_future_rows_or_treat_boolean_as_number(self):
        original={'observations':{'observations':[{'date':'2026-09-20','value':'3.9'},
            {'date':'2026-09-19','value':False},{'date':'2026-09-21','value':'5'}]}}
        rows=model.native_rows(original,'2026-09-20')
        self.assertEqual(len(rows),2);self.assertIsNone(rows[0]['value_decimal'])
        self.assertEqual(rows[1]['source_row_index'],0)


class FakeError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self,objects=None):self.objects=dict(objects or {});self.writes=[];self.reads=[]
    def get_object(self,**kw):
        key=kw['Key'];self.reads.append(key)
        if key not in self.objects:raise FakeError('NoSuchKey')
        raw=self.objects[key];return {'Body':io.BytesIO(raw),'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,**kw):
        old=self.objects.get(kw['Key'])
        if kw.get('IfNoneMatch')=='*' and old is not None:raise FakeError('PreconditionFailed')
        if kw.get('IfMatch') and (old is None or hashlib.sha256(old).hexdigest()!=kw['IfMatch']):raise FakeError('PreconditionFailed')
        self.objects[kw['Key']]=kw['Body'];self.writes.append(kw)


def store_fixture():
    macro=json.loads((FIXTURES/'macro.json').read_bytes());mm=json.loads((FIXTURES/'manifest.json').read_bytes())
    ciss=json.loads((FIXTURES/'ciss.json').read_bytes());cm=json.loads((FIXTURES/'ciss-manifest.json').read_bytes())
    objects={'data/report-measurements.json':model.encoded(macro),'data/ciss-stress.json':model.encoded(ciss),
        macro['replay']['manifest_key']:model.encoded(mm),ciss['replay']['manifest_key']:model.encoded(cm)}
    for module,manifest in ((store.report_observations,mm),(store.ciss_source_model,cm)):
        objects[manifest['compiler']['key']]=Path(module.__file__).read_bytes()
    for item in mm['inputs'].values():
        for ref in item['evidence'].values():objects[ref['key']]=gzip.compress((FIXTURES/'originals'/(ref['sha256']+'.json')).read_bytes(),mtime=0)
    ce=cm['histories'][store.ciss_source_model.HEAD]['evidence']
    objects[ce['key']]=gzip.compress((FIXTURES/'originals'/(ce['sha256']+'.csv')).read_bytes(),mtime=0)
    return objects,{'contract':'crisis-inputs.v1','generated_at':STAMP,'macro':macro,'ciss':ciss,'context':{}}


class ReplayAndPublication(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.objects,cls.inputs=store_fixture()
        s=Storage(cls.objects);cls.output=store.compile_output(cls.inputs,store.reader(s,'b'))
    def setUp(self):self.s=Storage(self.objects);self.read=store.reader(self.s,'b')

    def test_native_ecb_headline_reconstructs_from_exact_csv(self):
        row=self.output['ciss']
        self.assertEqual(row['value_decimal'],'0.03389345690680703')
        self.assertEqual(row['observation_date'],'2026-09-15')
        self.assertEqual(row['unit'],'dimensionless_index');self.assertFalse(row['calls_eligible'])
        self.assertTrue(len(row['history'])>5000)
        self.assertIn('ECB:'+row['series_id'],self.output['dependency_graph']['roots'])

    def test_complete_original_source_fixture_has_portable_digest(self):
        expected=(FIXTURES/'expected-output.sha256').read_text().strip()
        self.assertEqual(model.digest(self.output),expected)

    def test_immutable_inputs_code_and_complete_output_replay_exactly(self):
        ref=store.retain(self.s,'b',self.inputs,self.output)
        self.assertEqual(store.replay(ref,self.read),self.output)
        manifest=json.loads(self.read(ref['manifest_key']))
        self.assertIn('crisis_research_store',manifest['compilers'])

    def test_original_source_tampering_is_rejected_before_publication(self):
        ref=self.inputs['macro']['measurements']['SOFR']['evidence']['observations']
        self.s.objects[ref['key']]=gzip.compress(b'{}',mtime=0)
        with self.assertRaisesRegex(ValueError,'original source bytes differ'):store.compile_output(self.inputs,self.read)
        self.assertNotIn(model.CURRENT,self.s.objects)

    def test_compiler_replacement_and_output_tampering_are_rejected(self):
        ref=store.retain(self.s,'b',self.inputs,self.output);manifest=json.loads(self.read(ref['manifest_key']))
        key=manifest['compilers']['crisis_research_model']['key'];self.s.objects[key]=b'altered'
        with self.assertRaisesRegex(ValueError,'reviewed compiler differs'):store.replay(ref,self.read)
        self.s.objects[key]=Path(model.__file__).read_bytes()
        self.s.objects[manifest['output']['key']]=b'{}'
        with self.assertRaisesRegex(ValueError,'artifact content differs'):store.replay(ref,self.read)

    def test_private_keys_and_foreign_evidence_paths_cannot_be_read(self):
        for key in ('data/brain.json','data/portfolio.json','data/evidence/fred/../../brain.json','audit-private/x'):
            with self.assertRaisesRegex(ValueError,'unapproved'):self.read(key)
        self.assertEqual(self.s.reads,[])

    def test_ecb_source_value_tampering_is_detected_independently(self):
        source=store.ciss_original(self.inputs['ciss'],self.read);source['packet']=deepcopy(source['packet'])
        row=next(r for r in source['packet']['series'] if r['key']==store.ciss_source_model.HEAD)
        row['latest_decimal']='0.9'
        with self.assertRaisesRegex(ValueError,'published value differs'):model.ciss_native(source,STAMP)

    def test_ecb_acquisition_expiry_cannot_renew_native_value(self):
        source=store.ciss_original(self.inputs['ciss'],self.read)
        row=model.ciss_native(source,'2026-09-24T09:30:00+00:00')
        self.assertIsNone(row['value']);self.assertEqual(row['quality']['status'],'stale')
        self.assertEqual(row['last_observed_value'],'0.03389345690680703')

    def test_publication_preserves_complete_prior_packet_and_alias(self):
        prior=b'{"generated_at":"2026-09-19T00:00:00Z","old_score":99,"whole":"preserve all bytes"}'
        self.s.objects[model.CURRENT]=prior;packet={**self.output,'replay':{'test':'fixture'}}
        self.assertTrue(store.publish(self.s,'b',model.CURRENT,packet))
        self.assertTrue(store.publish(self.s,'b','data/defcon.json',packet))
        self.assertEqual(self.s.objects[model.CURRENT],self.s.objects['data/defcon.json'])
        self.assertEqual(self.s.objects[store.PRIVATE+hashlib.sha256(prior).hexdigest()+'.bin'],prior)

    def test_new_compilation_cannot_replace_newer_source_vintage(self):
        newer={**self.output,'generated_at':'2026-09-20T10:30:00Z','source_generated_at':'2026-09-20T10:00:00Z'}
        self.s.objects[model.CURRENT]=model.encoded(newer)
        candidate={**self.output,'generated_at':'2026-09-20T11:00:00Z'}
        self.assertFalse(store.publish(self.s,'b',model.CURRENT,candidate))
        self.assertEqual(json.loads(self.s.objects[model.CURRENT]),newer)

    def test_concurrent_newer_publication_wins_without_text_merging(self):
        old=model.encoded({'generated_at':'2026-09-19T00:00:00Z','old_score':5})
        newer={**self.output,'generated_at':'2026-09-20T11:00:00Z'}
        s=self.s;s.objects[model.CURRENT]=old;original_put=s.put_object
        def racing_put(**kw):
            if kw['Key']==model.CURRENT:s.objects[model.CURRENT]=model.encoded(newer)
            return original_put(**kw)
        with patch.object(s,'put_object',side_effect=racing_put):self.assertFalse(store.publish(s,'b',model.CURRENT,self.output))
        self.assertEqual(json.loads(s.objects[model.CURRENT]),newer)

    def test_real_public_only_store_path_never_reads_private_contexts(self):
        class Clock:
            @staticmethod
            def now(tz):return datetime(2026,9,20,9,45,tzinfo=timezone.utc)
        with patch.object(store,'datetime',Clock):result=store.run(self.s,'b')
        self.assertTrue(result['published'])
        self.assertEqual(self.s.objects[model.CURRENT],self.s.objects['data/defcon.json'])
        self.assertEqual(json.loads(self.s.objects['data/crisis-composite-history.json'])['snapshots'],[])
        self.assertTrue(all(store.allowed(k) or k.startswith(store.PRIVATE) for k in self.s.reads))
        self.assertFalse(any('brain' in k or 'portfolio' in k for k in self.s.reads))

    def test_actual_handler_ignores_legacy_notification_and_private_context_flags(self):
        path=HERE.parent/'source/lambda_function.py';tree=ast.parse(path.read_text(encoding='utf-8'))
        node=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
        scope={'s3':self.s,'S3_BUCKET':'b','json':json};exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
        with patch.object(store,'run',return_value={'published':True}) as run:
            result=scope['lambda_handler']({'test_telegram':True,'backfill':True,'private_context':True},None)
        run.assert_called_once_with(self.s,'b');self.assertEqual(result['statusCode'],200)


if __name__=='__main__':unittest.main()
