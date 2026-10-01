"""Synthetic desk supplement integration: reuse exact reducer and retained evidence."""
from pathlib import Path
from unittest import mock
import copy, gzip, json, os, sys, unittest
sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).parent)]
import etf_desk_model as model
import etf_desk_store as store
from test_etf_desk_model import fixture, AT
from test_etf_desk_store import fixture as stored_fixture
from test_etf_ownership_summary import pair, GENERATED


def enable(inputs):
    inputs.update(contract='etf-desk-inputs.v2', extra_holdings_summary_policy=model.SUPPLEMENT_POLICY)
    return inputs


class Supplement(unittest.TestCase):
    def test_exact_16_inventory_no_overlaps_and_no_duplicate_shards(self):
        desk = model.catalog.DESK
        inputs, objects, flows, holdings = fixture(desk, tuple(store.flow_catalog.ETF_UNIVERSE))
        old_objects = {}; old = model.build(inputs, objects.__getitem__, lambda k,v: old_objects.__setitem__(k,v), flows, holdings)
        emitted = {}; enable(inputs)
        with mock.patch.object(model.holdings, 'reconstruct_or_reject', wraps=model.holdings.reconstruct_or_reject) as reconstruct:
            out = model.build(inputs, objects.__getitem__, lambda k,v: emitted.__setitem__(k,v), flows, holdings)
        self.assertEqual(reconstruct.call_count, 32)
        ref = out['extra_holdings_summary']; manifest = json.loads(emitted[ref['manifest']['key']])
        self.assertEqual(set(manifest['funds']), model.SUPPLEMENT_FUNDS)
        self.assertEqual(ref['configured_funds'], sorted(model.SUPPLEMENT_FUNDS))
        self.assertEqual(ref['canonical_overlap_excluded'], 100)
        self.assertEqual(manifest['configured_fund_count'], 16)
        self.assertFalse(set(manifest['funds']) & set(holdings['funds']))
        for ticker, refs in manifest['funds'].items():
            h = out['funds'][ticker]['holdings']
            self.assertEqual(refs['current_snapshot'], h['current']['snapshot'])
            self.assertEqual(refs['prior_snapshot'], h['prior']['snapshot'])
            self.assertEqual(refs['comparison'], h['comparison'])
        self.assertEqual({k:v for k,v in emitted.items() if k in old_objects}, old_objects)
        self.assertTrue(all('/directories/' in k for k in emitted.keys() - old_objects.keys()))
        self.assertEqual({k:v for k,v in out.items() if k not in ('version','extra_holdings_summary')},
                         {k:v for k,v in old.items() if k != 'version'})
        self.assertEqual(out['version'], '2.1.0')

    def test_duplicates_overlaps_missing_extra_and_unreviewed_expansion_fail_closed(self):
        for case in ('duplicate_desk', 'overlap', 'missing', 'expansion', 'v1_policy', 'bad_policy'):
            inputs, objects, flows, holdings = fixture(); enable(inputs); desk = ('SPY','VOO','BND')
            if case == 'duplicate_desk': desk += ('BND',)
            if case == 'overlap': inputs['extra_holdings']['SPY'] = inputs['extra_holdings']['BND']
            if case == 'missing': del inputs['extra_holdings']['BND']
            if case == 'expansion':
                desk = ('SPY','VOO','UNAPPROVED'); inputs, objects, flows, holdings = fixture(desk); enable(inputs)
            if case == 'v1_policy': inputs['contract'] = 'etf-desk-inputs.v1'
            if case == 'bad_policy': inputs['extra_holdings_summary_policy'] = 'unreviewed'
            with self.subTest(case=case), mock.patch.object(model.catalog,'DESK',desk), self.assertRaises(ValueError):
                model.build(inputs,objects.__getitem__,lambda *a: None,flows,holdings)

    def test_extra_holdings_passes_partial_identity_dates_zero_null_and_expiry_unchanged(self):
        for case in ('qualified','partial','missing','ambiguous','same_date','date_mismatch','zero_null','expired'):
            a,b = pair(); a['ticker'] = b['ticker'] = 'BND'
            if case == 'partial': a['quality'].update(status='partial',pagination_complete=False)
            if case == 'missing': a['quality']['missing_identity_rows']=1; a['rows'][0]['identity_key']=None
            if case == 'ambiguous': a['quality']['duplicate_identity_rows']=1; a['rows'].append(copy.deepcopy(a['rows'][0]))
            if case == 'same_date': b=copy.deepcopy(a)
            if case == 'date_mismatch': a['effective_dates']['2026-09-16']=1
            if case == 'zero_null': a['rows'][0].update(shares_held_raw_decimal='0',weight_raw_decimal=None)
            if case == 'expired': a['source_valid_until']=GENERATED
            summary = model.holdings_model.OwnershipSummary(GENERATED,1); emitted={}
            with self.subTest(case=case), mock.patch.object(model.holdings,'reconstruct_or_reject',side_effect=[a,b]):
                h=model.extra_holdings({'current':{},'prior':{}},lambda k: None,GENERATED,
                                      lambda k,v: emitted.__setitem__(k,v),summary)
                direct=model.holdings_model.OwnershipSummary(GENERATED,1)
                direct.add('BND',a,b,model.holdings.compare(a,b),h['current']['snapshot'],h['prior']['snapshot'],h['comparison'],{})
                expected={}; wanted=direct.finish(lambda k,v: expected.__setitem__(k,v))
                actual={}; result=summary.finish(lambda k,v: actual.__setitem__(k,v))
                self.assertEqual((result,actual),(wanted,expected))
                manifest=json.loads(actual[result['manifest']['key']]); fund=manifest['funds']['BND']
                if case in ('qualified','same_date','zero_null'): self.assertIsNone(fund['qualification_exclusion'])
                else: self.assertIsNotNone(fund['qualification_exclusion'])
                if case == 'same_date': self.assertEqual(fund['comparison_exclusion'],'incompatible_effective_dates')
                if case == 'zero_null':
                    snapshot=json.loads(emitted[h['current']['snapshot']['key']])
                    row=json.loads(emitted[snapshot['parts'][0]['key']])['rows'][0]
                    self.assertEqual(row['shares_held_raw_decimal'],'0'); self.assertIsNone(row['weight_raw_decimal'])


class RetainedSupplement(unittest.TestCase):
    def setUp(self):
        for patch in (mock.patch.object(model.catalog,'DESK',('SPY','VOO','BND')),
                      mock.patch.dict(store.flow_catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True)):
            patch.start(); self.addCleanup(patch.stop)

    def compile(self,db,inputs):
        read=store.reader(db,'fixture'); output=store.compile_output(inputs,read,lambda k,v:store.immutable(db,'fixture',k,v,read=read))
        ref=store.retain(db,'fixture',inputs,output,read)
        self.assertEqual(model.encoded(store.replay(ref,read)),model.encoded(output))
        return output,ref

    def test_immediate_predecessor_compiler_bytes_replay_exactly(self):
        from test_etf_holdings_store import Storage
        root=Path(__file__).resolve().parents[3]
        fixture=json.loads(gzip.decompress((root/'tests/fixtures/desk-supplement-predecessor-synthetic.json.gz').read_bytes()))
        db=Storage(); db.objects={k:v.encode() for k,v in fixture['objects'].items()}
        expected={k:v for k,v in fixture['packet'].items() if k!='replay'}
        actual=store.replay(fixture['packet']['replay'],store.reader(db,'fixture'))
        self.assertEqual(model.encoded(actual),model.encoded(expected))
        self.assertNotIn('extra_holdings_summary',actual)
        run=json.loads(db.objects[fixture['packet']['replay']['manifest_key']])
        for module in ('etf_desk_model','etf_desk_store'):
            key=run['compilers'][module]['key']; original=db.objects[key]; db.objects[key]+=b' '
            with self.assertRaises(ValueError): store.replay(fixture['packet']['replay'],store.reader(db,'fixture'))
            db.objects[key]=original

    def test_new_replay_rejects_tampered_manifest_and_page(self):
        db,inputs=stored_fixture(); out,ref=self.compile(db,enable(inputs))
        manifest_ref=out['extra_holdings_summary']['manifest']; manifest=json.loads(db.objects[manifest_ref['key']])
        page=next(g['parts'][0] for g in manifest['cohorts'] if g['parts'])
        for item in (manifest_ref,page):
            original=db.objects[item['key']]; db.objects[item['key']]+=b' '
            with self.assertRaises(ValueError): store.replay(ref,store.reader(db,'fixture'))
            db.objects[item['key']]=original

    def test_all_reducer_overflows_preserve_base_and_emit_no_summary_prefix(self):
        db,inputs=stored_fixture(); old,_=self.compile(db,inputs)
        for limit in ('SUMMARY_MAX_RECORDS','SUMMARY_MAX_OBSERVATIONS','SUMMARY_MAX_BYTES','SUMMARY_PAGE_BYTES','SUMMARY_MANIFEST_BYTES','SUMMARY_MAX_PAGES'):
            db,inputs=stored_fixture(); before=set(db.objects)
            with self.subTest(limit=limit), mock.patch.object(model.holdings_model,limit,0):
                out,_=self.compile(db,enable(inputs))
                self.assertEqual(out['extra_holdings_summary']['status'],'unavailable')
                self.assertEqual(out['funds'],old['funds'])
                self.assertFalse(any('/directories/' in k for k in set(db.objects)-before))

    def test_acquisition_brake_retry_and_recovery_preserve_old_and_new_bytes(self):
        db,inputs=stored_fixture(); payload={k:inputs[k] for k in ('query_date','profiles','extra_flows','extra_holdings','provider_requests','original_provider_bytes')}
        packets=[]; results=[]
        with mock.patch.object(store,'collect',return_value=payload) as collect:
            for number,value in enumerate(('true','false','true')):
                stamp='2026-09-21T10:0%d:00+00:00'%number
                with mock.patch.dict(os.environ,{'ETF_OWNERSHIP_SUMMARY_ENABLED':value}),mock.patch.object(store,'now',return_value=stamp):
                    result=store.run(db,'fixture','brake-'+str(number),'execution'); results.append(result)
                    packet=json.loads(db.objects[model.CURRENT]); packets.append(packet)
                    self.assertEqual('extra_holdings_summary' in packet,value=='true')
                    self.assertEqual(result['provider_requests'],inputs['provider_requests'])
                    saved=dict(db.objects)
                    self.assertEqual(store.run(db,'fixture','brake-'+str(number),'retry'),result)
                    self.assertEqual(saved,db.objects)
                    for prior in packets:
                        expected={k:v for k,v in prior.items() if k!='replay'}
                        self.assertEqual(model.encoded(store.replay(prior['replay'],store.reader(db,'fixture'))),model.encoded(expected))
                    if value == 'false':
                        recovered=store.run(db,'fixture','recover-new','exec',recover_run=packets[0]['replay'])
                        self.assertEqual(recovered['provider_requests_this_execution'],0)
                        self.assertEqual(recovered['replay']['output_sha256'],packets[0]['replay']['output_sha256'])
                        self.assertEqual(json.loads(db.objects[model.CURRENT]),packet)
            self.assertEqual(collect.call_count,3)
        with mock.patch.object(store,'now',return_value='2026-10-01T00:00:00Z'),self.assertRaises(ValueError):
            store.recovery_inputs(packets[0]['replay'],store.reader(db,'fixture'))

    def test_absent_or_malformed_brake_keeps_legacy_input(self):
        for value in (None,'','false','TRUE','1',' true ','unexpected'):
            db,inputs=stored_fixture(); payload={k:inputs[k] for k in ('query_date','profiles','extra_flows','extra_holdings','provider_requests','original_provider_bytes')}
            with self.subTest(value=value),mock.patch.dict(os.environ,{},clear=True),mock.patch.object(store,'collect',return_value=payload),mock.patch.object(store,'now',return_value=AT):
                if value is not None: os.environ['ETF_OWNERSHIP_SUMMARY_ENABLED']=value
                result=store.run(db,'fixture','off','exec'); retained=json.loads(db.objects[result['retained_input']['key']])
                self.assertEqual(retained['contract'],'etf-desk-inputs.v1')
                self.assertNotIn('extra_holdings_summary_policy',retained)
                self.assertNotIn('extra_holdings_summary',json.loads(db.objects[model.CURRENT]))


if __name__ == '__main__': unittest.main()
