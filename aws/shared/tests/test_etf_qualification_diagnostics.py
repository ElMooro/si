"""Invented evidence only: explanatory metadata cannot change membership decisions."""
import copy, gzip, json, sys, unittest
from pathlib import Path
from unittest import mock
sys.path[:0] = [str(Path(__file__).resolve().parents[1]), str(Path(__file__).parent)]
import etf_holdings_model as model
import etf_holdings_native as native
import etf_holdings_store as store
import etf_desk_store as desk
from test_etf_ownership_summary import pair, GENERATED, output
from test_etf_holdings_store import Storage, fixture
ROOT = Path(__file__).resolve().parents[3]


def diagnosed(a, b, count=1):
    acc = model.OwnershipSummary(GENERATED, count, model.DIAGNOSTIC_POLICY); objects = {}
    for i in range(count):
        fund = 'F' + str(i)
        acc.add(fund, a, b, native.compare(a,b), {'synthetic':'current'}, {'synthetic':'prior'},
                {'synthetic':'comparison'}, {})
    ref = acc.finish(lambda k,v: objects.__setitem__(k,v))
    doc = json.loads(objects[ref['manifest']['key']]) if ref['status']=='complete' else None
    if doc:
        diagnostics = json.loads(objects[doc['qualification_diagnostics']['key']])
        for fund,info in diagnostics['funds'].items(): doc['funds'][fund]['qualification_diagnostics'] = info
    return ref, doc, objects


class Diagnostics(unittest.TestCase):
    def test_isolated_and_overlapping_quality_causes_leave_decisions_and_pages_exact(self):
        fields = ('missing_identity_rows','duplicate_identity_rows','rows_with_field_errors')
        for mask in range(8):
            a,b = pair()
            for bit,key in enumerate(fields): a['quality'][key] = int(bool(mask & (1 << bit)))
            old_ref,old,old_objects = output({'F0':(a,b)})
            ref,new,objects = diagnosed(a,b)
            expected = [key for bit,key in enumerate(fields) if mask & (1 << bit)]
            info = new['funds']['F0']['qualification_diagnostics']
            self.assertEqual(info['current']['exclusion_reasons'], expected)
            self.assertEqual(info['current']['quality_counters'],{k:a['quality'].get(k) for k in model.QUALITY_COUNTERS})
            self.assertEqual(new['cohorts'],old['cohorts'])
            for k,v in old_objects.items():
                if k != old_ref['manifest']['key']: self.assertEqual(objects[k],v)
            self.assertEqual(new['funds']['F0']['qualification_exclusion'],old['funds']['F0']['qualification_exclusion'])
            self.assertEqual(new['funds']['F0']['comparison_exclusion'],old['funds']['F0']['comparison_exclusion'])
            self.assertEqual(a['quality']['status'],'complete_returned_snapshot')
            self.assertEqual(new['funds']['F0']['qualification_exclusion'] is None,mask == 0)

    def test_missing_metadata_stays_null_and_unknown_not_measured_zero(self):
        for field in model.QUALITY_COUNTERS:
            for value in ('absent',None,True,-1):
                a,b = pair()
                if value=='absent': a['quality'].pop(field,None)
                else: a['quality'][field]=value
                d=model.qualification_diagnostics(a,native.clock(GENERATED))
                self.assertEqual(d['quality_counters'][field],None if value=='absent' else value)
                if field in ('missing_identity_rows','duplicate_identity_rows','rows_with_field_errors'):
                    self.assertIn(field+'_metadata_unresolved',d['exclusion_reasons'])

    def test_all_independent_clock_pagination_and_quality_reasons(self):
        a,b=pair();a['quality'].update(status='partial',pagination_complete=False,missing_identity_rows=2)
        a['source_valid_until']=GENERATED;a['processed_date']='2099-01-01';a['effective_dates']['2020-01-01']=1
        d=model.qualification_diagnostics(a,native.clock(GENERATED))
        self.assertEqual(d['exclusion_reasons'],['incomplete_returned_snapshot','source_check_not_current',
                         'mixed_or_future_effective_dates','future_processing_date','missing_identity_rows'])
        self.assertEqual(model.summary_reason(a,native.clock(GENERATED)),'incomplete_returned_snapshot')
        for key,reason in [('source_valid_until','invalid_source_clocks'),('effective_dates','invalid_effective_dates'),('processed_date','invalid_processing_date')]:
            a,b=pair();a.pop(key)
            self.assertIn(reason,model.qualification_diagnostics(a,native.clock(GENERATED))['exclusion_reasons'])

    def test_valid_null_zero_types_and_same_date_revision(self):
        for kind in ('Bond','Metal','Crypto'):
            a,b=pair();a['rows'][0].update(shares_held_raw_decimal='0',weight_raw_decimal=None,market_value_raw_decimal=None,currency_traded=None,asset_class=kind)
            _,doc,_=diagnosed(a,a)
            fund=doc['funds']['F0'];d=fund['qualification_diagnostics']
            self.assertEqual(d['current']['exclusion_reasons'],[])
            self.assertIsNone(fund['qualification_exclusion'])
            self.assertEqual(fund['comparison_exclusion'],'incompatible_effective_dates')
            self.assertFalse(d['comparable_snapshots'])
            self.assertEqual(d['comparison_date_reason'],'same_effective_date_revision')
            self.assertEqual(a['rows'][0]['shares_held_raw_decimal'],'0')
            self.assertIsNone(a['rows'][0]['weight_raw_decimal'])

    def test_partial_does_not_get_mislabeled_as_date_incompatible(self):
        a,b=pair();a['quality'].update(status='partial',pagination_complete=False)
        _,doc,_=diagnosed(a,b)
        d=doc['funds']['F0']['qualification_diagnostics']
        self.assertFalse(d['comparable_snapshots'])
        self.assertIsNone(d['comparison_date_reason'])
        self.assertEqual(d['current']['exclusion_reasons'],['incomplete_returned_snapshot'])
        self.assertEqual(model.comparison_date_reason(b,a),'current_effective_date_not_after_prior')

    def test_prior_reasons_do_not_exclude_current_membership(self):
        a,b=pair();b['quality'].update(duplicate_identity_rows=2,rows_with_field_errors=1)
        _,doc,_=diagnosed(a,b);f=doc['funds']['F0']
        self.assertIsNone(f['qualification_exclusion'])
        self.assertEqual(f['qualification_diagnostics']['prior']['exclusion_reasons'],['duplicate_identity_rows','rows_with_field_errors'])
        self.assertIsNotNone(f['comparison_exclusion'])

    def test_manifest_bound_remains_atomic(self):
        a,b=pair();writes=[];acc=model.OwnershipSummary(GENERATED,1,model.DIAGNOSTIC_POLICY)
        acc.add('F0',a,b,native.compare(a,b),{}, {}, {}, {})
        with mock.patch.object(model,'SUMMARY_MANIFEST_BYTES',100):
            ref=acc.finish(lambda *args:writes.append(args))
        self.assertEqual(ref['status'],'unavailable');self.assertEqual(writes,[])


class Replay(unittest.TestCase):
    def setUp(self):
        for p in (mock.patch.dict(model.catalog.ETF_UNIVERSE,{'SPY':{'category':'broad'},'VOO':{'category':'broad'}},clear=True),
                  mock.patch.object(desk.catalog,'DESK',('SPY','VOO','BND'))):
            p.start();self.addCleanup(p.stop)

    def test_immediate_predecessor_canonical_summary_and_desk_supplement_exact_bytes(self):
        f=json.loads(gzip.decompress((ROOT/'tests/fixtures/holdings-diagnostics-predecessor-synthetic.json.gz').read_bytes()))
        db=Storage();db.objects={k:v.encode() for k,v in f['objects'].items()}
        for name,s in [('canonical',store),('desk',desk)]:
            packet=f[name];expected={k:v for k,v in packet.items() if k!='replay'}
            self.assertEqual(model.encoded(s.replay(packet['replay'],s.reader(db,'fixture'))),model.encoded(expected))
        self.assertEqual(db.writes,[])

    def test_new_policy_roundtrip_and_both_importers(self):
        import test_etf_desk_store as ds
        db,di=ds.fixture();old=json.loads(db.objects[model.CURRENT]);run=json.loads(db.objects[old['replay']['manifest_key']]);i=json.loads(db.objects[run['input']['key']])
        i.update(ownership_summary_policy=model.OWNERSHIP_POLICY,ownership_diagnostic_policy=model.DIAGNOSTIC_POLICY)
        read=store.reader(db,'fixture');out=store.compile_output(i,read,lambda k,v:store.immutable(db,'fixture',k,v));ref=store.retain(db,'fixture',i,out,read)
        self.assertEqual(store.replay(ref,store.reader(db,'fixture')),out)
        manifest=json.loads(db.objects[out['ownership_summary']['manifest']['key']]);self.assertEqual(manifest['diagnostic_policy'],model.DIAGNOSTIC_POLICY)
        diagnostic_key=manifest['qualification_diagnostics']['key'];saved=db.objects[diagnostic_key]
        db.objects[diagnostic_key]+=b' '
        with self.assertRaises(ValueError):store.replay(ref,store.reader(db,'fixture'))
        db.objects[diagnostic_key]=saved
        db.objects[model.CURRENT]=model.encoded({**out,'replay':ref})
        look={'contract':'etf-lookthrough-inputs.v1','kind':'lookthrough','generated_at':out['generated_at'],'canonical_source':store.snapshot(db,'fixture',model.CURRENT),'previous':None}
        lo=store.compile_output(look,store.reader(db,'fixture'),lambda *a:None);lr=store.retain(db,'fixture',look,lo)
        self.assertEqual(store.replay(lr,store.reader(db,'fixture')),lo)
        di['canonical_holdings']=desk.snapshot(db,'fixture',model.CURRENT,desk.reader(db,'fixture'));dp=ds.RetainedDesk().native(db,di)
        self.assertEqual(desk.replay(dp['replay'],desk.reader(db,'fixture')),{k:v for k,v in dp.items() if k!='replay'})
        for val in ('invented',None,True):
            i['ownership_diagnostic_policy']=val
            with self.assertRaisesRegex(ValueError,'diagnostic'):store.compile_output(i,store.reader(db,'fixture'),lambda *a:None)
        i.pop('ownership_summary_policy');i['ownership_diagnostic_policy']=model.DIAGNOSTIC_POLICY
        with self.assertRaisesRegex(ValueError,'diagnostic'):store.compile_output(i,store.reader(db,'fixture'),lambda *a:None)
