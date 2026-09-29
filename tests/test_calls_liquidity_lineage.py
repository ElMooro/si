"""Complete retained FRED corpus through the unpublished Calls lineage candidate."""
from datetime import timedelta
from fractions import Fraction
from pathlib import Path
import hashlib,sys,unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/ops/checks'),str(ROOT/'aws/lambdas/justhodl-liquidity-flow/tests')]
import calls_liquidity_lineage as model
import test_liquidity_native as native


def packet_at(at=None):
    objects,inputs,_=native.setup();client=native.Storage(objects)
    if at:inputs['generated_at']=at
    output=native.store.compile_output(inputs,native.store.reader(client,'fixture'))
    ref=native.store.retain(client,'fixture',inputs,output)
    return model.encoded({**output,'replay':ref}),dict(client.objects)


class CallsLiquidityLineage(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.raw,cls.objects=packet_at()
        cls.at=native.STAMP
        cls.result=model.inspect(cls.raw,native.store.reader(native.Storage(cls.objects),'fixture'),cls.at)

    def test_all_originals_calendar_dates_and_comparison_windows_are_replayed(self):
        result=self.result
        self.assertEqual(result['packet_sha256'],hashlib.sha256(self.raw).hexdigest())
        self.assertEqual(result['coverage']['calendar_dates'],180)
        self.assertEqual(set(result['comparisons']),{'1d','1w','1m','3m'})
        self.assertEqual(result['coverage']['original_rows'],result['independent_arithmetic']['original_rows_checked'])
        self.assertGreater(result['coverage']['original_rows'],7000)
        self.assertEqual(result['independent_arithmetic']['exact_rational_comparisons'],776)
        self.assertEqual(set(result['sources']),set(model.SID_ORDER))
        self.assertTrue(result['current_use']['eligible'])
        self.assertEqual(result['reported_headline'],result['current_research_value'])
        self.assertTrue(all(result[key]is False for key in ('calls_eligible','sizing_eligible','execution_eligible','publication_eligible')))
        self.assertIsNone(result['independent_evidence_count'])

    def test_every_selected_observation_is_exactly_indexed_into_its_retained_response(self):
        read=native.store.reader(native.Storage(self.objects),'fixture')
        originals={sid:native.store.strict(read(source['originals']['observations']['key']))['observations'] for sid,source in self.result['sources'].items()}
        for identity,row in self.result['observations'].items():
            self.assertEqual(identity,'fred-observation-'+model.digest(row))
            original=originals[row['series_id']][row['original_row']]
            self.assertEqual(row['observation_date'],original['date'])
            self.assertEqual(row['reported_native_value'],original['value'])
        for sample in [self.result['latest_reconstructed'],*self.result['calendar_history_180d']]:
            total=Fraction(0);complete=True
            for sid,part in sample['components'].items():
                row=self.result['observations'].get(part['observation_id'])
                if part['normalized_value']['exact_decimal'] is None:complete=False;continue
                expected=Fraction(row['reported_native_value'])/(1 if sid=='RRPONTSYD' else 1000)
                self.assertEqual(Fraction(part['normalized_value']['exact_decimal']),expected)
                self.assertEqual(part['coefficient'],1 if sid=='WALCL' else -1)
                total+=expected*part['coefficient']
            self.assertEqual(Fraction(sample['net']['exact_decimal']) if sample['net']['exact_decimal'] is not None else None,total if complete else None)

    def test_signed_changes_reconcile_without_calling_tga_declines_new_money(self):
        for row in self.result['comparisons'].values():
            contributions=[]
            for sid,part in row['components'].items():
                change=part['reported_level_change']['exact_decimal'];signed=part['signed_formula_contribution']['exact_decimal']
                self.assertEqual(Fraction(signed) if signed is not None else None,
                    Fraction(change)*(1 if sid=='WALCL' else -1) if change is not None else None)
                contributions.append(Fraction(signed) if signed is not None else None)
            self.assertEqual(Fraction(row['change']['exact_decimal']) if row['change']['exact_decimal'] is not None else None,
                sum(contributions) if all(v is not None for v in contributions) else None)
            self.assertFalse(row['causal_flow_estimate']);self.assertFalse(row['historical_information_availability_verified'])

    def test_carried_observations_share_identity_and_h41_inputs_share_one_family(self):
        references=[part['observation_id'] for row in self.result['calendar_history_180d'] for part in row['components'].values() if part['observation_id']]
        self.assertLess(len(set(references)),len(references))
        family=next(row for row in self.result['source_families'] if row['family']=='fed_h41')
        self.assertEqual(family['series'],['WALCL','WTREGEN'])
        self.assertEqual(self.result['sources']['WTREGEN']['frequency'],'W')
        self.assertEqual(self.result['sources']['RRPONTSYD']['frequency'],'D')

    def test_expired_current_context_keeps_complete_historical_traces(self):
        later=(model.store.model.clock(self.at)+timedelta(hours=27)).isoformat()
        result=model.inspect(self.raw,native.store.reader(native.Storage(self.objects),'fixture'),later)
        self.assertIsNone(result['current_research_value']);self.assertFalse(result['current_use']['eligible'])
        for field in ('observations','latest_reconstructed','calendar_history_180d','comparisons'):
            self.assertEqual(result[field],self.result[field])
        self.assertIn('WALCL:acquisition_age',result['current_use']['issues'])
        self.assertIn('native_packet:publication_age',result['current_use']['issues'])

    def test_newer_publication_cannot_renew_original_acquisition_clocks(self):
        raw,objects=packet_at((model.store.model.clock(self.at)+timedelta(hours=1)).isoformat())
        result=model.inspect(raw,native.store.reader(native.Storage(objects),'fixture'),
                             (model.store.model.clock(self.at)+timedelta(hours=26,seconds=1)).isoformat())
        self.assertNotIn('native_packet:publication_age',result['current_use']['issues'])
        self.assertIn('WALCL:acquisition_age',result['current_use']['issues'])
        self.assertIsNone(result['current_research_value'])

    def test_complete_packet_and_archive_tampering_fail_without_live_fallback(self):
        for field in ('current','authority','extra'):
            packet=model.store.strict(self.raw)
            if field=='current':packet['current']['net_liquidity_b']=123
            elif field=='authority':packet['calls_eligible']=True
            else:packet['PRIVATE-CANARY']='PRIVATE-CANARY'
            with self.assertRaises(ValueError):model.inspect(model.encoded(packet),native.store.reader(native.Storage(self.objects),'fixture'),self.at)
        for key in (self.result['source_replay']['manifest_key'],self.result['sources']['WALCL']['originals']['observations']['key']):
            base=native.store.reader(native.Storage(self.objects),'fixture')
            with self.assertRaisesRegex(ValueError,'hash differs'):
                model.inspect(self.raw,lambda path:b'{}' if path==key else base(path),self.at)
        future=(model.store.model.clock(self.at)-timedelta(seconds=1)).isoformat()
        with self.assertRaises(ValueError):model.inspect(self.raw,native.store.reader(native.Storage(self.objects),'fixture'),future)

    def test_only_immutable_paths_are_read_and_each_whole_artifact_is_read_once(self):
        calls=[];base=native.store.reader(native.Storage(self.objects),'fixture')
        def read(key):calls.append(key);return base(key)
        result=model.inspect(self.raw,read,self.at)
        self.assertEqual(len(calls),len(set(calls)))
        self.assertEqual(len(calls),result['coverage']['retained_artifacts'])
        self.assertEqual(result['coverage']['retained_uncompressed_bytes'],sum(row['bytes'] for row in result['retained_artifact_inventory']))
        guard=model.ImmutableReader(read);count=len(calls)
        for key in (model.store.SOURCE,model.store.SETTLEMENT,model.store.model.CURRENT,
                    'data/prospective-outcomes.json','private/account.json','audit-private/anything',
                    'data/evidence/fred/../anything',None):
            with self.assertRaises(ValueError):guard(key)
        self.assertEqual(len(calls),count)
        with patch.object(model,'MAX_TOTAL',1),self.assertRaisesRegex(ValueError,'byte bound'):
            model.inspect(self.raw,read,self.at)


if __name__=='__main__':unittest.main(verbosity=2)
