from pathlib import Path
from unittest.mock import patch
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import option_population_research as desk
import option_population_model as model
import test_option_flow_store as fixtures


class Tests(unittest.TestCase):
    def setUp(self):
        t=fixtures.StoreTests();t.setUp()
        try:self.s3,source_inputs,output,replay=t.candidate()
        finally:t.tearDown()
        self.read=model.evidence.reader(self.s3,'synthetic')
        protect=lambda doc:model.evidence.protect(self.s3,'synthetic',model.upstream.encoded(doc),self.read)
        self.inputs={'contract':'option-population-inputs.v1','compiled_at':'2026-09-21T12:01:00Z',
            'source_publication':protect({**output,'replay':replay}),
            'predecessors':{desk.CURRENT:None,desk.LEGACY:protect({'version':'1.3.0','market_composite':{},'calculation_config':{'n_underlyings':10},'underlyings':{'SPY':{'legacy':'all retained'}}}),
                desk.HISTORY:protect({'history':[{'every':'row preserved'}]})}}
        self.artifacts={}
    def build(self):
        with patch.object(model,'UNDERLYINGS',('SPY',)):
            return desk.build(self.inputs,self.read,self.artifacts.__setitem__)
    def test_source_clock_and_derived_graph_are_preserved(self):
        out=self.build()
        self.assertNotEqual(out['generated_at'],out['source_capture_completed_at'])
        self.assertEqual(out['quality']['status'],'descriptive');self.assertFalse(out['calls_eligible'])
        restored=model.restore(out['underlyings']['SPY']['population'],self.artifacts.__getitem__)
        self.assertEqual(restored['totals'],out['underlyings']['SPY']['totals'])
        self.assertEqual(restored['source_run'],out['source_run'])
        members=model.checked(out['underlyings']['SPY']['source_membership'],'groups',self.artifacts.__getitem__)
        self.assertEqual(members['source_run'],out['source_run'])
        self.assertEqual(sum(len(g['source_rows']) for g in members['groups']),2)
        row=members['groups'][0]['source_rows'][0]
        self.assertEqual((row['source_page'],row['row_index']),(1,0))
        self.assertEqual(row['eligible_fields'],['reported_open_interest','gamma_oi_shares','delta_oi_shares'])
    def test_membership_coverage_drift_is_rejected(self):
        out=self.build();restored=model.restore(out['underlyings']['SPY']['population'],self.artifacts.__getitem__)
        restored['by_expiry_strike'][0]['sides']['call']['gamma_oi_shares']['included_rows']=0
        with self.assertRaisesRegex(ValueError,'contribution coverage'):desk.memberships(restored,self.read)
    def test_rerun_does_not_refresh_old_source(self):
        self.inputs['compiled_at']='2026-09-21T16:00:00Z';out=self.build()
        self.assertEqual(out['quality']['status'],'stale_capture')
        self.assertEqual(out['source_capture_completed_at'],'2026-09-21T12:00:00+00:00')
    def test_future_source_rejected(self):
        self.inputs['compiled_at']='2026-09-21T11:59:00Z'
        with self.assertRaises(ValueError):self.build()
    def test_whole_history_is_required(self):
        self.inputs['predecessors'][desk.HISTORY]=None
        with self.assertRaises(ValueError):self.build()
    def test_source_digests_are_checked(self):
        self.s3.data[self.inputs['source_publication']['key']]+=b' '
        self.read=model.evidence.reader(self.s3,'synthetic')
        with self.assertRaises(ValueError):self.build()
    def test_compatibility_cannot_restore_legacy_trading_authority(self):
        out=self.build();out['replay']={'manifest_key':'synthetic','output_sha256':'synthetic'}
        compat=desk.compatibility(out)
        self.assertEqual(compat['underlyings'],{});self.assertEqual(compat['market_composite'],{})
        self.assertEqual(compat['portfolio_action'],'WAIT');self.assertFalse(compat['calls_eligible'])
        self.assertEqual(compat['source_capture_completed_at'],out['source_capture_completed_at'])
        self.assertEqual(compat['retained_predecessor'],self.inputs['predecessors'][desk.LEGACY])


if __name__=='__main__':unittest.main(verbosity=2)
