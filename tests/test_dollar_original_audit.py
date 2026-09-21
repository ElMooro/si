from pathlib import Path
from copy import deepcopy
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops/checks'),str(ROOT/'aws/ops/staged'),str(ROOT/'tests')]
from dollar_fixture import fixture,STAMP
from dollar_original_audit import independent
import dollar_research_model as model,dollar_research_catalog as catalog
import ops_6022_dollar_original_candidate as candidate


class Tests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.packet,cls.originals,contexts,predecessors,cls.objects=fixture()
        cls.output=model.build(cls.packet,cls.originals,STAMP,contexts,predecessors)
    def test_every_rendered_series_and_month_reconciles(self):
        counts=independent(self.output,self.originals,self.packet,catalog)
        self.assertEqual(counts['original_series'],32);self.assertEqual(counts['calendar_comparisons'],128)
        self.assertEqual(counts['reciprocal_comparisons'],56);self.assertGreater(counts['rendered_points'],7000)
        self.assertEqual(counts['matched_months'],36)
    def test_changed_history_baseline_reciprocal_mean_curve_liquidity_and_authority_refused(self):
        paths=[('series','DEXUSEU','history',0,'exact_value'),('series','DEXUSEU','comparisons','month','baseline','original_row_index'),
            ('series','DEXUSEU','comparisons','month','dollar_strength','relative_percent','exact_value'),
            ('derived','us_germany_monthly','trail',-1,'us_daily_mean','exact_value'),('derived','treasury_curve','difference_bps','exact_value'),
            ('derived','net_liquidity','net_decimal'),('series','DEXUSEU','quote','numerator'),('calls_eligible',)]
        for path in paths:
            with self.subTest(path=path):
                output=deepcopy(self.output);parent=output
                for key in path[:-1]:parent=parent[key]
                parent[path[-1]]='incorrect'
                with self.assertRaises(AssertionError):independent(output,self.originals,self.packet,catalog)
    def test_stale_source_independent_check_preserves_history_and_rejects_current_liquidity(self):
        _,_,contexts,predecessors,_=fixture()
        output=model.build(self.packet,self.originals,'2026-09-24T00:00:00+00:00',contexts,predecessors)
        self.assertEqual(independent(output,self.originals,self.packet,catalog)['original_series'],32)
        self.assertIsNone(output['derived']['net_liquidity']['net'])
    def test_retained_reader_cannot_substitute_an_unqualified_source(self):
        raw=b'original';digest=model.sha(raw);key='data/report-research/compilers/'+digest+'.py'
        ref={'key':model.PRIVATE+digest+'.bin','sha256':digest,'bytes':len(raw)}
        def read(path):self.assertEqual(path,ref['key']);return raw
        read.remember=lambda *_:None
        reader=candidate.retained_reader(read,{'canonical_originals':{key:ref}})
        self.assertEqual(reader(key),raw)
        with self.assertRaises(ValueError):reader('data/report-research/compilers/'+'0'*64+'.py')
    def test_recorded_inputs_keep_whole_original_contexts_and_prior_clocks(self):
        captures={key:{'source_key':key,'status':'retained','acquired_at':STAMP,'original':{'whole':key}} for key in candidate.store.SOURCES}
        before={'contract':'dollar-source-preflight.v1','captures':captures}
        source={'contract':'dollar-canonical-source-qualification.v1','baseline':candidate.BASELINE,
            'source_commit':'5b74be08d6b5f9e05e9385b0a8b50572f77f6bd6','generated_at':STAMP,'canonical_packet':{'whole':'new canonical'}}
        inputs=candidate.recorded_inputs(before,source,STAMP)
        for key in candidate.store.SOURCES[1:]:self.assertEqual(inputs['captures'][key],captures[key])
        self.assertEqual(inputs['captures']['data/report-measurements.json']['original'],source['canonical_packet'])
        self.assertEqual(captures['data/report-measurements.json']['original'],{'whole':'data/report-measurements.json'})
        with self.assertRaises(AssertionError):candidate.recorded_inputs(before,source,'2026-09-20T00:00:00Z')


if __name__=='__main__':unittest.main(verbosity=2)
