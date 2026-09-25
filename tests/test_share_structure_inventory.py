from pathlib import Path
import copy,sys,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/ops/checks'))
import share_structure_inventory as model
SOURCE=(ROOT/'aws/lambdas/justhodl-share-flows/source/lambda_function.py').read_text(encoding='utf-8')


def fixture():
    values={key:{} for key in model.INPUTS}
    values[model.CURRENT]={'generated_at':'2026-09-25T12:00:00Z','tickers':{
        'ABC':{'as_of':'2026-09-25','sh_yoy_pct':0},'BRK.B':{'as_of':'2026-09-01','sh_yoy_pct':None},
        'OLD':{'as_of':'invalid'},'FUT':{'as_of':'2026-09-26'}}}
    values['data/phase-detector.json']={'tickers':{'ABC':{},'BRK.B':{},'NEW':{}}}
    values['data/master-ranker.json']={'rows':[{'ticker':f'T{n}'} for n in range(302)]}
    values['data/insider-radar.json']={'latest_buys':[{'ticker':f'B{n}'} for n in range(82)]}
    return values


class Tests(unittest.TestCase):
    def test_real_valuation_arrays_keep_all_compact_ticker_rows_old_engine_skipped(self):
        values=fixture();values['data/stock-valuations.json']={'heatmap':{
            'sp':[{'t':'UAL','g':'Industrials','m':35284,'a':94.0},{'t':'BRK.B'}],
            'hp':[{'t':'GCT','g':'Software - Infrastructure','m':1898,'a':76.5}]}}
        result=model.inventory(values,'2026-09-25',SOURCE)
        for label in ('UAL','GCT'):self.assertIn(label,result['candidate_labels_absent_from_legacy_request'])
        self.assertEqual(result['complete_reported_label_occurrences']['UAL'],[
            {'source_key':'data/stock-valuations.json','path':['heatmap','sp',0,'t']}])
        self.assertEqual(result['current']['rows'],4)

    def test_full_sources_keep_names_outside_old_caps_and_dotted_symbols(self):
        result=model.inventory(fixture(),'2026-09-25',SOURCE)
        self.assertEqual(result['current']['rows'],4)
        for label in ('BRK.B','OLD','T301','B81'):
            self.assertIn(label,result['candidate_provider_labels'])
            self.assertIn(label,result['candidate_labels_absent_from_legacy_request'])
            self.assertTrue(result['complete_reported_label_occurrences'][label])
        self.assertEqual(result['requested_labels_missing_current_rows'][0],'B0')
        self.assertTrue(result['all_current_rows_conserved'])
        self.assertFalse(result['free_float_change_qualified'])

    def test_clock_distribution_does_not_use_new_packet_clock_for_old_rows(self):
        result=model.inventory(fixture(),'2026-09-25',SOURCE)
        self.assertEqual(result['current_row_age_days'],{'-1':1,'0':1,'24':1})
        self.assertEqual(result['current_rows_older_than_seven_days'],['BRK.B'])
        self.assertEqual(result['future_row_dates'],['FUT'])
        self.assertEqual(result['invalid_row_dates'],['OLD'])

    def test_whole_sources_and_invalid_row_shapes_are_required(self):
        values=fixture();del values[model.INPUTS[-1]]
        with self.assertRaises(ValueError):model.inventory(values,'2026-09-25',SOURCE)
        values=fixture();values[model.CURRENT]['tickers']['BAD']=None
        with self.assertRaises(ValueError):model.inventory(values,'2026-09-25',SOURCE)
        values=fixture();values['data/insider-clusters.json']=None
        result=model.inventory(values,'2026-09-25',SOURCE)
        self.assertEqual(result['missing_inputs'],['data/insider-clusters.json'])

    def test_typed_invalid_labels_and_duplicate_occurrences_are_not_erased(self):
        values=fixture();values['data/opportunities.json']={'all':[{'ticker':'ABC'},{'ticker':'ABC'},'not a ticker',{}]}
        before=copy.deepcopy(values);result=model.inventory(values,'2026-09-25',SOURCE)
        self.assertEqual(values,before)
        self.assertEqual(len(result['complete_reported_label_occurrences']['ABC']),4)
        self.assertIn('not a ticker',result['reported_labels_not_provider_request_eligible'])
        self.assertEqual(result['shape_issues'][0]['path'],['all',3,'symbol'])


if __name__=='__main__':unittest.main(verbosity=2)
