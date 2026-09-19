import copy
import importlib.util
import sys
import types
import unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path
from unittest.mock import patch
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'shared'))
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/'shared/tests'))
from pd_fails_context_tests import run as pd_context_checks
pd_context_checks()
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None)}):
    spec=importlib.util.spec_from_file_location('hot_test',Path(__file__).resolve().parents[1]/'source/lambda_function.py')
    e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
TODAY=datetime.now(timezone.utc);DAY=TODAY.strftime('%Y%m%d')


class Integrity(unittest.TestCase):
    def test_publication_timestamp_has_timezone_and_preserves_observation_date(self):
        writes={}
        observation=(TODAY-timedelta(days=2)).date().isoformat()
        tw={'status':'LIVE','quality':{'status':'fresh','observation_date':observation}}
        with patch.object(e,'taiwan',return_value=tw),patch.object(e,'_put',side_effect=lambda key,value:writes.update({key:value})):
            e._legacy_unvalidated_handler({},None)
        doc=writes[e.OUT_KEY]
        stamp=datetime.fromisoformat(doc['quality']['publication_date'])
        self.assertIsNotNone(stamp.tzinfo)
        self.assertEqual(doc['quality']['publication_date'],doc['generated_at'])
        self.assertEqual(doc['quality']['observation_date'],observation)
        self.assertEqual(doc['pd_settlement_fails']['scope_id'],'treasury_incl_tips')
        self.assertIsNone(doc['pd_settlement_fails']['combined_bn'])
        self.assertEqual(doc['units'],'TWD_bn')

    def test_fresh_zero_is_a_real_observation(self):
        row=e.board_metrics({DAY:0})
        self.assertEqual(row['status'],'LIVE');self.assertEqual(row['latest_bn'],0)
        self.assertIsNone(row['sum_5obs_bn'])

    def test_stale_same_day_boards_do_not_make_live_combined(self):
        old=(TODAY-timedelta(days=8)).strftime('%Y%m%d')
        ledger={'rows':{old:1e9}}
        with patch.object(e,'_g',side_effect=lambda key:copy.deepcopy(ledger)),patch.object(e,'twse_fetch',return_value=(None,'offline')),patch.object(e,'tpex_fetch',return_value=(None,'offline')):
            result=e.taiwan({})
        self.assertEqual(result['status'],'STALE');self.assertIsNone(result['latest_bn'])
        self.assertIsNone(result['combined']['latest_bn'])

    def test_corrections_to_existing_date_are_persisted(self):
        writes={};ledger={'rows':{DAY:1}}
        with patch.object(e,'_g',side_effect=lambda key:copy.deepcopy(ledger)),patch.object(e,'twse_fetch',return_value=(DAY,20)),patch.object(e,'tpex_fetch',return_value=(DAY,30)),patch.object(e,'_put',side_effect=lambda key,value:writes.update({key:value})):
            e.taiwan({})
        self.assertEqual(writes[e.TWSE_LEDGER]['rows'][DAY],20)
        self.assertEqual(writes[e.TPEX_LEDGER]['rows'][DAY],30)

    def test_observation_windows_are_not_day_windows(self):
        rows={(TODAY-timedelta(days=i*2)).strftime('%Y%m%d'):1e9 for i in range(5)}
        result=e.board_metrics(rows)
        self.assertEqual(result['sum_5obs_bn'],5)
        self.assertIsNone(result['sum_5d_bn'])
        self.assertEqual(result['windows']['5']['calendar_span_days'],8)

    def test_short_history_cannot_claim_sixty_observation_zscore(self):
        rows={(TODAY-timedelta(days=i)).strftime('%Y%m%d'):i*1e9 for i in range(24)}
        self.assertIsNone(e.board_metrics(rows)['z_60_observations'])
        rows.update({(TODAY-timedelta(days=i)).strftime('%Y%m%d'):i*1e9 for i in range(24,61)})
        self.assertIsNotNone(e.board_metrics(rows)['z_60_observations'])

    def test_invalid_future_or_nonfinite_rows_cannot_vote(self):
        future=(TODAY+timedelta(days=1)).strftime('%Y%m%d')
        row=e.board_metrics({DAY:1,future:2,'20250101':float('nan')})
        self.assertEqual(row['quality']['invalid_rows'],2)
        self.assertIsNone(row['latest_bn'])

    def test_foreign_categories_are_disjoint_and_reconciled(self):
        rows=[['Foreign Investors (Foreign Dealers excluded)','12','3','9'],['Foreign Dealers','3','2','1'],['Total Foreign Investors','15','5','10']]
        self.assertEqual(e.foreign_net(rows),10)
        rows[0][3]='100'
        with self.assertRaises(ValueError):e.foreign_net(rows)

    def test_ledger_read_failure_cannot_replace_history_with_empty(self):
        with patch.object(e,'s3',types.SimpleNamespace(get_object=lambda **kw:(_ for _ in ()).throw(RuntimeError('read denied')))):
            with self.assertRaises(RuntimeError):e._g(e.TWSE_LEDGER)


if __name__=='__main__':
    suite=unittest.defaultTestLoader.loadTestsFromTestCase(Integrity)
    suite.addTests(unittest.defaultTestLoader.discover(str(Path(__file__).parent),pattern='test_research.py'))
    sys.exit(0 if unittest.TextTestRunner(verbosity=2).run(suite).wasSuccessful() else 1)
