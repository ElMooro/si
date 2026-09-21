import importlib.util
import json
import math
import sys
import types
import unittest
from datetime import datetime,timezone
from pathlib import Path
from unittest.mock import patch
with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:None),
                            'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'test'),
                            '_fred_shim':types.SimpleNamespace()}):
    spec=importlib.util.spec_from_file_location('macro_test',Path(__file__).resolve().parents[1]/'source/legacy_macro_nowcast.py')
    e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
NOW=datetime.now(timezone.utc);CURRENT=NOW.year*12+NOW.month-1
def date(m):return f'{m//12:04d}-{m%12+1:02d}-01'
H=[(date(m),100+i/4+math.sin(i/6)) for i,m in enumerate(range(CURRENT-180,CURRENT))]


def run(missing=None,stale=None):
    writes={}
    def fred(sid):return ([] if sid==missing else H[:-12] if sid==stale else H+[(date(CURRENT),9999)]),None
    confidence={'series':{'old':{'last':-34434,'asof_week':'2023-39'}},'composite_z':-1,'composite_n':1}
    with patch.object(e,'fred_fetch',side_effect=fred),patch.object(e,'s3',types.SimpleNamespace(put_object=lambda **kw:writes.update({kw['Key']:json.loads(kw['Body'])}))), \
         patch.object(e,'optional_block',side_effect=lambda module,*a:json.loads(json.dumps(confidence)) if module=='wl_series' else {}), \
         patch.object(e,'check_regime_change',side_effect=AssertionError('alert sent')):
        assert e.lambda_handler({'suppress_alerts':True})['statusCode']==200
    return writes[e.OUTPUT_KEY]


class Integrity(unittest.TestCase):
    def test_yoy_requires_exact_calendar_year_not_twelve_rows(self):
        h=[r for r in H if e.month_id(r[0])!=CURRENT-13]
        z,raw,error=e.transform_zscore(h,'yoy_pct')
        self.assertIsNone(z);self.assertEqual(error,'missing_exact_year_ago')

    def test_duplicate_month_and_nonfinite_values_not_scored(self):
        self.assertEqual(e.transform_zscore(H+[H[-1]],'level_z')[2],'duplicate_month')
        self.assertIsNone(e.transform_zscore([(date(CURRENT-1),float('nan'))],'level_z')[0])

    def test_monthly_return_does_not_skip_missing_target_month(self):
        scores=[{'date':d,'regime':'test'} for d,_ in H]
        prices=[(d,v) for d,v in H[::2]]
        perf=e.compute_spy_returns_by_regime(scores,prices)['test']
        self.assertIsNone(perf['horizons']['1m'])
        self.assertIsNotNone(perf['horizons']['6m'])
        self.assertIn('price-only',perf['return_basis'])

    def test_current_partial_month_is_excluded_from_handler(self):
        out=run()
        self.assertEqual(out['quality']['status'],'fresh')
        self.assertEqual({c['latest_date'] for c in out['components']},{date(CURRENT-1)})
        self.assertFalse(out['return_study']['dividends_included'])
        self.assertFalse(out['execution_eligible'])

    def test_missing_or_stale_component_expires_regime(self):
        for args in ({'missing':'PAYEMS'},{'stale':'INDPRO'}):
            out=run(**args)
            self.assertIsNone(out['normalized_score'])
            self.assertEqual(out['regime'],'UNAVAILABLE')
            self.assertEqual(out['quality']['status'],'incomplete')

    def test_history_is_explicitly_revised_not_real_time(self):
        out=run()
        self.assertTrue(out['historical_scores'])
        self.assertTrue(all(r['point_in_time_validated'] is False and r['n_components']==7 for r in out['historical_scores']))

    def test_unvalidated_watchlist_confidence_cannot_vote(self):
        out=run();c=out['global_confidence']
        self.assertIsNone(c['composite_z']);self.assertEqual(c['composite_n'],0)
        self.assertEqual(c['series']['old']['quality_status'],'CHECK_DATA')
        self.assertFalse(c['series']['old']['score_eligible'])


if __name__=='__main__':unittest.main()
