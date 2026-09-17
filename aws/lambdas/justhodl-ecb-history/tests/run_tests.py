import importlib.util
import sys
import types
import unittest
from datetime import date
from pathlib import Path
from unittest.mock import patch
SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path.insert(0,str(SOURCE))
from ecb_measurements import parse_csv,period_end,quality,summarize,yoy_series
HEADER='TIME_PERIOD,OBS_VALUE,FREQ,UNIT,UNIT_MULT,OBS_STATUS,TITLE\n'


class Measurements(unittest.TestCase):
    def test_millions_to_billions_and_quoted_csv(self):
        pts,m=parse_csv(HEADER+'2026-W37,26610,W,EUR,6,A,"Lending, euros"\n','ILM/W.test')
        self.assertAlmostEqual(pts[0][1],26.610)
        self.assertEqual(pts[0][0],'2026-09-11')
        self.assertEqual(m['unit'],'EUR_bn')
        self.assertEqual(m['source_unit_multipliers'],[6])

    def test_changes_in_source_scale_are_normalized_per_row(self):
        pts,m=parse_csv(HEADER+'2026-W36,26610,W,EUR,6,A,test\n2026-W37,26.61,W,EUR,9,A,test\n','ILM/W.test')
        self.assertAlmostEqual(pts[0][1],pts[1][1])
        self.assertEqual(m['source_unit_multipliers'],[6,9])

    def test_no_guess_for_absent_or_wrong_monetary_unit(self):
        for unit,mult in [('EUR',''),('USD','6'),('','6')]:
            with self.assertRaises(ValueError):parse_csv(HEADER+f'2026-W37,26610,W,{unit},{mult},A,test\n','ILM/W.test')

    def test_ciss_small_values_are_not_currency_scaled(self):
        pts,m=parse_csv(HEADER+'2026-09-15,0.033893,D,PURE_NUMB,0,A,test\n','CISS/D.test')
        self.assertAlmostEqual(pts[0][1],0.033893)
        self.assertEqual(m['unit'],'PURE_NUMB')

    def test_conflicting_duplicates_and_nonfinite_rejected(self):
        for values in ['1','nan']:
            with self.assertRaises(ValueError):parse_csv(HEADER+'2026-W37,2,W,EUR,6,A,test\n'+f'2026-W37,{values},W,EUR,6,A,test\n','ILM/W.test')

    def test_period_end_handles_quarter_month_and_leap_year(self):
        self.assertEqual(period_end('2024-02'),date(2024,2,29))
        self.assertEqual(period_end('2026-Q2'),date(2026,6,30))
        self.assertEqual(quality('2026-Q2','quarterly',date(2026,9,17))['status'],'fresh')
        self.assertEqual(quality('2027','annual',date(2026,9,17))['status'],'invalid')

    def test_old_weekly_history_not_presented_as_current(self):
        doc=summarize('x','old',[['2020-01-03',10]],{'frequency':'weekly','unit':'EUR_bn'},'ILM/W.test',date(2026,9,17))
        self.assertIsNone(doc['latest']);self.assertIsNone(doc['z_score'])
        self.assertEqual(doc['last_observed_value'],10)
        self.assertEqual(doc['quality']['status'],'stale')
        self.assertFalse(doc['discontinued'])

    def test_yoy_requires_calendar_match(self):
        self.assertEqual(yoy_series([['2024-02',100],['2025-03',110]]),[])
        self.assertEqual(yoy_series([['2024-02',100],['2025-02',110]]),[['2025-02',10.0]])

    def test_handler_keeps_failed_and_legacy_series_unavailable(self):
        import json
        writes={}
        legacy={'id':'old_assets','label':'Legacy assets','latest':19.3,'points':[['2026-09-11',19.3]]}
        fake=types.SimpleNamespace(
            put_object=lambda **kw:writes.update({kw['Key']:json.loads(kw['Body'])}),
            get_paginator=lambda *a:types.SimpleNamespace(paginate=lambda **kw:[{'Contents':[{'Key':'data/ecb-hist/old_assets.json'}]}]),
            get_object=lambda **kw:{'Body':types.SimpleNamespace(read=lambda:json.dumps(legacy).encode())})
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**kw:fake)}):
            spec=importlib.util.spec_from_file_location('ecb_handler_test',SOURCE/'lambda_function.py')
            e=importlib.util.module_from_spec(spec);spec.loader.exec_module(e)
        with patch.object(e,'SERIES',[('ILM/W.test','failed','Failed')]),patch.object(e,'EUROSTAT_CONF',[]),patch.object(e,'fetch_csv',return_value=None):
            e.lambda_handler()
        manifest=writes['data/ecb-hist/_manifest.json'];rows={r['id']:r for r in manifest['series']}
        self.assertIsNone(rows['failed']['latest'])
        self.assertIsNone(rows['old_assets']['latest'])
        self.assertEqual(rows['old_assets']['quality']['status'],'unvalidated')
        self.assertEqual(manifest['quality']['status'],'partial')


if __name__=='__main__':unittest.main()
