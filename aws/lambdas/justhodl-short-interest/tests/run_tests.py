"""D27/D28 producer provenance regressions through the actual publication handler."""
import contextlib
import importlib.util
import io
import json
import sys
import types
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import patch

HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE.parents[2]/'shared'))
NOW=datetime.now(timezone.utc)
SETTLEMENT=(NOW-timedelta(days=7)).date().isoformat()
VOLUME_DATE=(NOW-timedelta(days=1)).date().isoformat()


def load_source(path, name):
    spec=importlib.util.spec_from_file_location(name,path)
    mod=importlib.util.module_from_spec(spec);spec.loader.exec_module(mod)
    return mod


class MemoryS3:
    def __init__(self):self.writes={}
    def put_object(self,Bucket,Key,Body,**kwargs):
        def invalid(value):raise AssertionError('Non-finite public JSON: '+value)
        self.writes[Key]=json.loads(Body,parse_constant=invalid)


class ProducerProvenanceTests(unittest.TestCase):
    def run_handler(self, overlay=True, dated_inventory=True):
        fake=types.ModuleType('boto3');fake.client=lambda *a,**kw:None
        secret=types.ModuleType('managed_secret');secret.managed_secret=lambda *a,**kw:''
        finviz=types.ModuleType('finviz')
        finviz.load_short=lambda:{'GME':{'short_float_pct':60,'short_ratio':99,'float_shares':100},
                                 'AMC':{'short_float_pct':50,'short_ratio':9},
                                 'NEW':{'short_float_pct':80,'short_ratio':10}} if overlay else {}
        with patch.dict(sys.modules,{'boto3':fake,'managed_secret':secret,'finviz':finviz}):
            mod=load_source(HERE.parent/'source/lambda_function.py','short_interest_under_test')
            s3=MemoryS3()
            history={'GME':[{'date':VOLUME_DATE,'short_pct':42,'short_vol':420,'total_vol':1000}],
                     'AMC':[{'date':VOLUME_DATE,'short_pct':20,'short_vol':200,'total_vol':1000}]}
            inventory={'GME':{'short_interest':20_000_000,'days_to_cover':8,'si_change_pct':30,'settlement_date':SETTLEMENT},
                       'AMC':{'short_interest':10_000_000,'days_to_cover':None,'si_change_pct':0,'settlement_date':SETTLEMENT}} if dated_inventory else {}
            with patch.object(mod,'S3',s3),patch.object(mod,'WATCHLIST',['GME','AMC']), \
                 patch.object(mod,'WATCHLIST_SET',{'GME','AMC'}), \
                 patch.object(mod,'collect_short_volume_history',return_value=history), \
                 patch.object(mod,'fetch_finra_short_interest',return_value=inventory), \
                 patch.object(mod,'fetch_price_over_window',return_value={}), \
                 patch.object(mod.urllib.request,'urlopen',side_effect=AssertionError('network forbidden')), \
                 contextlib.redirect_stdout(io.StringIO()):
                response=mod.lambda_handler({},None)
        self.assertEqual(response['statusCode'],200)
        return s3.writes['data/short-interest.json']

    def test_finviz_overlay_preserves_dated_finra_inventory_and_daily_volume(self):
        before=self.run_handler(overlay=False);after=self.run_handler();row=after['by_ticker']['GME']
        self.assertEqual(row['days_to_cover'],8)
        self.assertEqual(row['days_to_cover_source'],'FINRA consolidated short interest')
        self.assertEqual(row['days_to_cover_as_of'],SETTLEMENT)
        self.assertEqual(row['short_interest_as_of'],SETTLEMENT)
        self.assertEqual(row['daily_short_volume_pct'],42)
        self.assertEqual(row['latest_short_pct'],42)
        self.assertEqual(row['daily_short_volume_as_of'],VOLUME_DATE)
        self.assertEqual(row['short_float_pct'],60)
        self.assertIsNone(row['short_float_as_of'])
        self.assertEqual(row['score'],before['by_ticker']['GME']['score'])
        self.assertEqual(after['measurement_contract'],'short-positioning.v2')

    def test_finviz_dtc_fallback_is_never_given_finra_settlement_date(self):
        row=self.run_handler()['by_ticker']['AMC']
        self.assertEqual(row['days_to_cover'],9)
        self.assertEqual(row['days_to_cover_source'],'Finviz short ratio')
        self.assertIsNone(row['days_to_cover_as_of'])
        self.assertEqual(row['short_interest_as_of'],SETTLEMENT)
        self.assertIsNotNone(row['finviz_retrieved_at'])

    def test_finviz_only_row_never_manufactures_volume_or_position_inventory(self):
        row=self.run_handler()['by_ticker']['NEW']
        for key in ('latest_short_pct','daily_short_volume_pct','daily_short_volume_as_of',
                    'short_interest','short_interest_as_of','days_to_cover_as_of','settlement_date'):
            self.assertIsNone(row[key],key)
        self.assertEqual(row['short_float_pct'],80)

    def test_actual_producer_payload_drives_both_actual_receiving_handlers(self):
        donor=self.run_handler()
        squeeze_tests=load_source(HERE.parents[1]/'justhodl-squeeze-fuel/tests/run_tests.py','squeeze_donor_contract_tests')
        tickets_tests=load_source(HERE.parents[1]/'justhodl-trade-tickets/tests/run_tests.py','ticket_donor_contract_tests')
        squeeze=squeeze_tests.DonorHandlerTests().run_handler(donor)
        self.assertTrue(squeeze['board'][0]['short_positioning']['applied'])
        self.assertEqual(squeeze['board'][0]['short_interest'],20_000_000)
        _,ticket=tickets_tests.DonorHandlerTests().run_handler(donor,tickets_tests.options())
        self.assertTrue(ticket['short_positioning']['positioning_usable'])
        self.assertEqual(ticket['short_positioning']['covering_risk'],'ELEVATED')
        # A current Finviz retrieval with no dated inventory must not activate SI.
        undated=self.run_handler(dated_inventory=False)
        _,ticket=tickets_tests.DonorHandlerTests().run_handler(undated,tickets_tests.options())
        self.assertFalse(ticket['short_positioning']['positioning_usable'])


if __name__=='__main__':unittest.main()
