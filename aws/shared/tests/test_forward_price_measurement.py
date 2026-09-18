from copy import deepcopy
from datetime import datetime, timezone, timedelta
import json
from pathlib import Path
import sys
import unittest
sys.path[:0]=[str(Path(__file__).resolve().parents[1]),str(Path(__file__).resolve().parent)]
from test_prospective_journal import recorded, NOW
from prospective_journal import read_record
from evidence_store import capture
from forward_price_measurement import evaluate
from outcome_price_evidence import PriceEvidenceVerifier, EASTERN


def fixture():
    store,_,refs=recorded(); record=read_record(store,'fixture',refs[0])
    # Friday registration: next six observed SPY sessions are Monday..Monday.
    days=['2026-09-21','2026-09-22','2026-09-23','2026-09-24','2026-09-25','2026-09-28']
    store.now=datetime(2026,9,29,10,tzinfo=timezone.utc)
    def packet(symbol,values,selected=days,start='2026-09-19'):
        raw=json.dumps({'ticker':symbol,'adjusted':True,'status':'OK','results':[
            {'t':int(datetime.fromisoformat(day).replace(tzinfo=EASTERN).timestamp()*1000),'c':v}
            for day,v in zip(selected,values)]}).encode()
        receipt=capture(store,'fixture','polygon','https://api.polygon.io/v2/aggs/ticker/'+symbol+'/range/1/day/'+start+'/2026-09-28?adjusted=true',raw,store.now)
        return {'raw':raw,'evidence':receipt}
    return store,record,packet,days


class Forward(unittest.TestCase):
    def test_known_price_moves_replay_against_actual_retained_fixture_bytes(self):
        store,record,packet,_=fixture()
        result=evaluate(record,5,packet('AAA',[100,101,102,103,104,110]),packet('SPY',[100,101,102,103,104,105]),store.now,PriceEvidenceVerifier(store,'fixture'))
        self.assertEqual(result['status'],'MEASURED_PRICE_ONLY')
        self.assertEqual(result['entry_session'],'2026-09-21');self.assertEqual(result['exit_session'],'2026-09-28')
        self.assertAlmostEqual(result['asset_price_return_pct'],10);self.assertAlmostEqual(result['asset_minus_benchmark_pct'],5)
        self.assertIsNone(result['portfolio_pnl']);self.assertIsNone(result['net_return_pct'])
        self.assertFalse(result['sizing_eligible'])

    def test_unelapsed_window_never_becomes_a_zero_return_or_failure(self):
        store,record,packet,_=fixture()
        result=evaluate(record,20,packet('AAA',[100]*6),packet('SPY',[100]*6),store.now,PriceEvidenceVerifier(store,'fixture'))
        self.assertEqual(result['status'],'PENDING_FORWARD_WINDOW')
        self.assertNotIn('asset_price_return_pct',result)

    def test_missing_asset_entry_never_slides_to_next_bar(self):
        store,record,packet,days=fixture()
        result=evaluate(record,5,packet('AAA',[100]*5,days[1:]),packet('SPY',[100]*6),store.now,PriceEvidenceVerifier(store,'fixture'))
        self.assertEqual(result['status'],'MISSING_MATCHING_ENDPOINT');self.assertEqual(result['entry_session'],days[0])

    def test_truncated_benchmark_cannot_choose_a_later_entry(self):
        store,record,packet,_=fixture()
        with self.assertRaises(ValueError):evaluate(record,5,packet('AAA',[100]*6),packet('SPY',[100]*6,start='2026-09-21'),store.now,PriceEvidenceVerifier(store,'fixture'))

    def test_unverified_or_corrupt_archive_cannot_supply_measurement(self):
        store,record,packet,_=fixture();asset=packet('AAA',[100]*6);benchmark=packet('SPY',[100]*6)
        with self.assertRaises(ValueError):evaluate(record,5,asset,benchmark,store.now,None)
        store.rows[asset['evidence']['key']]['Body']=b'bad'
        with self.assertRaises(ValueError):evaluate(record,5,asset,benchmark,store.now,PriceEvidenceVerifier(store,'fixture'))

    def test_current_daily_period_and_unregistered_horizons_are_not_used(self):
        store,record,packet,_=fixture()
        early=datetime(2026,9,28,20,tzinfo=timezone.utc)
        store.now=early;asset=packet('AAA',[100]*6);benchmark=packet('SPY',[100]*6)
        self.assertEqual(evaluate(record,5,asset,benchmark,early,PriceEvidenceVerifier(store,'fixture'))['status'],'PENDING_FORWARD_WINDOW')
        with self.assertRaises(ValueError):evaluate(record,1,asset,benchmark,store.now,PriceEvidenceVerifier(store,'fixture'))

    def test_later_capture_cannot_be_used_in_a_backdated_evaluation(self):
        store,record,packet,_=fixture()
        with self.assertRaises(ValueError):evaluate(record,5,packet('AAA',[100]*6),packet('SPY',[100]*6),store.now-timedelta(hours=1),PriceEvidenceVerifier(store,'fixture'))


if __name__=='__main__':unittest.main()
