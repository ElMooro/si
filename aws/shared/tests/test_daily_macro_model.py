from copy import deepcopy
from datetime import datetime, timedelta
import json
import unittest

from daily_macro_model import build, digest
from test_research_brief_model import source_packet, NOW


def auxiliary():
    return {'collected_at': NOW, 'observations': {
        'stocks': {'SPY': {'price': 100, 'history': [{'d':'2026-09-17','c':100}], 'grade':'BUY', 'score':99, 'risk_reward':99}},
        'crypto': {'BTC': {'price': 100}}, 'ecb_ciss': {'monthly': {'current': 0.1, 'month_pct':2205}},
        'news': [], 'tenor_research': {'status':'UNAVAILABLE'}}}


class DailyMacroTests(unittest.TestCase):
    def test_claim_units_calendar_comparison_and_no_legacy_authority(self):
        source=source_packet(); aux=auxiliary(); before=deepcopy((source,aux))
        report=build(source,aux,NOW)
        claims=next(row for category in report['fred'].values() for key,row in category.items() if key=='ICSA')
        self.assertEqual(claims['current'],196000)
        self.assertEqual(claims['unit'],'Number')
        self.assertEqual(claims['changes']['month']['baseline_date'],'2026-08-08')
        self.assertNotIn('196000K',report['ai_analysis']['summary'])
        for key in ('khalid_index','ka_index'):
            self.assertIsNone(report[key]['score']); self.assertIsNone(report[key]['regime'])
        self.assertEqual(report['ai_analysis']['portfolio']['construction'],{})
        self.assertEqual(report['decision']['meaning'],'abstain')
        self.assertEqual((source,aux),before)
        self.assertEqual(digest(report),digest(build(source,aux,NOW)))

    def test_current_macro_expires_without_restamping_old_observations(self):
        source=source_packet(); later=(datetime.fromisoformat(NOW)+timedelta(days=2)).isoformat()
        report=build(source,auxiliary(),later)
        claims=next(row for category in report['fred'].values() for key,row in category.items() if key=='ICSA')
        self.assertIsNone(claims['current']); self.assertIsNone(claims['month_pct'])
        self.assertEqual(claims['last_observed_value'],196000)
        self.assertEqual(claims['acquired_at'],source['measurements']['ICSA']['acquired_at'])
        self.assertIsNone(report['net_liquidity']['net'])
        self.assertIsNone(report['market_intelligence']['risk_score'])

    def test_stock_inputs_retained_without_grade_or_all_time_claims(self):
        report=build(source_packet(),auxiliary(),NOW)
        self.assertEqual(report['stocks']['SPY']['price'],100)
        self.assertEqual(report['stocks']['SPY']['history'],auxiliary()['observations']['stocks']['SPY']['history'])
        for key in ('grade','score','risk_reward'): self.assertIsNone(report['stocks']['SPY'][key])
        self.assertFalse(report['stocks']['SPY']['quality']['original_source_verified'])
        self.assertIsNone(report['ath_breakouts']['total_at_ath'])
        self.assertIsNone(report['market_flow']['total_buying'])
        self.assertIsNone(report['ecb_ciss']['monthly']['month_pct'])
        self.assertEqual(report['signals']['buys'],[])
        self.assertFalse(report['calls_eligible'])

    def test_future_or_legacy_source_is_not_promoted(self):
        with self.assertRaises(ValueError): build({'version':'V10'},auxiliary(),NOW)
        aux=auxiliary();aux['collected_at']='2099-01-01T00:00:00Z'
        with self.assertRaises(ValueError): build(source_packet(),aux,NOW)


if __name__=='__main__': unittest.main()
