from pathlib import Path
import sys, unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'aws/shared'))
from calls_free_brief import build


class Tests(unittest.TestCase):
    def test_three_periods_enter_the_brief_without_an_independent_vote(self):
        packet={'contract':'liquidity-flow-research.v1','generated_at':'2026-09-25T18:00:00Z',
            'current':{'net_liquidity_b':0,'observation_dates':{'WALCL':'2026-09-23','WTREGEN':'2026-09-23','RRPONTSYD':'2026-09-24'}},
            'quality':{'status':'fresh','observation_date':'2026-09-23'}}
        out=build(lambda key:packet if key=='data/liquidity-flow.json' else {},'2026-09-25T20:00:00Z')
        row=out['evidence'][0]
        self.assertEqual(row['value'],0);self.assertFalse(row['calls_eligible'])
        self.assertIn('weekly average ending 2026-09-23',row['note']);self.assertIn('daily operation 2026-09-24',out['brief_md'])
        self.assertIn('oldest selected input date',row['note']);self.assertEqual(out['coverage']['eligible_votes'],0)
        self.assertEqual(out['call_verb'],'WAIT')

    def test_malformed_period_metadata_does_not_crash_or_leak_arbitrary_text(self):
        for dates in ([], 'PRIVATE-CANARY', {'WALCL':'PRIVATE-CANARY'}):
            packet={'contract':'liquidity-flow-research.v1','current':{'observation_dates':dates}}
            out=build(lambda _:packet,'2026-09-25T20:00:00Z')
            self.assertNotIn('PRIVATE-CANARY',out['brief_md']);self.assertIn('unavailable',out['evidence'][0]['note'])


if __name__=='__main__':unittest.main(verbosity=2)
