from pathlib import Path
from datetime import datetime,timedelta
import sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-fomc-reaction/tests')]
from fomc_fixture import packet
import fomc_research as adapter
class Boundary(unittest.TestCase):
 def test_actual_cycle_read_excludes_legacy_regime_and_surprise(self):
  from cycle_native_test_support import synthesis_with
  out=synthesis_with('data/fomc-reaction.json',{'regime_context':{'quadrant':'GOLDILOCKS'},'surprise':{'label':'DOVISH'}})
  self.assertIsNone(out['cycle']['phase']);self.assertIsNone(out['synthesis']['score']);self.assertFalse(out['calls_eligible'])
 def test_native_context_requires_unchanged_bytes_and_current_source_clocks(self):
  _,_,p=packet();at=datetime.fromisoformat(p['generated_at']);self.assertTrue(adapter.context(p,at)['available'])
  self.assertFalse(adapter.context(p,at+timedelta(hours=27))['available']);self.assertFalse(adapter.context(p,at-timedelta(seconds=1))['available'])
  p['events'][0]['two_year_daily_direction']='down';self.assertFalse(adapter.context(p,at)['available'])
 def test_other_feeds_unchanged_no_legacy_context_is_promoted(self):
  canary={'regime_context':{'quadrant':'GOLDILOCKS'}};self.assertIs(adapter.guard('data/other.json',canary),canary)
  view=adapter.guard('data/fomc-reaction.json',canary);self.assertIsNone(view['regime_context']);self.assertTrue(all(view[k] is False for k in adapter.PERMISSIONS))
if __name__=='__main__':unittest.main(verbosity=2)
