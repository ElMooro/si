"""Exercise deployed FedWatch assignments without running account consumers."""
from pathlib import Path
from datetime import datetime,timedelta
import sys,textwrap,unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/lambdas/justhodl-fedwatch-rate-probability/tests')]
from fedwatch_fixture import packet
import fedwatch_research as adapter
CANARY={'next_6mo_summary':{'scenario':'AGGRESSIVE_HIKING','cumulative_implied_move_bps':200},
 'meetings_ahead':[{'probabilities_pct':{'hike_50':100}}],'calls_eligible':True}
class Boundaries(unittest.TestCase):
 def test_actual_consumer_assignments_drop_unqualified_probabilities(self):
  for fn,name in [('justhodl-cycle-clock','fedwatch'),('justhodl-katlin','F')]:
   with self.subTest(fn=fn):
    source=(ROOT/'aws/lambdas'/fn/'source/lambda_function.py').read_text(encoding='utf-8')
    lines=[line for line in source.splitlines() if '"fedwatch_research"' in line]
    self.assertEqual(len(lines),1)
    ns={'load':lambda *a:CANARY,'s3_json':lambda *a:CANARY,'gj':lambda *a:CANARY,'F':{}}
    exec(textwrap.dedent(lines[0]),ns);view=ns[name]['fedwatch'] if name=='F' else ns[name]
    self.assertIsNone(view['next_6mo_summary']['scenario']);self.assertEqual(view['meetings_ahead'],[])
    self.assertIsNone(view['current_fed_funds_range']['midpoint']);self.assertFalse(view['calls_eligible'])
 def test_native_fomc_rejects_unretained_fedwatch_forecasts(self):
  sys.path.insert(0,str(ROOT/'aws/lambdas/justhodl-fomc-reaction/source'))
  from fomc_research_store import calendar_source
  with self.assertRaises(ValueError):calendar_source(CANARY,lambda key:(_ for _ in ()).throw(AssertionError('No legacy read')))
 def test_native_context_is_hashed_dated_and_never_a_policy_vote(self):
  _,_,p=packet();at=datetime.fromisoformat(p['generated_at'])
  c=adapter.context(p,at);self.assertTrue(c['available']);self.assertEqual(c['dated_contracts'],12)
  for moment in (at-timedelta(seconds=1),at+timedelta(hours=27)):
   self.assertFalse(adapter.context(p,moment)['available'])
  for bad in (None,{},CANARY,dict(p,calls_eligible=True),dict(p,score=1)):
   self.assertFalse(adapter.context(bad,at)['available']);self.assertIsNone(adapter.qualified_score(bad))
  p['contracts'][0]['latest_bar']['close_field']=1
  self.assertFalse(adapter.context(p,at)['available'])
 def test_other_feeds_stay_unchanged_and_all_permissions_false(self):
  self.assertIs(adapter.guard('data/other.json',CANARY),CANARY)
  view=adapter.guard('data/fedwatch.json',CANARY)
  self.assertTrue(all(view[k] is False for k in adapter.PERMISSIONS));self.assertEqual(view['portfolio_action'],'WAIT')
  self.assertEqual(view['n_meetings_with_data'],0)
if __name__=='__main__':unittest.main(verbosity=2)
