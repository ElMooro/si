"""Current native board keeps descriptive Global Flow evidence and never grants it a vote."""
from pathlib import Path
import json,sys,unittest
ROOT=Path(__file__).resolve().parents[4]
sys.path.insert(0,str(ROOT/'tests'))
from signal_board_native_test_support import assert_abstention,assert_compiler_pin

def key():
    rows=json.loads((ROOT/'aws/lambdas/justhodl-signal-board/source/board_registry.json').read_bytes())
    matched=[r['source_key'] for r in rows if r['normalizer']=='n_globalflows']
    assert len(matched)==1
    return matched[0]

class Tests(unittest.TestCase):
    def test_legacy_scores_and_current_descriptive_source_do_not_vote(self):
        assert_compiler_pin()
        assert_abstention(key(),({}, {'inst_vs_retail':{'institutional':99,'retail':99},'hot_money':{'n_scored':20}},
            {'contract':'global-flow-research.v1','quality':{'aligned_funds':81,'configured_funds':128}}))
    def test_unqualified_claims_cannot_create_or_dilute_an_independent_vote(self):
        # The current complete inventory has no qualified votes. A claimed positive
        # score cannot invent one; every row/source remains retained and abstains.
        assert_abstention(key(),({'contract':'global-flow-research.v1','generated_at':'2020-01-01T00:00:00Z',
            'calls_eligible':True,'sizing_eligible':True,'score':99,'signal':1,'independent_investment_votes':1},))

if __name__=='__main__':unittest.main()
