"""Counterexamples in the preserved brief; no model or live consumer read."""
from pathlib import Path
from datetime import datetime,timezone
from typing import Dict,List,Optional
import ast,hashlib,json,re,unittest
ROOT=Path(__file__).resolve().parents[2]
RAW=(ROOT/'tests/fixtures/pre-momentum-leaders-pump-radar-brief.py.txt').read_bytes()
TREE=ast.parse(RAW)


def original():
    scope={'datetime':datetime,'timezone':timezone,'Dict':Dict,'List':List,'Optional':Optional,'json':json,'re':re}
    names=('compute_market_temperature','compute_whats_changed','build_user_prompt','extract_json')
    nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated synthetic brief functions>','exec'),scope)
    return scope


class Tests(unittest.TestCase):
    def test_no_evidence_is_reported_as_zero_temperature_and_cool(self):
        out=original()['compute_market_temperature']({},{},{})
        self.assertEqual(out,{'score':0,'rank':'COOL','components':{}})
    def test_current_portfolio_exposure_is_recycled_into_hot_market_evidence(self):
        fn=original()['compute_market_temperature'];radar={'top_candidates':[{'ticker':'SYN_A','pump_likelihood':100}]}
        baseline=fn(radar,{},{});levered=fn(radar,{'basket':{'total_exposure':100}},{})
        self.assertEqual(baseline['score'],65);self.assertEqual(levered['score'],80)
        self.assertEqual(levered['rank'],'HOT')
    def test_unrelated_options_tickers_can_increase_a_candidate_temperature(self):
        fn=original()['compute_market_temperature'];radar={'top_candidates':[{'ticker':'SYN_A','pump_likelihood':50}]}
        other={'per_ticker':[{'ticker':'OTHER'+str(i),'options_skew':'bullish'} for i in range(5)]}
        self.assertEqual(fn(radar,{},other)['score']-fn(radar,{},{})['score'],20)
    def test_reported_maximum_is_first_row_and_changes_with_order(self):
        fn=original()['compute_market_temperature'];rows=[{'pump_likelihood':10},{'pump_likelihood':90}]
        a=fn({'top_candidates':rows},{},{});b=fn({'top_candidates':rows[::-1]},{},{})
        self.assertEqual(a['components']['max_pump'],10);self.assertEqual(b['components']['max_pump'],90)
        self.assertNotEqual(a['score'],b['score'])
    def test_comparing_three_ideas_to_five_candidates_manufactures_new_signals(self):
        fn=original()['compute_whats_changed']
        same_population=[{'ticker':'SYN_'+str(i)} for i in range(5)]
        out=fn({'top_3_long_ideas':same_population[:3]},same_population)
        self.assertEqual(out['new_signals'],['SYN_3','SYN_4'])
    def test_context_is_silently_cut_in_the_middle_of_json(self):
        payload={'synthetic':{'rows':['kept_'+str(i)+'x'*100 for i in range(200)],'tail':'must survive'}}
        prompt=original()['build_user_prompt'](payload)
        block=prompt.split('```json\n',1)[1].split('\n```',1)[0]
        self.assertEqual(len(block),10000);self.assertNotIn('must survive',block)
        with self.assertRaises(json.JSONDecodeError):json.loads(block)
    def test_complete_original_matches_actual_source_capture(self):
        p=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['source_checks']['justhodl-pump-radar-brief']
        self.assertEqual(len(RAW),p['bytes']);self.assertEqual(hashlib.sha256(RAW).hexdigest(),p['sha256'])


if __name__=='__main__':unittest.main(verbosity=2)
