"""Reproduce unsupported sizing in a preserved predecessor, using synthetic inputs only."""
from pathlib import Path
from copy import deepcopy
from typing import Dict
import ast,unittest

ROOT=Path(__file__).resolve().parents[2]
RAW=(ROOT/'tests/fixtures/pre-momentum-leaders-catalyst-clusters.py.txt').read_bytes()
TREE=ast.parse(RAW)


def original():
    assignment=next(n for n in TREE.body if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='GRADE_WEIGHTS' for t in n.targets))
    ns={'Dict':Dict,'GRADE_WEIGHTS':ast.literal_eval(assignment.value)}
    nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in ('grade_cluster','recommend_action')]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated original catalyst arithmetic>','exec'),ns)
    return ns


def cluster(grade='B'):
    tickers=['SYN_A','SYN_B','SYN_C']
    return {'members':tickers,'member_records':[{'ticker':t,'catalyst_grade':grade} for t in tickers],
            'cluster_type':'EARNINGS_WINDOW','scope':'BASKET_LOCAL'}


class Tests(unittest.TestCase):
    def test_no_momentum_observations_become_perfect_agreement_and_nonzero_size_changes(self):
        ns=original();c=cluster();c.update(ns['grade_cluster'](c,{}))
        self.assertEqual(c['momentum_spread'],0);self.assertEqual(c['quality_grade'],'B')
        self.assertEqual([r['momentum'] for r in c['ranked_members']],[0,0,0])
        original_sizes={t:10 for t in c['members']};saved=deepcopy(original_sizes)
        action=ns['recommend_action'](c,original_sizes,'NEUTRAL')
        self.assertEqual(action['leader_new_size'],13)
        self.assertEqual(action['laggard_new_sizes'],{'SYN_B':5,'SYN_C':5})
        self.assertEqual(original_sizes,saved)
    def test_missing_values_not_only_measured_zero_can_crash_the_old_grade(self):
        ns=original();c=cluster()
        with self.assertRaises(TypeError):ns['grade_cluster'](c,{'SYN_A':None})
    def test_adjacent_missing_evidence_still_suggests_opening_a_position(self):
        ns=original();c=cluster();c['scope']='ADJACENT';c.update(ns['grade_cluster'](c,{}))
        action=ns['recommend_action'](c,{},'NEUTRAL')
        self.assertEqual(action['action'],'CONSIDER_ADD');self.assertEqual(action['suggested_entry'],'SYN_A')


if __name__=='__main__':unittest.main(verbosity=2)
