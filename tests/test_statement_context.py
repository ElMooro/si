from pathlib import Path
from copy import deepcopy
import json, sys, unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]/'aws/shared'))
import statement_context as context


def current():
    fixture=json.loads((Path(__file__).resolve().parent/'fixtures/statement-research-public.json').read_text())
    return json.loads(fixture['objects']['data/forensic-screen.json'])


class Tests(unittest.TestCase):
    def test_native_lineage_remains_descriptive_with_zero_decision_votes(self):
        p=current();view=context.decision_view(p);c=view['research_context']
        self.assertTrue(c['native_reference_available']);self.assertEqual(c['canonical']['replay'],p['replay'])
        self.assertEqual(view['all_results'],[]);self.assertEqual(view['independent_investment_votes'],0)
        self.assertIsNone(view['m_score']);self.assertIsNone(view['grade'])
        self.assertFalse(c['original_provider_replay_performed_by_consumer']);self.assertFalse(c['current_freshness_verified_by_consumer'])

    def test_legacy_fraud_scores_or_strength_grades_never_pass_through(self):
        old={'version':'2.2.0','all_results':[{'symbol':'ABC','m_score':2,'concern_score':90,'strength_grade':'A+'}],
            'cleanest_top_25':[{'symbol':'XYZ','strength_score':99}]}
        view=context.decision_view(old)
        self.assertFalse(view['research_context']['native_reference_available']);self.assertEqual(view['all_results'],[])
        self.assertEqual(view['cleanest_top_25'],[]);self.assertIsNone(view['grade'])

    def test_digest_tampering_forecast_flags_and_duplicate_identities_cannot_qualify(self):
        for mutate in (lambda p:p.update(calls_eligible=True),lambda p:p.update(provider_rows=1),
            lambda p:p['replay'].update(output_sha256='0'*64),lambda p:p['issuers'].append(p['issuers'][0]),
            lambda p:p['quality'].update(accounting_audit=True),lambda p:p.update(generated_at='2099-01-01T00:00:00Z')):
            p=current();mutate(p);self.assertFalse(context.context(p)['native_reference_available'])
        for value in (None,[],{'generated_at':1}):self.assertFalse(context.context(value)['native_reference_available'])

    def test_universe_is_explicitly_identity_only_and_shared_cik_is_not_double_evidence(self):
        p=current();rows=context.reported_universe(p)
        self.assertEqual(len(rows),6);self.assertEqual({v['symbol'] for v in rows},{'ABC','XYZ','PART','EMPTY','BADCIK','BADDATE'})
        self.assertTrue(all(v['sector'] is None and v['independent_investment_votes']==0 for v in rows))
        self.assertEqual(rows[0]['reported_issuer_roots'],rows[-1]['reported_issuer_roots'])
        self.assertEqual(context.reported_universe({'all_results':[{'symbol':'ABC'}]}),[])

    def test_guard_only_applies_to_accounting_and_never_mutates_source(self):
        p=current();before=deepcopy(p)
        self.assertIs(context.guard('data/other.json',p),p)
        view=context.guard(context.CURRENT,p);view['research_context']['canonical']['replay']['output_sha256']='bad'
        self.assertEqual(p,before)


if __name__=='__main__':unittest.main(verbosity=2)
