"""Synthetic counterexamples in the preserved classifier; never calls a model."""
from pathlib import Path
from datetime import datetime,timezone
from typing import Dict,List,Optional
import ast,hashlib,json,re,unittest
ROOT=Path(__file__).resolve().parents[2]
RAW=(ROOT/'tests/fixtures/pre-momentum-leaders-catalyst-classifier.py.txt').read_bytes()
TREE=ast.parse(RAW)


def original():
    scope={'datetime':datetime,'timezone':timezone,'Dict':Dict,'List':List,'Optional':Optional,'json':json,'re':re}
    for node in TREE.body:
        if isinstance(node,ast.Assign) and len(node.targets)==1 and isinstance(node.targets[0],ast.Name) and node.targets[0].id in ('VALID_TYPES','VALID_GRADES','VALID_DURABILITY','MAX_CANDIDATES'):
            scope[node.targets[0].id]=ast.literal_eval(node.value)
    names=('validate_record','extract_json_array','build_candidate_universe')
    nodes=[node for node in TREE.body if isinstance(node,ast.FunctionDef) and node.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<synthetic-only preserved classifier functions>','exec'),scope)
    return scope


class Tests(unittest.TestCase):
    def test_wrong_issuer_survives_expected_ticker_validation(self):
        out=original()['validate_record']({'ticker':'SYN_B','catalyst_grade':'A'},'SYN_A')
        self.assertEqual(out['ticker'],'SYN_B');self.assertEqual(out['catalyst_grade'],'A')
    def test_no_response_becomes_negative_grade_and_invented_month_horizon(self):
        out=original()['validate_record']({},'SYN_A')
        self.assertEqual(out['catalyst_grade'],'D');self.assertEqual(out['catalyst_type'],'NO_CLEAR_CATALYST')
        self.assertEqual(out['thesis_durability'],'1M')
    def test_missing_or_unknown_grade_is_not_distinguished_from_reported_d(self):
        fn=original()['validate_record']
        self.assertEqual(fn({'catalyst_grade':'unknown'},'SYN_A')['catalyst_grade'],fn({'catalyst_grade':'D'},'SYN_A')['catalyst_grade'])
    def test_secondary_list_can_be_silently_replaced_by_truncated_text(self):
        out=original()['validate_record']({'secondary_catalysts':'unverified string'},'SYN_A')
        self.assertEqual(out['secondary_catalysts'],'unver');self.assertIsInstance(out['secondary_catalysts'],str)
    def test_non_date_suffix_is_accepted_as_dated_event(self):
        out=original()['validate_record']({'catalyst_date':'2099-01-01not-a-date'},'SYN_A')
        self.assertEqual(out['catalyst_date'],'2099-01-01not-a-date');self.assertIsInstance(out['days_to_catalyst'],int)
    def test_non_string_generated_fields_can_crash_the_validator(self):
        fn=original()['validate_record']
        for field,value in [('catalyst_grade',True),('primary_catalyst',42),('thesis_durability',True)]:
            with self.assertRaises((TypeError,AttributeError)):fn({field:value},'SYN_A')
    def test_complete_original_matches_captured_native_source(self):
        p=json.loads((ROOT/'docs/audit/2026-09-28/momentum-leaders-original-baseline.json').read_bytes())['source_checks']['justhodl-catalyst-classifier']
        self.assertEqual(len(RAW),p['bytes']);self.assertEqual(hashlib.sha256(RAW).hexdigest(),p['sha256'])


if __name__=='__main__':unittest.main(verbosity=2)
