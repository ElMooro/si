"""Offline AWS policy representation roundtrips and permission-drift rejection."""
from copy import deepcopy
import json
from pathlib import Path
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "aws/ops/checks"), str(ROOT / "aws/shared")]
import audit_20260909_security as security
import audit_20260909_privacy_migration as migration


def normalized(policy):
    result = deepcopy(policy)
    statements = result["Statement"]
    statements = [statements] if isinstance(statements, dict) else statements
    for statement in statements:
        for field in ("Action", "NotAction", "Resource", "NotResource"):
            if field in statement:
                values = statement[field] if isinstance(statement[field], list) else [statement[field]]
                statement[field] = values[0] if len(values) == 1 else list(reversed(values))
        if statement.get("Principal") == "*":
            statement["Principal"] = {"AWS": ["*"]}
        for terms in statement.get("Condition", {}).values():
            for key, value in terms.items():
                terms[key] = list(reversed(value)) if isinstance(value, list) else [value]
    result["Statement"] = list(reversed(statements))
    return result


class SemanticsTests(unittest.TestCase):
    def setUp(self):
        self.statement = migration.temporary_statement()
        self.policy = {"Version": "2012-10-17", "Statement": [self.statement]}

    def test_documented_collection_and_principal_forms_match_without_mutation(self):
        original = deepcopy(self.policy)
        actual = normalized(self.policy)
        self.assertTrue(security.policies_equal(self.policy, actual))
        self.assertTrue(security.has_policy_statement(actual, self.statement))
        self.assertEqual(self.policy, original)
        self.assertFalse(security.policy_diagnostic(actual, self.policy)["raw_equal"])
        self.assertTrue(security.policy_diagnostic(actual, self.policy)["equivalent"])
        self.assertTrue(security.policies_equal({**self.policy, "Statement": self.statement}, self.policy))

    def test_every_permission_change_fails_even_if_sid_matches(self):
        mutations = [
            lambda s: s["Resource"].pop(),
            lambda s: s.update(Resource="arn:aws:s3:::different/*"),
            lambda s: s.update(Resource="*"),
            lambda s: s.update(Action="s3:GetObject"),
            lambda s: s.update(Effect="Allow"),
            lambda s: s.update(Principal={"AWS": "123456789012"}),
            lambda s: s.update(Principal={"AWS": "*", "Service": "s3.amazonaws.com"}),
            lambda s: s.update(Condition={"StringNotEquals": {"aws:PrincipalAccount": "123456789012"}}),
            lambda s: s.update(Condition={"StringNotEqualsIfExists": {"aws:PrincipalAccount": migration.ACCOUNT}}),
            lambda s: s.update(Condition={"StringEquals": {"aws:PrincipalAccount": migration.ACCOUNT}}),
            lambda s: s.update(Condition={}),
            lambda s: s["Condition"].update(Null={"aws:PrincipalArn": "true"}),
            lambda s: s.update(NotAction=s.pop("Action")),
            lambda s: s.update(NotPrincipal=s.pop("Principal")),
            lambda s: s.update(Action=[]),
            lambda s: s.update(Principal={"AWS": []}),
        ]
        for number, mutate in enumerate(mutations):
            with self.subTest(number=number):
                actual = deepcopy(self.policy)
                mutate(actual["Statement"][0])
                self.assertFalse(security.policies_equal(actual, self.policy))
                self.assertFalse(security.has_policy_statement(actual, self.statement))

    def test_condition_scalar_types_remain_distinct(self):
        for left, right in ((True, 1), (1, "1"), (False, 0)):
            a, b = deepcopy(self.policy), deepcopy(self.policy)
            a["Statement"][0]["Condition"] = {"Bool": {"fixture": left}}
            b["Statement"][0]["Condition"] = {"Bool": {"fixture": right}}
            self.assertFalse(security.policies_equal(a, b))

    def test_unknown_fields_version_and_unrelated_statements_are_preserved(self):
        variants = []
        a = deepcopy(self.policy); a["Version"] = "2008-10-17"; variants.append(a)
        a = deepcopy(self.policy); del a["Version"]; variants.append(a)
        a = deepcopy(self.policy); a["Statement"].append({"Effect":"Allow","Action":"s3:*","Resource":"*"}); variants.append(a)
        a = deepcopy(self.policy); a["Statement"][0]["Unexpected"] = ["a", "b"]; variants.append(a)
        for variant in variants:
            self.assertFalse(security.policies_equal(variant, self.policy))
        a, b = deepcopy(variants[-1]), deepcopy(variants[-1])
        b["Statement"][0]["Unexpected"].reverse()
        self.assertFalse(security.policies_equal(a, b))
        duplicated = deepcopy(self.policy); duplicated["Statement"].append(deepcopy(self.statement))
        self.assertFalse(security.policies_equal(duplicated, self.policy))
        self.assertFalse(security.has_policy_statement(duplicated, self.statement))

    def test_prewrite_normalization_is_allowed_but_concurrent_permission_change_blocks(self):
        class S3:
            def __init__(self, drift):
                self.policy = {"Version":"2012-10-17", "Statement":[{"Sid":"Fixture", "Effect":"Allow", "Principal":"*", "Action":["s3:GetObject"], "Resource":["arn:aws:s3:::fixture/public/*"]}]}
                self.reads = self.puts = 0
                self.drift = drift
            def get_bucket_policy(self, **kwargs):
                self.reads += 1
                actual = normalized(self.policy) if self.reads > 1 else self.policy
                if self.drift and self.reads == 2:
                    actual["Statement"][0]["Action"] = "s3:*"
                return {"Policy": json.dumps(actual)}
            def put_bucket_policy(self, **kwargs):
                self.puts += 1; self.policy = json.loads(kwargs["Policy"])
        safe = S3(False); job = migration.Migration(ROOT, {"s3":safe}); job.policy(True)
        self.assertEqual(safe.puts, 1); self.assertTrue(job.temp_installed)
        drift = S3(True)
        with self.assertRaisesRegex(migration.MigrationError, "bucket_policy_changed_during_merge"):
            migration.Migration(ROOT, {"s3":drift}).policy(True)
        self.assertEqual(drift.puts, 0)

    def test_metadata_diagnostic_never_emits_unrelated_values(self):
        actual = deepcopy(self.policy)
        actual["Statement"].append({"Sid":"sensitive-sid", "Effect":"Allow", "Resource":"sensitive-resource", "Principal":{"AWS":"sensitive-principal"}})
        text = json.dumps(security.policy_diagnostic(actual, self.policy))
        self.assertNotIn("sensitive", text)
        self.assertFalse(security.policy_diagnostic(actual, self.policy)["equivalent"])


if __name__ == "__main__":
    unittest.main()
