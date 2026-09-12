"""Dependency-free behavioural tests: constrained creation and failure safety."""
import copy
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import Mock, patch

SOURCE = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SOURCE))
import brief_schedules as bs


class Missing(Exception):
    pass


class FakeScheduler:
    exceptions = types.SimpleNamespace(ResourceNotFoundException=Missing)

    def __init__(self):
        self.rows = {}
        self.created = []

    def get_schedule(self, Name, GroupName):
        if Name not in self.rows:
            raise Missing()
        return copy.deepcopy(self.rows[Name])

    def create_schedule(self, **kw):
        self.created.append(copy.deepcopy(kw))
        row = {k:v for k,v in kw.items() if k != "ClientToken"}
        row["Arn"] = "arn:aws:scheduler:us-east-1:857687956942:schedule/default/" + row["Name"]
        self.rows[row["Name"]] = row


class FakeEvents:
    exceptions = types.SimpleNamespace(ResourceNotFoundException=Missing)

    def describe_rule(self, Name):
        raise Missing()


def manifest():
    rows = []
    for name, (expr, mode) in bs.SPECS.items():
        rows.append({"kind": "scheduler", "name": name, "group": "default",
                     "expr": expr, "state": "ENABLED", "timezone": "UTC",
                     "flexible_time_window": {"Mode": "OFF"},
                     "targets": [{"arn": bs.FUNCTION_ARN, "role_arn": bs.ROLE_ARN,
                                  "input": json.dumps({"mode": mode}), "path": None,
                                  "retry_policy": {"MaximumEventAgeInSeconds": 3600, "MaximumRetryAttempts": 2}}]})
    return {"schedules": rows, "rules": [{"kind": "events", "name": "unrelated",
             "expr": "rate(1 hour)", "state": "DISABLED", "targets": []}]}


class ScheduleTests(unittest.TestCase):
    def setUp(self):
        self.s = FakeScheduler()
        self.e = FakeEvents()
        self.m = manifest()

    def test_create_six_exact_routes_and_idempotent_rerun(self):
        before = copy.deepcopy(self.m)
        result = bs.attach(self.m, self.s, self.e)
        self.assertEqual(set(result["created"]), set(bs.SPECS))
        for row in result["verified"]:
            self.assertEqual(row["input"], {"mode": bs.SPECS[row["name"]][1]})
            self.assertEqual(row["target"], bs.FUNCTION_ARN)
        self.assertEqual(bs.attach(self.m, self.s, self.e)["created"], [])
        self.assertEqual(len(self.s.created), 6)
        self.assertEqual(self.m, before)

    def test_bad_last_row_causes_zero_creates(self):
        for field, value in (("expr", "rate(1 minute)"), ("state", "DISABLED"),
                             ("timezone", "America/New_York"), ("group", "other")):
            with self.subTest(field=field):
                m = copy.deepcopy(self.m)
                m["schedules"][-1][field] = value
                with self.assertRaises(ValueError): bs.attach(m, self.s, self.e)
                self.assertEqual(self.s.created, [])

    def test_reject_other_function_role_extra_input_or_input_path(self):
        for field, value in (("arn", bs.FUNCTION_ARN + "-other"), ("role_arn", "other"),
                             ("input", '{"mode":"all"}'), ("input", '{"mode":"verdict","extra":true}'),
                             ("path", "$.detail")):
            with self.subTest(field=field, value=value):
                m = copy.deepcopy(self.m)
                m["schedules"][-1]["targets"][0][field] = value
                with self.assertRaises(ValueError): bs.attach(m, self.s, self.e)
                self.assertEqual(self.s.created, [])

    def test_duplicate_missing_and_classic_declarations_rejected(self):
        variants = []
        m = copy.deepcopy(self.m); m["schedules"].pop(); variants.append(m)
        m = copy.deepcopy(self.m); m["schedules"][-1] = m["schedules"][0]; variants.append(m)
        m = copy.deepcopy(self.m); m["rules"].append({"name": next(iter(bs.SPECS))}); variants.append(m)
        for m in variants:
            with self.assertRaises(ValueError): bs.attach(m, self.s, self.e)
        self.assertEqual(self.s.created, [])

    def test_classic_rule_even_disabled_blocks_creation(self):
        self.e.describe_rule = Mock(return_value={"State": "DISABLED"})
        with self.assertRaises(ValueError): bs.attach(self.m, self.s, self.e)
        self.assertEqual(self.s.created, [])

    def test_access_denied_is_not_missing(self):
        self.s.get_schedule = Mock(side_effect=PermissionError("denied"))
        with self.assertRaises(PermissionError): bs.attach(self.m, self.s, self.e)
        self.assertEqual(self.s.created, [])

    def test_conflicting_existing_schedule_prevents_any_creation(self):
        req = bs.requests_from_manifest(self.m)[-1]
        req["Target"]["Input"] = '{"mode":"plumbing"}'
        self.s.rows[req["Name"]] = req
        with self.assertRaises(ValueError): bs.attach(self.m, self.s, self.e)
        self.assertEqual(self.s.created, [])

    def test_partial_api_failure_can_resume_without_duplicate_creates(self):
        create = self.s.create_schedule
        def partial(**kw):
            if len(self.s.created) == 2: raise RuntimeError("transient API error")
            create(**kw)
        self.s.create_schedule = partial
        with self.assertRaises(RuntimeError): bs.attach(self.m, self.s, self.e)
        self.s.create_schedule = create
        self.assertEqual(len(bs.attach(self.m, self.s, self.e)["created"]), 4)
        self.assertEqual(len(self.s.created), 6)

    def test_creation_response_alone_is_not_verification(self):
        create = self.s.create_schedule
        def bad_create(**kw):
            create(**kw)
            self.s.rows[kw["Name"]]["State"] = "DISABLED"
        self.s.create_schedule = bad_create
        with self.assertRaises(ValueError): bs.attach(self.m, self.s, self.e)

    def test_real_handler_scoped_mode_never_calls_general_enforcement(self):
        clients = {name:Mock() for name in ("s3", "ssm", "events", "scheduler")}
        clients["s3"].get_object.return_value = {"Body": io.BytesIO(json.dumps(self.m).encode())}
        clients["ssm"].get_parameter.return_value = {"Parameter": {"Value": "enforce"}}
        boto = types.SimpleNamespace(client=lambda name, **kw: clients[name])
        config = types.ModuleType("botocore.config"); config.Config = lambda **kw: None
        with patch.dict(sys.modules, {"boto3": boto, "botocore": types.ModuleType("botocore"), "botocore.config": config}):
            spec = importlib.util.spec_from_file_location("tested_reconciler", SOURCE / "lambda_function.py")
            module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        with patch.object(bs, "attach", return_value={"created": [], "verified": []}) as attach, \
                patch.object(module, "read_live", return_value=({}, {})), \
                patch.object(module, "enforce", side_effect=AssertionError("broad enforce forbidden")):
            result = module.lambda_handler({"mode": "attach-brief-compiler"})
        self.assertTrue(result["ok"])
        self.assertEqual(result["enforced"], 0)
        attach.assert_called_once()
        saved = json.loads(clients["s3"].put_object.call_args.kwargs["Body"])
        self.assertIsNotNone(saved["compiler_attachment"])


if __name__ == "__main__":
    unittest.main()
