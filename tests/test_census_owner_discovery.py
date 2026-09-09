"""Offline, actual discovery-path tests; no AWS credentials or SDK required."""
import ast
from contextlib import redirect_stdout
import importlib.util
import io
import json
from pathlib import Path
import threading
import time
import unittest
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
SOURCE = ROOT / "aws/ops/checks/audit_20260909_census_owner.py"
spec = importlib.util.spec_from_file_location("census_owner", SOURCE)
mod = importlib.util.module_from_spec(spec)
spec.loader.exec_module(mod)
CANARY = "PRIVATE_ENV_LOG_PROVIDER_RESPONSE_CANARY"


def arn(name, alias=""):
    return f"arn:aws:lambda:us-east-1:857687956942:function:{name}" + (":" + alias if alias else "")


def function(name):
    return {"FunctionName": name, "FunctionArn": arn(name), "CodeSha256": "L" * 43 + "=",
            "Environment": {"Variables": {"TOKEN": CANARY}}, "Description": CANARY}


def url(name, alias="", host=mod.TARGET_HOST):
    return {"FunctionArn": arn(name, alias), "FunctionUrl": "https://" + host + "/",
            "Cors": {"AllowHeaders": [CANARY]}}


class SDKError(Exception):
    def __init__(self, code="AccessDeniedException"):
        super().__init__(CANARY)
        self.response = {"Error": {"Code": code, "Message": CANARY}, "Request": CANARY}


class STS:
    def __init__(self, account=mod.ACCOUNT):
        self.account = account

    def get_caller_identity(self):
        return {"Account": self.account, "UserId": CANARY, "Arn": CANARY}


class Lambda:
    def __init__(self):
        self.functions = {None: {"Functions": [function("other")], "NextMarker": "page2"},
                          "page2": {"Functions": [function("census")]}}
        self.urls = {("other", None): {"FunctionUrlConfigs": [url("other", host="different.lambda-url.us-east-1.on.aws")]},
                     ("census", None): {"FunctionUrlConfigs": [], "NextMarker": "aliaspage"},
                     ("census", "aliaspage"): {"FunctionUrlConfigs": [url("census", "live")]}}
        self.calls = []
        self.active = 0
        self.peak = 0
        self.delay = 0
        self.lock = threading.Lock()

    def list_functions(self, **kwargs):
        self.calls.append(("list_functions", kwargs))
        row = self.functions[kwargs.get("Marker")]
        if isinstance(row, Exception):
            raise row
        return row

    def list_function_url_configs(self, **kwargs):
        with self.lock:
            self.active += 1
            self.peak = max(self.peak, self.active)
        try:
            if self.delay:
                time.sleep(self.delay)
            self.calls.append(("list_function_url_configs", kwargs))
            row = self.urls[(kwargs["FunctionName"], kwargs.get("Marker"))]
            if isinstance(row, Exception):
                raise row
            return row
        finally:
            with self.lock:
                self.active -= 1

    def get_function_configuration(self, **kwargs):
        self.calls.append(("get_function_configuration", kwargs))
        name = kwargs["FunctionName"].split(":function:")[1].split(":")[0]
        return {**function(name), "FunctionArn": arn(name, "17"), "CodeSha256": "Q" * 43 + "=",
                "Handler": "lambda_function.lambda_handler", "Runtime": "python3.12", "State": "Active",
                "PackageType": "Zip", "Role": CANARY, "StateReason": CANARY,
                "Environment": {"Variables": {"TOKEN": CANARY}}, "Code": {"Location": CANARY}}


class CensusOwnerTests(unittest.TestCase):
    def run_scan(self, client=None, sts=None):
        output = io.StringIO()
        with redirect_stdout(output):
            result = mod.discover(client or Lambda(), sts or STS())
        self.assertNotIn(CANARY, json.dumps(result) + output.getvalue())
        self.assertEqual(output.getvalue(), "")
        return result

    def test_complete_both_paginations_resolves_only_matched_alias_metadata(self):
        client = Lambda()
        result = self.run_scan(client)
        self.assertTrue(result["ok"])
        self.assertEqual(result["status"], "FOUND")
        self.assertEqual(result["coverage"]["function_pages"], 2)
        self.assertEqual(result["coverage"]["url_pages"], 3)
        self.assertEqual(result["coverage"]["functions_enumerated"], 2)
        self.assertEqual(result["matches"], [{"function_arn": arn("census", "live"),
            "function_name": "census", "code_sha256": "Q" * 43 + "=", "handler": "lambda_function.lambda_handler",
            "runtime": "python3.12", "state": "Active"}])
        self.assertEqual([args for name, args in client.calls if name == "get_function_configuration"],
                         [{"FunctionName": arn("census", "live")}])
        self.assertTrue(all("FunctionVersion" not in args for name, args in client.calls))

    def test_exact_hostname_not_suffix_or_prefix(self):
        client = Lambda()
        client.urls[("census", "aliaspage")] = {"FunctionUrlConfigs": [
            url("census", host=mod.TARGET_HOST + ".attacker.example"),
            url("census", host="prefix" + mod.TARGET_HOST)]}
        result = self.run_scan(client)
        self.assertEqual(result["status"], "NOT_FOUND")
        self.assertFalse(result["ok"])
        self.assertTrue(result["coverage"]["url_scan_complete"])
        self.assertFalse(any(name == "get_function_configuration" for name, _ in client.calls))

    def test_failed_function_scan_keeps_match_but_cannot_attest_complete(self):
        client = Lambda()
        client.urls[("other", None)] = SDKError()
        result = self.run_scan(client)
        self.assertFalse(result["ok"])
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertEqual(len(result["matches"]), 1)
        self.assertEqual(result["error_types"], {"url_scan:AccessDeniedException": 1})

    def test_unknown_sdk_error_code_is_not_copied(self):
        client = Lambda()
        client.functions[None] = SDKError(CANARY)
        result = self.run_scan(client)
        self.assertEqual(result["error_types"], {"enumeration:UNCLASSIFIED_ERROR": 1})

    def test_wrong_account_never_enumerates(self):
        client = Lambda()
        result = self.run_scan(client, STS("000000000000"))
        self.assertFalse(result["account_verified"])
        self.assertEqual(client.calls, [])

    def test_repeated_function_marker_and_url_marker_fail_closed(self):
        for mode in ("functions", "urls"):
            with self.subTest(mode=mode):
                client = Lambda()
                if mode == "functions":
                    client.functions["page2"] = {"Functions": [], "NextMarker": "page2"}
                else:
                    client.urls[("census", "aliaspage")] = {"FunctionUrlConfigs": [], "NextMarker": "aliaspage"}
                result = self.run_scan(client)
                self.assertFalse(result["ok"])
                self.assertEqual(result["error_count"], 1)
                self.assertLess(len(client.calls), 7)

    def test_two_matches_are_ambiguous_not_arbitrary_winner(self):
        client = Lambda()
        client.urls[("other", None)] = {"FunctionUrlConfigs": [url("other")]}
        result = self.run_scan(client)
        self.assertEqual(result["status"], "AMBIGUOUS")
        self.assertFalse(result["ok"])
        self.assertEqual(len(result["matches"]), 2)

    def test_concurrency_is_bounded_and_all_functions_scanned(self):
        client = Lambda()
        names = ["function" + str(i) for i in range(30)]
        client.functions = {None: {"Functions": [function(name) for name in names]}}
        client.urls = {(name, None): {"FunctionUrlConfigs": []} for name in names}
        client.delay = 0.005
        result = self.run_scan(client)
        self.assertTrue(result["coverage"]["url_scan_complete"])
        self.assertEqual(result["coverage"]["functions_scanned"], 30)
        self.assertLessEqual(client.peak, 6)
        self.assertGreater(client.peak, 1)

    def test_page_and_time_limits_do_not_claim_complete(self):
        for constant, limit in (("MAX_FUNCTION_PAGES", 1), ("MAX_URL_PAGES", 1), ("MAX_SECONDS", 0)):
            with self.subTest(constant=constant), patch.object(mod, constant, limit):
                result = self.run_scan()
                self.assertEqual(result["status"], "INCOMPLETE")
                self.assertGreater(result["error_count"], 0)

    def test_missing_state_cannot_be_fabricated(self):
        client = Lambda()
        original = client.get_function_configuration
        def missing(**kwargs):
            result = original(**kwargs)
            del result["State"]
            return result
        client.get_function_configuration = missing
        result = self.run_scan(client)
        self.assertEqual(result["status"], "INCOMPLETE")
        self.assertEqual(len(result["matches"]), 1)
        self.assertEqual(result["matches"][0]["function_arn"], arn("census", "live"))
        self.assertIsNone(result["matches"][0]["state"])
        self.assertIsNone(result["matches"][0]["code_sha256"])
        self.assertEqual(result["error_types"], {"match_configuration:INVALID_METADATA": 1})

    def test_source_allows_only_read_metadata_methods_and_no_payload_transport(self):
        tree = ast.parse(SOURCE.read_text())
        methods = {node.func.attr for node in ast.walk(tree) if isinstance(node, ast.Call)
                   and isinstance(node.func, ast.Attribute) and isinstance(node.func.value, ast.Name)
                   and node.func.value.id in {"client", "sts"}}
        self.assertEqual(methods, {"get_caller_identity", "list_functions", "list_function_url_configs",
                                   "get_function_configuration"})
        imports = {node.module for node in ast.walk(tree) if isinstance(node, ast.ImportFrom)}
        self.assertFalse(imports & {"requests", "urllib.request", "botocore"})


if __name__ == "__main__":
    unittest.main()
