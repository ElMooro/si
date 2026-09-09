"""Offline early containment receipt and side-effect boundary checks."""
import io
import json
import os
from pathlib import Path
import sys
import types
import unittest
import urllib.error
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "aws/ops/checks"), str(ROOT / "aws/shared")]
import audit_20260909_containment as containment
from test_policy_semantics import normalized


class PolicyOnlyS3:
    def __init__(self):
        self.policy = {"Version": "2012-10-17", "Statement": [{"Sid": "ExistingPublicModelRule", "Effect": "Allow", "Principal": "*", "Action": "s3:GetObject", "Resource": "arn:aws:s3:::fixture/public/*"}]}
        self.puts = 0
    def get_bucket_policy(self, **kwargs): return {"Policy": json.dumps(self.policy)}
    def put_bucket_policy(self, **kwargs): self.policy = json.loads(kwargs["Policy"]); self.puts += 1
    def __getattr__(self, name): raise AssertionError("Forbidden object/service operation: " + name)


class Reply:
    def __init__(self, data=None, status=200): self.status=status; self.data=data
    def __enter__(self): return self
    def __exit__(self, *args): pass
    def close(self): pass
    def read(self, size=-1):
        if self.data is None: raise AssertionError("Private body read forbidden")
        raw = json.dumps(self.data).encode(); self.data = {}; return raw


def fixture(*, wrong_account=False, fail_purge=False, exposed=False):
    s3=PolicyOnlyS3();requests=[]
    def http(request, **kwargs):
        requests.append((request.full_url, request.get_method()))
        if request.full_url.startswith("https://api.cloudflare.com/"):
            if fail_purge: raise urllib.error.HTTPError(request.full_url, 403, "fixture", {}, io.BytesIO(b"sensitive-error-fixture"))
            result=[{"name":"justhodl.ai","id":"fixture-zone"}] if request.get_method()=="GET" else {"id":"fixture-purge"}
            return Reply({"success":True,"result":result})
        assert request.get_method()=="HEAD"
        return Reply(status=200 if exposed else 403)
    clients={"s3":s3,"sts":types.SimpleNamespace(get_caller_identity=lambda:{"Account":"wrong" if wrong_account else containment.ACCOUNT})}
    return s3,requests,http,clients


class ContainmentTests(unittest.TestCase):
    def test_aws_normalized_readback_passes_install_and_final_verification(self):
        class S3(PolicyOnlyS3):
            def get_bucket_policy(self, **kwargs): return {"Policy": json.dumps(normalized(self.policy))}
        s3, requests, http, clients = fixture(); clients["s3"] = s3 = S3()
        with patch.dict(os.environ, {"CLOUDFLARE_API_TOKEN":"synthetic-token"}): result=containment.contain(ROOT,clients,http)
        self.assertTrue(result["ok"]); self.assertTrue(result["policy_write_acknowledged"])
        self.assertEqual(result["temporary_containment_state"], "verified_present")
        self.assertTrue(any(c["check"] == "bucket_policy_readback" and c["equivalent"] and not c["raw_equal"] for c in result["checks"]))

    def test_readback_failure_never_reports_unknown_policy_as_absent(self):
        for put_timeout in (False, True):
            with self.subTest(put_timeout=put_timeout):
                class S3(PolicyOnlyS3):
                    def get_bucket_policy(self, **kwargs):
                        if self.puts: raise RuntimeError("sensitive-read-error")
                        return super().get_bucket_policy(**kwargs)
                    def put_bucket_policy(self, **kwargs):
                        super().put_bucket_policy(**kwargs)
                        if put_timeout: raise TimeoutError("sensitive-timeout")
                s3, requests, http, clients = fixture(); clients["s3"] = s3 = S3()
                result=containment.contain(ROOT,clients,http)
                self.assertFalse(result["ok"]); self.assertTrue(result["policy_write_attempted"])
                self.assertEqual(result["policy_write_acknowledged"], not put_timeout)
                self.assertIsNone(result["temporary_containment_retained"])
                self.assertEqual(result["temporary_containment_state"], "unknown")
                self.assertEqual(s3.puts, 1); self.assertFalse(requests)
                self.assertNotIn("sensitive", json.dumps(result))

    def test_changed_policy_fails_and_reports_observed_temporary_control_truthfully(self):
        for remove_temporary in (False, True):
            with self.subTest(remove_temporary=remove_temporary):
                class S3(PolicyOnlyS3):
                    def put_bucket_policy(self, **kwargs):
                        super().put_bucket_policy(**kwargs)
                        if remove_temporary:
                            self.policy["Statement"] = [s for s in self.policy["Statement"] if s["Sid"] != containment.temporary_statement()["Sid"]]
                        else:
                            self.policy["Statement"][0]["Resource"] = "sensitive-drift-resource"
                s3, requests, http, clients = fixture(); clients["s3"] = s3 = S3()
                result=containment.contain(ROOT,clients,http)
                self.assertFalse(result["ok"]); self.assertFalse(result["policy_verified"])
                self.assertEqual(result["temporary_containment_retained"], not remove_temporary)
                self.assertEqual(result["reason"], "bucket_policy_not_verified")
                self.assertEqual(s3.puts, 1); self.assertFalse(requests)
                self.assertNotIn("sensitive", json.dumps(result))

    def test_permission_only_success_preserves_unrelated_policy_and_all_objects(self):
        s3,requests,http,clients=fixture()
        with patch.dict(os.environ,{"CLOUDFLARE_API_TOKEN":"synthetic-token"}): result=containment.contain(ROOT,clients,http)
        self.assertTrue(result["ok"]);self.assertTrue(result["temporary_containment_retained"]);self.assertEqual(s3.puts,1)
        self.assertTrue(any(s["Sid"]=="ExistingPublicModelRule" for s in s3.policy["Statement"]))
        for key in ("private_payloads_read","original_objects_written","producer_invocations","service_configuration_changes"):self.assertEqual(result[key],0)
        self.assertFalse(result["temporary_containment_removed"])
        self.assertTrue(all(method=="HEAD" for url,method in requests if "api.cloudflare.com" not in url))
        self.assertTrue(any(c["check"]=="final_containment_policy" and c["ok"] for c in result["checks"]))

    def test_wrong_account_performs_no_policy_write_or_http(self):
        s3,requests,http,clients=fixture(wrong_account=True);result=containment.contain(ROOT,clients,http)
        self.assertFalse(result["ok"]);self.assertEqual(s3.puts,0);self.assertFalse(requests)

    def test_purge_failure_keeps_verified_s3_containment_and_reports_partial_without_error_body(self):
        s3,requests,http,clients=fixture(fail_purge=True)
        with patch.dict(os.environ,{"CLOUDFLARE_API_TOKEN":"synthetic-token"}):result=containment.contain(ROOT,clients,http)
        self.assertFalse(result["ok"]);self.assertTrue(result["policy_verified"]);self.assertTrue(result["s3_denial_verified"])
        self.assertTrue(result["temporary_containment_retained"]);self.assertEqual(s3.puts,1)
        self.assertNotIn("sensitive-error-fixture",json.dumps(result));self.assertNotIn("synthetic-token",json.dumps(result))

    def test_unexpected_public_head_success_fails_without_reading_body_or_removing_deny(self):
        s3,requests,http,clients=fixture(exposed=True)
        with patch.dict(os.environ,{"CLOUDFLARE_API_TOKEN":"synthetic-token"}):result=containment.contain(ROOT,clients,http)
        self.assertFalse(result["ok"]);self.assertFalse(result["s3_denial_verified"]);self.assertTrue(result["temporary_containment_retained"])
        self.assertEqual(result["private_payloads_read"],0);self.assertEqual(s3.puts,1)


if __name__=="__main__":unittest.main()
