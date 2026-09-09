"""audit 2026-09-08 INST-03 -- aws/shared/api_auth.py behavioural tests (dependency-free).

boto3 is stubbed so the module imports without AWS; the DynamoDB rate table is an
in-memory fake that can be told to raise ClientError.
"""
from __future__ import annotations

import importlib.util
import sys
import types
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]


class _FakeClientError(Exception):
    pass


class _FakeDDB:
    def __init__(self):
        self.counts = {}
        self.raise_errors = False
        self.calls = 0

    def update_item(self, **kw):
        self.calls += 1
        if self.raise_errors:
            raise _FakeClientError("ProvisionedThroughputExceeded")
        pk = kw["Key"]["pk"]["S"]
        if "TableName" in kw and kw["TableName"].endswith("api-keys"):
            return {}
        self.counts[pk] = self.counts.get(pk, 0) + 1
        return {"Attributes": {"count": {"N": str(self.counts[pk])}}}

    def get_item(self, **kw):
        return {}


def _load():
    fake_boto3 = types.ModuleType("boto3")
    ddb = _FakeDDB()
    fake_boto3.client = lambda *a, **k: ddb
    fake_bc = types.ModuleType("botocore")
    fake_exc = types.ModuleType("botocore.exceptions")
    fake_exc.ClientError = _FakeClientError
    fake_bc.exceptions = fake_exc
    spec = importlib.util.spec_from_file_location("api_auth_under_test", ROOT / "aws" / "shared" / "api_auth.py")
    mod = importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules, {"boto3": fake_boto3, "botocore": fake_bc, "botocore.exceptions": fake_exc}):
        spec.loader.exec_module(mod)
    return mod, ddb


def _event(origin=None, ip="203.0.113.7", key=None):
    headers = {}
    if origin:
        headers["origin"] = origin
    if key:
        headers["authorization"] = "Bearer " + key
    return {"headers": headers, "requestContext": {"http": {"sourceIp": ip}}}


def test_origin_alone_never_grants_enterprise():
    mod, ddb = _load()
    meta, err = mod.authorize(_event(origin="https://justhodl.ai"), allowed_origins=["https://justhodl.ai"])
    assert err is None, err
    assert meta["tier"] == "SITE", meta
    assert meta["auth_mode"] == "origin"
    assert meta["client_ip"] == "203.0.113.7"
    assert ddb.calls >= 3, "site traffic is counted in all three windows"


def test_site_tier_is_metered_per_client_ip():
    mod, ddb = _load()
    ev = _event(origin="https://justhodl.ai", ip="198.51.100.9")
    per_sec = mod.TIERS["SITE"]["per_sec"]
    last = None
    for _ in range(per_sec + 1):
        last = mod.authorize(ev, allowed_origins=["https://justhodl.ai"])
    meta, err = last
    assert meta is None and err is not None and err["statusCode"] == 429, err
    # another IP is unaffected
    meta2, err2 = mod.authorize(_event(origin="https://justhodl.ai", ip="198.51.100.10"), allowed_origins=["https://justhodl.ai"])
    assert err2 is None and meta2["tier"] == "SITE"


def test_spoofed_origin_without_key_gets_site_not_enterprise_and_foreign_origin_is_refused():
    mod, _ = _load()
    meta, err = mod.authorize(_event(origin="https://evil.example"), allowed_origins=["https://justhodl.ai"])
    assert meta is None and err["statusCode"] == 401
    meta, err = mod.authorize(_event(origin="https://justhodl.ai"), allowed_origins=["https://justhodl.ai"])
    assert meta["tier"] != "ENTERPRISE"


def test_rate_table_error_does_not_fail_open():
    mod, ddb = _load()
    ddb.raise_errors = True
    ev = _event(origin="https://justhodl.ai", ip="192.0.2.44")
    got_429 = False
    for _ in range(mod.DEGRADED_CAP_PER_MIN + 2):
        meta, err = mod.authorize(ev, allowed_origins=["https://justhodl.ai"])
        if err is not None and err["statusCode"] == 429:
            got_429 = True
            break
    assert got_429, "degraded mode must cap, never return unlimited"


def test_strict_mode_still_requires_key():
    mod, _ = _load()
    meta, err = mod.authorize(_event(origin="https://justhodl.ai"))
    assert meta is None and err["statusCode"] == 401
