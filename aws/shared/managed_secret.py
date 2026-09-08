"""managed_secret -- resolve a provider credential from managed configuration only.

audit 2026-09-08 INST-06: several engines carried a literal key as the
``os.environ.get(NAME, "<literal>")`` fallback. In a public repository that is
a published credential. This helper keeps the same call shape but the fallback
is an SSM SecureString, resolved once per container, never a string in source.

    from managed_secret import managed_secret
    FMP_KEY = managed_secret(("FMP_KEY", "FMP_API_KEY"), ("/justhodl/fmp/api-key",))

Resolution order: first non-empty environment variable in ``env_names``, then
the first SSM parameter in ``ssm_names`` that exists. Returns "" when nothing is
configured so the engine degrades the way it always did on a missing key.
Canonical parameter names are written by ops_5219_audit_containment.py.
"""
from __future__ import annotations

import os
from typing import Iterable

_cache: dict = {}


def managed_secret(env_names: Iterable[str], ssm_names: Iterable[str] = (), region: str = "us-east-1") -> str:
    for name in env_names:
        v = os.environ.get(name, "")
        if v:
            return v
    for pname in ssm_names:
        if pname in _cache:
            if _cache[pname]:
                return _cache[pname]
            continue
        try:
            import boto3  # local import: keeps the module importable in tests without boto3
            v = boto3.client("ssm", region_name=region).get_parameter(Name=pname, WithDecryption=True)["Parameter"]["Value"]
        except Exception as e:  # ParameterNotFound, AccessDenied, no network in tests
            print("[managed_secret] %s unavailable: %s" % (pname, str(e)[:80]))
            v = ""
        _cache[pname] = v or ""
        if v:
            return v
    return ""
