"""Read-only post-deployment confidentiality checks; never downloads note text.

Root release orchestration installs the policy before verification. This file
does not mutate IAM, bucket policy, caches, notes, journals or billing records.
"""
import json
import hashlib
import urllib.error
import urllib.request

from private_artifact import MIRRORED_ARTIFACTS, PRIVATE_KEYS as SOURCE_PRIVATE_KEYS, PRIVATE_PREFIXES

MIRRORED_KEYS = tuple(MIRRORED_ARTIFACTS)
PRIVATE_KEYS = tuple(sorted(SOURCE_PRIVATE_KEYS))
SANITIZED_KEYS = ("data/brain-compiler.json", "data/tv-workbench.json", "data/canary-warroom.json", "data/tradingview.json", "data/domain-barometers.json", "data/best-setups.json", "data/master-allocation.json", "data/position-sizing.json", "data/engine-conflicts.json", "data/search/providers/tradingview_vault_live.json.gz", "data/sizing.json", "data/ai-commentary/portfolio.json")
SANITIZED_PREFIXES = ("equity-research/", "etf-flows/history/", "macro/history/")
SANITIZED_KEYS += ("data/vol-regime.json", "data/wealth-plan-snapshot.json", "data/tax-plan-snapshot.json", "_health/fleet.json")
SANITIZED_KEYS += ("data/_fleet-monitor.json", "data/_freshness-monitor.json")
SANITIZED_KEYS += ("data/source-map.json",)
SANITIZED_KEYS += ("etf-flows/daily.json", "macro/regime.json")

WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"


def _policy_json(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def _policy_values(value):
    # IAM collection elements are unordered alternatives. Preserve JSON types;
    # never coerce a condition string, number or Boolean into another type.
    values = value if isinstance(value, list) else [value]
    return [json.loads(item) for item in sorted({_policy_json(item) for item in values})]


def _policy_strings(value):
    values = value if isinstance(value, list) else [value]
    if not values or any(not isinstance(item, str) or not item for item in values):
        raise ValueError("invalid_policy_string_collection")
    return _policy_values(values)


def canonical_statement(statement):
    """Normalize only documented IAM representation alternatives.

    AWS IAM policy grammar permits scalar/singleton arrays, and Principal docs
    permit '*' / {'AWS':'*'}. No wildcard expansion, case folding, removed Sid,
    condition-operator rewriting, or ignored unknown element is allowed.
    https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_grammar.html
    https://docs.aws.amazon.com/IAM/latest/UserGuide/reference_policies_elements_principal.html
    """
    if not isinstance(statement, dict):
        raise ValueError("invalid_policy_statement")
    result = json.loads(_policy_json(statement))
    for key in ("Action", "NotAction", "Resource", "NotResource"):
        if key in result:
            result[key] = _policy_strings(result[key])
    for key in ("Principal", "NotPrincipal"):
        principal = result.get(key)
        if isinstance(principal, dict):
            if not principal:
                raise ValueError("invalid_policy_principal")
            principal = {kind: _policy_strings(values) for kind, values in principal.items()}
            result[key] = "*" if principal == {"AWS": ["*"]} else principal
        elif key in result and principal != "*":
            raise ValueError("invalid_policy_principal")
    if isinstance(result.get("Condition"), dict):
        result["Condition"] = {
            operator: {key: _policy_values(value) for key, value in terms.items()}
            if isinstance(terms, dict) else terms
            for operator, terms in result["Condition"].items()}
    return result


def policy_statements(policy):
    statements = policy.get("Statement", [])
    statements = [statements] if isinstance(statements, dict) else statements
    if not isinstance(statements, list) or any(not isinstance(s, dict) for s in statements):
        raise ValueError("invalid_policy_statements")
    return statements


def canonical_policy(policy):
    if not isinstance(policy, dict):
        raise ValueError("invalid_policy_document")
    result = json.loads(_policy_json(policy))
    # Preserve the statement count and every top-level field, including Id and
    # Version. Only collection order and documented value syntax may differ.
    if "Statement" in policy:
        result["Statement"] = sorted((canonical_statement(s) for s in policy_statements(policy)), key=_policy_json)
    return result


def policies_equal(left, right):
    try:
        return _policy_json(canonical_policy(left)) == _policy_json(canonical_policy(right))
    except (TypeError, ValueError, AttributeError):
        return False


def has_policy_statement(policy, expected):
    try:
        matches = [s for s in policy_statements(policy) if s.get("Sid") == expected.get("Sid")]
        return len(matches) == 1 and _policy_json(canonical_statement(matches[0])) == _policy_json(canonical_statement(expected))
    except (TypeError, ValueError, AttributeError):
        return False


def policy_diagnostic(actual, expected):
    """Safe structural evidence: hashes/counts/fixed field names, never values."""
    rows = []
    fields = ("Sid", "Effect", "Principal", "NotPrincipal", "Action", "NotAction", "Resource", "NotResource", "Condition")
    for wanted in policy_statements(expected):
        # Do not publish unrelated policy identifiers, principals or conditions.
        if not wanted.get("Sid", "").startswith("Audit20260909"):
            continue
        matches = [s for s in policy_statements(actual) if s.get("Sid") == wanted["Sid"]]
        row = {"sid": wanted["Sid"], "observed_matches": len(matches), "equivalent": has_policy_statement(actual, wanted)}
        if len(matches) == 1:
            observed, desired = canonical_statement(matches[0]), canonical_statement(wanted)
            row["different_fields"] = [key for key in fields if _policy_json(observed.get(key)) != _policy_json(desired.get(key))]
            row["unknown_elements_changed"] = {k: v for k, v in observed.items() if k not in fields} != {k: v for k, v in desired.items() if k not in fields}
        rows.append(row)
    return {"equivalent": policies_equal(actual, expected), "raw_equal": actual == expected,
            "expected_sha256": hashlib.sha256(_policy_json(canonical_policy(expected)).encode()).hexdigest(),
            "observed_sha256": hashlib.sha256(_policy_json(canonical_policy(actual)).encode()).hexdigest(),
            "expected_statements": len(policy_statements(expected)), "observed_statements": len(policy_statements(actual)),
            "required_statements": rows}


def anonymous_deny_statement(bucket, owner_account="857687956942"):
    """Merge by Sid into existing policy, preserving unrelated statements.

    Denies both present and historical versions to anonymous AND unrelated AWS
    accounts. Same-account IAM readers retain their existing permission grants.
    """
    return {"Sid": "Audit20260909PrivatePersonalArtifacts", "Effect": "Deny", "Principal": "*",
            "Action": ["s3:GetObject", "s3:GetObjectVersion"],
            "Resource": sorted({f"arn:aws:s3:::{bucket}/{key}" for key in PRIVATE_KEYS} | {f"arn:aws:s3:::{bucket}/{key.removeprefix('data/')}" for key in PRIVATE_KEYS} | {f"arn:aws:s3:::{bucket}/{prefix}*" for prefix in PRIVATE_PREFIXES} | {f"arn:aws:s3:::{bucket}/{prefix.removeprefix('data/')}*" for prefix in PRIVATE_PREFIXES}),
            "Condition": {"StringNotEquals": {"aws:PrincipalAccount": owner_account}}}


def historical_deny_statement(bucket, owner_account="857687956942"):
    """Sanitized current feeds remain public; old versions retain IAM access only."""
    return {"Sid": "Audit20260909PrivateDerivativeHistory", "Effect": "Deny", "Principal": "*",
            "Action": ["s3:GetObjectVersion"],
            "Resource": [f"arn:aws:s3:::{bucket}/{key}" for key in SANITIZED_KEYS]
                        + [f"arn:aws:s3:::{bucket}/{key.removeprefix('data/')}" for key in SANITIZED_KEYS]
                        + [f"arn:aws:s3:::{bucket}/{prefix}*" for prefix in SANITIZED_PREFIXES],
            "Condition": {"StringNotEquals": {"aws:PrincipalAccount": owner_account}}}


def _head(url, extra=None):
    request = urllib.request.Request(url, method="HEAD",
        headers={"User-Agent": "JustHodl-Audit20260909/1.0", "Cache-Control": "no-cache", **(extra or {})})
    try:
        with urllib.request.urlopen(request, timeout=15) as response:
            return {"status": response.status, "cache_control": response.headers.get("Cache-Control", "")}
    except urllib.error.HTTPError as error:
        # Deliberately never read a response body, even if access is unexpectedly granted.
        return {"status": error.code, "cache_control": error.headers.get("Cache-Control", "")}


def check(s3, bucket="justhodl-dashboard-live", service_token=None):
    checks = []
    policy = json.loads(s3.get_bucket_policy(Bucket=bucket)["Policy"])
    statement = anonymous_deny_statement(bucket)
    checks.append({"check": "external_s3_deny_installed", "ok": has_policy_statement(policy, statement)})
    checks.append({"check": "external_history_deny_installed", "ok": has_policy_statement(policy, historical_deny_statement(bucket))})
    for key in PRIVATE_KEYS:
        for base in (WORKER, "https://justhodl.ai", "https://www.justhodl.ai"):
            result = _head(base + "/" + key)
            checks.append({"check": "anonymous_worker_denied", "host": base, "key": key,
                           "status": result["status"], "ok": result["status"] in (401, 403) or (not key.startswith("data/") and base != WORKER and result["status"] == 404)})
        result = _head(f"https://{bucket}.s3.us-east-1.amazonaws.com/{key}")
        checks.append({"check": "anonymous_s3_denied", "key": key, "status": result["status"], "ok": result["status"] == 403})
        # IAM service-read preservation check, metadata only.
        try:
            head = s3.head_object(Bucket=bucket, Key=key)
            checks.append({"check": "iam_source_preserved", "key": key, "ok": head.get("ContentLength", 0) > 0})
        except Exception as error:
            code = str(getattr(error, "response", {}).get("Error", {}).get("Code", ""))
            # Alert/history files may never have existed. Their absence is safe;
            # every full mirrored source is required and every other error fails.
            if key not in MIRRORED_KEYS and code in ("404", "NoSuchKey", "NotFound"):
                checks.append({"check": "optional_private_source_absent", "key": key, "ok": True})
            else:
                checks.append({"check": "iam_source_preserved", "key": key, "ok": False, "reason": "source metadata unavailable"})
        if service_token and key in MIRRORED_KEYS:
            result = _head(WORKER + "/" + key, {"X-JH-Service-Token": service_token})
            checks.append({"check": "authenticated_mirror_ready", "key": key, "status": result["status"],
                           "ok": result["status"] == 200 and "no-store" in result["cache_control"]})
    return {"ok": all(c["ok"] for c in checks), "checks": checks, "private_response_bodies_read": 0}
