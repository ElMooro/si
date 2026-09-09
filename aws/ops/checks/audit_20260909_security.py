"""Read-only post-deployment confidentiality checks; never downloads note text.

Root release orchestration installs the policy before verification. This file
does not mutate IAM, bucket policy, caches, notes, journals or billing records.
"""
import json
import urllib.error
import urllib.request

from private_artifact import MIRRORED_ARTIFACTS, PRIVATE_KEYS as SOURCE_PRIVATE_KEYS, PRIVATE_PREFIXES

MIRRORED_KEYS = tuple(MIRRORED_ARTIFACTS)
PRIVATE_KEYS = tuple(sorted(SOURCE_PRIVATE_KEYS))
SANITIZED_KEYS = ("data/brain-compiler.json", "data/tv-workbench.json", "data/canary-warroom.json", "data/tradingview.json", "data/domain-barometers.json", "data/best-setups.json", "data/master-allocation.json", "data/position-sizing.json", "data/engine-conflicts.json", "data/search/providers/tradingview_vault_live.json.gz", "data/sizing.json", "data/ai-commentary/portfolio.json")
SANITIZED_PREFIXES = ("equity-research/",)
SANITIZED_KEYS += ("data/vol-regime.json", "data/wealth-plan-snapshot.json", "data/tax-plan-snapshot.json")

WORKER = "https://justhodl-data-proxy.raafouis.workers.dev"


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
    checks.append({"check": "external_s3_deny_installed", "ok": statement in policy.get("Statement", [])})
    checks.append({"check": "external_history_deny_installed", "ok": historical_deny_statement(bucket) in policy.get("Statement", [])})
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
