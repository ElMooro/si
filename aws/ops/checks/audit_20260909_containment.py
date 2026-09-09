"""Permission-only early containment; no private bodies or producer operations.

This intentionally stops before deployment parity, service configuration, mirror
seeding, original writes, sanitization or cache warming. Ops5230 owns those later
steps and is the only workflow that may remove temporary containment.
"""
import json
import urllib.error
import urllib.request

from audit_20260909_privacy_migration import ACCOUNT, BUCKET, Migration, MigrationError, temporary_statement
from audit_20260909_security import PRIVATE_KEYS, SANITIZED_KEYS, WORKER, anonymous_deny_statement, historical_deny_statement


def head_status(url, http):
    request = urllib.request.Request(url, method="HEAD", headers={
        "User-Agent": "JustHodl-Containment5238/1.0", "Cache-Control": "no-cache"})
    try:
        with http(request, timeout=15) as response:
            return response.status
    except urllib.error.HTTPError as error:
        # Do not read the body, including on unexpected success or redirects.
        status = error.code
        error.close()
        return status


def contain(root, clients, http=urllib.request.urlopen):
    result = {"ok": False, "containment_only": True,
              "private_payloads_read": 0, "original_objects_written": 0,
              "producer_invocations": 0, "service_configuration_changes": 0,
              "temporary_containment_removed": False, "checks": []}
    job = Migration(root, clients, http=http)
    checks = result["checks"]
    try:
        result["step"] = "verify_account"
        if clients["sts"].get_caller_identity().get("Account") != ACCOUNT:
            raise MigrationError("wrong_aws_account")
        result["step"] = "install_containment"
        job.policy(True)
        checks.extend(job.rows)
        result["temporary_containment_retained"] = True
        result["policy_verified"] = True
        result["step"] = "purge_edge_cache"
        try:
            job.purge()
            result["edge_purge_verified"] = True
            checks.append({"check": "edge_cache_purge", "ok": True})
        except Exception as error:
            result["edge_purge_verified"] = False
            checks.append({"check": "edge_cache_purge", "ok": False,
                           "reason": str(error) if isinstance(error, MigrationError) else "cache_purge_failed_details_withheld"})
        # Permission denial does not depend on new Lambda code. Read metadata
        # only; private mirrors can legitimately be unseeded at this stage.
        result["step"] = "verify_anonymous_denial"
        for key in sorted(set(PRIVATE_KEYS) | set(SANITIZED_KEYS)):
            status = head_status(f"https://{BUCKET}.s3.us-east-1.amazonaws.com/{key}", http)
            checks.append({"check": "anonymous_s3_denied", "key": key, "status": status, "ok": status == 403})
        for key in PRIVATE_KEYS:
            status = head_status(WORKER + "/" + key, http)
            checks.append({"check": "anonymous_worker_denied", "key": key, "status": status, "ok": status in (401, 403)})
        result["s3_denial_verified"] = all(c["ok"] for c in checks if c["check"] == "anonymous_s3_denied")
        result["worker_denial_verified"] = all(c["ok"] for c in checks if c["check"] == "anonymous_worker_denied")
        # Verify policy still contains all three controls after the HEAD pass.
        current = json.loads(clients["s3"].get_bucket_policy(Bucket=BUCKET)["Policy"]).get("Statement", [])
        expected = [anonymous_deny_statement(BUCKET, ACCOUNT), historical_deny_statement(BUCKET, ACCOUNT), temporary_statement()]
        result["temporary_containment_retained"] = temporary_statement() in current
        result["policy_verified"] = all(statement in current for statement in expected)
        checks.append({"check": "final_containment_policy", "ok": result["policy_verified"]})
        result["ok"] = result["policy_verified"] and result["s3_denial_verified"] and result["worker_denial_verified"] and result["edge_purge_verified"]
        result["step"] = "contained_awaiting_full_migration" if result["ok"] else "partial_containment_requires_followup"
        result["next_step"] = "Complete exact-code release and ops5230 before restoring sanitized public access."
    except Exception as error:
        result["ok"] = False
        result["failed_step"] = result["step"]
        result["reason"] = str(error) if isinstance(error, MigrationError) else "containment_failed_details_withheld"
        result["error_class"] = type(error).__name__
        result.setdefault("temporary_containment_retained", job.temp_installed)
    return result
