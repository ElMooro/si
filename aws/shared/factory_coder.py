"""Student may propose code. It may not ship code.
Allowed write prefix: factory/queue/ only (text patch + test).
Forbidden: aws/, .github/, cloudflare/, IAM, EventBridge, secrets.
"""
from __future__ import annotations
import re

ALLOWED_PREFIX = "factory/queue/"
FORBIDDEN = ("aws/", ".github/", "cloudflare/", "aws/lambdas", "id_rsa", "ghp_", "AKIA")


def admit_patch(path, body, tests):
    p = str(path or "").replace("\\", "/").lstrip("/")
    if not p.startswith(ALLOWED_PREFIX) or ".." in p:
        return False, "path_not_queue"
    if not p.endswith(".py") and not p.endswith(".md"):
        return False, "path_type"
    text = str(body or "")
    if len(text) > 80_000:
        return False, "too_large"
    low = text.lower()
    if any(tok.lower() in low for tok in FORBIDDEN):
        return False, "forbidden_tree"
    if re.search(r"(boto3\\.client\\(['\"]iam|events\\.PutRule|sagemaker)", text):
        return False, "forbidden_api"
    if not str(tests or "").strip():
        return False, "tests_required"
    return True, {
        "schema_version": "factory-queue-patch.v1",
        "path": p,
        "bytes": len(text.encode()),
        "status": "awaiting_owner_release",
        "ship": False,
    }


def execute_task(task, payload=None):
    payload = payload if isinstance(payload, dict) else {}
    kind = str(task or "").lower()
    if kind in ("propose_patch", "code", "patch"):
        ok, detail = admit_patch(payload.get("path"), payload.get("body"), payload.get("tests"))
        return {"ok": ok, "task": "propose_patch", "detail": detail,
                "note": "Queued only. Pages/lambdas change when Khalid or Claude ships."}
    return {"ok": False, "error": "unknown_task", "allowed": ["propose_patch"]}
