"""Publish owner artifacts to authenticated Worker storage, never a public URL.

S3 remains the canonical IAM-readable engine copy. The deployment must deny
anonymous GetObject on those keys; a Cache-Control header is not authorization.
"""
import json
import os
import urllib.request

from managed_secret import managed_secret

PRIVATE_KEYS = frozenset({"data/brain.json", "data/brain-history.json", "data/journal-graded.json", "data/my-brief.json", "data/devils-advocate.json", "data/notes-index.json", "data/notes-themes.json", "data/playbook-rules.json", "data/tradingview-notes.json"})
PRIVATE_PREFIXES = ("data/_askdesk/", "data/search/index/", "equity-research-history/")


def public_source_allowed(key):
    return (isinstance(key, str) and key.startswith("data/") and ".." not in key
            and key not in PRIVATE_KEYS and not key.startswith(PRIVATE_PREFIXES))


def service_headers():
    token = managed_secret(("JH_SERVICE_TOKEN",), ("/justhodl/api-admin/token",))
    if not token:
        raise RuntimeError("private artifact service identity unavailable")
    return {"User-Agent": "JustHodl-PrivateArtifacts/20260909", "X-JH-Service-Token": token}


def publish_private(kind, document):
    if kind not in {"brain", "brain-history", "journal-graded", "my-brief", "devils-advocate", "notes-index", "notes-themes", "playbook-rules"}:
        raise ValueError("unknown private artifact")
    base = os.environ.get("PRIVATE_ARTIFACT_PROXY", "https://justhodl-data-proxy.raafouis.workers.dev").rstrip("/")
    request = urllib.request.Request(base + "/private-artifact?kind=" + kind,
        data=json.dumps(document, default=str).encode(), method="PUT",
        headers={**service_headers(), "Content-Type": "application/json"})
    with urllib.request.urlopen(request, timeout=25) as response:
        if response.status != 200:
            raise RuntimeError("private artifact publication failed")
        acknowledgement = json.loads(response.read())
        if acknowledgement.get("ok") is not True:
            raise RuntimeError("private artifact publication not acknowledged")
