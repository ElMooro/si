"""Publish owner artifacts to authenticated Worker storage, never a public URL.

S3 remains the canonical IAM-readable engine copy. The deployment must deny
anonymous GetObject on those keys; a Cache-Control header is not authorization.
"""
import hmac
import json
import os
import urllib.request

from managed_secret import managed_secret

MIRRORED_ARTIFACTS = {
    "data/brain.json": "brain", "data/brain-history.json": "brain-history",
    "data/brain-constitution.json": "brain-constitution",
    "data/journal-graded.json": "journal-graded", "data/my-brief.json": "my-brief",
    "data/devils-advocate.json": "devils-advocate", "data/notes-index.json": "notes-index",
    "data/notes-themes.json": "notes-themes", "data/playbook-rules.json": "playbook-rules",
    "portfolio/snapshot.json": "portfolio-snapshot", "portfolio/risk.json": "portfolio-risk",
    "portfolio/sizing.json": "portfolio-sizing", "portfolio/catalysts.json": "portfolio-catalysts",
    "data/risk-sizer.json": "risk-sizer", "data/pm-decision.json": "pm-decision",
    "data/pm-decision-history.json": "pm-decision-history", "data/behavior-mirror.json": "behavior-mirror",
    "data/ai-brief.json": "ai-brief",
    "data/user-watchlist.json": "user-watchlist", "data/vol-regime-private.json": "vol-regime-private",
    "data/user-trades.json": "personal-trades", "data/user-trades-stats.json": "personal-trades-stats",
    "portfolio/catalyst-alert-history.json": "portfolio-catalyst-history",
    "portfolio/risk-alert-history.json": "portfolio-risk-history",
    "portfolio/sizing-alert-history.json": "portfolio-sizing-history",
    "data/history/behavior-mirror-history.json": "behavior-mirror-history",
    "data/portfolio-manager-brief.json": "portfolio-manager-brief",
}
# Equivalent output aliases use the canonical mirror, never a second seed that
# could replace a current mirror with an older copy after a partial S3 write.
PRIVATE_ARTIFACT_ALIASES = {"risk/recommendations.json": "data/risk-sizer.json"}
# Exact empty schemas returned by the corresponding producers before any event.
# Migration may create these only with IfNoneMatch, after checking absence.
OWNER_HISTORY_DEFAULTS = {
    "portfolio/catalyst-alert-history.json": {}, "portfolio/risk-alert-history.json": {},
    "portfolio/sizing-alert-history.json": {}, "data/history/behavior-mirror-history.json": {"snapshots": []},
}
PRIVATE_KEYS = frozenset(MIRRORED_ARTIFACTS) | {
    "data/tradingview-notes.json", "risk/recommendations.json", "data/ai-brief.md", "data/_telegram-chat.json",
    "data/history/behavior-mirror-history.json", "portfolio/sizing-alert-history.json",
    "portfolio/catalyst-alert-history.json", "portfolio/risk-alert-history.json",
    "portfolio/holdings.json", "portfolio/pm-history.json",
    "data/history/_fleet-monitor-history.jsonl",
    "data/tv-sources.json",
}
PRIVATE_PREFIXES = ("data/_askdesk/", "data/search/index/", "equity-research-history/",
                    "backtest/ledger/", "data/ai-commentary/history/portfolio/") + tuple(
    "history/archive/feed/" + key + "/"
    for key in sorted(set(PRIVATE_KEYS) | {value.removeprefix("data/") for value in PRIVATE_KEYS})
)


def is_private_source(key):
    """Classify canonical IAM keys and legacy /data aliases before any read."""
    if not isinstance(key, str) or not key or ".." in key or "%" in key or "\\" in key:
        return True
    normalized = key.lstrip("/").removeprefix("data/")
    keys = {value.removeprefix("data/") for value in PRIVATE_KEYS}
    prefixes = tuple(value.removeprefix("data/") for value in PRIVATE_PREFIXES)
    if normalized in keys or normalized.startswith(prefixes):
        return True
    archive = "history/archive/feed/"
    if normalized.startswith(archive):
        archived_key = normalized[len(archive):].removeprefix("data/")
        return any(archived_key == value or archived_key.startswith(value + "/") for value in keys)
    return False


def public_source_allowed(key):
    return (isinstance(key, str) and key.startswith("data/") and ".." not in key
            and not is_private_source(key))


def service_headers():
    token = managed_secret(("JH_SERVICE_TOKEN",), ("/justhodl/api-admin/token",))
    if not token:
        raise RuntimeError("private artifact service identity unavailable")
    return {"User-Agent": "JustHodl-PrivateArtifacts/20260909", "X-JH-Service-Token": token}


def publish_private(kind, document):
    if kind not in MIRRORED_ARTIFACTS.values():
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


def private_http_denied(event):
    """Return a fixed HTTP denial or None. IAM/EventBridge invokes stay trusted.

    Function URLs wrap input in requestContext/headers; body contents never
    change that envelope. Check this before reading a private account source.
    """
    if not isinstance(event, dict) or not any(key in event for key in ("requestContext", "headers", "httpMethod")):
        return None
    raw_headers = event.get("headers") or {}
    headers = {str(k).lower(): str(v) for k, v in raw_headers.items()} if isinstance(raw_headers, dict) else {}
    provided = headers.get("x-jh-service-token", "")
    try:
        expected = service_headers()["X-JH-Service-Token"] if provided else ""
    except Exception:
        expected = ""
    if provided and expected and hmac.compare_digest(provided, expected):
        return None
    return {"statusCode": 401, "headers": {"Content-Type": "application/json", "Cache-Control": "private, no-store"},
            "body": json.dumps({"error": "private account service authentication required"})}
