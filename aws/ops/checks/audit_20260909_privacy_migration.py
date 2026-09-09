"""Reviewed migration primitives. No operation executes at import time.

Reports contain only fixed check names, public object keys, counts, hashes and
AWS status metadata. Never log exception messages, payloads, signed URLs or env.
"""
import ast
import base64
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import time
import urllib.error
import urllib.request
import uuid
import zipfile

from audit_20260909_security import (
    MIRRORED_ARTIFACTS, MIRRORED_KEYS, SANITIZED_KEYS, SANITIZED_PREFIXES, WORKER,
    anonymous_deny_statement, historical_deny_statement, check, _head as security_head,
    policies_equal, has_policy_statement, policy_diagnostic,
)
from public_brain_projection import (
    brief_public, devils_public, notes_public, playbook_public, sanitize_public,
)
from private_artifact import OWNER_HISTORY_DEFAULTS
from core_layer_reconciliation import layer_package

BUCKET = "justhodl-dashboard-live"
ACCOUNT = "857687956942"
REGION = "us-east-1"
TOKEN_PARAM = "/justhodl/api-admin/token"
TEMP_SID = "Audit20260909DerivativeMigrationInProgress"
BACKUP_PREFIX = "audit-private/20260909-originals/"
BACKUP_SID = "Audit20260909ImmutableOriginalBackups"
PUBLISHERS = ("brain-sync", "journal-grader", "my-brief", "devils-advocate", "notes-intel", "playbook-engine", "ask",
              "portfolio-snapshot", "portfolio-risk", "portfolio-sizer", "portfolio-catalysts", "risk-sizer",
              "pm-decision", "behavior-mirror", "ai-brief", "history-api", "watchlist", "vol-regime", "trade-journal", "ai-brief-router")
PRODUCERS = ("brain-compiler", "tv-workbench", "canary-warroom", "tradingview", "domain-barometers", "sizing-engine",
             "best-setups", "master-allocator", "position-sizer", "engine-conflicts", "equity-research", "provider-catalog",
             "wealth-plan", "tax-plan", "fleet-monitor", "fleet-error-monitor", "fleet-freshness-monitor", "source-map",
             "etf-fund-flows", "macro-regime")
READINESS = tuple(dict.fromkeys(PUBLISHERS + PRODUCERS + ("ask-desk", "symdir", "ai-chat", "page-ai-commentary")))
MAX_OBJECT = 200 * 1024 * 1024
VAULT_SHARD_KEY = "data/search/providers/tradingview-vault-live.json.gz"
LEGACY_VAULT_SHARD_KEY = "data/search/providers/tradingview_vault_live.json.gz"


def project_public(key, document, vault=None):
    # The reviewed projection's identifier predates the producer's slug
    # normalization. Map the actual storage key to that same pure contract;
    # runtime producers already use vault_search_rows directly.
    projection_key = LEGACY_VAULT_SHARD_KEY if key.removeprefix("data/") == VAULT_SHARD_KEY.removeprefix("data/") else key
    return sanitize_public(projection_key, document, vault=vault)


def backup_deny_statement():
    return {"Sid":BACKUP_SID,"Effect":"Deny","Principal":"*",
        "Action":["s3:GetObject","s3:GetObjectVersion"],
        "Resource":[f"arn:aws:s3:::{BUCKET}/{BACKUP_PREFIX}*"],
        "Condition":{"StringNotEquals":{"aws:PrincipalAccount":ACCOUNT}}}


class MigrationError(RuntimeError):
    """Only pass a fixed, non-sensitive machine-readable reason."""


def require(ok, reason):
    if not ok:
        raise MigrationError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(doc):
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


def gzip_object(key, metadata):
    return key.endswith(".gz") or "gzip" in {item.strip() for item in str(metadata.get("ContentEncoding", "")).lower().split(",")}


def verify_core_receipt(root):
    path = Path(root) / "aws/ops/reports/5234_core_layer_reconciliation.json"
    require(path.exists(), "successful_core_layer_receipt_required")
    receipt = json.loads(path.read_text())
    rows = receipt.get("consumers", [])
    require(receipt.get("ok") is True and receipt.get("status") == "VERIFIED_CURRENT_STATE"
            and receipt.get("unresolved_count") == 0 and bool(rows)
            and receipt.get("consumer_count") == len(rows)
            and all(row.get("status") == "VERIFIED_CURRENT_STATE" for row in rows),
            "complete_core_layer_reconciliation_required")
    require(receipt.get("expected_layer_sha256") == layer_package(Path(root))[1],
            "core_layer_receipt_source_package_mismatch")
    return {"consumer_count": len(rows), "desired_layer": receipt.get("desired_layer"),
            "expected_layer_sha256": receipt["expected_layer_sha256"]}


def personal_trade_schema(root):
    # Reuse the reviewed producer's pure schema and computation, without loading
    # its AWS clients, HTTP routes, mark-to-market task or any live function.
    path = root / "aws/lambdas/justhodl-trade-journal/source/lambda_function.py"
    names = {"empty_private_artifacts", "compute_stats"}
    functions = [node for node in ast.parse(path.read_text()).body
                 if isinstance(node, ast.FunctionDef) and node.name in names]
    require({node.name for node in functions} == names, "personal_trade_bootstrap_schema_missing")
    scope = {"datetime": datetime, "timezone": timezone}
    exec(compile(ast.Module(body=functions, type_ignores=[]), str(path), "exec"), scope)
    return scope["empty_private_artifacts"], scope["compute_stats"]


def bounded_read(stream, maximum=MAX_OBJECT):
    try:
        raw = stream.read(maximum + 1)
    finally:
        stream.close()
    require(len(raw) <= maximum, "object_exceeds_reviewed_size_limit")
    return raw


def error_code(exc):
    return getattr(exc, "response", {}).get("Error", {}).get("Code")


def merge_policy(policy, statements, *, remove=()):
    out = dict(policy)
    out.setdefault("Version", "2012-10-17")
    existing = out.get("Statement", [])
    if isinstance(existing, dict):
        existing = [existing]
    sids = {s["Sid"] for s in statements} | set(remove)
    out["Statement"] = [s for s in existing if s.get("Sid") not in sids] + statements
    return out


def temporary_statement():
    statement = historical_deny_statement(BUCKET, ACCOUNT)
    return {**statement, "Sid": TEMP_SID, "Action": ["s3:GetObject", "s3:GetObjectVersion"]}


def desired_members(root, name):
    source = root / "aws/lambdas" / ("justhodl-" + name) / "source"
    members = {str(p.relative_to(source)): p.read_bytes() for p in source.rglob("*")
               if p.is_file() and "__pycache__" not in p.parts and p.suffix != ".pyc"}
    require("lambda_function.py" in members, "local_handler_missing")
    pending = list(members.items())
    while pending:
        member, raw = pending.pop()
        if not member.endswith(".py"):
            continue
        tree = ast.parse(raw)
        for node in ast.walk(tree):
            imported = [n.name.split(".")[0] for n in node.names] if isinstance(node, ast.Import) else (
                [(node.module or "").split(".")[0]] if isinstance(node, ast.ImportFrom) else [])
            for module in imported:
                filename = module + ".py"
                shared = root / "aws/shared" / filename
                if shared.is_file() and filename not in members:
                    members[filename] = shared.read_bytes()
                    pending.append((filename, members[filename]))
    return members


def verify_zip(raw, config, members):
    require(base64.b64encode(hashlib.sha256(raw).digest()).decode() == config.get("CodeSha256"), "downloaded_zip_hash_mismatch")
    with zipfile.ZipFile(io.BytesIO(raw)) as archive:
        names = archive.namelist()
        for member, expected in members.items():
            require(names.count(member) == 1, "deployed_source_member_missing_or_duplicate")
            require(digest(archive.read(member)) == digest(expected), "deployed_source_member_mismatch")
    return {"members": len(members), "source_sha256": digest(encoded({k: digest(v) for k, v in members.items()})),
            "code_sha256": config["CodeSha256"]}


def get_alias(lam, name):
    try:
        return lam.get_alias(FunctionName=name, Name="live")
    except Exception as exc:
        if error_code(exc) == "ResourceNotFoundException":
            return None
        raise


def stable_config(config, *, qualified=False):
    require(config.get("State") == "Active" and
            (config.get("LastUpdateStatus") == "Successful" or qualified and config.get("LastUpdateStatus") is None), "lambda_not_stable")
    require(config.get("RevisionId") and config.get("CodeSha256"), "lambda_revision_unavailable")
    require(not (config.get("Environment") or {}).get("Error")
            and not (config.get("ImageConfigResponse") or {}).get("Error"), "lambda_config_unreadable")


def business_config(config, additions=()):
    """All execution configuration, excluding response/version metadata only."""
    ignored = {"ResponseMetadata", "RevisionId", "LastModified", "Version", "LastUpdateStatus",
               "LastUpdateStatusReason", "LastUpdateStatusReasonCode", "StateReason", "StateReasonCode",
               "RuntimeVersionConfig"}
    result = {key: value for key, value in config.items() if key not in ignored}
    arn = result.get("FunctionArn")
    if isinstance(arn, str) and ":function:" in arn:
        prefix, name = arn.split(":function:", 1)
        result["FunctionArn"] = prefix + ":function:" + name.split(":", 1)[0]
    environment = dict(result.get("Environment") or {})
    environment["Variables"] = {key: value for key, value in environment.get("Variables", {}).items() if key not in additions}
    result["Environment"] = environment
    if isinstance(result.get("SnapStart"), dict):
        result["SnapStart"] = {key: value for key, value in result["SnapStart"].items() if key != "OptimizationStatus"}
    return result


def alias_business(alias):
    return {key: value for key, value in alias.items() if key not in {"RevisionId", "ResponseMetadata"}}


def checked_live_alias(lam, function, expected, *, owned_mutation=False):
    current = get_alias(lam, function)
    require(current is not None and current.get("RevisionId") and alias_business(current) == alias_business(expected)
            and (owned_mutation or current["RevisionId"] == expected.get("RevisionId")), "live_alias_changed_during_config_update")
    return current


def checked_config(lam, function, expected, *, owned_mutation=False):
    current = lam.get_function_configuration(FunctionName=function)
    stable_config(current)
    require(business_config(current) == business_config(expected)
            and (owned_mutation or current["RevisionId"] == expected.get("RevisionId")), "lambda_config_update_not_verified")
    return current


def pin_verified_version(lam, function, expected):
    """Pin exact code/config, reusing an immutable version after a completed retry."""
    checked_config(lam, function, expected)
    try:
        result = lam.publish_version(FunctionName=function, RevisionId=expected['RevisionId'],
                                     CodeSha256=expected['CodeSha256'])
        version = str(result.get('Version', ''))
    except Exception as exc:
        if error_code(exc) != 'ResourceConflictException':
            raise
        checked_config(lam, function, expected)
        candidates = []
        for page in lam.get_paginator('list_versions_by_function').paginate(FunctionName=function):
            candidates.extend(row['Version'] for row in page.get('Versions', [])
                              if str(row.get('Version', '')).isdigit()
                              and row.get('CodeSha256') == expected['CodeSha256'])
        version = ''
        for candidate in sorted(candidates, key=int, reverse=True):
            config = lam.get_function_configuration(FunctionName=function, Qualifier=candidate)
            stable_config(config, qualified=True)
            if business_config(config) == business_config(expected):
                version = candidate
                break
    require(version.isdigit() and int(version)>0, 'exact_numbered_version_unavailable')
    pinned = lam.get_function_configuration(FunctionName=function, Qualifier=version)
    stable_config(pinned, qualified=True)
    require(business_config(pinned) == business_config(expected), 'pinned_version_config_mismatch')
    checked_config(lam, function, expected, owned_mutation=True)
    return version


def update_environment(lam, function, additions, expected_sha):
    """CAS only the intended env values; never promote unrelated staged config."""
    before = lam.get_function_configuration(FunctionName=function)
    stable_config(before)
    require(before["CodeSha256"] == expected_sha, "lambda_changed_before_config_update")
    alias = get_alias(lam, function)
    if alias:
        require(alias.get("RevisionId") and str(alias.get("FunctionVersion", "")).isdigit()
                and int(alias["FunctionVersion"]) > 0, "live_alias_version_unavailable")
        require(not (alias.get("RoutingConfig") or {}).get("AdditionalVersionWeights"), "weighted_live_alias_requires_review")
        active = lam.get_function_configuration(FunctionName=function, Qualifier=alias["FunctionVersion"])
        stable_config(active, qualified=True)
        require(active["CodeSha256"] == expected_sha, "live_alias_code_differs_from_verified_latest")
        require(business_config(active, additions) == business_config(before, additions), "live_latest_config_drift_requires_review")
        checked_live_alias(lam, function, alias)
    variables = {**before.get("Environment", {}).get("Variables", {}), **additions}
    desired = {**before, "Environment": {"Variables": variables}}
    if business_config(before) == business_config(desired) and (not alias or business_config(active) == business_config(desired)):
        # Retry after a completed migration: unchanged code/config cannot always
        # be published again. Verify both pins strictly and leave them in place.
        checked_config(lam, function, before)
        if alias:
            checked_live_alias(lam, function, alias)
        else:
            require(get_alias(lam, function) is None, "live_alias_created_during_config_update")
        return {"version": alias["FunctionVersion"] if alias else "$LATEST", "code_sha256": expected_sha,
                "environment_keys": len(variables), "already_configured": True}
    lam.update_function_configuration(FunctionName=function, RevisionId=before["RevisionId"], Environment={"Variables": variables})
    # Bounded SDK waiter; no payload or environment is ever emitted.
    lam.get_waiter("function_updated_v2").wait(FunctionName=function, WaiterConfig={"Delay": 3, "MaxAttempts": 100})
    after = checked_config(lam, function, desired, owned_mutation=True)
    if alias:
        alias = checked_live_alias(lam, function, alias, owned_mutation=True)
        version = lam.publish_version(FunctionName=function, RevisionId=after["RevisionId"], CodeSha256=expected_sha)
        require(str(version.get("Version", "")).isdigit() and int(version["Version"]) > 0, "published_version_missing")
        # Our publish may advance revision metadata, never code/config/routing.
        after = checked_config(lam, function, after, owned_mutation=True)
        alias = checked_live_alias(lam, function, alias, owned_mutation=True)
        candidate = lam.get_function_configuration(FunctionName=function, Qualifier=version["Version"])
        stable_config(candidate, qualified=True)
        require(candidate.get("Version") == version["Version"] and business_config(candidate) == business_config(after), "published_config_version_not_verified")
        # No owned mutations occurred after these baselines: revisions are now
        # strict too, catching external edits while the candidate was inspected.
        checked_config(lam, function, after)
        checked_live_alias(lam, function, alias)
        moved = lam.update_alias(FunctionName=function, Name="live", RevisionId=alias["RevisionId"], FunctionVersion=version["Version"])
        require(alias_business(moved) == alias_business({**alias, "FunctionVersion": version["Version"]}), "config_version_not_promoted")
        checked_live_alias(lam, function, moved)
        return {"version": version["Version"], "code_sha256": expected_sha, "environment_keys": len(variables)}
    require(get_alias(lam, function) is None, "live_alias_created_during_config_update")
    return {"version": "$LATEST", "code_sha256": expected_sha, "environment_keys": len(variables)}


class Migration:
    def __init__(self, root, clients, http=urllib.request.urlopen, *, on_progress=None):
        self.root, self.clients, self.http = Path(root), clients, http
        self.on_progress = on_progress
        self.rows = []
        self.step = "initialization"
        self.epoch = "privacy-5230-" + uuid.uuid4().hex
        self.ready = {}
        self.temp_installed = None  # Unknown until a policy readback proves it.
        self.policy_write_attempted = False
        self.policy_write_acknowledged = False
        self.policy_expected = None

    def record(self, stage, **metadata):
        self.rows.append({"check": stage, **metadata})
        if self.on_progress is not None:
            self.on_progress(self)

    def verify_backup_access(self, key):
        require(key.startswith(BACKUP_PREFIX), "backup_key_outside_private_prefix")
        for base in (WORKER, "https://justhodl.ai", "https://www.justhodl.ai",
                     f"https://{BUCKET}.s3.{REGION}.amazonaws.com"):
            result = security_head(base + "/" + key)
            denied = result["status"] in (401, 403)
            if base in ("https://justhodl.ai", "https://www.justhodl.ai"):
                denied = denied or result["status"] == 404
            self.record("original_backup_anonymous_denied", host=base, key=key,
                        status=result["status"], ok=denied)
            require(denied, "original_backup_public_access_not_denied")

    def policy(self, temporary):
        s3 = self.clients["s3"]
        try:
            current = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        except Exception as exc:
            if error_code(exc) != "NoSuchBucketPolicy":
                raise
            current = {"Version": "2012-10-17", "Statement": []}
        historical = historical_deny_statement(BUCKET, ACCOUNT)
        statements = [anonymous_deny_statement(BUCKET, ACCOUNT), backup_deny_statement()]
        if temporary:
            statements.append(temporary_statement())
        else:
            statements.append(historical)
        updated = merge_policy(current, statements, remove=(TEMP_SID, historical["Sid"]))
        # Detect ordinary concurrent policy changes immediately before mutation.
        try:
            reread = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        except Exception as exc:
            if error_code(exc) != "NoSuchBucketPolicy":
                raise
            reread = {"Version": "2012-10-17", "Statement": []}
        require(policies_equal(reread, current), "bucket_policy_changed_during_merge")
        policy_text = encoded(updated).decode()
        require(len(policy_text.encode()) <= 20 * 1024, "merged_bucket_policy_exceeds_s3_limit")
        self.policy_expected = updated
        self.policy_write_attempted = True
        self.policy_write_acknowledged = False
        self.temp_installed = None
        s3.put_bucket_policy(Bucket=BUCKET, Policy=policy_text)
        self.policy_write_acknowledged = True
        actual = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        self.temp_installed = has_policy_statement(actual, temporary_statement())
        self.record("bucket_policy_readback", **policy_diagnostic(actual, updated))
        require(policies_equal(actual, updated), "bucket_policy_not_verified")
        self.record("bucket_policy", private_deny=True, historical_deny=True, temporary_current_deny=temporary,
                    encoded_bytes=len(policy_text.encode()), redundant_history_covered_by_temporary=temporary)

    def cf(self, path, payload=None):
        token = os.environ.get("CLOUDFLARE_API_TOKEN")
        require(bool(token), "cloudflare_purge_identity_missing")
        req = urllib.request.Request("https://api.cloudflare.com/client/v4" + path,
            method="POST" if payload is not None else "GET", data=encoded(payload) if payload is not None else None,
            headers={"Authorization": "Bearer " + token, "Content-Type": "application/json"})
        with self.http(req, timeout=30) as response:
            doc = json.loads(bounded_read(response, 1024 * 1024))
        require(doc.get("success") is True, "cloudflare_operation_not_acknowledged")
        return doc.get("result")

    def purge(self):
        zones = self.cf("/zones?name=justhodl.ai")
        zones = [z for z in zones or [] if z.get("name") == "justhodl.ai"]
        require(len(zones) == 1, "cloudflare_zone_not_unique")
        self.cf("/zones/" + zones[0]["id"] + "/purge_cache", {"purge_everything": True})
        self.record("cloudflare_zone_purge", ok=True)

    def readiness(self, name):
        function = "justhodl-" + name
        lam = self.clients["lambda"]
        members = desired_members(self.root, name)
        latest = None
        for qualifier in (None, "live"):
            if qualifier and get_alias(lam, function) is None:
                continue
            result = lam.get_function(FunctionName=function, **({"Qualifier": qualifier} if qualifier else {}))
            config = result["Configuration"]
            stable_config(config, qualified=bool(qualifier))
            with self.http(result["Code"]["Location"], timeout=90) as response:
                raw = bounded_read(response, 250 * 1024 * 1024)
            meta = verify_zip(raw, config, members)
            if qualifier:
                require(config["CodeSha256"] == latest["CodeSha256"], "live_alias_code_differs_from_verified_latest")
                require(not (get_alias(lam, function).get("RoutingConfig") or {}).get("AdditionalVersionWeights"), "weighted_live_alias_requires_review")
            else:
                latest = config
            self.record("exact_deployed_code", function=function, qualifier=qualifier or "$LATEST", **meta)
        self.ready[name] = latest

    def configure(self, token):
        iam, lam = self.clients["iam"], self.clients["lambda"]
        resource = f"arn:aws:ssm:{REGION}:{ACCOUNT}:parameter{TOKEN_PARAM}"
        policy = {"Version": "2012-10-17", "Statement": [{"Sid": "ReadExactPrivateArtifactServiceIdentity", "Effect": "Allow",
                   "Action": ["ssm:GetParameter"], "Resource": resource}]}
        for name in PUBLISHERS:
            function = "justhodl-" + name
            role_arn = self.ready[name]["Role"]
            require(role_arn.startswith(f"arn:aws:iam::{ACCOUNT}:role/"), "publisher_role_outside_owner_account")
            role = role_arn.rsplit("/", 1)[1]
            iam.put_role_policy(RoleName=role, PolicyName="Audit20260909PrivateArtifactIdentity", PolicyDocument=json.dumps(policy))
            actual = iam.get_role_policy(RoleName=role, PolicyName="Audit20260909PrivateArtifactIdentity")["PolicyDocument"]
            require(policies_equal(actual, policy), "exact_ssm_grant_not_verified")
            result = update_environment(lam, function, {"JH_SERVICE_TOKEN": token}, self.ready[name]["CodeSha256"])
            self.record("private_publisher_config", function=function, **result)

    def read_object(self, key, optional=False):
        try:
            obj = self.clients["s3"].get_object(Bucket=BUCKET, Key=key)
        except Exception as exc:
            if optional and error_code(exc) in {"NoSuchKey", "404", "NotFound"}:
                return None, None
            raise
        require(obj.get("ContentLength", 0) <= MAX_OBJECT, "object_exceeds_reviewed_size_limit")
        raw = bounded_read(obj["Body"])
        obj["_audit_raw_bytes"] = raw
        if gzip_object(key, obj):
            raw = bounded_read(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        doc = json.loads(raw)
        require(isinstance(doc, dict), "source_document_not_object")
        return obj, doc

    def preserve_original(self,key,previous):
        raw=previous.get("_audit_raw_bytes")
        require(isinstance(raw,bytes),"original_bytes_unavailable")
        etag=previous.get("ETag")
        require(bool(etag),"original_etag_missing")
        raw_hash=digest(raw)
        backup_key=BACKUP_PREFIX+digest(encoded([key,etag,raw_hash]))+".json"
        snapshot={"schema_version":"private-original-backup.v1","source_key":key,"source_etag":etag,
                  "raw_sha256":raw_hash,"raw_size_bytes":len(raw),"raw_base64":base64.b64encode(raw).decode()}
        expected=encoded(snapshot)
        try:
            self.clients["s3"].put_object(Bucket=BUCKET,Key=backup_key,Body=expected,
                ContentType="application/json",CacheControl="private, no-store",IfNoneMatch="*")
        except Exception as exc:
            if error_code(exc) not in {"PreconditionFailed","412","ConditionalRequestConflict","409"}:raise
        saved=self.clients["s3"].get_object(Bucket=BUCKET,Key=backup_key)
        actual=bounded_read(saved["Body"],MAX_OBJECT*2+4096)
        require(actual==expected,"immutable_original_backup_not_verified")
        self.record("immutable_original_preserved",key=key,backup_key=backup_key,bytes=len(raw),sha256=raw_hash)

    def put_public(self, key, doc, previous=None):
        if previous is None:
            previous,_ = self.read_object(key,optional=True)
        body = encoded(doc)
        if gzip_object(key, previous or {}):
            body = gzip.compress(body, mtime=0)
        args = {"Bucket": BUCKET, "Key": key, "Body": body, "ContentType": "application/json", "CacheControl": "no-cache"}
        if previous:
            self.preserve_original(key,previous)
            require(bool(previous.get("ETag")), "source_etag_missing")
            args["IfMatch"] = previous["ETag"]
            # Retain encryption and operational metadata; do not emit either.
            for field in ("Metadata", "ContentEncoding", "ServerSideEncryption", "SSEKMSKeyId", "BucketKeyEnabled"):
                if field in previous:
                    args[field] = previous[field]
        else:
            args["IfNoneMatch"] = "*"
        self.clients["s3"].put_object(**args)
        _, stored = self.read_object(key)
        require(encoded(stored) == encoded(doc), "public_write_readback_mismatch")
        self.record("public_projection_written", key=key, bytes=len(body), sha256=digest(encoded(doc)))

    def seed(self, token):
        private = {}
        for key, kind in MIRRORED_ARTIFACTS.items():
            for attempt in range(3):
                obj, doc = self.read_object(key)
                raw = encoded(doc)
                require(len(raw) <= 20000000, "private_artifact_exceeds_worker_limit")
                req = urllib.request.Request(WORKER + "/private-artifact?kind=" + kind, method="PUT", data=raw,
                    headers={"User-Agent": "JustHodl-PrivateArtifacts/20260909", "X-JH-Service-Token": token, "Content-Type": "application/json"})
                try:
                    with self.http(req, timeout=30) as response:
                        ack = json.loads(bounded_read(response, 1024))
                        require(response.status == 200 and ack.get("ok") is True, "private_mirror_not_acknowledged")
                except urllib.error.HTTPError as exc:
                    self.record("private_mirror_write_http_failure", key=key, status=exc.code)
                    exc.close()  # Never read or report an error body or request headers.
                    raise MigrationError("private_mirror_write_rejected") from None
                head = self.clients["s3"].head_object(Bucket=BUCKET, Key=key)
                if head.get("ETag") == obj.get("ETag"):
                    break
            else:
                raise MigrationError("private_source_changed_during_seeding")
            self.mirror_head(kind, token)
            private[kind] = doc
            self.record("private_mirror_seeded", key=key, bytes=len(raw), sha256=digest(raw))
        return private

    def bootstrap_private_derivatives(self):
        # Preserve the existing full volatility view before the public current
        # object is narrowed to its fixed model universe. Never overwrite a
        # private object already published by the new producer.
        key = "data/vol-regime-private.json"
        _, existing = self.read_object(key, optional=True)
        if existing is not None:
            self.record("private_derivative_already_present", key=key)
        else:
            _, original = self.read_object("data/vol-regime.json")
            self.create_private_original(key, original)

        # A personal journal can legitimately be absent before the first manual
        # trade. Existing journals/stats always win; never replace a live ledger.
        empty, compute_stats = personal_trade_schema(self.root)
        ledger_key, stats_key = "data/user-trades.json", "data/user-trades-stats.json"
        _, ledger = self.read_object(ledger_key, optional=True)
        _, stats = self.read_object(stats_key, optional=True)
        if ledger is None:
            require(stats is None or stats.get("n_total") == 0, "personal_ledger_missing_with_nonempty_stats")
            ledger = self.create_private_original(ledger_key, empty()["personal-trades"])
        require(isinstance(ledger.get("trades"), list), "personal_ledger_schema_invalid")
        if stats is None:
            self.create_private_original(stats_key, compute_stats(ledger))

        # An empty alert/history file is legitimate before the first event.
        # Preserve every existing row; never infer past activity or overwrite it.
        for key, empty in OWNER_HISTORY_DEFAULTS.items():
            _, existing = self.read_object(key, optional=True)
            if existing is None:
                self.create_private_original(key, empty)

    def create_private_original(self, key, original):
        raw = encoded(original)
        try:
            self.clients["s3"].put_object(Bucket=BUCKET, Key=key, Body=raw, ContentType="application/json",
                                        CacheControl="private, no-store", IfNoneMatch="*")
        except Exception as exc:
            if error_code(exc) not in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
                raise
            _, existing = self.read_object(key)
            require(existing is not None, "private_derivative_creation_race_unresolved")
            raw = encoded(existing)
            original = existing
        else:
            _, stored = self.read_object(key)
            require(encoded(stored) == raw, "private_derivative_bootstrap_mismatch")
        self.record("private_derivative_bootstrapped", key=key, bytes=len(raw), sha256=digest(raw))
        return original

    def mirror_head(self, kind, token):
        # KV is eventually consistent across locations; retry HEAD only and never
        # fetch a private response body for verification. 63 seconds total delay.
        for attempt in range(7):
            req = urllib.request.Request(WORKER + "/private-artifact?kind=" + kind, method="HEAD", headers={"User-Agent": "JustHodl-PrivateArtifacts/20260909", "X-JH-Service-Token": token})
            try:
                with self.http(req, timeout=20) as response:
                    if response.status == 200 and "no-store" in response.headers.get("Cache-Control", ""):
                        return
                    require(response.status in (404, 503), "private_mirror_invalid_access_contract")
            except urllib.error.HTTPError as exc:
                require(exc.code in (404, 503), "private_mirror_invalid_access_contract")
                exc.close()
            if attempt < 6:
                time.sleep(2 ** attempt)
        raise MigrationError("private_mirror_not_readable_after_propagation_window")

    def verify_service_access(self, token):
        # HEAD establishes the deployed Worker's identity boundary before any
        # private body is sent. A cold, authenticated mirror may return 503.
        request = urllib.request.Request(WORKER + "/private-artifact?kind=brain", method="HEAD",
            headers={"User-Agent": "JustHodl-PrivateArtifacts/20260909", "X-JH-Service-Token": token})
        try:
            with self.http(request, timeout=20) as response:
                status, headers = response.status, response.headers
        except urllib.error.HTTPError as exc:
            status, headers = exc.code, exc.headers
            exc.close()
        valid = status in (200, 503) and "no-store" in headers.get("Cache-Control", "")
        self.record("service_identity_head", status=status, ok=valid)
        require(valid, "worker_service_identity_not_verified")

    def scrub(self, key, vault=None, optional=False):
        for attempt in range(3):
            obj, doc = self.read_object(key, optional=optional)
            if doc is None:
                self.record("absent_legacy_alias", key=key)
                return
            projected = project_public(key, doc, vault=vault)
            if encoded(projected) == encoded(doc):
                # A retry must not mint a new LastModified or another backup for
                # an unchanged projection. This also preserves publication age.
                self.record("public_projection_already_safe", key=key, sha256=digest(encoded(doc)))
                return
            try:
                self.put_public(key, projected, obj)
                return
            except Exception as exc:
                if error_code(exc) not in {"PreconditionFailed", "412", "ConditionalRequestConflict", "409"}:
                    raise
        raise MigrationError("public_source_changed_during_scrub")

    def scrub_all(self, private):
        _, vault = self.read_object("data/tradingview.json")
        vault = sanitize_public("data/tradingview.json", vault)
        for key in SANITIZED_KEYS:
            self.scrub(key, vault=vault, optional=key == LEGACY_VAULT_SHARD_KEY)
            alias = key.removeprefix("data/")
            if alias != key:
                self.scrub(alias, vault=vault, optional=True)
        for prefix in SANITIZED_PREFIXES:
            count = 0
            for page in self.clients["s3"].get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        self.scrub(obj["Key"])
                        count += 1
            self.record("public_family_scrub", prefix=prefix, count=count)
        _, setups = self.read_object("data/best-setups.json")
        allowed = {r.get("ticker") for r in setups.get("top_setups", []) if isinstance(r, dict)}
        siblings = {"my-brief": brief_public(private["my-brief"]), "devils-advocate": devils_public(private["devils-advocate"], allowed),
                    "notes-index": notes_public("notes-index", private["notes-index"]), "notes-themes": notes_public("notes-themes", private["notes-themes"]),
                    "playbook-rules": playbook_public(private["playbook-rules"])}
        for kind, doc in siblings.items():
            self.put_public("data/" + kind + "-public.json", doc)

    def invoke(self, name, payload, version):
        response = self.clients["lambda"].invoke(FunctionName="justhodl-" + name, Qualifier=version,
            InvocationType="RequestResponse", Payload=encoded(payload))
        require(response.get("StatusCode") == 200 and not response.get("FunctionError"), "lambda_invocation_failed")
        require(response.get("ExecutedVersion") == version, "lambda_executed_version_mismatch")
        result = json.loads(bounded_read(response["Payload"]))
        require(result.get("statusCode", 200) == 200, "lambda_handler_failed")
        body = json.loads(result["body"]) if isinstance(result.get("body"), str) else result
        require(body.get("ok") is True, "lambda_handler_not_acknowledged")
        return body

    def rebuild_search(self):
        _, old = self.read_object("data/search/provider-shards.json")
        self.readiness("provider-catalog")  # Exact source gate immediately before rebuilding.
        cfg = self.ready["provider-catalog"]
        version = pin_verified_version(self.clients["lambda"], "justhodl-provider-catalog", cfg)
        require(str(version).isdigit(), "provider_rebuild_version_missing")
        started = time.time()
        self.invoke("provider-catalog", {}, version)
        head, new = self.read_object("data/search/provider-shards.json")
        require(head["LastModified"].timestamp() >= started - 1, "provider_manifest_not_refreshed")
        meta = new.get("index") or {}
        require(meta.get("key", "").startswith("data/search/index/") and meta.get("key") != (old.get("index") or {}).get("key"), "provider_index_generation_not_replaced")
        obj = self.clients["s3"].get_object(Bucket=BUCKET, Key=meta["key"])
        require(obj.get("ContentLength") == meta.get("bytes"), "provider_index_size_mismatch")
        sha = hashlib.sha256()
        stream = obj["Body"]
        try:
            for chunk in iter(lambda: stream.read(1024 * 1024), b""):
                sha.update(chunk)
        finally:
            stream.close()
        require(sha.hexdigest() == meta.get("sha256"), "provider_index_digest_mismatch")
        _, vault = self.read_object("data/tradingview.json")
        _, shard = self.read_object(VAULT_SHARD_KEY)
        require(encoded(shard) == encoded(project_public(VAULT_SHARD_KEY, shard, vault=vault)), "provider_rebuild_contains_nonpublic_search_fields")
        self.record("provider_catalog_rebuilt", version=version, index_sha256=sha.hexdigest(), index_bytes=meta["bytes"], documents=new.get("documents"))
        # A unique config creates new environments; force warm also reloads SQLite.
        result = update_environment(self.clients["lambda"], "justhodl-symdir", {"JH_PRIVACY_CACHE_EPOCH": self.epoch}, self.ready["symdir"]["CodeSha256"])
        if result["version"] == "$LATEST":
            config = self.clients["lambda"].get_function_configuration(FunctionName="justhodl-symdir")
            pinned = self.clients["lambda"].publish_version(FunctionName="justhodl-symdir", RevisionId=config["RevisionId"], CodeSha256=config["CodeSha256"])
            result["version"] = pinned["Version"]
        warm = self.invoke("symdir", {"mode": "warm", "queryStringParameters": {"force": "1"}}, result["version"])
        require(warm.get("warehouse_ready") is True and warm.get("reloaded") is True, "symdir_warehouse_cache_not_refreshed")
        self.record("symdir_cache_refreshed", version=result["version"], warehouse_ready=True, epoch=self.epoch)

    def run(self):
        require(self.clients["sts"].get_caller_identity().get("Account") == ACCOUNT, "wrong_aws_account")
        self.step = "install_containment"
        self.policy(True)
        self.purge()
        self.verify_backup_access(BACKUP_PREFIX + "access-probe.json")
        self.step = "verify_deployed_code"
        for name in READINESS:
            self.readiness(name)
        self.step = "configure_service_identity"
        parameter = self.clients["ssm"].get_parameter(Name=TOKEN_PARAM, WithDecryption=True)["Parameter"]
        require(parameter.get("Type") == "SecureString" and bool(parameter.get("Value")), "managed_service_token_missing")
        token = parameter["Value"]
        self.verify_service_access(token)
        self.configure(token)
        self.step = "preserve_private_derivative_originals"
        self.bootstrap_private_derivatives()
        self.step = "seed_private_mirrors"
        private = self.seed(token)
        self.step = "sanitize_current_public_objects"
        self.scrub_all(private)
        del private
        backup = next((row["backup_key"] for row in self.rows
                       if row["check"] == "immutable_original_preserved"), None)
        if backup is not None:
            self.verify_backup_access(backup)
        self.step = "rebuild_search_and_clear_warm_cache"
        self.rebuild_search()
        self.step = "verify_confidentiality"
        result = check(self.clients["s3"], BUCKET, token)
        self.rows.extend(result["checks"])
        require(result["ok"], "private_access_verification_failed")
        self.step = "restore_sanitized_public_access"
        self.policy(False)
        self.purge()
        self.step = "complete"
        return {"ok": True, "epoch": self.epoch, "checks": self.rows, "private_payloads_reported": 0}
