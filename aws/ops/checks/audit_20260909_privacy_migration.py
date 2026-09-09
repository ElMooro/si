"""Reviewed migration primitives. No operation executes at import time.

Reports contain only fixed check names, public object keys, counts, hashes and
AWS status metadata. Never log exception messages, payloads, signed URLs or env.
"""
import ast
import base64
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
    anonymous_deny_statement, historical_deny_statement, check,
)
from public_brain_projection import (
    brief_public, devils_public, notes_public, playbook_public, sanitize_public,
)

BUCKET = "justhodl-dashboard-live"
ACCOUNT = "857687956942"
REGION = "us-east-1"
TOKEN_PARAM = "/justhodl/api-admin/token"
TEMP_SID = "Audit20260909DerivativeMigrationInProgress"
PUBLISHERS = ("brain-sync", "journal-grader", "my-brief", "devils-advocate", "notes-intel", "playbook-engine", "ask",
              "portfolio-snapshot", "portfolio-risk", "portfolio-sizer", "portfolio-catalysts", "risk-sizer",
              "pm-decision", "behavior-mirror", "ai-brief")
PRODUCERS = ("brain-compiler", "tv-workbench", "canary-warroom", "tradingview", "domain-barometers", "sizing-engine",
             "best-setups", "master-allocator", "position-sizer", "engine-conflicts", "equity-research", "provider-catalog")
READINESS = tuple(dict.fromkeys(PUBLISHERS + PRODUCERS + ("ask-desk", "symdir")))
MAX_OBJECT = 200 * 1024 * 1024


class MigrationError(RuntimeError):
    """Only pass a fixed, non-sensitive machine-readable reason."""


def require(ok, reason):
    if not ok:
        raise MigrationError(reason)


def digest(raw):
    return hashlib.sha256(raw).hexdigest()


def encoded(doc):
    return json.dumps(doc, sort_keys=True, separators=(",", ":"), ensure_ascii=False, allow_nan=False).encode()


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


def stable_config(config):
    require(config.get("State") == "Active" and config.get("LastUpdateStatus") == "Successful", "lambda_not_stable")
    require(config.get("RevisionId") and config.get("CodeSha256"), "lambda_revision_unavailable")


def update_environment(lam, function, additions, expected_sha):
    """CAS config update; promote a same-code config version for existing live alias."""
    before = lam.get_function_configuration(FunctionName=function)
    stable_config(before)
    require(before["CodeSha256"] == expected_sha, "lambda_changed_before_config_update")
    alias = get_alias(lam, function)
    if alias:
        require(not (alias.get("RoutingConfig") or {}).get("AdditionalVersionWeights"), "weighted_live_alias_requires_review")
        active = lam.get_function_configuration(FunctionName=function, Qualifier=alias["FunctionVersion"])
        require(active["CodeSha256"] == expected_sha, "live_alias_code_differs_from_verified_latest")
    variables = {**before.get("Environment", {}).get("Variables", {}), **additions}
    lam.update_function_configuration(FunctionName=function, RevisionId=before["RevisionId"], Environment={"Variables": variables})
    # Bounded SDK waiter; no payload or environment is ever emitted.
    lam.get_waiter("function_updated_v2").wait(FunctionName=function, WaiterConfig={"Delay": 3, "MaxAttempts": 100})
    after = lam.get_function_configuration(FunctionName=function)
    stable_config(after)
    require(after["CodeSha256"] == expected_sha and after.get("Environment", {}).get("Variables") == variables, "lambda_config_update_not_verified")
    if alias:
        require(get_alias(lam, function).get("RevisionId") == alias["RevisionId"], "live_alias_changed_during_config_update")
        version = lam.publish_version(FunctionName=function, RevisionId=after["RevisionId"], CodeSha256=expected_sha)
        require(str(version.get("Version", "")).isdigit(), "published_version_missing")
        moved = lam.update_alias(FunctionName=function, Name="live", RevisionId=alias["RevisionId"], FunctionVersion=version["Version"])
        require(moved.get("FunctionVersion") == version["Version"], "config_version_not_promoted")
        return {"version": version["Version"], "code_sha256": expected_sha, "environment_keys": len(variables)}
    return {"version": "$LATEST", "code_sha256": expected_sha, "environment_keys": len(variables)}


class Migration:
    def __init__(self, root, clients, http=urllib.request.urlopen):
        self.root, self.clients, self.http = Path(root), clients, http
        self.rows = []
        self.step = "initialization"
        self.epoch = "privacy-5230-" + uuid.uuid4().hex
        self.ready = {}
        self.temp_installed = False

    def record(self, stage, **metadata):
        self.rows.append({"check": stage, **metadata})

    def policy(self, temporary):
        s3 = self.clients["s3"]
        try:
            current = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        except Exception as exc:
            if error_code(exc) != "NoSuchBucketPolicy":
                raise
            current = {"Version": "2012-10-17", "Statement": []}
        statements = [anonymous_deny_statement(BUCKET, ACCOUNT), historical_deny_statement(BUCKET, ACCOUNT)]
        if temporary:
            statements.append(temporary_statement())
        updated = merge_policy(current, statements, remove=(TEMP_SID,))
        # Detect ordinary concurrent policy changes immediately before mutation.
        try:
            reread = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        except Exception as exc:
            if error_code(exc) != "NoSuchBucketPolicy":
                raise
            reread = {"Version": "2012-10-17", "Statement": []}
        require(reread == current, "bucket_policy_changed_during_merge")
        s3.put_bucket_policy(Bucket=BUCKET, Policy=json.dumps(updated))
        actual = json.loads(s3.get_bucket_policy(Bucket=BUCKET)["Policy"])
        require(actual == updated, "bucket_policy_not_verified")
        self.temp_installed = temporary
        self.record("bucket_policy", private_deny=True, historical_deny=True, temporary_current_deny=temporary)

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
            stable_config(config)
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
            require(actual == policy, "exact_ssm_grant_not_verified")
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
        if key.endswith(".gz"):
            raw = bounded_read(gzip.GzipFile(fileobj=io.BytesIO(raw)))
        doc = json.loads(raw)
        require(isinstance(doc, dict), "source_document_not_object")
        return obj, doc

    def put_public(self, key, doc, previous=None):
        body = encoded(doc)
        if key.endswith(".gz"):
            body = gzip.compress(body, mtime=0)
        args = {"Bucket": BUCKET, "Key": key, "Body": body, "ContentType": "application/json", "CacheControl": "no-cache"}
        if previous:
            require(bool(previous.get("ETag")), "source_etag_missing")
            args["IfMatch"] = previous["ETag"]
            # Retain encryption and operational metadata; do not emit either.
            for field in ("Metadata", "ContentEncoding", "ServerSideEncryption", "SSEKMSKeyId", "BucketKeyEnabled"):
                if field in previous:
                    args[field] = previous[field]
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
                    headers={"X-JH-Service-Token": token, "Content-Type": "application/json"})
                with self.http(req, timeout=30) as response:
                    ack = json.loads(bounded_read(response, 1024))
                    require(response.status == 200 and ack.get("ok") is True, "private_mirror_not_acknowledged")
                head = self.clients["s3"].head_object(Bucket=BUCKET, Key=key)
                if head.get("ETag") == obj.get("ETag"):
                    break
            else:
                raise MigrationError("private_source_changed_during_seeding")
            self.mirror_head(kind, token)
            private[kind] = doc
            self.record("private_mirror_seeded", key=key, bytes=len(raw), sha256=digest(raw))
        return private

    def mirror_head(self, kind, token):
        # KV is eventually consistent across locations; retry HEAD only and never
        # fetch a private response body for verification. 63 seconds total delay.
        for attempt in range(7):
            req = urllib.request.Request(WORKER + "/private-artifact?kind=" + kind, method="HEAD", headers={"X-JH-Service-Token": token})
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

    def scrub(self, key, vault=None, optional=False):
        for attempt in range(3):
            obj, doc = self.read_object(key, optional=optional)
            if doc is None:
                self.record("absent_legacy_alias", key=key)
                return
            projected = sanitize_public(key, doc, vault=vault)
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
            self.scrub(key, vault=vault)
            alias = key.removeprefix("data/")
            if alias != key:
                self.scrub(alias, vault=vault, optional=True)
        count = 0
        for prefix in SANITIZED_PREFIXES:
            for page in self.clients["s3"].get_paginator("list_objects_v2").paginate(Bucket=BUCKET, Prefix=prefix):
                for obj in page.get("Contents", []):
                    if obj["Key"].endswith(".json"):
                        self.scrub(obj["Key"])
                        count += 1
        self.record("equity_research_scrub", count=count)
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
        pinned = self.clients["lambda"].publish_version(FunctionName="justhodl-provider-catalog", RevisionId=cfg["RevisionId"], CodeSha256=cfg["CodeSha256"])
        version = pinned.get("Version")
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
        _, shard = self.read_object("data/search/providers/tradingview_vault_live.json.gz")
        require(encoded(shard) == encoded(sanitize_public("data/search/providers/tradingview_vault_live.json.gz", shard, vault=vault)), "provider_rebuild_contains_nonpublic_search_fields")
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
        self.step = "verify_deployed_code"
        for name in READINESS:
            self.readiness(name)
        self.step = "configure_service_identity"
        parameter = self.clients["ssm"].get_parameter(Name=TOKEN_PARAM, WithDecryption=True)["Parameter"]
        require(parameter.get("Type") == "SecureString" and bool(parameter.get("Value")), "managed_service_token_missing")
        token = parameter["Value"]
        self.configure(token)
        self.step = "seed_private_mirrors"
        private = self.seed(token)
        self.step = "sanitize_current_public_objects"
        self.scrub_all(private)
        del private
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
