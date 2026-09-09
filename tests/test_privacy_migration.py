"""Offline migration canaries: no AWS or private data access."""
import base64
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
import types
import tempfile
import urllib.error
import unittest
from unittest.mock import patch
import zipfile

ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT / "aws/shared"), str(ROOT / "aws/ops/checks")]
import audit_20260909_privacy_migration as migration
from public_brain_projection import PUBLIC_DEFAULT_SCENARIO, sanitize_public, vault_search_rows

MARKER = "SYNTHETIC_PRIVATE_PROSE_5230"


def object_error(code):
    error = RuntimeError(MARKER)
    error.response = {"Error": {"Code": code}}
    return error


class MemoryS3:
    def __init__(self, docs):
        self.docs = deepcopy(docs)
        self.writes = []
        self.fail_once = False

    def raw(self, key):
        raw = migration.encoded(self.docs[key])
        return gzip.compress(raw, mtime=0) if key.endswith(".gz") else raw

    def get_object(self, Bucket, Key):
        if Key not in self.docs:
            raise object_error("NoSuchKey")
        raw = self.raw(Key)
        return {"Body": io.BytesIO(raw), "ETag": migration.digest(raw), "ContentLength": len(raw),
                "Metadata": {"operational-tag": "retain"}, "LastModified": datetime.now(timezone.utc)}

    def put_object(self, **args):
        if args.get("IfNoneMatch") == "*" and args["Key"] in self.docs:
            raise object_error("PreconditionFailed")
        if self.fail_once and args.get("IfMatch"):
            self.fail_once = False
            self.docs[args["Key"]]["price"] = 234
            raise object_error("PreconditionFailed")
        if args.get("IfMatch") and args["IfMatch"] != migration.digest(self.raw(args["Key"])):
            raise object_error("PreconditionFailed")
        self.writes.append(args)
        raw = gzip.decompress(args["Body"]) if args["Key"].endswith(".gz") else args["Body"]
        self.docs[args["Key"]] = json.loads(raw)


class FakeLambda:
    def __init__(self, live=True):
        self.config = {"State": "Active", "LastUpdateStatus": "Successful", "RevisionId": "r1", "CodeSha256": "reviewed",
                       "Environment": {"Variables": {"UNCHANGED": "preserved", "AUTH_MODE": "owner"}},
                       "Role": "arn:aws:iam::857687956942:role/example", "FunctionName": "justhodl-fixture",
                       "FunctionArn": "arn:aws:lambda:us-east-1:857687956942:function:justhodl-fixture",
                       "Version": "$LATEST", "Runtime": "python3.12", "Handler": "lambda_function.lambda_handler",
                       "Timeout": 30, "MemorySize": 256, "Layers": [{"Arn": "arn:layer:core:1"}],
                       "LastModified": "latest-response-time", "SnapStart": {"ApplyOn": "PublishedVersions", "OptimizationStatus": "Off"},
                       "RuntimeVersionConfig": {"RuntimeVersionArn": "latest-runtime-patch"}}
        self.alias = {"RevisionId": "a1", "FunctionVersion": "3", "Name": "live", "Description": "Reviewed production", "RoutingConfig": {}} if live else None
        self.versions = {"3": self.numbered("3")}
        self.calls = []
        self.race = False

    def numbered(self, version):
        config = deepcopy(self.config)
        config.update(Version=version, FunctionArn=config["FunctionArn"]+":"+version,
                      RevisionId="version-"+version, LastModified="numbered-response-time")
        config.pop("LastUpdateStatus", None)
        config["SnapStart"]["OptimizationStatus"] = "On"
        config["RuntimeVersionConfig"] = {"RuntimeVersionArn": "numbered-runtime-patch"}
        return config

    def get_function_configuration(self, **kw):
        self.calls.append(("read", kw))
        config = deepcopy(self.versions[kw["Qualifier"]] if kw.get("Qualifier") else self.config)
        config["ResponseMetadata"] = {"RequestId": "read-"+str(len(self.calls))}
        return config

    def get_alias(self, **kw):
        if self.alias is None:
            raise object_error("ResourceNotFoundException")
        return {**deepcopy(self.alias), "ResponseMetadata": {"RequestId": "alias-"+str(len(self.calls))}}

    def update_function_configuration(self, **kw):
        self.calls.append(("update", kw))
        assert kw["RevisionId"] == "r1"
        self.config["Environment"] = deepcopy(kw["Environment"])
        self.config["RevisionId"] = "r2"
        if self.alias: self.alias["RevisionId"] = "own-config-alias-revision"
        if self.race:
            self.alias["FunctionVersion"] = "5"

    def get_waiter(self, name):
        return types.SimpleNamespace(wait=lambda **kw: None)

    def publish_version(self, **kw):
        self.calls.append(("publish", kw))
        assert kw["RevisionId"] == "r2" and kw["CodeSha256"] == "reviewed"
        self.versions["4"] = self.numbered("4")
        self.config["RevisionId"] = "r3"
        self.alias["RevisionId"] = "own-publish-alias-revision"
        return {"Version": "4"}

    def update_alias(self, **kw):
        self.calls.append(("alias", kw))
        assert kw["RevisionId"] == self.alias["RevisionId"] == "own-publish-alias-revision"
        self.alias.update(FunctionVersion=kw["FunctionVersion"], RevisionId="promoted-alias-revision")
        return deepcopy(self.alias)


class PublicMigrationTests(unittest.TestCase):
    def test_temporary_policy_deduplicates_history_without_weakening_protection(self):
        from audit_20260909_security import historical_protection_installed
        class PolicyStore:
            def __init__(self):
                self.policy={'Version':'2012-10-17','Statement':[migration.historical_deny_statement(migration.BUCKET)]}
            def get_bucket_policy(self,**kwargs):return {'Policy':json.dumps(self.policy)}
            def put_bucket_policy(self,**kwargs):self.policy=json.loads(kwargs['Policy'])
        store=PolicyStore();job=migration.Migration(ROOT,{'s3':store})
        job.policy(True)
        historical=migration.historical_deny_statement(migration.BUCKET)
        self.assertFalse(migration.has_policy_statement(store.policy,historical))
        self.assertTrue(historical_protection_installed(store.policy,migration.BUCKET))
        self.assertLess(len(migration.encoded(store.policy)),len(migration.encoded({**store.policy,'Statement':store.policy['Statement']+[historical]})))
        weakened=deepcopy(store.policy)
        next(row for row in weakened['Statement'] if row['Sid']==migration.TEMP_SID)['Action']=['s3:GetObject']
        self.assertFalse(historical_protection_installed(weakened,migration.BUCKET))
        job.policy(False)
        self.assertTrue(migration.has_policy_statement(store.policy,historical))
        self.assertTrue(migration.has_policy_statement(store.policy,migration.backup_deny_statement()))
        self.assertFalse(migration.has_policy_statement(store.policy,migration.temporary_statement()))

    def test_core_receipt_requires_complete_inventory_and_matching_layer_package(self):
        with tempfile.TemporaryDirectory() as folder:
            root=Path(folder);path=root/'aws/ops/reports/5234_core_layer_reconciliation.json'
            with self.assertRaisesRegex(migration.MigrationError,'successful_core_layer_receipt_required'):
                migration.verify_core_receipt(root)
            path.parent.mkdir(parents=True)
            doc={'ok':True,'status':'VERIFIED_CURRENT_STATE','unresolved_count':0,'consumer_count':1,
                 'consumers':[{'status':'VERIFIED_CURRENT_STATE'}],'expected_layer_sha256':'reviewed'}
            path.write_text(json.dumps(doc))
            with patch.object(migration,'layer_package',return_value=(b'', 'reviewed')):
                self.assertEqual(migration.verify_core_receipt(root)['consumer_count'],1)
                path.write_text(json.dumps({**doc,'consumer_count':2}))
                with self.assertRaisesRegex(migration.MigrationError,'complete_core_layer'):
                    migration.verify_core_receipt(root)
                path.write_text(json.dumps({**doc,'expected_layer_sha256':'different'}))
                with self.assertRaisesRegex(migration.MigrationError,'source_package_mismatch'):
                    migration.verify_core_receipt(root)

    def fixtures(self):
        return {
            "etf-flows/daily.json": {"generated_at": "2026-09-09T00:00:00Z", "metrics": [{"ticker": "SPY", "error": MARKER, "body": MARKER, "nav": 0}, {"ticker": "QQQ", "daily_flow_usd": 0}]},
            "macro/regime.json": {"generated_at": "2026-09-09T00:00:00Z", "asset_metrics": [{"ticker": "SPY", "error": MARKER, "headers": {"Authorization": MARKER}, "latest_close": None}]},
            "data/source-map.json": {"new_sources": [{"source": MARKER, "examples": [MARKER]}],
                                      "economics_agencies": [{"source": MARKER, "n_symbols": 1}]},
            "data/_fleet-monitor.json": {"version": "1.0.0", "n_lambdas_scanned": 10, "n_alerts_raised": 1,
                                          "dlq_status": {"error": MARKER}, "alerts": [{"lambda": "fixture", "severity": "WARNING",
                                          "invocations": 10, "errors": 2, "error_rate_pct": 20, "last_error_log": MARKER}]},
            "_health/fleet.json": {"engine": "fleet-monitor", "schema_version": "1.0", "summary": {"lambda_count": 12},
                                   "data_outputs": {"available": True, "degraded": [{"output": "fixture", "age_hours": 1, "issue": MARKER}]},
                                   "compute": {"available": False, "error": MARKER}, "dependencies": [{"name": "FRED", "status": "yellow", "detail": MARKER}]},
            "data/_freshness-monitor.json": {"version": "2.1.0", "status": "UNKNOWN", "reason": MARKER, "results": [{"key": "portfolio/snapshot.json", "reason": MARKER}]},
            "data/brain-compiler.json": {"claims": [{"claim": MARKER, "note_id": "n1", "concepts": ["Rates"], "score": 3}],
                                         "build_queue": [{"concept": "Rates", "sample_claims": [MARKER], "n_claims": 1}]},
            "data/tv-workbench.json": {"symbols": {"NVDA": {"value": 120, "notes": [{"text": MARKER, "note_id": "n1", "ts": 123}]}}},
            "data/canary-warroom.json": {"barometer": {"score": 61}, "brain_playbook": [{"text": MARKER, "note_id": "n1", "pinned": True}]},
            "data/tradingview.json": {"symbols": [{"symbol": "NVDA", "status": "LIVE", "value": 123, "note_snippet": MARKER}]},
            "data/domain-barometers.json": {"symbols": [{"symbol": "NVDA", "tier": "T2", "value": 123, "evidence": MARKER, "note_snippet": MARKER}]},
            "data/best-setups.json": {"top_setups": [{"ticker": "NVDA", "price": 123, "brain_aligned": MARKER, "khalid_note": {"view": MARKER, "n": 3}}],
                                      "brain_aligned": [{"ticker": "NVDA", "brain_aligned": MARKER}], "playbook_context": [{"text": MARKER, "id": "rule1", "score": 2}]},
            "data/master-allocation.json": {"signals_used": {"brain_posture": {"value": 0.4, "intensity": 0.4, "regime": MARKER}}, "target_allocation": {"cash": 8}},
            "data/position-sizing.json": {"risk_posture": MARKER, "posture_mult": 1.3, "regime": {"bond_vol": "NORMAL", "plumbing": "AMPLE", "combined_mult": 1.0},
                                          "sized_positions": [{"ticker": "NVDA", "conviction": 71, "suggested_size_pct": 2.8, "rationale": MARKER}]},
            "data/engine-conflicts.json": {"conflicts": [{"ticker": "NVDA", "type": "CONVICTION vs YOUR RULES", "bear": MARKER}], "n_conflicts": 1},
            "data/sizing.json": {"holdings": [{"ticker": MARKER, "qty": 20, "weight": 3}], "book_status": MARKER,
                                 "recommendations": [{"ticker": "NVDA", "baseline_px": 123, "final_w_pct": 2,
                                                      "overlap_flags": ["book:" + MARKER + " ρ0.9", "SPY ρ0.8"]}]},
            "data/ai-commentary/portfolio.json": {"page": "portfolio", "generated_at": "2026-09-09T00:00:00Z",
                                                  "commentary": {"headline": MARKER}, "preserved_from": MARKER},
            "data/vol-regime.json": {"composite_score": 25, "composite_regime": "NORMAL", "n_tickers": 2,
                                     "tickers": [{"ticker": "SPY", "regime": "NORMAL", "iv_atm_30d": 20},
                                                 {"ticker": MARKER, "regime": "PANIC", "iv_atm_30d": 60}],
                                     "most_stressed": [{"ticker": MARKER}]},
            "data/wealth-plan-snapshot.json": {"inputs": {"age": 42, "liquid_assets": 1234567, "name": MARKER},
                                                "goal": MARKER, "recommendations": [MARKER]},
            "data/tax-plan-snapshot.json": {"profile": {"income": 765432, "filing_status": MARKER},
                                             "suggestions": [MARKER]},
            "data/search/providers/tradingview_vault_live.json.gz": {"rows": [["tradingview-vault-live:NVDA", "NVDA", "instrument_ref", MARKER, 123, 2, True]], "count": 1},
            "data/search/providers/tradingview-vault-live.json.gz": {"rows": [["tradingview-vault-live:NVDA", "NVDA", "instrument_ref", MARKER, 123, 2, True]], "count": 1},
            "equity-research/NVDA.json": {"price": 123, "khalid_notes": {"n_notes": 3, "levels": [120, 140], "note_ids": ["n1"], "latest_note": MARKER, "llm_view": MARKER}},
        }

    def test_all_registered_legacy_outputs_and_aliases_remove_private_prose_idempotently(self):
        fixtures = self.fixtures()
        self.assertEqual(set(migration.SANITIZED_KEYS), set(fixtures) - {"equity-research/NVDA.json"})
        vault = fixtures["data/tradingview.json"]
        for key, doc in fixtures.items():
            before = deepcopy(doc)
            projected = migration.project_public(key, doc, vault=vault)
            self.assertNotIn(MARKER, json.dumps(projected), key)
            self.assertEqual(doc, before, key)
            self.assertEqual(projected, migration.project_public(key, projected, vault=vault), key)
            if key.startswith("data/"):
                self.assertEqual(projected, migration.project_public(key.removeprefix("data/"), doc, vault=vault), key)
        self.assertEqual(migration.project_public("equity-research/NVDA.json", fixtures["equity-research/NVDA.json"])["khalid_notes"]["levels"], [120, 140])
        self.assertEqual(migration.project_public("data/position-sizing.json", fixtures["data/position-sizing.json"])["sized_positions"][0]["suggested_size_pct"], 2.8)

    def test_policy_merge_preserves_unrelated_access_and_uses_exact_parent_denies(self):
        original = {"Version": "2012-10-17", "Statement": [{"Sid": "ExistingPublic", "Effect": "Allow", "Resource": "unchanged"}]}
        statements = [migration.anonymous_deny_statement(migration.BUCKET), migration.historical_deny_statement(migration.BUCKET), migration.temporary_statement()]
        result = migration.merge_policy(original, statements)
        self.assertEqual(result["Statement"][0], original["Statement"][0])
        self.assertEqual(migration.merge_policy(result, statements), result)
        restored = migration.merge_policy(result, statements[:2], remove=(migration.TEMP_SID,))
        self.assertEqual(restored["Statement"], result["Statement"][:-1])
        self.assertEqual(statements[0]["Condition"], {"StringNotEquals": {"aws:PrincipalAccount": migration.ACCOUNT}})
        self.assertIn("s3:GetObjectVersion", statements[1]["Action"])

    def test_source_map_marker_never_allows_raw_metadata_or_untyped_values(self):
        from public_brain_projection import SOURCE_MAP_PUBLICATION
        source = {"schema_version":"public-source-map.v1", "engine":"justhodl-source-map", "publication":dict(SOURCE_MAP_PUBLICATION),
                  "generated_at":MARKER, "symbols_with_source":0, "distinct_sources":MARKER, "raw_diag":MARKER,
                  "known_families":{"FRED":2, MARKER:5}, "economics_agencies":[{"source_family":{},"n_symbols":1}],
                  "cleaned_sources":{"FRED:DGS10":{"source_family":"FRED","source":MARKER,"updated":MARKER},
                                     MARKER:{"source_family":"FRED"},"FRED:INVALID":{"source_family":{}}},
                  "harvest_progress":{"walked":0,"total":True,"note":MARKER}, "errors":[MARKER]}
        projected = sanitize_public("data/source-map.json",source)
        self.assertNotIn(MARKER,json.dumps(projected));self.assertEqual(projected["symbols_with_source"],0)
        self.assertEqual(projected["known_families"],{"FRED":2});self.assertEqual(set(projected["cleaned_sources"]),{"FRED:DGS10"})
        self.assertIsNone(projected["harvest_progress"]["total"])
        self.assertEqual(projected,sanitize_public("data/source-map.json",projected))
        source["publication"]["raw_source_text_private"] = 1
        self.assertIn("LEGACY_PUBLIC_SOURCE_MAP_WITHHELD",sanitize_public("data/source-map.json",source)["errors"])

    def test_sizing_legacy_is_blocked_and_validated_future_book_contract_is_preserved(self):
        legacy = self.fixtures()["data/sizing.json"]
        clean = sanitize_public("data/sizing.json", legacy)
        self.assertIsNone(clean["holdings"])
        self.assertEqual(clean["book_status"], "BLOCKED")
        self.assertFalse(clean["execution_eligible"])
        self.assertTrue(clean["historical_book_context_removed"])
        self.assertEqual(clean["recommendations"][0]["overlap_flags"], ["SPY ρ0.8"])
        self.assertEqual(clean["recommendations"][0]["final_w_pct"], 2)
        future = {"holdings": None, "holdings_publication": "REDACTED_ACCOUNT_PRIVATE", "book_status": "READY",
                  "execution_eligible": False, "recommendations": [{"final_w_pct": 2,
                  "overlap_flags": ["correlated existing account exposure"]}]}
        self.assertEqual(sanitize_public("data/sizing.json", future), future)

    def test_calculator_legacy_and_untrusted_markers_drop_whole_private_scenario(self):
        for key in ("data/wealth-plan-snapshot.json", "data/tax-plan-snapshot.json"):
            legacy = self.fixtures()[key]
            placeholder = sanitize_public(key, legacy)
            self.assertEqual(placeholder["status"], "PUBLIC_MODEL_UNAVAILABLE")
            self.assertFalse(placeholder["available"])
            self.assertFalse({"inputs", "profile", "goal", "recommendations", "suggestions"} & set(placeholder))
            for marker in (None, {}, "PUBLIC_DEFAULT_MODEL",
                           {**PUBLIC_DEFAULT_SCENARIO, "contains_caller_inputs": 0},
                           {**PUBLIC_DEFAULT_SCENARIO, "contains_caller_inputs": True},
                           {**PUBLIC_DEFAULT_SCENARIO, "schema_version": "old"},
                           {**PUBLIC_DEFAULT_SCENARIO, "scope": "CALLER_SCENARIO"},
                           {**PUBLIC_DEFAULT_SCENARIO, "extra": MARKER}):
                self.assertEqual(sanitize_public(key, {**legacy, "publication": marker}), placeholder)
            s3 = MemoryS3({key: legacy})
            job = migration.Migration(ROOT, {"s3": s3})
            job.scrub(key)
            self.assertEqual(s3.docs[key], placeholder)
            self.assertNotIn(MARKER, json.dumps(job.rows))

    def test_explicit_public_default_calculator_numbers_and_contract_are_preserved(self):
        for key in ("data/wealth-plan-snapshot.json", "data/tax-plan-snapshot.json"):
            model = {"publication": deepcopy(PUBLIC_DEFAULT_SCENARIO), "inputs": {"age": 35, "annual_savings": 20000},
                     "allocation": {"expected_return_pct": 6.5}, "model_explanation": "Default model scenario"}
            self.assertEqual(sanitize_public(key, model), model)
            self.assertEqual(sanitize_public(key.removeprefix("data/"), model), model)

    def test_source_zip_exact_hash_and_transitive_private_helpers(self):
        members = migration.desired_members(ROOT, "my-brief")
        self.assertTrue({"lambda_function.py", "private_artifact.py", "managed_secret.py", "public_brain_projection.py"} <= set(members))
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w") as z:
            for key, value in members.items():
                z.writestr(key, value)
        raw = buf.getvalue()
        cfg = {"CodeSha256": base64.b64encode(hashlib.sha256(raw).digest()).decode()}
        self.assertEqual(migration.verify_zip(raw, cfg, members)["members"], len(members))
        bad = {**members, "lambda_function.py": b"old deployed code"}
        with self.assertRaisesRegex(migration.MigrationError, "member_mismatch"):
            migration.verify_zip(raw, cfg, bad)
        with self.assertRaisesRegex(migration.MigrationError, "zip_hash"):
            migration.verify_zip(raw, {"CodeSha256": "wrong"}, members)

    def test_config_merge_preserves_variables_and_cas_promotes_live_config(self):
        lam = FakeLambda()
        result = migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
        self.assertEqual(lam.config["Environment"]["Variables"]["UNCHANGED"], "preserved")
        self.assertEqual(result["version"], "4")
        self.assertNotIn(MARKER, json.dumps(result))
        self.assertEqual([c[0] for c in lam.calls if c[0] in {"publish", "alias"}], ["publish", "alias"])

    def test_config_does_not_create_alias_or_override_racing_alias(self):
        lam = FakeLambda(live=False)
        result = migration.update_environment(lam, "justhodl-fixture", {"JH_PRIVACY_CACHE_EPOCH": "new"}, "reviewed")
        self.assertEqual(result["version"], "$LATEST")
        self.assertFalse(any(c[0] == "publish" for c in lam.calls))
        lam = FakeLambda()
        lam.race = True
        with self.assertRaisesRegex(migration.MigrationError, "alias_changed"):
            migration.update_environment(lam, "justhodl-fixture", {"JH_PRIVACY_CACHE_EPOCH": "new"}, "reviewed")
        self.assertFalse(any(c[0] == "alias" for c in lam.calls))

    def test_wrong_deployed_hash_rejects_config_mutation(self):
        lam = FakeLambda()
        with self.assertRaisesRegex(migration.MigrationError, "changed_before"):
            migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "wrong")
        self.assertFalse(any(c[0] == "update" for c in lam.calls))

    def test_same_code_latest_business_drift_blocks_before_any_configuration_write(self):
        changes = [
            {"Timeout": 900}, {"Handler": "unreviewed.run"}, {"MemorySize": 1024},
            {"Environment": {"Variables": {"UNCHANGED": "preserved", "AUTH_MODE": "public"}}},
            {"Layers": [{"Arn": "arn:layer:core:9"}]}, {"Role": "different-role"},
            {"VpcConfig": {"SubnetIds": ["unreviewed-subnet"]}}, {"KMSKeyArn": "unreviewed-kms"},
            {"Runtime": "python3.13"}, {"RuntimeManagementConfig": {"UpdateRuntimeOn": "Manual"}},
            {"SnapStart": {"ApplyOn": "None", "OptimizationStatus": "Off"}},
            {"UnknownFutureExecutionOption": "changed"},
        ]
        for change in changes:
            with self.subTest(fields=list(change)):
                lam = FakeLambda()
                lam.config.update(deepcopy(change))
                with self.assertRaisesRegex(migration.MigrationError, "config_drift"):
                    migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
                self.assertFalse(any(c[0] in {"update", "publish", "alias"} for c in lam.calls))

    def test_only_exact_intended_env_keys_may_differ_between_live_and_latest(self):
        lam = FakeLambda()
        lam.versions["3"]["Environment"]["Variables"]["JH_SERVICE_TOKEN"] = "previous-token"
        lam.config["Environment"]["Variables"]["JH_SERVICE_TOKEN"] = "candidate-token"
        result = migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
        self.assertEqual(result["version"], "4")
        self.assertEqual(lam.versions["4"]["Environment"]["Variables"],
                         {"UNCHANGED": "preserved", "AUTH_MODE": "owner", "JH_SERVICE_TOKEN": MARKER})
        self.assertNotIn(MARKER, json.dumps(result))

    def test_completed_environment_migration_retry_verifies_without_republishing(self):
        lam = FakeLambda()
        migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
        before = [c for c in lam.calls if c[0] in {"update", "publish", "alias"}]
        result = migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
        self.assertEqual(result["version"], "4")
        self.assertTrue(result["already_configured"])
        self.assertEqual([c for c in lam.calls if c[0] in {"update", "publish", "alias"}], before)
        self.assertNotIn(MARKER, json.dumps(result))

    def test_config_mutation_cannot_absorb_concurrent_unintended_config_or_env_changes(self):
        for change in ({"Timeout": 900}, {"Layers": [{"Arn": "arn:layer:core:9"}]},
                       {"Environment": {"Variables": {"JH_SERVICE_TOKEN": MARKER, "AUTH_MODE": "public"}}}):
            class Changed(FakeLambda):
                def update_function_configuration(self, **kw):
                    super().update_function_configuration(**kw)
                    self.config.update(deepcopy(change))
            lam = Changed()
            with self.assertRaisesRegex(migration.MigrationError, "config_update_not_verified"):
                migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
            self.assertFalse(any(c[0] in {"publish", "alias"} for c in lam.calls))

    def test_published_candidate_full_config_hash_and_state_verified_before_alias_cas(self):
        changes = [{"Timeout": 900}, {"CodeSha256": "unreviewed"}, {"Handler": "other.run"},
                   {"Layers": [{"Arn": "arn:layer:core:9"}]}, {"State": "Pending"},
                   {"LastUpdateStatus": "Failed"}, {"Version": "99"},
                   {"Environment": {"Variables": {"AUTH_MODE": "public", "JH_SERVICE_TOKEN": MARKER}}}]
        for change in changes:
            with self.subTest(fields=list(change)):
                class Changed(FakeLambda):
                    def publish_version(self, **kw):
                        result = super().publish_version(**kw)
                        self.versions["4"].update(deepcopy(change))
                        return result
                lam = Changed()
                with self.assertRaises(migration.MigrationError):
                    migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
                self.assertFalse(any(c[0] == "alias" for c in lam.calls))

    def test_owned_publish_revision_refresh_rejects_alias_semantic_drift(self):
        for change in ({"FunctionVersion": "5"}, {"Description": "Unreviewed alias"},
                       {"RoutingConfig": {"AdditionalVersionWeights": {"5": 0.1}}}):
            class Changed(FakeLambda):
                def publish_version(self, **kw):
                    result = super().publish_version(**kw)
                    self.alias.update(deepcopy(change))
                    return result
            lam = Changed()
            with self.assertRaisesRegex(migration.MigrationError, "alias_changed"):
                migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
            self.assertFalse(any(c[0] == "alias" for c in lam.calls))

    def test_external_revision_only_change_after_owned_publish_blocks_promotion(self):
        for target in ("config", "alias"):
            class Changed(FakeLambda):
                def get_function_configuration(self, **kw):
                    result = super().get_function_configuration(**kw)
                    if kw.get("Qualifier") == "4":
                        getattr(self, target)["RevisionId"] = "external-late-revision"
                    return result
            lam = Changed()
            with self.assertRaises(migration.MigrationError):
                migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
            self.assertFalse(any(c[0] == "alias" for c in lam.calls))

    def test_unreadable_or_unstable_live_configuration_blocks_before_write(self):
        for change in ({"Environment": {"Error": {"ErrorCode": "KMSAccessDenied"}}},
                       {"ImageConfigResponse": {"Error": {"ErrorCode": "AccessDenied"}}},
                       {"State": "Pending"}, {"LastUpdateStatus": "InProgress"}):
            lam = FakeLambda()
            lam.versions["3"].update(deepcopy(change))
            with self.assertRaises(migration.MigrationError):
                migration.update_environment(lam, "justhodl-fixture", {"JH_SERVICE_TOKEN": MARKER}, "reviewed")
            self.assertFalse(any(c[0] == "update" for c in lam.calls))

    def test_s3_scrub_retries_race_without_losing_new_price_or_metadata(self):
        key = "equity-research/NVDA.json"
        s3 = MemoryS3({key: self.fixtures()[key]})
        s3.fail_once = True
        job = migration.Migration(ROOT, {"s3": s3})
        job.scrub(key)
        self.assertEqual(s3.docs[key]["price"], 234)
        self.assertEqual([w for w in s3.writes if w["Key"]==key][0]["Metadata"], {"operational-tag": "retain"})
        self.assertNotIn(MARKER, json.dumps(s3.docs[key]))
        self.assertNotIn(MARKER, json.dumps(job.rows))
        self.assertEqual([r for r in job.rows if r["check"]=="public_projection_written"][0]["sha256"], migration.digest(migration.encoded(s3.docs[key])))

    def test_exact_original_bytes_survive_scrub_without_bucket_versioning(self):
        key="equity-research/NVDA.json"
        store=MemoryS3({key:self.fixtures()[key]})
        original=store.raw(key);job=migration.Migration(ROOT,{"s3":store})
        job.scrub(key)
        preserved=[value for name,value in store.docs.items() if name.startswith(migration.BACKUP_PREFIX)]
        self.assertEqual(len(preserved),1)
        self.assertEqual(base64.b64decode(preserved[0]["raw_base64"]),original)
        self.assertNotIn(MARKER,json.dumps(store.docs[key]))
        self.assertNotIn(MARKER,json.dumps(job.rows))
        self.assertTrue(store.writes[0]["Key"].startswith(migration.BACKUP_PREFIX))
        self.assertEqual(store.writes[0]["IfNoneMatch"],"*")

    def test_failed_or_corrupt_backup_prevents_public_rewrite(self):
        key="equity-research/NVDA.json"
        class Corrupt(MemoryS3):
            def get_object(self,**kwargs):
                result=super().get_object(**kwargs)
                if kwargs["Key"].startswith(migration.BACKUP_PREFIX):result["Body"]=io.BytesIO(b"corrupt")
                return result
        store=Corrupt({key:self.fixtures()[key]});original=deepcopy(store.docs[key])
        with self.assertRaisesRegex(migration.MigrationError,"backup_not_verified"):
            migration.Migration(ROOT,{"s3":store}).scrub(key)
        self.assertEqual(store.docs[key],original)
        self.assertFalse(any(row["Key"]==key for row in store.writes))

    def test_gzip_content_encoding_retains_original_and_encoded_public_bytes(self):
        key="equity-research/NVDA.json"
        class EncodedStore(MemoryS3):
            def raw(self,key):
                raw=super().raw(key)
                return gzip.compress(raw,mtime=0) if not key.startswith(migration.BACKUP_PREFIX) else raw
            def get_object(self,**kwargs):
                result=super().get_object(**kwargs)
                if not kwargs["Key"].startswith(migration.BACKUP_PREFIX):result["ContentEncoding"]="gzip"
                return result
            def put_object(self,**args):
                if args.get("ContentEncoding")=="gzip":
                    self.assert_encoded=args["Body"]
                    args={**args,"Body":gzip.decompress(args["Body"])}
                return super().put_object(**args)
        store=EncodedStore({key:self.fixtures()[key]});original=store.raw(key)
        job=migration.Migration(ROOT,{"s3":store});job.scrub(key)
        backup=next(value for name,value in store.docs.items() if name.startswith(migration.BACKUP_PREFIX))
        self.assertEqual(base64.b64decode(backup["raw_base64"]),original)
        self.assertEqual(json.loads(gzip.decompress(store.assert_encoded)),store.docs[key])
        self.assertNotIn(MARKER,json.dumps(store.docs[key]))

    def test_backup_policy_denies_current_and_historical_external_reads(self):
        policy=migration.backup_deny_statement()
        self.assertEqual(set(policy["Action"]),{"s3:GetObject","s3:GetObjectVersion"})
        self.assertEqual(policy["Condition"],{"StringNotEquals":{"aws:PrincipalAccount":migration.ACCOUNT}})
        self.assertEqual(policy["Resource"],["arn:aws:s3:::"+migration.BUCKET+"/"+migration.BACKUP_PREFIX+"*"])

    def test_backup_boundary_requires_denial_on_all_public_hosts(self):
        job=migration.Migration(ROOT,{})
        with patch.object(migration,"security_head",return_value={"status":403}) as head:
            job.verify_backup_access(migration.BACKUP_PREFIX+"fixture.json")
        self.assertEqual(head.call_count,4)
        self.assertTrue(all(row["ok"] for row in job.rows))
        with patch.object(migration,"security_head",return_value={"status":200}):
            with self.assertRaisesRegex(migration.MigrationError,"public_access_not_denied"):
                job.verify_backup_access(migration.BACKUP_PREFIX+"fixture.json")

    def test_missing_canonical_is_failure_missing_alias_is_explicit(self):
        job = migration.Migration(ROOT, {"s3": MemoryS3({})})
        with self.assertRaises(RuntimeError):
            job.scrub("data/brain-compiler.json")
        job.scrub("brain-compiler.json", optional=True)
        self.assertEqual(job.rows, [{"check": "absent_legacy_alias", "key": "brain-compiler.json"}])

    def test_retry_keeps_already_projected_objects_and_publication_age_unchanged(self):
        key="data/engine-conflicts.json"
        store=MemoryS3({key:{"generated_at":"2026-09-09T00:00:00Z","rows":[]}})
        job=migration.Migration(ROOT,{"s3":store})
        job.scrub(key)
        self.assertEqual(store.writes,[])
        self.assertEqual(job.rows[0]["check"],"public_projection_already_safe")

    def test_shard_projection_uses_same_vault_search_helper_and_gzip_roundtrip(self):
        fixtures = self.fixtures()
        vault = fixtures["data/tradingview.json"]
        for key in (migration.VAULT_SHARD_KEY, migration.LEGACY_VAULT_SHARD_KEY):
            s3 = MemoryS3({key: fixtures[migration.LEGACY_VAULT_SHARD_KEY]})
            job = migration.Migration(ROOT, {"s3": s3})
            job.scrub(key, vault=vault)
            self.assertEqual(s3.docs[key]["rows"][0][3], vault_search_rows(vault)[0]["search"])
            self.assertEqual(s3.docs[key]["rows"][0][4:6], [123, 2])
            self.assertNotIn(MARKER, json.dumps(s3.docs[key]))
        self.assertIn(migration.VAULT_SHARD_KEY, migration.SANITIZED_KEYS)
        self.assertIn(migration.LEGACY_VAULT_SHARD_KEY, migration.SANITIZED_KEYS)
        with self.assertRaises(migration.MigrationError):
            migration.bounded_read(io.BytesIO(b"12345"), 4)

    def test_private_seed_kinds_exclude_raw_tradingview_corpus(self):
        self.assertEqual(len(migration.MIRRORED_KEYS), 26)
        self.assertNotIn("data/tradingview-notes.json", migration.MIRRORED_KEYS)
        self.assertEqual(migration.MIRRORED_ARTIFACTS["portfolio/snapshot.json"], "portfolio-snapshot")
        self.assertEqual(migration.MIRRORED_ARTIFACTS["portfolio/sizing.json"], "portfolio-sizing")
        self.assertNotIn("risk/recommendations.json", migration.MIRRORED_ARTIFACTS)
        self.assertNotIn("ask-desk", migration.PUBLISHERS)
        self.assertEqual(len(migration.PUBLISHERS), 20)
        self.assertEqual(len(migration.READINESS), 44)
        self.assertTrue({"etf-fund-flows", "macro-regime"} <= set(migration.PRODUCERS))
        self.assertTrue({"wealth-plan", "tax-plan"} <= set(migration.READINESS))
        self.assertFalse({"wealth-plan", "tax-plan"} & set(migration.PUBLISHERS))

    def test_private_vol_bootstrap_preserves_full_original_once_before_public_scrub(self):
        full = self.fixtures()["data/vol-regime.json"]
        store = MemoryS3({"data/vol-regime.json": full})
        job = migration.Migration(ROOT, {"s3": store})
        job.bootstrap_private_derivatives()
        self.assertEqual(store.docs["data/vol-regime-private.json"], full)
        self.assertEqual(store.writes[0]["IfNoneMatch"], "*")
        job.scrub("data/vol-regime.json")
        self.assertNotIn(MARKER, json.dumps(store.docs["data/vol-regime.json"]))
        self.assertIn(MARKER, json.dumps(store.docs["data/vol-regime-private.json"]))
        count = len(store.writes)
        job.bootstrap_private_derivatives()
        self.assertEqual(len(store.writes), count)

    def test_absent_manual_journal_bootstrap_uses_actual_empty_schema_without_pnl(self):
        store = MemoryS3({"data/vol-regime-private.json": {"tickers": []}})
        job = migration.Migration(ROOT, {"s3": store})
        job.bootstrap_private_derivatives()
        self.assertEqual(store.docs["data/user-trades.json"], {"version": 0, "trades": []})
        stats = store.docs["data/user-trades-stats.json"]
        self.assertEqual(set(stats), {"as_of", "n_total", "n_open", "n_closed"})
        self.assertEqual(stats["n_total"], 0)
        self.assertTrue(all(w["IfNoneMatch"] == "*" and w["CacheControl"] == "private, no-store" for w in store.writes))

    def test_absent_manual_stats_use_existing_full_ledger_without_overwriting_it(self):
        ledger = {"version": 4, "trades": [{"ticker": MARKER, "status": "OPEN", "size_usd": 2000,
                                              "current_pnl_pct": 5}]}
        store = MemoryS3({"data/vol-regime-private.json": {"tickers": []}, "data/user-trades.json": ledger})
        job = migration.Migration(ROOT, {"s3": store})
        job.bootstrap_private_derivatives()
        self.assertEqual(store.docs["data/user-trades.json"], ledger)
        stats = store.docs["data/user-trades-stats.json"]
        self.assertEqual(stats["n_open"], 1)
        self.assertEqual(stats["open_total_pnl_dollars"], 100)
        self.assertEqual([w["Key"] for w in store.writes if w["Key"] not in migration.OWNER_HISTORY_DEFAULTS], ["data/user-trades-stats.json"])
        self.assertNotIn(MARKER, json.dumps(job.rows))

    def test_owner_history_bootstrap_preserves_existing_rows_and_uses_only_empty_source_schemas(self):
        existing = {"synthetic-held-position": "2026-09-09T00:00:00Z"}
        store = MemoryS3({"data/vol-regime-private.json": {"tickers": []},
                          "data/user-trades.json": {"version": 0, "trades": []},
                          "data/user-trades-stats.json": {"n_total": 0},
                          "portfolio/risk-alert-history.json": existing})
        job = migration.Migration(ROOT, {"s3": store})
        job.bootstrap_private_derivatives()
        self.assertEqual(store.docs["portfolio/risk-alert-history.json"], existing)
        self.assertFalse(any(w["Key"] == "portfolio/risk-alert-history.json" for w in store.writes))
        for key, empty in migration.OWNER_HISTORY_DEFAULTS.items():
            if key != "portfolio/risk-alert-history.json": self.assertEqual(store.docs[key], empty)
        self.assertTrue(all(w["IfNoneMatch"] == "*" and w["CacheControl"] == "private, no-store" for w in store.writes))
        count = len(store.writes);job.bootstrap_private_derivatives();self.assertEqual(len(store.writes), count)

    def test_missing_manual_ledger_with_nonempty_stats_fails_closed(self):
        store = MemoryS3({"data/vol-regime-private.json": {"tickers": []}, "data/user-trades-stats.json": {"n_total": 2}})
        job = migration.Migration(ROOT, {"s3": store})
        with self.assertRaisesRegex(migration.MigrationError, "personal_ledger_missing_with_nonempty_stats"):
            job.bootstrap_private_derivatives()
        self.assertFalse(store.writes)

    def test_private_mirror_verification_retries_head_without_body_access(self):
        requests = []
        class Head:
            status = 200
            headers = {"Cache-Control": "private, no-store"}
            def __enter__(self): return self
            def __exit__(self, *args): pass
            def read(self, *args): raise AssertionError("must never read private verification body")
        def http(request, timeout):
            requests.append(request)
            if len(requests) == 1:
                raise urllib.error.HTTPError(request.full_url, 503, "synthetic cold KV", {}, None)
            return Head()
        job = migration.Migration(ROOT, {}, http=http)
        with patch.object(migration.time, "sleep") as sleep:
            job.mirror_head("brain", MARKER)
        self.assertEqual([r.method for r in requests], ["HEAD", "HEAD"])
        sleep.assert_called_once_with(1)
        self.assertNotIn(MARKER, json.dumps(job.rows))

    def test_service_identity_preflight_never_reads_private_body(self):
        for status, expected in ((200, True), (503, True), (401, False), (403, False)):
            requests = []
            class Head:
                headers = {"Cache-Control": "private, no-store"}
                def __enter__(self): return self
                def __exit__(self, *args): pass
                def read(self, *args): raise AssertionError("HEAD body must not be read")
            def http(request, timeout):
                requests.append(request)
                if status != 200:
                    raise urllib.error.HTTPError(request.full_url, status, MARKER, Head.headers, io.BytesIO(MARKER.encode()))
                response = Head();response.status = status;return response
            job = migration.Migration(ROOT, {}, http=http)
            if expected: job.verify_service_access(MARKER)
            else:
                with self.assertRaisesRegex(migration.MigrationError, "worker_service_identity_not_verified"):
                    job.verify_service_access(MARKER)
            self.assertEqual(requests[0].method, "HEAD")
            self.assertEqual(requests[0].get_header("User-agent"), "JustHodl-PrivateArtifacts/20260909")
            self.assertIsNone(requests[0].data)
            self.assertNotIn(MARKER, json.dumps(job.rows))

    def test_seed_http_error_records_only_fixed_key_and_status(self):
        key = next(iter(migration.MIRRORED_ARTIFACTS))
        store = MemoryS3({key: {"private_prose": MARKER}})
        errors = []
        def http(request, timeout):
            self.assertEqual(request.get_header("User-agent"), "JustHodl-PrivateArtifacts/20260909")
            error = urllib.error.HTTPError(request.full_url, 403, MARKER, {}, io.BytesIO(MARKER.encode()))
            errors.append(error);raise error
        job = migration.Migration(ROOT, {"s3": store}, http=http)
        with self.assertRaisesRegex(migration.MigrationError, "private_mirror_write_rejected"):
            job.seed(MARKER)
        self.assertEqual(job.rows, [{"check": "private_mirror_write_http_failure", "key": key, "status": 403}])
        self.assertTrue(errors[0].closed)
        self.assertNotIn(MARKER, json.dumps(job.rows))

    def test_wrong_account_never_mutates_and_code_gate_precedes_identity_or_seed(self):
        job = migration.Migration(ROOT, {"sts": types.SimpleNamespace(get_caller_identity=lambda: {"Account": "other"})})
        with self.assertRaisesRegex(migration.MigrationError, "wrong_aws_account"):
            job.run()
        self.assertEqual(job.step, "initialization")
        operations = []
        job = migration.Migration(ROOT, {"sts": types.SimpleNamespace(get_caller_identity=lambda: {"Account": migration.ACCOUNT})})
        job.policy = lambda temporary: operations.append(("policy", temporary))
        job.purge = lambda: operations.append(("purge", True))
        job.verify_backup_access = lambda key: operations.append(("backup_boundary", key.startswith(migration.BACKUP_PREFIX)))
        def reject_code(name):
            raise migration.MigrationError("deployed_source_member_mismatch")
        job.readiness = reject_code
        job.configure = lambda token: self.fail("identity must not be configured before exact source readiness")
        job.seed = lambda token: self.fail("private source must not be read before exact source readiness")
        with self.assertRaisesRegex(migration.MigrationError, "member_mismatch"):
            job.run()
        self.assertEqual(operations, [("policy", True), ("purge", True), ("backup_boundary", True)])
        self.assertEqual(job.step, "verify_deployed_code")

    def test_invocation_rejects_wrong_version_without_reporting_lambda_payload(self):
        response = {"StatusCode": 200, "ExecutedVersion": "old", "Payload": io.BytesIO(json.dumps({"ok": True, "private_note": MARKER}).encode())}
        job = migration.Migration(ROOT, {"lambda": types.SimpleNamespace(invoke=lambda **kw: response)})
        with self.assertRaisesRegex(migration.MigrationError, "executed_version_mismatch"):
            job.invoke("fixture", {}, "4")
        self.assertEqual(response["Payload"].tell(), 0)
        self.assertNotIn(MARKER, json.dumps(job.rows))


if __name__ == "__main__":
    unittest.main()
