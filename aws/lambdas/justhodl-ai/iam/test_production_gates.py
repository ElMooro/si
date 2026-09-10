#!/usr/bin/env python3
"""Standard-library tests for the offline JustHodl AI infrastructure gates."""

from __future__ import annotations

import importlib.util
import json
import sys
import unittest
from pathlib import Path


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location(
    "validate_production_gates", HERE / "validate_production_gates.py"
)
assert SPEC and SPEC.loader
gates = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = gates
SPEC.loader.exec_module(gates)


class ProductionGateTests(unittest.TestCase):
    def test_all_static_gates_pass(self) -> None:
        failed = [item for item in gates.validate() if not item.passed]
        self.assertEqual([], failed, "\n".join(f"{x.name}: {x.detail}" for x in failed))

    def test_plan_is_offline_and_reflects_repository_cutover_bindings(self) -> None:
        plan = gates.change_plan()
        self.assertEqual("offline-dry-run", plan["mode"])
        self.assertEqual(0, plan["aws_api_calls"])
        blockers = {item["id"]: item for item in plan["blockers"]}
        self.assertFalse(blockers["LAMBDA_ROLE_CUTOVER_PENDING"]["active"])
        self.assertFalse(blockers["BRAIN_SOURCE_CUTOVER_PENDING"]["active"])
        self.assertFalse(blockers["MODEL_REGISTRY_BINDING_PENDING"]["active"])
        self.assertFalse(blockers["MLFLOW_BINDING_PENDING"]["active"])
        self.assertTrue(blockers["NO_LIVE_AWS_DIFF"]["active"])

    def test_plan_never_overwrites_dashboard_policy(self) -> None:
        merge = next(
            item
            for item in gates.change_plan()["changes"]
            if item["action"] == "MERGE_BUCKET_POLICY"
        )
        self.assertIn("never replace", merge["precondition"])

    def test_checked_in_change_plan_is_current(self) -> None:
        saved_path = (
            gates.REPO_ROOT
            / "aws"
            / "ops"
            / "pending"
            / "justhodl_ai_infrastructure_change_plan.json"
        )
        saved = json.loads(saved_path.read_text(encoding="utf-8"))
        self.assertEqual(gates.change_plan(), saved["change_plan"])
        self.assertTrue(saved["ok"])
        self.assertEqual(len(gates.validate()), len(saved["checks"]))

    def test_template_is_valid_json_and_does_not_manage_lambda(self) -> None:
        template = json.loads(gates.TEMPLATE_PATH.read_text(encoding="utf-8"))
        resource_types = {
            resource["Type"] for resource in template["Resources"].values()
        }
        self.assertNotIn("AWS::Lambda::Function", resource_types)
        self.assertIn("AWS::SageMaker::FeatureGroup", resource_types)
        self.assertIn("AWS::SageMaker::ModelPackageGroup", resource_types)
        self.assertIn("AWS::SageMaker::MlflowTrackingServer", resource_types)
        self.assertIn("AWS::Events::EventBus", resource_types)
        self.assertEqual(
            "justhodl-ai-feature-store-prod",
            template["Resources"]["FeatureStoreExecutionRole"]["Properties"][
                "RoleName"
            ],
        )

    def test_lambda_domain_policies_use_deployable_managed_policy_quotas(
        self,
    ) -> None:
        template = gates.load_json(gates.TEMPLATE_PATH)
        role = template["Resources"]["LambdaExecutionRole"]["Properties"]
        self.assertEqual(
            [
                {"Ref": logical_id}
                for logical_id in gates.LAMBDA_MANAGED_POLICY_IDS.values()
            ],
            role["ManagedPolicyArns"],
        )
        self.assertLessEqual(
            len(role["ManagedPolicyArns"]),
            gates.IAM_ROLE_MANAGED_POLICY_ATTACHMENT_LIMIT,
        )
        for policy_name, logical_id in gates.LAMBDA_MANAGED_POLICY_IDS.items():
            with self.subTest(policy_name=policy_name):
                policy = gates._managed_policy(template, logical_id)
                self.assertEqual(
                    gates.load_json(gates.LAMBDA_POLICY_PATHS[policy_name]),
                    policy,
                )
                self.assertLessEqual(
                    gates._compact_policy_size(policy),
                    gates.IAM_MANAGED_POLICY_DOCUMENT_LIMIT,
                )
        self.assertLessEqual(
            sum(
                gates._compact_policy_size(item["PolicyDocument"])
                for item in role.get("Policies", [])
            ),
            gates.IAM_ROLE_INLINE_POLICY_AGGREGATE_LIMIT,
        )

    def test_registry_and_mlflow_resources_are_deletion_safe(self) -> None:
        resources = gates.load_json(gates.TEMPLATE_PATH)["Resources"]
        for logical_id in (
            "ModelPackageGroup",
            "MlflowTrackingServer",
            "MlflowArtifactBucket",
            "MlflowTrackingRole",
        ):
            with self.subTest(logical_id=logical_id):
                self.assertEqual("Retain", resources[logical_id]["DeletionPolicy"])
                self.assertEqual(
                    "Retain", resources[logical_id]["UpdateReplacePolicy"]
                )

    def test_mlflow_role_is_scoped_to_the_named_private_store(self) -> None:
        template = gates.load_json(gates.TEMPLATE_PATH)
        policy = gates._role_policy(
            template, "MlflowTrackingRole", "justhodl-ai-mlflow-artifacts"
        )
        for statement in gates.statements(policy):
            self.assertNotIn("*", gates.as_list(statement.get("Resource", [])))
        resources = [
            json.dumps(resource, sort_keys=True)
            for action in ("s3:ListBucket", "s3:GetObject", "s3:PutObject")
            for resource in gates.resources_for_action(policy, action)
        ]
        self.assertTrue(resources)
        self.assertTrue(all("MlflowArtifactBucket" in item for item in resources))

    def test_mlflow_clients_cannot_delete_runs_or_approve_packages(self) -> None:
        template = gates.load_json(gates.TEMPLATE_PATH)
        sm_policy = gates.load_json(gates.SAGEMAKER_POLICY_PATH)
        mlflow_policy = gates._role_policy(
            template, "MlflowTrackingRole", "justhodl-ai-mlflow-artifacts"
        )
        allowed = gates.actions(sm_policy, "Allow") | gates.actions(
            mlflow_policy, "Allow"
        )
        self.assertNotIn("sagemaker:UpdateModelPackage", allowed)
        self.assertNotIn("sagemaker-mlflow:*", allowed)
        self.assertFalse(
            any(action.startswith("sagemaker-mlflow:Delete") for action in allowed)
        )
        self.assertFalse(
            template["Resources"]["MlflowTrackingServer"]["Properties"][
                "AutomaticModelRegistration"
            ]
        )
        client = gates.statement_by_sid(sm_policy, "WriteGovernedMlflowRuns")
        self.assertIsNotNone(client)
        self.assertEqual(
            gates.MLFLOW_CLIENT_ACTIONS,
            set(gates.as_list(client.get("Action", []))),
        )
        self.assertEqual(gates.MLFLOW_TRACKING_SERVER_ARN, client["Resource"])

    def test_mlflow_kms_and_bucket_policy_are_scoped_and_retained(self) -> None:
        template = gates.load_json(gates.TEMPLATE_PATH)
        policy = gates._role_policy(
            template, "MlflowTrackingRole", "justhodl-ai-mlflow-artifacts"
        )
        kms = gates.statement_by_sid(policy, "UseArtifactEncryptionKey")
        self.assertIsNotNone(kms)
        self.assertEqual({"Fn::GetAtt": ["AiDataKey", "Arn"]}, kms["Resource"])
        self.assertEqual(
            {
                "kms:CallerAccount": gates.ACCOUNT_ID,
                "kms:ViaService": f"s3.{gates.REGION}.amazonaws.com",
            },
            kms["Condition"]["StringEquals"],
        )
        bucket_policy = template["Resources"]["MlflowArtifactBucketPolicy"]
        self.assertEqual("Retain", bucket_policy["DeletionPolicy"])
        self.assertEqual("Retain", bucket_policy["UpdateReplacePolicy"])
        self.assertTrue(
            {
                "DenyInsecureTransport",
                "DenyCrossAccountAccess",
                "DenyPublicObjectAcl",
            }.issubset(
                {
                    statement["Sid"]
                    for statement in gates.statements(
                        bucket_policy["Properties"]["PolicyDocument"]
                    )
                }
            )
        )

    def test_registry_and_mlflow_environment_bindings_match_config(self) -> None:
        template = gates.load_json(gates.TEMPLATE_PATH)
        emitted = json.loads(
            template["Outputs"]["RequiredLambdaEnvironment"]["Value"]["Fn::Sub"]
        )
        config = gates.load_json(gates.FUNCTION_CONFIG_PATH)["env"]
        self.assertEqual(config["MODEL_PACKAGE_GROUP"], emitted["MODEL_PACKAGE_GROUP"])
        self.assertEqual(
            config["MLFLOW_EXPERIMENT_NAME"], emitted["MLFLOW_EXPERIMENT_NAME"]
        )
        self.assertEqual(
            "${MlflowTrackingServer}", emitted["MLFLOW_TRACKING_SERVER_NAME"]
        )
        self.assertEqual(
            "${MlflowTrackingServer.TrackingServerArn}",
            emitted["MLFLOW_TRACKING_SERVER_ARN"],
        )
        self.assertEqual(
            gates.MLFLOW_TRACKING_SERVER_ARN,
            config["MLFLOW_TRACKING_SERVER_ARN"],
        )

    def test_lambda_live_governance_permissions_are_exact_and_cannot_approve(self) -> None:
        policy = gates.load_json(
            gates.LAMBDA_POLICY_PATHS[
                "justhodl-ai-training-deployment-control"
            ]
        )
        package = gates.statement_by_sid(
            policy, "CreatePendingGovernedModelPackages"
        )
        card = gates.statement_by_sid(policy, "CreateDraftGovernedModelCards")
        lineage = gates.statement_by_sid(policy, "PublishGovernedMlflowLineage")
        self.assertEqual(
            {
                "sagemaker:CreateModelPackage",
                "sagemaker:DescribeModelPackage",
                "sagemaker:ListTags",
            },
            set(gates.as_list(package["Action"])),
        )
        self.assertEqual(
            {
                f"arn:aws:sagemaker:{gates.REGION}:{gates.ACCOUNT_ID}:"
                f"model-package-group/{gates.MODEL_PACKAGE_GROUP}",
                f"arn:aws:sagemaker:{gates.REGION}:{gates.ACCOUNT_ID}:"
                f"model-package/{gates.MODEL_PACKAGE_GROUP}/*",
            },
            set(gates.as_list(package["Resource"])),
        )
        self.assertEqual(
            {"sagemaker:CreateModelCard", "sagemaker:DescribeModelCard"},
            set(gates.as_list(card["Action"])),
        )
        self.assertEqual(
            f"arn:aws:sagemaker:{gates.REGION}:{gates.ACCOUNT_ID}:"
            "model-card/jh-ai-*",
            card["Resource"],
        )
        self.assertEqual(
            {
                "sagemaker-mlflow:GetRun",
                "sagemaker-mlflow:GetExperiment",
                "sagemaker-mlflow:LogBatch",
            },
            set(gates.as_list(lineage["Action"])),
        )
        self.assertEqual(gates.MLFLOW_TRACKING_SERVER_ARN, lineage["Resource"])
        self.assertNotIn("sagemaker:UpdateModelPackage", gates.actions(policy))

    def test_training_evidence_is_version_read_only_and_config_has_no_token(self) -> None:
        policy = gates.load_json(
            gates.LAMBDA_POLICY_PATHS["justhodl-ai-runtime-data"]
        )
        evidence = gates.statement_by_sid(policy, "ReadVersionedTrainingEvidence")
        self.assertEqual(
            {"s3:GetObject", "s3:GetObjectVersion"},
            set(gates.as_list(evidence["Action"])),
        )
        self.assertEqual(
            f"arn:aws:s3:::justhodl-ai-{gates.ACCOUNT_ID}/"
            "ai/governance/training-evidence/v1/*",
            evidence["Resource"],
        )
        sources = gates.statement_by_sid(
            policy, "ReadVersionedGovernedTrainingSources"
        )
        self.assertEqual(
            {"s3:GetObject", "s3:GetObjectVersion"},
            set(gates.as_list(sources["Action"])),
        )
        self.assertTrue(
            all(
                resource.startswith(
                    f"arn:aws:s3:::justhodl-ai-{gates.ACCOUNT_ID}/ai/"
                )
                for resource in gates.as_list(sources["Resource"])
            )
        )
        immutable = gates.statement_by_sid(
            policy, "MaterializeContentAddressedTrainingInputs"
        )
        self.assertEqual(
            {"s3:GetObject", "s3:PutObject"},
            set(gates.as_list(immutable["Action"])),
        )
        immutable_resources = [
            f"arn:aws:s3:::justhodl-ai-training-inputs-{gates.ACCOUNT_ID}-"
            f"{gates.REGION}/sha256/*/train/payload.data",
            f"arn:aws:s3:::justhodl-ai-training-inputs-{gates.ACCOUNT_ID}-"
            f"{gates.REGION}/sha256/*/validation/payload.data",
        ]
        self.assertEqual(immutable_resources, immutable["Resource"])
        sagemaker_policy = gates.load_json(gates.SAGEMAKER_POLICY_PATH)
        training_reads = gates.statement_by_sid(
            sagemaker_policy, "ReadPrivateTrainingInputs"
        )
        self.assertIn(
            immutable_resources[0],
            gates.as_list(training_reads["Resource"]),
        )
        self.assertIn(
            immutable_resources[1],
            gates.as_list(training_reads["Resource"]),
        )
        template = gates.load_json(gates.TEMPLATE_PATH)
        bucket = template["Resources"]["TrainingInputBucket"]
        self.assertTrue(bucket["Properties"]["ObjectLockEnabled"])
        self.assertEqual(
            "COMPLIANCE",
            bucket["Properties"]["ObjectLockConfiguration"]["Rule"][
                "DefaultRetention"
            ]["Mode"],
        )
        statements = {
            row["Sid"]: row
            for row in template["Resources"]["TrainingInputBucketPolicy"][
                "Properties"
            ]["PolicyDocument"]["Statement"]
        }
        self.assertIn("s3:DeleteObjectVersion", statements["DenyObjectDeletion"]["Action"])
        self.assertEqual(
            "*",
            statements["RequireWriteOnceConditional"]["Condition"][
                "StringNotEquals"
            ]["s3:if-none-match"],
        )
        config = gates.load_json(gates.FUNCTION_CONFIG_PATH)["env"]
        self.assertEqual("production", config["AI_ENVIRONMENT"])
        self.assertTrue(config["AI_GOVERNANCE_OWNER"])
        self.assertNotIn("AI_GOVERNANCE_APPROVAL_TOKEN", config)
        self.assertNotIn("AI_CANARY_APPROVAL_TOKEN", config)

    def test_new_prediction_index_writer_is_not_mutable(self) -> None:
        policy = gates.load_json(
            gates.LAMBDA_POLICY_PATHS["justhodl-ai-ledger-archive"]
        )
        for statement in gates.statements(policy):
            if gates.LEDGER_TABLE not in json.dumps(statement.get("Resource", "")):
                continue
            action = set(gates.as_list(statement.get("Action", [])))
            self.assertFalse(
                action
                & {
                    "dynamodb:UpdateItem",
                    "dynamodb:DeleteItem",
                    "dynamodb:BatchWriteItem",
                }
            )

    def test_lambda_cannot_overwrite_canonical_brain_objects(self) -> None:
        policies = [
            gates.load_json(path) for path in gates.LAMBDA_POLICY_PATHS.values()
        ]
        canonical_puts = [
            resource
            for policy in policies
            for resource in gates.resources_for_action(policy, "s3:PutObject")
            if gates.CANONICAL_BRAIN_BUCKET in str(resource)
        ]
        self.assertEqual(
            [
                f"arn:aws:s3:::{gates.CANONICAL_BRAIN_BUCKET}/signals/v1/*"
            ],
            canonical_puts,
        )

        brain_policy = gates.load_json(
            gates.LAMBDA_POLICY_PATHS["justhodl-ai-brain-signal-boundary"]
        )
        canonical_read = gates.statement_by_sid(
            brain_policy, "ReadCanonicalBrainObject"
        )
        self.assertEqual(
            f"arn:aws:s3:::{gates.CANONICAL_BRAIN_BUCKET}/data/brain.json",
            canonical_read["Resource"],
        )
        self.assertNotIn("s3:PutObject", canonical_read["Action"])

        template = gates.load_json(gates.TEMPLATE_PATH)
        bucket_policy = template["Resources"]["CanonicalBrainBucketPolicy"][
            "Properties"
        ]["PolicyDocument"]
        deny = gates.statement_by_sid(
            bucket_policy, "DenyLambdaCanonicalBrainMutation"
        )
        self.assertEqual("Deny", deny["Effect"])
        self.assertEqual(
            {"Fn::Sub": "${CanonicalBrainBucket.Arn}/signals/v1/*"},
            deny["NotResource"],
        )

    def test_signal_outbox_and_ledger_archives_have_separate_prefix_policies(
        self,
    ) -> None:
        brain = gates.load_json(
            gates.LAMBDA_POLICY_PATHS["justhodl-ai-brain-signal-boundary"]
        )
        ledger = gates.load_json(
            gates.LAMBDA_POLICY_PATHS["justhodl-ai-ledger-archive"]
        )
        outbox = gates.statement_by_sid(
            brain, "ReadWriteGovernedSignalOutbox"
        )
        archive = gates.statement_by_sid(ledger, "AppendPredictionArchive")
        self.assertEqual(
            f"arn:aws:s3:::justhodl-ai-{gates.ACCOUNT_ID}/ai/signals/outbox/v1/*",
            outbox["Resource"],
        )
        self.assertEqual(
            f"arn:aws:s3:::{gates.LEDGER_ARCHIVE_BUCKET}/predictions/*",
            archive["Resource"],
        )
        self.assertEqual(
            {"s3:GetObject", "s3:PutObject"}, set(archive["Action"])
        )

    def test_runtime_and_sagemaker_policies_reject_bucket_wide_object_access(
        self,
    ) -> None:
        runtime = gates.load_json(
            gates.LAMBDA_POLICY_PATHS["justhodl-ai-runtime-data"]
        )
        sm = gates.load_json(gates.SAGEMAKER_POLICY_PATH)
        for policy in (runtime, sm):
            resources = [
                str(resource)
                for action in ("s3:GetObject", "s3:PutObject")
                for resource in gates.resources_for_action(policy, action)
            ]
            self.assertNotIn(
                f"arn:aws:s3:::justhodl-ai-{gates.ACCOUNT_ID}/ai/*",
                resources,
            )
            self.assertNotIn(
                f"arn:aws:s3:::{gates.CANONICAL_BRAIN_BUCKET}/*",
                resources,
            )
            self.assertNotIn(
                f"arn:aws:s3:::{gates.FEATURE_STORE_BUCKET}/*",
                resources,
            )

    def test_feature_groups_do_not_reuse_training_deployment_role(self) -> None:
        template = gates.load_json(gates.TEMPLATE_PATH)
        resources = template["Resources"]
        expected = {"Fn::GetAtt": ["FeatureStoreExecutionRole", "Arn"]}
        for logical_id in ("SignalFeatureGroup", "PredictionFeatureGroup"):
            self.assertEqual(
                expected, resources[logical_id]["Properties"]["RoleArn"]
            )
        policy = gates._role_policy(
            template,
            "FeatureStoreExecutionRole",
            "justhodl-ai-feature-store-offline",
        )
        text = json.dumps(policy, sort_keys=True)
        self.assertNotIn(gates.CANONICAL_BRAIN_BUCKET, text)
        self.assertNotIn(f"justhodl-ai-{gates.ACCOUNT_ID}/ai/", text)
        self.assertIn("FeatureStoreBucket", text)


if __name__ == "__main__":
    unittest.main()
