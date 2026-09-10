"""Unit tests for the fail-closed JustHodl AI live rollout."""

from __future__ import annotations

import importlib.util
import io
import json
import sys
import tempfile
import unittest
from pathlib import Path

from botocore.exceptions import ClientError, WaiterError


SCRIPT = (
    Path(__file__).resolve().parents[1]
    / "pending"
    / "justhodl_ai_live_rollout.py"
)
SPEC = importlib.util.spec_from_file_location("justhodl_ai_live_rollout", SCRIPT)
assert SPEC and SPEC.loader
rollout = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = rollout
SPEC.loader.exec_module(rollout)


def client_error(code: str, message: str = "test") -> ClientError:
    return ClientError(
        {"Error": {"Code": code, "Message": message}},
        "FakeOperation",
    )


class FakeReporter:
    def __init__(self) -> None:
        self.data = {
            "run_id": "test-run",
            "change_sets": [],
            "checks": [],
            "activation": {},
        }
        self.flush_count = 0
        root = Path(tempfile.mkdtemp(prefix="jh-ai-rollout-test-"))
        self.path = root / "report.json"
        self.state_path = root / "state.json"

    def flush(self) -> None:
        self.flush_count += 1

    def check(self, name, passed, **detail) -> None:
        self.data["checks"].append({"name": name, "passed": passed, **detail})


class FakeSession:
    def __init__(self, clients, region_name=rollout.REGION) -> None:
        self.clients = clients
        self.region_name = region_name

    def client(self, name, region_name=None):
        return self.clients[name]


def subject(clients, *, apply=True):
    aws = rollout.AwsClients(FakeSession(clients))
    return rollout.Rollout(
        aws,
        FakeReporter(),
        apply=apply,
        canary_input=rollout.DEFAULT_CANARY_INPUT,
        approval_secret_id="test-secret",
        sleep=lambda _: None,
    )


class FakeSts:
    def __init__(self, account):
        self.account = account

    def get_caller_identity(self):
        return {"Account": self.account, "Arn": "arn:aws:iam::test:user/test"}


class ImmediateWaiter:
    def wait(self, **kwargs):
        return None


class FailedStackWaiter:
    def wait(self, **kwargs):
        raise WaiterError(name="stack_create_complete", reason="terminal failure", last_response={})


class FakeFailedCloudFormation:
    def __init__(self):
        self.describes = 0

    def describe_stacks(self, StackName):
        self.describes += 1
        if self.describes == 1:
            raise client_error("ValidationError", "does not exist")
        return {
            "Stacks": [
                {
                    "StackName": StackName,
                    "StackStatus": "CREATE_FAILED",
                    "StackStatusReason": "synthetic stack failure",
                }
            ]
        }

    def create_change_set(self, **kwargs):
        self.change_set = kwargs
        return {"Id": "fake"}

    def get_waiter(self, name):
        if name == "change_set_create_complete":
            return ImmediateWaiter()
        return FailedStackWaiter()

    def describe_change_set(self, **kwargs):
        return {
            "Status": "CREATE_COMPLETE",
            "Changes": [
                {
                    "ResourceChange": {
                        "Action": "Add",
                        "LogicalResourceId": "Example",
                        "ResourceType": "AWS::S3::Bucket",
                    }
                }
            ],
        }

    def execute_change_set(self, **kwargs):
        self.executed = kwargs


class BrainChecksumS3:
    def __init__(self, corrupt=False):
        self.corrupt = corrupt
        self.destination = None
        self.head_calls = 0

    def get_object(self, Bucket, Key, VersionId=None):
        if Bucket == rollout.DASHBOARD_BUCKET:
            return {
                "Body": io.BytesIO(b'{"brain":"canonical"}'),
                "VersionId": "source-v1",
                "ContentType": "application/json",
            }
        body = self.destination
        if self.corrupt:
            body = b'{"brain":"corrupt"}'
        return {"Body": io.BytesIO(body)}

    def head_object(self, Bucket, Key, VersionId=None):
        self.head_calls += 1
        if self.destination is None:
            raise client_error("404")
        return {
            "VersionId": "dest-v1",
            "Metadata": {"sha256": rollout.sha256_bytes(self.destination)},
            "ServerSideEncryption": "aws:kms",
            "SSEKMSKeyId": "arn:kms:test",
        }

    def put_object(self, **kwargs):
        self.destination = kwargs["Body"]
        return {"VersionId": "dest-v1"}


class PolicyS3:
    def __init__(self, current):
        self.policy = current

    def get_bucket_policy(self, Bucket):
        return {"Policy": json.dumps(self.policy)}

    def put_bucket_policy(self, Bucket, Policy):
        self.policy = json.loads(Policy)


class MutableLambda:
    def __init__(self):
        self.role = "new-role"
        self.code_sha = "new-code"
        self.environment = {"NEW": "value"}
        self.alias_updates = []
        self.mapping_states = {
            "mapping-signal": "Enabled",
            "mapping-prediction": "Enabled",
            "mapping-model": "Enabled",
        }
        self.function_mappings = dict(
            zip(rollout.CONSUMER_FUNCTIONS, self.mapping_states)
        )

    def update_function_code(self, **kwargs):
        self.code_sha = "old-code"
        return {}

    def update_function_configuration(self, **kwargs):
        self.role = kwargs["Role"]
        self.environment = kwargs["Environment"]["Variables"]
        return {}

    def get_waiter(self, name):
        return ImmediateWaiter()

    def update_alias(self, **kwargs):
        self.alias_updates.append(kwargs)
        return {}

    def get_function_configuration(self, FunctionName):
        return {
            "Role": self.role,
            "CodeSha256": self.code_sha,
            "Environment": {"Variables": self.environment},
        }

    def list_event_source_mappings(self, FunctionName):
        uuid = self.function_mappings[FunctionName]
        return {"EventSourceMappings": [{"UUID": uuid}]}

    def get_event_source_mapping(self, UUID):
        return {"UUID": UUID, "State": self.mapping_states[UUID]}

    def update_event_source_mapping(self, UUID, Enabled):
        self.mapping_states[UUID] = "Enabled" if Enabled else "Disabled"
        return {"UUID": UUID, "State": self.mapping_states[UUID]}


class MutableEvents:
    def __init__(self):
        self.states = {name: "ENABLED" for name in rollout.RULE_NAMES}

    def disable_rule(self, Name):
        self.states[Name] = "DISABLED"

    def enable_rule(self, Name):
        self.states[Name] = "ENABLED"

    def describe_rule(self, Name):
        return {"Name": Name, "State": self.states[Name]}


class DiscoverySageMaker:
    def __init__(self):
        self.endpoint_pages = 0
        self.package_pages = 0

    def list_endpoints(self, **kwargs):
        self.endpoint_pages += 1
        if self.endpoint_pages == 1:
            return {
                "Endpoints": [
                    {"EndpointName": "jh-ai-blue", "EndpointStatus": "InService"},
                    {"EndpointName": "unmanaged", "EndpointStatus": "InService"},
                ],
                "NextToken": "endpoints-2",
            }
        return {
            "Endpoints": [
                {"EndpointName": "jh-ai-failed", "EndpointStatus": "Failed"}
            ]
        }

    def describe_endpoint(self, EndpointName):
        return {
            "EndpointArn": f"arn:endpoint:{EndpointName}",
            "EndpointConfigName": f"{EndpointName}-config",
        }

    def list_tags(self, ResourceArn):
        if ResourceArn.endswith("jh-ai-blue"):
            return {"Tags": [{"Key": "justhodl-ai-managed", "Value": "true"}]}
        return {"Tags": []}

    def describe_endpoint_config(self, EndpointConfigName):
        return {"ProductionVariants": [{"VariantName": "AllTraffic"}]}

    def list_model_packages(self, **kwargs):
        self.package_pages += 1
        if self.package_pages == 1:
            return {
                "ModelPackageSummaryList": [
                    {
                        "ModelPackageArn": "arn:package:approved/7",
                        "ModelApprovalStatus": "Approved",
                    }
                ],
                "NextToken": "packages-2",
            }
        return {"ModelPackageSummaryList": []}

    def list_models(self, **kwargs):
        return {
            "Models": [
                {"ModelName": "jh-ai-classifier-model"},
                {"ModelName": "unrelated-model"},
            ]
        }

    def describe_model(self, ModelName):
        return {
            "PrimaryContainer": {
                "Image": "xgboost:governed",
                "ModelDataUrl": "s3://private/model.tar.gz",
            }
        }

    def list_training_jobs(self, **kwargs):
        return {
            "TrainingJobSummaries": [
                {"TrainingJobName": "jh-ai-classifier-training"},
                {"TrainingJobName": "unrelated-training"},
            ]
        }

    def describe_training_job(self, TrainingJobName):
        return {
            "TrainingJobStatus": "Completed",
            "AlgorithmSpecification": {"TrainingImage": "xgboost:governed"},
            "ModelArtifacts": {"S3ModelArtifacts": "s3://private/output.tar.gz"},
        }


class RolloutSafetyTests(unittest.TestCase):
    def test_account_mismatch_fails_before_mutation(self):
        operation = subject({"sts": FakeSts("000000000000")}, apply=False)
        with self.assertRaisesRegex(rollout.RolloutError, "account mismatch"):
            operation.verify_target()

    def test_plan_discovers_only_managed_live_canary_candidates(self):
        sagemaker = DiscoverySageMaker()
        operation = subject({"sagemaker": sagemaker}, apply=False)
        operation.discover_canary_candidates()
        candidates = operation.report.data["canary_candidates"]
        self.assertEqual(
            candidates["managed_in_service_endpoints"],
            [
                {
                    "endpoint_name": "jh-ai-blue",
                    "endpoint_config": "jh-ai-blue-config",
                    "variants": ["AllTraffic"],
                    "created_at": None,
                }
            ],
        )
        self.assertEqual(
            candidates["approved_model_packages"][0]["model_package_arn"],
            "arn:package:approved/7",
        )
        self.assertEqual(sagemaker.endpoint_pages, 2)
        self.assertEqual(sagemaker.package_pages, 2)
        inventory = operation.report.data["legacy_sagemaker_inventory"]
        self.assertEqual(len(inventory["ai_endpoints"]), 2)
        self.assertEqual(
            inventory["ai_models"][0]["model_name"], "jh-ai-classifier-model"
        )
        self.assertEqual(
            inventory["ai_training_jobs"][0]["status"], "Completed"
        )

    def test_failed_stack_is_reported_and_not_accepted(self):
        cfn = FakeFailedCloudFormation()
        operation = subject({"cloudformation": cfn})
        with self.assertRaisesRegex(rollout.RolloutError, "CREATE_FAILED"):
            operation.deploy_stack("test-stack", rollout.BASE_TEMPLATE)
        self.assertTrue(hasattr(cfn, "executed"))

    def test_brain_checksum_mismatch_fails_closed(self):
        operation = subject({"s3": BrainChecksumS3(corrupt=True)})
        with self.assertRaisesRegex(rollout.RolloutError, "checksum"):
            operation.migrate_brain("private-brain", "arn:kms:test")

    def test_brain_checksum_and_version_are_verified(self):
        operation = subject({"s3": BrainChecksumS3(corrupt=False)})
        evidence = operation.migrate_brain("private-brain", "arn:kms:test")
        self.assertEqual(evidence["destination_version_id"], "dest-v1")
        self.assertEqual(
            evidence["sha256"],
            rollout.sha256_bytes(b'{"brain":"canonical"}'),
        )

    def test_policy_merge_preserves_unrelated_statements(self):
        unrelated = {
            "Sid": "KeepUnrelatedAccess",
            "Effect": "Allow",
            "Principal": {"AWS": "arn:aws:iam::123456789012:root"},
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::example/public/*",
        }
        existing_stale_reviewed = {
            "Sid": "DenyPublicBrainReads",
            "Effect": "Deny",
            "Principal": "*",
            "Action": "s3:GetObject",
            "Resource": "arn:aws:s3:::stale/*",
        }
        s3 = PolicyS3(
            {
                "Version": "2012-10-17",
                "Statement": [unrelated, existing_stale_reviewed],
            }
        )
        operation = subject({"s3": s3})
        evidence = operation.merge_dashboard_policy()
        self.assertEqual(evidence["unrelated_statement_count"], 1)
        self.assertEqual(s3.policy["Statement"][0], unrelated)
        self.assertEqual(
            {item["Sid"] for item in s3.policy["Statement"]},
            {
                "KeepUnrelatedAccess",
                "DenyPublicBrainReads",
                "DenyDashboardBrainWrites",
            },
        )

    def test_lambda_rollback_restores_code_role_environment_and_aliases(self):
        lam = MutableLambda()
        operation = subject({"lambda": lam})
        operation.mutated_lambda = True
        operation.rollback_state = {
            "original_code_sha256": "old-code",
            "code_backup": {
                "bucket": "private",
                "key": "backup.zip",
                "sha256": "backup-sha",
                "version_id": "v1",
            },
            "role": "old-role",
            "environment": {"SECRET_KEY": "still-secret", "OTHER": "old"},
            "published_version": "17",
            "aliases": [
                {
                    "Name": "live",
                    "FunctionVersion": "17",
                    "Description": "old alias",
                    "RoutingConfig": {},
                }
            ],
        }
        operation.report.data["rollback"] = {}
        operation.rollback_lambda()
        self.assertEqual(lam.role, "old-role")
        self.assertEqual(
            lam.environment, {"SECRET_KEY": "still-secret", "OTHER": "old"}
        )
        self.assertEqual(lam.alias_updates[0]["FunctionVersion"], "17")
        self.assertTrue(operation.report.data["rollback"]["verified"])

    def test_canary_failure_keeps_all_rules_and_mappings_disabled(self):
        lam = MutableLambda()
        events = MutableEvents()
        operation = subject({"lambda": lam, "events": events})
        with self.assertRaisesRegex(rollout.RolloutError, "activation gate failed"):
            operation.activate(canary_ok=False, alarms_ok=True, rollback_ok=True)
        self.assertEqual(set(lam.mapping_states.values()), {"Disabled"})
        self.assertEqual(set(events.states.values()), {"DISABLED"})
        self.assertEqual(
            operation.report.data["activation"],
            {"rules": "disabled", "mappings": "disabled"},
        )

    def test_success_activates_exactly_three_mappings_and_rules(self):
        lam = MutableLambda()
        events = MutableEvents()
        for uuid in lam.mapping_states:
            lam.mapping_states[uuid] = "Disabled"
        for name in events.states:
            events.states[name] = "DISABLED"
        operation = subject({"lambda": lam, "events": events})
        operation.activate(canary_ok=True, alarms_ok=True, rollback_ok=True)
        self.assertEqual(set(lam.mapping_states.values()), {"Enabled"})
        self.assertEqual(set(events.states.values()), {"ENABLED"})
        self.assertEqual(operation.report.data["activation"]["rules"], "enabled")
        self.assertEqual(operation.report.data["activation"]["mappings"], "enabled")

    def test_apply_requires_exact_approval_phrase(self):
        with self.assertRaises(SystemExit):
            rollout.parse_args(["--apply", "--approval-phrase", "almost"])
        parsed = rollout.parse_args(
            ["--apply", "--approval-phrase", rollout.APPROVAL_PHRASE]
        )
        self.assertTrue(parsed.apply)


if __name__ == "__main__":
    unittest.main()
