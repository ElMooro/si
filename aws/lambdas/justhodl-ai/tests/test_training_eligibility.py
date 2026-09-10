from __future__ import annotations

import sys
import hashlib
import io
import json
import unittest
from datetime import datetime, timedelta, timezone
from pathlib import Path

SRC = Path(__file__).resolve().parents[1] / "source"
sys.path.insert(0, str(SRC))

import training  # noqa: E402
from training_eligibility import (  # noqa: E402
    TrainingEligibilityError,
    S3TrainingEvidenceResolver,
    evaluate_training_eligibility,
    split_digest,
)
from validation_splits import (  # noqa: E402
    SampleInterval,
    combinatorial_purged_cv_splits,
    purged_walk_forward_splits,
)


T0 = datetime(2026, 1, 1, tzinfo=timezone.utc)


def evidence(
    training_uri="s3://private/ai/datasets/train/",
    validation_uri="s3://private/ai/datasets/validation/",
):
    samples = []
    intervals = []
    for index in range(12):
        start = T0 + timedelta(days=index)
        end = start + timedelta(hours=1)
        intervals.append(SampleInterval(start, end))
        samples.append(
            {
                "sample_id": "sample-%02d" % index,
                "outcome": {
                    "schema_version": "1.0",
                    "label": "POSITIVE" if index % 2 else "NEGATIVE",
                    "entry_time": start.isoformat(),
                    "exit_time": end.isoformat(),
                    "net_return": 0.02 if index % 2 else -0.02,
                    "transaction_cost": 0.001,
                },
            }
        )
    walk = purged_walk_forward_splits(
        intervals,
        n_splits=2,
        test_size=2,
        min_train_size=4,
        purge=timedelta(minutes=1),
        embargo=timedelta(minutes=1),
    )
    cpcv = combinatorial_purged_cv_splits(
        intervals,
        n_groups=4,
        test_groups=1,
        purge=timedelta(minutes=1),
        embargo=timedelta(minutes=1),
    )

    def folds(values):
        return [
            {
                "split": index,
                "status": "COMPLETED",
                "test_count": len(value.test_indices),
                "metric": {"name": "balanced_accuracy", "value": 0.6},
            }
            for index, value in enumerate(values)
        ]

    return {
        "schema_version": "1.0",
        "purpose": "MARKET_OUTCOME_CLASSIFICATION",
        "dataset_id": "outcomes-v1",
        "training_uri": training_uri,
        "validation_uri": validation_uri,
        "dataset_digest": "a" * 64,
        "samples": samples,
        "evaluation": {
            "walk_forward": {
                "n_splits": 2,
                "test_size": 2,
                "min_train_size": 4,
                "purge_seconds": 60,
                "embargo_seconds": 60,
                "split_digest": split_digest(walk),
                "fold_results": folds(walk),
            },
            "cpcv": {
                "n_groups": 4,
                "test_groups": 1,
                "purge_seconds": 60,
                "embargo_seconds": 60,
                "split_digest": split_digest(cpcv),
                "fold_results": folds(cpcv),
            },
        },
    }


class FakeVersionedS3:
    def __init__(self):
        self.objects = {}
        self.current = {}
        self.current_versions = {}
        self.materialization_writes = 0

    def put_json(self, key, value, version):
        raw = json.dumps(value, sort_keys=True, separators=(",", ":")).encode()
        self.objects[("private", key, version)] = raw
        self.current[("private", key)] = raw
        return {
            "uri": "s3://private/" + key,
            "version_id": version,
            "sha256": hashlib.sha256(raw).hexdigest(),
        }

    def put_bytes(self, key, raw, version):
        self.objects[("private", key, version)] = raw
        self.current[("private", key)] = raw
        return {
            "uri": "s3://private/" + key,
            "version_id": version,
            "sha256": hashlib.sha256(raw).hexdigest(),
        }

    def put_object(self, Bucket, Key, Body, IfNoneMatch=None, **kwargs):
        if Bucket == "training-inputs" and not (
            Key.startswith("sha256/")
            and (
                Key.endswith("/train/payload.data")
                or Key.endswith("/validation/payload.data")
            )
        ):
            raise RuntimeError("bucket policy denied sibling object")
        if IfNoneMatch == "*" and (Bucket, Key) in self.current:
            raise RuntimeError("PreconditionFailed")
        raw = Body if isinstance(Body, bytes) else str(Body).encode()
        self.current[(Bucket, Key)] = raw
        self.materialization_writes += 1
        version = "immutable-v%d" % self.materialization_writes
        self.current_versions[(Bucket, Key)] = version
        self.objects[(Bucket, Key, version)] = raw
        return {"VersionId": version}

    def get_object(self, Bucket, Key, VersionId=None):
        if VersionId is None:
            raw = self.current[(Bucket, Key)]
            return {
                "Body": io.BytesIO(raw),
                "VersionId": self.current_versions.get((Bucket, Key)),
            }
        return {
            "Body": io.BytesIO(self.objects[(Bucket, Key, VersionId)]),
            "VersionId": VersionId,
        }

    def list_objects_v2(self, Bucket, Prefix="", MaxKeys=1000):
        keys = sorted(
            key
            for bucket, key in self.current
            if bucket == Bucket and key.startswith(Prefix)
        )[:MaxKeys]
        return {"Contents": [{"Key": key} for key in keys], "KeyCount": len(keys)}


def trusted(root, *, s3=None, return_reference=False):
    s3 = s3 or FakeVersionedS3()
    training_key = root["training_uri"].split("s3://private/", 1)[1]
    validation_key = root["validation_uri"].split("s3://private/", 1)[1]
    if training_key.endswith("/"):
        training_key += "train.csv"
    if validation_key.endswith("/"):
        validation_key += "validation.csv"
    training_ref = s3.put_bytes(
        training_key,
        b"label,f1\n0,0.1\n1,0.9\n",
        "train-v1",
    )
    validation_ref = s3.put_bytes(
        validation_key,
        b"label,f1\n0,0.2\n1,0.8\n",
        "validation-v1",
    )
    dataset = {
        key: root[key]
        for key in (
            "schema_version",
            "purpose",
            "dataset_id",
            "training_uri",
            "validation_uri",
            "samples",
        )
    }
    dataset["training_input"] = training_ref
    dataset["validation_input"] = validation_ref
    evaluation = {
        "schema_version": root["schema_version"],
        "dataset_id": root["dataset_id"],
        "training_uri": root["training_uri"],
        "validation_uri": root["validation_uri"],
        "evaluation": root["evaluation"],
    }
    dataset_ref = s3.put_json(
        "ai/governance/training-evidence/v1/datasets/data.json",
        dataset,
        "dataset-v1",
    )
    evaluation_ref = s3.put_json(
        "ai/governance/training-evidence/v1/evaluations/eval.json",
        evaluation,
        "evaluation-v1",
    )
    receipt_ref = s3.put_json(
        "ai/governance/training-evidence/v1/receipts/receipt.json",
        {
            "schema_version": "1.0",
            "issuer": "justhodl-ai-evaluation-pipeline",
            "status": "VERIFIED",
            "dataset_artifact": dataset_ref,
            "evaluation_artifact": evaluation_ref,
        },
        "receipt-v1",
    )
    if return_reference:
        return s3, receipt_ref
    return S3TrainingEvidenceResolver(
        s3, "private", materialization_bucket="training-inputs"
    ).resolve(receipt_ref)


class FakeSageMaker:
    def __init__(self):
        self.training = []

    def create_training_job(self, **request):
        self.training.append(request)


class TrainingEligibilityTests(unittest.TestCase):
    def test_both_split_families_and_completed_fold_results_are_mandatory(self):
        checked = evaluate_training_eligibility(
            trusted(evidence()),
            expected_dataset_id="outcomes-v1",
            expected_training_uri="s3://private/ai/datasets/train/",
            expected_validation_uri="s3://private/ai/datasets/validation/",
        )
        self.assertEqual(checked.labels, ("NEGATIVE", "POSITIVE"))
        self.assertEqual(checked.sample_count, 12)
        self.assertEqual(len(checked.evidence_digest), 64)

        no_cpcv = evidence()
        del no_cpcv["evaluation"]["cpcv"]
        with self.assertRaisesRegex(TrainingEligibilityError, "cpcv"):
            evaluate_training_eligibility(trusted(no_cpcv))

        zero_embargo = evidence()
        zero_embargo["evaluation"]["walk_forward"]["embargo_seconds"] = 0
        with self.assertRaisesRegex(TrainingEligibilityError, "zero-gap"):
            evaluate_training_eligibility(trusted(zero_embargo))

        incomplete = evidence()
        incomplete["evaluation"]["cpcv"]["fold_results"].pop()
        with self.assertRaisesRegex(TrainingEligibilityError, "one completed"):
            evaluate_training_eligibility(trusted(incomplete))

    def test_classifier_launcher_cannot_bypass_verified_outcomes(self):
        sm = FakeSageMaker()
        with self.assertRaisesRegex(TrainingEligibilityError, "verified"):
            training.start_classifier_job(
                sm,
                role_arn="arn:role",
                train_uri="s3://private/ai/datasets/train/",
                validation_uri="s3://private/ai/datasets/validation/",
                out_uri="s3://private/output/",
                n_classes=2,
                instance_type="ml.m5.xlarge",
                max_runtime_s=600,
                spot=True,
                tags=[],
                eligibility=None,
            )
        checked = evaluate_training_eligibility(trusted(evidence()))
        with self.assertRaisesRegex(TrainingEligibilityError, "training_uri"):
            training.start_classifier_job(
                sm,
                role_arn="arn:role",
                train_uri="s3://private/other/",
                validation_uri="s3://private/ai/datasets/validation/",
                out_uri="s3://private/output/",
                n_classes=2,
                instance_type="ml.m5.xlarge",
                max_runtime_s=600,
                spot=True,
                tags=[],
                eligibility=checked,
            )
        result = training.start_classifier_job(
            sm,
            role_arn="arn:role",
            train_uri=checked.training_uri,
            validation_uri=checked.validation_uri,
            out_uri="s3://private/output/",
            n_classes=2,
            instance_type="ml.m5.xlarge",
            max_runtime_s=600,
            spot=True,
            tags=[],
            eligibility=checked,
        )
        environment = sm.training[-1]["Environment"]
        self.assertEqual(
            environment["JH_TRAINING_ELIGIBILITY_DIGEST"],
            checked.evidence_digest,
        )
        self.assertEqual(
            result["cpcv_split_digest"], checked.cpcv_split_digest
        )

    def test_finetune_launcher_requires_matching_verified_training_uri(self):
        sm = FakeSageMaker()
        checked = evaluate_training_eligibility(trusted(evidence()))
        spec = {
            "training_supported": True,
            "training_image": "123.dkr.ecr.us-east-1.amazonaws.com/train:1",
            "default_training_instance": "ml.g5.xlarge",
            "model_id": "model-1",
            "hyperparameters": {},
        }
        with self.assertRaisesRegex(TrainingEligibilityError, "training_uri"):
            training.start_jumpstart_finetune(
                sm,
                spec=spec,
                role_arn="arn:role",
                training_uri="s3://private/other/",
                out_uri="s3://private/output/",
                instance_type=None,
                max_runtime_s=600,
                spot=True,
                tags=[],
                eligibility=checked,
            )
        training.start_jumpstart_finetune(
            sm,
            spec=spec,
            role_arn="arn:role",
            training_uri=checked.training_uri,
            out_uri="s3://private/output/",
            instance_type=None,
            max_runtime_s=600,
            spot=True,
            tags=[],
            eligibility=checked,
        )
        self.assertEqual(len(sm.training), 1)

    def test_caller_authored_evidence_zero_digest_and_invented_metrics_fail(self):
        sm = FakeSageMaker()
        with self.assertRaisesRegex(TrainingEligibilityError, "server-resolved"):
            evaluate_training_eligibility(evidence())

        good = evidence()
        s3 = FakeVersionedS3()
        resolved = trusted(good, s3=s3)
        self.assertEqual(evaluate_training_eligibility(resolved).sample_count, 12)

        with self.assertRaisesRegex(TrainingEligibilityError, "non-zero"):
            S3TrainingEvidenceResolver(
                s3, "private", materialization_bucket="training-inputs"
            ).resolve(
                {
                    "uri": (
                        "s3://private/ai/governance/training-evidence/v1/"
                        "receipts/receipt.json"
                    ),
                    "version_id": "receipt-v1",
                    "sha256": "0" * 64,
                }
            )
        with self.assertRaisesRegex(TrainingEligibilityError, "only"):
            receipt_digest = hashlib.sha256(
                s3.objects[
                    (
                        "private",
                        "ai/governance/training-evidence/v1/"
                        "receipts/receipt.json",
                        "receipt-v1",
                    )
                ]
            ).hexdigest()
            S3TrainingEvidenceResolver(
                s3, "private", materialization_bucket="training-inputs"
            ).resolve(
                {
                    "uri": (
                        "s3://private/ai/governance/training-evidence/v1/"
                        "receipts/receipt.json"
                    ),
                    "version_id": "receipt-v1",
                    "sha256": receipt_digest,
                    "samples": good["samples"],
                    "evaluation": good["evaluation"],
                }
            )
        self.assertEqual(sm.training, [])

    def test_fake_missing_or_mutated_input_uri_never_reaches_sagemaker(self):
        sm = FakeSageMaker()
        s3, receipt_ref = trusted(
            evidence(),
            return_reference=True,
        )
        dataset_key = "ai/governance/training-evidence/v1/datasets/data.json"
        dataset = json.loads(s3.objects[("private", dataset_key, "dataset-v1")])
        dataset["training_uri"] = (
            "s3://private/ai/datasets/auditor-fake-does-not-exist/"
        )
        dataset_ref = s3.put_json(dataset_key, dataset, "dataset-fake")
        receipt_key = "ai/governance/training-evidence/v1/receipts/receipt.json"
        receipt = json.loads(s3.objects[("private", receipt_key, "receipt-v1")])
        receipt["dataset_artifact"] = dataset_ref
        receipt_ref = s3.put_json(receipt_key, receipt, "receipt-fake")
        with self.assertRaisesRegex(
            TrainingEligibilityError, "exact training_uri bytes"
        ):
            S3TrainingEvidenceResolver(
                s3, "private", materialization_bucket="training-inputs"
            ).resolve(receipt_ref)
        self.assertEqual(sm.training, [])

        s3, receipt_ref = trusted(evidence(), return_reference=True)
        s3.objects[
            (
                "private",
                "ai/datasets/train/train.csv",
                "train-v1",
            )
        ] = b"mutated after receipt"
        with self.assertRaisesRegex(TrainingEligibilityError, "digest"):
            S3TrainingEvidenceResolver(
                s3, "private", materialization_bucket="training-inputs"
            ).resolve(receipt_ref)
        self.assertEqual(sm.training, [])

    def test_sagemaker_receives_only_verified_content_addressed_objects(self):
        sm = FakeSageMaker()
        s3, receipt_ref = trusted(evidence(), return_reference=True)
        checked = evaluate_training_eligibility(
            S3TrainingEvidenceResolver(
                s3, "private", materialization_bucket="training-inputs"
            ).resolve(receipt_ref),
            expected_training_uri="s3://private/ai/datasets/train/",
            expected_validation_uri="s3://private/ai/datasets/validation/",
        )
        training.start_classifier_job(
            sm,
            role_arn="arn:role",
            train_uri=checked.training_uri,
            validation_uri=checked.validation_uri,
            out_uri="s3://private/output/",
            n_classes=2,
            instance_type="ml.m5.xlarge",
            max_runtime_s=600,
            spot=True,
            tags=[],
            eligibility=checked,
        )
        channels = {
            item["ChannelName"]: item["DataSource"]["S3DataSource"]["S3Uri"]
            for item in sm.training[0]["InputDataConfig"]
        }
        self.assertEqual(channels["train"], checked.training_uri)
        self.assertEqual(channels["validation"], checked.validation_uri)
        self.assertIn(
            "s3://training-inputs/sha256/",
            channels["train"],
        )
        self.assertTrue(channels["train"].endswith("/train/payload.data"))
        destination = channels["train"].split("s3://training-inputs/", 1)[1]
        s3.current[("training-inputs", destination)] = b"mutated destination"
        with self.assertRaisesRegex(TrainingEligibilityError, "mutated"):
            S3TrainingEvidenceResolver(
                s3, "private", materialization_bucket="training-inputs"
            ).resolve(receipt_ref)

    def test_write_once_policy_and_single_payload_check_reject_sibling_injection(self):
        s3, receipt_ref = trusted(evidence(), return_reference=True)
        checked = evaluate_training_eligibility(
            S3TrainingEvidenceResolver(
                s3, "private", materialization_bucket="training-inputs"
            ).resolve(receipt_ref)
        )
        key = checked.training_uri.split("s3://training-inputs/", 1)[1]
        with self.assertRaisesRegex(RuntimeError, "denied sibling"):
            s3.put_object(
                Bucket="training-inputs",
                Key=key.rsplit("/", 1)[0] + "/injected.data",
                Body=b"malicious sibling",
                IfNoneMatch="*",
            )
        # Reproduce a storage-side policy bypass: the resolver's exact-one
        # listing check still rejects consumption before SageMaker starts.
        sibling = key.rsplit("/", 1)[0] + "/injected.data"
        s3.current[("training-inputs", sibling)] = b"malicious sibling"
        s3.current_versions[("training-inputs", sibling)] = "injected-v1"
        with self.assertRaisesRegex(TrainingEligibilityError, "exactly one"):
            S3TrainingEvidenceResolver(
                s3, "private", materialization_bucket="training-inputs"
            ).resolve(receipt_ref)


if __name__ == "__main__":
    unittest.main()
