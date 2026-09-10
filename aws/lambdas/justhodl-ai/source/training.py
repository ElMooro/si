"""training -- the four training tiers the AI page exposes, all on the plain SageMaker API.

Tier 1  TRANSFER LEARNING (the article): RoBERTa-SEC embeddings -> a classifier trained by the
        XGBoost built-in (CSV, label first). Minutes, cents (spot), and deployable serverless.
Tier 2  JUMPSTART FINE-TUNE: any hub card with TrainingSupported (transfer_learning.py recipe:
        pretrained weights as the `model` channel, the script bundle via sagemaker_submit_directory).
Tier 3  AUTOPILOT / AutoML V2: text classification (fine-tunes a transformer on the Brain rows
        with the operator's labels) or tabular (any warehouse CSV + target column); Autopilot
        explores candidates and picks the best -- the "most powerful managed" tier.
Tier 4  HYPERPOD: resilient GPU clusters for from-scratch runs. Gated: refused unless the policy
        unlock is set and the caller types the confirmation phrase, because it bills per node-hour.

Every job carries MaxRuntimeInSeconds, tags, and lands in the private bucket.
"""
from __future__ import annotations

import re
import time
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from training_eligibility import (
    TrainingEligibility,
    TrainingEligibilityError,
    require_training_eligibility,
)

REGION = "us-east-1"
# SageMaker built-in XGBoost (framework-mode image, us-east-1 registry account 683313688378).
XGB_IMAGE = "683313688378.dkr.ecr.us-east-1.amazonaws.com/sagemaker-xgboost:1.7-1"
HYPERPOD_CONFIRM = "I UNDERSTAND HYPERPOD BILLS PER NODE HOUR"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def _name(prefix: str) -> str:
    now = datetime.now(timezone.utc)
    return re.sub(r"[^A-Za-z0-9-]", "-", "%s-%s-%03d" % (prefix, now.strftime("%Y%m%d-%H%M%S"), now.microsecond // 1000))[:63]


def _stop(max_runtime_s: int, spot: bool) -> Dict[str, int]:
    sc = {"MaxRuntimeInSeconds": int(max_runtime_s)}
    if spot:
        sc["MaxWaitTimeInSeconds"] = int(max_runtime_s) * 2
    return sc


def _safe_name(base: str, kind: str, stamp: str) -> str:
    """SageMaker names: <=63 chars, [a-zA-Z0-9](-*[a-zA-Z0-9]){0,62} -- never end on a dash (ops 5302)."""
    tail = ("-%s-%s" % (kind, stamp)) if kind else ("-%s" % stamp)
    name = (base[: 63 - len(tail)].rstrip("-") + tail)[:63]
    return re.sub(r"-+", "-", name).strip("-")


def _stamp() -> str:
    """Second-precision names collided when a serverless attempt and its real-time fallback ran in the same
    second (ops 5301) -- millisecond + counter stamps never do."""
    global _STAMP_N
    _STAMP_N = globals().get("_STAMP_N", 0) + 1
    return "%d-%d" % (int(time.time() * 1000) % 10_000_000_000, _STAMP_N)

# ─────────────────────────────────────────────────────────────────── tier 1
def start_classifier_job(sm, *, role_arn: str, train_uri: str, validation_uri: str, out_uri: str, n_classes: int,
                         instance_type: str, max_runtime_s: int, spot: bool, tags: List[dict], num_round: int = 300,
                         max_depth: int = 4, eta: float = 0.08, job_name: Optional[str] = None,
                         eligibility: TrainingEligibility) -> Dict[str, Any]:
    eligibility = require_training_eligibility(eligibility)
    if train_uri != eligibility.training_uri:
        raise TrainingEligibilityError(
            "classifier input must match verified eligibility training_uri"
        )
    if validation_uri != eligibility.validation_uri:
        raise TrainingEligibilityError(
            "classifier input must match verified eligibility validation_uri"
        )
    if int(n_classes) != len(eligibility.labels):
        raise TrainingEligibilityError(
            "n_classes must match verified outcome-label classes"
        )
    name = job_name or _name("jh-ai-brain-clf")
    hp = {"objective": "multi:softprob", "num_class": str(int(n_classes)), "num_round": str(int(num_round)), "max_depth": str(int(max_depth)),
          "eta": str(eta), "subsample": "0.85", "colsample_bytree": "0.7", "min_child_weight": "2", "eval_metric": "mlogloss",
          "early_stopping_rounds": "30", "verbosity": "1"}
    kw = dict(
        TrainingJobName=name, RoleArn=role_arn,
        AlgorithmSpecification={"TrainingImage": XGB_IMAGE, "TrainingInputMode": "File"},
        HyperParameters=hp,
        InputDataConfig=[
            {"ChannelName": "train", "ContentType": "text/csv", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": train_uri, "S3DataDistributionType": "FullyReplicated"}}},
            {"ChannelName": "validation", "ContentType": "text/csv", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": validation_uri, "S3DataDistributionType": "FullyReplicated"}}},
        ],
        OutputDataConfig={"S3OutputPath": out_uri},
        ResourceConfig={"InstanceType": instance_type, "InstanceCount": 1, "VolumeSizeInGB": 20},
        StoppingCondition=_stop(max_runtime_s, spot),
        EnableManagedSpotTraining=bool(spot), Tags=tags,
        Environment={
            "JH_TRAINING_ELIGIBILITY_DIGEST": eligibility.evidence_digest,
            "JH_DATASET_DIGEST": eligibility.dataset_digest,
            "JH_TRAINING_INPUT_DIGEST": eligibility.training_input_digest,
            "JH_VALIDATION_INPUT_DIGEST": eligibility.validation_input_digest,
            "JH_TRAINING_INPUT_VERSION_ID": eligibility.training_input_version_id,
            "JH_VALIDATION_INPUT_VERSION_ID": eligibility.validation_input_version_id,
            "JH_WALK_FORWARD_SPLIT_DIGEST": eligibility.walk_forward_split_digest,
            "JH_CPCV_SPLIT_DIGEST": eligibility.cpcv_split_digest,
        },
    )
    sm.create_training_job(**kw)
    return {
        "job_name": name,
        "tier": 1,
        "image": XGB_IMAGE,
        "instance_type": instance_type,
        "spot": spot,
        "hyperparameters": hp,
        "training_eligibility_digest": eligibility.evidence_digest,
        "walk_forward_split_digest": eligibility.walk_forward_split_digest,
        "cpcv_split_digest": eligibility.cpcv_split_digest,
        "started_at": now_iso(),
    }


# ─────────────────────────────────────────────────────────────────── tier 2
def start_jumpstart_finetune(sm, *, spec: dict, role_arn: str, training_uri: str, out_uri: str, instance_type: Optional[str],
                             max_runtime_s: int, spot: bool, tags: List[dict], hyperparameters: Optional[Dict[str, str]] = None,
                             job_name: Optional[str] = None, eligibility: TrainingEligibility) -> Dict[str, Any]:
    eligibility = require_training_eligibility(eligibility)
    if training_uri != eligibility.training_uri:
        raise TrainingEligibilityError(
            "fine-tune input must match verified eligibility training_uri"
        )
    if not spec.get("training_supported") or not spec.get("training_image"):
        raise RuntimeError("hub card %s does not publish a training recipe (TrainingSupported=%s); keys=%s" % (spec.get("model_id"), spec.get("training_supported"), spec.get("doc_keys")))
    it = instance_type or spec.get("default_training_instance")
    if not it:
        raise RuntimeError("no training instance type: pass one or the card must publish DefaultTrainingInstanceType")
    hp = {k: str(v["default"]) for k, v in (spec.get("hyperparameters") or {}).items() if v.get("default") is not None}
    hp.update({str(k): str(v) for k, v in (hyperparameters or {}).items()})
    if spec.get("training_script"):
        hp["sagemaker_program"] = hp.get("sagemaker_program", "transfer_learning.py")
        hp["sagemaker_submit_directory"] = spec["training_script"]
        hp.setdefault("sagemaker_container_log_level", "20")
        hp.setdefault("sagemaker_job_name", job_name or "")
        hp.setdefault("sagemaker_region", REGION)
    name = job_name or _name("jh-ai-ft-%s" % spec["model_id"][:20])
    hp["sagemaker_job_name"] = name
    channels = [{"ChannelName": "training", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": training_uri, "S3DataDistributionType": "FullyReplicated"}}}]
    if spec.get("training_artifact"):
        channels.append({"ChannelName": "model", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": spec["training_artifact"], "S3DataDistributionType": "FullyReplicated"}}})
    gpu = bool(re.match(r"^ml\.(g|p|trn)", it))
    sm.create_training_job(
        TrainingJobName=name, RoleArn=role_arn,
        AlgorithmSpecification={"TrainingImage": spec["training_image"], "TrainingInputMode": "File"},
        HyperParameters=hp, InputDataConfig=channels, OutputDataConfig={"S3OutputPath": out_uri},
        ResourceConfig={"InstanceType": it, "InstanceCount": 1, "VolumeSizeInGB": 100 if gpu else 30},
        StoppingCondition=_stop(max_runtime_s, spot), EnableManagedSpotTraining=bool(spot), Tags=tags,
        Environment={
            "JH_TRAINING_ELIGIBILITY_DIGEST": eligibility.evidence_digest,
            "JH_DATASET_DIGEST": eligibility.dataset_digest,
            "JH_TRAINING_INPUT_DIGEST": eligibility.training_input_digest,
            "JH_VALIDATION_INPUT_DIGEST": eligibility.validation_input_digest,
            "JH_TRAINING_INPUT_VERSION_ID": eligibility.training_input_version_id,
            "JH_VALIDATION_INPUT_VERSION_ID": eligibility.validation_input_version_id,
            "JH_WALK_FORWARD_SPLIT_DIGEST": eligibility.walk_forward_split_digest,
            "JH_CPCV_SPLIT_DIGEST": eligibility.cpcv_split_digest,
        },
    )
    return {"job_name": name, "tier": 2, "model_id": spec["model_id"], "image": spec["training_image"], "instance_type": it, "spot": spot,
            "hyperparameters": {k: v for k, v in hp.items() if not k.startswith("sagemaker_")},
            "training_eligibility_digest": eligibility.evidence_digest,
            "walk_forward_split_digest": eligibility.walk_forward_split_digest,
            "cpcv_split_digest": eligibility.cpcv_split_digest,
            "started_at": now_iso()}


# ─────────────────────────────────────────────────────────────────── tier 3
def start_autopilot_text(sm, *, role_arn: str, csv_uri: str, out_uri: str, max_runtime_s: int, tags: List[dict],
                         job_name: Optional[str] = None) -> Dict[str, Any]:
    name = job_name or _name("jh-ai-automl-text")
    sm.create_auto_ml_job_v2(
        AutoMLJobName=name, RoleArn=role_arn, Tags=tags,
        AutoMLJobInputDataConfig=[{"ChannelType": "training", "ContentType": "text/csv;header=present", "CompressionType": "None",
                                   "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": csv_uri}}}],
        OutputDataConfig={"S3OutputPath": out_uri},
        AutoMLProblemTypeConfig={"TextClassificationJobConfig": {"ContentColumn": "text", "TargetLabelColumn": "label",
                                                                  "CompletionCriteria": {"MaxAutoMLJobRuntimeInSeconds": int(max_runtime_s)}}},
    )
    return {"job_name": name, "tier": 3, "problem": "text-classification", "csv_uri": csv_uri, "started_at": now_iso()}


def start_autopilot_tabular(sm, *, role_arn: str, csv_uri: str, out_uri: str, target: str, problem_type: Optional[str],
                            max_runtime_s: int, max_candidates: int, tags: List[dict], job_name: Optional[str] = None) -> Dict[str, Any]:
    name = job_name or _name("jh-ai-automl-tab")
    cfg = {"TargetAttributeName": target, "Mode": "ENSEMBLING",
           "CompletionCriteria": {"MaxAutoMLJobRuntimeInSeconds": int(max_runtime_s), "MaxCandidates": int(max_candidates),
                                  "MaxRuntimePerTrainingJobInSeconds": int(max_runtime_s)}}
    if problem_type in ("BinaryClassification", "MulticlassClassification", "Regression"):
        cfg["ProblemType"] = problem_type
    sm.create_auto_ml_job_v2(
        AutoMLJobName=name, RoleArn=role_arn, Tags=tags,
        AutoMLJobInputDataConfig=[{"ChannelType": "training", "ContentType": "text/csv;header=present", "CompressionType": "None",
                                   "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": csv_uri}}}],
        OutputDataConfig={"S3OutputPath": out_uri},
        AutoMLProblemTypeConfig={"TabularJobConfig": cfg},
    )
    return {"job_name": name, "tier": 3, "problem": problem_type or "auto", "target": target, "csv_uri": csv_uri, "started_at": now_iso()}


# ────────────────────────────────────────────────────────── deploy trained
def deploy_training_output(sm, *, job_name: str, role_arn: str, endpoint_name: str, serverless: bool, instance_type: Optional[str],
                           tags: List[dict], serverless_memory_mb: int = 2048, serverless_max_conc: int = 2) -> Dict[str, Any]:
    d = sm.describe_training_job(TrainingJobName=job_name)
    if d.get("TrainingJobStatus") != "Completed":
        raise RuntimeError("training job %s is %s (%s)" % (job_name, d.get("TrainingJobStatus"), d.get("FailureReason") or d.get("SecondaryStatus")))
    art = (d.get("ModelArtifacts") or {}).get("S3ModelArtifacts")
    img = (d.get("AlgorithmSpecification") or {}).get("TrainingImage")
    if not art or not img:
        raise RuntimeError("job %s has no artifact/image" % job_name)
    # framework-mode training images double as hosting images for the built-ins
    model_name = _safe_name(endpoint_name, "", _stamp())
    sm.create_model(ModelName=model_name, ExecutionRoleArn=role_arn, Tags=tags,
                    PrimaryContainer={"Image": img, "ModelDataUrl": art, "Environment": {"SAGEMAKER_REGION": REGION}})
    cfg_name = _safe_name(endpoint_name, "cfg", _stamp())
    variant = {"VariantName": "AllTraffic", "ModelName": model_name}
    if serverless:
        variant["ServerlessConfig"] = {"MemorySizeInMB": int(serverless_memory_mb), "MaxConcurrency": int(serverless_max_conc)}
    else:
        variant.update({"InstanceType": instance_type, "InitialInstanceCount": 1})
    sm.create_endpoint_config(EndpointConfigName=cfg_name, ProductionVariants=[variant], Tags=tags)
    try:
        sm.describe_endpoint(EndpointName=endpoint_name)
        sm.update_endpoint(EndpointName=endpoint_name, EndpointConfigName=cfg_name)
        action = "updated"
    except Exception:
        sm.create_endpoint(EndpointName=endpoint_name, EndpointConfigName=cfg_name, Tags=tags)
        action = "created"
    return {"endpoint": endpoint_name, "model": model_name, "endpoint_config": cfg_name, "model_data": art, "image": img,
            "serverless": serverless, "action": action}


# ─────────────────────────────────────────────────────────────────── tier 4
def hyperpod_gate(policy: Dict[str, Any], confirm: str) -> Optional[str]:
    if not policy.get("hyperpod_unlocked"):
        return "HyperPod is locked by policy (hyperpod_unlocked=false). It is a persistent GPU cluster billed per node-hour; unlock it deliberately on the AI page."
    if (confirm or "").strip() != HYPERPOD_CONFIRM:
        return "type the confirmation phrase exactly: %s" % HYPERPOD_CONFIRM
    return None


def create_hyperpod_cluster(sm, *, name: str, role_arn: str, instance_type: str, count: int, lifecycle_s3: str, tags: List[dict]) -> Dict[str, Any]:
    sm.create_cluster(
        ClusterName=name, Tags=tags,
        InstanceGroups=[{"InstanceGroupName": "workers", "InstanceType": instance_type, "InstanceCount": int(count), "ExecutionRole": role_arn,
                         "LifeCycleConfig": {"SourceS3Uri": lifecycle_s3, "OnCreate": "on_create.sh"}, "ThreadsPerCore": 1}],
    )
    return {"cluster": name, "instance_type": instance_type, "count": int(count), "started_at": now_iso()}


# ─────────────────────────────────────────────────────────────── inventory
def list_jobs(sm, max_items: int = 40) -> List[dict]:
    out = []
    try:
        for j in (sm.list_training_jobs(MaxResults=min(max_items, 100), SortBy="CreationTime", SortOrder="Descending").get("TrainingJobSummaries") or []):
            out.append({"kind": "training", "name": j["TrainingJobName"], "status": j.get("TrainingJobStatus"), "created_at": str(j.get("CreationTime")),
                        "ended_at": str(j.get("TrainingEndTime") or ""), "secondary": j.get("SecondaryStatus")})
    except Exception as e:
        out.append({"kind": "training", "error": str(e)[:120]})
    try:
        for j in (sm.list_auto_ml_jobs(MaxResults=min(max_items, 100), SortBy="CreationTime", SortOrder="Descending").get("AutoMLJobSummaries") or []):
            out.append({"kind": "automl", "name": j["AutoMLJobName"], "status": j.get("AutoMLJobStatus"), "secondary": j.get("AutoMLJobSecondaryStatus"),
                        "created_at": str(j.get("CreationTime")), "ended_at": str(j.get("EndTime") or ""), "failure": (j.get("FailureReason") or "")[:160]})
    except Exception as e:
        out.append({"kind": "automl", "error": str(e)[:120]})
    return out
