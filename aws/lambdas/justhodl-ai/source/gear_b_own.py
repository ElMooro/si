"""Gear B, owned model source (2026-09-13): the student's base weights, training recipe and image live in
Khalid's own S3/ECR. No hub card is needed and no hub recipe is trusted.

  factory/models/base/<model_id>/manifest.json   -- written by the staging job (factory/training/stage_base_weights.py):
                                                    repo, revision, license, files[{path, sha256, bytes}], total_bytes
  factory/training/current.json                  -- written by scripts/factory_training_pin.py:
                                                    training_image (pinned by digest when known), bundle_key + bundle_sha256,
                                                    program (train_qlora.py), requirements sha, pinned_at
  control (factory/control/gearb.json)           -- model_source: "own", model_id: e.g. qwen2-5-coder-7b-instruct

own_spec() returns the same shape launch_sft() reads from a hub card, so the refusal chain, budget check,
spot, cap and job record are unchanged. Refuses when the license is not permissive, a file lacks a sha256,
the manifest and control disagree, or the image is not the pinned one.
"""
from __future__ import annotations

import json
import re
from typing import Any, Dict

BASE_PREFIX = "factory/models/base/"
PIN_KEY = "factory/training/current.json"
PERMISSIVE = ("apache-2.0", "mit", "bsd-3-clause", "bsd-2-clause")
IMAGE_RX = re.compile(r"^[0-9]{12}\.dkr\.ecr\.[a-z0-9-]+\.amazonaws\.com/[a-z0-9._/-]+:[A-Za-z0-9._-]+(@sha256:[0-9a-f]{64})?$")
HEX64 = re.compile(r"^[0-9a-f]{64}$")


class OwnSpecRefused(RuntimeError):
    pass


def is_metadata_path(path: str) -> bool:
    """Hub download caches and SageMaker upload markers are not weights and carry no hash."""
    return path.startswith(".cache/") or path.endswith(".sagemaker-uploaded") or path.endswith("/.gitignore") or path == ".gitattributes"


def _get_json(s3, bucket: str, key: str):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001
        return None


def validate_base_manifest(manifest: Dict[str, Any], model_id: str) -> Dict[str, Any]:
    if not isinstance(manifest, dict) or manifest.get("schema_version") != "factory-base-weights.v1":
        raise OwnSpecRefused("base manifest missing or wrong schema for %s" % model_id)
    if manifest.get("model_id") != model_id:
        raise OwnSpecRefused("base manifest model_id %s != control %s" % (manifest.get("model_id"), model_id))
    lic = str(manifest.get("license") or "").lower()
    if lic not in PERMISSIVE:
        raise OwnSpecRefused("license %r is not permissive; refusing to train on it" % lic)
    files = [f for f in (manifest.get("files") or []) if not is_metadata_path(str(f.get("path") or ""))]
    if not files or not any(f.get("path") == "config.json" for f in files):
        raise OwnSpecRefused("base weights incomplete: config.json not listed")
    for f in files:
        if not HEX64.match(str(f.get("sha256") or "")) or int(f.get("bytes") or 0) <= 0:
            raise OwnSpecRefused("unhashed or empty file in base manifest: %s" % f.get("path"))
    if not any(str(f.get("path", "")).endswith((".safetensors", ".bin")) for f in files):
        raise OwnSpecRefused("base weights incomplete: no weight shards listed")
    if not manifest.get("revision") or not manifest.get("s3_prefix", "").startswith("s3://"):
        raise OwnSpecRefused("base manifest lacks revision or s3_prefix")
    return manifest


def validate_pin(pin: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(pin, dict) or pin.get("schema_version") != "factory-training-pin.v1":
        raise OwnSpecRefused("training pin missing (run scripts/factory_training_pin.py)")
    image = str(pin.get("training_image") or "")
    if not IMAGE_RX.match(image):
        raise OwnSpecRefused("training image is not an ECR uri: %r" % image[:80])
    if pin.get("require_digest") and "@sha256:" not in image:
        raise OwnSpecRefused("training image is not pinned by digest")
    if not str(pin.get("bundle_uri") or "").startswith("s3://") or not HEX64.match(str(pin.get("bundle_sha256") or "")):
        raise OwnSpecRefused("training bundle is not pinned by sha256")
    if pin.get("program") != "train_qlora.py":
        raise OwnSpecRefused("training program must be train_qlora.py")
    return pin


def own_spec(s3, private_bucket: str, control: Dict[str, Any]) -> Dict[str, Any]:
    """The spec launch_sft() expects, built from Khalid's own S3/ECR only."""
    model_id = str(control.get("model_id") or "")
    manifest = validate_base_manifest(_get_json(s3, private_bucket, BASE_PREFIX + model_id + "/manifest.json"), model_id)
    pin = validate_pin(_get_json(s3, private_bucket, PIN_KEY))
    hyper = {"sagemaker_program": {"default": pin["program"]},
             "base_revision": {"default": manifest["revision"]},
             "base_manifest_sha256": {"default": manifest.get("manifest_sha256") or ""},
             "epochs": {"default": str(pin.get("epochs", 1))},
             "max_steps": {"default": str(pin.get("max_steps", 400))},
             "lora_r": {"default": str(pin.get("lora_r", 16))},
             "learning_rate": {"default": str(pin.get("learning_rate", "2e-4"))},
             "max_seq_len": {"default": str(pin.get("max_seq_len", 2048))},
             "load_in_4bit": {"default": "true"}}
    return {"model_id": model_id, "model_source": "own", "version": manifest["revision"],
            "display_name": manifest.get("repo") or model_id, "license": manifest["license"],
            "training_supported": True, "training_image": pin["training_image"],
            "training_artifact": manifest["s3_prefix"], "training_script": pin["bundle_uri"],
            "training_bundle_sha256": pin["bundle_sha256"], "hyperparameters": hyper,
            "default_training_instance": pin.get("default_instance", "ml.g5.2xlarge"),
            "supported_training_instances": pin.get("instances", ["ml.g5.2xlarge", "ml.g5.4xlarge", "ml.g5.12xlarge"]),
            "gated": False, "fetched_at": pin.get("pinned_at")}


def burst_spec(s3, private_bucket: str, control: Dict[str, Any], *, mode: str = "burst", adapter_uri: str = None,
               tasks_uri: str = None, samples_per_task: int = 4, temperature: float = 0.8) -> Dict[str, Any]:
    """Spec for a trace burst / exam job on the owned image: same weights, same bundle, generate.py as the program.
    Bursts sample K candidates per task (creativity); the unprivileged verifier turns passes into training rows."""
    base = own_spec(s3, private_bucket, control)
    hyper = {"sagemaker_program": {"default": "generate.py"}, "mode": {"default": mode},
             "samples_per_task": {"default": str(samples_per_task if mode == "burst" else 1)},
             "temperature": {"default": str(temperature if mode == "burst" else 0.0)},
             "max_new_tokens": {"default": "1024"}, "task_cap": {"default": "2000"},
             "adapter_generation": {"default": (adapter_uri or "base").rstrip("/").split("/")[-1]}}
    spec = {**base, "kind": mode, "hyperparameters": hyper, "adapter_uri": adapter_uri, "tasks_uri": tasks_uri}
    if not tasks_uri or not str(tasks_uri).startswith("s3://"):
        raise OwnSpecRefused("tasks_uri (s3://) is required for a %s job" % mode)
    return spec
