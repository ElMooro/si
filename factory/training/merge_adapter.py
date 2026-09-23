"""Merge the champion adapter into the base weights for serving (2026-09-23).

A promoted Gear B champion is only a LoRA adapter on S3; the owned endpoint serves the base weights, so without this
step no improvement ever reaches the daily read, the Monday wall or the chat. This job (SageMaker script mode, the same
pinned image and bundle as train_qlora.py) loads the base in bf16, applies the adapter, merge_and_unload()s it, and
uploads the merged model -- safetensors shards, config, tokenizer -- UNCOMPRESSED to `merged_prefix`, then writes
merge_manifest.json there LAST (per-file sha256 + bytes). scripts/factory_serve_champion.py deploys it.

Channels:  /opt/ml/input/data/model    base weights (staged, hashed)
           /opt/ml/input/data/adapter  the champion adapter (adapter_config.json somewhere inside)
Hyperparameters: merged_prefix (s3://bucket/prefix/), generation, base_revision
"""
from __future__ import annotations

import hashlib
import json
import os
import shutil
import sys
import time
from pathlib import Path

MODEL_DIR = Path(os.environ.get("SM_CHANNEL_MODEL", "/opt/ml/input/data/model"))
ADAPTER_DIR = Path(os.environ.get("SM_CHANNEL_ADAPTER", "/opt/ml/input/data/adapter"))
OUT_DIR = Path(os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
HP_FILE = Path("/opt/ml/input/config/hyperparameters.json")


def hyperparameters() -> dict:
    hp = {}
    if HP_FILE.exists():
        hp = json.loads(HP_FILE.read_text())
    for k, v in os.environ.items():
        if k.startswith("SM_HP_"):
            hp.setdefault(k[6:].lower(), v)
    return hp


def file_sha256(path: Path) -> str:
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def find_adapter(root: Path) -> Path:
    hits = sorted(root.rglob("adapter_config.json"))
    if not hits:
        raise SystemExit("no adapter_config.json under %s" % root)
    return hits[0].parent


def workdir() -> Path:
    for cand in (Path("/opt/ml/input/merged"), Path("/tmp/merged")):
        try:
            cand.mkdir(parents=True, exist_ok=True)
            (cand / ".probe").write_text("ok"); (cand / ".probe").unlink()
            return cand
        except OSError:
            continue
    raise SystemExit("no writable work directory")


def main() -> int:
    hp = hyperparameters()
    prefix = str(hp.get("merged_prefix") or "")
    if not prefix.startswith("s3://") or not prefix.endswith("/"):
        raise SystemExit("merged_prefix must be s3://bucket/prefix/")
    bucket, key_prefix = prefix[5:].split("/", 1)
    started = time.time()
    import boto3
    import torch
    from peft import PeftModel
    from transformers import AutoModelForCausalLM, AutoTokenizer
    adapter = find_adapter(ADAPTER_DIR)
    model = AutoModelForCausalLM.from_pretrained(str(MODEL_DIR), torch_dtype=torch.bfloat16, device_map={"": "cpu"}, low_cpu_mem_usage=True)
    model = PeftModel.from_pretrained(model, str(adapter))
    model = model.merge_and_unload()
    work = workdir()
    model.save_pretrained(str(work), safe_serialization=True, max_shard_size="4GB")
    AutoTokenizer.from_pretrained(str(MODEL_DIR), use_fast=True).save_pretrained(str(work))
    for extra in MODEL_DIR.iterdir():        # small files the base ships that save_pretrained does not rewrite (generation_config, license, ...)
        if extra.is_file() and not (work / extra.name).exists() and extra.stat().st_size < 50 * 1024 * 1024:
            shutil.copy2(extra, work / extra.name)
    s3 = boto3.client("s3")
    files = {}
    for path in sorted(p for p in work.iterdir() if p.is_file() and not p.name.startswith(".")):
        files[path.name] = {"bytes": path.stat().st_size, "sha256": file_sha256(path)}
        s3.upload_file(str(path), bucket, key_prefix + path.name)
    manifest = {"schema_version": "factory-merged-model.v1", "generation": hp.get("generation"), "base_revision": hp.get("base_revision"),
                "adapter_sha256": {p.name: file_sha256(p) for p in sorted(adapter.iterdir()) if p.is_file()},
                "merged_prefix": prefix, "files": files, "seconds": round(time.time() - started, 1),
                "at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    s3.put_object(Bucket=bucket, Key=key_prefix + "merge_manifest.json", Body=json.dumps(manifest, indent=1).encode(), ContentType="application/json")
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    (OUT_DIR / "merge_manifest.json").write_text(json.dumps(manifest, indent=1))
    shutil.rmtree(work, ignore_errors=True)
    print(json.dumps({k: manifest[k] for k in ("generation", "merged_prefix", "seconds")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
