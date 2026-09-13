#!/usr/bin/env python3
"""Owned QLoRA recipe for the student's base weights (SageMaker script mode; factory-training-pin.v1).

Inputs (SageMaker channels):
  /opt/ml/input/data/model      the staged base weights (factory/models/base/<model_id>/<revision>/)
  /opt/ml/input/data/training   keep-only-passes rows: jsonl {prompt, completion} built by gear_b.curate
Output:
  /opt/ml/model/adapter/        LoRA adapter (safetensors) + adapter_config.json
  /opt/ml/model/train_manifest.json  rows, tokens, steps, loss curve, base revision, adapter sha256 -- the exam
                                     job (never this script) decides whether the adapter is promoted.
Hyperparameters arrive as strings from launch_sft (sagemaker_program, base_revision, max_steps, lora_r, ...).
No network is needed at train time; everything the job reads is in its channels.
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

MODEL_DIR = Path(os.environ.get("SM_CHANNEL_MODEL", "/opt/ml/input/data/model"))
TRAIN_DIR = Path(os.environ.get("SM_CHANNEL_TRAINING", "/opt/ml/input/data/training"))
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


def as_int(v, default):
    try:
        return int(str(v))
    except (TypeError, ValueError):
        return default


def as_float(v, default):
    try:
        return float(str(v))
    except (TypeError, ValueError):
        return default


def load_rows(train_dir: Path, max_rows: int = 200000):
    rows = []
    for path in sorted(train_dir.rglob("*.jsonl")):
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if isinstance(row, dict) and isinstance(row.get("prompt"), str) and isinstance(row.get("completion"), str):
                rows.append({"prompt": row["prompt"], "completion": row["completion"]})
            if len(rows) >= max_rows:
                return rows
    return rows


def dir_sha256(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        if p.is_file():
            h.update(p.relative_to(root).as_posix().encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def main() -> int:
    hp = hyperparameters()
    max_steps = as_int(hp.get("max_steps"), 400)
    lora_r = as_int(hp.get("lora_r"), 16)
    lr = as_float(hp.get("learning_rate"), 2e-4)
    max_seq_len = as_int(hp.get("max_seq_len"), 2048)
    epochs = as_int(hp.get("epochs"), 1)
    load_in_4bit = str(hp.get("load_in_4bit", "true")).lower() == "true"
    rows = load_rows(TRAIN_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {"schema_version": "factory-train-manifest.v1", "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "base_revision": hp.get("base_revision"), "base_manifest_sha256": hp.get("base_manifest_sha256"),
                "eligibility_digest": os.environ.get("JH_GEARB_ELIGIBILITY_DIGEST"), "train_sha256": os.environ.get("JH_GEARB_TRAIN_SHA256"),
                "holdout_digest": os.environ.get("JH_GEARB_HOLDOUT_DIGEST"), "rows": len(rows),
                "hyperparameters": {"max_steps": max_steps, "lora_r": lora_r, "learning_rate": lr, "max_seq_len": max_seq_len,
                                    "epochs": epochs, "load_in_4bit": load_in_4bit}, "status": "starting"}
    (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
    if not rows:
        manifest.update(status="refused", reason="no training rows in channel")
        (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
        print(json.dumps(manifest)); return 3
    if not (MODEL_DIR / "config.json").exists():
        manifest.update(status="refused", reason="base weights channel has no config.json")
        (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
        print(json.dumps(manifest)); return 4

    import torch  # noqa: E402  -- only inside the container
    from datasets import Dataset  # noqa: E402
    from peft import LoraConfig, get_peft_model, prepare_model_for_kbit_training  # noqa: E402
    from transformers import AutoModelForCausalLM, AutoTokenizer, BitsAndBytesConfig  # noqa: E402
    from trl import SFTConfig, SFTTrainer  # noqa: E402

    tok = AutoTokenizer.from_pretrained(str(MODEL_DIR), use_fast=True)
    if tok.pad_token is None:
        tok.pad_token = tok.eos_token
    quant = BitsAndBytesConfig(load_in_4bit=True, bnb_4bit_quant_type="nf4", bnb_4bit_compute_dtype=torch.bfloat16,
                               bnb_4bit_use_double_quant=True) if load_in_4bit else None
    model = AutoModelForCausalLM.from_pretrained(str(MODEL_DIR), quantization_config=quant, torch_dtype=torch.bfloat16,
                                                 device_map={"": 0}, attn_implementation="sdpa")
    if load_in_4bit:
        model = prepare_model_for_kbit_training(model)
    lora = LoraConfig(r=lora_r, lora_alpha=2 * lora_r, lora_dropout=0.05, bias="none", task_type="CAUSAL_LM",
                      target_modules=["q_proj", "k_proj", "v_proj", "o_proj", "gate_proj", "up_proj", "down_proj"])
    model = get_peft_model(model, lora)
    trainable = sum(p.numel() for p in model.parameters() if p.requires_grad)
    ds = Dataset.from_list([{"text": r["prompt"] + r["completion"] + tok.eos_token} for r in rows])
    cfg = SFTConfig(output_dir=str(OUT_DIR / "trainer"), max_steps=max_steps, num_train_epochs=epochs, learning_rate=lr,
                    per_device_train_batch_size=2, gradient_accumulation_steps=8, gradient_checkpointing=True,
                    logging_steps=10, save_strategy="no", bf16=True, max_seq_length=max_seq_len, dataset_text_field="text",
                    packing=True, report_to=[], warmup_ratio=0.03, lr_scheduler_type="cosine", seed=7)
    trainer = SFTTrainer(model=model, args=cfg, train_dataset=ds, processing_class=tok)
    t0 = time.time()
    result = trainer.train()
    adapter_dir = OUT_DIR / "adapter"
    model.save_pretrained(str(adapter_dir), safe_serialization=True)
    tok.save_pretrained(str(adapter_dir))
    losses = [{"step": h.get("step"), "loss": h.get("loss")} for h in trainer.state.log_history if "loss" in h]
    manifest.update(status="trained", finished_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), seconds=round(time.time() - t0, 1),
                    steps=int(trainer.state.global_step), trainable_parameters=int(trainable), train_loss=float(result.training_loss),
                    loss_curve=losses[-50:], adapter_sha256=dir_sha256(adapter_dir), adapter_dir="adapter/",
                    promotion="decided by the exam job on the frozen holdout, never here")
    (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({k: manifest[k] for k in ("status", "rows", "steps", "train_loss", "seconds", "adapter_sha256")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
