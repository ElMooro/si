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

Step plan (2026-09-16): the number of optimizer steps is DERIVED FROM THE DATA -- packed sequences x epochs / batch --
and `max_steps` is only a ceiling. A fixed 400 steps over 570 rows meant ~70 epochs and ~5 h on ml.g5.2xlarge inside a
3 h runtime cap: four jobs in a row died at ~50% with nothing saved (jh-gearb-gen4..gen7). `time_budget_s` (default
9600, the launcher passes max_runtime_s - 1200) is a hard stop: training ends early, the adapter is still saved, and the
manifest says stopped_by=time_budget so the exam judges what actually trained. Nothing here decides promotion.
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
    """Accepts both dataset contracts: {prompt, completion} and the builder's {instruction, context, response}
    (rendered through template.json when present, else a fixed instruction/response frame)."""
    rows = []
    template = None
    tpl = train_dir / "template.json"
    if tpl.exists():
        try:
            template = json.loads(tpl.read_text())
        except ValueError:
            template = None
    frame = (template or {}).get("prompt") or "Below is an instruction that describes a task. Write a response that appropriately completes the request.\n\n### Instruction:\n{instruction}\n\n### Response:\n"
    for path in sorted(train_dir.rglob("*.jsonl")):
        for line in path.read_text().splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if not isinstance(row, dict):
                continue
            if isinstance(row.get("prompt"), str) and isinstance(row.get("completion"), str):
                rows.append({"prompt": row["prompt"], "completion": row["completion"], "rejected": row.get("rejected") if isinstance(row.get("rejected"), str) else None})
            elif isinstance(row.get("instruction"), str) and isinstance(row.get("response"), str):
                try:
                    prompt = frame.format(instruction=row["instruction"], context=row.get("context") or "")
                except (KeyError, IndexError):
                    prompt = frame.replace("{instruction}", row["instruction"]).replace("{context}", row.get("context") or "")
                rows.append({"prompt": prompt, "completion": row["response"], "rejected": row.get("rejected") if isinstance(row.get("rejected"), str) else None})
            if len(rows) >= max_rows:
                return rows
    return rows


def step_plan(total_tokens: int, max_seq_len: int, epochs: int, per_device: int, grad_accum: int, cap: int) -> dict:
    """Optimizer steps from the data: packed sequences (packing=True) x epochs / (per_device x grad_accum), capped."""
    packed = max(1, -(-int(total_tokens) // max(1, int(max_seq_len))))
    per_step = max(1, int(per_device) * int(grad_accum))
    steps_per_epoch = max(1, -(-packed // per_step))
    planned = max(1, steps_per_epoch * max(1, int(epochs)))
    return {"total_tokens": int(total_tokens), "packed_sequences": packed, "sequences_per_step": per_step,
            "steps_per_epoch": steps_per_epoch, "epochs": int(epochs), "planned_steps": planned, "cap": int(cap),
            "max_steps": min(planned, int(cap)) if int(cap) > 0 else planned}


def budget_exhausted(started: float, now_s: float, steps_done: int, budget_s: float) -> bool:
    """True when the next step would cross the budget (average step time so far, one step of margin)."""
    if budget_s <= 0:
        return False
    elapsed = max(0.0, now_s - started)
    avg = elapsed / steps_done if steps_done > 0 else 0.0
    return elapsed + avg >= budget_s


def dir_sha256(root: Path) -> str:
    h = hashlib.sha256()
    for p in sorted(root.rglob("*")):
        if p.is_file():
            h.update(p.relative_to(root).as_posix().encode())
            h.update(p.read_bytes())
    return h.hexdigest()


def train_dpo(model, tok, rows, lora, hp, *, epochs, max_steps, lr, max_seq_len, time_budget_s, started, meta) -> int:
    """Preference training (2026-09-22): rows that carry a `rejected` completion become (prompt, chosen, rejected) pairs --
    a passing attempt against a failing attempt on the same task. TRL's DPOTrainer with the LoRA adapter as the policy and
    the adapter-disabled base as the reference (ref_model=None). Same adapter output, same exam afterwards."""
    import time
    import torch  # noqa: F401
    from datasets import Dataset  # noqa: E402
    from trl import DPOConfig, DPOTrainer  # noqa: E402
    pairs = [{"prompt": r["prompt"], "chosen": r["completion"] + tok.eos_token, "rejected": r["rejected"] + tok.eos_token} for r in rows if r.get("rejected") and r["rejected"] != r["completion"]]
    meta["mode"] = "dpo"; meta["pairs"] = len(pairs)
    if len(pairs) < int(hp.get("min_pairs") or 50):
        meta["status"] = "refused"; meta["reason"] = "only %d preference pairs (min %s)" % (len(pairs), hp.get("min_pairs") or 50)
        (OUT_DIR / "train_manifest.json").write_text(json.dumps(meta, indent=2))
        print(json.dumps(meta)); return 3
    total_tokens = sum(len(tok(p["prompt"] + p["chosen"] + p["rejected"]).input_ids) for p in pairs[:2000]) * (len(pairs) / max(1, min(len(pairs), 2000)))
    plan = step_plan(int(total_tokens), max_seq_len, epochs, 1, 8, max_steps)
    meta["step_plan"] = plan
    beta = as_float(hp.get("beta"), 0.1)
    cfg_kw = dict(output_dir=str(OUT_DIR / "trainer"), max_steps=plan["max_steps"], num_train_epochs=epochs, learning_rate=as_float(hp.get("dpo_learning_rate"), 2e-5),
                  per_device_train_batch_size=1, gradient_accumulation_steps=8, logging_steps=5, save_strategy="no", bf16=True, gradient_checkpointing=True,
                  beta=beta, max_length=max_seq_len, max_prompt_length=max_seq_len // 2, remove_unused_columns=False, report_to=[])
    try:
        cfg = DPOConfig(**cfg_kw)
    except TypeError:
        for k in ("max_length", "max_prompt_length"):
            cfg_kw.pop(k, None)
        cfg = DPOConfig(**cfg_kw)
    ds = Dataset.from_list(pairs)
    kw = dict(model=model, ref_model=None, args=cfg, train_dataset=ds, peft_config=None)
    try:
        trainer = DPOTrainer(processing_class=tok, **kw)
    except TypeError:
        trainer = DPOTrainer(tokenizer=tok, **kw)
    from transformers import TrainerCallback  # noqa: E402

    class Budget(TrainerCallback):
        def __init__(self, started_s, budget_s):
            self.started, self.budget = started_s, budget_s

        def on_step_end(self, args, state, control, **kwargs):
            if budget_exhausted(self.started, time.time(), int(state.global_step), self.budget) and state.global_step < state.max_steps:
                control.should_training_stop = True
                meta["stopped_early_at_step"] = int(state.global_step)
            return control

    trainer.add_callback(Budget(started, time_budget_s))
    result = trainer.train()
    meta["train_loss"] = getattr(result, "training_loss", None); meta["steps_done"] = int(trainer.state.global_step)
    adapter_dir = OUT_DIR / "adapter"
    trainer.model.save_pretrained(str(adapter_dir), safe_serialization=True)
    tok.save_pretrained(str(adapter_dir))
    meta["status"] = "done"; meta["adapter_sha256"] = dir_sha256(adapter_dir)
    (OUT_DIR / "train_manifest.json").write_text(json.dumps(meta, indent=2))
    print(json.dumps(meta)); return 0


def main() -> int:
    _started = time.time()
    hp = hyperparameters()
    max_steps = as_int(hp.get("max_steps"), 400)
    lora_r = as_int(hp.get("lora_r"), 16)
    lr = as_float(hp.get("learning_rate"), 2e-4)
    max_seq_len = as_int(hp.get("max_seq_len"), 2048)
    epochs = as_int(hp.get("epochs"), 1)
    time_budget_s = as_int(hp.get("time_budget_s"), 9600)  # launcher passes max_runtime_s - 1200; default fits the 3 h cap
    load_in_4bit = str(hp.get("load_in_4bit", "true")).lower() == "true"
    rows = load_rows(TRAIN_DIR)
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {"schema_version": "factory-train-manifest.v1", "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "base_revision": hp.get("base_revision"), "base_manifest_sha256": hp.get("base_manifest_sha256"),
                "eligibility_digest": os.environ.get("JH_GEARB_ELIGIBILITY_DIGEST"), "train_sha256": os.environ.get("JH_GEARB_TRAIN_SHA256"),
                "holdout_digest": os.environ.get("JH_GEARB_HOLDOUT_DIGEST"), "rows": len(rows),
                "hyperparameters": {"max_steps": max_steps, "lora_r": lora_r, "learning_rate": lr, "max_seq_len": max_seq_len,
                                    "epochs": epochs, "load_in_4bit": load_in_4bit, "time_budget_s": time_budget_s}, "status": "starting"}
    (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
    if not rows:
        manifest.update(status="refused", reason="no training rows in channel")
        (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
        print(json.dumps(manifest)); return 3
    if not (MODEL_DIR / "config.json").exists():
        manifest.update(status="refused", reason="base weights channel has no config.json")
        (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
        print(json.dumps(manifest)); return 4
    # F20: the bytes we are about to train on are re-hashed against the staged manifest when it is present in the channel
    base_manifest = next((p for p in (MODEL_DIR / "manifest.json", MODEL_DIR.parent / "manifest.json") if p.exists()), None)
    if base_manifest is not None:
        try:
            listed = {f["path"]: f["sha256"] for f in json.loads(base_manifest.read_text()).get("files", []) if f.get("path") and f.get("sha256")}
        except (ValueError, TypeError):
            listed = {}
        mismatched = []
        for rel, expect in listed.items():
            fp = MODEL_DIR / rel
            if not fp.exists():
                mismatched.append(rel + ":missing"); continue
            h = hashlib.sha256()
            with fp.open("rb") as fh:
                for chunk in iter(lambda: fh.read(1 << 22), b""):
                    h.update(chunk)
            if h.hexdigest() != expect:
                mismatched.append(rel)
        manifest["base_files_verified"] = len(listed) - len(mismatched)
        if mismatched:
            manifest.update(status="refused", reason="base weight bytes do not match the staged manifest: %s" % mismatched[:5])
            (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
            print(json.dumps(manifest)); return 6

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
    mode = str(hp.get("mode") or "sft").strip().lower()
    if mode == "dpo":
        manifest["mode"] = "dpo"
        return train_dpo(model, tok, rows, lora, hp, epochs=epochs, max_steps=max_steps, lr=lr, max_seq_len=max_seq_len, time_budget_s=time_budget_s,
                         started=_started, meta=manifest)
    texts = [r["prompt"] + r["completion"] + tok.eos_token for r in rows]
    ds = Dataset.from_list([{"text": t} for t in texts])
    total_tokens = sum(min(max_seq_len, len(ids)) for ids in tok(texts, add_special_tokens=False)["input_ids"])
    plan = step_plan(total_tokens, max_seq_len, epochs, 2, 8, max_steps)
    manifest["plan"] = plan
    (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({"plan": plan, "time_budget_s": time_budget_s}))
    cfg = SFTConfig(output_dir=str(OUT_DIR / "trainer"), max_steps=plan["max_steps"], num_train_epochs=epochs, learning_rate=lr,
                    per_device_train_batch_size=2, gradient_accumulation_steps=8, gradient_checkpointing=True,
                    logging_steps=10, save_strategy="no", bf16=True, max_seq_length=max_seq_len, dataset_text_field="text",
                    packing=True, report_to=[], warmup_ratio=0.03, lr_scheduler_type="cosine", seed=7)
    trainer = SFTTrainer(model=model, args=cfg, train_dataset=ds, processing_class=tok)
    from transformers import TrainerCallback  # noqa: E402
    stop_note = {}

    class TimeBudget(TrainerCallback):
        def __init__(self, started, budget):
            self.started, self.budget = started, float(budget)

        def on_step_end(self, args, state, control, **kwargs):
            if budget_exhausted(self.started, time.time(), int(state.global_step), self.budget) and state.global_step < state.max_steps:
                stop_note.update(stopped_by="time_budget", at_step=int(state.global_step), budget_s=self.budget)
                control.should_training_stop = True
            return control

    t0 = time.time()
    trainer.add_callback(TimeBudget(t0, time_budget_s))
    result = trainer.train()
    adapter_dir = OUT_DIR / "adapter"
    model.save_pretrained(str(adapter_dir), safe_serialization=True)
    tok.save_pretrained(str(adapter_dir))
    losses = [{"step": h.get("step"), "loss": h.get("loss")} for h in trainer.state.log_history if "loss" in h]
    manifest.update(status="trained", finished_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), seconds=round(time.time() - t0, 1),
                    steps=int(trainer.state.global_step), trainable_parameters=int(trainable), train_loss=float(result.training_loss),
                    loss_curve=losses[-50:], adapter_sha256=dir_sha256(adapter_dir), adapter_dir="adapter/",
                    steps_planned=int(plan["max_steps"]), **stop_note,
                    promotion="decided by the exam job on the frozen holdout, never here")
    (OUT_DIR / "train_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({k: manifest[k] for k in ("status", "rows", "steps", "train_loss", "seconds", "adapter_sha256")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
