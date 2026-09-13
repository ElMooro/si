#!/usr/bin/env python3
"""Trace bursts and exams for the student's champion (owned; SageMaker script mode; managed spot GPU).

Two modes, one script, one image:
  burst  -- sample K candidates per curriculum task at temperature T (diversity is where creativity comes
            from; the checker is where it becomes skill). Output: traces jsonl for the unprivileged verifier
            (runner, network-less). Nothing here decides pass/fail.
  exam   -- greedy generation over the frozen exam task set for a candidate adapter; output: completions jsonl
            the exam job scores against hidden tests. This script never sees the tests.

Channels:  /opt/ml/input/data/model     base weights (staged, hashed)
           /opt/ml/input/data/adapter   optional LoRA adapter (a champion generation)
           /opt/ml/input/data/tasks     jsonl {task_id, prompt, family, holdout: false}
Output:    /opt/ml/model/traces.jsonl (burst) or exam.jsonl (exam) + burst_manifest.json
Refuses:   any task flagged holdout in burst mode (holdout tasks may only be seen by the exam mode).
"""
from __future__ import annotations

import hashlib
import json
import os
import sys
import time
from pathlib import Path

MODEL_DIR = Path(os.environ.get("SM_CHANNEL_MODEL", "/opt/ml/input/data/model"))
ADAPTER_DIR = Path(os.environ.get("SM_CHANNEL_ADAPTER", "/opt/ml/input/data/adapter"))
TASK_DIR = Path(os.environ.get("SM_CHANNEL_TASKS", "/opt/ml/input/data/tasks"))
OUT_DIR = Path(os.environ.get("SM_MODEL_DIR", "/opt/ml/model"))
HP_FILE = Path("/opt/ml/input/config/hyperparameters.json")


def hyperparameters() -> dict:
    hp = json.loads(HP_FILE.read_text()) if HP_FILE.exists() else {}
    for k, v in os.environ.items():
        if k.startswith("SM_HP_"):
            hp.setdefault(k[6:].lower(), v)
    return hp


def load_tasks(task_dir: Path, mode: str, cap: int):
    tasks, refused = [], 0
    for path in sorted(task_dir.rglob("*.jsonl")):
        for line in path.read_text().splitlines():
            if not line.strip():
                continue
            try:
                row = json.loads(line)
            except ValueError:
                continue
            if not isinstance(row, dict) or not isinstance(row.get("prompt"), str) or not row.get("task_id"):
                continue
            if mode == "burst" and row.get("holdout"):
                refused += 1
                continue
            tasks.append({"task_id": str(row["task_id"]), "prompt": row["prompt"], "family": str(row.get("family") or "code")})
            if len(tasks) >= cap:
                return tasks, refused
    return tasks, refused


def plan(hp: dict, mode: str) -> dict:
    """Sampling plan from hyperparameters (strings). Exam is always greedy and single-sample."""
    k = max(1, min(int(str(hp.get("samples_per_task", 4))), 16)) if mode == "burst" else 1
    temperature = float(str(hp.get("temperature", 0.8))) if mode == "burst" else 0.0
    return {"mode": mode, "samples_per_task": k, "temperature": temperature, "top_p": float(str(hp.get("top_p", 0.95))),
            "max_new_tokens": max(64, min(int(str(hp.get("max_new_tokens", 1024))), 4096)),
            "task_cap": max(1, min(int(str(hp.get("task_cap", 2000))), 20000))}


def main() -> int:
    hp = hyperparameters()
    mode = str(hp.get("mode", "burst")).lower()
    if mode not in ("burst", "exam"):
        print(json.dumps({"status": "refused", "reason": "mode must be burst or exam"})); return 2
    p = plan(hp, mode)
    tasks, refused = load_tasks(TASK_DIR, mode, p["task_cap"])
    OUT_DIR.mkdir(parents=True, exist_ok=True)
    manifest = {"schema_version": "factory-burst-manifest.v1", "started_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
                "plan": p, "tasks": len(tasks), "holdout_refused": refused, "base_present": (MODEL_DIR / "config.json").exists(),
                "adapter_present": (ADAPTER_DIR / "adapter_config.json").exists(), "status": "starting",
                "adapter_generation": hp.get("adapter_generation"), "eligibility": "traces are candidates only; the verifier grades"}
    (OUT_DIR / "burst_manifest.json").write_text(json.dumps(manifest, indent=2))
    if not tasks or not manifest["base_present"]:
        manifest.update(status="refused", reason="no tasks" if not tasks else "base weights channel has no config.json")
        (OUT_DIR / "burst_manifest.json").write_text(json.dumps(manifest, indent=2)); print(json.dumps(manifest)); return 3

    from vllm import LLM, SamplingParams  # noqa: E402  -- inside the container only
    from vllm.lora.request import LoRARequest  # noqa: E402
    llm = LLM(model=str(MODEL_DIR), dtype="bfloat16", enable_lora=manifest["adapter_present"], max_lora_rank=64,
              gpu_memory_utilization=0.90, max_model_len=int(str(hp.get("max_model_len", 8192))))
    lora = LoRARequest("champion", 1, str(ADAPTER_DIR)) if manifest["adapter_present"] else None
    sp = SamplingParams(n=p["samples_per_task"], temperature=p["temperature"], top_p=p["top_p"], max_tokens=p["max_new_tokens"], seed=7)
    t0 = time.time()
    outs = llm.generate([t["prompt"] for t in tasks], sp, lora_request=lora)
    name = "traces.jsonl" if mode == "burst" else "exam.jsonl"
    n = 0
    with (OUT_DIR / name).open("w") as f:
        for task, out in zip(tasks, outs):
            for i, cand in enumerate(out.outputs):
                text = cand.text
                row = {"task_id": task["task_id"], "family": task["family"], "sample": i, "completion": text,
                       "completion_sha256": hashlib.sha256(text.encode()).hexdigest(), "tokens": len(cand.token_ids),
                       "finish": cand.finish_reason, "mode": mode, "adapter_generation": hp.get("adapter_generation")}
                f.write(json.dumps(row) + "\n"); n += 1
    manifest.update(status="generated", finished_at=time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), seconds=round(time.time() - t0, 1),
                    candidates=n, output=name, output_sha256=hashlib.sha256((OUT_DIR / name).read_bytes()).hexdigest())
    (OUT_DIR / "burst_manifest.json").write_text(json.dumps(manifest, indent=2))
    print(json.dumps({k: manifest[k] for k in ("status", "tasks", "candidates", "seconds", "output_sha256")}))
    return 0


if __name__ == "__main__":
    sys.exit(main())
