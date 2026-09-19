#!/usr/bin/env python3
"""How smart is it? A reasoning exam for the owned model (2026-09-19), graded exactly, no self-grading.

  python3 scripts/factory_reasoning_exam.py --gsm8k 40 --code 20 --wait-min 20

Two held-out families:
  gsm8k          grade-school word problems from openai/grade-school-math (MIT), a fixed seeded sample of the TEST split
                 (never trained on here); the model reasons step by step and ends with "Final answer: <number>";
                 graded exact on the number.
  code_reading   short Python programs generated from seeded templates (loops, strings, dicts, recursion, comprehensions);
                 the ground truth is the program's real output, executed here; the model predicts the printed output;
                 graded exact after whitespace normalisation.

Results: factory/exams/reasoning/<run_id>.json (private, every item and answer) + latest.json; the engine projects the
aggregate onto data/ai.json.reasoning_exam. The point is a baseline that every future champion is graded against.
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import re
import subprocess
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "aws" / "shared"))
from factory_forecast import qwen_prompt  # noqa: E402

PRIVATE = os.environ.get("AI_PRIVATE_BUCKET", "justhodl-ai-857687956942")
CONTROL_KEY = "factory/control/inference.json"
OUT_PREFIX = "factory/exams/reasoning/"
GSM8K_URL = "https://raw.githubusercontent.com/openai/grade-school-math/master/grade_school_math/data/test.jsonl"
MATH_SYSTEM = "You are a careful reasoner. Solve the problem step by step, showing the arithmetic. End with exactly one line: Final answer: <number>"
CODE_SYSTEM = ("You are a Python expert. You are shown a complete Python 3 program. Predict EXACTLY what it prints when run: reply with the "
               "printed output only, line for line, no explanation, no code fences.")


def now_iso():
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


# ---------------------------------------------------------------- gsm8k
def load_gsm8k(n: int, text: str | None = None) -> list:
    if text is None:
        with urllib.request.urlopen(urllib.request.Request(GSM8K_URL, headers={"User-Agent": "justhodl-exam/1.0"}), timeout=60) as r:
            text = r.read().decode("utf-8")
    rows = [json.loads(l) for l in text.splitlines() if l.strip()]
    rows.sort(key=lambda x: hashlib.sha256(x["question"].encode()).hexdigest())      # a fixed, unbiased sample
    out = []
    for row in rows[:n]:
        gold = row["answer"].split("####")[-1].strip().replace(",", "")
        out.append({"id": "gsm8k-" + hashlib.sha256(row["question"].encode()).hexdigest()[:10], "family": "gsm8k", "prompt": row["question"], "gold": gold})
    return out


def last_number(text: str):
    m = re.search(r"Final answer:\s*\$?\s*(-?[\d,]*\.?\d+)", text or "", re.I)
    cand = m.group(1) if m else None
    if cand is None:
        nums = re.findall(r"-?[\d,]*\.?\d+", text or "")
        cand = nums[-1] if nums else None
    if cand is None:
        return None
    cand = cand.replace(",", "")
    try:
        v = float(cand)
        return str(int(v)) if v == int(v) else str(v)
    except Exception:
        return None


def grade_number(answer: str, gold: str) -> bool:
    got = last_number(answer)
    if got is None:
        return False
    try:
        return abs(float(got) - float(gold)) < 1e-6
    except Exception:
        return got == gold


# ---------------------------------------------------------------- code reading
def code_items(n: int, seed: int = 20260919) -> list:
    rng = random.Random(seed)
    templates = [
        lambda r: "xs = %r\nacc = 0\nfor i, x in enumerate(xs):\n    if x %% 2 == %d:\n        acc += x * i\nprint(acc)\n" % ([r.randint(1, 12) for _ in range(6)], r.randint(0, 1)),
        lambda r: "s = %r\nout = ''.join(c.upper() if i %% %d == 0 else c for i, c in enumerate(s))\nprint(out[::-1])\n" % (r.choice(["banana", "pipeline", "market", "lambda", "holdout"]), r.randint(2, 3)),
        lambda r: "d = {}\nfor w in %r:\n    d[w[0]] = d.get(w[0], 0) + len(w)\nprint(sorted(d.items()))\n" % ([r.choice(["apple", "avocado", "bond", "bull", "bear", "gold", "gilt", "yen"]) for _ in range(5)],),
        lambda r: "def f(n):\n    return 1 if n < 2 else f(n - 1) + %d * f(n - 2)\nprint(f(%d))\n" % (r.randint(1, 3), r.randint(5, 8)),
        lambda r: "xs = [%s]\nprint([x for x in xs if x > sum(xs) / len(xs)])\n" % ", ".join(str(r.randint(1, 30)) for _ in range(6)),
        lambda r: "a, b = %d, %d\nwhile b:\n    a, b = b, a %% b\nprint(a)\n" % (r.randint(20, 120), r.randint(6, 60)),
        lambda r: "m = [[%d, %d], [%d, %d]]\nprint([sum(col) for col in zip(*m)], max(max(row) for row in m))\n" % tuple(r.randint(1, 9) for _ in range(4)),
        lambda r: "words = %r\nprint(' '.join(sorted(set(words), key=lambda w: (len(w), w))))\n" % ([r.choice(["rate", "spread", "curve", "carry", "vol", "beta", "flow"]) for _ in range(6)],),
    ]
    items = []
    for i in range(n):
        src = templates[i % len(templates)](rng)
        proc = subprocess.run([sys.executable, "-I", "-c", src], capture_output=True, text=True, timeout=10)
        if proc.returncode != 0:
            continue
        items.append({"id": "code-%03d" % i, "family": "code_reading", "prompt": src, "gold": proc.stdout.rstrip("\n")})
    return items


def norm_lines(text: str) -> str:
    text = re.sub(r"^```[a-z]*\n|\n```$", "", (text or "").strip(), flags=re.M)
    return "\n".join(line.rstrip() for line in text.strip().splitlines())


def grade_output(answer: str, gold: str) -> bool:
    return norm_lines(answer) == norm_lines(gold)


# ---------------------------------------------------------------- the exam
def run_exam(cloud, items: list, control: dict, *, wait_s: float, sleep_s: float, clock=time.time, sleep=time.sleep, run_id: str) -> dict:
    pending = {}
    for it in items:
        system = MATH_SYSTEM if it["family"] == "gsm8k" else CODE_SYSTEM
        user = it["prompt"] if it["family"] == "gsm8k" else "Program:\n" + it["prompt"] + "\nOutput:"
        payload = {"inputs": qwen_prompt(system, user), "parameters": {"max_new_tokens": 600 if it["family"] == "gsm8k" else 120, "temperature": 0.0, "top_p": 1.0, "stop": ["<|im_end|>", "<|endoftext|>"]}}
        key = "%s%s/requests/%s.json" % (OUT_PREFIX, run_id, it["id"])
        cloud.s3.put_object(Bucket=PRIVATE, Key=key, Body=json.dumps(payload).encode(), ContentType="application/json")
        resp = cloud.rt.invoke_endpoint_async(EndpointName=control["endpoint_name"], InputLocation="s3://%s/%s" % (PRIVATE, key), ContentType="application/json", Accept="application/json",
                                              InferenceId="rx-%s-%s" % (run_id[-6:], it["id"][:16]), InvocationTimeoutSeconds=900, RequestTTLSeconds=1800)
        pending[it["id"]] = {"output": resp.get("OutputLocation"), "failure": resp.get("FailureLocation")}
    answers, started = {}, clock()
    while pending and clock() - started < wait_s:
        for iid in list(pending):
            for kind in ("output", "failure"):
                loc = pending[iid].get(kind)
                key = loc[len("s3://%s/" % PRIVATE):] if loc and loc.startswith("s3://%s/" % PRIVATE) else None
                body = cloud.read(key) if key else None
                if body is None:
                    continue
                doc = body[0] if isinstance(body, list) and body else body
                answers[iid] = {"text": (doc or {}).get("generated_text") if isinstance(doc, dict) and kind == "output" else None, "error": None if kind == "output" else str(body)[:200]}
                pending.pop(iid, None)
                break
        if pending:
            sleep(sleep_s)
    rows = []
    for it in items:
        ans = answers.get(it["id"])
        text = (ans or {}).get("text")
        correct = None
        if text is not None:
            correct = grade_number(text, it["gold"]) if it["family"] == "gsm8k" else grade_output(text, it["gold"])
        rows.append({"id": it["id"], "family": it["family"], "gold": it["gold"], "answer_head": str(text or "")[-300:] if text else None, "correct": correct,
                     "error": (ans or {}).get("error") if ans else "no answer within %ds" % wait_s})
    fam = {}
    for f in ("gsm8k", "code_reading"):
        rs = [r for r in rows if r["family"] == f]
        graded = [r for r in rs if r["correct"] is not None]
        fam[f] = {"n": len(rs), "answered": len(graded), "correct": sum(1 for r in graded if r["correct"]), "accuracy": round(sum(1 for r in graded if r["correct"]) / len(graded), 4) if graded else None}
    return {"schema_version": "factory-reasoning-exam.v1", "run_id": run_id, "at": now_iso(), "model": control.get("model_id"), "revision": control.get("revision"),
            "adapter_generation": control.get("adapter_generation"), "endpoint": control.get("endpoint_name"), "families": fam, "rows": rows}


class Cloud:
    def __init__(self, s3, rt):
        self.s3, self.rt = s3, rt

    def read(self, key):
        try:
            return json.loads(self.s3.get_object(Bucket=PRIVATE, Key=key)["Body"].read())
        except Exception as exc:  # noqa: BLE001
            if "NoSuchKey" in str(exc) or "Not Found" in str(exc) or "404" in str(exc):
                return None
            raise


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--gsm8k", type=int, default=40)
    ap.add_argument("--code", type=int, default=20)
    ap.add_argument("--wait-min", type=float, default=20)
    args = ap.parse_args(argv)
    import boto3
    from botocore.config import Config
    cfg = Config(read_timeout=60, retries={"max_attempts": 3})
    cloud = Cloud(boto3.client("s3", region_name="us-east-1", config=cfg), boto3.client("sagemaker-runtime", region_name="us-east-1", config=cfg))
    control = cloud.read(CONTROL_KEY) or {}
    if not (control.get("enabled") and control.get("endpoint_name")):
        print(json.dumps({"ok": False, "error": "owned inference control missing/disabled"})); return 2
    items = load_gsm8k(args.gsm8k) + code_items(args.code)
    run_id = "reasoning-%s" % datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    result = run_exam(cloud, items, control, wait_s=args.wait_min * 60, sleep_s=10, run_id=run_id)
    cloud.s3.put_object(Bucket=PRIVATE, Key="%s%s.json" % (OUT_PREFIX, run_id), Body=json.dumps(result, indent=1, default=str).encode(), ContentType="application/json")
    latest = {k: v for k, v in result.items() if k != "rows"}
    cloud.s3.put_object(Bucket=PRIVATE, Key=OUT_PREFIX + "latest.json", Body=json.dumps(latest, indent=1, default=str).encode(), ContentType="application/json")
    print(json.dumps({"ok": True, "run_id": run_id, "families": result["families"]}))
    return 0 if any(v["answered"] for v in result["families"].values()) else 1


if __name__ == "__main__":
    sys.exit(main())
