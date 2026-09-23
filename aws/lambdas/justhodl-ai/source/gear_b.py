"""Gear B -- capped weight training for the Compound Factory student (Claude Ship 2, 2026-09-13).

Doctrine (docs/factory/GEAR_A.md "Weight training/Gear B"): thousands of passes alone are not enough.
This module refuses to spend until every one of these is on disk and digested:

  provenance  -- every row names its source key/URL and the checker that graded it
  licensing   -- every row carries a license from ALLOWED_LICENSES (own work, permissive OSS, CC-BY)
  diversity   -- no family may exceed `max_family_share` of the dataset
  dedup       -- exact and prompt-level duplicates are dropped
  protection  -- the frozen holdout manifest is digested and any row touching it is excluded
  model       -- an open-weight hub card with a published training recipe (TrainingSupported)
  budget      -- the owner-approved Gear B sub-budget (daily + season) inside the cost-guard policy

What it never does: create an endpoint, create a role, call a paid model API, write the Gear A
coding-family champion, or train on anything from factory/traces/_reject/.  Weights are graded by a
SageMaker *batch transform* over the frozen exam prompts (a job, not an endpoint); the isolated
runner (factory-code-exam.yml) executes the tests and writes the verdict; promotion goes through
factory_core.promotion_decision exactly like every other candidate.

Keys (private bucket unless noted):
  factory/control/gearb.json                 owner policy (written by an ops script, never by an engine)
  factory/gearb/datasets/gen-N/{train.jsonl,template.json,manifest.json}
  factory/gearb/jobs/<job>.json              one immutable object per launched job (the cost ledger)
  factory/gearb/candidates/gen-N.json        trained artifact + exam state
  factory/gearb/champion.json                weights champion (separate from Gear A's coding champion)
  factory/holdout/manifest.json              frozen by scripts/factory_holdout.py (runner), read-only here
"""
from __future__ import annotations

import hashlib
import json
import math
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Iterable, List, Optional, Tuple

import cost_guard as cg

VERSION = "gear-b.1"
CONTROL_KEY = "factory/control/gearb.json"
HOLDOUT_KEY = "factory/holdout/manifest.json"
CHAMPION_KEY = "factory/gearb/champion.json"
DATASET_PREFIX = "factory/gearb/datasets/"
JOBS_PREFIX = "factory/gearb/jobs/"
CANDIDATE_PREFIX = "factory/gearb/candidates/"
CURRICULUM_VERIFIED_PREFIX = "factory/curriculum/code/verified/"
TRACES_PREFIX = "factory/traces/code/"
SKILLBOOK_PREFIX = "factory/skillbook/"
REJECT_PREFIX = "factory/traces/_reject/"
EXAM_PROMPTS_KEY = "factory/exams/code/holdout-prompts.jsonl"

ALLOWED_LICENSES = frozenset({"own", "MIT", "Apache-2.0", "BSD-2-Clause", "BSD-3-Clause", "CC-BY-4.0", "CC0-1.0", "Unlicense", "ISC"})
ALLOWED_SOURCE_KINDS = frozenset({"gear_a_trace", "skillbook", "oss_commit", "public_benchmark_train", "self_trace"})  # self_trace = keep-only-passes from an owned burst, verified by the runner
TAG_PURPOSE = "factory-gear-b"

DEFAULT_CONTROL = {
    "schema_version": "gearb-control.v1",
    "enabled": False,
    "model_id": "huggingface-llm-qwen2-5-coder-7b-instruct",
    "model_version": None,
    "instance_type": "ml.g5.2xlarge",
    "exam_instance_type": "ml.g5.2xlarge",
    "daily_budget_usd": 0.0,
    "season_cap_usd": 0.0,
    "max_runtime_s": 3 * 3600,
    "exam_max_runtime_s": 3600,
    "min_sft_rows": 2000,
    "min_dpo_pairs": 0,
    "max_family_share": 0.25,
    "max_jobs_per_day": 1,
    "lora": {"peft_type": "lora", "lora_r": "16", "lora_alpha": "32", "lora_dropout": "0.05", "epoch": "1",
             "learning_rate": "0.0001", "max_input_length": "2048", "instruction_tuned": "True", "chat_dataset": "False"},
    "approved_by": None,
    "approved_at": None,
    "season_id": None,
}

UTC = timezone.utc


class GearBRefused(RuntimeError):
    """The action is refused; the reason is the message. Nothing was created."""


def now() -> datetime:
    return datetime.now(UTC)


def now_iso() -> str:
    return now().isoformat().replace("+00:00", "Z")


def _canonical(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def digest(value: Any) -> str:
    return hashlib.sha256(_canonical(value).encode("utf-8")).hexdigest()


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


# ───────────────────────────────────────────────────────────────── S3 helpers
def get_json(s3, bucket: str, key: str, default=None):
    try:
        return json.loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
    except Exception:  # noqa: BLE001 -- missing stays missing
        return default


def put_json_absent(s3, bucket: str, key: str, obj: Any) -> bool:
    """Create-if-absent. Returns False when the key already exists (never overwrites)."""
    body = _canonical(obj).encode("utf-8")
    try:
        s3.put_object(Bucket=bucket, Key=key, Body=body, ContentType="application/json", IfNoneMatch="*")
        return True
    except Exception as exc:  # noqa: BLE001
        code = getattr(exc, "response", {}).get("Error", {}).get("Code", "")
        if code in ("PreconditionFailed", "412"):
            return False
        raise


def put_json(s3, bucket: str, key: str, obj: Any) -> None:
    s3.put_object(Bucket=bucket, Key=key, Body=_canonical(obj).encode("utf-8"), ContentType="application/json")


def list_keys(s3, bucket: str, prefix: str, limit: int = 20000) -> List[str]:
    keys: List[str] = []
    token = None
    while True:
        kw = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": 1000}
        if token:
            kw["ContinuationToken"] = token
        page = s3.list_objects_v2(**kw)
        for row in page.get("Contents") or []:
            keys.append(row["Key"])
            if len(keys) >= limit:
                return keys
        token = page.get("NextContinuationToken")
        if not token:
            return keys


# ───────────────────────────────────────────────────────────────── control
def load_control(s3, private_bucket: str) -> Dict[str, Any]:
    doc = get_json(s3, private_bucket, CONTROL_KEY) or {}
    control = dict(DEFAULT_CONTROL)
    control.update({k: v for k, v in doc.items() if k in DEFAULT_CONTROL or k.startswith("_")})
    # owned-lane fields (2026-09-13): never dropped -- model_source decides own_spec vs hub card at launch
    for k in ("model_source", "model_version", "owned"):
        if k in doc:
            control[k] = doc[k]
    lora = dict(DEFAULT_CONTROL["lora"])
    lora.update({str(k): str(v) for k, v in (doc.get("lora") or {}).items()})
    control["lora"] = lora
    control["_present"] = bool(doc)
    return control


def validate_control(control: Dict[str, Any], policy: Dict[str, Any]) -> Optional[str]:
    """None when the owner policy is coherent with the cost-guard policy; else the refusal reason."""
    if not control.get("_present"):
        return "factory/control/gearb.json is absent -- Gear B is off until an owner ops script writes it"
    if not control.get("enabled"):
        return "gearb.enabled is false"
    if not control.get("approved_by") or not control.get("approved_at"):
        return "gearb control lacks approved_by/approved_at -- the owner approval is the only thing that turns spend on"
    daily = float(control.get("daily_budget_usd") or 0)
    season = float(control.get("season_cap_usd") or 0)
    if daily <= 0 or season <= 0:
        return "gearb daily_budget_usd and season_cap_usd must both be > 0"
    if daily > float(policy.get("daily_budget_usd") or 0):
        return "gearb daily_budget_usd %.2f exceeds cost-guard policy.daily_budget_usd %.2f" % (daily, float(policy.get("daily_budget_usd") or 0))
    if not re.match(r"^[a-z0-9][a-z0-9\-]{3,80}$", str(control.get("model_id") or "")):
        return "gearb model_id is not a hub card id"
    for key in ("instance_type", "exam_instance_type"):
        why = cg.instance_allowed(policy, str(control.get(key) or ""), "training")
        if why:
            return "gearb %s: %s" % (key, why)
    if int(control.get("max_runtime_s") or 0) <= 0 or int(control.get("max_runtime_s")) > int(policy.get("training_max_runtime_s") or 0) * 12:
        return "gearb max_runtime_s must be > 0 and within 12x policy.training_max_runtime_s"
    return None


# ───────────────────────────────────────────────────────────────── ledger / spend
def _job_records(s3, private_bucket: str) -> List[Dict[str, Any]]:
    """One row per job identity (audit F08): a terminal record supersedes the live record of the same job_name;
    an unreadable describe leaves the last known state marked unknown, never dropped (F09)."""
    by_job: Dict[str, Dict[str, Any]] = {}
    for key in list_keys(s3, private_bucket, JOBS_PREFIX, limit=5000):
        doc = get_json(s3, private_bucket, key)
        if not isinstance(doc, dict):
            continue
        name = str(doc.get("job_name") or key)
        prev = by_job.get(name)
        terminal = str(doc.get("status") or "") in ("Completed", "Failed", "Stopped", "error")
        if prev is None or terminal or str(prev.get("status") or "") not in ("Completed", "Failed", "Stopped"):
            by_job[name] = doc if prev is None or terminal else prev
    return list(by_job.values())


def spend(records: Iterable[Dict[str, Any]], since: datetime) -> Dict[str, Any]:
    """Committed spend = the MaxRuntime cap of every job launched since `since` (never optimistic)."""
    total, n = 0.0, 0
    for row in records:
        try:
            at = datetime.fromisoformat(str(row.get("launched_at")).replace("Z", "+00:00"))
        except Exception:  # noqa: BLE001
            continue
        if at >= since:
            total += float(row.get("cap_usd") or 0)
            n += 1
    return {"usd": round(total, 4), "jobs": n}


def budget_check(control: Dict[str, Any], records: List[Dict[str, Any]], cap_usd: float, projected: Dict[str, Any], policy: Dict[str, Any], hourly: float, hours: float) -> Optional[str]:
    today = spend(records, now().replace(hour=0, minute=0, second=0, microsecond=0))
    if today["jobs"] >= int(control.get("max_jobs_per_day") or 1):
        return "gearb max_jobs_per_day reached (%d launched today)" % today["jobs"]
    if today["usd"] + cap_usd > float(control["daily_budget_usd"]):
        return "gearb daily budget: %.2f committed today + %.2f for this job > %.2f" % (today["usd"], cap_usd, float(control["daily_budget_usd"]))
    season_since = datetime.fromisoformat(str(control.get("approved_at")).replace("Z", "+00:00"))
    season = spend(records, season_since)
    if season["usd"] + cap_usd > float(control["season_cap_usd"]):
        return "gearb season cap: %.2f committed since approval + %.2f > %.2f" % (season["usd"], cap_usd, float(control["season_cap_usd"]))
    why = cg.check_budget(policy, projected, hourly, hours=hours)
    if why:
        return "cost-guard: " + why
    return None


# ───────────────────────────────────────────────────────────────── dataset
def _row_from_gear_a_trace(key: str, doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Gear A coding-family traces are one repair family each; only a graded pass becomes a row."""
    verdict = doc.get("verdict") if isinstance(doc.get("verdict"), dict) else {}
    passed = doc.get("ok") is True or verdict.get("passed") is True or doc.get("status") == "pass"
    if not passed:
        return None
    prompt = doc.get("prompt") or doc.get("task") or doc.get("family_prompt")
    solution = doc.get("candidate") or doc.get("program") or doc.get("solution")
    if not isinstance(prompt, str) or not isinstance(solution, str):
        return None
    return {"kind": "gear_a_trace", "family": str(doc.get("family") or key.rsplit("/", 1)[-1].split(".")[0]),
            "license": "own", "source": key, "checker": str(doc.get("checker") or verdict.get("checker") or "justhodl-factory-grader"),
            "prompt": prompt, "solution": solution}


def _receipt_ok(doc: Dict[str, Any], s3=None, bucket: Optional[str] = None) -> bool:
    """A training row is admitted only when its verifier receipt EXISTS and BINDS the same solution and test bytes with a
    supervisor judge, a v3+ checker and at least one executed case (audit A15/B05). Without a store to resolve against,
    the row is refused -- a label is never evidence."""
    receipt_key = doc.get("receipt")
    if not isinstance(receipt_key, str) or s3 is None or not bucket:
        return False
    receipt = get_json(s3, bucket, receipt_key)
    if not isinstance(receipt, dict) or receipt.get("passed") is not True:
        return False
    m = re.match(r"^factory-code-verify:v(\d+)-supervisor-judge$", str(receipt.get("checker") or ""))
    if not m or int(m.group(1)) < 3 or receipt.get("judge") != "supervisor" or int(receipt.get("cases") or 0) < 1:
        return False
    if receipt.get("solution_sha256") != doc.get("solution_sha256") or receipt.get("tests_sha256") != doc.get("tests_sha256"):
        return False
    return hashlib.sha256(str(doc.get("solution") or "").encode("utf-8")).hexdigest() == doc.get("solution_sha256")


def _row_from_skill(key: str, doc: Dict[str, Any], s3=None, bucket: Optional[str] = None) -> Optional[Dict[str, Any]]:
    prompt = doc.get("problem") or doc.get("prompt")
    solution = doc.get("skill") or doc.get("program") or doc.get("solution")
    if not isinstance(prompt, str) or not isinstance(solution, str):
        return None
    if not _receipt_ok(dict(doc, solution=solution), s3, bucket):
        return None            # a skill without a resolved verifier receipt is not training material (A15/B05)
    return {"kind": "skillbook", "family": str(doc.get("family") or key.rsplit("/", 1)[-1].split(".")[0]),
            "license": "own", "source": key, "checker": str(doc.get("checker")),
            "prompt": prompt, "solution": solution}


def _row_from_verified(key: str, doc: Dict[str, Any], s3=None, bucket: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Rows written by the isolated runner (factory-code-exam.yml) with verified_by=owner_runner."""
    if doc.get("verified_by") != "owner_runner" or doc.get("passed") is not True:
        return None
    kind = str(doc.get("kind") or "")
    if kind not in ALLOWED_SOURCE_KINDS:
        return None
    if kind == "self_trace":
        checker = str(doc.get("checker") or "")
        m = re.match(r"^factory-code-verify:v(\d+)-supervisor-judge$", checker)
        # only a supervisor-judged verifier (v3+) with a bound receipt mints training material; older labels are history (F01/A03/A05)
        if not m or int(m.group(1)) < 3 or doc.get("judge") != "supervisor" or int(doc.get("cases") or 0) < 1:
            return None
        if not _receipt_ok(doc, s3, bucket):
            return None        # receipt missing, unbound or hash-mismatched (B05)
    prompt, solution = doc.get("prompt"), doc.get("solution")
    if not isinstance(prompt, str) or not isinstance(solution, str) or not doc.get("source_url"):
        return None
    return {"kind": kind, "family": str(doc.get("family") or doc.get("repo") or "public"),
            "license": str(doc.get("license") or ""), "source": str(doc.get("source_url")), "checker": str(doc.get("checker") or "factory-code-exam"),
            "task_id": str(doc.get("task_id") or ""), "source_sha": str(doc.get("source_sha") or ""),
            "prompt": _lf(prompt), "solution": _lf(solution),
            "rejected": _lf(doc["rejected"]) if isinstance(doc.get("rejected"), str) and doc["rejected"].strip() else None}   # preference_pair rows (2026-09-22)


def _lf(text: str) -> str:
    """LF-only, no trailing whitespace: verified rows written before 2026-09-13 carry CRLF from the MBPP source."""
    text = text.replace("\r\n", "\n").replace("\r", "\n")
    return "\n".join(line.rstrip() for line in text.split("\n")).strip("\n") + "\n"


def collect_rows(s3, private_bucket: str, public_bucket: str, limit_per_source: int = 20000) -> Tuple[List[Dict[str, Any]], Dict[str, int]]:
    rows: List[Dict[str, Any]] = []
    counts = {"traces_seen": 0, "skills_seen": 0, "verified_seen": 0, "unmapped": 0}
    for key in list_keys(s3, public_bucket, TRACES_PREFIX, limit_per_source):
        if not key.endswith(".json"):
            continue
        counts["traces_seen"] += 1
        doc = get_json(s3, public_bucket, key)
        row = _row_from_gear_a_trace(key, doc) if isinstance(doc, dict) else None
        rows.append(row) if row else counts.__setitem__("unmapped", counts["unmapped"] + 1)
    for key in list_keys(s3, private_bucket, SKILLBOOK_PREFIX, limit_per_source):
        if not key.endswith(".json"):
            continue
        counts["skills_seen"] += 1
        doc = get_json(s3, private_bucket, key)
        row = _row_from_skill(key, doc, s3, private_bucket) if isinstance(doc, dict) else None
        rows.append(row) if row else counts.__setitem__("unmapped", counts["unmapped"] + 1)
    for key in list_keys(s3, private_bucket, CURRICULUM_VERIFIED_PREFIX, limit_per_source):
        if not key.endswith(".json"):
            continue
        counts["verified_seen"] += 1
        doc = get_json(s3, private_bucket, key)
        row = _row_from_verified(key, doc, s3, private_bucket) if isinstance(doc, dict) else None
        rows.append(row) if row else counts.__setitem__("unmapped", counts["unmapped"] + 1)
    return rows, counts


def curate(rows: List[Dict[str, Any]], holdout: Dict[str, Any], control: Dict[str, Any]) -> Dict[str, Any]:
    """Pure function: provenance, licensing, holdout exclusion, dedup, diversity. Deterministic order."""
    blocked_ids = set(map(str, (holdout.get("code") or {}).get("task_ids") or []))
    blocked_shas = set(map(str, (holdout.get("code") or {}).get("source_shas") or []))
    kept: List[Dict[str, Any]] = []
    dropped = {"license": 0, "provenance": 0, "holdout": 0, "duplicate": 0, "diversity": 0, "reject_prefix": 0}
    seen_exact, seen_prompt = set(), set()
    for row in sorted(rows, key=lambda r: (r["kind"], r["family"], r["source"])):
        if REJECT_PREFIX in row["source"]:
            dropped["reject_prefix"] += 1
            continue
        if row.get("license") not in ALLOWED_LICENSES:
            dropped["license"] += 1
            continue
        if not row.get("source") or not row.get("checker"):
            dropped["provenance"] += 1
            continue
        if row.get("task_id") in blocked_ids or row.get("source_sha") in blocked_shas:
            dropped["holdout"] += 1
            continue
        exact = sha256_bytes((row["prompt"] + "\u0000" + row["solution"]).encode("utf-8"))
        prompt_sha = sha256_bytes(re.sub(r"\s+", " ", row["prompt"]).strip().lower().encode("utf-8"))
        if exact in seen_exact or prompt_sha in seen_prompt:
            # 2026-09-22: a preference pair (same prompt + a failing attempt) replaces a plain duplicate of that prompt
            if isinstance(row.get("rejected"), str) and row["rejected"]:
                for i, k in enumerate(kept):
                    if k.get("_prompt_sha") == prompt_sha and not k.get("rejected"):
                        kept[i] = dict(row, row_sha=exact, _prompt_sha=prompt_sha)
                        break
                else:
                    dropped["duplicate"] += 1
                continue
            dropped["duplicate"] += 1
            continue
        seen_exact.add(exact)
        seen_prompt.add(prompt_sha)
        kept.append(dict(row, row_sha=exact, _prompt_sha=prompt_sha))
    share = float(control.get("max_family_share") or 0.25)
    cap = max(1, math.ceil(len(kept) * share)) if kept else 0
    per_family: Dict[str, int] = {}
    diverse: List[Dict[str, Any]] = []
    for row in kept:
        n = per_family.get(row["family"], 0)
        if n >= cap and len({r["family"] for r in kept}) > 1:
            dropped["diversity"] += 1
            continue
        per_family[row["family"]] = n + 1
        diverse.append(row)
    licenses: Dict[str, int] = {}
    kinds: Dict[str, int] = {}
    for row in diverse:
        licenses[row["license"]] = licenses.get(row["license"], 0) + 1
        kinds[row["kind"]] = kinds.get(row["kind"], 0) + 1
    return {"rows": diverse, "dropped": dropped, "licenses": licenses, "kinds": kinds, "families": per_family,
            "holdout_digest": digest(holdout) if holdout else None}


def next_generation(s3, private_bucket: str) -> int:
    gens = []
    for key in list_keys(s3, private_bucket, DATASET_PREFIX, 5000):
        m = re.match(re.escape(DATASET_PREFIX) + r"gen-(\d+)/", key)
        if m:
            gens.append(int(m.group(1)))
    return (max(gens) + 1) if gens else 1


def last_launched_manifest(s3, private_bucket: str) -> Optional[Dict[str, Any]]:
    """The newest dataset a real SFT job consumed (job records of kind sft/pref), else None."""
    launched = {r.get("generation") for r in _job_records(s3, private_bucket) if str(r.get("kind") or "sft") in ("sft", "pref") and r.get("job_name")}
    if not launched:
        return None
    for key in sorted((k for k in list_keys(s3, private_bucket, DATASET_PREFIX, 5000) if k.endswith("/manifest.json")),
                      key=lambda k: int(re.search(r"gen-(\d+)/", k).group(1)), reverse=True):
        m = get_json(s3, private_bucket, key)
        if isinstance(m, dict) and m.get("generation") in launched:
            return m
    return None


def new_task_fraction(s3, private_bucket: str, task_ids: List[str]) -> Dict[str, Any]:
    """2026-09-22: repeat-SFT is about the TASKS, not the label on the rows. The fraction of this dataset's tasks that the
    last launched generation never trained on (its manifest's task_ids, or its train.jsonl prompts for older manifests)."""
    last = last_launched_manifest(s3, private_bucket)
    if not last:
        return {"new_task_fraction": 1.0, "new_tasks": len(task_ids), "vs_generation": None}
    seen = set(map(str, last.get("task_ids") or []))
    if not seen and last.get("prefix"):
        try:
            body = s3.get_object(Bucket=private_bucket, Key=last["prefix"] + "train.jsonl")["Body"].read().decode("utf-8", "replace")
            seen = {"p:" + sha256_bytes(str(json.loads(line).get("instruction") or "").encode("utf-8"))[:24] for line in body.splitlines() if line.strip()}
        except Exception:  # noqa: BLE001
            seen = set()
    if not task_ids:
        return {"new_task_fraction": 0.0, "new_tasks": 0, "vs_generation": last.get("generation")}
    # older train files carry prompts only: compare on the prompt digest form when a task id is not in the seen set
    new = [t for t in task_ids if t not in seen]
    return {"new_task_fraction": round(len(new) / len(task_ids), 4), "new_tasks": len(new), "vs_generation": last.get("generation"), "seen_basis": "task_ids" if last.get("task_ids") else "train.jsonl"}


PAIRS_PREFIX = "factory/curriculum/code/pairs/"


def load_pairs(s3, private_bucket: str, limit: int = 20000) -> Dict[str, List[Dict[str, Any]]]:
    """task_id -> recorded failing attempts of the owned model (factory-pref-pair.v1, written by scripts/factory_pairs.py
    from each burst's own traces: a sample that FAILED the task's tests). Empty when none exist."""
    out: Dict[str, List[Dict[str, Any]]] = {}
    for key in list_keys(s3, private_bucket, PAIRS_PREFIX, limit):
        if not key.endswith(".json"):
            continue
        doc = get_json(s3, private_bucket, key)
        if not isinstance(doc, dict) or doc.get("schema_version") != "factory-pref-pair.v1":
            continue
        rej = doc.get("rejected")
        if isinstance(rej, str) and rej.strip() and doc.get("task_id"):
            out.setdefault(str(doc["task_id"]), []).append(doc)
    return out


def attach_pairs(rows: List[Dict[str, Any]], pairs: Dict[str, List[Dict[str, Any]]]) -> List[Dict[str, Any]]:
    """Give each row whose task has a recorded failing attempt a `rejected` completion (never the row's own solution).
    A task's first row gets the first attempt; rows are otherwise unchanged. Pure."""
    used: Dict[str, int] = {}
    out = []
    for row in rows:
        tid = str(row.get("task_id") or "")
        cands = pairs.get(tid) or []
        if cands and not row.get("rejected"):
            i = used.get(tid, 0)
            if i < len(cands):
                rej = _lf(str(cands[i]["rejected"]))
                if rej.strip() and rej.strip() != str(row.get("solution") or "").strip():
                    row = dict(row, rejected=rej, rejected_reason=cands[i].get("rejected_reason"))
                    used[tid] = i + 1
        out.append(row)
    return out


def launch_mode(control: Dict[str, Any], manifest: Dict[str, Any]) -> str:
    """sft | dpo for this launch. control.train_mode: 'sft', 'dpo', or 'auto' (dpo when the dataset carries at least
    control.min_pairs preference pairs -- a passing and a failing attempt on the same task -- else sft). 2026-09-22."""
    want = str(control.get("train_mode") or "auto").strip().lower()
    pairs = int(manifest.get("pref_pairs") or 0)
    floor = int(control.get("min_pairs") or 50)
    if want == "dpo":
        return "dpo"
    if want == "sft":
        return "sft"
    return "dpo" if pairs >= floor else "sft"


def latest_unlaunched_manifest(s3, private_bucket: str) -> Optional[Dict[str, Any]]:
    """The newest eligible dataset that no job has consumed yet, else None."""
    manifests = [k for k in list_keys(s3, private_bucket, DATASET_PREFIX, 5000) if k.endswith("/manifest.json")]
    if not manifests:
        return None
    latest = get_json(s3, private_bucket, sorted(manifests, key=lambda k: int(re.search(r"gen-(\d+)/", k).group(1)))[-1])
    if not isinstance(latest, dict) or not latest.get("ok"):
        return None
    launched = {r.get("generation") for r in _job_records(s3, private_bucket)}
    return None if latest.get("generation") in launched else latest


def build_dataset(s3, private_bucket: str, public_bucket: str, control: Dict[str, Any]) -> Dict[str, Any]:
    """Write factory/gearb/datasets/gen-N/ (create-if-absent) or explain exactly what is missing."""
    holdout = get_json(s3, private_bucket, HOLDOUT_KEY)
    if not isinstance(holdout, dict) or holdout.get("frozen_at") is None:
        raise GearBRefused("holdout manifest %s is not frozen -- run scripts/factory_holdout.py on the runner first" % HOLDOUT_KEY)
    rows, counts = collect_rows(s3, private_bucket, public_bucket)
    pairs = load_pairs(s3, private_bucket)                    # 2026-09-22: the owned model's failing attempts (scripts/factory_pairs.py)
    rows = attach_pairs(rows, pairs)
    counts["pairs_attached"] = sum(1 for r in rows if r.get("rejected"))
    counts["pair_records"] = sum(len(v) for v in pairs.values())
    cur = curate(rows, holdout, control)
    floor = int(control.get("min_sft_rows") or 0)
    status = {"schema_version": "gearb-dataset.v1", "at": now_iso(), "seen": counts, "kept": len(cur["rows"]), "pref_pairs": sum(1 for r in cur["rows"] if isinstance(r.get("rejected"), str) and r["rejected"]),
              "dropped": cur["dropped"], "licenses": cur["licenses"], "kinds": cur["kinds"], "families": len(cur["families"]),
              "floor": floor, "holdout_digest": cur["holdout_digest"]}
    if len(cur["rows"]) < floor:
        status["ok"] = False
        status["missing_rows"] = floor - len(cur["rows"])
        status["reason"] = "waiting_for_traces: %d verified rows of %d" % (len(cur["rows"]), floor)
        return status
    gen = next_generation(s3, private_bucket)
    prefix = "%sgen-%d/" % (DATASET_PREFIX, gen)
    # 2026-09-22: a preference_pair row carries its failing attempt as `rejected`; the trainer's dpo mode pairs it against the
    # passing `response` on the same prompt, the sft mode ignores it
    train_lines = "\n".join(_canonical({"instruction": r["prompt"], "context": "", "response": r["solution"], **({"rejected": r["rejected"]} if isinstance(r.get("rejected"), str) and r["rejected"] else {})}) for r in cur["rows"]) + "\n"
    template = {"prompt": "Below is an instruction that describes a task. Write a response that appropriately completes the request.\n\n### Instruction:\n{instruction}\n\n### Response:\n", "completion": "{response}"}
    train_bytes = train_lines.encode("utf-8")
    task_ids = sorted({str(r.get("task_id") or ("p:" + sha256_bytes(str(r.get("prompt") or "").encode("utf-8"))[:24])) for r in cur["rows"]})
    novelty = new_task_fraction(s3, private_bucket, task_ids)
    manifest = dict(status, ok=True, generation=gen, prefix=prefix, train_sha256=sha256_bytes(train_bytes), train_bytes=len(train_bytes),
                    template_sha256=sha256_bytes(_canonical(template).encode("utf-8")),
                    rows_sha256=[r["row_sha"] for r in cur["rows"]][:50000], task_ids=task_ids[:50000], **novelty,
                    sources={"traces": TRACES_PREFIX, "skillbook": SKILLBOOK_PREFIX, "verified": CURRICULUM_VERIFIED_PREFIX},
                    excluded=[REJECT_PREFIX], version=VERSION)
    manifest["eligibility_digest"] = digest({k: manifest[k] for k in ("train_sha256", "template_sha256", "holdout_digest", "licenses", "kinds", "kept", "generation")})
    try:
        s3.put_object(Bucket=private_bucket, Key=prefix + "train.jsonl", Body=train_bytes, ContentType="application/jsonl", IfNoneMatch="*")
        s3.put_object(Bucket=private_bucket, Key=prefix + "template.json", Body=_canonical(template).encode("utf-8"), ContentType="application/json", IfNoneMatch="*")
    except Exception as exc:  # noqa: BLE001
        raise GearBRefused("dataset gen-%d already exists or the write failed: %s" % (gen, exc))
    if not put_json_absent(s3, private_bucket, prefix + "manifest.json", manifest):
        raise GearBRefused("manifest for gen-%d already exists" % gen)
    return manifest


# ───────────────────────────────────────────────────────────────── training job
def _stop(max_runtime_s: int, spot: bool = True) -> Dict[str, int]:
    out = {"MaxRuntimeInSeconds": int(max_runtime_s)}
    if spot:
        out["MaxWaitTimeInSeconds"] = int(max_runtime_s) + 3600
    return out


CAPACITY_PREFIX = "factory/gearb/capacity_stops/"


def use_spot(s3, private_bucket: str, control: Dict[str, Any]) -> bool:
    """Managed spot unless the control says no, or a spot job was stopped for missing capacity within the cooldown
    (2026-09-23: the first DPO generation sat in 'Insufficient capacity' -- progress beats a 60% discount)."""
    if control.get("spot") is False:
        return False
    cooldown = int(control.get("spot_cooldown_s") or 21600)
    cutoff = now() - timedelta(seconds=cooldown)
    for key in list_keys(s3, private_bucket, CAPACITY_PREFIX, 200):
        doc = get_json(s3, private_bucket, key) or {}
        try:
            if datetime.fromisoformat(str(doc.get("at")).replace("Z", "+00:00")) >= cutoff:
                return False
        except Exception:  # noqa: BLE001
            continue
    return True


def rescue_capacity(sm, s3, private_bucket: str, control: Dict[str, Any]) -> List[str]:
    """Stop a Gear B / exam / merge spot job that has waited for capacity longer than control.spot_patience_s (30 min),
    and record it: the next launch runs on-demand (use_spot), so a starved spot pool never stalls the loop for hours."""
    patience = int(control.get("spot_patience_s") or 1800)
    stopped = []
    for prefix in ("jh-gearb-", "jh-exam-", "jh-merge-"):
        for j in sm.list_training_jobs(StatusEquals="InProgress", NameContains=prefix, MaxResults=10).get("TrainingJobSummaries", []):
            d = sm.describe_training_job(TrainingJobName=j["TrainingJobName"])
            if d.get("SecondaryStatus") != "Starting" or not d.get("EnableManagedSpotTraining"):
                continue
            msgs = " ".join(str(t.get("StatusMessage") or "") for t in (d.get("SecondaryStatusTransitions") or [])[-3:])
            age = (now() - d["CreationTime"]).total_seconds()
            if "capacity" in msgs.lower() and age >= patience:
                sm.stop_training_job(TrainingJobName=j["TrainingJobName"])
                put_json(s3, private_bucket, CAPACITY_PREFIX + j["TrainingJobName"] + ".json",
                         {"job_name": j["TrainingJobName"], "at": now_iso(), "waited_s": int(age), "message": msgs[:300]})
                stopped.append(j["TrainingJobName"])
    return stopped


def launch_sft(sm, s3, *, spec: Dict[str, Any], role_arn: str, private_bucket: str, control: Dict[str, Any], policy: Dict[str, Any],
               manifest: Dict[str, Any], projected: Dict[str, Any], pricing: Dict[str, Any], region: str) -> Dict[str, Any]:
    """Launch ONE managed-spot LoRA fine-tune from the hub card's own recipe. Refuses before creating anything."""
    why = validate_control(control, policy)
    if why:
        raise GearBRefused(why)
    if not spec.get("training_supported") or not spec.get("training_image"):
        raise GearBRefused("hub card %s publishes no training recipe (TrainingSupported=%s)" % (spec.get("model_id"), spec.get("training_supported")))
    if not manifest.get("ok") or not manifest.get("eligibility_digest"):
        raise GearBRefused("dataset manifest is not eligible")
    it = str(control["instance_type"])
    price = cg.hourly_price(pricing, s3, private_bucket, it, family="training")
    hourly = price.get("usd_per_hour")
    if not hourly:
        raise GearBRefused("no live price for %s (training) -- refusing unpriced spend" % it)
    hours = float(control["max_runtime_s"]) / 3600.0
    cap_usd = round(float(hourly) * hours, 4)  # spot bills less; the cap is the on-demand ceiling
    records = _job_records(s3, private_bucket)
    why = budget_check(control, records, cap_usd, projected, policy, float(hourly), hours)
    if why:
        raise GearBRefused(why)
    stamp = now().strftime("%Y%m%d-%H%M%S")
    name = re.sub(r"[^a-zA-Z0-9-]", "-", "jh-gearb-gen%d-%s" % (manifest["generation"], stamp))[:63].rstrip("-")
    hp = {k: str(v["default"]) for k, v in (spec.get("hyperparameters") or {}).items() if v.get("default") is not None}
    hp["mode"] = launch_mode(control, manifest)            # sft | dpo, decided from this dataset's preference pairs (2026-09-22)
    hp.update({str(k): str(v) for k, v in (control.get("lora") or {}).items()})
    if spec.get("training_script"):
        hp["sagemaker_program"] = hp.get("sagemaker_program", "transfer_learning.py")
        hp["sagemaker_submit_directory"] = spec["training_script"]
        hp.setdefault("sagemaker_container_log_level", "20")
        hp.setdefault("sagemaker_region", region)
    hp["sagemaker_job_name"] = name
    # hard stop inside the runtime cap: the trainer plans its steps from the data and saves the adapter before this budget
    # (four fixed-400-step jobs died at MaxRuntime with nothing saved, 2026-09-15/16)
    hp.setdefault("time_budget_s", str(max(900, int(control["max_runtime_s"]) - 1200)))
    training_uri = "s3://%s/%s" % (private_bucket, manifest["prefix"])
    out_uri = "s3://%s/factory/champions/gen-%d/" % (private_bucket, manifest["generation"])
    channels = [{"ChannelName": "training", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": training_uri, "S3DataDistributionType": "FullyReplicated"}}}]
    if spec.get("training_artifact"):
        channels.append({"ChannelName": "model", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": spec["training_artifact"], "S3DataDistributionType": "FullyReplicated"}}})
    record = {"schema_version": "gearb-job.v1", "job_name": name, "kind": hp.get("mode", "sft"), "generation": manifest["generation"], "model_id": spec["model_id"],
              "instance_type": it, "spot": use_spot(s3, private_bucket, control), "max_runtime_s": int(control["max_runtime_s"]), "usd_per_hour": float(hourly),
              "cap_usd": cap_usd, "price_source": price.get("source"), "training_uri": training_uri, "out_uri": out_uri,
              "eligibility_digest": manifest["eligibility_digest"], "train_sha256": manifest["train_sha256"], "holdout_digest": manifest.get("holdout_digest"),
              "launched_at": now_iso(), "status": "launching", "version": VERSION}
    if not put_json_absent(s3, private_bucket, JOBS_PREFIX + name + ".json", record):
        raise GearBRefused("job record %s already exists" % name)
    sm.create_training_job(
        TrainingJobName=name, RoleArn=role_arn,
        AlgorithmSpecification={"TrainingImage": spec["training_image"], "TrainingInputMode": "File"},
        HyperParameters=hp, InputDataConfig=channels, OutputDataConfig={"S3OutputPath": out_uri},
        ResourceConfig={"InstanceType": it, "InstanceCount": 1, "VolumeSizeInGB": 100},
        StoppingCondition=_stop(int(control["max_runtime_s"]), record["spot"]), EnableManagedSpotTraining=bool(record["spot"]),
        Tags=cg.tags(TAG_PURPOSE, None) + [{"Key": "jh-factory", "Value": "gearb-gen-%d" % manifest["generation"]}],
        Environment={"JH_GEARB_ELIGIBILITY_DIGEST": manifest["eligibility_digest"], "JH_GEARB_TRAIN_SHA256": manifest["train_sha256"],
                     "JH_GEARB_HOLDOUT_DIGEST": str(manifest.get("holdout_digest") or "")},
    )
    return record


EXAM_JOBS_PREFIX = "factory/bursts/jobs/"
EXAM_RESULTS_PREFIX = "factory/exams/code/results/"
EXAM_PROMPTS_ONLY_PREFIX = "factory/exams/code/prompts-only/"
CHAMPIONS_PREFIX = "factory/champions/"


def _exam_records(s3, private_bucket: str) -> List[Dict[str, Any]]:
    out = []
    for key in list_keys(s3, private_bucket, EXAM_JOBS_PREFIX, 2000):
        if not key.endswith(".json") or key.endswith(".terminal.json"):
            continue
        doc = get_json(s3, private_bucket, key)
        if isinstance(doc, dict) and doc.get("kind") == "exam":
            out.append(doc)
    return out


def _extract_adapter(s3, private_bucket: str, candidate: Dict[str, Any]) -> Dict[str, Any]:
    """Copy the trained adapter out of the job's model.tar.gz into factory/champions/gen-N/adapter/ (create-if-absent manifest)."""
    import hashlib
    import io
    import tarfile
    gen = int(candidate["generation"])
    prefix = "%sgen-%d/adapter/" % (CHAMPIONS_PREFIX, gen)
    man_key = "%sgen-%d/manifest.json" % (CHAMPIONS_PREFIX, gen)
    have = get_json(s3, private_bucket, man_key)
    if have:
        return {"adapter_prefix": prefix, "manifest": have, "extracted": False}
    key = str(candidate["artifact"]).split(private_bucket + "/", 1)[1]
    tf = tarfile.open(fileobj=io.BytesIO(s3.get_object(Bucket=private_bucket, Key=key)["Body"].read()))
    names = tf.getnames()
    mf = [n for n in names if n.endswith("train_manifest.json")]
    train_manifest = json.loads(tf.extractfile(mf[0]).read()) if mf else {}
    if train_manifest.get("status") != "trained":
        raise GearBRefused("training manifest status %s -- refusing to examine" % train_manifest.get("status"))
    files, total = [], 0
    for m in tf.getmembers():
        if m.isfile() and ("/adapter/" in ("/" + m.name) or m.name.startswith("adapter/")):
            rel = m.name.split("adapter/", 1)[1]
            data = tf.extractfile(m).read()
            s3.put_object(Bucket=private_bucket, Key=prefix + rel, Body=data)
            files.append({"path": rel, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}); total += len(data)
    if not any(f["path"] == "adapter_config.json" for f in files):
        raise GearBRefused("no adapter_config.json in the job output (%s)" % names[:8])
    manifest = {"schema_version": "factory-adapter.v1", "generation": gen, "job": candidate.get("job_name"), "artifact": candidate["artifact"],
                "files": files, "total_bytes": total, "train_manifest": train_manifest, "extracted_at": now_iso()}
    put_json_absent(s3, private_bucket, man_key, manifest)
    return {"adapter_prefix": prefix, "manifest": manifest, "extracted": True}


MERGE_JOBS_PREFIX = "factory/gearb/merges/"
SERVED_PREFIX = "factory/models/served/"


def merge_pending(sm, s3, *, private_bucket: str, control: Dict[str, Any], role_arn: str, pricing, region: str = "us-east-1") -> Optional[Dict[str, Any]]:
    """2026-09-23: a promoted champion is an adapter the endpoint never serves. When champion.json names a generation
    whose merged weights do not exist yet, launch ONE merge job (factory/training/merge_adapter.py: base + adapter ->
    merge_and_unload -> uncompressed shards + merge_manifest.json under factory/models/served/gen-N/).
    scripts/factory_serve_champion.py (runner, factory-exam.yml) swaps the endpoint to it. Never trains, never serves."""
    import cost_guard as cg
    import gear_b_own as own
    champ = get_json(s3, private_bucket, CHAMPION_KEY)
    if not isinstance(champ, dict) or not champ.get("adapter") or not champ.get("generation"):
        return None
    gen = int(str(champ["generation"]).replace("gen-", ""))
    merged_prefix = "%sgen-%d/" % (SERVED_PREFIX, gen)
    if get_json(s3, private_bucket, merged_prefix + "merge_manifest.json"):
        if champ.get("merged_prefix") != "s3://%s/%s" % (private_bucket, merged_prefix):
            champ["merged_prefix"] = "s3://%s/%s" % (private_bucket, merged_prefix)
            champ["merged_at"] = now_iso()
            put_json(s3, private_bucket, CHAMPION_KEY, champ)
        return {"generation": gen, "merged": True}
    rec_key = "%sgen-%d.json" % (MERGE_JOBS_PREFIX, gen)
    rec = get_json(s3, private_bucket, rec_key)
    if isinstance(rec, dict):
        try:
            st = sm.describe_training_job(TrainingJobName=rec["job_name"])["TrainingJobStatus"]
        except Exception:  # noqa: BLE001
            st = "Unknown"
        if st in ("InProgress", "Stopping", "Unknown"):
            return {"generation": gen, "merge": st, "job_name": rec["job_name"]}
        if st in ("Failed", "Stopped") and int(rec.get("attempts") or 1) >= 2:
            return {"generation": gen, "merge": st, "job_name": rec["job_name"], "gave_up": True}
    it = str(control.get("instance_type") or "ml.g5.2xlarge")
    max_s = int(control.get("merge_max_runtime_s") or 3600)
    hourly = cg.hourly_price(pricing, s3, private_bucket, it, family="training").get("usd_per_hour")
    if not hourly:
        raise GearBRefused("no live price for %s (merge) -- refusing unpriced spend" % it)
    adapter_uri = "s3://%s/%s" % (private_bucket, str(champ["adapter"]).lstrip("/"))
    prompts = sorted({EXAM_PROMPTS_ONLY_PREFIX + k[len(EXAM_PROMPTS_ONLY_PREFIX):].split("/", 1)[0] + "/"
                      for k in list_keys(s3, private_bucket, EXAM_PROMPTS_ONLY_PREFIX, 2000) if "/" in k[len(EXAM_PROMPTS_ONLY_PREFIX):]})
    spec = own.burst_spec(s3, private_bucket, control, mode="exam", tasks_uri="s3://%s/%s" % (private_bucket, prompts[-1]) if prompts else None, adapter_uri=adapter_uri)
    name = re.sub(r"[^a-zA-Z0-9-]", "-", "jh-merge-gen%d-%s" % (gen, now().strftime("%Y%m%d-%H%M%S")))[:63].rstrip("-")
    hp = {"sagemaker_program": "merge_adapter.py", "sagemaker_submit_directory": spec["training_script"], "sagemaker_container_log_level": "20",
          "sagemaker_region": region, "sagemaker_job_name": name, "merged_prefix": "s3://%s/%s" % (private_bucket, merged_prefix), "generation": "gen-%d" % gen,
          "base_revision": str(spec.get("version") or "")}
    channels = [{"ChannelName": "model", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": spec["training_artifact"], "S3DataDistributionType": "FullyReplicated"}}},
                {"ChannelName": "adapter", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": adapter_uri, "S3DataDistributionType": "FullyReplicated"}}}]
    record = {"schema_version": "gearb-merge-job.v1", "job_name": name, "kind": "merge", "generation": gen, "adapter_uri": adapter_uri, "merged_prefix": hp["merged_prefix"],
              "instance_type": it, "spot": use_spot(s3, private_bucket, control), "max_runtime_s": max_s, "usd_per_hour": float(hourly), "cap_usd": round(float(hourly) * max_s / 3600.0, 4),
              "launched_at": now_iso(), "attempts": int((rec or {}).get("attempts") or 0) + 1, "status": "launching"}
    put_json(s3, private_bucket, rec_key, record)
    kw = dict(TrainingJobName=name, RoleArn=role_arn, AlgorithmSpecification={"TrainingImage": spec["training_image"], "TrainingInputMode": "File"},
              HyperParameters=hp, InputDataConfig=channels, OutputDataConfig={"S3OutputPath": "s3://%s/%s" % (private_bucket, MERGE_JOBS_PREFIX)},
              ResourceConfig={"InstanceType": it, "InstanceCount": 1, "VolumeSizeInGB": 150},
              StoppingCondition=_stop(max_s, record["spot"]), EnableManagedSpotTraining=bool(record["spot"]), Environment={"JH_MERGE": name})
    try:
        sm.create_training_job(**kw, Tags=cg.tags("factory-merge-gen%d" % gen, 3) + [{"Key": "jh-factory", "Value": "merge-gen-%d" % gen}])
    except Exception as exc:  # noqa: BLE001
        if "AddTags" not in str(exc):
            raise
        sm.create_training_job(**kw)
    return {"generation": gen, "merge": "launched", "job_name": name, "cap_usd": record["cap_usd"]}


def examine_pending(sm, s3, *, private_bucket: str, control: Dict[str, Any], role_arn: str, pricing, region: str = "us-east-1") -> Optional[Dict[str, Any]]:
    """The exam launches itself: newest candidate with exam.status pending_exam -> adapter extracted -> one frozen-holdout
    exam job (greedy, prompts only, spot, capped by exam_max_runtime_s). One exam in flight at a time; the grade is
    written by factory-exam.yml (network-less container) and the decision by decide_pending(). Never trains."""
    import cost_guard as cg
    import gear_b_own as own
    cands = [get_json(s3, private_bucket, k) for k in list_keys(s3, private_bucket, CANDIDATE_PREFIX, 500)]
    pending = sorted([c for c in cands if isinstance(c, dict) and (c.get("exam") or {}).get("status") == "pending_exam"], key=lambda c: int(c.get("generation") or 0))
    if not pending:
        return None
    cand = pending[-1]
    gen = int(cand["generation"])
    for rec in _exam_records(s3, private_bucket):
        if str(rec.get("exam_generation")) == "gen-%d" % gen:
            return {"generation": gen, "skipped": "exam job already launched", "job_name": rec.get("job_name")}
    inflight = [j["TrainingJobName"] for j in sm.list_training_jobs(StatusEquals="InProgress", NameContains="jh-exam-gen", MaxResults=20).get("TrainingJobSummaries", [])]
    if inflight:
        return {"generation": gen, "skipped": "an exam is in flight", "job_name": inflight[0]}
    ad = _extract_adapter(s3, private_bucket, cand)
    adapter_uri = "s3://%s/%s" % (private_bucket, ad["adapter_prefix"])
    it = str(control.get("instance_type") or "ml.g5.2xlarge")
    max_s = int(control.get("exam_max_runtime_s") or 3600)
    price = cg.hourly_price(pricing, s3, private_bucket, it, family="training")
    hourly = price.get("usd_per_hour")
    if not hourly:
        raise GearBRefused("no live price for %s (exam) -- refusing unpriced spend" % it)
    cap = round(float(hourly) * max_s / 3600.0, 4)
    prefixes = sorted({EXAM_PROMPTS_ONLY_PREFIX + k[len(EXAM_PROMPTS_ONLY_PREFIX):].split("/", 1)[0] + "/"
                       for k in list_keys(s3, private_bucket, EXAM_PROMPTS_ONLY_PREFIX, 2000) if "/" in k[len(EXAM_PROMPTS_ONLY_PREFIX):]})
    if not prefixes:
        raise GearBRefused("no frozen prompts-only exam under %s" % EXAM_PROMPTS_ONLY_PREFIX)
    tasks_uri = "s3://%s/%s" % (private_bucket, prefixes[-1])
    spec = own.burst_spec(s3, private_bucket, control, mode="exam", tasks_uri=tasks_uri, adapter_uri=adapter_uri)
    name = re.sub(r"[^a-zA-Z0-9-]", "-", "jh-exam-gen%d-%s" % (gen, now().strftime("%Y%m%d-%H%M%S")))[:63].rstrip("-")
    hp = {k: str(v["default"]) for k, v in spec["hyperparameters"].items()}
    hp.update({"sagemaker_submit_directory": spec["training_script"], "sagemaker_container_log_level": "20", "sagemaker_region": region,
               "sagemaker_job_name": name, "max_model_len": "8192", "task_cap": "1000", "adapter_generation": "gen-%d" % gen})
    channels = [{"ChannelName": "model", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": spec["training_artifact"], "S3DataDistributionType": "FullyReplicated"}}},
                {"ChannelName": "tasks", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": tasks_uri, "S3DataDistributionType": "FullyReplicated"}}},
                {"ChannelName": "adapter", "DataSource": {"S3DataSource": {"S3DataType": "S3Prefix", "S3Uri": adapter_uri, "S3DataDistributionType": "FullyReplicated"}}}]
    out_uri = "s3://%s/factory/bursts/" % private_bucket
    record = {"schema_version": "factory-burst-job.v1", "job_name": name, "kind": "exam", "generation": gen, "exam_generation": "gen-%d" % gen, "model_id": spec["model_id"],
              "adapter_uri": adapter_uri, "training_image": spec["training_image"], "bundle_uri": spec["training_script"], "tasks_uri": tasks_uri,
              "instance_type": it, "spot": use_spot(s3, private_bucket, control), "max_runtime_s": max_s, "usd_per_hour": float(hourly), "cap_usd": cap, "out_uri": out_uri,
              "launched_at": now_iso(), "launched_by": "gear_b.examine_pending", "status": "launching"}
    if not put_json_absent(s3, private_bucket, EXAM_JOBS_PREFIX + name + ".json", record):
        raise GearBRefused("exam job record %s already exists" % name)
    kw = dict(TrainingJobName=name, RoleArn=role_arn, AlgorithmSpecification={"TrainingImage": spec["training_image"], "TrainingInputMode": "File"},
              HyperParameters=hp, InputDataConfig=channels, OutputDataConfig={"S3OutputPath": out_uri},
              ResourceConfig={"InstanceType": it, "InstanceCount": 1, "VolumeSizeInGB": 120},
              StoppingCondition=_stop(max_s, record["spot"]), EnableManagedSpotTraining=bool(record["spot"]), Environment={"JH_BURST": name})
    try:
        sm.create_training_job(**kw, Tags=cg.tags("factory-exam-gen%d" % gen, 3) + [{"Key": "jh-factory", "Value": "exam-gen-%d" % gen}])
    except Exception as exc:  # noqa: BLE001
        if "AddTags" not in str(exc):
            raise
        sm.create_training_job(**kw)
    cand["exam"] = {"status": "exam_running", "job_name": name, "launched_at": record["launched_at"], "adapter_prefix": ad["adapter_prefix"]}
    put_json(s3, private_bucket, "%sgen-%d.json" % (CANDIDATE_PREFIX, gen), cand)
    return {"generation": gen, "job_name": name, "cap_usd": cap, "adapter_extracted": ad["extracted"], "launched": True}


def decide_pending(s3, private_bucket: str) -> Optional[Dict[str, Any]]:
    """When a candidate's exam result exists (written by the runner), apply the shared promotion contract against the
    pinned base exam; a promoted adapter becomes factory/gearb/champion.json, a rejected one keeps its result on record."""
    cands = [get_json(s3, private_bucket, k) for k in list_keys(s3, private_bucket, CANDIDATE_PREFIX, 500)]
    running = [c for c in cands if isinstance(c, dict) and (c.get("exam") or {}).get("status") == "exam_running"]
    if not running:
        return None
    base = get_json(s3, private_bucket, EXAM_RESULTS_PREFIX + "base.json")
    decisions = []
    for cand in running:
        gen = int(cand["generation"])
        result = None
        for key in list_keys(s3, private_bucket, EXAM_RESULTS_PREFIX + "gen-%d-" % gen, 50):
            doc = get_json(s3, private_bucket, key)
            if isinstance(doc, dict) and doc.get("burst") == cand["exam"].get("job_name"):
                result = dict(doc, result_key=key)
        if not result:
            continue
        if not base:
            decisions.append({"generation": gen, "held": "no pinned base exam"}); continue
        decision = promotion(result, base)
        record = {"schema_version": "gearb-decision.v1", "generation": gen, "job_name": cand["exam"].get("job_name"), "result_key": result["result_key"],
                  "candidate_score": result.get("score"), "base_score": base.get("score"), "delta": round(float(result.get("score") or 0) - float(base.get("score") or 0), 4),
                  "critical_failures": result.get("critical_failures"), "decision": decision, "decided_at": now_iso(), "version": VERSION}
        put_json_absent(s3, private_bucket, "factory/gearb/decisions/gen-%d.json" % gen, record)
        if decision.get("eligible"):
            champion = {"schema_version": "gearb-champion.v1", "generation": gen, "adapter": cand["exam"].get("adapter_prefix"), "score": result.get("score"),
                        "base_score": base.get("score"), "evaluation_id": result.get("evaluation_id"), "job_name": cand.get("job_name"), "promoted_at": now_iso(),
                        "release_status": decision.get("release_status"), "version": VERSION}
            put_json(s3, private_bucket, CHAMPION_KEY, champion)
            cand["exam"] = dict(cand["exam"], status="promoted", decided_at=record["decided_at"], result_key=result["result_key"])
        else:
            cand["exam"] = dict(cand["exam"], status="rejected", reason=decision.get("reason"), decided_at=record["decided_at"], result_key=result["result_key"])
        put_json(s3, private_bucket, "%sgen-%d.json" % (CANDIDATE_PREFIX, gen), cand)
        decisions.append(record)
    return {"decisions": decisions} if decisions else None


def poll_jobs(sm, s3, private_bucket: str) -> List[Dict[str, Any]]:
    """Describe every launching/running job; write an immutable terminal record when one finishes."""
    updates = []
    for row in _job_records(s3, private_bucket):
        if row.get("status") not in ("launching", "InProgress", "Stopping"):
            continue
        try:
            d = sm.describe_training_job(TrainingJobName=row["job_name"])
        except Exception as exc:  # noqa: BLE001
            # F09: an unreadable describe keeps the last known state and marks it unknown; it still blocks launches
            updates.append({"job_name": row["job_name"], "status": "unknown", "last_known_status": row.get("status"), "error": str(exc)[:200]})
            continue
        status = d.get("TrainingJobStatus")
        if status in ("Completed", "Failed", "Stopped"):
            terminal = dict(row, status=status, billable_s=d.get("BillableTimeInSeconds"), training_s=d.get("TrainingTimeInSeconds"),
                            artifact=(d.get("ModelArtifacts") or {}).get("S3ModelArtifacts"), failure=(d.get("FailureReason") or "")[:400],
                            finished_at=now_iso(), billed_usd_estimate=round(float(row.get("usd_per_hour") or 0) * float(d.get("BillableTimeInSeconds") or 0) / 3600.0, 4))
            put_json_absent(s3, private_bucket, JOBS_PREFIX + row["job_name"] + ".terminal.json", terminal)
            put_json(s3, private_bucket, JOBS_PREFIX + row["job_name"] + ".json", terminal)
            if status == "Completed" and terminal.get("artifact"):
                candidate = {"schema_version": "gearb-candidate.v1", "generation": row["generation"], "artifact": terminal["artifact"],
                             "job_name": row["job_name"], "eligibility_digest": row.get("eligibility_digest"), "holdout_digest": row.get("holdout_digest"),
                             "trained_at": terminal["finished_at"], "exam": {"status": "pending_exam"}, "version": VERSION}
                put_json_absent(s3, private_bucket, "%sgen-%d.json" % (CANDIDATE_PREFIX, row["generation"]), candidate)
            updates.append({"job_name": row["job_name"], "status": status})
        else:
            updates.append({"job_name": row["job_name"], "status": status})
    return updates


# ───────────────────────────────────────────────────────────────── exam + promotion
def promotion(candidate_eval: Dict[str, Any], champion_eval: Optional[Dict[str, Any]], *, minimum_cases: int = 30) -> Dict[str, Any]:
    """Weights are promoted only by the shared factory contract; the base model's own exam is the baseline."""
    from factory_core import promotion_decision  # shared module, same rule as every other candidate
    if not champion_eval:
        return {"eligible": False, "reason": "baseline_exam_missing"}
    return promotion_decision(candidate_eval, champion_eval, minimum_cases=minimum_cases)


def public_status(s3, private_bucket: str, policy: Dict[str, Any]) -> Dict[str, Any]:
    """Aggregate counts and money only; never job names, ARNs, or dataset text."""
    control = load_control(s3, private_bucket)
    why = validate_control(control, policy)
    records = _job_records(s3, private_bucket)
    since_day = now().replace(hour=0, minute=0, second=0, microsecond=0)
    season_since = None
    if control.get("approved_at"):
        try:
            season_since = datetime.fromisoformat(str(control["approved_at"]).replace("Z", "+00:00"))
        except Exception:  # noqa: BLE001
            season_since = None
    latest_manifest = None
    gens = []
    for key in list_keys(s3, private_bucket, DATASET_PREFIX, 5000):
        if key.endswith("/manifest.json"):
            gens.append(key)
    if gens:
        latest_manifest = get_json(s3, private_bucket, sorted(gens)[-1]) or None
    champion = get_json(s3, private_bucket, CHAMPION_KEY) or {}
    holdout = get_json(s3, private_bucket, HOLDOUT_KEY) or {}
    status = "armed" if why is None else "off"
    return {
        "version": VERSION, "status": status, "refusal": why, "enabled": bool(control.get("enabled")),
        "model_card": control.get("model_id"), "instance_type": control.get("instance_type"),
        "budget": {"daily_usd": control.get("daily_budget_usd"), "season_cap_usd": control.get("season_cap_usd"),
                   "committed_today_usd": spend(records, since_day)["usd"],
                   "committed_season_usd": spend(records, season_since)["usd"] if season_since else None},
        "holdout": {"frozen": bool(holdout.get("frozen_at")), "frozen_at": holdout.get("frozen_at"), "digest": digest(holdout) if holdout else None},
        "dataset": {k: latest_manifest.get(k) for k in ("generation", "kept", "floor", "missing_rows", "licenses", "kinds", "families", "ok", "reason", "at")} if latest_manifest else {"generation": 0, "kept": 0, "floor": control.get("min_sft_rows"), "ok": False, "reason": "no dataset built yet"},
        "jobs": {"total": len(records), "by_status": {s: sum(1 for r in records if r.get("status") == s) for s in sorted({r.get("status") or "unknown" for r in records})}},
        "champion": {"generation": champion.get("generation"), "score": champion.get("score"), "promoted_at": champion.get("promoted_at")} if champion else {"generation": 0, "note": "base model; no weights promoted"},
    }


def tick(sm, s3, *, private_bucket: str, public_bucket: str, policy: Dict[str, Any], role_arn: str, projected: Dict[str, Any], pricing: Dict[str, Any],
         describe_card, region: str = "us-east-1", launch: bool = True) -> Dict[str, Any]:
    """Hourly: poll jobs, (re)build the dataset if none is eligible, launch at most one SFT inside the caps."""
    out: Dict[str, Any] = {"at": now_iso(), "polled": [], "built": None, "launched": None, "refusal": None}
    control = load_control(s3, private_bucket)
    why = validate_control(control, policy)
    if why:
        out["refusal"] = why
        return out
    try:
        out["capacity_stops"] = rescue_capacity(sm, s3, private_bucket, control)     # a spot job starved of capacity never stalls the loop
    except Exception as cexc:  # noqa: BLE001
        out["capacity_stops"] = {"error": str(cexc)[:200]}
    out["polled"] = poll_jobs(sm, s3, private_bucket)
    if any(u.get("status") in ("launching", "InProgress", "Stopping", "unknown") for u in out["polled"]):
        out["refusal"] = "a job is still running" if not any(u.get("status") == "unknown" for u in out["polled"]) else "a job's state is unknown (reconcile before reserving more compute)"
        return out
    # the exam launches and decides itself (2026-09-17): trained candidate -> adapter -> frozen exam -> shared contract
    try:
        out["decided"] = decide_pending(s3, private_bucket)
        out["examined"] = examine_pending(sm, s3, private_bucket=private_bucket, control=control, role_arn=role_arn, pricing=pricing, region=region)
        try:
            out["merged"] = merge_pending(sm, s3, private_bucket=private_bucket, control=control, role_arn=role_arn, pricing=pricing, region=region)
        except Exception as mexc:  # noqa: BLE001 -- serving must never break training
            out["merged"] = {"error": str(mexc)[:300]}
    except GearBRefused as exc:
        out["examined"] = {"refusal": str(exc)}
    except Exception as exc:  # noqa: BLE001
        out["examined"] = {"error": str(exc)[:300]}
    if isinstance(out.get("examined"), dict) and (out["examined"].get("launched") or out["examined"].get("skipped") == "an exam is in flight"):
        out["refusal"] = "an exam is in flight (single training instance) -- training waits"
        return out
    manifest = latest_unlaunched_manifest(s3, private_bucket)
    if manifest is None:
        try:
            manifest = build_dataset(s3, private_bucket, public_bucket, control)
        except GearBRefused as exc:
            out["refusal"] = str(exc)
            return out
    out["built"] = {k: manifest.get(k) for k in ("ok", "generation", "kept", "floor", "missing_rows", "reason")}
    if not manifest.get("ok") or not launch:
        out["refusal"] = manifest.get("reason") or "launch disabled"
        return out
    try:
        if str(control.get("model_source") or "hub") == "own":
            import gear_b_own  # owned weights + recipe + image in Khalid's S3/ECR (2026-09-13); same refusal chain
            try:
                spec = gear_b_own.own_spec(s3, private_bucket, control)
            except gear_b_own.OwnSpecRefused as exc:
                raise GearBRefused("own model source: %s" % exc)
        else:
            spec = describe_card(control["model_id"], control.get("model_version"))
        out["launched"] = launch_sft(sm, s3, spec=spec, role_arn=role_arn, private_bucket=private_bucket, control=control, policy=policy,
                                     manifest=manifest, projected=projected, pricing=pricing, region=region)
    except GearBRefused as exc:
        out["refusal"] = str(exc)
    return out
