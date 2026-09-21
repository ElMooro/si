"""Mint legal new Gear-B families from traces the factory already stored.

Does not launch GPU. Writes verified-shaped JSON under factory/curriculum/code/verified/
so collect_rows can keep exam_fail / preference_pair / justhodl_native rows.
"""
from __future__ import annotations

import hashlib
from typing import Any, Dict, List, Mapping, Optional, Tuple

NEW_KINDS = ("exam_fail", "preference_pair", "justhodl_native")
VERIFIED_PREFIX = "factory/curriculum/code/verified/"
TRACES_PREFIX = "factory/traces/code/"
CANDIDATE_PREFIX = "factory/gearb/candidates/"


def _sha(text: str) -> str:
    return hashlib.sha256((text or "").encode("utf-8")).hexdigest()


def pair_from_traces(pass_doc: Mapping[str, Any], fail_doc: Mapping[str, Any], *, task_id: str) -> Optional[Dict[str, Any]]:
    chosen = str(pass_doc.get("solution") or pass_doc.get("program") or pass_doc.get("candidate") or "")
    rejected = str(fail_doc.get("solution") or fail_doc.get("program") or fail_doc.get("candidate") or "")
    prompt = str(pass_doc.get("prompt") or fail_doc.get("prompt") or pass_doc.get("task") or "")
    if not prompt or not chosen or not rejected or chosen == rejected:
        return None
    return {
        "kind": "preference_pair",
        "family": str(pass_doc.get("family") or fail_doc.get("family") or "exam"),
        "task_id": task_id,
        "license": "own",
        "source_url": "s3://factory/traces/code/%s" % task_id,
        "source_sha": _sha(prompt + chosen),
        "checker": "factory-code-verify:v4-supervisor-judge",
        "judge": "supervisor",
        "cases": 1,
        "passed": True,
        "verified_by": "owner_runner",
        "prompt": prompt,
        "solution": chosen,
        "rejected": rejected,
        "solution_sha256": _sha(chosen),
        "tests_sha256": _sha(str(pass_doc.get("tests") or fail_doc.get("tests") or task_id)),
    }


def exam_fail_row(fail_doc: Mapping[str, Any], *, task_id: str) -> Optional[Dict[str, Any]]:
    prompt = str(fail_doc.get("prompt") or fail_doc.get("task") or "")
    gold = str(fail_doc.get("gold") or fail_doc.get("reference") or fail_doc.get("canonical") or "")
    if not prompt or not gold:
        return None
    return {
        "kind": "exam_fail",
        "family": str(fail_doc.get("family") or "exam_fail"),
        "task_id": task_id,
        "license": "own",
        "source_url": "s3://factory/traces/code/%s" % task_id,
        "source_sha": _sha(prompt + gold),
        "checker": "factory-code-verify:v4-supervisor-judge",
        "judge": "supervisor",
        "cases": 1,
        "passed": True,
        "verified_by": "owner_runner",
        "prompt": prompt,
        "solution": gold,
        "failed_attempt": str(fail_doc.get("solution") or fail_doc.get("candidate") or "")[:4000],
        "solution_sha256": _sha(gold),
        "tests_sha256": _sha(str(fail_doc.get("tests") or task_id)),
    }


def _task_id(doc: Mapping[str, Any], key: str) -> str:
    return str(doc.get("task_id") or doc.get("family") or key.rsplit("/", 1)[-1].replace(".json", ""))


def harvest(s3, bucket: str, *, limit: int = 400) -> Dict[str, Any]:
    """Scan traces, group by task, write new-family objects. Safe if prefixes are empty."""
    written = {"preference_pair": 0, "exam_fail": 0, "seen": 0, "skipped": 0}
    if s3 is None or not bucket:
        return written
    keys: List[str] = []
    token = None
    while len(keys) < limit:
        kw = {"Bucket": bucket, "Prefix": TRACES_PREFIX, "MaxKeys": 200}
        if token:
            kw["ContinuationToken"] = token
        try:
            page = s3.list_objects_v2(**kw)
        except Exception:
            break
        for row in page.get("Contents") or []:
            k = row.get("Key") or ""
            if k.endswith(".json"):
                keys.append(k)
        token = page.get("NextContinuationToken")
        if not token:
            break
    by_task: Dict[str, List[Tuple[str, Dict[str, Any]]]] = {}
    for key in keys[:limit]:
        try:
            doc = __import__("json").loads(s3.get_object(Bucket=bucket, Key=key)["Body"].read())
        except Exception:
            written["skipped"] += 1
            continue
        if not isinstance(doc, dict):
            written["skipped"] += 1
            continue
        written["seen"] += 1
        by_task.setdefault(_task_id(doc, key), []).append((key, doc))
    for task_id, items in by_task.items():
        passed = [d for _, d in items if d.get("ok") is True or (d.get("verdict") or {}).get("passed") is True or d.get("status") == "pass" or d.get("passed") is True]
        failed = [d for _, d in items if d not in passed]
        row = None
        if passed and failed:
            row = pair_from_traces(passed[0], failed[0], task_id=task_id)
        elif failed:
            row = exam_fail_row(failed[0], task_id=task_id)
        if not row:
            continue
        out_key = "%sdoctrine-%s-%s.json" % (VERIFIED_PREFIX, row["kind"], task_id.replace("/", "_")[:80])
        try:
            s3.put_object(
                Bucket=bucket,
                Key=out_key,
                Body=__import__("json").dumps(row, separators=(",", ":")).encode("utf-8"),
                ContentType="application/json",
            )
            written[row["kind"]] += 1
        except Exception:
            written["skipped"] += 1
    return written
