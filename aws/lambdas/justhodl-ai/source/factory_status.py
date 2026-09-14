"""Where the factory stands, said by the factory itself -- from objects, never from an LLM (2026-09-14).

status_text(store)  : the self-report the owner gets for "where do you stand / what did you learn / status".
                      Every sentence maps to an S3 object; a missing object is reported as missing.
intake_task(...)    : the owner's "task: ..." messages become immutable task cards under factory/queue/tasks/.
                      A task is executable only if it can be graded: code tasks need acceptance tests (the owner
                      supplies them or the task is queued as 'ungraded' and answered from reading only);
                      inside tasks map to known lane actions (burst, verify, exam, status, spawn); outside tasks
                      become reading receipts + a card for the next burst. Nothing runs from chat text directly.
"""
from __future__ import annotations

import json
import re

from factory_core import digest, identifier, iso

INSIDE_ACTIONS = {"burst": "dispatch a trace burst on the curriculum (ops)", "verify": "run factory-trace-verify on the last burst",
                  "exam": "run the frozen HumanEval exam on the current adapter", "status": "self-report", "spawn": "materialize recruits"}
STATUS_WORDS = ("status", "where do you stand", "where are you", "what did you learn", "what have you learned", "progress", "report", "how far")


def _get(store, bucket, key):
    try:
        doc, _ = store.read(bucket, key)
        return doc if isinstance(doc, dict) else None
    except Exception:  # noqa: BLE001
        return None


def _list(store, bucket, prefix, cap=200):
    try:
        resp = store.s3.list_objects_v2(Bucket=bucket, Prefix=prefix, MaxKeys=cap)
        return sorted(o["Key"] for o in resp.get("Contents", []))
    except Exception:  # noqa: BLE001
        return []


def facts(store):
    """The objects behind the self-report."""
    pri, pub = store.private, store.public
    control = _get(store, pri, "factory/control/gearb.json") or {}
    model_id = control.get("model_id")
    manifest = _get(store, pri, "factory/models/base/%s/manifest.json" % model_id) if model_id else None
    champion = _get(store, pri, "factory/champions/current.json")
    bursts = [_get(store, pri, k) for k in _list(store, pri, "factory/bursts/jobs/")]
    bursts = [b for b in bursts if b]
    summaries = []
    for b in bursts[-5:]:
        keys = _list(store, pri, "factory/bursts/%s/summary-" % b.get("job_name"))
        if keys:
            summaries.append((b.get("job_name"), _get(store, pri, keys[-1])))
    jobs = [_get(store, pri, k) for k in _list(store, pri, "factory/gearb/jobs/")]
    jobs = [j for j in jobs if j]
    verified_keys = _list(store, pri, "factory/curriculum/code/verified/", cap=1000)
    state = _get(store, pub, "data/student-state.json") or {}
    return {"control": control, "manifest": manifest, "champion": champion, "bursts": bursts, "summaries": summaries, "jobs": jobs,
            "verified_count": len(verified_keys), "verified_truncated": len(verified_keys) >= 1000, "state": state}


def status_text(store):
    f = facts(store)
    c, m = f["control"], f["manifest"]
    lines = []
    if m:
        lines.append("Base model: %s (%s, license %s, %d hashed files, %.1f GB) staged in your bucket; training image pinned by digest in your ECR." % (
            m.get("repo"), str(m.get("revision"))[:12], m.get("license"), len(m.get("files") or []), float(m.get("total_bytes") or 0) / 1e9))
    else:
        lines.append("Base model: NOT staged (no manifest under factory/models/base/); I cannot train yet.")
    if c:
        lines.append("Gear B control: enabled=%s source=%s budget $%s/day $%s/season, written by %s." % (
            c.get("enabled"), c.get("model_source"), c.get("daily_budget_usd"), c.get("season_cap_usd"), c.get("written_by")))
    else:
        lines.append("Gear B control: absent -- training is off.")
    if f["champion"]:
        lines.append("Champion: generation %s (%s)." % (f["champion"].get("generation"), f["champion"].get("adapter") or f["champion"].get("uri")))
    else:
        lines.append("Champion: none promoted yet -- the base model is the champion; generation 1 trains once curate has its row floor.")
    if f["bursts"]:
        b = f["bursts"][-1]
        lines.append("Bursts: %d launched; last %s -- %s tasks x K=%s at T=%s on %s (cap $%s)." % (
            len(f["bursts"]), b.get("job_name"), b.get("tasks"), b.get("samples_per_task"), b.get("temperature"), b.get("instance_type"), b.get("cap_usd")))
    else:
        lines.append("Bursts: none yet.")
    for name, s in f["summaries"]:
        rep = (s or {}).get("report") or {}
        seen, passed = int(rep.get("seen") or 0), int(rep.get("passed") or 0)
        lines.append("Verified %s: %d candidates, %d passed (%s%%), %s rows kept for training." % (
            name, seen, passed, round(100.0 * passed / seen, 1) if seen else "n/a", (s or {}).get("rows_written")))
    lines.append("Verified curriculum rows on disk: %s%s." % (f["verified_count"], "+" if f["verified_truncated"] else ""))
    if f["jobs"]:
        j = f["jobs"][-1]
        lines.append("Training jobs: %d; last %s status %s (gen %s, cap $%s)." % (len(f["jobs"]), j.get("job_name"), j.get("status"), j.get("generation"), j.get("cap_usd")))
    else:
        lines.append("Training jobs: none launched yet.")
    st = f["state"]
    wall = (st.get("wall") or {})
    ranks = (st.get("ranks") or {})
    health = (st.get("health") or {})
    lines.append("Wall: %s entries posted, %s graded; season %s. Chain of command: %s active cards, %s retired. Health: %s (%d errors)." % (
        wall.get("posted", wall.get("entries", "?")), wall.get("graded", "?"), (st.get("season") or {}).get("id") if isinstance(st.get("season"), dict) else st.get("season"),
        ranks.get("active", "?"), ranks.get("retired", "?"), health.get("status", "?"), len(health.get("errors") or [])))
    lines.append("What I have learned so far = the rows above that passed an independent checker; nothing else counts. Read receipts are not lessons.")
    return "\n".join(lines)


def classify_task(text):
    t = text.lower()
    for word in INSIDE_ACTIONS:
        if re.search(r"\b%s\b" % word, t):
            return "inside", word
    if any(w in t for w in ("my repo", "justhodl", "lambda", "warehouse", "engine", "bucket", "s3://", "ops ")):
        return "inside", "lane-work"
    return "outside", "research"


def intake_task(store, agent, owner, text, *, tests=None):
    """Record the owner's task as an immutable card and say exactly what will happen with it."""
    if not owner:
        return None
    body = text.strip()
    body = re.sub(r"^task\s*:\s*", "", body, flags=re.I)
    scope, action = classify_task(body)
    now = iso(store.clock())
    tid = "task-" + digest(body + now)[:12]
    gradable = bool(tests) or action in ("burst", "verify", "exam", "status", "spawn")
    card = {"schema_version": "factory-task.v1", "id": tid, "from": identifier(agent), "at": now, "text": body[:2000], "scope": scope,
            "action": action, "gradable": gradable, "tests": (tests or "")[:4000] or None, "status": "queued",
            "route": ("lane action: " + INSIDE_ACTIONS[action]) if action in INSIDE_ACTIONS else
                     ("next burst as a prompt with the owner's acceptance tests; the verifier grades it" if tests else
                      "ungraded: answered from reading receipts + warehouse; supply `tests:` to make it a graded task")}
    store.immutable(store.private, "factory/queue/tasks/" + tid + ".json", card)
    return card


def is_status_request(text):
    t = text.lower()
    return any(w in t for w in STATUS_WORDS)


def is_task_request(text):
    return bool(re.match(r"^\s*task\s*:", text, flags=re.I))


def split_tests(text):
    """'task: ... tests: <python asserts>' -> (task text, tests)."""
    m = re.search(r"\btests\s*:\s*(.+)$", text, flags=re.S | re.I)
    if not m:
        return text, None
    return text[: m.start()].strip(), m.group(1).strip()
