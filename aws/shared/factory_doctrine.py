"""Factory military discipline. Teachers leave the hot path.
Ranks and kill rules live in factory-doctrine.json at repo root (copied into the
lambda zip only if the engine bundles it; otherwise these defaults match that file).
"""
from __future__ import annotations

RANKS = ("recruit", "specialist", "nco", "captain", "colonel", "student")
SPAWN_CAP = {"recruit": 0, "specialist": 0, "nco": 3, "captain": 8, "colonel": 15, "student": 20}
RESERVED = {"owner"}


def rank_of(card):
    r = str((card or {}).get("rank") or "recruit").lower()
    return r if r in SPAWN_CAP else "recruit"


def can_spawn(parent, want):
    if rank_of(parent) in RESERVED or str((parent or {}).get("id") or "").startswith("teacher-"):
        return False, "reserved"
    cap = SPAWN_CAP[rank_of(parent)]
    try:
        n = int(want or 0)
    except (TypeError, ValueError):
        return False, "spawn_count_required"
    if n < 1:
        return False, "spawn_count_required"
    if n > cap:
        return False, "rank_spawn_cap"
    return True, min(n, cap)


def child_card(parent, role, task, now):
    return {
        "rank": "recruit",
        "role": role,
        "task": (task or "")[:500],
        "co": (parent or {}).get("id") or (parent or {}).get("agent"),
        "spawned_at": now,
        "graded": 0,
        "errors": 0,
        "pass_rate": None,
        "prior_pass_rate": None,
    }


def verdict(card, window=None):
    """warehouse-graded window → promote | hold | retire."""
    c = card or {}
    graded = int(c.get("graded") or 0)
    errors = int(c.get("errors") or 0)
    pr = c.get("pass_rate")
    prior = c.get("prior_pass_rate")
    reasons = []
    if c.get("rogue") or c.get("writes_outside_factory"):
        return "retire", "rogue"
    if c.get("hallucinating") or (graded and not c.get("warehouse_keys")):
        return "retire", "hallucinating"
    if graded == 0 and c.get("stalled"):
        return "retire", "stalled"
    if graded < 8:
        return "hold", "min_graded_tasks"
    err = (errors / graded) if graded else 1.0
    if err > 0.15:
        return "retire", "error_rate"
    if prior is not None and pr is not None and float(pr) <= float(prior):
        return "hold", "not_self_improving"
    return "promote", "window_cleared"


def next_rank(current):
    r = rank_of({"rank": current})
    i = RANKS.index(r) if r in RANKS else 0
    return RANKS[min(i + 1, len(RANKS) - 1)]
