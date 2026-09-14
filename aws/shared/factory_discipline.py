"""Military discipline enforced inside the existing student tick (factory-doctrine.v1).

Doctrine lives in factory_doctrine.py / factory-doctrine.json. This module is the MP:
it builds one card per agent alias from warehouse-graded evidence only (market
results on official prints, grader verdicts on traces and code), computes the
21-day promotion window, calls factory_doctrine.verdict, and applies the outcome.

  * ranks are part of the authoritative student state (factory/runtime/current.json)
    and travel with the public projection as aliases only -- no IAM widening needed;
  * every promote/retire is an immutable ``discipline`` event (factory/events/*),
    never edited, never deleted; retired cards keep their evidence and are not invoked;
  * spawn caps come from factory_doctrine.SPAWN_CAP through the parent's current rank;
    children always start recruit and are materialized here, bounded by ROSTER_CAP,
    from spawn requests the gateway leaves under factory/queue/ (student-readable).

Pure logic plus bounded S3 reads through the factory Store. No model calls.
"""
from __future__ import annotations

from datetime import timedelta

from factory_core import digest, identifier, iso, timestamp
from factory_doctrine import RANKS, SPAWN_CAP, next_rank, rank_of, verdict

SCHEMA = "factory-ranks.v1"
WINDOW_DAYS = 21
STALL_DAYS = 14
ROSTER_CAP = 48          # active cards; compute stays COMPUTE_INFLIGHT in the gateway
READ_CAP = 300           # graded objects read per discipline pass
SPAWN_QUEUE_CAP = 10     # spawn requests examined per pass
PASS_SCORE = 0.5         # wall score at/above which a graded week counts as a pass
SUPERVISOR = "student"
BUILTIN = {"student": "student", "coder": "recruit", "researcher": "recruit", "investor": "recruit", "deployer": "recruit"}
PROMOTION_CEILING = "colonel"   # only the supervisor holds rank student; nobody is minted owner/teacher-*


def default_ranks(now):
    cards = {alias: {"rank": rank, "status": "active", "kind": "builtin", "since": iso(now), "co": None,
                     "graded_window": 0, "errors_window": 0, "pass_rate": None, "prior_pass_rate": None,
                     "last_graded_at": None, "last_verdict": None, "last_verdict_at": None, "verdicts": 0}
             for alias, rank in BUILTIN.items()}
    return {"schema_version": SCHEMA, "cards": cards, "spawn_requests": {}, "updated_at": iso(now),
            "doctrine": {"window_days": WINDOW_DAYS, "min_graded_tasks": 8, "max_error_rate": 0.15,
                         "stall_days": STALL_DAYS, "roster_cap": ROSTER_CAP, "spawn_cap": dict(SPAWN_CAP)}}


def ensure_ranks(state, now):
    ranks = state.get("ranks")
    if not isinstance(ranks, dict) or ranks.get("schema_version") != SCHEMA:
        ranks = default_ranks(now)
        state["ranks"] = ranks
    return ranks


def parent_rank(ranks, agent):
    """(rank, card) for a spawning agent. Unknown agents are recruits; retired cards cannot act."""
    agent = identifier(agent)
    card = ((ranks or {}).get("cards") or {}).get(agent)
    if card and card.get("status") == "retired":
        return "retired", card
    if card:
        return rank_of(card), card
    return BUILTIN.get(agent, "recruit"), None


def active_count(ranks):
    return sum(1 for c in (ranks.get("cards") or {}).values() if c.get("status") == "active")


# ---------------------------------------------------------------- grades → cards
def _list(store, bucket, prefix, cap):
    keys = []
    token = None
    while len(keys) < cap:
        args = {"Bucket": bucket, "Prefix": prefix, "MaxKeys": min(1000, cap - len(keys))}
        if token:
            args["ContinuationToken"] = token
        response = store.s3.list_objects_v2(**args)
        for item in response.get("Contents", []):
            keys.append((item["Key"], item.get("LastModified")))
        token = response.get("NextContinuationToken")
        if not response.get("IsTruncated") or not token:
            break
    return keys


LIST_CAP = 5000


def collect_grades(store, now, *, read_cap=READ_CAP):
    """Rows of warehouse-graded work: {alias, kind, at, outcome pass|fail|void, keys}.

    The whole prefix is listed (bounded by LIST_CAP) and filtered by LastModified first, then the NEWEST
    read_cap objects are read -- old results can never crowd out recent evidence. If more than read_cap
    recent objects exist the pass is marked truncated and no verdict is applied (see apply_verdicts)."""
    rows, reads = [], 0
    horizon = now - timedelta(days=2 * WINDOW_DAYS)
    sources = (("factory/salon/results/", "market"), ("factory/verdicts/", "verdict"))
    recent = []
    for prefix, kind in sources:
        listed = _list(store, store.private, prefix, LIST_CAP)
        if len(listed) >= LIST_CAP:
            rows.append({"truncated": True, "reason": "listing_cap:" + prefix})
        for key, modified in listed:
            if modified is not None and modified < horizon:
                continue
            recent.append((modified, key, kind))
    recent.sort(key=lambda r: (r[0] is None, r[0]), reverse=True)
    if len(recent) > read_cap:
        rows.append({"truncated": True, "reason": "read_cap"})
    for modified, key, kind in recent[:read_cap]:
        if True:
            doc, _ = store.read(store.private, key)
            reads += 1
            if not isinstance(doc, dict):
                continue
            row = _grade_row(doc, key, kind)
            if row:
                rows.append(row)
    return rows


def _grade_row(doc, key, kind):
    alias = doc.get("agent")
    if not isinstance(alias, str) or not alias:
        return None
    at = doc.get("graded_at")
    if not at:
        return None
    if kind == "market":
        status = doc.get("status")
        if status == "void":
            outcome = "void"
        elif status == "graded":
            outcome = "pass" if float(((doc.get("metrics") or {}).get("score") or 0)) >= PASS_SCORE else "fail"
        else:
            return None
        used = ["factory/official-prints/%s/%s.json" % (doc.get("week"), doc.get("symbol"))]
    else:
        if doc.get("kind") not in ("trace", "code"):
            return None
        outcome = "pass" if doc.get("ok") is True else "fail"
        used = ["factory/quarantine/%s.json" % doc.get("trace_id")] if doc.get("kind") == "trace" else ["factory/exams/code-identity-v1.json"]
    return {"alias": identifier(alias), "kind": kind, "at": at, "outcome": outcome, "keys": used, "evidence": key}


def window_stats(rows, now):
    """Per alias: this 21-day window vs the prior one. Voids count as errors, never as passes."""
    end, start, prior_start = now, now - timedelta(days=WINDOW_DAYS), now - timedelta(days=2 * WINDOW_DAYS)
    stats = {}
    for row in rows:
        if row.get("truncated"):
            continue
        at = timestamp(row["at"])
        if at < prior_start or at > end:
            continue
        card = stats.setdefault(row["alias"], {"graded": 0, "errors": 0, "passes": 0, "prior_graded": 0, "prior_passes": 0,
                                               "warehouse_keys": [], "last_graded_at": None})
        bucket = "this" if at >= start else "prior"
        if bucket == "this":
            card["graded"] += 1
            card["errors"] += int(row["outcome"] != "pass")
            card["passes"] += int(row["outcome"] == "pass")
            for k in row["keys"]:
                if k not in card["warehouse_keys"] and len(card["warehouse_keys"]) < 40:
                    card["warehouse_keys"].append(k)
        else:
            card["prior_graded"] += 1
            card["prior_passes"] += int(row["outcome"] == "pass")
        if card["last_graded_at"] is None or at > timestamp(card["last_graded_at"]):
            card["last_graded_at"] = row["at"]
    for card in stats.values():
        card["pass_rate"] = round(card["passes"] / card["graded"], 4) if card["graded"] else None
        card["prior_pass_rate"] = round(card["prior_passes"] / card["prior_graded"], 4) if card["prior_graded"] else None
    return stats


# ---------------------------------------------------------------- verdicts
def doctrine_card(alias, card, stat, now):
    since = timestamp(card.get("since") or iso(now))
    last = stat.get("last_graded_at") or card.get("last_graded_at")
    idle_from = timestamp(last) if last else since
    stalled = stat.get("graded", 0) == 0 and (now - idle_from) >= timedelta(days=STALL_DAYS)
    return {"id": alias, "rank": card.get("rank"), "graded": stat.get("graded", 0), "errors": stat.get("errors", 0),
            "pass_rate": stat.get("pass_rate"), "prior_pass_rate": stat.get("prior_pass_rate"),
            "warehouse_keys": stat.get("warehouse_keys", []), "stalled": stalled,
            "rogue": bool(card.get("rogue")), "hallucinating": bool(card.get("hallucinating"))}


def apply_verdicts(store, state, now, *, rows=None):
    """Run the doctrine over every card. Returns the list of applied changes (promote/retire only)."""
    ranks = ensure_ranks(state, now)
    rows = collect_grades(store, now) if rows is None else rows
    stats = window_stats(rows, now)
    changes = []
    truncated = any(r.get("truncated") for r in rows)
    for alias, card in sorted(ranks["cards"].items()):
        if card.get("status") != "active":
            continue
        stat = stats.get(alias, {})
        if truncated:
            card.update(last_verdict="hold:evidence_incomplete", last_verdict_at=iso(now))
            continue
        decision, reason = verdict(doctrine_card(alias, card, stat, now))
        promoted_at = card.get("promoted_at")
        if decision == "promote" and promoted_at:
            fresh = [r for r in rows if not r.get("truncated") and r["alias"] == alias and timestamp(r["at"]) > timestamp(promoted_at)]
            errors = sum(1 for r in fresh if r["outcome"] != "pass")
            if len(fresh) < 8 or errors / len(fresh) > 0.15:
                decision, reason = "hold", "no_new_qualifying_evidence_since_promotion:%d_new_grades" % len(fresh)
        if alias == SUPERVISOR and decision == "retire":
            decision, reason = "hold", "supervisor_retirement_is_owner_control:" + reason
        card.update(graded_window=stat.get("graded", 0), errors_window=stat.get("errors", 0),
                    pass_rate=stat.get("pass_rate"), prior_pass_rate=stat.get("prior_pass_rate"),
                    last_graded_at=stat.get("last_graded_at") or card.get("last_graded_at"),
                    last_verdict=decision + ":" + reason, last_verdict_at=iso(now))
        if decision == "promote":
            before = rank_of(card)
            after = next_rank(before)
            if alias != SUPERVISOR and RANKS.index(after) > RANKS.index(PROMOTION_CEILING):
                after = PROMOTION_CEILING
            if after == before:
                card["last_verdict"] = "hold:at_ceiling"
                continue
            card["rank"] = after
            card["promoted_at"] = iso(now)
            card["pass_rate_at_promotion"] = stat.get("pass_rate")
        elif decision == "retire":
            card["status"] = "retired"
            card["retired_at"] = iso(now)
        else:
            continue
        card["verdicts"] = int(card.get("verdicts") or 0) + 1
        receipt = {"alias": alias, "decision": decision, "reason": reason, "rank": card["rank"], "status": card["status"],
                   "window_days": WINDOW_DAYS, "window_end": iso(now), "graded": stat.get("graded", 0),
                   "errors": stat.get("errors", 0), "pass_rate": stat.get("pass_rate"),
                   "prior_pass_rate": stat.get("prior_pass_rate"), "warehouse_keys": stat.get("warehouse_keys", []),
                   "doctrine": "factory-doctrine.v1", "evidence_immutable": True}
        event_id = "discipline-" + alias + "-" + now.strftime("%Y%m%d") + "-" + digest(receipt)[:8]
        try:
            store.append_event("discipline", event_id, receipt)
            receipt["event"] = event_id
        except Exception as exc:  # noqa: BLE001 -- the decision stands in state; the receipt is retried next pass
            receipt["event_error"] = type(exc).__name__
        changes.append(receipt)
    ranks["truncated"] = any(r.get("truncated") for r in rows)
    ranks["updated_at"] = iso(now)
    return changes


# ---------------------------------------------------------------- spawn
def check_spawn(ranks, agent, want):
    """(ok, count_or_reason) for a spawn request, from the parent's current rank."""
    rank, card = parent_rank(ranks, agent)
    if rank == "retired":
        return False, "retired_card"
    if active_count(ranks) >= ROSTER_CAP:
        return False, "roster_full"
    from factory_doctrine import can_spawn  # local import keeps this module import-light for tests
    return can_spawn({"id": agent, "rank": rank}, want)


def materialize(store, state, now, *, queue_cap=SPAWN_QUEUE_CAP):
    """Turn gateway spawn requests (factory/queue/spawn-*.json) into recruit cards, bounded by ROSTER_CAP."""
    ranks = ensure_ranks(state, now)
    created = []
    for key, _ in _list(store, store.private, "factory/queue/spawn-", queue_cap):
        request, _ = store.read(store.private, key)
        if not isinstance(request, dict):
            continue
        batch = str(request.get("id") or "")
        if not batch:
            continue
        done = int((ranks.get("spawn_requests") or {}).get(batch, 0))
        wanted = int(request.get("requested") or 0)
        parent = identifier(str(request.get("spawned_by") or "student"))
        rank, card = parent_rank(ranks, parent)
        cap = SPAWN_CAP.get(rank, 0)
        allowed = min(wanted, cap) if rank != "retired" else 0
        while done < allowed and active_count(ranks) < ROSTER_CAP:
            done += 1
            alias = "r-" + batch[-8:] + "-" + str(done)
            ranks["cards"][alias] = {"rank": "recruit", "status": "active", "kind": "recruit", "since": iso(now), "co": parent,
                                     "batch": batch, "role": str(request.get("role") or "researcher")[:40],
                                     "task": str(request.get("task") or "")[:200], "graded_window": 0, "errors_window": 0,
                                     "pass_rate": None, "prior_pass_rate": None, "last_graded_at": None,
                                     "last_verdict": None, "last_verdict_at": None, "verdicts": 0}
            created.append(alias)
        ranks.setdefault("spawn_requests", {})[batch] = done
        if len(ranks["spawn_requests"]) > 200:
            oldest = sorted(ranks["spawn_requests"])[: len(ranks["spawn_requests"]) - 200]
            for old in oldest:
                ranks["spawn_requests"].pop(old, None)
    return created


def projection(ranks):
    """Aliases only, for the public feed."""
    cards = (ranks or {}).get("cards") or {}
    return {"schema_version": SCHEMA, "updated_at": (ranks or {}).get("updated_at"),
            "chain_of_command": "student -> colonel -> captain -> nco -> specialist -> recruit",
            "cards": [{"alias": alias, "rank": c.get("rank"), "status": c.get("status"), "kind": c.get("kind"),
                       "co": c.get("co"), "graded_window": c.get("graded_window", 0), "errors_window": c.get("errors_window", 0),
                       "pass_rate": c.get("pass_rate"), "prior_pass_rate": c.get("prior_pass_rate"),
                       "last_verdict": c.get("last_verdict"), "last_verdict_at": c.get("last_verdict_at")}
                      for alias, c in sorted(cards.items(), key=lambda kv: (RANKS.index(rank_of(kv[1])) * -1, kv[0]))][:200],
            "active": active_count(ranks or {}), "retired": sum(1 for c in cards.values() if c.get("status") == "retired")}
