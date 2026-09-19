"""Extensions to self_improve — imported by self_improve and market_read."""
from __future__ import annotations

CONSTITUTION_V1 = [
    "Never treat Brain retrieval as a return forecast.",
    "A stance without a falsifier is invalid process — mark process_score down.",
    "Do not repeat a live lesson with weight >= 1 without a new regime fact on the board.",
    "Advisory-only while official prints are missing; do not promote market weights.",
    "Prefer pending/unknown to a confident empty call.",
    "Cite only board sources that are FRESH.",
]


def enforce_lessons(parsed, lessons):
    out = dict(parsed or {})
    if out.get("parse_error"):
        return out
    flags = []
    live = [l for l in (lessons or []) if isinstance(l, dict) and float(l.get("weight") or 0) >= 1.0 and l.get("hit") is False]
    for lesson in live:
        sleeve = str(lesson.get("asset") or lesson.get("sleeve") or "").lower()
        bad_stance = str(lesson.get("stance") or "").upper()
        if not sleeve or sleeve == "unknown":
            continue
        block = out.get(sleeve) if isinstance(out.get(sleeve), dict) else None
        if not block:
            continue
        stance = str(block.get("stance") or "").upper()
        if stance and bad_stance and stance == bad_stance:
            flags.append("repeats_lesson:%s:%s" % (sleeve, bad_stance))
    out["lesson_flags"] = flags
    if flags:
        out["lesson_violation"] = True
        blockers = list(out.get("release_blockers") or [])
        blockers.extend(flags)
        out["release_blockers"] = blockers
        out["decision_status"] = out.get("decision_status") or "ADVISORY_ONLY"
    return out


def process_score_read(parsed, board):
    out = dict(parsed or {})
    checks = {
        "valid_json": not bool(out.get("parse_error")),
        "has_overall": bool(str(out.get("overall") or "").strip()),
        "has_macro": bool(str(out.get("macro") or "").strip()),
        "four_sleeves": all(isinstance(out.get(k), dict) and out[k].get("stance") for k in ("stocks", "bonds", "metals", "crypto")),
        "has_falsifier": bool(out.get("what_would_change_my_mind")),
        "fresh_facts_only": True,
    }
    sources = (board or {}).get("sources") or {}
    cited_stale = [k for k, v in sources.items() if isinstance(v, dict) and v.get("status") not in (None, "FRESH", "OK")]
    if cited_stale:
        checks["fresh_facts_only"] = False
        out["stale_sources"] = cited_stale[:12]
    n = sum(1 for v in checks.values() if v)
    out["process_score"] = round(n / max(len(checks), 1), 4)
    out["process_checks"] = checks
    out["constitution"] = CONSTITUTION_V1
    return out


def think_rank(candidates, grader_fn):
    scored = []
    for i, cand in enumerate(candidates or []):
        try:
            s = float(grader_fn(cand))
        except Exception:
            s = -1.0
        scored.append((s, i, cand))
    scored.sort(key=lambda x: (-x[0], x[1]))
    best = scored[0] if scored else (None, None, None)
    return {
        "n": len(scored),
        "best_score": best[0],
        "best_index": best[1],
        "best": best[2],
        "discarded": [{"index": i, "score": s} for s, i, _ in scored[1:6]],
    }
