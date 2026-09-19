"""Self-improve contract for the owned JustHodl model (2026-09-19).

The factory already trains. It does not get smarter when the data does not
change. This module is the brake and the gate:

- refuse another SFT hour on the same eligibility digest after N flat gens
- refuse promotion on a tie with the base exam
- require a market-holdout lift before weights can become champion
- turn graded market windows into lessons the next read must carry
- admit new families (exam fails, preference pairs, native desk tasks)

Nothing here creates an endpoint or spends GPU time. Gear-B tick calls it.
"""
from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Mapping, Optional

VERSION = "self-improve.1"
UTC = timezone.utc

BASE_CODING_PASSED = 135
BASE_CODING_N = 164
BASE_CODING_SCORE = 0.8232
BASE_MARKET_SCORE = 0.4982
BASE_MARKET_CRISIS = 0.6182
FROZEN_HOLDOUT_DIGEST_PREFIX = "b926ab5e305c6ce5764adfcbc93ea5f89bc596424f31e26a4cdea54066781fd2"


def _f(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def tighten_promotion(
    raw: Mapping[str, Any],
    candidate_eval: Mapping[str, Any],
    base_eval: Mapping[str, Any],
    *,
    market_eval: Optional[Mapping[str, Any]] = None,
    market_base: Optional[Mapping[str, Any]] = None,
    coding_min_delta_problems: int = 2,
    coding_min_delta_score: float = 0.015,
    market_min_delta_score: float = 0.02,
    crisis_max_regression: float = 0.05,
) -> Dict[str, Any]:
    """Ties do not promote. Market holdout must also move, when present."""
    out = dict(raw or {})
    reasons: List[str] = []
    if not out.get("eligible"):
        reasons.append(str(out.get("reason") or "factory_contract_refused"))

    cand_score = _f(candidate_eval.get("score"), _f(out.get("candidate_score")))
    base_score = _f(base_eval.get("score"), BASE_CODING_SCORE)
    cand_passed = _i(candidate_eval.get("passed"), _i(candidate_eval.get("n_passed")))
    base_passed = _i(base_eval.get("passed"), BASE_CODING_PASSED)
    cand_n = _i(candidate_eval.get("n"), _i(candidate_eval.get("n_answered"), BASE_CODING_N))

    score_lift = cand_score - base_score
    problem_lift = cand_passed - base_passed
    if cand_n and base_eval.get("n") and _i(base_eval.get("n")) != cand_n:
        reasons.append("holdout_n_changed")
    if problem_lift < coding_min_delta_problems and score_lift < coding_min_delta_score:
        reasons.append(
            "coding_tie_or_worse: passed %s/%s vs base %s/%s (need +%d problems or +%.3f score)"
            % (cand_passed, cand_n or BASE_CODING_N, base_passed, BASE_CODING_N, coding_min_delta_problems, coding_min_delta_score)
        )

    m_eval = market_eval or candidate_eval.get("market") or {}
    m_base = market_base or {}
    if m_eval:
        m_score = _f((m_eval.get("model_scores") or m_eval).get("score"), _f(m_eval.get("score")))
        m_crisis = _f((m_eval.get("model_scores") or m_eval).get("crisis_acc"), _f(m_eval.get("crisis_acc")))
        b_score = _f((m_base.get("model_scores") or m_base).get("score"), BASE_MARKET_SCORE)
        b_crisis = _f((m_base.get("model_scores") or m_base).get("crisis_acc"), BASE_MARKET_CRISIS)
        if m_score < b_score + market_min_delta_score:
            reasons.append(
                "market_holdout_flat: score %.4f vs base %.4f (need +%.3f)"
                % (m_score, b_score, market_min_delta_score)
            )
        if m_crisis + 1e-9 < b_crisis - crisis_max_regression:
            reasons.append(
                "crisis_regression: %.3f vs base %.3f (max drop %.2f)"
                % (m_crisis, b_crisis, crisis_max_regression)
            )

    if reasons:
        out["eligible"] = False
        out["reason"] = "; ".join(reasons)
        out["release_status"] = "refused"
    else:
        out["eligible"] = True
        out["release_status"] = out.get("release_status") or "promote"
        out["reason"] = None
    out["self_improve"] = VERSION
    out["coding_delta_score"] = round(score_lift, 4)
    out["coding_delta_problems"] = problem_lift
    return out


def refuse_repeat_sft(
    job_records: Iterable[Mapping[str, Any]],
    manifest: Mapping[str, Any],
    control: Mapping[str, Any],
) -> Optional[str]:
    """If the last N completed SFT jobs used the same digest, do not buy another GPU hour."""
    digest = str(manifest.get("eligibility_digest") or manifest.get("train_sha256") or "")
    if not digest:
        return "manifest has no eligibility_digest — refuse launch"
    need_families = int(control.get("require_new_family_after_flat_gens") or 3)
    kinds = manifest.get("kinds") or {}
    only_old = set(kinds) <= {"public_benchmark_train"} or (
        _i((kinds or {}).get("public_benchmark_train")) >= _i(manifest.get("kept")) * 0.9
        and _i(manifest.get("families") or 0) <= 2
    )
    same: List[str] = []
    for row in sorted(job_records, key=lambda r: str(r.get("launched_at") or ""), reverse=True):
        if str(row.get("kind") or "sft") not in ("sft", "pref"):
            continue
        if str(row.get("eligibility_digest") or row.get("train_sha256") or "") == digest:
            same.append(str(row.get("job_name") or row.get("generation")))
        if len(same) >= need_families:
            break
    if only_old:
        return (
            "curiosity refuse-SFT: dataset is still the old public_benchmark_train "
            "(%s rows, %s families, kinds=%s). Add exam_fail / preference_pair / "
            "justhodl_native families before another GPU hour."
            % (manifest.get("kept"), manifest.get("families"), kinds)
        )
    if len(same) >= need_families:
        return (
            "curiosity refuse-SFT: last %d jobs reused eligibility_digest %s… — "
            "unique_tasks must grow or kind must be pref on new pairs"
            % (len(same), digest[:12])
        )
    pref_pairs = _i(manifest.get("pref_pairs") or kinds.get("preference_pair"))
    if _i(control.get("min_dpo_pairs") or 0) > 0 and pref_pairs < _i(control.get("min_dpo_pairs")) and only_old:
        return "min_dpo_pairs=%s not met (have %s) and no new family" % (control.get("min_dpo_pairs"), pref_pairs)
    return None


def lesson_from_grade(call: Mapping[str, Any], window: int, outcome: Mapping[str, Any]) -> Dict[str, Any]:
    stance = str(call.get("stance") or call.get("prediction") or "").upper()
    asset = str(call.get("asset") or call.get("entity_id") or call.get("sleeve") or "unknown")
    label = str(outcome.get("label") or "")
    net = _f(outcome.get("net_return"))
    hit = bool(outcome.get("hit"))
    if "hit" not in outcome:
        if stance in ("AVOID", "SELL", "UNDERWEIGHT", "STAY OUT", "STAY_OUT") and net <= 0:
            hit = True
        elif stance in ("HOLD", "NEUTRAL") and abs(net) < 0.03:
            hit = True
        elif stance in ("SELECTIVE", "PICK CAREFULLY", "ACCUMULATE", "BUY", "OVERWEIGHT") and net > 0:
            hit = True
        else:
            hit = False
    if hit:
        text = "On %s, %s at %dd was consistent with the tape (net %+.2f%%)." % (asset, stance or "the call", window, 100 * net)
    else:
        text = "On %s, %s at %dd was wrong (net %+.2f%%). Do not repeat that sleeve stance without a new regime fact." % (
            asset, stance or "the call", window, 100 * net
        )
    return {
        "schema_version": "lesson.v1",
        "lesson": text,
        "evidence": "window=%dd label=%s net_return=%s call_id=%s" % (window, label, net, call.get("call_id") or call.get("prediction_id")),
        "window": window,
        "weight": 1.0 if not hit else 0.6,
        "call_id": call.get("call_id") or call.get("prediction_id"),
        "asset": asset,
        "stance": stance,
        "outcome": label or ("HIT" if hit else "MISS"),
        "hit": hit,
        "net_return": net,
    }


def inject_lessons_into_read(read: Mapping[str, Any], lessons: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    out = dict(read)
    carried = [dict(x) for x in lessons if isinstance(x, Mapping) and x.get("lesson")]
    out["lessons"] = carried
    out["lessons_carried"] = len(carried)
    block = "\n".join("- %s (%s)" % (x.get("lesson"), x.get("evidence") or "") for x in carried[:24])
    out["lessons_prompt"] = (
        "LESSONS CARRIED FROM GRADED CALLS — treat these as binding prior mistakes:\n" + block
        if block else ""
    )
    return out


def category_floor(counts: Mapping[str, int], floor: int = 20) -> Dict[str, Any]:
    learned = [k for k, n in counts.items() if n >= floor]
    excluded = [k for k in ("lesson", "macro") if counts.get(k, 0) < floor]
    return {
        "categories_learned": learned,
        "categories_excluded": excluded,
        "counts": dict(counts),
        "floor": floor,
        "ready": "lesson" not in excluded and "macro" not in excluded,
    }
