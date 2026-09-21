"""Runtime wrap of Gear B and market_read. Installed at lambda import.

Doctrine: do not train the model to sound like a better analyst.
Train the loop that can prove the next answer is less wrong than the last one.

Install is a no-op outside the Lambda runtime so factory unit tests keep the
unwrapped contract.
"""
from __future__ import annotations

import os
from typing import Any, Dict

_INSTALLED = False
NEW_SOURCE_KINDS = frozenset({"exam_fail", "preference_pair", "justhodl_native"})


def _in_lambda_runtime() -> bool:
    return bool(os.environ.get("AWS_LAMBDA_FUNCTION_NAME") or os.environ.get("JH_AI_DOCTRINE") == "1")


def install() -> bool:
    global _INSTALLED
    if _INSTALLED:
        return True
    if not _in_lambda_runtime():
        return False
    ok = False
    try:
        ok = _widen_kinds() or ok
    except Exception:
        pass
    try:
        ok = _wrap_gear_b() or ok
    except Exception:
        pass
    try:
        ok = _wrap_market_read() or ok
    except Exception:
        pass
    _INSTALLED = True
    return ok


def _widen_kinds() -> bool:
    import gear_b
    gear_b.ALLOWED_SOURCE_KINDS = frozenset(set(gear_b.ALLOWED_SOURCE_KINDS) | set(NEW_SOURCE_KINDS))
    return True


def _wrap_gear_b() -> bool:
    import gear_b
    import self_improve

    orig_tick = gear_b.tick
    orig_promo = gear_b.promotion
    orig_status = gear_b.public_status
    if getattr(orig_tick, "_doctrine", False):
        return True

    def tick(sm, s3, *, private_bucket: str, public_bucket: str, policy: Dict[str, Any], role_arn: str,
             projected: Dict[str, Any], pricing: Dict[str, Any], describe_card, region: str = "us-east-1",
             launch: bool = True):
        preview = orig_tick(
            sm, s3, private_bucket=private_bucket, public_bucket=public_bucket, policy=policy,
            role_arn=role_arn, projected=projected, pricing=pricing, describe_card=describe_card,
            region=region, launch=False,
        )
        preview["doctrine"] = self_improve.VERSION
        if preview.get("refusal"):
            return preview
        if not launch:
            return preview
        try:
            control = gear_b.load_control(s3, private_bucket)
            manifest = gear_b.latest_unlaunched_manifest(s3, private_bucket) or dict(preview.get("built") or {})
            why = self_improve.refuse_repeat_sft(gear_b._job_records(s3, private_bucket), manifest, control)
        except Exception as exc:
            why = None
            preview["doctrine_error"] = str(exc)[:200]
        if why:
            preview["refusal"] = why
            preview["launched"] = None
            return preview
        out = orig_tick(
            sm, s3, private_bucket=private_bucket, public_bucket=public_bucket, policy=policy,
            role_arn=role_arn, projected=projected, pricing=pricing, describe_card=describe_card,
            region=region, launch=True,
        )
        out["doctrine"] = self_improve.VERSION
        return out

    def promotion(candidate_eval, champion_eval, *, minimum_cases: int = 30, market_eval=None, market_base=None):
        raw = orig_promo(candidate_eval, champion_eval, minimum_cases=minimum_cases)
        return self_improve.tighten_promotion(
            raw, candidate_eval or {}, champion_eval or {}, market_eval=market_eval, market_base=market_base
        )

    def public_status(s3, private_bucket: str, policy: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(orig_status(s3, private_bucket, policy) or {})
        out["doctrine"] = self_improve.VERSION
        out["doctrine_tick_wrapped"] = True
        ds = out.get("dataset") or {}
        try:
            why = self_improve.refuse_repeat_sft([], ds, gear_b.load_control(s3, private_bucket))
        except Exception:
            why = None
        out["doctrine_would_refuse_sft"] = bool(why)
        out["doctrine_refuse_reason"] = (why or "")[:180] or None
        return out

    tick._doctrine = True  # type: ignore[attr-defined]
    promotion._doctrine = True  # type: ignore[attr-defined]
    public_status._doctrine = True  # type: ignore[attr-defined]
    gear_b.tick = tick
    gear_b.promotion = promotion
    gear_b.public_status = public_status
    return True


def _wrap_market_read() -> bool:
    import market_read as mr
    import self_improve

    orig_compose = mr.compose_read
    orig_lessons = mr.write_lessons
    if getattr(orig_compose, "_doctrine", False):
        return True

    def compose_read(board, play, complete_fn, lessons=None, playbook_text=True, budget=None):
        kw = {"lessons": lessons, "playbook_text": playbook_text}
        if budget is not None:
            kw["budget"] = budget
        parsed = orig_compose(board, play, complete_fn, **kw)
        try:
            parsed = self_improve.enforce_lessons(parsed, (lessons or {}).get("lessons") or [])
            parsed = self_improve.process_score_read(parsed, board)
        except Exception as exc:
            parsed = dict(parsed or {})
            parsed["self_improve_error"] = str(exc)[:200]
        return parsed

    def write_lessons(graded_rows, prior, complete_fn, fallback_fn=None):
        out = orig_lessons(graded_rows, prior, complete_fn, fallback_fn=fallback_fn)
        if out.get("lessons"):
            return out
        rows = [r for r in (graded_rows or []) if r.get("windows")]
        if not rows:
            return out
        built = []
        for r in rows:
            windows = r.get("windows") or {}
            if not isinstance(windows, dict):
                continue
            for w, oc in windows.items():
                if isinstance(oc, dict):
                    try:
                        built.append(self_improve.lesson_from_grade(r, int(w), oc))
                    except Exception:
                        continue
        if built:
            out = dict(out or {})
            out["lessons"] = built[:6]
            out["summary"] = out.get("summary") or "deterministic lessons from graded windows"
        return out

    compose_read._doctrine = True  # type: ignore[attr-defined]
    write_lessons._doctrine = True  # type: ignore[attr-defined]
    mr.compose_read = compose_read
    mr.write_lessons = write_lessons
    return True
