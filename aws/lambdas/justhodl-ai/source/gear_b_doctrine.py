"""Runtime wrap of Gear B. Installed at lambda import.

Doctrine: do not train the model to sound like a better analyst.
Train the loop that can prove the next answer is less wrong than the last one.
"""
from __future__ import annotations

from typing import Any, Dict

_INSTALLED = False


def install() -> bool:
    global _INSTALLED
    if _INSTALLED:
        return True
    try:
        import gear_b
        import self_improve
    except Exception:
        return False

    orig_tick = gear_b.tick
    orig_promo = gear_b.promotion

    def tick(sm, s3, *, private_bucket: str, public_bucket: str, policy: Dict[str, Any], role_arn: str,
             projected: Dict[str, Any], pricing: Dict[str, Any], describe_card, region: str = "us-east-1",
             launch: bool = True):
        preview = orig_tick(
            sm, s3, private_bucket=private_bucket, public_bucket=public_bucket, policy=policy,
            role_arn=role_arn, projected=projected, pricing=pricing, describe_card=describe_card,
            region=region, launch=False,
        )
        if not launch:
            return preview
        try:
            control = gear_b.load_control(s3, private_bucket)
            manifest = gear_b.latest_unlaunched_manifest(s3, private_bucket) or {}
            if not manifest:
                manifest = dict(preview.get("built") or {})
            why = self_improve.refuse_repeat_sft(gear_b._job_records(s3, private_bucket), manifest, control)
        except Exception as exc:
            why = None
            preview["doctrine_error"] = str(exc)[:200]
        if why:
            preview["refusal"] = why
            preview["launched"] = None
            preview["doctrine"] = self_improve.VERSION
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

    tick._doctrine = True  # type: ignore[attr-defined]
    promotion._doctrine = True  # type: ignore[attr-defined]
    gear_b.tick = tick
    gear_b.promotion = promotion
    _INSTALLED = True
    return True
