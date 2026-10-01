# Separate pending approval: Katlin's unqualified shortage leg

This is a read-only consumer trace and proposed repair. **No Katlin production change is in the diagnostic draft.** The false-green correction awaits specific user approval and independent review.

## Confirmed path

1. Bond Warroom's [`eurodollar_shortage`](https://github.com/ElMooro/si/blob/98f8049edaa148b660008de4c6487549d0071062/aws/lambdas/justhodl-bond-warroom/source/lambda_function.py#L699) returns `UNQUALIFIED`, `score=null`, `points=null`, `calls_eligible=false`, `sizing_eligible=false` when the funding classifier is unavailable.
2. Katlin's [`war_room`, lines1814–1817](https://github.com/ElMooro/si/blob/98f8049edaa148b660008de4c6487549d0071062/aws/lambdas/justhodl-katlin/source/lambda_function.py#L1814) tests only whether state is nonempty. It assigns 85 for a SHORTAGE substring, 50 for WATCH, otherwise 20. Thus UNQUALIFIED becomes 20.
3. [`add`, lines1792–1797](https://github.com/ElMooro/si/blob/98f8049edaa148b660008de4c6487549d0071062/aws/lambdas/justhodl-katlin/source/lambda_function.py#L1792) marks risk below45 GREEN. The null points are retained as value, concealing neither the null nor the green contradiction.
4. [`war_room`, lines2004–2027](https://github.com/ElMooro/si/blob/98f8049edaa148b660008de4c6487549d0071062/aws/lambdas/justhodl-katlin/source/lambda_function.py#L2004) includes the leg at weight1 in its local weighted thermometer. Local posture/cap thresholds then apply. Separate binding capital authority tightens the effective permission; a green local leg does not prove permission to enter.

At 2026-10-01T06:52:39.766372Z, [public Katlin output](https://justhodl.ai/data/katlin.json) had `war_room.legs[2]`: Eurodollar shortage, risk20, GREEN, read `UNQUALIFIED (None pts)`, value null. [Public bond output](https://justhodl.ai/data/bond-warroom.json) supplied the unqualified block. Current URLs can advance; this is a timestamped observation, not a forecast.

## Exact bounded fail-closed proposal — not implemented

For an explicit UNQUALIFIED block or literal false calls/sizing authority, record the shortage leg as unavailable using the existing missing-leg mechanism; do not contribute 20, zero, another neutral number, or any score to the denominator. Preserve the previous mapping byte-for-byte for other legacy qualified states in the first bounded patch. Any broader allowlist or numeric-qualification policy requires its own review.

Removing a contributor changes the local thermometer and can move local posture/cap; it is not merely a text fix. Review must compare all output changes with the unchanged implementation on the entire retained public fixture and isolated invented cases. Required scenarios: explicit UNQUALIFIED, either false permission, missing block, legacy CALM/WATCH/SHORTAGE, mixed and single-leg cases, local threshold boundaries, expired/missing/DATA_HOLD binding authority, false entry permission, zero effective sizing/cap, and malformed values. Verify that binding authority cannot be loosened and unrelated legs remain identical. Do not use other funding observations as replacement votes, change thresholds, or restore withdrawn classifier scores.
