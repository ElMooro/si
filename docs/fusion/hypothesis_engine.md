# Hypothesis engine (Release 3)

Not built in Release 1 by design (spec: do not migrate everything before validating the foundation). Groundwork:
signals carry `supports[]`, `affects[]`, `invalidation`, and fusion already separates SUPPORTING (positive
effective) from OPPOSING evidence per horizon.

Release 3 shape: `data/jh-hypotheses/<entity>.json` with hypothesis_id, entity_id, type (absolute_bullish,
absolute_bearish, relative_outperformance, relative_underperformance, regime, sector_rotation, macro_transmission,
risk), benchmark, statement, horizon, status, supporting/opposing/neutral/invalidating signal ids,
invalidation_conditions, fusion_score, confidence. The conviction-engine's subjects ("Broad risk / equity beta",
"US equity -- value tilt") become the first regime/macro hypotheses; per-entity absolute hypotheses are seeded from
the pilot universe. Attachment is deterministic (direction + horizon + cluster) and audited in the ledger.
