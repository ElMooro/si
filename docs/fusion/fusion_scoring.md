# Fusion scoring v1 (deterministic, no ML)

Implementation: `aws/shared/jh_fusion_core.py` (pure functions; `run_fusion` -> `fuse_entity` -> `fuse_horizon`).
Every factor is published per signal in the ledger and in `data/jh-fusion.json` (`horizons.<H>.signals[]`).

## Per signal
```
weight    = confidence x freshness x reliability x independence x evidence_quality x inherited(0.7 | 1)
effective = score x weight x regime_fit
```
- freshness = `exp(-ln2 x age_days / half_life_days)` (from the snapshot)
- reliability = `data/engine-trust.json` effective_trust (non-WARMING) else signal-scorecard multiplier else 1.0,
  clamped [0.25, 1.5]; basis published per engine
- independence = evidence-cluster diminishing `1/(1+0.6k)` for the k-th confirmation inside a cluster, then x(1-corr)
  when signal-orthogonality reports engine correlation >= 0.5 with a stronger same-sign signal
- evidence_quality = geometric mean of quality.{source_reliability, data_completeness, calculation_quality}
- regime_fit = 1 unless the family is FLOW/MARKET/CATALYST/FUNDAMENTAL and the signal leans against the regime axis
  (then `1 - 0.4 x |regime score|`)
- inherited = 0.7 for the market subject's MACRO/RISK signals inherited by an equity/etf/crypto entity

## Per entity x horizon
```
fusion_score = sum(effective) / sum(weight)        [-1, 1]
conviction   = round(|fusion_score| x 100), direction = bucket(fusion_score)
bullish_evidence = sum(effective > 0), bearish_evidence = sum(|effective < 0|)   (both published)
independent_evidence_count = evidence clusters with |effective| >= 0.05
raw_signal_count           = all signals in the horizon (own + inherited)
family_scores[F]           = sum(effective)/sum(weight) inside family F (+ n, weight)
cluster_scores[C]          = same per evidence cluster
contradiction (0-100) = 100 x (0.6 two_sided + 0.25 family_disagreement + 0.15 horizon_disagreement)
   two_sided = 2 min(bull, bear)/(bull+bear); family_disagreement = share of material families against the overall sign;
   horizon_disagreement = 1 - |sum(sign per horizon)|/n across the entity's horizons with evidence
   classes: LOW < 25, MODERATE < 50, HIGH < 75, EXTREME
fusion_coverage = sum(expected[F] x present[F]) / sum(expected)   present = 1 fresh, 0.5 stale, 0 absent
   expected families per entity type live in config/jh-fusion-universe.json (crypto expects no FUNDAMENTAL)
confidence = (0.30 x (1 - e^(-clusters/3)) + 0.25 coverage + 0.15 reliability + 0.15 freshness + 0.10 completeness
              + 0.05 regime_certainty) x (1 - contradiction/200) x (1 - 0.15 horizon_disagreement)
```
Conviction answers "how bullish is the evidence"; confidence answers "how much should we trust it". A single
strong signal yields high conviction with low confidence -- by design.

## Also published
`top_supporting_evidence` / `top_opposing_evidence` (up to 8 each, with all factors), `attribution`
(`engine#type -> share of the net`), `what_changed` (prev score, delta, new/gone engines, contribution deltas vs the
prior run), `velocity` (1d/5d/20d deltas from the entity's own history, classification RAPID_CONVICTION_BUILD ...
RAPID_CONVICTION_DECAY), `best_horizon` (largest evidence mass x coverage), `capital_decision`, `size_modifier`,
`hard_vetoes`, `soft_vetoes`, `missing_families`.

`historical_edge`, `asymmetry` and `opportunity_score` are **not** published in Release 1 -- they need the analog
and asymmetry engines (Release 6) and would otherwise be fabricated.
