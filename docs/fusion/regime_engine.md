# Regime engine (Release 1 v1, Release 4 target)

## What exists (reused)
`justhodl-regime-composite` (7 dimensions VOL/RISK_ON/LIQUIDITY/POLICY/REFLATION/SMART_MONEY/FUNDAMENTALS,
meta-regime label, composite -100..100), `justhodl-macro-regime`, `justhodl-cross-asset-regime` (emits
`regime.changed`), `justhodl-factor-regime`, `justhodl-vol-regime`, `justhodl-risk-regime`, `justhodl-regime-map`,
`justhodl-global-business-cycle` (phase + downturn probability), `justhodl-regime-conditional-trust`
(per-regime engine reliability from graded outcomes).

## Release 1
The market subject `market:US_EQUITY` receives MACRO signals (regime composite, LCE, global cycle) and RISK signals
(risk gate, crisis composite, tail risk). `jh_fusion_core.regime_context` computes a regime axis = freshness x
confidence weighted mean of those scores in [-1, 1] with labels SUPPORTIVE / MILDLY_SUPPORTIVE / MIXED /
MILDLY_HOSTILE / HOSTILE and a certainty (mean confidence x freshness). The axis feeds `regime_fit` and the
`regime_certainty` confidence component; a label change publishes `jhsignal.regime_changed`. The full per-leg detail
is published under `regime.legs`.

## Release 4
- Regime vector with the spec's dimensions (growth, inflation, liquidity, credit, rates, real rates, dollar,
  systemic stress, volatility, breadth, positioning, funding, commodity pressure), 0-100 each, sourced from the
  engines above and `justhodl-cycle-features`.
- Regime labels (EARLY_RECOVERY, GOLDILOCKS, LATE_CYCLE, REFLATION, STAGFLATION, LIQUIDITY_CONTRACTION,
  DEFLATIONARY_RISK_OFF, CREDIT_CRISIS, POLICY_EASING, POLICY_TIGHTENING) as a rule table over the vector.
- `regime_fit` and reliability replaced by conditional reliability from `data/regime-conditional-trust.json`
  with the fallback ladder regime+sector -> regime -> sector -> global and minimum-sample thresholds.
