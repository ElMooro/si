# Holdings exclusions in four composites

The calculation revision `holdings-direct-and-cluster-excluded.v1` removes direct
13F and legacy smart-money cluster scores from Compound Aggregator, Attention
Confluence, Flow Confluence and Master Ranker. This is a bounded consumer repair,
not certification of their other inputs, independence, returns or sizing.

- Exclusion happens before ticker-universe creation, scores, weights and agreement
  counts. Empty arrays mean an excluded inference, not zero holdings or activity.
- Each output identifies excluded input keys, source clocks, and a canonical-JSON
  input digest. This digest identifies the observed JSON; it does not by itself
  archive the input, authenticate an upstream claim or prove a full replay.
- Compound history keeps earlier snapshots, but percentile calculations use only
  days carrying the new score basis. Old scores are not commensurable with the
  revised calculation. Existing rolling history retention is unchanged.
- Master Ranker rejects old compound and flow-confluence packets without this
  calculation version. It also checks the actual component names/counts and
  compound arithmetic; a version field cannot disguise a legacy component.
- Four pages describe the boundary using the same packet they display. Missing
  or old packets show an unconfirmed status. They do not advertise independent
  evidence or validated returns on the strength of these changes.
- Controlled acceptance suppresses notifications and ranker events. Suppressed
  compound refreshes neither send alerts nor consume alert-deduplication state.

`ops_5884_holdings_derived_acceptance.py` checks actual packaged source and shared
modules against the exact source receipt/CodeSha256, preserves whole preceding
public products and score histories privately, invokes the three upstream
consumers before the ranker, and checks public JSON, HTML, JS and CSS. It publishes
`data/holdings-derived-consumer-verification.json` only after all checks pass.

Remaining work includes the native disclosure-overlap replacement, other indirect
consumers (including old CapitalFlow and stealth composites), point-in-time replay
of whole composite decisions, and independently qualified portfolio consequences.
