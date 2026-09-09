# Direct private-source graph closure check

Read-only source trace on 9 September 2026, followed by the separately authorized
personal trade-journal producer fix. No cloud requests or private payloads were read
or logged. The scan searched all Lambda Python sources for the 24 canonical private
paths then present in `aws/shared/private_artifact.py`, plus legacy aliases without
`data/`; 36 source files matched. It checked consumers outside the security migration's
32-function readiness set and pending personal-watchlist/volatility changes.

One additional confirmed personal-data boundary was found: `justhodl-trade-journal`,
which is distinct from public modeled `justhodl-trade-evaluator` research.

- Before this fix, `aws/lambdas/justhodl-trade-journal/source/lambda_function.py`
  lines 497–504 returned the complete personal ledger and stats on GET, before the
  write-only authorization at line 507. Its schema contains manual entry dates,
  prices, dollar size, shares, thesis text and current/realized P&L.
- `trade-journal.html` lines 226/260 called the Function URL directly without GET
  authentication. The security owner handles its authenticated Worker/page route.
- The producer wrote `data/user-trades.json` and `data/user-trades-stats.json` with
  `max-age=60`; these keys were outside the previous private map. Exact-key search
  found no other Lambda consumers of either artifact.
- The producer now guards the outer HTTP envelope before scheduled flags/body
  parsing or account access. Authenticated service calls support existing CRUD;
  valid legacy administrative callers and trusted IAM scheduled invocations remain
  supported. Responses and canonical objects use `private, no-store`; both full
  artifacts require authenticated private publication. Six actual-handler tests
  cover anonymous GET/schedule spoof, owner reads, CRUD, old admin/scheduled behavior,
  publication failure and honest absent-account seeds.

The following matched consumers did not establish additional laundering:

| Consumer | Traced reason |
|---|---|
| signal-board / conviction | `n_pm_decision` copies only normalized `posture_word`; PM computes that field from public master-ranker/crisis/capitulation context before reading owner position metrics. It does not export PM holdings, actions or account risk. |
| alpha-compass / master-ranker | Exports structured note counts/stance/score/timestamps already explicitly allowed by `public_brain_projection.notes_public`; no note prose was found in these direct joins. |
| llm-health | Emits health booleans for Brain output fields, not their contents. |
| morning-intelligence | Brain prompt context is delivered to the configured owner Telegram. Its S3 run log contains system-run counts and public market-regime metrics, not the personal brief or Brain prose. |
| history-snapshotter | Historical AI-brief content stays in IAM DynamoDB or matching private `history/archive/feed/...` prefixes. Public index/status outputs contain metadata, not historical bodies; the history API is separately guarded. |
| sizing-engine | Current source uses the reviewed public sanitizer to remove actual holdings and account ticker references; new public modeled-book work remains a distinct risk-agent responsibility. |

Existing covered producers, public projections, authenticated private mirrors and
private archive prefixes were not reported again as new findings. Structured public
market research was not reclassified merely because it uses the word “book.”
This is a bounded direct-source closure check, not a claim that every possible
computed/runtime path or live endpoint was tested. The security migration must still
deploy the private keys/Worker routes, protect historical aliases, and verify live
anonymous denial and authenticated owner access.

The subsequent API route review found a separate caller-input disclosure: Wealth
Plan and Tax Plan copied every HTTP calculation, including its submitted financial
profile, into their shared public snapshot. This does not arise from a private
portfolio donor: Tax Plan's position source is the modeled signal portfolio.

Both calculator handlers now return HTTP scenarios with `private, no-store` and
never persist them. HTTP transport takes precedence over spoofed scheduled/body
flags. OPTIONS returns before model reads; GET and JSON POST remain supported.
Input/result printing is removed. A trusted non-HTTP invocation ignores supplied
scenario parameters, computes the existing default model, and publishes only a
snapshot labeled `publication.schema_version=public-default-scenario.v1`,
`scope=PUBLIC_DEFAULT_MODEL`, `contains_caller_inputs=false`. Existing calculation
fields and default numeric behavior are preserved. Each producer's actual handler
runner covers GET, POST, scheduled spoof, v1 HTTP, preflight, default publication,
and HTTP-default nonpublication with in-memory S3. Wealth Monte Carlo executes
its real arithmetic with a small test sample; these tests certify publication
isolation, not financial model calibration.

Historical/current unsafe snapshots still require the coordinated deployment
migration. The security release owns replacement of unmarked snapshots with a
whole-document unavailable marker, historical access denial, cache handling, and
browser POST transport. Source changes alone do not prove those live controls.
