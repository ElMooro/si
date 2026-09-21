# Provider fund-flow research

The provider-flow producer retains all 300 configured fund identities. Its
canonical packet is `data/provider-fund-flow-research.json`; Capital Flow Radar
is a replayed projection at `data/capital-flow-radar.json`. The provider is ETF
Global through the existing Massive/Polygon subscription. No new subscription,
paid AI model, notification, account read or portfolio write is used.

## Measurement contract

- `effective_date` dates the observation; `processed_date` identifies the acquired
  provider version. Neither is replaced by the other or by the Lambda clock.
- Every acquired successful response is retained whole in the protected original
  store. Normalized rows identify the response hash, page and original row index.
  Provider error bodies and credentials are not retained or published.
- A latest processed revision is selected per effective date. Conflicting rows
  for the same version invalidate the fund's arithmetic rather than falling back
  to an older number. Null flow remains unknown; reported zero remains zero.
- The reporting grid consists of observed SPY effective dates. It is explicitly
  **not a verified exchange calendar**. Exact listed dates are required for 1, 5
  and 21-observation windows, both at the common end and each fund's latest end.
- Provider-reported creation/redemption flow is distinct from turnover,
  underlying stock purchases, investor identity and price impact. Separate prior
  NAV and current NAV share-change calculations expose reconciliation residuals;
  neither replaces reported flow or establishes a universal provider formula.
- Group totals require every configured member on the same dates. Partial subset
  totals and excluded funds are separate fields. Deduplicated universe totals
  cannot be reproduced by adding overlapping displayed groups. Bull plus inverse
  fund flow and bull minus inverse comparison have distinct meanings; neither is
  underlying leveraged exposure.
- Acquisition freshness and latest observation age both constrain availability.
  Recompiling the same evidence does not renew its source lifetime. Historical
  first-publication times are not claimed by a newly acquired historical response.

## Reproduction and publication

Original bytes, normalized histories, compiler bytes, inputs, output and a run
manifest are retained by content hash. Publication follows complete replay.
The runner can use `scripts/replay_provider_flow_research.py` to reconstruct
the public packet; a public hash alone is not the original-source replay proof.
The exact reviewed compiler version is required. The first migration preserves
the complete previous packets, including the large constituent-pressure payload,
the latest dated flow history and both legacy path variants when they exist.

Scheduled runs claim a durable request before collection. Retries of that request
return the retained status without repeating collection. Conditional writes reject
older publications and observation regression. All 14 legacy compatibility paths
point to the canonical packet; unsupported old score containers are empty and
explicitly superseded. If an alias write fails, the request records failure and
the previous bytes remain protected; a green workflow is not acceptance proof.

## Consumer and portfolio limits

The canonical packet and Radar have zero independent investment votes, null calls,
WAIT/abstention and all forecast, sizing and execution permissions false. Named
legacy readers exclude direct provider flow, inferred stock pressure, look-through
and stealth contributions. Existing stored confluence rows containing these
unqualified components are also rejected. Other input families are not certified
by this boundary. The old handlers are preserved unimported.

ETF census keeps the entire configured universe. The holdings collector retains
membership but emits null unsupported stock-flow and quadrant fields. Its complete
original-response holdings replay is a subsequent stage. The legacy flow-AI path
returns an explicit research-only status before any paid model or notification.

Both pages verify retained output/run bytes and selected fund-history bytes. Their
portfolio calculator accepts the user's signed USD exposure, assumed NAV shock
and total costs: gross change = exposure × shock / 100; net change = gross − costs.
It is a constant-unit, linear mark-to-NAV scenario. It does not estimate a fund's
leveraged path, executable market price, distributions, financing, borrow, taxes,
liquidity or a historical total return. Inputs remain in the browser and changing
an input or source invalidates the displayed result.

## Acceptance

The native source candidate is not live proof. Acceptance requires exact commit
receipts, actual deployed ZIP/import closure, retained original-source replay,
independent source-row arithmetic, protected-original denial, correct public
JSON/HTML and browser checks. The intended proof key is
`data/provider-flow-research-verification.json`. Only the two reviewed public
research producers are invoked for acceptance; decision consumers are not invoked.
