# Unified research network

The network connects existing engine publications to one evidence dossier and
16 deterministic specialist reviews. It enriches 13 existing consumers without
changing their investment decisions, risk holds, scores or schedules. The public
dossier is available from Command Desk and `dossier.html#research-network`.

## Integration map

| Group | Sources and connection | Result |
|---|---|---|
| Economy and industry | Macro Nowcast, Nowcast Desk, Global Business Cycle, Physical Economy, PortWatch, Industry Rotation, Earnings Confluence | Common dated macro context for industry and earnings research |
| Liquidity and funding | Global Liquidity, Liquidity Credit, USD Funding, Eurodollar Plumbing, Credit Composite → Engine Fusion / Khalid Risk | Attributed funding and credit context alongside authoritative risk policy |
| Treasury and collateral | Treasury, auctions, settlement fails, rehypothecation, Bond Warroom, Dollar Radar, FX Intelligence | Observed conditions kept separate from transmission hypotheses |
| Ownership and flows | ETF True Flows, Global Flow Desk, Flow Lookthrough, Holdings Research, 13F Positions, Insider Radar, Buybacks | Reporting periods and complete originals; unresolved holdings joins stay explicit |
| Fundamentals and accounting | Census, Fundamentals, Stock Valuations, revisions, Earnings Quality, Forensic Screen, Beneish | Valuation and accounting evidence in the same dossier |
| Earnings and catalysts | Transcripts, NLP, Earnings Confluence, PEAD, Catalyst Calendar, SEC Filings | Available event evidence and reported clocks; unavailable transcripts remain withheld |
| Technical conditions | Bottom, Katlin, Phase Detector, Accumulation Radar, Tape Reader, Tape Truth, Volatility Squeeze → Khalid | Setup observations kept separate from qualified entries |
| Options and positioning | Options Analytics / Confluence, Dealer GEX, Short Interest, catalysts, Liquidity Capacity | Attributed positioning and liquidity evidence |
| Industry and supply chain | Forward Orders, Supply Chain Graph, Industry Rotation, Theme Cascade, Patent Velocity | Native relationships and company observations retained for inspection |
| Crypto and dollar liquidity | Crypto Confluence, Funding, Basis, Stablecoin Flow / Peg, Exchange / ETF Flows, Dollar Radar, USD Funding | Crypto namespace separated from listed securities; no inferred symbol aliases |
| Idea synthesis | Best Ideas, Master Ranker, Equity / Flow / Options Confluence, Khalid, Katlin, native Ticker 360 | One candidate dossier with original source pointers |
| Thesis challenge | Engine Conflicts, public Devil's Advocate, Narrative vs Tape, Kill Theses | Reported opposing directions, horizons, missing information and invalidation evidence |
| Portfolio context | Factor Risk, correlation, stress and liquidity → private Portfolio Risk | Dossiers bound to the exact authenticated holdings input |
| Evaluation and learning | Genealogy, prospective outcomes, Signal Scorecard, Engine Trust, orthogonality, Alpha Decay → JH Fusion | Registration-time evidence retained separately from outcome grades and promotion |
| Data and delivery health | Contract Violations, Freshness Monitor, Event Flow Health | Missing, stale, future, invalid and unavailable sources remain distinct |
| Unified briefing | Network, Engine Fusion, JH Fusion, Khalid Risk, Alpha Council, Financial Secretary | Shared review coverage and publication receipts on Command Desk / dossier |

The authoritative registry is `aws/shared/research_network_registry.py`: 88
public input paths, explicit native row adapters and 13 consumer subscriptions.
The two briefing outputs are also read back to observe their processing receipts.
Specialists are deterministic evidence reviewers, not additional LLM calls.

## Publication and identity contract

Ticker 360 retains its legacy output and compiles `research-network.v1` on its
existing schedule. Each complete permitted source is retained at a raw-content
SHA-256 address as gzip. Exact JSON pointers bind projected observations to the
original. Omitted nested fields are enumerated and the complete packet remains
inspectable. A malformed declared row container withholds that source's entire
row projection; it cannot masquerade as complete coverage.

Each entity has a source-scoped reported-symbol identity. Crypto and listed
securities cannot merge. Mixed sources require an explicit asset class or stay
in an unresolved namespace. These are research groupings, **not qualified
instrument-master joins**. Share classes and spellings are not silently aliased.
Publication time, reported observation clocks, reporting periods and horizons
remain separate. An unknown observation time is never replaced by a fresh file
timestamp. Independent investment votes remain zero until ancestry and identity
can actually be qualified.

Immutable shards and a manifest are written before a conditional current-pointer
update. Failed shards, timeout or a stale concurrent writer leave the previous
complete pointer intact. Manifest identity includes source/registry/compiler
content and the prior publication. Readers verify shard key, size, hash and
publication identity. Raw-content identity tolerates gzip envelope differences
between Python versions while rejecting changed decompressed bytes.

Each dossier answers the nine audit questions through attributed observations,
supporting/opposing reported directions, missing angles, horizons, invalidations,
private portfolio availability and changes. Change tracking distinguishes a new
source publication from changed observation content. Missing relationships,
invalidation rules and source independence are displayed as unresolved, never
invented from scores or bullish prose. Full relational models and causal
transmission estimates are not manufactured by the composition layer.

## Consumers, privacy and learning

Adapters attach a `research_network` field to existing publications after their
normal calculations. The field carries the exact publication hash, immutable
manifest reference, read time, subscribed specialist reviews and source context.
Direct self-sources (including the prospective evaluator's output alias) are
excluded. Shared upstream ancestry is still unqualified. Missing or stale
network context is explicit and cannot take down the existing decision engine.

The next network publication observes each public consumer's receipt. A receipt
with unavailable status cannot claim it processed the previous publication.
EventBridge acceptance, source-file freshness and actual consumer processing are
different facts. Existing schedules govern convergence; this is not immediate
event-driven recomputation and introduces no recursive invocation loop.

Private holdings are read only through the existing Portfolio Risk invocation.
Research is joined to explicitly identified positions and the exact holdings
input hash inside the existing authenticated output. Public compilation has no
portfolio read or return route. Existing concentration, stress, hedge and sizing
calculations remain authoritative; research adds context rather than inventing
new approved portfolio calculations.

New prospective forecasts receive immutable research-context sidecars only if
the network actually existed in S3 before registration. Both S3 LastModified and
the publication clock are checked. Old forecasts are never backfilled. Failed
sidecar writes stay visible rather than retrying with later information. Existing
forecast schemas, grades, outcome eligibility and promotion policy are preserved.

## Event and producer repairs

`system_events.publish_many` attempts every event in batches of at most ten and
returns per-input acknowledgement results. Partial or missing acknowledgements
are failures. Master Ranker stores its checkpoint and pending outbox together
before emission, then conditionally removes only acknowledged events. Retries
keep stable event IDs; concurrent stale writers fail. Suppressed runs cannot
advance the delivery checkpoint. Delivery is at least once, not exactly once;
consumer effects must tolerate duplicate deliveries.

Firm Book now reads nested pair-leg identities rather than converting whole
objects to strings. This prevents malformed symbols propagating into factor and
liquidity outputs. Previously captured malformed rows remain rejected; history
is not rewritten.

## Verification and release acceptance

Offline verification uses complete hashed native captures with
`scripts/replay_research_network.py`, fake S3 with conditional writes, adversarial
Python integration tests, the full frontend suite and actual Edge desktop/mobile
browser replay. Browser tests block all external calls and cover missing and
tampered publications, keyboard controls, refresh, overflow and private-path
exclusion. Python 3.12 matches deployed Lambda runtime and historical AST gates.
Historical source-preservation tests reverse the exact checked transition before
applying their unchanged predecessor hashes; no prior gate is discarded.

The replay explicitly refuses to count a captured allowed source that failed
ingestion as success. Public earnings-transcript access was unavailable, so its
adapter remains disabled. No alternate credential/path is used to fetch it.
13F aggregate joins, missing periods and malformed legacy position identities
remain explicit limitations even when their complete originals are available.

Release through the configured review branch. On approved merge, deploy every
direct and transitive importer of changed shared modules using the normal
workflow. Verify exact commit/source receipts for all affected functions, normal
schedule bindings, the commit-bound Pages artifact and natural publications.
Then verify one new network publication, subsequent consumer receipts, private
portfolio context through its authenticated workflow, and a newly registered
prospective sidecar. An offline pass does not certify live IAM, runtime capacity,
private account behavior or natural scheduled delivery.

The compiler adds S3 reads and immutable original/shard storage but no provider
subscriptions or LLM calls. Retained history grows with source changes and
publications; retention policy is not silently changed. Runtime/memory and
storage growth must be observed during release acceptance before broadening
cadence or retention. Existing schedules and Lambda configuration are unchanged.
