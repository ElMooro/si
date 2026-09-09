# Treasury auction output ownership

Source review, 2026-09-09. No cloud reads, notifications, production invokes, or deployment are part of this evidence.

`053c578` removes the unused copied detector handler and its I/O dependencies from auction-desk's `crisis_scoring.py`. The configured desk handler only called metric extraction and scoring from that module. The copied writer made source ownership ambiguous and created unnecessary import-time storage/credential dependencies; there is no evidence here that the configured desk handler actually executed the copied writer in production. Pure scoring parity is retained. Separate engine identities are now present in the detector and desk outputs.

| Producer | Owned output | Relationship |
|---|---|---|
| `justhodl-auction-crisis-detector` | `data/auction-crisis.json`; `data/archive/auction-crisis/YYYYMMDD.json` | Live numerical detector and its daily snapshot. The handler also reads its previous current snapshot for auction change detection. |
| `justhodl-auction-desk` | `data/auction-desk.json`; dedicated `data/warm/treasury-auctions/` history, daily, reaction and composite keys | Builds auction/buyback grades and historical reactions; reads the detector's current composite to append a live point. Its scoring module has no handler, imports or I/O. |
| `justhodl-auction-crisis-ai` | `data/auction-crisis-ai.json`; `data/archive/auction-crisis-ai/`; alert-state key | Reads the detector, publishes a separate narrative. `INPUT_KEY` is not a writer declaration. |
| `justhodl-tenor-signal-interpreter` | `data/auction-tenor-signals.json` | Computes its own signals from provider records. Its unused `AUCTION_CRISIS_KEY` constant is not a runtime read or write. |
| `justhodl-auction-grader` | `data/auction-grades.json` | Reads detector `recent_auctions` and keeps grades separate. |

No consumer was routed to an inferred replacement key. Existing auction-crisis consumers require the detector schema. The obsolete artifact-producer catalog's AI/tenor writer labels and the conservative scanner's copied-desk writer claim must be replaced with current source evidence, not learned from whichever stale payload was cached last. The public archive index previously excluded this ambiguous family; publishing new archive coverage requires its own reviewed index/configuration update and deployed-producer verification.

## Consumer audit

The source search covered all Lambda Python modules, page HTML/JavaScript and recursive dynamic brief-context configuration. Source references below describe the intended schema consumption; they are not proof that every consumer was refreshed or successfully ran in production.

| Consumer (`justhodl-` prefix for engines) | Actual detector data consumed | Outcome |
|---|---|---|
| `ai-brief.compress_auction` | `composite_score`, `regime`, `interpretation` | Retains zero and maps the detector explanation correctly. |
| `ai-chat.build_context` | `composite_score`, `regime` | Retains zero in auction context; existing private-source boundaries preserved. |
| `ai-website-synthesis.ENGINE_INPUTS` | Regime, score, interpretation, auction count, tail risk, tenor decomposition, triggers | Matching numerical schema; its AI narrative is a separate input. |
| `alert-router.check_auction_crisis` | Score, regime, interpretation | Correct explanation field and zero-safe score extraction. |
| `allocator.rule_auction_crisis` | Score | Canonical score field; allocation thresholds unchanged. |
| `auction-crisis-ai.lambda_handler` | Rich v2 report and source timestamp | Actual handler fixture confirms separate output/archive writes. |
| `auction-desk.lambda_handler` | Current score and 14-day auction count | Actual handler fixture confirms base remains unchanged and only desk keys are written. |
| `auction-grader.lambda_handler` | Recent metrics, source timestamp, regime and score | Actual handler consumes detector-produced fixture and writes one grade artifact. |
| `auction-interpreter` | Recent auction metrics; fired aggregates; forward calendar | Replaced absent `indicators`, `upcoming_auctions`, `bid_to_cover`, `tail_bps` input fields with the actual v2 names. The high-minus-median tail remains explicitly identified as a proxy. |
| `bond-desk.lambda_handler` | Regime | Matching field. |
| `chart-data.fetch_internal` | `composite_history.series[].date/composite`; current score fallback | Replaced nonexistent `history/crisis_score`; current fallback uses the producer observation timestamp, not an invented current date. |
| `confluence-meta.lambda_handler` | HEAD-only ETag, last-modified and byte count | Metadata provenance ledger; does not consume the numerical body. |
| `crisis-canaries.lambda_handler` | Score for its own time-series/slope history | Actual read-block regression preserves zero. |
| `kb-matcher.build_today_state` | Score | Retains zero. |
| `liquidity-inflection.treasury_auction_signal` | Score/regime, tenor highlights, fired tail aggregate, issuance, curve slope, forward bill calendar | Matching nested schema and explicit gross-issuance interpretation. |
| `morning-intelligence` | Score and regime | Retains zero. |
| `page-ai-commentary` | Auction page input body | Existing registry references detector key; no key change. |
| `regime-conditional-router.detect_treasury_auction_crisis` | Score and regime | Replaced nonexistent `score/crisis_score/state`; an actual 80/ACUTE_STRESS fixture now reaches the router instead of zero. Missing input remains absent in evidence. |
| `signal-board.n_auction_crisis` | Score/regime, auction count and escalation probability | Matching nested schema; model probability calibration is outside this ownership fix. |
| `smart-wake._stress` | Score | Existing numeric check already retains zero. |
| `wave-signal-logger.log_auction_crisis` | Score and regime | Existing direct read already retains zero. |
| `ai-brief-router` dynamic contexts | Five cross-regime inputs; one full primary input; two sniffer inputs | The eight contexts are yield-curve-, bonds-, liquidity-, macro-data-, conviction- and auction-decisive-call, frontrun-sniffer, and macro-frontrun-sniffer. Their configured paths remain the detector key. |

`auction-crisis.js`, `auctions.html`, `bonds.html`, `news.html`, `risk-regime.html`, and the Treasury desk's explicit wire consume the detector schema. The dedicated auction page separately reads the AI narrative key. Bonds now rejects missing/nonfinite scores and unknown regimes instead of inventing CALM/0, and safely renders provider text. News uses the canonical score/explanation. Other detector page bindings remain unchanged. A link or feed label alone is not counted as a body consumer.

## Offline evidence and limits

`tests/deployment/test_auction_output_ownership.py`: ten tests pass, including real detector, desk, AI, tenor and grader handlers with mocked external providers/storage; scoring parity; chart and regime-router readers; interpreter prompt mapping; and zero-score preservation. `tests/auction-summary.test.js`: two tests pass against the actual Bonds rendering function, covering genuine zero, incomplete data and hostile provider strings.

This change establishes source ownership and concrete schema compatibility. It does not validate crisis thresholds, the fixed historical scoring baselines, model-generated probabilities, executable when-issued quotes, production schedule cadence, or historical object provenance. The workflow must deploy changed producers/consumers and verify their exact artifacts before claiming production closure.
