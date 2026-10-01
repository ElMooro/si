# Khalid technical-only pilot — retrospective evidence, not strategy validation

The full requested strategy is **untestable with the currently inspected
point-in-time inputs**. A small price/volume subset can be reproduced locally from
retained public warehouse files. No production engine, policy or page changed.

PR19 remains a separate offline validator review. Its reviewed documentation
correction is at `a30668c32c272d2957e5005f493b366ddf684f36`: capitulation close
location is `(close-low)/(high-low) >= 0.25`, with a nonzero range. It is not a
25% price rise above the low.

## Scope frozen before the outcome run

The separately committed `khalid-technical-pilot-spec.json` freezes a convenience
sample (BTC, ETH, AAPL; SPY audit-only), September 5, 2020–September 25, 2026,
504-bar warm-up and five existing browser gates in order: 50% drawdown, RSI reset,
Bollinger compression, range compression, capitulation. No threshold was relaxed
after inspecting returns. The exact browser scorer is reused offline and checked
by SHA-256, not reimplemented. It receives only trailing prefixes and retains its
existing 560-bar truncation/Wilder RSI behavior.

This omits unresolved '3m' support, SPX decline/resilience, verified ETF inflows,
point-in-time biotech/smallcap exclusions, holdings/classification history and
other browser/backend requirements. Results cannot be attributed to the full
Khalid strategy. Horizons below are **crypto calendar days**, not equity sessions.

## Feasibility and quality

Only static, anonymous `justhodl.ai` warehouse objects were read. No vendor API,
AWS invocation, private source, proxy chart route, acquisition or schedule was
used. Chart routes were avoided because their fallback may fetch provider data.

- BTC and ETH each supply 2,212 consecutive dates in the frozen window with
  finite, internally consistent OHLCV and no duplicate dates. Provider accuracy,
  historical first availability and venue completeness are not certified.
- AAPL has **18 conflicting same-date OHLCV records** in the window. The whole
  asset is excluded under the frozen rule; no preferred record was chosen.
- SPY supplies 1,520 observed bars in the window. It is audit-only: session
  calendars, historical adjustment basis and crypto/equity close alignment have
  not been certified. No SPY excess return or resilience claim is calculated.
- Per-symbol US-equity banks and several ETF alternatives returned HTTP 403.
  That does not establish missing data. The full-market manifest reports 1,279
  sessions / 1.47 GB; no full-market download was attempted for this bounded pilot.
- The inspected flow artifact reports historical first availability unverified.
  Retained holdings snapshots do not establish historical index membership or
  certified point-in-time sector/capitalization filters.

Retained input files plus `manifest.json` are in the execution workspace at
`/workspace/scratch/khalid-technical-pilot/` (about 2.3 MB decoded). The checked-in
result lists exact public paths, acquisition clocks and content hashes. Provider
bodies are not republished in the repository. Public paths are mutable: exact
replay requires preserving the retained bodies, not simply downloading them later.

## Gate funnel

| Asset | After warm-up | Drawdown | RSI | Bollinger | Tight range | Capitulation |
| --- | ---: | ---: | ---: | ---: | ---: | ---: |
| BTC | 1,709 | 367 | 160 | 36 | 15 | 13 |
| ETH | 1,709 | 675 | 300 | 56 | 25 | 10 |

Rows are nested, repeated asset-date observations, not independent trades.

## Gross observations; no alpha or profitability inference

| Asset | Native-day horizon | Mature dates | Right-censored | Overlap clusters | Mean / median gross price change |
| --- | ---: | ---: | ---: | ---: | --- |
| BTC | 21 | 13 | 0 | 3 | +24.83% / +26.57% |
| BTC | 63 | 8 | 5 | 2 | +33.06% / +36.87% |
| BTC | 126 | 8 | 5 | 1 | +61.28% / +72.66% |
| ETH | 21 | 10 | 0 | 2 | +11.91% / +11.03% |
| ETH | 63 | 10 | 0 | 2 | +38.23% / +39.22% |
| ETH | 126 | 10 | 0 | 2 | +67.83% / +59.62% |

The small cluster counts prevent treating these repeated dates as strong
independent evidence. No confidence interval or significance claim is supplied.
The result JSON retains every event date, endpoint, return, min/max and exclusion
count. These are retrospective close-to-close price changes with no benchmark,
commissions, spread, slippage, impact, funding, borrow, dividend or executable-fill
model. A selected convenience sample with omitted gates is not evidence of alpha.

No fitting or chronological holdout was performed. PR19's validator excludes all
mature candidate labels because historical decision/feature/label availability
is absent. Its fold field is used only for an eligibility diagnostic at the
specification freeze instant, explicitly not a historical train/test split.

## Reproduce and verify

```sh
PYTHONDONTWRITEBYTECODE=1 python scripts/research/khalid_technical_pilot.py /workspace/scratch/khalid-technical-pilot > /tmp/replay.json
cmp /tmp/replay.json docs/research/khalid-technical-pilot-result.json
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p 'test_khalid_technical_pilot.py' -v
PYTHONDONTWRITEBYTECODE=1 python -m unittest discover -s tests -p 'test_katlin_label_boundary.py' -v
```

Seven pilot tests cover actual-scorer future-price mutation, exact outcome
arithmetic, horizon censoring, availability rejection, duplicate conflicts,
calendar gaps, invalid OHLCV, overlap counts and input/scorer hash refusal.
Eight PR19 boundary tests remain passing. Python preflight and staged inventory
checks apply before commit. Independent review remains required.

## Current production consumer trace — unchanged

`justhodl-katlin/source/lambda_function.py`: `run_backtest` writes
`data/katlin-backtest.json`; feed loader line 1679 reads it; `learned_prior`
line 3535 and handler line 3754 derive expected excess/learned pillars;
`gates_and_tier` line 2759 uses history thresholds; `build_basket` line 3456 uses
expected excess in model allocation. `validation_summary`, `regime_history` and
`barbell_base_rate` expose diagnostics. Khalid's `_katlin_rows` at
`aws/lambdas/justhodl-khalid/source/lambda_function.py:516` adapts picks/watch.

The offline scripts have no production import or artifact publisher. Production
leakage correction still awaits the separate specific approval. Flow semantics,
holdings coverage/UI and a future `khalid.html` evidence panel remain separate work.
