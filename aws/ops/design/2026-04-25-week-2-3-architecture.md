# JustHodl.AI — Week 2-3 Architecture Design Document

**Author:** Claude (overnight session 2026-04-24/25)
**Status:** DRAFT — for Khalid's review before any code is written
**Scope:** Move the system from "data fetched + signals logged + outcomes scored"
to "formal predictions + backtesting + ranked opportunities + sized positions"

This document does NOT contain code. It contains design decisions that need
human review. Each section presents options with tradeoffs. Default
recommendations are clearly marked but should not be treated as decisions.

---

## Where we are now (state of 2026-04-24 night)

The Week 1 fixes shipped tonight closed the most fundamental gap: **the
learning loop is no longer silently dead**. From this moment forward:

1. Every newly-logged signal records `baseline_price` and (where applicable)
   `baseline_benchmark_price` at log-time
2. Outcome-checker fires daily (weekday 22:30 UTC), monthly (1st 8:00 UTC),
   and weekly (Sun 8:00 UTC)
3. Calibrator runs Sunday 9:00 UTC and computes per-signal accuracy weights
4. Coverage expanded from 10 to 24 signal types in the calibration weights

But "predictions" right now means: a directional label (UP/DOWN/NEUTRAL) on
a signal value, with a baseline price stored at log-time, evaluated against
a single threshold (±0.5%). That's binary scoring. It's not yet a
prediction in the formal sense that a hedge fund would use.

The four open problems Week 2-3 should solve:

- **No formal prediction schema** — current "predictions" don't have target
  prices, expected magnitudes, confidence intervals, entry/exit conditions,
  or stop levels.
- **No backtesting** — every signal is being evaluated forward-only.
  We have no way to know if a new signal type would have been profitable
  historically before deploying it.
- **No unified ranker** — the system has separate outputs from screener,
  valuations, investor agents, edge-engine, and crypto-intel, but nothing
  that combines them into a single "today's best opportunities" list.
- **Constant weights, not learned** — calibrator computes accuracy but the
  Khalid Index uses hardcoded blend weights (75% core + 15% CFTC + 10% smart
  money). Position sizing isn't a function of signal accuracy.

Each section below proposes a concrete schema or design with options.

---

## 1. Predictions Schema

A "prediction" is a forward-looking statement attached to a signal. Every
signal already produces something — at minimum a directional label. The
question is **how rich should the prediction structure be, and how much of
that richness is required vs optional?**

### Required fields (proposal)

These are needed to backtest, score, and size positions:

```python
{
    # Identity
    "signal_id":             str,    # UUID, links to justhodl-signals
    "prediction_id":         str,    # UUID, this prediction's own ID
    "signal_type":           str,    # khalid_index, screener_top_pick, etc.

    # Target
    "ticker":                str,    # SPY, BTC-USD, NVDA, etc. (asset acted on)
    "benchmark":             str|None,  # SPY for relative predictions, None for absolute

    # The prediction itself
    "horizon_days":          int,    # 1, 3, 7, 14, 30, 60, 90 — REQUIRED
    "predicted_direction":   str,    # UP / DOWN / NEUTRAL / OUTPERFORM / UNDERPERFORM
    "confidence":            float,  # 0.0 to 1.0
    "logged_price":          float,  # baseline price at prediction time
    "logged_at":             str,    # ISO timestamp
}
```

### Optional fields (proposal)

These enable richer scoring but aren't required for the loop to work:

```python
{
    # Magnitude (lets us score "predicted +5%, got +3%" as partial credit)
    "predicted_magnitude_pct": float|None,    # e.g. 2.5 means "+2.5%"
    "predicted_magnitude_band": str|None,     # WEAK | MODERATE | STRONG

    # Targets (lets us track whether price hit a specific level)
    "target_price":          float|None,      # absolute price target
    "stop_loss_price":       float|None,      # downside cutoff

    # Risk framing (lets us compute Sharpe-like metrics)
    "expected_volatility_pct": float|None,    # implied or historical
    "regime_tag":            str|None,        # BULL/BEAR/CRISIS at log-time
    "khalid_score_at_log":   int|None,        # snapshot of regime indicator

    # Reasoning (for human/AI review)
    "rationale":             str|None,        # why this prediction was made
    "supporting_signals":    list|None,       # related signal_ids that agree
}
```

### Design questions for Khalid

#### Q1.1 — Should `predicted_magnitude_pct` be required or optional?

| Option | Pros | Cons |
|---|---|---|
| **A: Required** | Enables Sharpe/return-based scoring. Makes signals more rigorous. | Some signals are inherently directional (regime), forcing a magnitude is artificial. |
| **B: Optional with default** | Backward-compatible, easy to migrate. | Inconsistent scoring across signals. |
| **C: Required with NULL allowed** | Clean schema, explicit. | Same as B in practice. |

**My recommendation:** B — start optional, observe which signals naturally
produce magnitudes, then move to required for those over time.

#### Q1.2 — Should we use absolute (`target_price=$720`) or relative (`+1.5%`) targets?

| Option | Pros | Cons |
|---|---|---|
| **A: Absolute price** | Easier to backtest exactly. Matches how traders think. | Stale fast — a $720 SPY target is nonsense in 6 months. |
| **B: Relative %** | Time-invariant. Easy to aggregate across tickers. | Less concrete, harder to read. |
| **C: Both — store as relative %, compute absolute at log-time and store both** | Flexible. | Slightly more storage. |

**My recommendation:** C. Trivially small storage cost, both views are useful.

#### Q1.3 — How should `confidence` be defined?

This is the most consequential design choice. Three meaningful interpretations:

| Option | Definition | Calibration |
|---|---|---|
| **A: Raw conviction** | "How sure am I, 0-1?" Subjective. | Calibrator measures realized accuracy and adjusts. |
| **B: Probability** | "P(this prediction is correct)". Should match realized hit rate. | Reliability diagram — calibrator should produce a curve mapping stated→realized. |
| **C: Sample size** | "How many comparable cases support this?" | Wider intervals = lower confidence. |

**My recommendation:** B (probability), with the calibrator producing
**reliability curves** (e.g., predictions stamped 0.7 confidence should
be correct 70% of the time after calibration). This is what hedge funds use
and it's directly comparable across signal types.

#### Q1.4 — Where does the prediction live?

| Option | Storage | Tradeoffs |
|---|---|---|
| **A: New DynamoDB table `justhodl-predictions`** | Separate from signals, one prediction per signal could expand to many predictions per signal | Most flexible, requires new IAM perms + schema work |
| **B: Embedded in `justhodl-signals` (current pattern)** | Add `predictions: []` field to existing signal record | Cheaper, simpler, but signals become heavier |
| **C: Both — emit to predictions table for queryability, mirror in signal record** | Query both ways | Storage cost ~2x |

**My recommendation:** B for v1 (signal-as-prediction with richer fields),
migrate to A only if we hit signals being too large for DynamoDB's 400KB
item limit, which we won't anytime soon.

---

## 2. Backtesting Framework

Backtesting answers: "If we'd been running this signal logic 90/180/365 days
ago, what would the P&L have been?" Without it, **every new signal is
deployed live without prior validation**.

### The minimum viable backtester

A new Lambda `justhodl-backtester` that:

1. **Loads historical FRED + Polygon data** (we already have it — daily-report
   fetches it on every run)
2. **Replays the signal-logger logic** against historical S3 snapshots
3. **For each replayed signal, fetches the actual price N days later**
4. **Scores using the same `score_directional()` / `score_relative()` logic**
5. **Aggregates: total predictions, hit rate, avg return, Sharpe-like ratio**
6. **Writes report to** `s3://justhodl-dashboard-live/backtests/<run_id>.json`

### Three implementation paths

#### Path A — Snapshot-based (easiest, least accurate)

Use `justhodl-dashboard-live`'s S3 versioned history (if enabled) or daily
backups to replay signal-logger logic against historical snapshots of
`data/report.json`, `crypto-intel.json`, etc.

- **Pros:** Reuses existing data; can run today
- **Cons:** Only works for as far back as snapshots exist. We don't have
  versioning enabled yet, so today this means "0 history".
- **Setup cost:** Enable S3 versioning + wait 30-90 days for history to
  accumulate

#### Path B — Reconstruct-from-source (medium effort)

Re-fetch historical FRED bars + Polygon historical aggregates for every
day in the backtest window. Re-run the same signal-logger logic on each
historical "day".

- **Pros:** Doesn't need snapshots; can backtest a year+ today
- **Cons:** API rate limits (FRED gives historical, Polygon free tier limited).
  Some data sources don't have historicals (CFTC reports come weekly).
- **Setup cost:** ~4-8 hours of writing the historical fetchers

#### Path C — Hybrid (best, most work)

For sources that have historical APIs (FRED, Polygon, CoinGecko), reconstruct.
For sources that don't (CFTC, some derived signals), use whatever snapshots
we have plus forward-only verification. Mark backtest results with
"reconstructed" vs "snapshot" provenance.

- **Pros:** Best fidelity; explicit about uncertainty
- **Cons:** Most code; requires careful provenance tracking

**My recommendation:** Path B for v1, then enable S3 versioning so v2 can
become Path C automatically.

### Design question

#### Q2.1 — Walk-forward or static holdout?

| Option | How it works | Pros | Cons |
|---|---|---|---|
| **A: Static holdout** | Train on Jan-Jun 2025, test on Jul-Dec 2025 | Simple, single number per signal | Doesn't catch regime drift |
| **B: Walk-forward** | Train on rolling 90-day window, test on next 30 days, advance, repeat | Catches drift, more realistic | More compute, harder to interpret |
| **C: Both, report separately** | — | Most info | Most work |

**My recommendation:** B. Walk-forward is the standard for time-series
ML evaluation and catches the failure mode where a signal worked in 2023
but stopped working in 2025.

---

## 3. Daily Ranker — "Today's Best Opportunities"

Right now the system produces:

- `screener/data.json` → 503 stocks scored on Piotroski/Altman
- `valuations-data.json` → 18 valuation metrics across asset classes
- `investor-agents` Lambda → 6-persona consensus on a single ticker
- `edge-data.json` → composite edge score, regime, correlation breakdowns
- `crypto-intel.json` → BTC/ETH technicals, on-chain ratios
- `flow-data.json` → options flow, fund flows, sentiment
- `data/report.json` → 188 stocks with 30+ metrics each

But nothing combines these into "Khalid, here are the 5 things to look at today."

### Proposed ranker output

```json
{
    "generated_at": "2026-04-25T13:00:00Z",
    "regime": "BEAR | NEUTRAL | BULL | CRISIS",
    "khalid_index": 43,
    "top_opportunities": [
        {
            "ticker": "NVDA",
            "asset_class": "stock",
            "conviction": 0.82,           // 0-1, computed from signal accuracies
            "horizon_days": 30,
            "direction": "UP",
            "predicted_magnitude_pct": 4.5,
            "supporting_signals": ["screener_top_pick", "edge_composite", "buffett_indicator"],
            "agreeing_signal_count": 3,
            "disagreeing_signal_count": 0,
            "rationale": "Top Piotroski score + edge regime expansion + valuation cheap. Investor panel: 4 BUY, 2 HOLD.",
            "risk_factors": ["VIX elevated above 25", "earnings in 12 days"]
        },
        ...up to 10
    ],
    "top_avoid": [
        // Same shape but for SELL/AVOID conviction
    ],
    "regime_summary": "Brief paragraph explaining current regime and how it shapes today's picks"
}
```

### Conviction formula (proposal)

For each ticker, gather all signals that mention it (directly or via class):

```
conviction = sum(signal_accuracy_weight × signal_confidence × directional_agreement)
           / sum(signal_accuracy_weight × signal_confidence)
```

Where `directional_agreement = +1` if the signal predicts UP, `-1` if DOWN,
`0` if NEUTRAL. Final score is in `[-1, +1]`. Map to 0-1 conviction by `(x+1)/2`.

The key dependency: `signal_accuracy_weight` comes from the calibrator's
`/justhodl/calibration/weights` SSM parameter. **This is why the calibrator
finally working is the prerequisite for a meaningful ranker.**

### Design questions

#### Q3.1 — How many tickers in the universe?

| Option | Universe | Cost |
|---|---|---|
| **A: 188 tracked stocks + 25 crypto** | What we have today | Minimal — already cached |
| **B: S&P 500 + Nasdaq 100 + 25 crypto (~600 names)** | Wider | ~5x Polygon calls |
| **C: 188 tracked + 503 screener stocks (deduped)** | What current system actually scores | Free (already done) |

**My recommendation:** C. The screener already analyzes 503 stocks. Use that
universe.

#### Q3.2 — How often does the ranker run?

| Option | Frequency | Use case |
|---|---|---|
| **A: Daily 8 AM ET** | Once per market open | Morning brief |
| **B: 4-hourly** | Multiple times | Catch midday signal changes |
| **C: On signal-logger fire (every 6h)** | Tied to data refresh | Most up-to-date |

**My recommendation:** A. The ranker is a digestible morning summary.
4-hourly creates noise; better to keep this stable.

---

## 4. Position Sizing from Learned Accuracy

Right now the Khalid Index uses **constant blend weights**:

```python
khalid_score = 0.75 * core_macro
             + 0.15 * cftc_crisis_score
             + 0.10 * smart_money_flow
```

These weights never change. The calibrator computes **per-signal accuracy
weights** but they only affect a separate calibrator-output dict — they
don't feed back into Khalid Index calculation.

### What hedge funds do

1. **Fractional Kelly:** position size = (edge × win_rate − loss_rate) / odds.
   Most use 0.25-0.5 Kelly to control drawdown.
2. **Volatility-scaled:** position size inversely proportional to recent
   volatility. ATR-based sizing.
3. **Correlation-adjusted:** if two signals agree on the same trade, don't
   double the size — account for correlation.

### Proposal: three-layer sizing

```
final_size = base_position
           × signal_accuracy_multiplier      # from calibrator
           × volatility_scale                # ATR-based
           × correlation_haircut             # if multi-signal agreement
           × regime_adjuster                 # crisis = 0.5x, bull = 1.0x
```

### Design questions

#### Q4.1 — Should sizing produce concrete dollar amounts or just weights?

| Option | Output | Use case |
|---|---|---|
| **A: Just weights (0-1 conviction)** | Like the ranker conviction | Display in UI; user sizes manually |
| **B: Position multiplier (0.5x, 1.0x, 2.0x base)** | Relative to a base allocation | Closer to actionable |
| **C: Dollar amounts (e.g., "$5K NVDA, $3K SPY")** | Concrete | Most useful, but requires portfolio total |

**My recommendation:** B. The system shouldn't presume to know your total
portfolio size, but giving a relative multiplier ("this is a 1.5x conviction
trade") is actionable.

#### Q4.2 — Should the calibrator weights flow back into Khalid Index?

This is the **biggest single decision**. Right now the Khalid Index is
hardcoded weights. If the calibrator finds that `cftc_gold` has 75%
accuracy while `cftc_crude` has 45%, should the Khalid Index automatically
weight gold more?

| Option | Behavior | Risk |
|---|---|---|
| **A: Yes, full automation** | Calibrator outputs become the live weights | Drift / overfitting risk; bad weights silently propagate |
| **B: Calibrator suggests, you approve** | Calibrator writes "proposed_weights.json", you click apply | Adds friction; relies on human-in-loop |
| **C: Both indexes coexist** | Khalid Index stays fixed, "Khalid Index Adaptive" uses learned weights | Most informative; can A/B compare |

**My recommendation:** C. Run both side-by-side. The **fixed** Khalid Index
becomes the benchmark; the **adaptive** one becomes the experiment. After
3-6 months, you have data to decide whether adaptive deserves to take over.

---

## 5. Dependencies and Sequencing

The four pieces above have a strong dependency order:

```
[Predictions Schema] ──► [Backtesting] ──► [Position Sizing]
        │                      │                  ▲
        │                      │                  │
        └──► [Daily Ranker] ◄──┘                  │
                  │                                │
                  └────────────────────────────────┘
```

Concretely:

1. **Predictions Schema must come first.** Backtesting and ranking both
   query against this schema. Without a stable schema, the others churn.
2. **Backtesting validates the schema.** As we backtest, we'll find fields
   missing or misnamed. Iterate the schema based on what backtesting needs.
3. **Daily Ranker needs accuracy weights from calibrator.** The calibrator
   already exists but needs ~1 month of real outcomes (now flowing) to
   produce trustworthy weights.
4. **Position Sizing needs both ranker conviction AND calibrator weights.**
   It's last because it consumes everything upstream.

### Recommended order of work

1. **Week 2A (3-4 hours):** Predictions schema migration. Add fields to
   `log_sig()` in signal-logger. Backfill optional fields as `null` for
   existing rows. No backwards-incompatible changes.

2. **Week 2B (4-6 hours):** Backtesting Lambda v1. Path B (reconstruct from
   FRED+Polygon historical bars). Aggregate report. No live deployment —
   just produces a JSON report we can read.

3. **Week 3A (3-4 hours):** Daily Ranker. Reads conviction + accuracy weights
   + ticker universe; produces daily JSON. Lambda runs at 8 AM ET. Adds a
   simple HTML page at justhodl.ai/today.html.

4. **Week 3B (2-3 hours):** Position sizing layer. Adds a multiplier field
   to ranker output. Documents the formula clearly.

**Total estimated work: 12-17 hours** spread across 2 weeks. None of this
is a 30-minute job. Each step needs careful design review before code.

---

## 6. What I am NOT proposing

To be clear, this is OUT OF SCOPE for Week 2-3:

- **Order execution.** No connection to Alpaca/Interactive Brokers/Robinhood.
  Sizing produces a recommendation; you place trades manually.
- **Real-time intraday signals.** Everything stays at minimum 1-day horizon.
  Intraday is a different system (separate latency/cost requirements).
- **Options/futures pricing models.** We use options flow as a sentiment
  input, not for pricing.
- **Custom ML model training.** The "ML predictions" Lambda exists but is
  using simple statistical models. Training neural networks on financial
  data is its own project (and famously low ROI for retail-tier capital).

These are all valid future projects. They shouldn't be conflated with the
Week 2-3 work.

---

## 7. Open questions for Khalid

Before any code is written, please answer:

1. **Q1.1** — Magnitude required, optional, or null-allowed?
2. **Q1.2** — Absolute price targets, relative %, or both?
3. **Q1.3** — Confidence as raw conviction, probability, or sample size?
4. **Q1.4** — Predictions in new table, embedded in signals, or both?
5. **Q2.1** — Static holdout backtests, walk-forward, or both?
6. **Q3.1** — Ranker universe: 213 tracked, ~600 wider, or 691 (tracked + screener)?
7. **Q3.2** — Ranker frequency: daily, 4-hourly, or 6-hourly?
8. **Q4.1** — Sizing output: weights, multipliers, or dollar amounts?
9. **Q4.2** — Calibrator weights into Khalid Index: full automation, human-approved, or A/B parallel?

Default recommendations are noted for each. Replying "go with defaults"
or "go with B/C/A on Q1.3, Q3.1, Q4.2" is a complete answer — I don't
need essays on each.

Once you answer (or accept defaults), Week 2 work can begin.
