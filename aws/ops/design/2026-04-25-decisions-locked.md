# Week 2-3 — Locked Design Decisions

**Decided:** 2026-04-25 by Khalid (defaults accepted as proposed in the
[Week 2-3 Architecture Document](2026-04-25-week-2-3-architecture.md))

This document is the canonical reference for design decisions during
Week 2-3 implementation. If a decision needs to change later, append
a dated revision rather than rewriting.

## Decisions

| # | Question | Decision | Implementation impact |
|---|---|---|---|
| Q1.1 | Magnitude required, optional, or null-allowed? | **B — Optional with default** | `predicted_magnitude_pct` is nullable in schema; signal-logger only sets it for signals where it's natural (momentum, screener) |
| Q1.2 | Target prices: absolute, relative, or both? | **C — Both** | At log-time store `predicted_magnitude_pct` (relative) AND `predicted_target_price` (computed: `baseline × (1 + pct/100)`) |
| Q1.3 | Confidence semantics? | **B — Probability** | Calibrator must produce reliability curves over time (stated 0.7 → realized hit rate 0.7) |
| Q1.4 | Where predictions live? | **B — Embedded in `justhodl-signals`** | New fields added to existing item shape; no new DynamoDB table |
| Q2.1 | Backtest method? | **B — Walk-forward** | Backtester replays signal-logger logic against historical data in rolling 90-day windows |
| Q3.1 | Ranker universe? | **C — 188 tracked + 503 screener (deduped)** | Ranker reads both `data/report.json` stocks AND `screener/data.json` stocks |
| Q3.2 | Ranker frequency? | **A — Daily 8 AM ET** | New EB rule `justhodl-daily-ranker` cron(0 13 * * ? *) UTC = 8 AM ET (winter) / 9 AM ET (summer DST) |
| Q4.1 | Sizing output format? | **B — Position multiplier** | Ranker output includes `position_multiplier` field (0.0x to 2.0x relative to base) |
| Q4.2 | Calibrator → Khalid Index automation? | **C — Both coexist (A/B parallel)** | Original Khalid Index unchanged. New `khalid_index_adaptive` computed alongside using calibrator weights. |

## Implementation order

Per the design doc, work proceeds in this dependency order:

1. **Week 2A — Predictions schema migration** (THIS PHASE)
   - Add new fields to `log_sig()` in `justhodl-signal-logger`
   - Backfill optional fields as `null` for existing rows (no migration needed
     since DynamoDB is schemaless; new fields just appear as `None` in old items)
   - Verify fresh logger run produces new schema cleanly

2. **Week 2B — Backtester Lambda** (after 2A)
3. **Week 3A — Daily Ranker Lambda** (after 2B + ~1 month of real outcomes)
4. **Week 3B — Position sizing layer** (after 3A)

## Deferred to future revisions

These are explicit non-decisions — defer until data tells us more:

- **Magnitude bands (`WEAK / MODERATE / STRONG`)** — let the calibrator find
  natural breakpoints from realized data instead of hardcoding
- **Risk factors enrichment** — initial ranker emits `risk_factors: []`; we'll
  populate it after we see what real predictions look like
- **Adaptive Khalid Index weight formula** — start as a simple weighted
  average using calibrator accuracies; revisit after 30 days of comparison
  to fixed Khalid Index

## Field addendum to predictions schema

Final embedded schema for `justhodl-signals` items (changes in **bold**):

```python
{
    # ─── Existing fields (unchanged) ─────────────────────
    "signal_id":             str,
    "signal_type":           str,
    "signal_value":          str,
    "predicted_direction":   str,    # UP / DOWN / NEUTRAL / OUTPERFORM / UNDERPERFORM
    "confidence":            Decimal,  # NOW TREATED AS PROBABILITY (Q1.3)
    "measure_against":       str,
    "baseline_price":        Decimal | None,
    "baseline_benchmark_price": Decimal | None,  # set in last session's fix
    "benchmark":             str | None,
    "check_windows":         list[str],
    "check_timestamps":      dict,
    "outcomes":              dict,
    "logged_at":             str,
    "logged_epoch":          int,
    "status":                str,
    "metadata":              dict,
    "ttl":                   int,

    # ─── NEW Week 2A fields ─────────────────────
    "schema_version":        "2",                # bump from implicit 1
    "predicted_magnitude_pct": float | None,     # Q1.1 — optional, set when natural
    "predicted_target_price": Decimal | None,    # Q1.2 — computed from magnitude
    "horizon_days_primary":  int,                # the longest check_window
    "regime_at_log":         str | None,         # Khalid regime snapshot
    "khalid_score_at_log":   int | None,         # Khalid score snapshot
    "rationale":             str | None,         # human/AI-readable why
    "supporting_signals":    list[str] | None,   # related signal_ids
}
```

`schema_version: "2"` lets future consumers (backtester, ranker) check
which schema they're reading and handle missing fields gracefully.

Old items have no `schema_version` field at all — that's the implicit v1
marker.
