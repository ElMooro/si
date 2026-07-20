# 36 — Deal-Scanner v2.0.0: full-market + graded deal-win family (ops 3571–73, 2026-07-20)

Trigger: IREN $2.8bn AI contract fired on v1 and pumped +19% same day — Khalid ordered
full-market coverage (all industries, all caps) + fusion into every engine that needs it.

## Engine (justhodl-deal-scanner, VERSION 2.0.0, timeout 420s)
- Sources: FMP /stable/news/press-releases-latest + stock-latest 14 pages each (2,800)
  + Polygon news 8 pages (800) = ~3,600 items/run, ~1,300 unique tickers on tape.
  NO universe filter — every ticker on the PR/news tape is eligible.
- Benzinga leg DELETED (Massive 403 fleet-wide since 2026-07-15; key removed from source).
- Origin tagging per item (fmp_pr / fmp_news / polygon) → coverage.sources counts.
- Crossref cap 300→450 tickers (revenue + market cap), 20 workers.
- NEW feed blocks: by_sector (all 11 GICS, zeros shown), by_cap (nano→mega, 6/6 hit day one),
  coverage {sources, n_items, n_unique_tickers_in_tape, sectors_with_deals/11, caps_with_deals/6,
  runs_per_day 8}. deals[:120]→[:200]. summary += signals_logged, signals[], sectors/caps hit.

## Graded family: deal-win (shared signals_emit → regime-stamped, suppress-aware, deduped)
- Bar: age_h ≤ 30 AND (highlight green OR ai_megadeal OR ($1B+ AND vs_mc ≥5%)), ≤10/run.
- UP [5,21,63] vs SPY at announcement-time price (yprice). Conf 0.66 green+AI-mega / 0.62 green / 0.58.
- Metadata: deal_value_usd, vs_mc_pct, materiality_pct (9999=pre-revenue), cap_bucket, sector,
  highlight, ai_megadeal, age_h + regime snapshot (free via log_signal).
- DAY-ONE: IREN ($4bn ARR target, 33.34% mcap, conf .66, base 40.237) · EPR ($1.6B, 33.6%) ·
  MRAI ($26.4M, 75.2% mcap!) · KMDA ($50M, 12.2%). DDB row deal-win#IREN#2026-07-20 verified.
- Grading loop + alpha-triage + PROVEN gate now apply automatically to the family.

## Fusion (ops 3572, all PASS)
- best-setups: _deal_idx from data/deal-scanner.json (≤72h, best-score per symbol) →
  _s["deal_context"] in the setups[:25] enrichment loop (same slot as sector/census context).
- master-ranker: t["deal_win"] overlay (mirrors khalid_note/squeeze_fuel), n_deal in fusion print.
- morning-intelligence: feed "deal_scanner" + facts fresh_deal_wins[:5] + deal_signals_today.
- why.html: #jhDealRadar section + window.fillJHDealRadar (mirrors ops-3299 DollarFlows closure,
  uses gj9/E9 helpers) — up to 3 fresh deals for the ticker, pills, link to full board.
- alpha-families.html: 7th card c-deals (deal-win · [5,21,63] vs SPY), feed appended to
  Promise.allSettled (positional destructure ...,dw), footer feed list updated.
- deal-scanner.html ADDITIVE: +2 statbar tiles (Sectors Hit x/11, Graded Signals),
  Market Coverage sector table, All Cap Tiers table, Graded Signals strip, note rewrite.

## Gotchas (new)
- ⚠️ LIVE EB rule deal-scanner-daily was ALREADY cron(5 */3 * * ? *) (every 3h, 8 runs/day) while
  config.json said daily 22:00 — deployed cadence had been upgraded out-of-band. ALWAYS
  events.describe_rule the live cadence before touching schedules; never downgrade live reality.
  Ops 3573 aligned config + all page/engine strings to every-3h. Parity verified.
- ⚠️ Sandbox: create_file tool writes under /home/claude/... but bash HOME=/root — copy artifacts
  into the /root/work/si clone before git add, or files silently miss the commit.
- coverage.n_tickers_crossref counts DEAL-candidate tickers (post-filter), not tape tickers —
  20 on a normal day is correct; the 450 cap matters on heavy news days.
