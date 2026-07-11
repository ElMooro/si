# why.html roadmap — Tier 2/3 continuation (banked at ops 3125, next ops 3126)
Khalid directive: "Build every single one of them one by one." Tier 1 (#1-#5) SHIPPED+PASS at ops 3125.
Pattern: batch per tier, verify per feature (per-feature marker sets in one ops), 3118 ASCII markers, 3116 marks[0]-new, Event+S3-poll.

## Tier 2 — equity-research engine extensions (24h-cached, real data only)
#6 EARNINGS-VOL EDGE: next earnings date + countdown (FMP /stable/ earnings calendar); options-implied move
   (extend build_options_expectations straddle math) vs median realized |T+1| move over past 8 prints
   (price series x historical earnings dates) -> RICH/CHEAP vol read; PEAD stats avg T+1/T+5/T+20 by beat/miss
   (surprises already feed renderEarningsTrack — reuse, don't refetch).
#7 SEASONALITY: 20y avg return + hit-rate by month (needs one added FMP full-history EOD call, cached with doc).
#8 FACTOR EXPOSURE RADAR: value/momentum/quality/low-vol/size percentiles vs universe (compose from
   forensic-screen + share-flows + master-ranker universes page-side where possible) + factor-regime doc join
   -> "regime tailwind/headwind for this factor mix".
#9 SINGLE-NAME OPTIONS POSITIONING: IV rank vs own 1y REQUIRES a new daily IV ledger
   (data/iv-history/<T>.json appender, EventBridge Scheduler — classic rule cap SATURATED, use
   arn:aws:iam::857687956942:role/justhodl-scheduler-role); 25d skew + P/C OI from chain.

## Tier 3 — product
#10 PDF TEAR-SHEET: print stylesheet + one-click export button (client-side first; server render later).
#11 COMPARE MODE: ?tickers=A,B side-by-side (two research docs, shared axis).
#12 WATCH ALERTS: per-user (Supabase entitlements + CF KV) — golden cross / concern>=40 / revision flip;
    multi-tenant per SaaS doctrine, queue behind entitlement wiring.

Continuation phrase: "continue the why.html roadmap" -> bootstrap, read this file + STATE.md, run #6 onward.
