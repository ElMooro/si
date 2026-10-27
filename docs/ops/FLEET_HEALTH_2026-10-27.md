# Fleet health, 2026-10-27 - the first scheduled day after the fixes, and the shared universe

Evidence: `aws/ops/reports/latest/ops_6687_*.md` (REPORT lines since 2026-10-26 23:40 UTC), `ops_6664_*.md` (census),
`ops_669{0,2,4,5,6}_*.md` (universe).

## 1. The five engines fixed on 2026-10-26, on their own schedules (ops 6687 + the Errors-metric census)

| Engine | Runs since 23:40 UTC | Errors | Max memory | Max duration |
|---|---|---|---|---|
| justhodl-analytics-snapshot | 1 (09:00) | 0 | 279 MB | 17.5 s (was 600 s timeout) |
| justhodl-earnings-whisper | 1 (08:00) | 0 | 219 MB | 1.5 s (was 60 s timeout) |
| justhodl-event-flow-monitor | 1 (05:00) | 0 | 100 MB | 2.8 s (was 120 s timeout) |
| justhodl-signal-genealogy | 2 (06:46, 13:37) | 0 | 616 MB | 372 s (was an exception every run) |
| treasury-auto-updater | 0 (switched off) | 0 | - | - |

The FINRA chain: justhodl-short-interest 192 s / 382 MB, 0 errors; the factor book's 02:50 join 322 s / 1,255 MB with
`coverage.finra_short` 19,302 (Polygon short 3,885), FINRA settlement 2026-10-15 from the research-head shards (23,678
issues, 256 shards).  The why.html desk (justhodl-equity-research): 368 user requests, 0 errors, max 305 MB, 7.8 s.

## 2. The census since 23:40 UTC (ops 6664, Errors metric)

604 functions invoked, **16 with errors** (46 errors in 2,792 invocations):

| Cause | Functions |
|---|---|
| **data/universe.json was empty** (section 3) | earnings-pead 3, revenue-acceleration 3 ("No selected universe"), momentum-breakout 3, volatility-squeeze-hunter 3 ("No original-scope membership") |
| Benzinga calendar 403 on the Polygon key (since 2026-10-10) | estimate-revisions 6 ("Original received calendar required"; preserves its publication by design) |
| 600 s timeouts | causality-scanner 3, predictability 3, chart-patterns 2 |
| "Request already attempted" (an async retry after a failed first attempt) | dark-pool 3, share-flows 3, forensic-screen 3 |
| Runtime.OutOfMemory at 256 MB | opportunity-screener 3 |
| provider acquisition failed | port-cargo 3 |
| research deadline, no publication | research-backtest 3 |
| pre-existing / single | portfolio-snapshot 1 (OrderingUnavailable), census-us 1 (850 s) |

## 3. data/universe.json - empty for a day, and polluted every day

`data/universe.json` is the shared input of ~45 engines.  S3 versions (ops 6690): 5,221-5,240 stocks every day except
**2026-10-16 19:53 and 2026-10-26 18:06 UTC: 0 stocks** - every `/company-screener` range answered HTTP 429 (the FMP
bandwidth block), v4 returned `[]` per failed range and wrote the empty result over the good one.  The engines that
select from it raised for the next 24 h (72 h Errors: earnings-pead 12/13, momentum-breakout 6/7, revenue-acceleration
6/7, volatility-squeeze-hunter 6/7, microcap-float-squeeze 3/5, options-flow-scanner 3/5).  The builder's own 18:06 UTC
run on 2026-10-27 got answers again and restored 5,230 stocks.  (Its three 2026-10-25 runs had died
Runtime.OutOfMemory at 512 MB while the bank-first shim decoded whole-market tables for the curated-seed profiles; the
point-scan rule deployed 2026-10-26 fixed that - 139 MB on 2026-10-27.)

ops 6694/6695 then showed the **live** universe itself carried 172 listings that are not a company's equity, each
with its issuer's market cap: notes (AT&T's 5.35% notes TBB as a $140B large cap, Southern Co SOMN/SOJE ~$105B, Entergy
Louisiana, Duke, Prudential, CMS, Xcel, T-Mobile), preferreds (Strategy STRC/STRF/STRK, Super Micro SMCIP), 48 SPAC
warrants, 13 rights, SPAC units - v4's suffix filter cannot see tickers without a dash.  Engines that take the first N
names by market cap were spending slots on them.

**universe-builder v4.1.0:**
* a failed range is recorded and its market-cap span is filled from the FMP bank's profile-bulk (streamed; the
  screener request's filters), limited to the previous publication's members, labelled `source: bank_profile_bulk`
  with the bank's `as_of`; the bank is used while <= 10 days old;
* the listing rule on both paths: Polygon's reference type (via the factor book) decides, with a second signal required
  for the types Polygon occasionally mis-assigns (Tutor Perini TPC is typed PFD, Balchem BCPC SP - both stay);
* the publication guard: < 1,000 stocks, < 85% of the previous total or a bucket < 50% of its previous count -> nothing
  written, the previous file stays, the run fails loudly;
* `{"dry_run": true, "force_bank": true}` is the verification path.

Proof (ops 6692, before the listing rule): a whole universe built from the bank alone in 11.6 s / 145 MB, 95.0% of the
live members and the same cap bucket for 98.2% of them; the difference was exactly the 172 non-equity listings plus
16 names the bank lists as inactive / below $1M.  ops 6696 (19:09 UTC, with the listing rule):

| | Result |
|---|---|
| bank-only dry run (no FMP call, nothing written) | 5,046 stocks, guard passes, 11.0 s / 152 MB; 100% of them live members, same cap bucket for 98.2% |
| first real run | screener answered every range; 13,769 raw -> listing rule dropped 606 (incl. ETNs / single-stock ETFs / trusts the screener returns) -> **5,056 published** (was 5,230); 17.6 s / 186 MB |
| membership change | 174 removed (the non-equity listings), 0 added; TBB, SOMN, SOJE, STRC, STRF, SMCIP, TMUSL, PFH gone; GWW, NWS, NWSA, ARM, ASTS, TPC, BCPC, AAPL, MSFT, JPM, MNR kept |

## 4. The factor book's identity flags (bank-factors v1.0.7)

The same dataset showed the factor book's heuristics marking real common stocks as not common - the "ends in WS/WW"
warrant regex (W.W. Grainger GWW, News Corp NWS/NWSA, 1-800-FLOWERS, Wolverine, Worthington Steel), "depositary share"
(every ADR named "American Depositary Shares": ARM, PONY, CHA ...), " preferred" (Preferred Bank) and the substring
" spac" (AST SpaceMobile, Extra Space Storage, MDA Space ...) - so they were missing from every common-only screen and
rank of the fused engines.  v1.0.7: a Polygon common-stock type outweighs those heuristics unless the name names the
instrument ("6.25 % Notes", "Depositary Shares, each representing ... Preferred" keep their flag); "spac" is a word.
Verified (ops 6697, a kick at 19:18 UTC, 352 s / 1,258 MB): common stocks 5,320 -> 5,356; GWW, NWS, NWSA, FLWS, WWW,
ARM, PONY, PFBC, ASTS, EXR, MDA common; TMUSL, AFGB, TBB, BAC-PL, SOMN, STRC still not.

## 5. Open (not changed today)

estimate-revisions' Benzinga calendar (403 on the Polygon key; a reviewed-contract engine that preserves its
publication); the three 600 s timeouts (causality-scanner, predictability, chart-patterns); the "Request already
attempted" trio (dark-pool, share-flows, forensic-screen - the async retry of a failed first attempt is refused by
design; the first attempt's failure is the thing to read); opportunity-screener at 256 MB; port-cargo's provider;
research-backtest's deadline; portfolio-snapshot's ordering check; census-us once at 850 s.

## 6. The S3 bytes-growth alarm (20:03 UTC): justhodl-worldbank-full

Alarm `justhodl-guard-s3-bytes-growth-1d`: the live bucket grew 82.5 GB in one daily datapoint (threshold 80 GB).
ops 6698 (bytes by writer from the access logs): **justhodl-worldbank-full wrote 70.8 GB on Oct 26 and 75.6 GB on
Oct 27** - everything else together ~10 GB/day.  ops 6699: the engine wrote its whole state document
(`data/warm/worldbank-full/_state/state.json`, 2.2 MB indented, the `have` map of 29,490 indicators) **twice per
indicator**, so the weekly re-drain that started Oct 25 03:20 left 56,690 state versions / 143 GB in the live bucket
while the indicator zips themselves total 0.48 GB; the sealed archive replicated every version (+44 GB Oct 25, +48 GB
Oct 26).  This is also the "weekly ~140 GB spike" the governor's storage fuse was tuned around.

**worldbank-full v1.1.0** (deployed 20:28 UTC): a ~100-byte inflight marker replaces the per-indicator state write (a run
that dies mid-indicator still counts toward the 4-try quarantine), the state is checkpointed at most every 120 s, compact
+ gzip (2.2 MB -> 216 KB), and each zip is hashed over its CSV content so a re-drain rewrites only what the Bank changed.
First light (ops 6700): one run took 1,059 indicators off the queue writing 8 state versions / 1.68 MB - **0.002 MB of
state per indicator instead of 4.4 MB**; v1.0.0 would have written ~4.7 GB for the same run.  The 143 GB of old state
versions in the live bucket expire under the existing rule `ops6541-bound-noncurrent-versions` (10 newest, 1 day), and
the archive's copies under its own seal rule `archive-keep-3-old-copies` (3 newest, 1 day) - correction to the first
note: the archive is NOT left holding them.  Khalid (19:36 ET): "if it's just multiple copies of the same thing I want
them gone, I only need one copy".  ops 6701: every version is a snapshot of the same progress file (phase / queue /
have counts; the World Bank data is in `src/<indicator>.zip`, untouched); the drain finished at 20:59 UTC (29,468
indicators banked, queue 0).  The runner may not delete versions in either bucket (explicit deny in both bucket
policies - by design), so the two lifecycle rules remove the 56,705 excess copies (153.5 GB) in each bucket; AWS stops
billing storage for a copy once it is past its expiry date, even while the removal is queued.  What remains: the
current file plus up to 10 (live) / 3 (archive) recent old copies, ~0.24 MB each since v1.1.0's gzip.  v1.1.1 writes
the per-indicator crash marker only in the run after a crash and checkpoints every 300 s.  Follow-up check ops 6702
(read-only) scheduled for 2026-10-29 15:07 UTC.
