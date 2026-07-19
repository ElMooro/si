# 34 — Fundamental Graphs stack (ops 3462–3471) — LIVE, DO NOT REBUILD

**Audit-first:** this whole capability exists. EXTEND, never duplicate.

## What it is
TradingView-class fundamentals comparator. Engine `justhodl-fundamental-graphs`
(Function URL, public, CORS at URL level ONLY, gzip) + flagship page
`/fundamental-graphs.html` (sidebar pin: Research & Tools) + embedded module
inside `/why.html` (`#jhFundGraphs`, script id `fgwhy-3470`).

- 3 symbols × 20 metrics × 10+ yrs (44 FQ / 12 FY), any ticker on demand.
- **200-series catalog per symbol** — raw statement lines + ALL ratios computed
  in-house TTM-proper with `mcap_t = close_t × diluted shares_t` (no vendor
  ratio endpoints). Growth/CAGR family, HF set (GP/Assets Novy-Marx, Rule of
  40, Greenblatt EY+ROC, EV/GP, FCF conversion, NET buyback yield per
  SBC_WASH, total yield incl. debt paydown), credit block, per-employee,
  estimates (history+future), scores per period: Altman Z & Z″, Piotroski,
  Beneish, Sloan, Springate, Zmijewski, Fulmer H, KZ, Tobin's Q.
- Page: Q/A/TTM (client rolling-4Q on `f:1` keys), Values / %Chg / YoY, log
  scale, today divider + dashed estimates, price overlay, dual-axis unit
  groups, crosshair, right-edge value pills, 7 Lens presets, snapshot Table
  (LOWER_BETTER set drives best-in-row), symbol typeahead, CSV/PNG export,
  ★ metric favorites (`jh_fg_favm`) + per-metric color tags (`jh_fg_metc`),
  saved graphs (`jh_fg_saves`), deep links `?s=&m=&p=&r=&mo=&px=&lg=`.

## Engine contract
- URL: `GET /?symbol=X&period=quarter|annual[&refresh=1][&gz=1]`,
  `GET /?search=q` (symdir-first, FMP dual-search fallback),
  Event `{"warm":[...]}`, `{"warm_auto":true}`.
- Function URL: `https://fqb6ztg7v6ax4qzylimqjiezmq0kqyyy.lambda-url.us-east-1.on.aws`
  (also published to `data/fundgraph/config.json {api_url}`; page has baked
  FALLBACK_API). timeout 900 / mem 512 / env FMP_KEY,S3_BUCKET,CACHE_TTL_SEC.
- Caches: `data/fundgraph/cache/{SYM}_{period}_v11.json` (TTL 20h — bump
  `CACHE_VER` when catalog changes). Demand markers `data/fundgraph/hits/{SYM}`
  on every URL hit. Symbol directory `data/fundgraph/symdir.json`
  (US_EXCH-filtered FMP stock-list, 8d TTL, Monday force-refresh).
- Warmer: EventBridge Scheduler `fundamental-graphs-warmer-sched`
  cron(25 9 * * ? *), Input `{"warm_auto":true}` → STATIC_CORE(36) ∪ hits≤7d
  (cap 60), time-guarded via context; annuals + symdir on Mondays.
- FMP /stable endpoints used: income/balance/cash-flow statements,
  analyst-estimates, profile, historical-price-eod/light, employee-count,
  stock-list (symdir), search-name/search-symbol (fallback only).

## Gotchas banked (cost real debugging time)
1. **Dual CORS = browser "Failed to fetch"**: URL-level Cors AND function
   ACAO ⇒ duplicate `*, *`; browsers reject, curl/urllib don't. Function must
   emit ZERO CORS headers; URL config is single authority (ops 3464).
2. Lambda-URL responses use **lowercase header names** — probe with
   HTTPMessage case-insensitive `get`/`get_all`, never `dict(r.headers)`
   capitalized lookups (ops 3463 false-FAIL).
3. **`list.index()` inside a sort key of the list being sorted** ⇒ CPython
   empties the list during sort ⇒ ValueError ⇒ 502 (ops 3469). Use
   `sorted(enumerate(...))`.
4. `sed old > new` when old path is wrong ⇒ **empty file that py_compile
   passes**. Executed ops move to `ran/` on commit-back — source follow-ups
   from `aws/ops/ran/`.
5. FMP `search-symbol` matches TICKERS ("micro"→MICRO.BK); `search-name`
   buries megacaps. FIX (v1.1.8): own directory `data/fundgraph/symdir2.json`
   built from company-screener TWO BANDS (>2B + 5M–2B, limit 10000 each,
   union ≈7.3k US names) — rows [sym,name,exch,marketCap]; search ranks
   tier (exact>sym-prefix>name-prefix>word>contains) then **mcap DESC**
   (Microsoft > MicroBot). ⚠ /stable `stock-list` = 90k rows but ONLY
   [companyName,symbol] — no exchange field, unusable for filtering;
   stock/list + available-traded/list = 404 on /stable.
6. Estimates are CURRENT-vintage — never fabricate historical forward
   multiples from them.
7. Page↔engine key parity is checked pre-push (CAT keys ⊆ lambda source,
   regex must be `[a-zA-Z0-9_]+` — digits in keys like `rule_of_40`).
8. warm boto3 invoke needs `Config(read_timeout≥920)` for 900s runs.

## Ops trail
3462 build (G3 CHTR grossProfit real-data cross-check) · 3463 header-case ·
3464 CORS single-authority + FALLBACK_API · 3465 200-metric library +
Favorites/Institutional/color-tags · 3466 warmer + log/today/export/Load-favs ·
3467 lenses/typeahead/Table + why.html link · 3468–69 search rank + 502 fix ·
3470 why.html embedded module (9-chip vitals, 6 lenses) · 3471–74 symdir
saga (diag hook ?symdir=1 → stock-list fieldless finding → two-band union
→ mcap ranking; quartet green: micro→MSFT#1) + this archive.

## Arc 2 — ops 3477–3491 (shared core, fusion, macro) — LIVE, DO NOT REBUILD

**Shared assets (single source, both surfaces consume):**
- `/fg-catalog.js` — FG_CAT **202** keys + FG_TABS + FG_INST (OPS3476+3482).
- `/fg-chart.js` — core **v7** `FGChart.{render,fmt,grp,ratio,niceTicks}`:
  dual-axis groups, log, today divider, dashed estimates, gap-safe paths,
  value pills, crosshair, p10–p90 band (`opts.band`), earnings beat/miss
  (`opts.earnings`), NBER backdrop (always-on), placed markers
  (`opts.marks/onMark/onUnmark`, click snaps to nearest data date),
  events rail (`opts.events` congress ◆ / insider ▲▼ + tooltips),
  **per-series own scales** (`own:true`; price = own+style) — joint render
  is byte-identical to solo renders (independence proof).
  `FGChart.ratio(a,b,maxGap=140d)` at-or-before aligned, zero/gap guards.

**Flagship v2.4 URL params:** `s,m,p,r,mo,px,lg,e,ev,mk,rt,mx`.
Features: lenses, typeahead (symdir2), snapshot Table (+per-cell pXX),
pctile/z legend chips, saves (carry ratios; load resets ratios/marks),
Earnings, Events, 📍markers, **Ratio builder** (6 max, `rt=a~mA~b~mB`),
**Macro overlays** (3 max, `mx=`), red-flag digest cards, whale chip.

**why.html module `fgwhy-3478`:** live-updating picker (MIXOK Values
override), 470px shared-core chart (Values/%/YoY, 1-10Y, log, FQ/TTM,
price, Earnings, Events), per-ticker markers (`jh_fgwhy_marks`), flags
chips, whale chip. Shared stores: `jh_fg_favm`, `jh_fg_metc`.

**Engine v1.4.0 (cache v14) doc additions:** `earnings` [[d,act,est]],
`flags` (10-rule digest, `derive_flags`), `whales_q`, `events`,
`vintage_days`, `implied_fcf_growth_pct` + `implied_vs_actual_gap_pct`
(2-stage 10y rev-DCF, r=9%, gT=2.5%, bisection — inversion err 0).

**Fleet joins (schemas AUDITED from writers):**
- `data/13f-flows-by-ticker.json` = **COMPACT COHORT ledger**
  `{t:{TICK:{b,s,n,wb,ws,wn,nf,na,tv,fb,fs}}}` — clone-alpha whale
  cohort, NOT all-institutions. GOOGL `b`=$11.545B (the banked figure was
  the BOUGHT side); n==b−s exact. Label as "tracked whale cohort".
- `data/congress-direct.json` → `senate.transactions`
  {filer,tx_date(MM/DD/YYYY or ISO),owner,asset,ticker,type,amount}.
- `data/insider-trades.json` big_buys{ticker,insider,role,value,filed_at}
  + clusters{last_filing,insider_count,total_value}.
- `data/insider-sell-cluster.json` = **WINDOW feed, no per-cluster
  dates** {n_distinct_sellers,total_sale_value_usd,…} → mark at feed
  `generated_at` only.

**wl-series bridge (macro):** `justhodl-wl-series-api` Function URL
`https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws`
`?sym=` → weekly points **keyed by ISO WEEK ("2026-28")** —
`new Date()` is Invalid; ALWAYS convert with chart-pro-parity `wk2d()`
(2026-28→2026-07-06). Resolved id map (probe 3490): US10Y=TVC:US10Y,
US02Y=TVC:US02Y, HYOAS=FRED:BAMLH0A0HYM2, FEDFUNDS=FRED:FEDFUNDS,
UNRATE=FRED:UNRATE, DXY=TVC:DXY. T10Y2Y = client-derived 10Y−2Y
(week-aligned). CPI YoY NOT in the wl map (dropped honestly).

**Gotchas (added this arc):**
9. Unasserted `str.replace` patches silently no-op — EVERY anchor gets
   an assert, and anchors match bytes-as-served (r-string `\uXXXX`
   artifacts live in the module/flagship).
10. **Ops files are NEVER cloned** (sed or `.replace` digit-swaps) —
    3483 crashed pre-report, 3487 poisoned its own byte-gates. Purpose-
    write every ops.
11. Mixed-unit selections >2 groups auto-switch % — module needs the
    MIXOK user override or Values becomes unreachable.
12. CI node shims: capture `window.FGChart` AFTER the IIFE; never
    string-replace the export.

**Harnesses:** `/home/claude/microband2.js` core truth-suite (px
no-collapse, mark place/remove, NBER, ratio math, events rail, own-scale
independence) · `repro2.js` module behaviors (live-add, negatives-in-
Values, TTM 4×, persistence, pctile chip, earnings dots, band, flag-chip
evidence, marker place/persist/remove).

**Ops trail 2:** 3477 shared core · 3478 pctile/earnings · 3479 vintage
· 3480 flags · 3481 marks+px-own · 3482–85 impliedG/NBER/whale saga ·
3486–88 ratio · 3489 events · 3490–91 macro bridge + wk2d.
Parked: Tier-3 (sector medians, factor-DNA radar, DuPont stack), module
ratio/macro inheritance, CPI overlay if the wl map gains it.



---

## Arc 3–4 addendum (ops 3482–3511, 2026-07-18/19) — verdict/TA/volume/Tier-3 layers

**Engine `justhodl-fundamental-graphs` v1.10.2, CACHE v20.** Doc fields added this arc: `tech` {events,status{...,volume}}, `factor_dna` {state,n_universe,axes[{k,label,val,pct,dir}],conviction{rank,score,n_systems,systems,rationale}|null}, P gains px_ma20/50/100/200, px_bb_up/dn, rsi_14, volume_w, vol_ma20 (TA/vol keys are toggle-only, NOT in catalog — catalog-parity is catalog⊆engine).

**Layer map (all PASS-gated):**
- 3482–85 impliedG/NBER/whales · 3486–88 pairs ratio (FGChart.ratio) · 3489 fleet event markers (congress ◆ / insider ▲▼) · 3490–92 macro via wl-series bridge (ISO-WEEK wk2d) + module parity + shared FG_* promotion · 3493–94 SECMED p50 + vs-Sector UI · 3495–97 verdict layer (26 rules N/T/S, FIN suppression, six-suite battery) · 3498 ELITE (ELITE_NORM 18 + sector top-decile via bands p10/p90 v2) · 3499 controls+semantics (👁 hide/✕, red=sev3 flash, yellow=warn, green=elite, EXTREME flash; EXTREME_NORM 8) · 3500–01 TA (daily-bar SMA/BB/RSI-Wilder/crosses/GC-DC/dbl-patterns; compute AFTER P assembly line ~798) · 3502–03 volume (continuity ≥95%/3y else named dormancy; weekly SUM reconciled EXACT to the share ISO-wk 2026-27 = 257,092,060; RVOL; VOL_SPIKE ≥2.5×; core v10 bars primitive) + eye-fix + macro dropdowns (loadMacro rollback) + auto tech markers (200/GC/DC/confirmed-DBL always-on) · 3504 module parity (hide+ghost, Table, CSV pure builder, PNG) · 3505–06 full-spectrum verdicts (tech-basis rules px_vs_200/ma_stack/ma_regime-dated/dbl/rsi≥80/rvol≥3; ROE/ROA/DIO; **tech cap-exempt** — fundamentals capped 14/12, tech appended) + module three-card block (green/red/digest ●, single data-vk emitter) · 3507–11 Tier-3 (nightly medians — pulled out of Monday gate; factor-DNA radar on the 496-name FORENSIC universe with goodness-flip FACTOR_LOW {beneish,sloan,pe,peg,concern_score}, master-ranker = conviction chip only (top_tickers is a 25-name conviction list, NOT a factor panel); DuPont SIGNED log-contribution stacks around a zero axis summing exactly to log ROE, both surfaces).

**Flagship v3.2 URL params:** s,m,p,r,mo,px,lg,e,ev,mk,rt,mx,ta,sc,h + S.dup. Buttons: DuPont/TA/vs-Sector/📍/Ratio/Table/Earnings/Events; mxsel dropdown + mxchips; #fgRadar #fgDup #fgTech #fgFlags. Module ids: jhfgMxSel/data-mxrm, jhfgTa/jhfgTaStrip, jhfgDup/jhfgDupBox, jhfgTbl2/Csv2/Png2, data-eye/data-unhide (jh_fgwhy_hidden), jhfgVerd cards + digest data-fk, jhfgRadar.

**Harness v13 = 37 behaviors, exit-code enforced** (repro2.js; microband2.js core 9 suites incl pxAux shared-scale + bars). Fixture DOC carries tech/status.volume/factor_dna.conviction/flags×2.

**Gotchas 13–24 (append to 1–12):**
13. python open('w') truncates BEFORE encode errors — safe_write (encode→tmp→os.replace) always; emoji in page patches as \\uXXXX doubled.
14. assert-every-replace applies to HARNESS edits too.
15. Engine helper call-sites use REAL var names (profile not prof) — grep before wiring.
16. P initialized ~line 798 — P-writing blocks sit AFTER assembly (3500 NameError caught live).
17. Synthetic TA fixtures: exact ties never seed cross prev-sign; duplicate peak values kill unique-pivot; ramp must stop short of the peak.
18. bull_stack strict > — flat synthetics fail correctly.
19. Flagship metric chips: template attrs MUST close ("style=..." quote) and the generic '.x' binder was scoped to [data-m] — new chip controls need their own data-attrs or the last onclick wins.
20. mxPop died because shared-promotion renamed its source; ANY popover → dropdown rewrite must call the real loader (loadMacro/loadMacro2) with unavailable-rollback.
21. Verdict caps evict low-sev entries — regime/state verdicts must be cap-exempt (3506).
22. DuPont: log(NM/100)<0 always for NM<100% — only SIGNED log stacks are mathematically true.
23. master-ranker top_tickers = 25 conviction rows {ticker,score,n_systems,systems,contributions,rationale} — join as overlay, never as a percentile universe. Forensic all_results rows carry piotroski/altman_z/beneish_m/sloan/fcf_yield/pe_ttm/peg/strength_score/concern_score(+ HIGH=BAD → FACTOR_LOW).
24. Harness mk(base,NEG) second arg NEGATES; jsdom shim must not clobber page window.__ops (use __lastOpts); final PASS line must exit on the FULL aggregate.

**Equity-FTD queue note (#5):** ignition already owns the SEC CNS fetch (`load_ftd()`: cnsfails{YYYYMM}{a|b}.zip, pipe cols date|CUSIP|SYMBOL|QTY|desc|price, half-month per-symbol sums + chg ratio) — the family build EXTENDS this (per-day rows, $-value, days-to-cover via FMP volume, schema-v2 signals + grading), never re-fetches from scratch.
