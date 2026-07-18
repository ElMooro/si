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
   ordering buries megacaps. Own symdir search is the fix (v1.1.5).
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
3470 why.html embedded module (9-chip vitals, 6 lenses) · 3471 symdir search
+ this archive.
