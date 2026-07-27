# ops 3960 — PROBE 2: brain lexicon v2 + subject priority + futures join

**Status:** success  
**Duration:** 1.6s  
**Finished:** 2026-07-27T04:06:36+00:00  

## Data

| LIQUIDITY | MACRO | RISK | T1a | T1b | T2 | T3 | T3f | T4 | T5 | T6 | assets | classified | driver_LIQUIDITY | driver_MACRO | driver_RISK | drivers | from_his_own_words | own_words_pct | total | weak_labeled_notes | wl_LIQUIDITY | wl_MACRO | wl_RISK |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  |  | 6754 | 1905 | 3140 | 1709 |
|  |  |  | 9 | 3 | 480 | 5 | 1 | 3 | 60 | 0 |  |  |  |  |  |  |  |  |  |  |  |  |  |
| 141 | 336 | 84 |  |  |  |  |  |  |  |  |  | 561 |  |  |  |  | 492 | 87.7 | 561 |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  | 221 |  |  |  |  | 340 |  |  |  |  |  |  |  |
|  |  |  |  |  |  |  |  |  |  |  |  |  | 92 | 192 | 56 |  |  |  |  |  |  |  |  |

## Log
## A. lexicon v2 — in-anchor AND frequent, then weak-label expansion

- `04:06:36`   seed MACRO (n=299): ['absolutely', 'accord', 'account', 'adding', 'additionally', 'ahead', 'allow', 'allowing', 'along', 'angel', 'anticipation', 'anyway', 'apply', 'applying', 'appropriately', 'asia', 'asset', 'attractive', 'average', 'barometer', 'bearish', 'becoming', 'behavior', 'behind', 'believe', 'benchmark']
- `04:06:36`   seed LIQUIDITY (n=267): ['according', 'accounts', 'actions', 'affect', 'affects', 'agreement', 'alone', 'always', 'amount', 'ample', 'assets', 'balances', 'bank', 'banking', 'barometer', 'base', 'basis', 'bear', 'becomes', 'begun', 'biggest', 'billion', 'bitcoin', 'bogmbbm', 'bond', 'borrow']
- `04:06:36`   seed RISK (n=250): ['ability', 'addittion', 'already', 'always', 'amount', 'angels', 'arent', 'asset', 'assets', 'backbone', 'balance', 'bamlc', 'bamlh', 'bamlhe', 'banking', 'becomes', 'better', 'bigger', 'biggest', 'bofa', 'bond', 'bonds', 'borrow', 'broader', 'brokers', 'businesses']
- `04:06:36`   LEX MACRO (n=988): economy, macro, real, inflation, leverage, economic, countries, number, note, drains, value, quality, strengthens, favor, strong, indicators, trend, last, curve, raise, lead, returns, rise, provide, leading, falling, portfolio, chart, manufacturing, right
- `04:06:36`   LEX LIQUIDITY (n=696): banks, bank, repo, reserve, reserves, lending, bottom, funding, securities, federal, stress, bull, borrow, bitcoin, reverse, carry, institutions, lend, overnight, loan, typically, position, sofr, commercial, short-term, increasing, access, billion, excess, line
- `04:06:36`   LEX RISK (n=321): bond, bonds, credit, spread, spreads, corporate, sheet, equities, selling, measure, default, watch, equity, chaos, stay, corp, panic, bofa, event, income, step, leveraged, earnings, forced, maturity, bottoms, exit, called, shadow, fixed
## B. ladder

- `04:06:36`   learned category priors (T1/T2 only): {"commodity": "MACRO", "credit": "RISK", "equity": "MACRO", "futures": "MACRO", "fx": "MACRO", "macro": "MACRO", "other": "MACRO", "plumbing": "LIQUIDITY", "rates": "MACRO", "vol": "MACRO"}
## C. driver vs asset split (no self-prediction)

## D. worked examples

- `04:06:36`   RRPONTSYD  → LIQUIDITY [T2 ] own notes ['tv-a16acd9879dc9220', 'tv-f448e7c41a5a5693'] score=280320850v7006890 terms=['transferred', 'knee', 'chaneel', 'agrees', 'tempora
- `04:06:36`   SOFR       → LIQUIDITY [T2 ] own notes ['tv-f6ef08c9f739c287', 'tv-1f33aec6ccfcf62a'] score=19949566v278 terms=['instituions', 'requiring', 'replacing', 'institution', '
- `04:06:36`   WRESBAL    → LIQUIDITY [T1a] subject of anchor tv-f452edc700d3a8da
- `04:06:36`   JPLG       → LIQUIDITY [T1a] subject of anchor tv-b3ec3933837d5155
- `04:06:36`   MOVE       → RISK      [T1a] subject of anchor tv-14a76b6087dc80eb
- `04:06:36`   VIX        → MACRO     [T2 ] own notes ['tv-0c6357351cc61b1a', 'tv-e4a678dd4aba61ea'] score=3184771v30 terms=['generates', 'expectations', 'forward', 'closes', 'sentimen
- `04:06:36`   VVIX       → LIQUIDITY [T2 ] own notes ['tv-cdefc56e453c652a', 'tv-2c7983504fc0f37a'] score=14v8 terms=['short-term', 'quickly']
- `04:06:36`   SKEW       → MACRO     [T2 ] own notes ['tv-9b9a60d9a86ee929', 'tv-09640a0a721b0eb8'] score=2547791v14 terms=['edges', 'sentiment', 'leading', 'curve', 'ends']
- `04:06:36`   DXY        → LIQUIDITY [T1a] subject of anchor tv-9fa576184567fa8f
- `04:06:36`   USDX       → MACRO     [T2 ] own notes ['tv-9272761fabc17977', 'tv-d12594ebe94e32ee'] score=42v0 terms=['bullish', 'bearish', 'falling']
- `04:06:36`   US10Y      → MACRO     [T1a] subject of anchor tv-b4c32545ea1dc640
- `04:06:36`   US02Y      → MACRO     [T2 ] own notes ['tv-1319552adc54e57d', 'tv-2eabc51a489f4778'] score=16879691v467 terms=['overheated', 'confident', 'mandate', 'afraid', 'recessio
- `04:06:36`   TEDRATE    → RISK      [T2 ] own notes ['tv-2a53d1de698ed44e', 'tv-2a9eaca03ec90218'] score=56v15 terms=['risk-free', 'widens', 'default', 'spread', 'credit', 'measure']
- `04:06:36`   XAUUSD     → MACRO     [T2 ] own notes ['tv-423ac77b1559efa6', 'tv-24db5be5de3dfd0a'] score=35v0 terms=['perform', 'falling']
- `04:06:36`   GOLD       → MACRO     [T2 ] own notes ['tv-3dbac12c4327292b', 'tv-06e196b7baa0a856'] score=10828472v4199664 terms=['annualized', 'minute', 'predicting', 'bullish', 'fla
- `04:06:36`   CL1!       → MACRO     [T2 ] own notes ['tv-cd4416418c37c69f', 'tv-92c1db49abcd1af9'] score=84078177v8191975 terms=['sanctions', 'shipping', 'renewable', 'horizons', 'in
- `04:06:36`   HG1!       → MACRO     [T2 ] own notes ['tv-53730fbcb0775a83', 'tv-cda6eee04f61ae87'] score=5732918v7 terms=['interpret', 'input', 'trends', 'passed', 'industrial', 'acc
- `04:06:36`   GC2!       → MACRO     [T2 ] own notes ['tv-d9c34dff403ff7d7', 'tv-6beb1b500c04f858'] score=3821818v59 terms=['annualized', 'bullish', 'dramatically', 'bought', 'wrecks'
- `04:06:36`   SR32!      → MACRO     [T2 ] own notes ['tv-e8ea1cc6dc9b2a4b', 'tv-cfcc7b300f021b87'] score=184714101v272 terms=['imperial', 'cascading', 'evaluate', 'endured', 'untethe
- `04:06:36`   ZF1!       → MACRO     [T2 ] own notes ['tv-55bfc5da2e79f21f', 'tv-e931b262ace35714'] score=333v74 terms=['perform', 'expectations', 'tend', 'rise', 'inversely', 'fall']
- `04:06:36`   SPX        → MACRO     [T2 ] own notes ['tv-39e5a021df56a47b', 'tv-50acf6781eeee2ac'] score=698096294v286618644 terms=['frontier', 'imperial', 'divide', 'timing', 'apply
- `04:06:36`   USM2       → LIQUIDITY [T2 ] own notes ['tv-da6c576551ccab9b', 'tv-0ea53726ec4eb02e'] score=6299363v10 terms=['distinguishing', 'deposits', 'reporting', 'checks', 'remai
- `04:06:36`   USCLI      → MACRO     [T2 ] own notes [] score=22v0 terms=['economic', 'indicators', 'leading']
- `04:06:36`   USIRYY     → MACRO     [T2 ] own notes ['tv-cd8a3382285dfc55', 'tv-d2fd25b4cb733bc5'] score=56051825v46 terms=['overheated', 'mandate', 'chair', 'emphasized', 'accommoda
- `04:06:36`   BDI        → MACRO     [T2 ] own notes ['tv-bf9bc0bbaa83998d', 'tv-c5ba85661337e63a'] score=30573392v6 terms=['shipping', 'baltic', 'input', 'spots', 'rise', 'economic']
- `04:06:36`   USNFP      → MACRO     [T2 ] own notes ['tv-963105f8dac801c6', 'tv-21fd189838fad255'] score=9554261v5 terms=['hyperinflation', 'number', 'dynamics', 'employment', 'consu
- `04:06:36`   USFER      → MACRO     [T3 ] co-occurs with 9 MACRO symbols
- `04:06:36`   USLEI      → MACRO     [T2 ] own notes ['tv-a878e91b61293580', 'tv-5d91231ce6660cec'] score=5732617v14 terms=['permits', 'published', 'expectations', 'orders', 'consumer
- `04:06:36`   USINTR     → MACRO     [T2 ] own notes ['tv-73dee9663e8cd469', 'tv-c312ef1723cf3703'] score=66879326v13648496 terms=['pyramided', 'deplete', 'discounted', 'vast', 'input
- `04:06:36`   CN10Y      → LIQUIDITY [T2 ] own notes ['tv-973fb82dc1cda723', 'tv-dd797ca0a506555c'] score=14v9 terms=['underperforming', 'drained']
- `04:06:36` ✅   seeds found for 3 domains
- `04:06:36` ✅   expanded lexicon non-trivial
- `04:06:36` ✅   100% classified
- `04:06:36` ✅   no T6 backstop needed
- `04:06:36` ✅   majority from his own notes (T1/T2 >= 60%)
- `04:06:36` ✅   all 3 domains have drivers
- `04:06:36` ✅ PASS_ALL — 561/561 classified, 492 (87.7%) from his own notes; MACRO 336 LIQUIDITY 141 RISK 84
