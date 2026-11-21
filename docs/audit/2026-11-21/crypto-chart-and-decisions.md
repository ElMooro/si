# 2026-11-21 (afternoon) - bitcoin on the chart, crypto freshness, and the four decisions

Khalid, 12:12 ET: "INDEX:BTCUSD · No supported chart route; chart unchanged i cant pull btc on chart. investigate why i
cant pull symbols out of search bar ... also complete this ... also do this: Merge the one-file-per-symbol change ...
World Bank pass monthly ... May I open justhodl-repo and justhodl-polygon-bank ... i want all my crypto and stocks and
etfs to refresh for my data to stay accurate without my monthly budget going over $250".

Every number below is from a read-only op whose report is in `aws/ops/reports/latest/`.

## 1. Why bitcoin would not chart (ops 6879, 6882)

| what was picked | before | cause |
|---|---|---|
| `INDEX:BTCUSD` (TradingView's first answer for "bitcoin") | no route | the symbol map sends it to a provider coin id ("bitcoin") the router cannot hand to the chart; a map hit it cannot use ended the search |
| `INDEX:ETHUSD` | chart unchanged | went to `^ETHUSD`, which nothing serves |
| `BINANCE:BTCUSDT`, `BINANCE:SOLUSDT`, `COINBASE:SOLUSD`, `KRAKEN:XBTUSD` | blank | "native" route: the engine sends every `VENUE:` id to `/series` and returns nothing when that fails |
| `SOLUSD` (no venue) | chart unchanged | no feed answers under that name |
| `X:BTCUSD`, `X:SOLUSD` ... (the directory's own ids) | drew, but ended a day behind | the banked daily bars alone |
| `CME:BTC1!` | no route | map hit, same dead end |

Fix: one reviewed crypto table at the top of the watchlist router (`jh-chart-tvwatch.js`), and the engine
(`jh-chart-engine.js`) opens the directory id and the bare pair on the same tape. A spot pair of a coin **that has
banked bars** (90 coins), under `INDEX` or 18 crypto venues, is `<COIN>-USD`. CME bitcoin / ether futures go to
`BTC=F` / `ETH=F`. Not routed, on purpose: cross pairs (`ETHBTC`), perpetuals, dated futures, `CRYPTOCAP:*`,
on-chain ids - the warehouse holds no such series.

Proof on the live site, real browser (ops 6882, 19:00 UTC): 19 of 20 ids chart (the 20th is `CRYPTOCAP:BTC.D`);
typing "btc" / "bitcoin" / "eth" / "ethereum" / "sol" in Symbol Search and **clicking the row** changes the chart 7
times of 7; `INDEX:BTCUSD` -> BTC-USD, 4,441 daily bars, newest 2026-11-21.

## 2. Crypto data that was wrong or late (ops 6880, 6881, 6883, 6886)

- The Polygon plan carries crypto **end of day only**: 403 on the crypto snapshot, on the same day's grouped daily,
  on every crypto intraday aggregate and on last trade.
- The banked bars are Coinbase's own daily candles (equal to the cent on 81 of 93 coins).
- The outside daily history the data proxy joined onto the bank was **another coin** for ARB, TON, AERO and JUP
  (closes 99% away): TON 0.977 in the bank, 0.0018 on the tape.
- 22 coins had no outside bars, so their tape ended yesterday; the Binance fallback never ran (HTTP 451).
- Crypto had **no intraday bars**: the hourly view of bitcoin was 4,441 daily bars.
- The bank itself stopped on 2026-10-10 for COMP, GALA, JUP, ZK, ZRO (katlin's crypto lane), and the files of AR,
  CFX and VIRTUAL hold no rows.

Fix, in the Cloudflare worker (`cloudflare/workers/justhodl-data-proxy/src/crypto-tape.js`), at no AWS cost:
an outside series joins the banked tape only when it is the same coin (median distance between closes within 3% on the
newest 30 shared days, at least 5); the days after the bank's last completed day - today's forming bar - come from
Coinbase Exchange, then Binance's public market-data host; a bank that stopped up to 290 days ago is carried on to
today; hourly and minute bars come from the venue's public klines after the same check.

After (ops 6883, 18:55 UTC): 84 of 93 tapes carry today's bar (71 from Coinbase); ARB 0.1138, TON 0.9615; behind are
only coins whose bank stopped and no same-coin venue lists them (EOS, GALA, IOTA, JUP, MATIC, RNDR). Intraday
(ops 6886, 19:21 UTC, a burst of 40 requests): 37 of 40 timeframes answered with bars of the asked spacing; a busy
venue now answers "crypto intraday unavailable", uncached, and is asked again.

## 3. The four decisions

| decision | done | proof |
|---|---|---|
| Merge the one-snapshot-document change | history book 1.3.0 live 19:24 UTC | ops 6887: 224 of 225 engines carry the two-layout reader (all 225 packages downloaded); ops 6888: dry run 27,127 documents, first run 129 s, 0 failed, 333 of 333 families read back equal to the nine documents they replace, second pass writes 0 |
| World Bank pass monthly | `justhodl-worldbank-full-weekly`: `rate(7 days)` -> `cron(0 1 1 * ? *)` | ops 6884 dry run, ops 6885 applied and read back; next pass 2026-12-01 01:00 UTC; Lambda about $11.60 -> $2.77 a month |
| open `justhodl-repo` | history files written only when their bytes changed | suite 4 + 16; deployed; baseline ops 6891: 1,658 of 1,698 files rewritten by the 06:10 UTC run; first run on the new code 2026-11-22 06:10 UTC |
| open `justhodl-polygon-bank` | 1.2.1 compares payloads without Polygon's `request_id` | suite 11; deployed; baseline ops 6891: last run on 1.2.0 wrote 889 and skipped 354; first run on 1.2.1 at 22:41 UTC |

The deploy gate was fixed on the way: `ubuntu-latest` now ships Python 3.14 and one engine suite pins an `ast.dump`
digest; the gate runs on 3.12, the Lambda runtime.

## 4. Found behind the bond desk's red test, and fixed (ops 6889, 6890)

The one engine held back from the reader lane, `justhodl-bond-desk`, was red for a real reason. It and the sector
fusion accept the ETF research publication only from compilers with a pinned sha256; `justhodl-etf-true-flows`
3.0.1 / 3.1.0 (2026-11-11 / 11-12) changed two of them and added a fifth (`etf_issuers`) and the pins did not follow.
Since then the bond desk published `flow_status: withheld_ValueError` every day and the sector flow state was not
published at all (last: 2026-11-10).

- ops 6889, read-only, the consumers' own code against the live publication: refused with the old four pins, accepted
  with the five deployed digests (bond desk: 11 cohorts, 33 windows, 138 members, 66 rational sums, all checks passed;
  sector fusion: issuer packet restored, 11 sectors).
- Commit ad0cea2a: `bond_flow_qualification.UPSTREAM` and `aws/shared/sector_fusion_pins.ETF_COMPILERS` name the five
  deployed files; `aws/shared/sector_issuer_native.py` is again the exact copy of `etf_native.py` its test requires.
  Deployed 19:40 UTC; the bond desk now also carries the history-book reader (225 of 225).
- ops 6890, after the deploy: the three packages carry the pins byte for byte; one run of the bond desk ->
  `flow_status: dated_issuer_cohorts`, 11 cohorts; one run of `justhodl-sector-flow-state` -> published 19:44 UTC,
  11 sectors (first publication since 2026-11-10).

All 11 cohorts read `partial_coverage_subtotal`: the issuers' own dated files cover 1 - 4 funds of each cohort, so the
desk shows a coverage subtotal and no cohort total. That is the desk's honest answer, not a fault.

Not fixed, found on the way (ops 6891): the bond desk's world score is `UNAVAILABLE` because four of its five regions
do not pass its own required-input rule - `us` (unqualified_bond_vol_vote), `japan` (unqualified_yen_vote),
`global_funding` (unqualified_funding_vote), `em` (unqualified_flow_forecast). Only `europe` counts (80.1).

## 5. What is still open

- FMP bandwidth add-on (150 GB / 30 days): not answered; the largest cause of really-late feeds.
- The bond desk's world score (section 4): four regions fail its required-input rule.
- First records of the three changed writers: re-run ops 6891 on 2026-11-22 (it counts files by the time of their last write).
- `justhodl-fmp-bank` and `justhodl-worldbank-full` sources are still behind the privacy guard.
- katlin's crypto lane stopped banking COMP, GALA, JUP, ZK, ZRO on 2026-10-10 and runs Tuesday - Saturday.
- 3,374 of 8,259 router rows in the watchlists have no chart route; most are TradingView-only data (FTSE 453, ICE 389,
  USI 143, DJ 87, HSI 52, on-chain 212) plus about 1,000 `ECONOMICS:` ids with no mapped series.
