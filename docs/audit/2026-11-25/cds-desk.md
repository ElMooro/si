# CDS desk 2.0: the misread coupon, every credit product, sovereigns, listed companies worldwide, and all of it on the page

**Asked (Khalid, 2026-11-25 11:33 CT):** "Yes audit and improve the CDS engine next and improve it also to include more
companies and all asset classes and even sovereign CDS and public companies worldwide. Make sure you use beautiful
visuals for easy disgesting the data and make sure all the engine data is visible on the page iux"

**Where:** https://justhodl.ai/cds.html - engine `justhodl-cds-desk` 2.0.0, documents `data/cds-desk.json`,
`data/cds-desk-history.json` and the new `data/cds-desk-tape.json`.

## The short answer

| | 1.6 (this morning) | 2.0 |
|---|---|---|
| Names with a level | 240 | 477 |
| - sovereigns | 29 | 42 (and 18 more with a reference quote, of 119 known issuers) |
| - U.S. companies | 145 | 237 |
| - companies outside the U.S. | 66 | 165 |
| - subordinated bank and company names | not read | 33 |
| Names that trade but show no level | 252 | 109 |
| Quiet names (printed in the year, too rarely for a level) | dropped | 442 listed |
| Indices | 6 | 12 |
| Products of the credit file read | senior single names and 6 indices | every one: index, options, tranches, mortgage indices, senior, subordinated, and the rest counted by name |
| Companies tied to a listed stock | none | 566 carry a home listing, on 33 exchanges; 304 are tied by the credit board, most with the company's bond spread beside the swap |
| AT&T 5-year CDS | 7.1 bp, and listed twice | 99.5 bp, once (its bonds: 145 bp) |
| Expedia 5-year CDS | 17.6 bp | 86.6 bp (its bonds: 116 bp) |

Nothing here is a call. The desk measures what the public tape printed; it does not say where a spread is going.

## Audit first (read-only: ops 6953, 6954)

The engine reads the files U.S. swap dealers must publish (DTCC, SEC and CFTC files, 806 file days since 2024-09-03).
Most single-name prints carry only an unsigned upfront payment, so each fits two spreads - one above the coupon, one
below - and the engine has to find evidence for one of them. The audit found seven defects.

1. **A coupon read a hundred times too small - the cause of AT&T at 7 bp.** One trading platform writes a 1% coupon as
   `1` and a 5% coupon as `5`; everyone else writes `0.01` and `0.05`. The 1.6 engine read `1` as one basis point. With
   a one-basis-point coupon only one of the two spreads is possible, so the engine took it as certain ("single branch,
   firm") and published it: AT&T 7.1 bp, Expedia 17.6 bp. On other days the same happened to Caterpillar, UnitedHealth,
   Ford, AutoZone, Oracle, Microsoft, Carnival, Amazon, Nvidia, Mexico and Avnet, and each wrong level then became the
   anchor for the days after it. The platform has written coupons that way in 57 to 532 prints a month, every month
   since September 2024 - the misreading was two years old, not six days.
2. **AT&T twice.** The name map kept "AT&T" and "AT & T" as two entities.
3. **Tranches read as index prints.** A tranche of CDX trades at a different level than the index; they were mixed.
4. **404 of 59,587 priced name-days sat far from the name's own level** with nothing repeating them.
5. **A level older than ten days became "no level"** (Spain, the United Kingdom, Germany), though the level was known.
6. **About 750 of 1,350 names in the bank were not on the page at all.**
7. **Most products were ignored:** index options (about $81B in six days), subordinated names, commercial-mortgage
   indices, the Asian, Japanese, Australian and subordinated-financial indices, total return swaps, baskets.

The warehouse already held what was needed to put beside a name (ops 6954): the credit board's company bond spreads,
the Global Sovereign desk's reference CDS quote for 38 countries, and the FMP bank's worldwide list of listed companies
(about 96,000 rows a day).

## Six dry runs before anything was written (ops 6955, and the raw rows in ops 6958)

The whole public tape was measured again six times on the runner, writing nothing. Each run was read name by name
against the one before, and each found what the one before could not. From the third run on the document is built in
memory and held to a gate; only a document that passes can be written.

| Run (UTC) | With a level | What it found |
|---|---|---|
| 1 - 18:01 | 443 | A basket print with a notional of 1e14. Listings matched to namesakes abroad (EDP to EDP Corporation of Tokyo, Continental to a Taipei company, Altice USA's CSC Holdings to a Hong Kong broker). Virgin Media Finance at 36,103 bp. Option "expiries" in 2031. |
| 2 - 18:30 | 446 | Listings clean. History days at 1.8 million bp (Telecom Italia) and 70 million (Hertz): quotes in the wrong unit. Hertz at 49 bp for one day. A high-yield option strike of 0.94 of par read as 9,400 bp. |
| 3 - 18:55 | 448 | First run with the gate; passed. Grifols and NEXT lost their level - which showed why HSBC, Intesa and Munich Re had been without one since run 1: an old level, or the company's bonds, sat between the name's two possible spreads and ended the search. |
| 4 - 19:11 | 459 | HSBC back at 47.9 bp. Hertz still at 49 bp that day. 24 history days at an eighth, or eight times, the name's own level (South Africa at 5,224 bp for two days between 121 and 121; Toyota at the ceiling for a day - the new distressed rule had carried one mis-filed amount). |
| 5 - 19:40 | 453 | Ops 6958 read the raw rows: seven Hertz prints quoted "49" points, two of them legs whose cash is netted against another. Hertz clean; 6 stray days left. But CSX, Northrop Grumman and Quest froze at 56 bp: in late September each carried the same sixteen prints at one upfront - a portfolio booked at one flat price - and a guard added in this run had thrown out CSX's own exactly-confirmed quotes. |
| 6 - 19:59 | 477 | Flat-priced portfolio prints left out of every name; the guard doubts only a loosely confirmed quote. No stray day left. Twenty-one names gained against run 4 (Allianz 46, Shell 34, Siemens 34, TotalEnergies 36, FedEx 37, ConocoPhillips 27 and others); Teva 175 to 37 (a 2.6-year quote at 22 bp the same week agrees); Norfolk Southern 52 to 16 (52 was the flat price). Gate passed. This is the code that was deployed. |

## What changed

### Engine `justhodl-cds-desk` 2.0.0

**The reading (each one a defect a run found, each with a test built on the real case):**

- A coupon is read in the unit that lands on a coupon the market trades with: `1` is 100 bp, `5` is 500 bp, `0.01` is
  100 bp. How the file wrote it is kept and counted on the page.
- One entity, one key. Tranches are their own product.
- A print far from the name's own level that nothing repeats is shown as unconfirmed and is never the level or an
  anchor. A real move is confirmed by the next day.
- A name whose two possible spreads are within a fifth of each other trades at its coupon: the level is their middle,
  shown with both ("at coupon").
- A level 11 to 45 days old stays the name's level, with its date, marked "aged", never counted as liquid.
- **Distressed names are read in points.** Past 10,000 bp a running spread measures only the recovery assumed, so the
  level is carried at 10,000 bp and the page shows points upfront. A print of 40 points or more that a 40% recovery
  cannot reach is carried the same way instead of being dropped (Virgin Media Finance, Hertz, Xerox, Altice USA).
- **A quote that is really points upfront.** A dealer platform files "49" points in the spread field. The print gives
  itself away: the quote is the cash it disseminates plus the accrued coupon. 1.6 read Hertz at 49 bp that day.
- **A leg quoted like the day's points quotes is points too,** even when its own cash is netted against another leg
  (two of the seven Hertz prints: their cash happens to be what a 49 bp spread on a 5% coupon costs, so 1.6 called
  them confirmed quotes).
- **A portfolio booked at one flat price is nobody's level.** The same coupon, expiry and upfront on four or more
  names in one day is left out of every name (1.6 had published Quest at 52 to 56 bp for a week on such prints).
- **A spike that came back was never the level.** Up to three priced days about seven times away from the name's own
  level, after which it prints where it was, are listed as unconfirmed whatever their size.
- **A single print past the model's reach on a healthy name is a mis-filed amount,** not a default: it is carried at
  the ceiling only for a name already past 1,000 bp or when a second print of the day says the same.
- **A quote in the wrong unit.** One platform writes 57 under the decimal notation (570,000 bp) and 0.49 under basis
  points. Such a quote is read in the unit a credit can trade at and believed only next to the name's own level.
- **An old level that cannot pick a branch no longer ends the search.** A firm level months old, or the company's
  bonds, used to be the last word even when it sat between the name's two possible spreads; the name's own newer
  levels and its index are now asked.
- A company's own bonds (the credit board) and a sovereign's reference quote may pick the side, only when the two
  spreads are far apart.
- A notional no trade can have (above $50B on one print) is counted as a print, not as size.

**The coverage:**

- **Subordinated** bank and company names: their own group, priced at 20% recovery, never mixed with the senior name.
- **Twelve index families** (six new: iTraxx Sub Financials, CDX Financials, CDX EM HY, iTraxx Asia ex-Japan, Japan,
  Australia). An index with no print for ten days is shown with its date, not as today's level.
- **Every product counted** by day, 5 days and 30 days: index, options, tranches, senior, subordinated, sovereign, and
  the contracts with no index behind them under the name the file itself gives them.
- **Index options** by strike and expiry for CDX IG, CDX HY, iTraxx Europe and Crossover, with the share struck on the
  weaker-credit side of today's level. The file does not say payer or receiver; the page says so.
- **Commercial-mortgage indices (CMBX)** by rating tier and series, in points upfront.
- **Sovereigns:** 119 known issuers; a country with no print carries the Global Sovereign desk's reference quote,
  marked as a quote, never as a print.
- **Listed companies worldwide:** a company is tied to its stock by the credit board's reviewed tie, else by the same
  name in the worldwide list - home exchange first, and a name whose swaps trade in euros, sterling or francs only to a
  European listing, in yen only to a Japanese one.
- **Quiet names** are listed with their last print instead of dropped.
- **The prints behind every level** (`data/cds-desk-tape.json`, 30 days, up to 400 a name) and each file day as the
  desk read it (`data/warm/cds/prints/`, so a day can be measured again from the warehouse alone).

`{"dry_run": true}` on any action does the same work and writes nothing. Memory 2048 MB, timeout 900 s. Schedule
unchanged: once a day, 04:00 UTC Tuesday to Saturday.

### The page - `cds.html`, `jh-cds-desk.js`, new `jh-cds-views.js`

Everything that was on the 1.6 page is where it was. Added:

- **The credit market at a glance** (top): a card for each index with its year's range; a heat map with one tile a
  sector (a region for sovereigns), sized by names, coloured by the middle name's change - red wider, green tighter -
  for one day, a week, a month or three months, or by percentile or level; a tile opens its names; the ten that widened
  and tightened most; where each group's levels sit.
- **Subordinated** table, beside the U.S. and global ones, with the same chips and sorting.
- **Stock, Its bonds, CDS - bonds** columns on the company tables.
- **Every product of the credit file**, **Options on the indices**, **Commercial-mortgage indices**, **Quiet names**,
  **What the desk repaired, and what it holds back**.
- **Everything the engine published:** every key of the document opens down to the last value; every name downloads
  as one file.
- **A name's window** now ends with everything the desk has on it: level (in points for a distressed name), change,
  its place in its own year, how it trades, its bonds beside it, every figure with what it means, and the prints behind
  the level, sortable, downloadable.
- **The map** draws a reference quote fainter with a broken outline and the sign for "about".
- **Phone:** the page was 545 px wide on a 390 px screen before today (wide tables); tables now scroll inside their
  panel, the heat map is drawn tall at true text size, index cards two across.

Colours were checked with the palette validator on the page's dark surface and live in a file the site build does not
re-skin.

## Decisions taken for you

- **Points, not a running spread, for distressed names.** The alternative was to show 36,000 bp. Carried at 10,000 bp
  for ranking, read in points.
- **A reference quote is not a print.** It is on the map and the table, marked, and never enters a level.
- **Subordinated names at 20% recovery** (the market convention), in their own group.
- **A listing needs the right part of the world**, so a namesake abroad is not the company. One name was reviewed and
  denied by hand (Rogers Group is not Rogers Corporation).
- **The Y / 1-year rule and every 1.6 number that was right are unchanged.** Six names in both documents differ by
  more than a fifth; each is listed under Proof.
- **Identifiers:** the reference obligation's identifier (ISIN / RED code) is never read into anything kept.

## Found, and not changed

- **The tape is what U.S.-regulated dealers print.** A European or Asian company appears only when a U.S. person is on
  the trade, so non-U.S. coverage is thinner than a paid CDS feed's and will stay so on free data.
- **Options:** the public file gives strike, expiry and size, not payer or receiver, nor the index by name (the index
  is read from currency and coupon). 305 of 1,520 option prints in 30 days are on an underlier that cannot be named.
- **"CDS Corp SN" with no seniority** (about 290 prints in five days, large tickets) is counted, not priced: without
  the seniority a recovery would be a guess.
- **The credit board's bond spreads lag the tape** (fund holdings, monthly), so "CDS - bonds" for a name in fast
  distress compares today's swap with last month's bonds. The date is on the page.
- **Three European names still look like the wrong side of the coupon:** ENI (153 bp; its bonds 94), Standard
  Chartered (164 bp; its bonds 125) and Munich Re (173 bp). Each prints only an unsigned upfront that fits about 50 bp
  or about 155 bp, nothing on the tape fixes the side, and the statistical evidence (how the two candidates move with
  the index) chose the wide one. The page marks their sign as inferred and shows the swap wider than the company's own
  bonds in the "CDS - bonds" column, which is the tell. A rule that lets a company's bonds overrule an inferred sign
  would fix these three and would also flip names that really do trade wide of their bonds (DXC, Equinix), so it was
  not added without your word.
- **Six names had a level this morning and have none now:** the duplicate AT&T key (gone on purpose), Exxon (quiet),
  Intesa, Sodexo and Naturgy (their upfronts fit two spreads and nothing picks one - they are listed as "either / or"
  with both), and one of two spellings of Munich Re (the file names it two ways; the other spelling carries the level).
- **Some names in this data are in real distress** (Hertz, Xerox, Altice USA and Virgin Media Finance print at 60 to
  85 points upfront). That is what the tape says; the desk shows it and does not interpret it.

## Proof

- **The write (ops 6956, 20:36-20:52 UTC).** The op first checked that the deployed package is this commit's four
  files with 2048 MB and 900 s, read 806 file days in 718 s, built the document in memory, held it to the gate
  (passed), copied the 1.6 bank, document and history aside, and wrote: 806 day files of prints (34 MB), the bank
  (1,441 series, 597 execution days), the document (generated 20:49:48 UTC), the history (707 names) and the tape of
  prints (688 names, 13,515 prints). Read back from the warehouse: the same document.
- **The Lambda itself (same op).** One invoke of the scheduled path with `dry_run`: 81 s, 396 MB of 2048 MB, the same
  477 names, nothing written. Tomorrow's 04:00 UTC run is not the first time 2.0 runs there.
- **Live, in a real browser on justhodl.ai (ops 6957, 20:53 UTC): 0 failed checks.**
  - Documents as the site serves them: every count is its rows; nobody on the desk twice; AT&T 99.53 bp, Expedia
    86.6 bp; no company under 8 bp and no level past the ceiling; 437 of 437 levels lie inside what that day's own
    prints (the tape document) can mean, worked in the op, not by the engine; every product class adds to its window;
    every drawn option strike is near its index; no listing outside the part of the world its swaps' currency says;
    nothing shaped like an ISIN in the published prints.
  - The page: 12 index cards with the document's levels (iTraxx Japan dated and last); 13 sector tiles each showing the
    middle name's change worked in the op, red wider and green tighter; one-week, sovereign and level views checked the
    same way; the Financials tile opens its 85 names and closes; the ten that widened most are the document's
    (Goodyear +259, Whirlpool +138); AT&T's window shows 99.5 bp beside its bonds at 145 bp, 46 labelled figures and
    its 45 prints, sorted both ways; products for one day, five and thirty; four option ladders with the document's
    bars and expiries; 32 mortgage series; 442 quiet names opened whole and closed; all 29 keys of the document listed
    and the groups branch opened; 44 subordinated rows; 196 of 295 global rows with a listed stock on 26 exchanges; 9
    countries drawn on the map with a reference quote (the other 9 that carry one are too small for the map and are in the table); every 1.6 section still there, in its order; no bad text, no page error.
  - A phone (390 px): heat map drawn tall at true text size, index cards two across, a name's window fits the screen,
    no sideways scroll.
- **Tests:** the engine's suite 44 of 44 (25 new, each built on a real case above); page tests 5,273 of 5,273 (12
  new, on a fixture cut from the real document); deployment suite 1,257 (9 new: one door for writes, no identifier
  kept, every published key has words on the page, the credit board still reads the document).
- **The credit board** reads this document at 22:35 UTC; its test reads the 2.0 document (AT&T once, at 99.5 bp).

## Cost

- **Running:** one run a day as before. On the Lambda's own clock 81 s at 2048 MB - about a quarter of a cent a run,
  under ten cents a month. The day files of prints are 34 MB in all (well under a cent a month); two are rewritten a day.
- **One-offs today, all disclosed:** six dry runs and two probes on the GitHub runner (they read the public DTCC
  files and a few warehouse documents; nothing invoked, nothing written); one write run (806 small writes, three
  copies, four documents); one dry invoke of the Lambda (81 s). Together under one cent of AWS.
- No new schedule, no event, no key: the engine still reads public files and the warehouse.

## To take it back

- The 1.6 bank, document and history were copied to `data/warm/cds/before-2.0/` before the first 2.0 write; nothing was
  deleted. Copy the three back and redeploy the commit before `a32999f9 (that is fed7259b)` to return to 1.6.
- The page: `cds.html` loads `jh-cds-views.js`; removing that one script tag returns the 1.6 page (the desk script
  draws the 1.6 sections from either document).
