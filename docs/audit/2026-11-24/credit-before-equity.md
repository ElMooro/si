# Credit Before Equity: every company the bond market prints, an industry heat map, company CDS

Khalid, 2026-11-24 14:54 ET: "next one: Credit Before Equity ... https://justhodl.ai/credit-before-equity.html i want you to
expand it to cover as many companies as possible and it should have a heatmap that shows industries with biggest corp spread
widening which signal trouble and ones with lowes which signals safety also you i should be able to sort them by industry and
sort them ascending and descending and also add companies CDS credit default swaps"

Four things were asked: (1) as many companies as possible, (2) a heat map of industries by spread widening (most = trouble,
least = safety), (3) sorting by industry, ascending and descending, (4) company CDS.

## What the first engine (1.0.0) was — audit, ops 6939, 19:59 UTC

Read from its own live feed and the warehouse; nothing was changed by the audit.

- **26 hand-listed issuers** (12 banks, 14 companies). Every signal was NONE.
- **Its credit leg was not credit.** It read a structural default model (justhodl-cds-monitor: CreditGrades distance to
  default and a "synthetic CDS") computed from each company's own stock price and volatility. A board that asks whether
  credit moved before the stock cannot answer from a number made out of the stock.
- **It compared each day with the day before**, so its bars (0.05 of distance to default, 5 bp) were almost never met.
- **Its ledger had holes** (2026-10-16 to 10-21, 2026-10-30 to 11-21).
- The warehouse already held real credit that the board did not read: the corporate bonds desk (23,183 bonds of 5,045
  issuers, each with the yield its fund prints) and the CDS desk (203 corporate names with a traded level from DTCC's public
  prints, 358 more that trade without one).

## What changed

### Engine `justhodl-credit-before-equity` 2.0.0 (contract `credit-before-equity.v2`)

The warehouse only; no provider is asked for anything. Same schedule, once a day at 22:35 UTC. Memory 512 MB to 2,048 MB.

- **Credit is what the bond market prints.** Every US-dollar corporate bond the corporate bonds desk holds (the daily holdings
  of the iShares investment-grade, high-yield and emerging-market corporate books) becomes a spread: its yield minus the
  Treasury par yield at the bond's duration-matched maturity. 14,283 bonds on the first run.
- **A company** is the bonds of every legal issuer tied to one listed stock, weighted by the market value the funds hold; an
  issuer with no listed stock is kept as credit only. 2,946 companies on the first run: 1,085 tied to a listed stock (73.8% of
  the market value read), 1,861 credit only. The first engine had 26.
- **A change** is measured on the same bonds at both dates. Reference days: one week and three months from the board's own
  ledger (`credit/cbe-spreads/<holdings day>.json.gz`, one small file a day, every bond's spread); one month and the year's
  start worked back from the bond desk's own yield change until the ledger reaches them.
- **An industry** is the fund file's sector (20 on the board). Its change is the middle company's change, each company
  weighted by market value held, so one distressed name cannot carry an industry.
- **The signal.** Credit moved when a company's spread moved past its own book's change (investment grade, high yield,
  emerging markets) by at least 10 bp and 10% of where it started, its own spread having moved 5 bp or more the same way. The
  stock has not followed while, from the reference day's close to the newest close, it is not 3% or more in the direction
  credit points. Both, and it is a lead.
- **Company CDS.** Every corporate reference entity of the CDS desk with a level, tied to its listed stock where one exists,
  with its one-day, one-week, one-month and three-month change; a traded CDS says whether it confirms or contradicts the
  bonds; a company with no bonds on the board but a liquid CDS is read from the CDS.
- **The stock** is the Polygon EOD bank's close, split-repaired by the leaders engine's own rule (the same file, byte for
  byte).
- **The first engine's model is kept** for its 26 names, with every field it published, and is said to be a model.
- **Guards.** Under 5,000 bonds with a spread, 1,000 companies or 300 listed ones, or with no Treasury curve for the day,
  nothing is published and the last good document stays. A late bond desk, an unreadable CDS desk or model, or missing price
  files are said in `degraded`.
- **How names are tied** (`cbe_names.py`). A wrong tie would cross one company's credit with another company's stock, so the
  ladder is short: the exact name once legal forms are set aside; the same once HOLDINGS / GROUP / PARTNERS are set aside; a
  name the fund file cut at 34 characters that begins exactly one company's name; the company's name followed only by
  financing-vehicle words; and a reviewed list of 620 names (subsidiaries that borrow in their own name). A rule never ties a
  bank-like issuer to a non-financial company, nor a foreign issuer to an American company of the same name. Every tie made
  by a rule is published in the document (`match.by_rule`, 78 on the first run) and listed on the page.

### The page — `credit-before-equity.html`, `jh-credit-board.js`

- **Industry heat map.** One tile an industry, sized by the market value of its bonds, coloured by its spread change: red
  widening (trouble), green tightening (safety). Since: 1W / 1M / 3M / YTD. Book: all / investment grade / high yield /
  emerging. Colour by: change in bp, change in %, against the market, spread level. A second palette (orange / blue).
- **Two ranked lists** under it: widening most (trouble) and widening least or tightening (safety).
- **A tile opens the industry**: its middle change, how many of its companies widened, the six that widened most and the six
  that tightened most, and one click to see all of them in the table.
- **Leads**: credit worse and the stock has not fallen; credit better and the stock has not risen.
- **Every company in one table**, sortable by any of 15 columns either way: click a header, or use the Sort by list and the
  Ascending / Descending button. Sort by industry both ways, or group by industry. Filter by industry, listed stocks only,
  leads, names with a traded CDS, distressed; search by company or ticker.
- **Company CDS table**, sortable either way by any column, by sector, liquid names only.
- **The previous view** (the 26-name model board, as it was) is one click away at the top right, and one click back.
- **Evidence status** kept: it reconciles the document with itself (8 checks), says a count the engine did not publish is
  unknown rather than zero, and never claims the sources are fresh.

## Decisions taken for you

1. **No whole-market default model.** Companies with no bonds in the scanned funds and no traded CDS are not on the board. A
   model could cover them, and it would be computed from the stock - the thing the board is meant to be compared with.
2. **US-dollar bonds only.** Euro and other-currency bonds (7,557), floating-rate notes and convertibles (934) are left out:
   their spread needs another curve or is not a credit spread.
3. **Bonds under a year to run are left out** (189), as are yields the fund file prints at its limit (48) or at zero or less
   (2): none is a credit reading.
4. **A conglomerate's subsidiary is not tied to the parent's stock** where the stock does not answer to that business (BNSF,
   PacifiCorp, MidAmerican, Oncor under Berkshire; Sands China and Wynn Macau, which have their own listing elsewhere).
5. **A CDS level past 5,000 bp is shown in points upfront and not ranked** (one name today: Virgin Media Finance, which the
   CDS desk prints as 36,103 bp from a 58.8-point upfront quote).
6. **The board's ledger was filled from the bond desk's stored versions** (ops 6943): the bucket still held 13 earlier
   documents of the bond desk (2026-11-05 to 11-23), so the one-week comparison is real from the first run instead of
   appearing in a week. Three months appears in January.
7. **One run by hand** at first light, outside the 22:35 UTC schedule (ops 6945): one dry run inside Lambda and one
   publication. The schedule is untouched.

## Found, and not changed

- **The factor book does not hold Energy Transfer, Sunoco or USA Compression**, though all three trade and the price bank
  has their bars. The engine ties them from a short list of its own, and only while the price bank has a current bar.
  Equity Residential is in neither.
- **The factor book types Diversified Healthcare Trust as a fund**, so it is not a listed company to any engine.
- **The stock screener labels this board's record** "synthetic CDS / structural default model; not a traded CDS quote"
  (`aws/shared/equity_donor_inputs.py`). For the 26 model names that is still what the model fields are; for the other
  companies the record now carries real spreads. Changing that one line redeploys eight engines, so it was left for your word.
- **`justhodl-invest` still reads the model's day-over-day change** (`d_distance_to_default`, weight 0.06) and so reads
  nothing for the 1,060 companies that have a real spread and no model.
- **The CDS desk prints some levels that are hard to believe**: AT&T at 7 bp, Goodyear down 161 bp in a month while its bonds
  widened 260 bp, Ford's one-day change larger than its level. The board shows them as the desk prints them and reads a CDS
  only when it is liquid.
- **Paramount Skydance and Warner Bros. Discovery** stopped printing in the price bank on 2026-10-05 and are in no book under
  either name; $890M of their bonds are on the board as credit only.
- **The old page linked `/credit-stress.html`, which does not exist.** The link now goes to the Credit Desk.

## Proof

- **Audit**, ops 6939 (19:59 UTC) and the input cut, ops 6940 (20:04 UTC): read-only.
- **Dry run on the real warehouse**, ops 6942 (20:45 UTC, read-only): the engine's build on the runner with its writes left
  out; four companies' spreads worked again from the raw bonds and the curve agree to the tenth of a basis point (AAPL 57.9,
  CHTR 279.8, JPM 84.2, WHR 652.4); three industries' changes agree; ten reconciliation checks pass.
- **Ledger**, ops 6943 (20:35 UTC): 13 holdings days written, each read back the same.
- **Tests**: the engine's 26 offline tests on a cut of the real warehouse the runner took (the engine reproduces what the
  runner published from it, figure for figure); 23 page tests on the same document; 6 deployment tests; the repository's
  suites (5,238 node tests, 1,245 deployment tests).

## To take it back

Revert the commit that deployed 2.0.0: the engine and the page return to 1.0.0 and the next 22:35 UTC run publishes the 26-name
document again. The ledger under `credit/cbe-spreads/` is small (184 KB a day) and can stay; nothing else reads it.
