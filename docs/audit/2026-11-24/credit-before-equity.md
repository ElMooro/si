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
  issuer with no listed stock is kept as credit only. 2,946 companies on the first run: 1,085 tied to a listed stock (73.9% of
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
  financing-vehicle words; and a reviewed list of 623 names (subsidiaries that borrow in their own name; 31 of them are
  names deliberately left untied). A rule never ties a
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
  nothing for the 1,058 companies that have a real spread and no model.
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
- **First light**, ops 6945 (21:16 UTC): the deployed package is this checkout's, byte for byte; one dry run inside Lambda
  (20.8 s, 261 MB of 2,048 MB, nothing written), then one publication: 2,946 companies (1,085 listed), 48 leads (20 credit
  worse with the stock not down, 28 credit better with the stock not up), 203 CDS levels; ten read-back checks pass; the
  ledger holds 15 days; the site serves the document. Both invokes were by hand, outside the 22:35 UTC schedule.
- **Live proof**, ops 6944 (21:38 UTC, read-only, can be run again): **0 failed checks.** Five companies' spreads and their
  one-week changes worked again from the bond desk's raw bonds, the curve and the ledger day agree (AAPL 57.9 bp, +4.4 in the
  week; JPM 84.2, +7.0; CHTR 279.8, +19.1; WHR 652.4, +60.2; XOM 63.7, +4.8); all 201 industry-book-window changes are the
  middle company by value; 15 stock changes are two closes of the price bank; all 203 CDS levels are the CDS desk's. A real
  browser on justhodl.ai: evidence RECONCILED; 20 tiles with the document's figures, Energy (+20.9 bp, widest) drawn
  rgb(242, 54, 69) and Communications (-0.1 bp) rgb(46, 75, 80) - red and green, not the site's amber; the one-week, book
  and measure controls; the table sorted by its header, the Sort by list, the direction button, by industry both ways and
  grouped into 20 industries; the search; Show all (2,946 rows); the CDS table by one-month change and by level both ways;
  the previous view (26 names) and back; a phone with no sideways scroll; no page error. Report and ten pictures:
  `aws/ops/reports/latest/ops_6944_credit_before_equity_live_proof.md`.
- **The schedule's own run**, 22:35:41 UTC: the document the 22:35 rule published, judged by ops 6944 again (22:52 UTC,
  read-only): 2.0.0, not degraded, **0 failed checks**.
- **Tests**: the engine's 26 offline tests on a cut of the real warehouse the runner took (the engine reproduces what the
  runner published from it, figure for figure); 23 page tests on the same document; 6 deployment tests; the repository's
  suites (5,238 node tests, 1,245 deployment tests).

What the board said at first light (bond holdings of 2026-11-23): in the month since 2026-10-30, 19 of 20 industries are
wider; Energy widened most (+20.9 bp) and Communications least (-0.1 bp). In the week since 2026-11-16 all 20 are wider,
from Consumer non-cyclical (+4.9 bp) to Basic industry (+28.8 bp) among the classified industries.

## 2.1.0, the same evening - credit stress and the AI stack

Khalid, 17:51 ET: "ai companies and oracle are under enormous stress why arent they showing. i know re showing high stress
their cds".

### Why they were not showing

They were on the board, and nothing pointed at them.

1. **A lead needs the stock not to have followed.** Oracle's bonds were marked down hard (spread 327.6 bp, +51.6 bp in the
   month, +49.0 past its book) and its stock had already fallen 13% in the month, so the board - correctly, by its own rule -
   called no lead. The page had nowhere else to say "this credit is in trouble".
2. **The heat map shows the middle company of an industry.** Technology's middle company moved +2.8 bp over 196 companies.
   Meta sits in Communications and Amazon in Consumer cyclical; there was no AI group at all.
3. **The stress built over the year, not the month.** The table opened on the month's change over all 2,946 companies.
   Meta +69 bp, Broadcom +63 bp, Nvidia +48 bp, Amazon +40 bp since the year began, and close to flat in the month.

### What was added (beside what was there; nothing renamed, moved or removed)

**Engine 2.1.0**, contract still `credit-before-equity.v2`; every 2.0 key is published and the 25 columns of 2.0 keep their
places, with the new ones after them.

- **A stress rank for every company**, 0 to 100: the average of where it stands *in its own book* (investment grade, high
  yield, emerging markets) on three things - how wide its spread is, how far it has widened since the year began as a share
  of where it started, and how far it widened in the month. It does not look at the stock. 80 or more is "under stress".
  Published with its three parts (`rk_level`, `rk_year`, `rk_month`), and per industry: how many companies are under stress
  and what share of the industry's bond value they hold.
- **The AI stack theme** (`cbe_themes.py`): the layers and tickers of your AI infrastructure stack engine
  (`justhodl-ai-infra-stack`, `LAYERS` - its keys, its own layer names, its seeds; a deployment test holds this copy to that
  list, so a ticker added there is on the credit board the same day), and four layers that engine has no place for:
  hyperscalers (MSFT, GOOGL, AMZN, META, ORCL), server makers (DELL, HPE), the financing vehicles of AI data centers, and
  SoftBank Group. Published as one line for the theme and one a layer.
- **No lead when the two credit markets disagree.** AMD was a "credit better" lead: its bonds tightened 16.9 bp while its
  liquid CDS widened 38.3 bp. Such a company is now no lead and is named under the leads.
- **A window that is not there yet says when it can be.** The 3-month reason read "the ledger began on 2025-12-31", a
  worked-back day. It now reads "one will be from about 2027-01-08".

**Page.** A section *Credit stress* between the heat map and the leads: the AI stack as one line and by layer, and every
company ranked by stress (Show: every company / AI stack; layer; book; bonds held; sortable by every column either way).
Two tiles in the summary (how many listed companies are under stress and the three highest; the AI stack) that jump to it.
On the heat map: *Map of - Industries / AI stack* (one tile a company of the theme, sized by bonds held) and *Colour by -
Stress*. A Stress column in the table of every company and two more choices in its Show list. Under the leads: any lead
that was not called, and where a company goes whose stock already moved. Every section that was there is where it was, in
the order it had; the industry map is still what the board opens on, with the window and measure the browser remembers.

### Decisions taken for you

- **Three parts, averaged, inside the company's own book.** A high-yield spread is not compared with an investment-grade
  one. The month is one of the three so that a name still getting worse ranks above one that has stopped; the cost is that
  a company whose widening happened earlier in the year and has paused ranks in the middle (see the hyperscalers below).
- **The stress list opens on companies with $50M or more of bonds held** by the scanned funds, where the rank rests on more
  than one small bond. The control is on the page (any size, $10M, $50M, $250M, $1B). Opening the AI stack shows all of it.
- **Named in the summary and on the theme's line: the largest borrowers under stress**, not the highest rank - a $7M
  holding that ranks 99 is not the news.
- **AI data center financing vehicles, by name.** In by what the name says (... COMPUTE LLC, ... COMPUTECO, ... DATA
  CENTERS), or by review, each with who stands behind it: QTS (Blackstone); Hut 8's River Bend (`HUT 8 DC`) and Beacon Point
  (`BEACON POINT DC`, $4.25B of notes, June 2026); Fleet Data Centers / Tract Capital in Storey County, Nevada (`PR RNO
  PROPERTY OWNER 1`, $4.585B, May 2026; `SV RNO PROPERTY OWNER 1`, $3.8B, February 2026); the Next Frontier and Fluidstack
  venture in Indiana (`MERIDIAN ARC HOLDCO`, $5.7B, April 2026). Read in the law firms' notices of each offering (Cahill,
  Latham, Cooley).
- **SoftBank Group is in the theme** as "AI backers": its borrowing funds its AI holdings. Say the word and it comes out
  (one line in `cbe_themes.py`).
- **The map of the AI stack opens on the year to date in percent** - where the build-up shows - and keeps that apart from
  the industry map's own window and measure, which the table follows and the browser remembers.

### What the board says (bond holdings of 2026-11-23)

- **Oracle is the most stressed of the 537 companies with $50M or more held: 99.6.** Its spread, 327.6 bp, is the widest of
  the 648 listed investment-grade companies and 3.3 times the middle one (98.2 bp); +149 bp since the year began (+84%),
  +51.6 bp in the month, +21.1 bp in the week. Traded CDS 329.8 bp, +108 bp in three months, at 99.6% of its year's range.
  Stock -13% in the month, -42% this year.
- **The AI stack**: 66 companies (48 listed), $9.3B of bonds held, middle spread 151.9 bp, +37.3 bp since the year began
  (+44.8%), 36 of 48 wider by 5 bp or more. **9 under stress, holding 24.0% of its bond value**: Oracle 99.6, Beacon Point DC
  99.1, PR RNO Property Owner 89.5, Hut 8 DC 89.0, Intel 86.8, SV RNO Property Owner 85.7, CoreWeave 85.6 (spread 1,083 bp,
  CDS 1,136 bp), SoftBank Group 83.0, Galaxy Helios 81.7.
- **The hyperscalers and the chip makers are not under stress by this rank, and the page says why.** Their spreads roughly
  doubled this year - Nvidia +105%, Broadcom +99%, Meta +83%, Amazon +76%, Alphabet +64%, each in the top 3% of its book
  for the year - but from a low start: Amazon 98.6 bp and Alphabet 96.1 bp are at the investment-grade middle, Microsoft
  66.2 bp is under it, and the month was flat or tighter. Ranks: Meta 68.5, Broadcom 59.4, Alphabet 54.0, Nvidia 53.8,
  Amazon 50.8, Microsoft 36.5. The CDS of the five that doubled sit at 92% to 98% of their year's range (Microsoft's at
  74%). Sorting the list by *YTD change %* puts them at the top; the heat map of the theme, which opens on that measure,
  draws them red.

### Found, and not changed

- `ZENITH ARC LLC` (high yield, Technology, $78M held, 861 bp) is probably another AI data center vehicle. I could not
  confirm who stands behind it, so it is not in the theme.
- The factor book ties `Space Exploration Technologies Corp.` to a ticker `SPCX`; it ranks 7th by stress (275.7 bp,
  investment grade) on bonds with no year-start reading. Left as the book has it.
- A company with a very small holding can rank very high on one bond (Beacon Point DC, $7M held, 99.1). The size control
  and the Held column are there for that; nothing is hidden.

## To take it back

Revert the commit that deployed 2.0.0: the engine and the page return to 1.0.0 and the next 22:35 UTC run publishes the 26-name
document again. The ledger under `credit/cbe-spreads/` is small (184 KB a day) and can stay; nothing else reads it.
