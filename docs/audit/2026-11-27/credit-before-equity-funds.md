# Credit Before Equity — held by funds (2026-11-27)

Khalid, 2026-11-26 20:32 ET: "go" — Credit Before Equity first: who holds each bond, how far apart funds mark it, and how
funds are positioned in each company's credit default swaps.

## What was there

- `justhodl-credit-before-equity` 3.0.0: 4,180 companies (1,106 listed) from the corporate bonds desk's fund holdings,
  69 columns a company, a bond-by-bond document, a month-by-month history, the traded CDS of the CDS desk.
- `justhodl-sec-datasets` 1.0.0 (born the same day, ops 6986–6989): the SEC's Form N-PORT quarter read into tables —
  287,615 bonds, loans and asset-backed securities across 13,880 funds, 755 credit swap reference names, 89 indices.
- Nothing joined the two. The board could not say who holds a company's bonds or how they are marked.

## What was added

| Piece | Where | What |
|---|---|---|
| Fund layer 1.0.0 | `aws/lambdas/justhodl-credit-before-equity/source/cbe_funds.py` | arithmetic only: ties fund-held securities and swap names to board companies, adds 13 columns |
| Reader | `lambda_function.fund_layer` | reads `data/sec-fund-holdings.json` and the two tables it names; joined by the handler after `build()` |
| Columns | `data/credit-before-equity.json` → `issuers.cols[69:]` | `f_n f_funds f_face f_value f_px f_gap f_def f_pik f_loans fc_sold fc_bought fc_net fc_funds` |
| Block | `data/credit-before-equity.json` → `funds` | counts, how names were tied, first 100 of each list, flagged securities, every swap name, indices, dealers |
| Document | `data/credit-before-equity-funds.json` | every tied bond and loan (20 columns, no identifier) and the three lists of ties whole |
| Page | `jh-credit-funds.js`, section `s-funds`, the company sheet | tiles, two pictures, six lists, a company's own securities and swaps |

## Dry run on the real warehouse (ops 6990, read-only, five runs)

- Company bonds and loans in the SEC table: 67,426 worth $3.02tn. Tied to the board: 35,569 worth $2.50tn (82.8%).
- Companies reached: 3,502 of 4,180 (95.2% of the value the board reads); 1,069 of 1,106 listed.
- By rule: issuer's own name 30,904 securities / $2.09tn; abbreviations spelled out 2,057 / $231bn; name cut 955 / $90bn;
  co-issuer 245 / $41bn; the board's cut names 1,106 / $36bn; the board's naming rules 302 / $16bn.
- The 747 issuer names tied by a rule were read: the 260 largest and every 12th after. No wrong tie found.
- Swap names: 755; 323 tied to 274 companies; 10 are also dealers' names and enter no company.
- Left untied on purpose: governments, agencies and supranational lenders; issuers none of the bond desk's funds hold
  (Beignet Investor LLC $9.7bn in 378 funds, RD Michigan Prop Owner I $5.5bn in 344 funds); 452 listed companies the
  board does not cover (Western Digital, Ecopetrol, Akamai, Nebius, …). All are listed on the page.

## Limits, said on the page

- Holdings are as of each fund's own report date (this file: 30 Jun 2026), months before the board's bonds.
- A mark is a fund's own valuation per 100 of face, not a trade. A gap is read where five funds or more mark a security.
- Positions not in dollars are counted and not added up. A swap whose side the file does not make clear is not counted
  as sold or bought.
- Some filers write a coupon as a fraction (0.03125 for 3.125%); the table shows what was filed.
- None of it enters the board's spreads, signals, stress rank or chances.

## Proof

- Engine suite 52 tests (7 new, on real rows of the SEC tables), page tests 17 new on the real document, deployment
  suite 1,265, node suite 5,336 — all pass.
- ops 6991: deployed package is the checkout's; one dry run in Lambda; one disclosed one-off publication; every
  company's columns re-added from its rows; every row found in the SEC table; a real browser on the live site.
