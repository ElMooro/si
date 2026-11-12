# ETF creations engine — issuer registry (3.1.0, 2026-11-12)

`justhodl-etf-true-flows` computes net issuance at NAV (Δshares × issuer NAV per NYSE reference day) from issuer-published
share counts. Until 3.0.1 only the three issuers with downloadable share histories were covered (iShares workbooks, State
Street NAV-history workbooks, ProShares historical CSVs): 80 of 128 funds. The remaining 48 were "gaps".

Ops 6796–6800 (read-only, 2026-11-12) reviewed every other issuer's own site. The registry in
`aws/lambdas/justhodl-etf-true-flows/source/etf_issuers.py` is the result.

## Sources

| Issuer | Funds | Kind | Original | Figures |
|---|---|---|---|---|
| SPDR Gold Trust | GLD | history | `api.spdrgoldshares.com/api/v1/historical-archive?product=gld…` (XLSX since 2004-11-18) | NAV/share (6 dp), total net asset value, ounces; shares = TNAV/NAV rounded to the share (lands within ~4 shares of the 100,000-share baskets; 10-share quantum) |
| DWS Xtrackers | ASHR | history | `etf.deutsche-am.com/api/pdp/en-us/Export/etf/ASHR/Performance` (XLSX since 2013-11-05) | NAV, AUM, outstanding shares, dividends paid |
| Amplify | HACK | history | Firestore REST listing `funds/HACK/daily` (documents since 2014-11-11), paginated | NAV (4 dp), SharesOutstanding, NetAssets |
| Global X | ARGT BOTZ DRIV FINX LIT TAN URA | snapshot | fund page (server-rendered) | NAV + "ETF Prices as of", exact shares + "Fund Details as of" (both dates must agree); net assets only rounded ("$2.99 billion", kept as text) |
| VanEck | GDX SMH NLR VNM | snapshot | overview page (dated NAV) + `Main/FundDatasetBlock/Get` JSON (exact shares, undated) | the block is bound to the page's NAV date only when its holdings as-of equals that date |
| Invesco | QQQ RSP DBC DBA BKLN | snapshot | `dng-api.invesco.com/cache/v1/accounts/en_US/shareclasses/{T}/prices?…priceListing` | effectiveDate, nav (6 dp), sharesOutstanding, marketValue (net assets); needs `Origin: https://www.invesco.com` |
| USCF | USO UNG | snapshot | ALPS MarketingAPI `dailyprice/{T}` with the bearer token the issuer's site serves to every browser | displaydate, navextended (5 dp), so (shares), navtotal |
| First Trust | CIBR | snapshot | `EtfSummary.aspx?Ticker=CIBR` "Current Fund Data (as of …)" | Closing NAV, Outstanding Shares, Total Net Assets |
| ROBO Global | ROBO | snapshot | fund page `__NUXT_DATA__` NavsComponent | nav, shares_outstanding, net_assets, data_date |

Reviewed limits (reported in the packet as `reviewed_limit`, never as an unexplained gap):
Vanguard ×17 publish outstanding shares at month-end only (`advisors.vanguard.com …/pricing/outstanding-shares`,
effectiveDate = month end); ARK (Cloudflare challenge), Schwab (403 for automated clients), Grayscale (Vercel
checkpoint), Fidelity FBTC (no public fund route found), VXX (an ETN).

## Snapshot ledgers

A snapshot issuer publishes today's figures only, so the engine accrues each day's reviewed snapshot into a retained
ledger: `data/etf-research/ledgers/<sha256>.json` (contract `etf-snapshot-ledger.v1`, immutable, content-addressed)
with a mutable pointer per fund at `data/etf-research/ledgers/<TICKER>/current.json`
(`etf-snapshot-ledger-pointer.v1`). The pointer references go into the run's inputs document, so a replay of the run
reads exactly the ledger the build read.

Rules (`etf_issuers.accrue`): one row per observed as-of date; a row already held for a date is never overwritten — a
later differing observation is appended to `restatements`; every row names the retained original(s) it was parsed
from (`source`: url, acquired_at, evidence key, sha256) and the run clock that observed it (`observed_at`). A fund whose
site rejects the day's parse simply has no row for that day (the 5-day window then reports `incomplete_or_unreviewed`
until five consecutive reference days are held again). Ledger funds never move the packet's common alignment date.
The pointer is advanced only after publication and never behind a longer ledger (`etf_store.advance_pointers`).

## Web keys

Two issuers gate their data API behind a public web key their own site ships to every browser (Amplify's Firestore key
in `amplify-firestore.js`, USCF's ALPS bearer token in `api_key.php`). The store reads the key at run time
(`etf_store.transient_web_key`), sends it with that run's requests only, and never retains it: not in the evidence,
not in the originals' references (the stored URL carries no key), not in logs, not in the repo. A response that echoed
the key would be refused (`credential echoed`). Khalid's own provider keys stay in SSM as before.

## Verifying

- `python3 aws/lambdas/justhodl-etf-true-flows/tests/run_tests.py` — 39 tests, the registry ones on the issuers'
  real originals under `tests/fixtures/issuers/`.
- `aws/ops/STAGED/ops_6801_etf_registry_dry_run.py` — runs the engine's own `collect()` + `build()` against the live
  sources into memory (nothing written) and prints what every parser read.
- The packet (`data/etf-true-flows.json`) carries `coverage`, `snapshot_ledgers` (per fund: rows, latest_date,
  observed_today, status) and `reviewed_limits`.
