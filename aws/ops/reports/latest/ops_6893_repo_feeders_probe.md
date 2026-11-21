# ops 6893 - the repo board's feeders: bank against source (read-only), 2026-11-21T20:12+00:00

**Status:** success  
**Duration:** 28.1s  
**Finished:** 2026-11-21T20:12:58+00:00  

## Data

| bank_behind | bank_current | ofr_files | ofr_newest_write | source_not_on_board | source_series |
|---|---|---|---|---|---|
| 0 | 0 | 442 | 2026-08-06 | 0 | 0 |

## Log
## 1. The OFR bank

- `20:12:30` state written 2026-11-21 02:29 | status COMPLETE-maintaining | as_of 2026-11-21T02:29:38+00:00 | catalog 442 | done 442 | failures 0 | catalog source https://data.financialresearch.gov/v1/metadata/mnemonics | new last run []
- `20:12:31` series files: 442, 3.8 MB | by day of last write: 2026-08-06 442
## 2. The OFR API today

- `20:12:31` /metadata/mnemonics: 200, 10045 bytes, 0.4 s -> 442 mnemonics | not in the bank's catalog: []
- `20:12:32` /series/full?mnemonic=FNYR-SOFR-A: 200, 48852 bytes, 0.6 s | top keys ['FNYR-SOFR-A'] | inner keys ['timeseries', 'metadata'] | newest ('2026-11-19', 2157, 3.91)
- `20:12:33` /series/dataset?dataset=fnyr: 200, 0.3 MB, 1.7 s | top keys NoneType | 0 series | sample None keys [] | newest dates: 
- `20:12:40` /series/dataset?dataset=repo: 200, 2.2 MB, 6.9 s | top keys NoneType | 0 series | sample None keys [] | newest dates: 
- `20:12:43` /series/dataset?dataset=nypd: 200, 0.5 MB, 2.6 s | top keys NoneType | 0 series | sample None keys [] | newest dates: 
- `20:12:43` /series/dataset?dataset=mmf: 200, 0.1 MB, 0.7 s | top keys NoneType | 0 series | sample None keys [] | newest dates: 
- `20:12:46` /series/dataset?dataset=tyld: 200, 0.4 MB, 2.5 s | top keys NoneType | 0 series | sample None keys [] | newest dates: 
## 3. Bank against source, per mnemonic

- `20:12:46` source series 0 | bank current 0 | bank behind 0 ({}) | not banked 0 []
- `20:12:46` primary-dealer mnemonics at the source: 0 still printing ( ...), 0 ended before mid-2022
- `20:12:46` board inventory: 300 series | source series the inventory does not carry: 0 ( ...)
- `20:12:46`   of those, printing in the last 30 days: 
## 4. Hedge-fund catalog: sponsored repo

- `20:12:46` state written 2026-11-21 02:29 | status COMPLETE-maintaining | catalog 497 | refresh_cursor 253
- `20:12:47`   FICC-SPONSORED_REPO_VOL      bank newest 2026-09-18 (file written 2026-10-14) | source 200 newest 2026-10-13 (1640 obs, 0.4 s)
- `20:12:47`   FICC-SPONSORED_REVREPO_VOL   bank newest 2026-09-18 (file written 2026-10-14) | source 200 newest 2026-10-13 (1640 obs, 0.5 s)
- `20:12:48`   /hf/v1/series/dataset?dataset=ficc: 200, 40879 bytes, 0.5 s, 0 series []
## 5. The engine's own runs, 14 days

- `20:12:49` justhodl-ofr-stfm            runs/day 11-13:1 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 | errors 0 | mean duration 5.5 s
- `20:12:50` justhodl-repo                runs/day 11-13:1 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 | errors 0 | mean duration 598.4 s
- `20:12:51` justhodl-repo-market         runs/day 11-13:1 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 | errors 0 | mean duration 10.4 s
- `20:12:53` justhodl-nyfed-markets-full  runs/day 11-13:1 11-14:1 11-15:1 11-16:1 11-17:1 11-18:1 11-19:1 11-20:1 | errors 0 | mean duration 12.2 s
- `20:12:54` justhodl-nyfed-pd            runs/day 11-12:1 11-13:1 11-19:1 11-20:1 | errors 0 | mean duration 2.9 s
## 6. The other feeders

- `20:12:54` haircut index written 2026-08-16 | 81 series | keys ['built_at']
- `20:12:54`   www.newyorkfed.org/medialibrary/media/banking/pdf/tpr_stats_historical.xlsx -> 200, 101158 bytes, 0.6 s, last-modified None | 
- `20:12:55`   www.newyorkfed.org/medialibrary/media/research/banking_research/tri-party-repo/tpr-statistics-hi -> 200, 101158 bytes, 0.5 s, last-modified None | 
- `20:12:56`   www.dtcc.com/charts/daily-total-us-treasury-trade-fails.json -> 404, 0 bytes, 1.0 s, last-modified None | 
- `20:12:57`   www.dtcc.com/-/media/Files/Downloads/Charts/daily-treasury-fails.csv -> 404, 0 bytes, 1.0 s, last-modified None | 
- `20:12:57`   markets.newyorkfed.org/api/pd/list/timeseries.json -> 200, 335616 bytes, 0.4 s, last-modified None | {"pd": { "timeseries": [ { "seriesbreak": "SBN2024", "keyid": "PDPOSMBS-TOT", "description": "Total - Mortgage
- `20:12:57`   markets.newyorkfed.org/api/pd/get/PDSORA-UTSETTOT.json -> 200, 55567 bytes, 0.1 s, last-modified None | {"pd": { "timeseries": [ { "asofdate": "2013-04-03", "keyid": "PDSORA-UTSETTOT", "value": "1535302" } , { "aso
- `20:12:58`   markets.newyorkfed.org/api/pd/latest/SBN2024.json -> 200, 114798 bytes, 0.3 s, last-modified None | {"pd": { "timeseries": [ { "asofdate": "2026-11-11", "keyid": "PDWOTIPSC", "value": "-9341" }, { "asofdate": "
- `20:12:58` data/settlement-fails.json written 2026-11-20 21:30 | {"allocation_pct": null, "as_of": "2026-11-11", "call": null, "calls_eligible": false, "classes": "{6}", "contract": "fr2004-fails-research.v1", "decision_id": "fails-5f972fdbb2ead07899140fa1d7838fbc7c4a40a8e010de685e1c42009b13bad8", "engine": "settlement-fails", "execution_eligible": false, "generated_at": "2026-11-20T21:30:39.667691+00:00", "headline": "{48}", "legacy_context": "{6}", "limitatio
- `20:12:58` ✅ read-only; nothing changed
