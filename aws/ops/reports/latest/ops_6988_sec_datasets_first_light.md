# ops 6988 - justhodl-sec-datasets: birth and first light 2026-11-27T01:05+00:00

**Status:** success  
**Duration:** 484.5s  
**Finished:** 2026-11-27T01:14:01+00:00  

## Data

| failed_checks | raw_files | raw_gb |
|---|---|---|
| 0 | 52 | 2.44 |

## Log
## 1. function + schedule

- `01:05:57` justhodl-sec-datasets: runtime python3.12, memory 3008, timeout 900, storage 8192 MB, state Active, modified 2026-11-27T01:04:54, handler lambda_function.lambda_handler, env {"BANK_BYTES_PER_RUN": "2621440000", "HOLDINGS_KEY": "data/sec-fund-holdings.json", "MONEY_FUNDS_KEY": "data/sec-money-funds.json", "NMFP_PER_RUN": "12", "NPORT_SECONDS": "430", "OUT_PREFIX": "data/warm/sec-datasets/", "RAW_PREFIX": "sec-datasets/raw/", "S3_BUCKET": "justhodl-dashboard-live"}
- `01:05:57` schedule justhodl-sec-datasets-daily: cron(47 22 * * ? *) ENABLED tz UTC target justhodl-sec-datasets
- `01:05:57` a failed run was set to be retried 2 (the default) time(s) by Lambda itself; now 0: a run that fails waits for the next day
- `01:05:57` ✅ function and schedule match config.json (one run a day, no self-retry)
## 2. deployed schedule manifest := committed (additions-only)

- `01:05:58` S3 1044 entries, repo 1045; added [('scheduler', 'justhodl-sec-datasets-daily')] removed 0 changed 0
- `01:05:59` ✅ manifest uploaded (+1), backup backups/schedule-manifest/20261127T010558Z/
## 3. the Lambda's own dry run: nothing fetched, nothing written

- `01:06:03` 4s | version 1.0.0 | advertised {"nport": 28, "nmfp": 100, "bdc": 23, "form13f": 54, "insiders": 83, "formd": 75, "ncen": 33} | already banked {"nport": 0, "nmfp": 0, "bdc": 0, "form13f": 0, "insiders": 0, "formd": 0, "ncen": 0} | waiting to be read {"nport": 0, "nmfp": 0}
- `01:06:03` would bank 52 files (2.44 GB shown): nmfp/20261008-20261106_nmfp.zip, bdc/2026_10_bdc.zip, nport/2026q3_nport.zip, insiders/2026q3_form345.zip, formd/2026q3_d.zip, ncen/2026q3_ncen.zip, form13f/01jun2026-31aug2026_form13f.zip, nmfp/20260909-20261007_nmfp.zip, bdc/2026_09_bdc.zip ...
- `01:06:03` ✅ the warehouse is unchanged by the dry run (0 objects under the engine's prefixes)
- `01:06:03` ✅ the Lambda reached all seven data-set pages on sec.gov
## 4. first light

- `01:06:03` one-off invoke sent (Event, status 202) at 01:06:03 UTC - disclosed: this is the engine's first run, outside its 22:47 UTC schedule
- `01:11:48` finished after 344s wall: {"seconds": 341.2, "banked": 52, "bytes_banked": 2621124194, "months_read": 13, "quarters_read": 1, "published": {"money_funds": "2026-10", "fund_holdings": "2026-06-30"}}
- `01:11:48`   lambda: REPORT RequestId: 5431ec16-05cc-4af5-82e2-323f9f634a7c	Duration: 3100.54 ms	Billed Duration: 3439 ms	Memory Size: 3008 MB	Max Memory Used: 97 MB	Init Duration: 337.53 ms	
XRAY TraceId: 1-6b08d777-14a9064e0dc6bf404d18acf3	SegmentId: e9fca401db37de39	Sampled: true
- `01:11:48`   nport     advertised   28 | banked   4 (1.66 GB) | newest 2026q3_nport.zip | read into tables 1 | waiting 3
- `01:11:48`   nmfp      advertised  100 | banked  14 (0.14 GB) | newest 20261008-20261106_nmfp.zip | read into tables 13 | waiting 1
- `01:11:48`   bdc       advertised   23 | banked  11 (0.19 GB) | newest 2026_10_bdc.zip | read into tables 0 | waiting None
- `01:11:48`   form13f   advertised   54 | banked   4 (0.35 GB) | newest 01jun2026-31aug2026_form13f.zip | read into tables 0 | waiting None
- `01:11:48`   insiders  advertised   83 | banked   4 (0.04 GB) | newest 2026q3_form345.zip | read into tables 0 | waiting None
- `01:11:48`   formd     advertised   75 | banked   9 (0.02 GB) | newest 2026q3_d.zip | read into tables 0 | waiting None
- `01:11:48`   ncen      advertised   33 | banked   6 (0.04 GB) | newest 2026q3_ncen.zip | read into tables 0 | waiting None
- `01:11:48` ✅ the newest file of all seven sets is in the warehouse
- `01:11:48` ✅ a month of money funds and a quarter of holdings were read into tables
## 5. what the warehouse holds

- `01:11:48` raw files: 52, 2.44 GB under sec-datasets/raw/
- `01:11:48` ✅ every recorded file is stored at its recorded size (52 files)
- `01:11:48` ✅ raw files are outside the site's data/ tree
- `01:11:50` 20261008-20261106_nmfp.zip: stored sha256 7647d8e0b761df19... | recomputed 7647d8e0b761df19... | metadata {"sha256": "7647d8e0b761df19...", "source": "sec.gov", "pulled_at": "2026-11-27T01:06:06+00:00", "sec_last_modified": "Mon, 09 Nov 2026 19:46:28 GMT"} | sec.gov now says 10025399 bytes (HTTP 200)
- `01:11:50` ✅ the stored file is byte for byte what was recorded
- `01:11:50` ✅ and the size the SEC serves today
- `01:11:51` a stranger asking the site for a raw file: 404
- `01:11:51` ✅ raw files are not served by the site
- `01:11:51` a stranger asking the bucket itself for a raw file: 403
- `01:11:51` ✅ raw files are not served by the bucket itself
- `01:11:51` a stranger asking the site for data/sec-money-funds.json: 206
- `01:11:52` a stranger asking the site for data/sec-fund-holdings.json: 206
- `01:11:52` a stranger asking the site for data/warm/sec-datasets/manifest.json: 206
## 6. the published tables against a second reading on the runner

- `01:11:53` money funds 2026-10 (27030 bytes): 329 funds, net assets $9061.3bn, repo $2789.4bn to 502 counterparties, unsecured $626.5bn, 22 flow days | history rows 13
- `01:11:53` ✅ the published month equals a second reading of the banked file, figure for figure
- `01:11:53` ✅ its size is the industry's: 329 funds, $9061.3bn
- `01:13:40` second reading of 2026q3_nport.zip on the runner: 106s
- `01:13:42` published: summary.json 0.0 MB, bonds.json.gz 13.2 MB, cds.json.gz 0.0 MB, stock-loan.json.gz 0.5 MB
- `01:13:42` holdings as of 2026-06-30: 13,880 funds, net assets $46075.4bn, 4,883,330 holding rows | 287,615 securities | credit swaps 10583 positions on 755 names, $177.1bn | stock on loan $331.4bn of $35350.5bn
- `01:13:42` ✅ summary equals the second reading
- `01:13:42` ✅ bonds and loans: the same counts {"securities": 287615, "with_identifier": 236824, "placeholder_identifiers": 723, "set_aside_small": 331611, "set_aside_repeated_name": 6, "flag_default": 1997, "flag_arrears": 1088, "flag_pik": 1129}
- `01:13:42` ✅ bonds and loans: the same value, fund counts and marks in total (7856320699008, 1872920, 1010132, 23978822.5)
- `01:13:42` ✅ credit swaps: every name's row is the same (755 names)
- `01:13:42` ✅ stock on loan: the same totals
- `01:13:42` ✅ its size is the industry's
- `01:14:01` published objects scanned: 20 (32.1 MB of names and titles) against 389,039 CUSIPs and 401,564 ISINs of the SEC's file
- `01:14:01` ✅ no CUSIP and no ISIN appears in anything under data/ (0 and 0 found)
- `01:14:01` ✅ first light complete
