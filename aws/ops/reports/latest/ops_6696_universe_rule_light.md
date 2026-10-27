# ops 6696 - universe-builder v4.1.0 with the listing rule: dry run on the bank, then the first real run

**Status:** success  
**Duration:** 30.9s  
**Finished:** 2026-10-27T19:10:09+00:00  

## Log
- `19:09:39` deployed LastModified 2026-10-27T19:08:41.000+0000, CodeSha256 m+fB90bu4azkdH/3
- `19:09:39` live before: 5230 stocks, generated 2026-10-27T18:06:31+00:00, version q70uKlaOPTPfciWurases9xvqMGLHQO7
## 1. dry run, force_bank (no FMP call, nothing written)

- `19:09:51` ✅ would_publish True [] | total 5046 | by bucket {"nano": 946, "micro": 884, "small": 1261, "mid": 1069, "large": 814, "mega": 72, "unknown": 0} | by source {"bank_profile_bulk": 5041, "curated_seed": 5}
- `19:09:51` bank: as_of 2026-10-25T23:13:14+00:00 (age 1.83 d), membership previous_publication, in spans 6670, not members 1457, not equity 172
- `19:09:51` vs live: both 5046 (96.48% of live), only live 184, only bank 0, same bucket 4955; only-live sample ['AEAQW', 'AETR', 'AFGB', 'AFGC', 'AFGD', 'AFGE', 'AIZN', 'ALOVW', 'ANNAW', 'AOMN', 'AQNB', 'ARCIW', 'ASBA', 'ASPSW', 'ASPSZ', 'ATHS', 'AXINR', 'BACCR', 'BCTXL', 'BEPH', 'BEPI', 'BEPJ', 'BIPH', 'BIPI', 'BIVIW', 'BIXIW', 'BMNP', 'BRTMW', 'BUSEP', 'CAQUW', 'CCXIU', 'CCZ', 'CHAD', 'CIMN', 'CIMP', 'CMSA', 'CMSC', 'CMSD', 'CRBD', 'CSHRW']
- `19:09:51`    REPORT RequestId: e82329de-f0d3-4ad1-a55c-74a8bbda8e79 Duration: 10971.08 ms Billed Duration: 11416 ms Memory Size: 512 MB Max Memory Used: 152 MB Init Duration: 444.37 ms 
## 2. the real run

- `19:10:09`    [universe] v4.1.0 starting — exhaustive all-US-listed (bisection + ADRs)
- `19:10:09`    [universe] micro saturated at 1000 (50000000-300000000) → bisect @ 122474487
- `19:10:09`    [universe] nano saturated at 1000 (1000000-50000000) → bisect @ 7071067
- `19:10:09`    [universe] mega ($200000M-): raw=131
- `19:10:09`    [universe] large saturated at 1000 (10000000000-200000000000) → bisect @ 44721359549
- `19:10:09`    [universe] mid saturated at 1000 (2000000000-10000000000) → bisect @ 4472135954
- `19:10:09`    [universe] small saturated at 1000 (300000000-2000000000) → bisect @ 774596669
- `19:10:09`    [universe] large saturated at 1000 (10000000000-44721359549) → bisect @ 21147425268
- `19:10:09`    [universe] mid saturated at 1000 (2000000000-4472135954) → bisect @ 2990697562
- `19:10:09`    [universe] micro saturated at 1000 (50000000-122474487) → bisect @ 78254228
- `19:10:09`    [universe] small saturated at 1000 (300000000-774596669) → bisect @ 482057051
- `19:10:09`    [universe] nano saturated at 1000 (7071067-50000000) → bisect @ 18803014
- `19:10:09`    [universe] nano saturated at 1000 (18803014-50000000) → bisect @ 30661876
- `19:10:09`    [universe] mid saturated at 1000 (4472135954-10000000000) → bisect @ 6687403049
- `19:10:09`    [universe] small saturated at 1000 (774596669-2000000000) → bisect @ 1244665954
- `19:10:09`    [universe] large ($10000M-): raw=1862
- `19:10:09`    [universe] micro saturated at 1000 (122474487-300000000) → bisect @ 191682931
- `19:10:09`    [universe] nano ($1M-): raw=2654
- `19:10:09`    [universe] small ($300M-): raw=3631
- `19:10:09`    [universe] micro ($50M-): raw=2994
- `19:10:09`    [universe] mid ($2000M-): raw=2497
- `19:10:09`    [universe] combined: 13769 unique symbols
- `19:10:09`    [universe] valid: 5051 (filtered industry: 8053, symbol: 5, other: 54, not a company's equity: 606)
- `19:10:09`    [universe] curated seed missing from screener: 5 of 97
- `19:10:09`    [universe] curated seed added: 5
- `19:10:09`    [universe] wrote 1388311b
- `19:10:09`    [universe] by_bucket: {"nano": 959, "micro": 874, "small": 1259, "mid": 1070, "large": 821, "mega": 73, "unknown": 0}
- `19:10:09`    REPORT RequestId: 4d958d82-e87e-499b-a5f7-97776b06ea32 Duration: 17564.50 ms Billed Duration: 17565 ms Memory Size: 512 MB Max Memory Used: 186 MB 
- `19:10:09` ✅ real run: {"total_stocks": 5056, "by_bucket": {"nano": 959, "micro": 874, "small": 1259, "mid": 1070, "large": 821, "mega": 73, "unknown": 0}, "by_source": {"screener": 5051, "curated_seed": 5}, "ranges_failed": 0, "bank_fill": "not_needed", "duration_s": 17.3}
## 3. the new publication vs the one it replaced

- `19:10:09` new: 5056 stocks, generated 2026-10-27T19:10:08+00:00, engine 4.1.0, version 3T.n9ZzkKJzZqEix0mNn7lYHCzDPCGsF | by bucket {"nano": 959, "micro": 874, "small": 1259, "mid": 1070, "large": 821, "mega": 73, "unknown": 0} | by source {"screener": 5051, "curated_seed": 5}
- `19:10:09` screener: rows 13769, ranges failed 0 | bank fill: not_needed | listing rule: applied True, book 2026-10-27T02:50:45.023172+00:00, dropped 606 by reason {"note_by_name": 40, "type_etn": 41, "type_ets": 92, "type_etv": 84, "type_pfd_issuer_sibling": 13, "type_pfd_name": 10, "type_pfd_secondary_ticker": 10, "type_right": 42, "type_sp_issuer_sibling": 30, "type_sp_name": 40, "type_sp_secondary_ticker": 2, "type_unit_issuer_sibling": 7, "type_unit_name": 90, "type_unit_secondary_ticker": 10, "type_warrant": 90, "untyped_by_name_or_ticker": 5}
- `19:10:09`    note_by_name: ['ABXL', 'BEPH', 'BIPH', 'CGABL', 'CMSA', 'CMSD', 'CTGG', 'CTHH', 'DTW', 'GECCI', 'GEGGL', 'GPJA']
- `19:10:09`    type_etn: ['AIQD', 'AIQU', 'AMJB', 'AMUB', 'BDCX', 'BDCZ', 'BERZ', 'BNKU', 'BULZ', 'CARD', 'CARU', 'CEFD']
- `19:10:09`    type_ets: ['ADBU', 'AIYY', 'AMDY', 'AMZY', 'APLY', 'ARMH', 'ASMH', 'BABO', 'BMNZ', 'BPH', 'BRKC', 'BSTP']
- `19:10:09`    type_etv: ['AGQ', 'BAR', 'BAVA', 'BDRY', 'BHYP', 'BITB', 'BITW', 'BNO', 'BOIL', 'BSOL', 'BTC', 'BWET']
- `19:10:09`    type_pfd_issuer_sibling: ['BMNP', 'BUSEP', 'CHAD', 'FGCPP', 'FITB-PA', 'OCCIM', 'SMCIP', 'STRC', 'STRD', 'STRK', 'UMBFO', 'WHLRD']
- `19:10:09`    type_pfd_name: ['CTA-PB', 'EP-PC', 'LILAP', 'OAK-PA', 'OAK-PB', 'SATA', 'SATS', 'SCE-PG', 'SEAL-PB', 'STRF']
- `19:10:09`    type_pfd_secondary_ticker: ['ACP-PA', 'AHL-PE', 'ATH-PA', 'CDR-PB', 'CDR-PC', 'CMS-PB', 'CTA-PA', 'GLOP-PA', 'ICR-PA', 'TRTN-PC']
- `19:10:09`    type_right: ['AESPR', 'AMPGZ', 'APMCR', 'APURR', 'ARCLR', 'AXINR', 'BIDWR', 'CATLR', 'CRACR', 'CRANR', 'DMIIR', 'EMISR']
- `19:10:09`    type_sp_issuer_sibling: ['AFGB', 'AFGC', 'AFGD', 'AFGE', 'APOS', 'AQNB', 'ASBA', 'BEPI', 'BEPJ', 'BNH', 'CCZ', 'CIMN']
- `19:10:09`    type_sp_name: ['AEFC', 'AIZN', 'AOMN', 'ATHS', 'BNJ', 'CMSC', 'CRBD', 'CUBB', 'DTB', 'DTG', 'DTK', 'DUKB']
- `19:10:09`    type_sp_secondary_ticker: ['BIPI', 'PPLC']
- `19:10:09`    type_unit_issuer_sibling: ['CGCFU', 'DNMXU', 'JONEU', 'SHOTU', 'TBCVU', 'WLCOU', 'YICCU']
- `19:10:09`    type_unit_name: ['AACOU', 'AACPU', 'ACAAU', 'ACGCU', 'ADACU', 'AESPU', 'ALISU', 'ALPXU', 'AMACU', 'APACU', 'APMCU', 'ARCIU']
- `19:10:09`    type_unit_secondary_ticker: ['BRTMU', 'CCCTU', 'FISK', 'GHXIU', 'HVMCU', 'NHIVU', 'OGCP', 'TLNCU', 'VHCPU', 'XSLLU']
- `19:10:09`    type_warrant: ['ACGCW', 'ADACW', 'AEAQW', 'ALOVW', 'AMACW', 'ANNAW', 'ARCIW', 'ARCLW', 'ASPSW', 'ASPSZ', 'BCCQW', 'BCTXL']
- `19:10:09`    untyped_by_name_or_ticker: ['ALPXR', 'BACCR', 'ETI-P', 'KOKU', 'NPSFX']
- `19:10:09` membership: removed 174 (of which shown as listing-rule samples 56), added 0
- `19:10:09`    removed (first 60): ['AEAQW', 'AFGB', 'AFGC', 'AFGD', 'AFGE', 'AIZN', 'ALOVW', 'ANNAW', 'AOMN', 'AQNB', 'ARCIW', 'ASBA', 'ASPSW', 'ASPSZ', 'ATHS', 'AXINR', 'BACCR', 'BCTXL', 'BEPH', 'BEPI', 'BEPJ', 'BIPH', 'BIPI', 'BIVIW', 'BIXIW', 'BMNP', 'BRTMW', 'BUSEP', 'CAQUW', 'CCXIU', 'CCZ', 'CHAD', 'CIMN', 'CIMP', 'CMSA', 'CMSC', 'CMSD', 'CRBD', 'CSHRW', 'CTGG', 'CTHH', 'CUBB', 'DAAQW', 'DMIIR', 'DTB', 'DTG', 'DTK', 'DTW', 'DUKB', 'DUKU', 'EAI', 'ELC', 'ENJ', 'ENO', 'ETI-P', 'FACTW', 'FACWW', 'FGN', 'FGSN', 'FISK']
- `19:10:09`    added: []
- `19:10:09` ✅ must be gone: ['TBB', 'SOMN', 'SOJE', 'STRC', 'STRF', 'SMCIP', 'TMUSL', 'PFH'] -> still present []
- `19:10:09` ✅ must stay (when they were live before): ['GWW', 'NWSA', 'NWS', 'ARM', 'ASTS', 'TPC', 'BCPC', 'AAPL', 'MSFT', 'BRK-B', 'JPM', 'MNR'] -> lost []
- `19:10:09` ✅ ops 6696 complete
