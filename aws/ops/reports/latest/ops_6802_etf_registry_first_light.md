# ops 6802 - etf-true-flows 3.1.0 first light (issuer registry) 2026-11-12T17:40+00:00

**Status:** success  
**Duration:** 195.9s  
**Finished:** 2026-11-12T17:43:24+00:00  

## Data

| fails | gaps | pointers | version |
|---|---|---|---|
| [] | 44 | 20 | 3.1.0 |

## Log
- `17:40:08` function: code last modified 2026-11-12T17:38:53.000+0000, sha NG1NMnFuv1um, timeout 900s, memory 1536
- `17:40:09` before: version 3.0.1 generated 2026-11-12T15:45:58.949800+00:00 gaps 48
- `17:43:17` invoke: status 200, function error None, 189s
- `17:43:17` result: {"published": true, "generated_at": "2026-11-12T17:41:11.592834+00:00", "ledgers_moved": 20, "coverage": {"issuer_histories": 3, "ledgers_accruing": 20, "reviewed_limits": 24, "snapshot_ledgers": 20, "workbook_histories": 81}, "history_shards": 125, "quality": {"aligned_five_observation_estimates": 84, "all_funds_covered": false, "call": null, "calls_eligible": false, "configured_funds": 128, "execution_eligible": false, "native_histories": 104, "native_observations": 441147, "sizing_eligible": false, "status": "partial"}}
- `17:43:17` source status codes (386): {"amplify_HACK_2026-11-12": "HTTP_404"}
## 1. publication

- `17:43:18` version 3.1.0 generated 2026-11-12T17:41:11.592834+00:00 | quality {"aligned_five_observation_estimates": 84, "all_funds_covered": false, "call": null, "calls_eligible": false, "configured_funds": 128, "execution_eligible": false, "native_histories": 104, "native_observations": 441147, "sizing_eligible": false, "status": "partial"}
- `17:43:18` coverage {"issuer_histories": 3, "ledgers_accruing": 20, "reviewed_limits": 24, "snapshot_ledgers": 20, "workbook_histories": 81}
- `17:43:18` gaps (44): ARGT ARKK BKLN BND BOTZ CIBR DBA DBC DRIV EDV FBTC FINX GBTC GDX LIT MGK NLR QQQ ROBO RSP SCHD SCHG SCHP SMH TAN UNG URA USO VCIT VCSH VEA VGIT VGLT VGSH VIG VNM VOO VTI VTIP VTV VUG VWO VXX VYM
- `17:43:18` nav_source_counts {"Amplify_NATIVE_HISTORY": 1, "DWS Xtrackers_NATIVE_HISTORY": 1, "First Trust_NATIVE_SNAPSHOT_LEDGER": 1, "Global X_NATIVE_SNAPSHOT_LEDGER": 6, "Invesco_NATIVE_SNAPSHOT_LEDGER": 6, "NO_REVIEWED_HISTORY": 24, "ProShares_NATIVE_HISTORY": 12, "ROBO Global_NATIVE_SNAPSHOT_LEDGER": 1, "SPDR Gold Trust_NATIVE_HISTORY": 1, "State Street_NATIVE_HISTORY": 20, "USCF_NATIVE_SNAPSHOT_LEDGER": 2, "VanEck_NATIVE_SNAPSHOT_LEDGER": 4, "iShares_NATIVE_HISTORY": 49}
- `17:43:18`   ASHR  retained_native_history          5d 0.0 20d -7620000.0 obs 3273 first 2013-11-05 latest 2026-11-11 quality recent_source_check
- `17:43:18`   GLD   retained_native_history          5d 77991728.7 20d -1667934329.9 obs 5529 first 2004-11-18 latest 2026-11-11 quality recent_source_check
- `17:43:18`   HACK  retained_native_history          5d 28479205.0 20d 66574090.0 obs 39 first 2026-09-18 latest 2026-11-11 quality recent_source_check
- `17:43:18`   ARGT  retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 73.7 shares 7464975.0
- `17:43:18`   BKLN  retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 20.554179 shares 282000000.0
- `17:43:18`   BOTZ  retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 32.2 shares 92970000.0
- `17:43:18`   CIBR  retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 96.2 shares 173600002.0
- `17:43:18`   DBA   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 26.343531 shares 39150040.0
- `17:43:18`   DBC   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 30.881476 shares 56000040.0
- `17:43:18`   DRIV  retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 29.65 shares 10230002.0
- `17:43:18`   FINX  retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 24.67 shares 6200000.0
- `17:43:18`   GDX   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 85.47 shares 309552500.0
- `17:43:18`   LIT   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 61.15 shares 18144628.0
- `17:43:18`   NLR   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 92.4 shares 32816632.0
- `17:43:18`   QQQ   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 690.234086 shares 686450000.0
- `17:43:18`   ROBO  retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 73.87 shares 25125000.0
- `17:43:18`   RSP   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 222.651547 shares 474732663.0
- `17:43:18`   SMH   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 507.09 shares 129641874.0
- `17:43:18`   TAN   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 44.01584 shares 25208000.0
- `17:43:18`   UNG   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 10.11389 shares 59346103.0
- `17:43:18`   URA   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 33.66 shares 139011666.0
- `17:43:18`   USO   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 141.92213 shares 14423603.0
- `17:43:18`   VNM   retained_native_snapshot_ledger  ledger rows 1 latest 2026-11-11 observed_today 2026-11-11 status accruing nav 17.26 shares 26950000.0
- `17:43:18` reviewed limits (24): {"ARKK": "issuer_site_bot_challenge", "BND": "issuer_publishes_month_end_shares_only", "EDV": "issuer_publishes_month_end_shares_only", "FBTC": "no_issuer_route_reviewed", "GBTC": "issuer_site_bot_challenge", "MGK": "issuer_publishes_month_end_shares_only", "SCHD": "issuer_site_refuses_automated_clients", "SCHG": "issuer_site_refuses_automated_clients", "SCHP": "issuer_site_refuses_automated_clients", "VCIT": "issuer_publishes_month_end_shares_only", "VCSH": "issuer_publishes_month_end_shares_only", "VEA": "issuer_publishes_month_end_shares_only", "VGIT": "issuer_publishes_month_end_shares_only", "VGLT": "issuer_publishes_month_end_shares_only", "VGSH": "issuer_publishes_month_end_shares_only", "VIG": "issuer_publishes_month_end_shares_only", "VOO": "issuer_publishes_month_end_shares_only", "VTI": "issuer_publishes_month_end_shares_only", "VTIP": "issuer_publishes_month_end_shares_only", "VTV": "issuer_publishes_month_end_shares_only", "VUG": "issuer_publishes_month_end_shares_only", "VWO": "issuer_publishes_month_end_shares_only", "VXX": "exchange_traded_note_without_fund_share_publication", "VYM": "issuer_publishes_month_end_shares_only"}
## 2. ledger pointers and traceability

- `17:43:18`   ARGT  pointer rows 1 latest 2026-11-11 -> ledger 7fa06e1fd97d4fe bytes verified True | newest row 2026-11-11 nav 73.70 shares 7464975 from https://www.globalxetfs.com/funds/argt (564577 B, sha verified True)
- `17:43:18`   BKLN  pointer rows 1 latest 2026-11-11 -> ledger 27cf955b642d844 bytes verified True | newest row 2026-11-11 nav 20.554179 shares 282000000 from https://dng-api.invesco.com/cache/v1/accounts/en_US/sharecla (517 B, sha verified True)
- `17:43:19`   BOTZ  pointer rows 1 latest 2026-11-11 -> ledger d0ad829458e1ef7 bytes verified True | newest row 2026-11-11 nav 32.20 shares 92970000 from https://www.globalxetfs.com/funds/botz (626670 B, sha verified True)
- `17:43:19`   CIBR  pointer rows 1 latest 2026-11-11 -> ledger 2c6a142f37504f4 bytes verified True | newest row 2026-11-11 nav 96.20 shares 173600002 from https://www.ftportfolios.com/Retail/Etf/EtfSummary.aspx?Tick (1340928 B, sha verified True)
- `17:43:19`   DBA   pointer rows 1 latest 2026-11-11 -> ledger bb829cd7696d135 bytes verified True | newest row 2026-11-11 nav 26.343531 shares 39150040 from https://dng-api.invesco.com/cache/v1/accounts/en_US/sharecla (513 B, sha verified True)
- `17:43:20`   DBC   pointer rows 1 latest 2026-11-11 -> ledger 1e93d0a9c821013 bytes verified True | newest row 2026-11-11 nav 30.881476 shares 56000040 from https://dng-api.invesco.com/cache/v1/accounts/en_US/sharecla (514 B, sha verified True)
- `17:43:20`   DRIV  pointer rows 1 latest 2026-11-11 -> ledger 63e58162d6df42d bytes verified True | newest row 2026-11-11 nav 29.65 shares 10230002 from https://www.globalxetfs.com/funds/driv (610437 B, sha verified True)
- `17:43:20`   FINX  pointer rows 1 latest 2026-11-11 -> ledger c74e62e9ac779bc bytes verified True | newest row 2026-11-11 nav 24.67 shares 6200000 from https://www.globalxetfs.com/funds/finx (586383 B, sha verified True)
- `17:43:20`   GDX   pointer rows 1 latest 2026-11-11 -> ledger d6039537b5f4bed bytes verified True | newest row 2026-11-11 nav 85.47 shares 309552500 from https://www.vaneck.com/us/en/investments/gold-miners-etf-gdx (288873 B, sha verified True)
- `17:43:21`   LIT   pointer rows 1 latest 2026-11-11 -> ledger a45dc10c16383e6 bytes verified True | newest row 2026-11-11 nav 61.15 shares 18144628 from https://www.globalxetfs.com/funds/lit (613910 B, sha verified True)
- `17:43:21`   NLR   pointer rows 1 latest 2026-11-11 -> ledger a536d3942a68d40 bytes verified True | newest row 2026-11-11 nav 92.40 shares 32816632 from https://www.vaneck.com/us/en/investments/uranium-nuclear-ene (284439 B, sha verified True)
- `17:43:21`   QQQ   pointer rows 1 latest 2026-11-11 -> ledger e90b938da4bfb51 bytes verified True | newest row 2026-11-11 nav 690.234086 shares 686450000 from https://dng-api.invesco.com/cache/v1/accounts/en_US/sharecla (523 B, sha verified True)
- `17:43:21`   ROBO  pointer rows 1 latest 2026-11-11 -> ledger d1ae157841613b1 bytes verified True | newest row 2026-11-11 nav 73.87 shares 25125000 from https://roboglobaletfs.com/robo (132559 B, sha verified True)
- `17:43:22`   RSP   pointer rows 1 latest 2026-11-11 -> ledger 8503053d863957d bytes verified True | newest row 2026-11-11 nav 222.651547 shares 474732663 from https://dng-api.invesco.com/cache/v1/accounts/en_US/sharecla (524 B, sha verified True)
- `17:43:22`   SMH   pointer rows 1 latest 2026-11-11 -> ledger 6bb4187b2e6c615 bytes verified True | newest row 2026-11-11 nav 507.09 shares 129641874 from https://www.vaneck.com/us/en/investments/semiconductor-etf-s (285008 B, sha verified True)
- `17:43:22`   TAN   pointer rows 1 latest 2026-11-11 -> ledger 38a474cc8bc7cd3 bytes verified True | newest row 2026-11-11 nav 44.01584 shares 25208000 from https://dng-api.invesco.com/cache/v1/accounts/en_US/sharecla (512 B, sha verified True)
- `17:43:23`   UNG   pointer rows 1 latest 2026-11-11 -> ledger 046f67ae0917875 bytes verified True | newest row 2026-11-11 nav 10.11389 shares 59346103 from https://secure.alpsinc.com/MarketingAPI/api/v1/dailyprice/UN (783 B, sha verified True)
- `17:43:23`   URA   pointer rows 1 latest 2026-11-11 -> ledger 66935fc8bbecc58 bytes verified True | newest row 2026-11-11 nav 33.66 shares 139011666 from https://www.globalxetfs.com/funds/ura (609919 B, sha verified True)
- `17:43:23`   USO   pointer rows 1 latest 2026-11-11 -> ledger 20a204bd65a74a5 bytes verified True | newest row 2026-11-11 nav 141.92213 shares 14423603 from https://secure.alpsinc.com/MarketingAPI/api/v1/dailyprice/US (793 B, sha verified True)
- `17:43:23`   VNM   pointer rows 1 latest 2026-11-11 -> ledger 8631a19c83cda00 bytes verified True | newest row 2026-11-11 nav 17.26 shares 26950000 from https://www.vaneck.com/us/en/investments/vietnam-etf-vnm/ove (280621 B, sha verified True)
- `17:43:23` pointers present: 20 of 20
- `17:43:23` replay manifest e2254308e7ae6d8: compilers ['etf_issuers', 'etf_native', 'etf_research', 'etf_universe', 'evidence_store'], output sha ae6ec0c816f6
- `17:43:24` inputs: 149 originals, 0 ledger references, acquisition errors {"amplify_HACK_2026-11-12": "HTTP_404"}
- `17:43:24` web keys absent from every retained reference: True
- `17:43:24` ✅ registry live: 44 gaps (was 48), 20 ledgers accruing, histories for ASHR GLD HACK
