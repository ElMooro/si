# ops 6801 - issuer registry dry run with the engine's own code 2026-11-12T17:31+00:00 (read-only)

**Status:** success  
**Duration:** 139.2s  
**Finished:** 2026-11-12T17:33:51+00:00  

## Log
- `17:32:09` collect: 149 originals, 2 errors in 36s; retained 13.9 MB
## 1. registry acquisitions

- `17:32:09`   ARGT  globalx_ARGT           retained   564663 B sha 305c10bc2b93
- `17:32:09`   BKLN  invesco_BKLN           retained      517 B sha e32bc09c6c12
- `17:32:09`   BOTZ  globalx_BOTZ           retained   626713 B sha 4bbfe2c9d320
- `17:32:09`   CIBR  firsttrust_CIBR        retained  1340928 B sha 7eea384b9d9e
- `17:32:09`   DBA   invesco_DBA            retained      513 B sha b2eb8f9a88ab
- `17:32:09`   DBC   invesco_DBC            retained      514 B sha 8ca0afdd9225
- `17:32:09`   DRIV  globalx_DRIV           retained   610437 B sha 03eacdba2893
- `17:32:09`   FINX  globalx_FINX           retained   586844 B sha 45d41ec4ff5d
- `17:32:09`   GDX   vaneck_page_GDX        retained   288875 B sha 274b097eb101
- `17:32:09`   GDX   vaneck_block_GDX       retained    90191 B sha d0dfa04a43f2
- `17:32:09`   LIT   globalx_LIT            retained   613910 B sha 0e75f0a8550c
- `17:32:09`   NLR   vaneck_page_NLR        retained   284439 B sha e77a036ce492
- `17:32:09`   NLR   vaneck_block_NLR       retained    47550 B sha 1207c7bfa513
- `17:32:09`   QQQ   invesco_QQQ            retained      523 B sha d3ab789d103d
- `17:32:09`   ROBO  robo_ROBO              retained   132559 B sha 72c65125e455
- `17:32:09`   RSP   invesco_RSP            retained      524 B sha 4cc6b05a3d20
- `17:32:09`   SMH   vaneck_page_SMH        retained   285008 B sha 58e6e5b1fe41
- `17:32:09`   SMH   vaneck_block_SMH       retained    45792 B sha 8691a4e88c55
- `17:32:09`   TAN   invesco_TAN            retained      512 B sha 74080c5c2c75
- `17:32:09`   UNG   uscf_UNG               retained      783 B sha beaa3428abb5
- `17:32:09`   URA   globalx_URA            retained   610337 B sha b64591da04ec
- `17:32:09`   USO   uscf_USO               retained      793 B sha 21ffa65a2892
- `17:32:09`   VNM   vaneck_page_VNM        retained   280621 B sha 82b23e2eb31f
- `17:32:09`   VNM   vaneck_block_VNM       retained    84827 B sha 343244b1d29d
- `17:32:09`   ASHR  dws_ASHR               retained   194496 B sha a1292c40780b
- `17:32:09`   GLD   spdrgold_GLD           retained   543798 B sha 5b298b6e666b
- `17:32:09`   HACK  40 day documents requested; retained 39, missing ['2026-11-12:HTTP_404']
- `17:32:09`   error amplify_HACK_2026-11-12: HTTP_404
- `17:32:09`   non-registry errors: 0 ([])
## 2. what the parsers read

- `17:32:09`   ARGT  Global X         snapshot 2026-11-11 nav 73.70 shares 7464975 net assets $550.14 million
- `17:32:09`   BKLN  Invesco          snapshot 2026-11-11 nav 20.554179 shares 282000000 net assets 5796278349.35
- `17:32:09`   BOTZ  Global X         snapshot 2026-11-11 nav 32.20 shares 92970000 net assets $2.99 billion
- `17:32:09`   CIBR  First Trust      snapshot 2026-11-11 nav 96.20 shares 173600002 net assets 16700141737
- `17:32:09`   DBA   Invesco          snapshot 2026-11-11 nav 26.343531 shares 39150040 net assets 1031350295.87
- `17:32:09`   DBC   Invesco          snapshot 2026-11-11 nav 30.881476 shares 56000040 net assets 1729363870.54
- `17:32:09`   DRIV  Global X         snapshot 2026-11-11 nav 29.65 shares 10230002 net assets $303.34 million
- `17:32:09`   FINX  Global X         snapshot 2026-11-11 nav 24.67 shares 6200000 net assets $152.98 million
- `17:32:09`   GDX   VanEck           snapshot 2026-11-11 nav 85.47 shares 309552500 net assets $26.46B
- `17:32:09`   LIT   Global X         snapshot 2026-11-11 nav 61.15 shares 18144628 net assets $1.11 billion
- `17:32:09`   NLR   VanEck           snapshot 2026-11-11 nav 92.40 shares 32816632 net assets $3.03B
- `17:32:09`   QQQ   Invesco          snapshot 2026-11-11 nav 690.234086 shares 686450000 net assets 473811188526.53
- `17:32:09`   ROBO  ROBO Global      snapshot 2026-11-11 nav 73.87 shares 25125000 net assets 1855897406
- `17:32:09`   RSP   Invesco          snapshot 2026-11-11 nav 222.651547 shares 474732663 net assets 105699961633.94
- `17:32:09`   SMH   VanEck           snapshot 2026-11-11 nav 507.09 shares 129641874 net assets $65.74B
- `17:32:09`   TAN   Invesco          snapshot 2026-11-11 nav 44.01584 shares 25208000 net assets 1109551282.20
- `17:32:09`   UNG   USCF             snapshot 2026-11-11 nav 10.11389 shares 59346103 net assets 600220057.77
- `17:32:09`   URA   Global X         snapshot 2026-11-11 nav 33.66 shares 139011666 net assets $4.68 billion
- `17:32:09`   USO   USCF             snapshot 2026-11-11 nav 141.92213 shares 14423603 net assets 2047028408.51
- `17:32:09`   VNM   VanEck           snapshot 2026-11-11 nav 17.26 shares 26950000 net assets $465.16M
- `17:32:09`   ASHR  DWS Xtrackers    history 3273 rows 2013-11-05 -> 2026-11-11; last nav 30.7 shares 42350001 net assets 1300074443.19
- `17:32:10`   GLD   SPDR Gold Trust  history 5529 rows 2004-11-18 -> 2026-11-11; last nav 367.823974 shares 365100000 net assets 134292532991.65
- `17:32:10`   HACK  Amplify          history 39 rows 2026-09-18 -> 2026-11-11; last nav 117.4096 shares 28750000 net assets 3375526798.46
## 3. build on the retained originals (no ledgers yet: every snapshot fund starts its ledger today)

- `17:33:01` build: 51s, 125 history/ledger shards, version 3.1.0, quality {"status": "partial", "configured_funds": 128, "native_histories": 104, "aligned_five_observation_estimates": 84, "native_observations": 441147, "all_funds_covered": false, "call": null, "calls_eligible": false, "sizing_eligible": false, "execution_eligible": false}
- `17:33:01` coverage: {"workbook_histories": 81, "issuer_histories": 3, "snapshot_ledgers": 20, "ledgers_accruing": 20, "reviewed_limits": 24}
- `17:33:01` gaps (44): ARGT ARKK BKLN BND BOTZ CIBR DBA DBC DRIV EDV FBTC FINX GBTC GDX LIT MGK NLR QQQ ROBO RSP SCHD SCHG SCHP SMH TAN UNG URA USO VCIT VCSH VEA VGIT VGLT VGSH VIG VNM VOO VTI VTIP VTV VUG VWO VXX VYM
- `17:33:01` nav_source_counts: {"iShares_NATIVE_HISTORY": 49, "Global X_NATIVE_SNAPSHOT_LEDGER": 6, "NO_REVIEWED_HISTORY": 24, "DWS Xtrackers_NATIVE_HISTORY": 1, "State Street_NATIVE_HISTORY": 20, "ProShares_NATIVE_HISTORY": 12, "Invesco_NATIVE_SNAPSHOT_LEDGER": 6, "First Trust_NATIVE_SNAPSHOT_LEDGER": 1, "VanEck_NATIVE_SNAPSHOT_LEDGER": 4, "SPDR Gold Trust_NATIVE_HISTORY": 1, "Amplify_NATIVE_HISTORY": 1, "ROBO Global_NATIVE_SNAPSHOT_LEDGER": 1, "USCF_NATIVE_SNAPSHOT_LEDGER": 2}
- `17:33:01`   ASHR  retained_native_history net_flow_5d 0.0 net_flow_20d -7620000.0 obs 3273 status recent_source_check
- `17:33:01`   GLD   retained_native_history net_flow_5d 77991728.7 net_flow_20d -1667934329.9 obs 5529 status recent_source_check
- `17:33:01`   HACK  retained_native_history net_flow_5d 28479205.0 net_flow_20d 66574090.0 obs 39 status recent_source_check
- `17:33:01`   ARGT  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   BKLN  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   BOTZ  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   CIBR  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "First Trust", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   DBA   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   DBC   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   DRIV  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   FINX  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   GDX   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "VanEck", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   LIT   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   NLR   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "VanEck", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   QQQ   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   ROBO  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "ROBO Global", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   RSP   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   SMH   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "VanEck", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   TAN   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   UNG   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "USCF", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   URA   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   USO   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "USCF", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01`   VNM   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "VanEck", "latest_date": "2026-11-11", "status": "accruing"}
- `17:33:01` reviewed limits (24): {"ARKK": "issuer_site_bot_challenge", "BND": "issuer_publishes_month_end_shares_only", "EDV": "issuer_publishes_month_end_shares_only", "FBTC": "no_issuer_route_reviewed", "GBTC": "issuer_site_bot_challenge", "MGK": "issuer_publishes_month_end_shares_only", "SCHD": "issuer_site_refuses_automated_clients", "SCHG": "issuer_site_refuses_automated_clients", "SCHP": "issuer_site_refuses_automated_clients", "VCIT": "issuer_publishes_month_end_shares_only", "VCSH": "issuer_publishes_month_end_shares_only", "VEA": "issuer_publishes_month_end_shares_only", "VGIT": "issuer_publishes_month_end_shares_only", "VGLT": "issuer_publishes_month_end_shares_only", "VGSH": "issuer_publishes_month_end_shares_only", "VIG": "issuer_publishes_month_end_shares_only", "VOO": "issuer_publishes_month_end_shares_only", "VTI": "issuer_publishes_month_end_shares_only", "VTIP": "issuer_publishes_month_end_shares_only", "VTV": "issuer_publishes_month_end_shares_only", "VUG": "issuer_publishes_month_end_shares_only", "VWO": "issuer_publishes_month_end_shares_only", "VXX": "exchange_traded_note_without_fund_share_publication", "VYM": "issuer_publishes_month_end_shares_only"}
- `17:33:51` replay equal: output True, shards True
- `17:33:51` ✅ dry run complete; nothing written
