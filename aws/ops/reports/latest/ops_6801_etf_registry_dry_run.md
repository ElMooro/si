# ops 6801 - issuer registry dry run with the engine's own code 2026-11-12T17:26+00:00 (read-only)

**Status:** success  
**Duration:** 105.6s  
**Finished:** 2026-11-12T17:28:13+00:00  

## Log
- `17:26:50` collect: 105 originals, 7 errors in 22s; retained 13.6 MB
## 1. registry acquisitions

- `17:26:50`   ARGT  globalx_ARGT           retained   565038 B sha 4388845a9805
- `17:26:50`   BKLN  invesco_BKLN           retained      517 B sha e32bc09c6c12
- `17:26:50`   BOTZ  globalx_BOTZ           retained   626713 B sha de371e81d566
- `17:26:50`   CIBR  firsttrust_CIBR        retained  1340928 B sha 4a72bc073307
- `17:26:50`   DBA   invesco_DBA            retained      513 B sha b2eb8f9a88ab
- `17:26:50`   DBC   invesco_DBC            retained      514 B sha 8ca0afdd9225
- `17:26:50`   DRIV  globalx_DRIV           retained   610437 B sha 03eacdba2893
- `17:26:50`   FINX  globalx_FINX           retained   586383 B sha f245e1438d60
- `17:26:50`   GDX   vaneck_page_GDX        MISSING: HTTP_302
- `17:26:50`   GDX   vaneck_block_GDX       retained    90191 B sha d0dfa04a43f2
- `17:26:50`   LIT   globalx_LIT            retained   613953 B sha b2aa1b7a1dcb
- `17:26:50`   NLR   vaneck_page_NLR        MISSING: HTTP_302
- `17:26:50`   NLR   vaneck_block_NLR       retained    47550 B sha 1207c7bfa513
- `17:26:50`   QQQ   invesco_QQQ            retained      523 B sha d3ab789d103d
- `17:26:50`   ROBO  robo_ROBO              retained   132559 B sha 72c65125e455
- `17:26:50`   RSP   invesco_RSP            retained      524 B sha 4cc6b05a3d20
- `17:26:50`   SMH   vaneck_page_SMH        MISSING: HTTP_302
- `17:26:50`   SMH   vaneck_block_SMH       retained    45792 B sha 8691a4e88c55
- `17:26:50`   TAN   globalx_TAN            MISSING: HTTP_404
- `17:26:50`   UNG   uscf_UNG               retained      783 B sha beaa3428abb5
- `17:26:50`   URA   globalx_URA            retained   609876 B sha ead79aa43b11
- `17:26:50`   USO   uscf_USO               retained      793 B sha 21ffa65a2892
- `17:26:50`   VNM   vaneck_page_VNM        MISSING: HTTP_302
- `17:26:50`   VNM   vaneck_block_VNM       retained    84827 B sha 343244b1d29d
- `17:26:50`   ASHR  dws_ASHR               retained   194495 B sha 1c25b898f360
- `17:26:50`   GLD   spdrgold_GLD           retained   543798 B sha 5b298b6e666b
- `17:26:50`   HACK  amplify_HACK           MISSING: HTTP_403
- `17:26:50`   error amplify_HACK: HTTP_403
- `17:26:50`   error globalx_TAN: HTTP_404
- `17:26:50`   error vaneck_page_GDX: HTTP_302
- `17:26:50`   error vaneck_page_NLR: HTTP_302
- `17:26:50`   error vaneck_page_SMH: HTTP_302
- `17:26:50`   error vaneck_page_VNM: HTTP_302
- `17:26:50`   non-registry errors: 0 ([])
## 2. what the parsers read

- `17:26:50`   ARGT  Global X         snapshot 2026-11-11 nav 73.70 shares 7464975 net assets $550.14 million
- `17:26:50`   BKLN  Invesco          snapshot 2026-11-11 nav 20.554179 shares 282000000 net assets 5796278349.35
- `17:26:50`   BOTZ  Global X         snapshot 2026-11-11 nav 32.20 shares 92970000 net assets $2.99 billion
- `17:26:50`   CIBR  First Trust      snapshot 2026-11-11 nav 96.20 shares 173600002 net assets 16700141737
- `17:26:50`   DBA   Invesco          snapshot 2026-11-11 nav 26.343531 shares 39150040 net assets 1031350295.87
- `17:26:50`   DBC   Invesco          snapshot 2026-11-11 nav 30.881476 shares 56000040 net assets 1729363870.54
- `17:26:50`   DRIV  Global X         snapshot 2026-11-11 nav 29.65 shares 10230002 net assets $303.34 million
- `17:26:50`   FINX  Global X         snapshot 2026-11-11 nav 24.67 shares 6200000 net assets $152.98 million
- `17:26:50` ⚠   GDX   snapshot failed: KeyError 'vaneck_page_GDX'
- `17:26:50`   LIT   Global X         snapshot 2026-11-11 nav 61.15 shares 18144628 net assets $1.11 billion
- `17:26:50` ⚠   NLR   snapshot failed: KeyError 'vaneck_page_NLR'
- `17:26:50`   QQQ   Invesco          snapshot 2026-11-11 nav 690.234086 shares 686450000 net assets 473811188526.53
- `17:26:50`   ROBO  ROBO Global      snapshot 2026-11-11 nav 73.87 shares 25125000 net assets 1855897406
- `17:26:50`   RSP   Invesco          snapshot 2026-11-11 nav 222.651547 shares 474732663 net assets 105699961633.94
- `17:26:50` ⚠   SMH   snapshot failed: KeyError 'vaneck_page_SMH'
- `17:26:50` ⚠   TAN   snapshot failed: KeyError 'globalx_TAN'
- `17:26:50`   UNG   USCF             snapshot 2026-11-11 nav 10.11389 shares 59346103 net assets 600220057.77
- `17:26:50`   URA   Global X         snapshot 2026-11-11 nav 33.66 shares 139011666 net assets $4.68 billion
- `17:26:50`   USO   USCF             snapshot 2026-11-11 nav 141.92213 shares 14423603 net assets 2047028408.51
- `17:26:50` ⚠   VNM   snapshot failed: KeyError 'vaneck_page_VNM'
- `17:26:50`   ASHR  DWS Xtrackers    history 3273 rows 2013-11-05 -> 2026-11-11; last nav 30.7 shares 42350001 net assets 1300074443.19
- `17:26:51`   GLD   SPDR Gold Trust  history 5529 rows 2004-11-18 -> 2026-11-11; last nav 367.823974 shares 365100000 net assets 134292532991.65
- `17:26:51` ⚠   HACK  history failed: KeyError 'amplify_HACK'
## 3. build on the retained originals (no ledgers yet: every snapshot fund starts its ledger today)

- `17:27:33` build: 41s, 114 history/ledger shards, version 3.1.0, quality {"status": "partial", "configured_funds": 128, "native_histories": 98, "aligned_five_observation_estimates": 83, "native_observations": 441103, "all_funds_covered": false, "call": null, "calls_eligible": false, "sizing_eligible": false, "execution_eligible": false}
- `17:27:33` coverage: {"workbook_histories": 81, "issuer_histories": 2, "snapshot_ledgers": 15, "ledgers_accruing": 20, "reviewed_limits": 24}
- `17:27:33` gaps (45): ARGT ARKK BKLN BND BOTZ CIBR DBA DBC DRIV EDV FBTC FINX GBTC GDX HACK LIT MGK NLR QQQ ROBO RSP SCHD SCHG SCHP SMH TAN UNG URA USO VCIT VCSH VEA VGIT VGLT VGSH VIG VNM VOO VTI VTIP VTV VUG VWO VXX VYM
- `17:27:33` nav_source_counts: {"iShares_NATIVE_HISTORY": 49, "Global X_NATIVE_SNAPSHOT_LEDGER": 6, "NO_REVIEWED_HISTORY": 30, "DWS Xtrackers_NATIVE_HISTORY": 1, "State Street_NATIVE_HISTORY": 20, "ProShares_NATIVE_HISTORY": 12, "Invesco_NATIVE_SNAPSHOT_LEDGER": 5, "First Trust_NATIVE_SNAPSHOT_LEDGER": 1, "SPDR Gold Trust_NATIVE_HISTORY": 1, "ROBO Global_NATIVE_SNAPSHOT_LEDGER": 1, "USCF_NATIVE_SNAPSHOT_LEDGER": 2}
- `17:27:33`   ASHR  retained_native_history net_flow_5d 0.0 net_flow_20d -7620000.0 obs 3273 status recent_source_check
- `17:27:33`   GLD   retained_native_history net_flow_5d 77991728.7 net_flow_20d -1667934329.9 obs 5529 status recent_source_check
- `17:27:33`   HACK  no_reviewed_native_history net_flow_5d None net_flow_20d None obs None status unavailable
- `17:27:33`   ARGT  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   BKLN  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   BOTZ  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   CIBR  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "First Trust", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   DBA   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   DBC   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   DRIV  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   FINX  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   GDX   no_reviewed_native_history ledger {"issuer": "VanEck", "rows": 0, "latest_date": null, "status": "awaiting_first_reviewed_snapshot"}
- `17:27:33`   LIT   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   NLR   no_reviewed_native_history ledger {"issuer": "VanEck", "rows": 0, "latest_date": null, "status": "awaiting_first_reviewed_snapshot"}
- `17:27:33`   QQQ   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   ROBO  retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "ROBO Global", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   RSP   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Invesco", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   SMH   no_reviewed_native_history ledger {"issuer": "VanEck", "rows": 0, "latest_date": null, "status": "awaiting_first_reviewed_snapshot"}
- `17:27:33`   TAN   no_reviewed_native_history ledger {"issuer": "Global X", "rows": 0, "latest_date": null, "status": "awaiting_first_reviewed_snapshot"}
- `17:27:33`   UNG   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "USCF", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   URA   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "Global X", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   USO   retained_native_snapshot_ledger ledger {"rows": 1, "issuer": "USCF", "latest_date": "2026-11-11", "status": "accruing"}
- `17:27:33`   VNM   no_reviewed_native_history ledger {"issuer": "VanEck", "rows": 0, "latest_date": null, "status": "awaiting_first_reviewed_snapshot"}
- `17:27:33` reviewed limits (24): {"ARKK": "issuer_site_bot_challenge", "BND": "issuer_publishes_month_end_shares_only", "EDV": "issuer_publishes_month_end_shares_only", "FBTC": "no_issuer_route_reviewed", "GBTC": "issuer_site_bot_challenge", "MGK": "issuer_publishes_month_end_shares_only", "SCHD": "issuer_site_refuses_automated_clients", "SCHG": "issuer_site_refuses_automated_clients", "SCHP": "issuer_site_refuses_automated_clients", "VCIT": "issuer_publishes_month_end_shares_only", "VCSH": "issuer_publishes_month_end_shares_only", "VEA": "issuer_publishes_month_end_shares_only", "VGIT": "issuer_publishes_month_end_shares_only", "VGLT": "issuer_publishes_month_end_shares_only", "VGSH": "issuer_publishes_month_end_shares_only", "VIG": "issuer_publishes_month_end_shares_only", "VOO": "issuer_publishes_month_end_shares_only", "VTI": "issuer_publishes_month_end_shares_only", "VTIP": "issuer_publishes_month_end_shares_only", "VTV": "issuer_publishes_month_end_shares_only", "VUG": "issuer_publishes_month_end_shares_only", "VWO": "issuer_publishes_month_end_shares_only", "VXX": "exchange_traded_note_without_fund_share_publication", "VYM": "issuer_publishes_month_end_shares_only"}
- `17:28:13` replay equal: output True, shards True
- `17:28:13` ⚠ registry funds without a retained observation today: ['GDX', 'NLR', 'SMH', 'TAN', 'VNM', 'HACK']
- `17:28:13` ✅ dry run complete; nothing written
