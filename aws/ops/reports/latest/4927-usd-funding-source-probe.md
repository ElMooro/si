# ops 4927 — offshore-USD funding: source-resolution probe (read-only)

**Status:** success  
**Duration:** 14.1s  
**Finished:** 2026-08-20T03:35:19+00:00  

## Data

| bis_objects_banked | duration_s | fred_key_donor | generated | health | layers | lbs_cbs_gli_objects | metrics | status | verdict |
|---|---|---|---|---|---|---|---|---|---|
|  |  |  | ? | 85.1 | 7 |  | 34 |  | FUNCTIONING |
| 28 |  |  |  |  |  | 3 |  |  |  |
|  |  | dollar-strength-agent |  |  |  |  |  |  |  |
|  | 14 |  |  |  |  |  |  | PROBE-COMPLETE |  |

## Log
## A. eurodollar-plumbing.json — live metric inventory

- `03:35:05`   us_core       8  sofr_iorb,sofr99_iorb,on_rrp,tga,effr_iorb,gcf_tri,ofr_repo_depth,mmf_repo_pool
- `03:35:05`   bank_funding  4  cp_ois,cpn_ois,bill_ois,ofr_fsi_funding
- `03:35:05`   credit        2  hy_oas,ig_oas
- `03:35:05`   backstops     4  fed_swaps,fima_repo,fed_repo_srf,ecb_usd_provision
- `03:35:05`   settlement    6  ust_fails,fails_TIPS,fails_Corpor,fails_Agency,fails_Agency,fails_Other 
- `03:35:05`   fx            5  broad_dollar,stablecoin_offshore_usd,foreign_custody,net_due_foreign,xccy_basis
- `03:35:05`   hubs          5  hk_usd_hkd,cnh_cny,cnh_hibor_on,cnh_hibor_3m,jpy
## B. NY Fed Markets reference rates (direct) — TGCR/BGCR + volumes + percentiles

- `03:35:05`   sofr        200    fields=9 rate=3.65 vol=3010 pct=3.72
- `03:35:05`   tgcr        200    fields=9 rate=3.63 vol=1203 pct=3.67
- `03:35:05`   bgcr        200    fields=9 rate=3.63 vol=1234 pct=3.68
- `03:35:06`   effr        200    fields=11 rate=3.63 vol=89 pct=3.69
- `03:35:06`   obfr        200    fields=9 rate=3.63 vol=214 pct=3.69
- `03:35:06`   all_latest  200    fields=7 rate=- vol=- pct=-
- `03:35:06`   sofr_avg    200    fields=7 rate=- vol=- pct=-
## C. BIS — is Locational Banking already in the lake?

- `03:35:06` ✅ BIS catalog: 29 dataflows; 4 banking-relevant
- `03:35:06`     WS_CBS_PUB             Consolidated banking
- `03:35:06`     WS_DEBT_SEC2_PUB       International debt securities (BIS-compiled)
- `03:35:06`     WS_GLI                 Global liquidity indicators
- `03:35:06`     WS_LBS_D_PUB           Locational banking
- `03:35:06`     WS_CBS_PUB.dat.gz                                              3.31 MB  2026-08-06
- `03:35:06`     WS_GLI.dat.gz                                                  0.20 MB  2026-08-07
- `03:35:06`     WS_LBS_D_PUB.dat.gz                                            4.06 MB  2026-08-07
- `03:35:07` ✅ LBS object WS_LBS_D_PUB.dat.gz — 25 cols, 30986 sampled rows, USD rows in sample: 19999
- `03:35:07`     header: FREQ,L_MEASURE,L_POSITION,L_INSTR,L_DENOM,L_CURR_TYPE,L_PARENT_CTY,L_REP_BANK_TYPE,L_REP_CTY,L_CP_SECTOR,L_CP_COUNTRY,L_POS_TYPE,DECIMALS,UNIT_MEASURE,UNIT_MULT,AVAILABILITY,TITLE_GRP,TIME_FORMAT,COLLECTION,ORG_VISIBILITY,TIME_PERIOD,OBS_VALUE,OBS_STATUS,OBS_CONF,OBS_PRE_BREAK
- `03:35:07`     USD  : Q,F,C,A,TO1,F,5J,A,BR,A,5J,R,3,USD,6,K,,,S,E,2003-Q1,1643.983,A,F,
- `03:35:07`     USD  : Q,F,C,A,TO1,F,5J,A,BR,A,5J,R,3,USD,6,K,,,S,E,2003-Q2,559.716,A,F,
- `03:35:13`   v1_lbs_csv         200      300000b  FREQ,L_MEASURE,L_POSITION,L_INSTR,L_DENOM,L_CURR_TYPE,L_PARENT_CTY,L_REP_BANK_TYPE,L_REP_CTY,L_CP_SECTOR,L_CP_
- `03:35:14`   v2_lbs_csv         HTTPError:HTTP Error 404:         0b  
- `03:35:15`   v1_lbs_dsd_avail   200        1594b  <?xml version="1.0" ?><mes:Structure xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:mes="http://w
## D. FRED release-walk — H.8 wholesale funding + Commercial Paper (discovery, not guessing)

- `03:35:15` ✗ FRED /releases -> HTTPError:HTTP Error 429: Too Many Requests
## E. SOFR term structure (free forward-funding proxy)

- `03:35:16`   SOFR30DAYAVG   HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:16`   SOFR90DAYAVG   HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:16`   SOFR180DAYAVG  HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:16`   SOFRINDEX      HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:17`   SOFRVOL        HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:17`   SOFR1          HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:18`   SOFR25         HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:18`   SOFR75         HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:18`   SOFR99         HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:18`   BGCR           HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:18`   TGCR           HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:19`   OBFR           HTTPError:HTTP Error 429: Too Many Requests -           
- `03:35:19`   EFFRVOL        HTTPError:HTTP Error 429: Too Many Requests -           
