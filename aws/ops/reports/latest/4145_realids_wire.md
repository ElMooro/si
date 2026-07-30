# ops 4145 — real-id round-2 wire

**Status:** failure  
**Duration:** 436.8s  
**Finished:** 2026-07-30T17:01:50+00:00  

## Error

```
SystemExit: 1
```

## Data

| CBBS | FER | GDPYY | INTR | IRYY | LG | M0 | UR | cbbs_code | feed_err | lg_code | m0_code |
|---|---|---|---|---|---|---|---|---|---|---|---|
|  |  |  |  |  |  |  |  | S121_A_TA_ASEC_CB1SR |  | DCORP_A_ACO_PS | S121_L_MB_CBS |
|  |  |  |  |  |  |  |  |  | None |  |  |
| 144 | 183 | 261 | 46 | 240 | 141 | 143 | 234 |  |  |  |  |

## Log
## A. indicator census + code picks

- `16:55:28`   MFS_DC: 91 monthly indicators; top:
- `16:55:28`      303  DCORP_L_BM
- `16:55:28`      178  ODCORP_A_ACO_NRES
- `16:55:28`      178  ODCORP_A_ACO_PS
- `16:55:28`      178  ODCORP_A_ACO_S12R
- `16:55:28`      178  ODCORP_A_ACO_S1311MIXED
- `16:55:28`      178  ODCORP_A_ACO_S13M1
- `16:55:28`      178  ODCORP_A_F21_ACO_S121
- `16:55:28`      178  ODCORP_A_F2MSOTS_RR_ACO_S121
- `16:55:28`      178  ODCORP_A_OCO_S121
- `16:55:28`      178  ODCORP_L_F22_IBM
- `16:55:28`      178  ODCORP_L_F29_IBM
- `16:55:28`      178  ODCORP_L_F2M_XBM
- `16:55:28`      178  ODCORP_L_F3_IBM
- `16:55:28`      178  ODCORP_L_F3_XBM
- `16:55:55`   MFS_CBS: 58 monthly indicators; top:
- `16:55:55`      177  S121_A_OFAIL_ACO_NRES_FC_CBS
- `16:55:55`      175  S121_L_IL_LT_NRES_CBS
- `16:55:55`      174  S121_A_ACO_NRES_CBS
- `16:55:55`      174  S121_A_ACO_ODCORP_CBS
- `16:55:55`      174  S121_A_ACO_PS_CBS
- `16:55:55`      174  S121_A_ACO_S12R_CBS
- `16:55:55`      174  S121_A_ACO_S1311MIXED_CBS
- `16:55:55`      174  S121_A_ACO_S13M1_CBS
- `16:55:55`      174  S121_A_ACO_S1_Z_CBS
- `16:55:55`      174  S121_L_IMB_LT_ODCORP_CBS
- `16:55:55`      174  S121_L_IMB_LT_S1_Z_CBS
- `16:55:55`      174  S121_L_LT_NRES_CBS
- `16:55:55`      174  S121_L_LT_S1311MIXED_CBS
- `16:55:55`      174  S121_L_OLT_ODCORP_CBS
## B. bulk proofs

- `16:55:56`   BULK MFS_DC/.DCORP_A_ACO_PS.XDC.M -> countries=146 JPN=771655200000000 BRA=8861998144914.38
- `16:55:56`   BULK MFS_CBS/.S121_A_TA_ASEC_CB1SR.XDC.M -> countries=146 JPN=731110200000000 BRA=5027289480000
- `16:55:57`   BULK MFS_CBS/.S121_L_MB_CBS.XDC.M -> countries=146 JPN=582652600000000 BRA=1399128320000
## C. families-feed v1.1 self-patch + deploy

- `16:56:13` ✅   justhodl-families-feed settled at loop 1
## D. vault FAM_RX widen + settle + invoke

- `17:01:50` ✗   justhodl-tradingview never settled
- `17:01:50` ✅   vault fired async — artifact lands ~t+610s; next op reads it
- `17:01:50` ✅   LG bulk >=60
- `17:01:50` ✅   CBBS bulk >=40
- `17:01:50` ✅   M0 bulk >=40
- `17:01:50` ✅   feed v1.1 settled
- `17:01:50` ✅   LG >= 40
- `17:01:50` ✅   CBBS >= 40
- `17:01:50` ✅   M0 >= 40
- `17:01:50` ✗   vault v3.19.0 settled
- `17:01:50` ✗ FAILED: ['vault v3.19.0 settled']
