# ops 4928 — offshore-USD funding: identifier resolution (read-only)

**Status:** success  
**Duration:** 26.1s  
**Finished:** 2026-08-20T03:42:29+00:00  

## Data

| duration_s | fred_calls | fred_donor | latest_usd_period | modified | size_mb | status | truncated | usd_denominated_rows |
|---|---|---|---|---|---|---|---|---|
|  |  |  |  | 2026-08-07 | 4.06 |  | True |  |
|  |  |  | 2026-Q1 |  |  |  |  | 2517 |
|  |  | dollar-strength-agent |  |  |  |  |  |  |
|  | 18 |  |  |  |  |  |  |  |
| 26 |  |  |  |  |  | RESOLVED |  |  |

## Log
## A. BIS WS_LBS_D_PUB — the banked object, parsed properly

- `03:42:05` ✅ gunzipped: 648423 rows, 25 columns
- `03:42:06`   L_DENOM      TO1:266047, CHF:134714, TO3:129525, JPY:53287, UN9:37987, EUR:14931, GBP:9414, USD:2517
- `03:42:06`   L_POSITION   C:326252, L:322170
- `03:42:06`   L_MEASURE    F:370242, S:156597, G:76678, B:44905
- `03:42:06`   L_INSTR      G:397678, A:210299, B:38701, D:1744
- `03:42:06`   L_CP_SECTOR  A:214093, N:146994, X:71349, B:57619, P:34503, U:30867, C:22726, K:19500
- `03:42:06`   L_REP_CTY    5A:434352, CH:61592, DK:57666, SE:28724, IT:15868, JE:8876, GR:8808, NL:6087
- `03:42:06`   USD row: Q,S,L,A,USD,F,BE,A,5A,A,5J,R,3,USD,6,K,,,E,E,1994-Q2,20.0,A,F,
- `03:42:06`   USD row: Q,S,L,A,USD,F,BE,A,5A,A,5J,R,3,USD,6,K,,,E,E,1994-Q3,75.0,A,F,
- `03:42:06`   USD row: Q,S,L,A,USD,F,BE,A,5A,A,5J,R,3,USD,6,K,,,E,E,1994-Q4,699.0,A,F,
- `03:42:06`   USD row: Q,S,L,A,USD,F,BE,A,5A,A,5J,R,3,USD,6,K,,,E,E,1995-Q1,10147.0,A,F,
- `03:42:07`   BIS L_DENOM allowed: CHF,EUR,GBP,JPY,TO1,TO3,UN9,USD
- `03:42:08`   BIS L_POSITION allowed: C,L
- `03:42:08`   BIS L_MEASURE allowed: B,F,G,S
## B. FRED — paced release walk (H.8 + Commercial Paper)

- `03:42:09` ✅ releases=331  H.8=22  CP=86
- `03:42:10` ✅ H8 release 22: 1000 series -> 105 matches
- `03:42:10`     DPSACBW027SBOG           W   Bil. of U.S. $ end=2026-08-05 Deposits, All Commercial Banks
- `03:42:10`     DPSACBM027NBOG           M   Bil. of U.S. $ end=2026-07-01 Deposits, All Commercial Banks
- `03:42:10`     DPSACBW027NBOG           W   Bil. of U.S. $ end=2026-08-05 Deposits, All Commercial Banks
- `03:42:10`     DPSACBM027SBOG           M   Bil. of U.S. $ end=2026-07-01 Deposits, All Commercial Banks
- `03:42:10`     H8B1058NCBCMG            M   % Chg. at Annual Rate end=2026-07-01 Deposits, All Commercial Banks
- `03:42:10`     ODSACBW027SBOG           W   Bil. of U.S. $ end=2026-08-05 Other Deposits, All Commercial Banks
- `03:42:10`     DPSACBQ158SBOG           Q   % Chg. at Annual Rate end=2026-04-01 Deposits, All Commercial Banks
- `03:42:10`     LTDACBM027NBOG           M   Bil. of U.S. $ end=2026-07-01 Large Time Deposits, All Commercial Banks
- `03:42:10`     H8B3094NCBA              W   Mil. of U.S. $ end=2026-08-05 Borrowings, All Commercial Banks
- `03:42:10`     NDFACBW027SBOG           W   Bil. of U.S. $ end=2026-08-05 Net Due to Related Foreign Offices, All Commercial Banks
- `03:42:10`     LTDACBW027SBOG           W   Bil. of U.S. $ end=2026-08-05 Large Time Deposits, All Commercial Banks
- `03:42:10`     H8B3094NCBD              W   Mil. of U.S. $ end=2026-08-05 Borrowings, All Commercial Banks
- `03:42:10`     LTDACBM027SBOG           M   Bil. of U.S. $ end=2026-07-01 Large Time Deposits, All Commercial Banks
- `03:42:10`     NDFFRIM027SBOG           M   Bil. of U.S. $ end=2026-07-01 Net Due to Related Foreign Offices, Foreign-Related Institutio
- `03:42:10`     H8B3094NSMA              W   Mil. of U.S. $ end=2026-08-05 Borrowings, Small Domestically Chartered Commercial Banks
- `03:42:10`     H8B1058NCBCAG            A   % Chg. at Annual Rate end=2025-01-01 Deposits, All Commercial Banks
- `03:42:10`     H8B1072NCBCMG            M   % Chg. at Annual Rate end=2026-07-01 Large Time Deposits, All Commercial Banks
- `03:42:10`     ODSACBW027NBOG           W   Bil. of U.S. $ end=2026-08-05 Other Deposits, All Commercial Banks
- `03:42:10`     H8B3094NLGA              W   Mil. of U.S. $ end=2026-08-05 Borrowings, Large Domestically Chartered Commercial Banks
- `03:42:10`     LTDACBW027NBOG           W   Bil. of U.S. $ end=2026-08-05 Large Time Deposits, All Commercial Banks
- `03:42:10`     H8B1072NDMCMG            M   % Chg. at Annual Rate end=2026-07-01 Large Time Deposits, Domestically Chartered Commercial Banks
- `03:42:10`     LTDSCBW027SBOG           W   Bil. of U.S. $ end=2026-08-05 Large Time Deposits, Small Domestically Chartered Commercial B
- `03:42:11` ✅ CP release 86: 202 series -> 184 matches
- `03:42:11`     RIFSPPFAAD90NB           D   %         end=2026-08-18 90-Day AA Financial Commercial Paper Interest Rate
- `03:42:11`     COMPOUT                  W   Bil. of $ end=2026-08-12 Commercial Paper Outstanding
- `03:42:11`     ABCOMP                   W   Bil. of $ end=2026-08-12 Asset-Backed Commercial Paper Outstanding
- `03:42:11`     RIFSPPAAAD90NB           D   %         end=2026-08-18 90-Day AA Asset-Backed Commercial Paper Interest Rate
- `03:42:11`     RIFSPPNA2P2D90NB         D   %         end=2026-08-10 90-Day A2/P2 Nonfinancial Commercial Paper Interest Rate
- `03:42:11`     RIFSPPNA2P2D30NB         D   %         end=2026-08-18 30-Day A2/P2 Nonfinancial Commercial Paper Interest Rate
- `03:42:11`     RIFSPPNAAD30NB           D   %         end=2026-08-18 30-Day AA Nonfinancial Commercial Paper Interest Rate
- `03:42:11`     RIFSPPNAAD90NB           D   %         end=2026-08-18 90-Day AA Nonfinancial Commercial Paper Interest Rate
- `03:42:11`     FINCPN                   W   Bil. of $ end=2026-08-12 Financial Commercial Paper Outstanding
- `03:42:11`     RIFSPPFAAD07NB           D   %         end=2026-08-14 7-Day AA Financial Commercial Paper Interest Rate
- `03:42:11`     RIFSPPNA2P2D01NB         D   %         end=2026-08-18 Overnight A2/P2 Nonfinancial Commercial Paper Interest Rate
- `03:42:11`     RIFSPPAAAD30NB           D   %         end=2026-08-18 30-Day AA Asset-Backed Commercial Paper Interest Rate
- `03:42:11`     RIFSPPFAAD01NB           D   %         end=2026-08-18 Overnight AA Financial Commercial Paper Interest Rate
- `03:42:11`     COMPAPER                 W   Bil. of $ end=2026-08-12 Nonfinancial Commercial Paper Outstanding
- `03:42:11`     RIFSPPNAAD01NB           D   %         end=2026-08-18 Overnight AA Nonfinancial Commercial Paper Interest Rate
- `03:42:11`     RIFSPPNA2P2D07NB         D   %         end=2026-08-18 7-Day A2/P2 Nonfinancial Commercial Paper Interest Rate
- `03:42:11`     RIFSPPFAAD30NB           D   %         end=2026-08-12 30-Day AA Financial Commercial Paper Interest Rate
- `03:42:11`     FINCP                    W   Bil. of $ end=2026-08-12 Financial Commercial Paper Outstanding
- `03:42:11`     DTBSPCKM                 M   Mil. of $ end=2026-07-01 Commercial Paper Outstanding
- `03:42:11`     DTBSPCKANM               M   Mil. of $ end=2026-07-01 Asset-Backed Commercial Paper Outstanding
- `03:42:11`     COMPUTN                  W   Bil. of $ end=2026-08-12 Commercial Paper Outstanding
- `03:42:11`     NFINCP                   W   Bil. of $ end=2026-08-12 Nonfinancial Commercial Paper Outstanding
## C. SOFR term structure + rate-volume series

- `03:42:12`   SOFR30DAYAVG   LIVE  end=2026-08-19  %        30-Day Average SOFR
- `03:42:13`   SOFR90DAYAVG   LIVE  end=2026-08-19  %        90-Day Average SOFR
- `03:42:15`   SOFR180DAYAVG  LIVE  end=2026-08-19  %        180-Day Average SOFR
- `03:42:16`   SOFRINDEX      LIVE  end=2026-08-19  Index Apr 2, 2018 = 1 SOFR Index
- `03:42:17`   SOFRVOL        LIVE  end=2026-08-18  Bil. of U.S. $ Secured Overnight Financing Volume
- `03:42:18`   SOFR1          LIVE  end=2026-08-18  %        Secured Overnight Financing Rate: 1st Percentile
- `03:42:20`   SOFR25         LIVE  end=2026-08-18  %        Secured Overnight Financing Rate: 25th Percentile
- `03:42:21`   SOFR75         LIVE  end=2026-08-18  %        Secured Overnight Financing Rate: 75th Percentile
- `03:42:22`   SOFR99         LIVE  end=2026-08-18  %        Secured Overnight Financing Rate: 99th Percentile
- `03:42:23`   BGCR           DEAD  HTTP 400
- `03:42:24`   TGCR           DEAD  HTTP 400
- `03:42:26`   BGCRVOL        DEAD  HTTP 400
- `03:42:27`   TGCRVOL        DEAD  HTTP 400
- `03:42:28`   OBFRVOL        LIVE  end=2026-08-18  Bil. of U.S. $ Overnight Bank Funding Volume
- `03:42:29`   EFFRVOL        LIVE  end=2026-08-18  Bil. of U.S. $ Effective Federal Funds Volume
