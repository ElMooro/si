# ops 3970 — DISCOVERY: call the candidate official endpoints

**Status:** success  
**Duration:** 6.9s  
**Finished:** 2026-07-27T05:33:40+00:00  

## Data

| confirmed | failed | fred_key_present |
|---|---|---|
|  |  | True |
| 2 | 9 |  |

## Log
## A. ECB — German 10Y (EUBUND) via the YC curve

- `05:33:34`   [200] EUBUND -> 3.2247334899  https://data-api.ecb.europa.eu/service/data/YC/B.U2.EUR.4F.G_N_A.SV_C_YM.SR_10Y?format=jsondata&
## B. FRED — JPIRYY / CNPPIYY / USHMI candidates

- `05:33:34`   [200] JPIRYY       JPNCPIALLMINMEI      -> None
- `05:33:35`   [400] CNPPIYY      CHNPPIALLMINMEI      -> None
- `05:33:35`   [200] CNPPIYY_alt  CHNCPIALLMINMEI      -> {'realtime_start': '2026-06-01', 'realtime_end': '2026-06-01', 'date': '2025-04-01', 'value': '115.0297'}
- `05:33:35`   [400] USHMI        NAHBHMI              -> None
- `05:33:35`   [400] USHMI_alt    HMI                  -> None
- `05:33:36`   [400] USMPMI       NAPM                 -> None
- `05:33:36`   [400] USNMPR       NMFCI                -> None
## C. Eurostat — DENO / EUESI

- `05:33:37`   [200] DENO     sts_intv_m     -> None
- `05:33:37`   [200] EUESI    ei_bssi_m_r2   -> None
## D. BOJ — hunt a 3M rate series for JP03MY

- `05:33:37`   [200] IR01 metadata bytes=1289 3-month candidates=0
## E. SNB — CH02Y/CH03Y (registry records this parse as an open bug)

- `05:33:39`   [200] bytes=5608048
- `05:33:39`   timeseries n=22
- `05:33:39`       header=[{'dim': 'Overview', 'dimItem': 'Spot interest rates with different maturities f n_values=7534 last={'date': '2025-07-31', 'value': -0.096}
- `05:33:39`       header=[{'dim': 'Overview', 'dimItem': 'Spot interest rates with different maturities f n_values=7534 last={'date': '2025-07-31', 'value': -0.083}
- `05:33:39`       header=[{'dim': 'Overview', 'dimItem': 'Spot interest rates with different maturities f n_values=7534 last={'date': '2025-07-31', 'value': -0.043}
- `05:33:39`       header=[{'dim': 'Overview', 'dimItem': 'Spot interest rates with different maturities f n_values=7534 last={'date': '2025-07-31', 'value': 0.013}
## F. ONS — GBGDG

- `05:33:40`   [404] bytes=25 head=b'HTTP Error 404: Not Found'
## G. verdict

- `05:33:40`   CONFIRMED (wire these):
- `05:33:40`     EUBUND         ecb:YC 10Y               3.2247334899
- `05:33:40`     CNPPIYY_alt    fred:CHNCPIALLMINMEI     {'realtime_start': '2026-06-01', 'realtime_end': '2026-06-01', 'date':
- `05:33:40`   NOT RESOLVED (leave NO_FREE_SOURCE, documented):
- `05:33:40`     JPIRYY         fred:JPNCPIALLMINMEI
- `05:33:40`     CNPPIYY        fred:CHNPPIALLMINMEI
- `05:33:40`     USHMI          fred:NAHBHMI
- `05:33:40`     USHMI_alt      fred:HMI
- `05:33:40`     USMPMI         fred:NAPM
- `05:33:40`     USNMPR         fred:NMFCI
- `05:33:40`     DENO           eurostat:sts_intv_m
- `05:33:40`     EUESI          eurostat:ei_bssi_m_r2
- `05:33:40`     JP03MY         boj:IR01
- `05:33:40` 
- `05:33:40`   NOT GOVERNMENT DATA AT ALL — will NOT be faked with a substitute:
- `05:33:40`     USMPMI / USCPMI / USNMPR / EUMPMI / JPMPMI / TWMPMI = S&P Global and ISM licensed PMIs; USHMI = NAHB. No agency publishes them.
- `05:33:40` ✅ DISCOVERY COMPLETE — evidence recorded; wiring op follows for confirmed only
