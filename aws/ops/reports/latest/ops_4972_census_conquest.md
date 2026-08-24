## G-1 markers

**Status:** failure  
**Duration:** 669.7s  
**Finished:** 2026-08-24T23:59:55+00:00  

## Error

```
SystemExit: 1
```

## Log
- `23:48:46`   ok 'conquest-v115 ops4972'
- `23:48:46`   ok 'v1.0.2 ops4967'
- `23:48:46`   census key: present
## G0 settle census v1.1.5

- `23:48:46`   settled (0s)
## P1 per-slug recon + two-state confirmation

- `23:48:46`   ── aies-miscsector  base=https://api.census.gov/data/timeseries/aies/miscsector
- `23:48:46`      geo=[('us', [], [])] (200) tp=time req=['PBA', 'TYPOP', 'TAXSTAT', 'NAICS'] vars=['RCPT_NEWPT_SSBNFTS_CV', 'RCPT_NONOP_PROGSRVC_VAL', 'RCPT_MOTR_AGR_CV', 'RCPT_APSOFT_MAIN_DVAL', 'RCPT_CUST_IND_CV', 'RCPT_AUDVID_CV']
- `23:48:47`      us-star       200 rows~859 
- `23:48:47`      CONQUERED via us-star -> {"vars": ["RCPT_NEWPT_SSBNFTS_CV", "RCPT_NONOP_PROGSRVC_VAL", "RCPT_MOTR_AGR_CV", "RCPT_APSOFT_MAIN_DVAL", "RCPT_CUST_IND_CV", "RCPT_AUDVID_CV"], "tp": "time", 
- `23:48:47`   ── asm-industry  base=https://api.census.gov/data/timeseries/asm/industry
- `23:48:47`      geo=[('us', [], [])] (200) tp=time req=['NAICS'] vars=['BENHEA', 'BENPEC_S', 'INVMATE_F', 'INVWIPB_F', 'PCHADVT', 'MSCTOT_S_F']
- `23:48:48`      us-star       200 rows~9395 
- `23:48:48`      CONQUERED via us-star -> {"vars": ["BENHEA", "BENPEC_S", "INVMATE_F", "INVWIPB_F", "PCHADVT", "MSCTOT_S_F"], "tp": "time", "full_time": "from 1990", "extra": {"NAICS": "*"}}
- `23:48:48`   ── poverty-saipe-schdist  base=https://api.census.gov/data/timeseries/poverty/saipe/schdist
- `23:48:48`      geo=[('school district (elementary)', ['state'], ['state']), ('school district (secondary)', ['state'], ['state']), ('school district (unified)', ['state'], ['state']), ('school district administrative area', ['state'], ['state'])] (200) tp=time req=[] vars=['SAEPOVRAT5_17RV_PT', 'YEAR', 'SD_NAME', 'GEOID', 'SAEPOV5_17V_PT', 'SAEPOV5_17RV_PT']
- `23:48:48`      sub-in-state  200 rows~4 
- `23:48:48`      2nd-state failed (204)
- `23:48:48`      us-star       400 rows~0 error: unknown/unsupported geography hierarchy
- `23:48:49`      state-direct  400 rows~0 error: unknown/unsupported geography hierarchy
- `23:48:49`      no-geo        400 rows~0 error: missing 'for' argument
- `23:48:49`   ── pseo-earnings  base=https://api.census.gov/data/timeseries/pseo/earnings
- `23:48:50`      geo=[('us', [], [])] (200) tp=time req=['CIPCODE', 'GEOCOMP', 'GRAD_COHORT', 'DEGREE_LEVEL'] vars=['Y10_GRADS_EARN', 'GRAD_COHORT_YEARS', 'Y1_P50_EARNINGS', 'Y10_IPEDS_COUNT', 'AGG_LEVEL_PSEO', 'CIPCODE']
- `23:48:50`      us-star       400 rows~0 error: unknown predicate variable: 'time'
- `23:48:50`      state-direct  400 rows~0 error: unknown/unsupported geography hierarchy
- `23:48:50`      no-geo        400 rows~0 error: missing 'for' argument
- `23:48:51`   ── pseo-flows  base=https://api.census.gov/data/timeseries/pseo/flows
- `23:48:51`      geo=[('us', [], []), ('division', [], [])] (200) tp=time req=['CIPCODE', 'NAICS', 'GEOCOMP', 'GRAD_COHORT'] vars=['Y1_GRADS_NME', 'Y1_GRADS_EMP', 'GRAD_COHORT_YEARS', 'AGG_LEVEL_PSEO', 'CIPCODE', 'DIVISION']
- `23:48:51`      us-star       400 rows~0 error: unknown predicate variable: 'time'
- `23:48:51`      state-direct  400 rows~0 error: unknown/unsupported geography hierarchy
- `23:48:51`      no-geo        400 rows~0 error: missing 'for' argument
## P2 merge-write overrides

- `23:48:52`   GRAM_KEY now 5 entries (+2)
## P3 redo drive

- `23:49:22`   t+  30s ok=0/2 still-failed=0
- `23:49:52`   t+  60s ok=0/2 still-failed=1
- `23:50:22`   t+  90s ok=0/2 still-failed=2
- `23:50:52`   t+ 120s ok=0/2 still-failed=2
- `23:51:22`   t+ 150s ok=0/2 still-failed=2
- `23:51:53`   t+ 180s ok=0/2 still-failed=2
- `23:52:23`   t+ 210s ok=0/2 still-failed=2
- `23:52:53`   t+ 240s ok=0/2 still-failed=2
- `23:53:23`   t+ 270s ok=0/2 still-failed=0
- `23:53:53`   t+ 301s ok=0/2 still-failed=1
- `23:54:23`   t+ 331s ok=0/2 still-failed=0
- `23:54:53`   t+ 361s ok=0/2 still-failed=1
- `23:55:24`   t+ 391s ok=0/2 still-failed=0
- `23:55:54`   t+ 421s ok=0/2 still-failed=0
- `23:56:24`   t+ 451s ok=0/2 still-failed=1
- `23:56:54`   t+ 482s ok=0/2 still-failed=0
- `23:57:24`   t+ 512s ok=0/2 still-failed=1
- `23:57:54`   t+ 542s ok=0/2 still-failed=2
- `23:58:25`   t+ 572s ok=0/2 still-failed=0
- `23:58:55`   t+ 602s ok=0/2 still-failed=1
- `23:59:25`   t+ 633s ok=0/2 still-failed=0
- `23:59:55`   t+ 663s ok=0/2 still-failed=1
- `23:59:55`   aies-miscsector          ok=False rows=0 span=None..None mode=None
- `23:59:55`   asm-industry             ok=False rows=0 span=None..None mode=year_state
- `23:59:55`   UNCONQUERED poverty-saipe-schdist all shapes refused; last bodies logged
- `23:59:55`   UNCONQUERED pseo-earnings        all shapes refused; last bodies logged
- `23:59:55`   UNCONQUERED pseo-flows           all shapes refused; last bodies logged
- `23:59:55` G1 FAIL conquered-live=0/5 probe-refused=3
- `23:59:55` ops 4972 RED: G1
