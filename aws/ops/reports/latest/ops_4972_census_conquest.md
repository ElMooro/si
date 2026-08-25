## G-1 markers

**Status:** failure  
**Duration:** 8.8s  
**Finished:** 2026-08-25T03:50:41+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/ops_4972_census_conquest.py", line 190, in <module>
    u = qs(base, vars_pick, (None if _nt else tp),
        ^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^^
  File "/home/runner/work/si/si/aws/ops/pending/ops_4972_census_conquest.py", line 73, in qs
    tp + "=" + urllib.parse.quote_plus(pred)]
    ~~~^~~~~
TypeError: unsupported operand type(s) for +: 'NoneType' and 'str'

```

## Log
- `03:50:32`   ok 'conquest-v116 ops4972'
- `03:50:32`   ok 'v1.0.2 ops4967'
- `03:50:32`   census key: present
## G0 settle census v1.1.5

- `03:50:33`   settled (0s)
## P1 per-slug recon + two-state confirmation

- `03:50:33`   ── aies-miscsector  base=https://api.census.gov/data/timeseries/aies/miscsector
- `03:50:34`      geo=[('us', [], [])] (200) tp=time req=['PBA', 'TYPOP', 'TAXSTAT', 'NAICS'] vars=['RCPT_NEWPT_SSBNFTS_CV', 'RCPT_NONOP_PROGSRVC_VAL', 'RCPT_MOTR_AGR_CV', 'RCPT_APSOFT_MAIN_DVAL', 'RCPT_CUST_IND_CV', 'RCPT_AUDVID_CV']
- `03:50:35`      us-star       200 rows~859 
- `03:50:35`      CONQUERED via us-star -> {"vars": ["RCPT_NEWPT_SSBNFTS_CV", "RCPT_NONOP_PROGSRVC_VAL", "RCPT_MOTR_AGR_CV", "RCPT_APSOFT_MAIN_DVAL", "RCPT_CUST_IND_CV", "RCPT_AUDVID_CV"], "tp": "time", 
- `03:50:35`   ── asm-industry  base=https://api.census.gov/data/timeseries/asm/industry
- `03:50:35`      geo=[('us', [], [])] (200) tp=time req=['NAICS'] vars=['BENHEA', 'BENPEC_S', 'INVMATE_F', 'INVWIPB_F', 'PCHADVT', 'MSCTOT_S_F']
- `03:50:38`      us-star       200 rows~9395 
- `03:50:38`      CONQUERED via us-star -> {"vars": ["BENHEA", "BENPEC_S", "INVMATE_F", "INVWIPB_F", "PCHADVT", "MSCTOT_S_F"], "tp": "time", "full_time": "from 1989", "geo": "us:*", "extra": {"NAICS": "*
- `03:50:38`   ── poverty-saipe-schdist  base=https://api.census.gov/data/timeseries/poverty/saipe/schdist
- `03:50:39`      geo=[('school district (elementary)', ['state'], ['state']), ('school district (secondary)', ['state'], ['state']), ('school district (unified)', ['state'], ['state']), ('school district administrative area', ['state'], ['state'])] (200) tp=time req=[] vars=['SAEPOVRAT5_17RV_PT', 'YEAR', 'SD_NAME', 'GEOID', 'SAEPOV5_17V_PT', 'SAEPOV5_17RV_PT']
- `03:50:39`      sub-in-state  200 rows~4 
- `03:50:40`      CONQUERED via sub-in-state -> {"vars": ["SAEPOVRAT5_17RV_PT", "YEAR", "SD_NAME", "GEOID", "SAEPOV5_17V_PT", "SAEPOV5_17RV_PT"], "tp": "time", "full_time": "from 1989", "geo_iter": "state", "
- `03:50:40`   ── pseo-earnings  base=https://api.census.gov/data/timeseries/pseo/earnings
- `03:50:40`      geo=[('us', [], [])] (200) tp=time req=['CIPCODE', 'GEOCOMP', 'GRAD_COHORT', 'DEGREE_LEVEL'] vars=['Y10_GRADS_EARN', 'GRAD_COHORT_YEARS', 'Y1_P50_EARNINGS', 'Y10_IPEDS_COUNT', 'AGG_LEVEL_PSEO', 'CIPCODE']
- `03:50:40`      us-star       400 rows~0 error: unknown predicate variable: 'time'
