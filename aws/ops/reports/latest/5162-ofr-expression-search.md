# ops 5162 -- OFR aliases, full FSI history, expressions

**Status:** failure  
**Duration:** 40.8s  
**Finished:** 2026-09-03T14:02:54+00:00  

## Error

```
SystemExit: 1
```

## Log
## S1 deploy Symdir 1.10.0 and build a fresh generation

- `14:02:14`   zip: 147755 bytes
## 1. Lambda

- `14:02:14`   Lambda exists — updating
- `14:02:17` ✅   ✓ updated justhodl-symdir
- `14:02:36`   docs=1378310 OFR={'series': 1394, 'docs': 1397, 's': 1.0} OFR-FSI={'series': 9, 'rows': 6735, 'docs': 9, 's': 0.4} errors={}
## S2 verify natural names and aliases

- `14:02:49`   'OFR Financial Stress Index' -> direct=True worker=True
- `14:02:49`   'Primary Dealer Aggregate Fails to Deliver Total' -> direct=True worker=True
- `14:02:50`   'Primary Dealer Aggregate Fails to Receive Total' -> direct=True worker=True
- `14:02:50`   'Federal Reserve Bank of New York Primary Dealer Statistics' -> direct=True worker=True
## S3 verify full histories and expression operands

- `14:02:51`   ofr-fsi:OFR_FSI obs=6735 first=2000-01-03 last=2026-08-12
- `14:02:51`   ofr:NYPD-PD_AFtD_TOT-A obs=607 first=2015-01-07 last=2026-08-19
- `14:02:52`   ofr:NYPD-PD_AFtR_TOT-A obs=607 first=2015-01-07 last=2026-08-19
- `14:02:53`   FRED:FEDFUNDS obs=866 first=1954-07-01 last=2026-08-01
- `14:02:53`   TVC:US10Y obs=16154 first=1962-01-02 last=2026-09-02
## S4 verify exact Pages revision and expression client

- `14:02:54`   run_sha=9359977fff8bd4b7b779370d351dad66e9e175c2 chart_sha=0a707ba37ee08134135869085ad3004602e7775e Pages id=6245406719 state=success
- `14:02:54`   live=True details={'commit': '0a707ba37ee08134135869085ad3004602e7775e', 'manifest_hash': '7a69c42af589da2c30893d95243cf6ea853e0de39f9da9693db5df46000a57f6', 'live_hash': '1c8b9190830a21589186d5e46c290c7da688fc117d7da25f4ae05fcf074ea882', 'stamp': True, 'engine': True, 'renderer': True}
## verdict

- `14:02:54` ✗ fresh Symdir 1.10.0 index did not materialize
