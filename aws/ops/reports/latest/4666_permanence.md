# ops 4666 — S3 permanence + ICE recovery + repo official probes

**Status:** success  
**Duration:** 257.2s  
**Finished:** 2026-08-14T20:48:54+00:00  

## Log
## A1. Versioning

- `20:44:37`   status=Enabled mfa_delete=None
- `20:44:37` ✅   [protect] versioning Enabled (deletes become markers, prior versions survive)
## A2. Lifecycle — strip anything that expires banked history

- `20:44:37`   DROPPING expiry: id=expire-old-versions-after-30d prefix='' noncurrent={'NoncurrentDays': 30} current=None
- `20:44:37`   DROPPING expiry: id=noncurrent-30d prefix='' noncurrent={'NoncurrentDays': 30} current=None
- `20:44:37`   DROPPING expiry: id=jh-noncurrent-14d prefix='' noncurrent={'NoncurrentDays': 14} current=None
- `20:44:40`   rules now: 7 (dropped 3 expiries)
- `20:44:40` ✅   [protect] no expiry rule can reach banked history (offenders: [])
## A3. Deny-delete bucket policy on banked prefixes

- `20:44:45` ✅   [protect] deny-Delete* statement live on data/warm/* and data/providers/*
## A4. LIVE PROOF — deletion must actually fail

- `20:44:45`   delete attempt -> An error occurred (AccessDenied) when calling the DeleteObject operation: User: arn:aws:iam::857687956942:user
- `20:44:45` ✅   [protect] a real delete against banked prefix was REJECTED by the policy
## B. ICE recovery — 4 more series

- `20:44:50`   located 4/4 banked docs
- `20:44:51` ✅   BAMLC0A1CAAA: +6943 rows (splice mm=0/150) -> 7730 obs since 1996-12-31
- `20:44:51` ⚠   BAMLH0A0HYM2EY: fetch HTTP Error 404: Not Found
- `20:44:52` ✅   BAMLCC0A0CMTRIV: +6159 rows (splice mm=0/150) -> 6946 obs since 2000-01-03
- `20:44:53` ✅   BAMLHYH0A0HYM2TRIV: +1169 rows (splice mm=0/138) -> 1956 obs since 2019-02-15
- `20:44:53` ✅   [ice] 3/4 archives merged
## C. Repo official sources — reachability from AWS

- `20:48:54` ⚠   engine probe: Read timeout on endpoint URL: "https://lambda.us-east-1.amazonaws.com/
- `20:48:54`   QUEUED for bulk importer: OFR datasets metadata -> https://data.financialresearch.gov/v1/metadata/datasets
- `20:48:54`   QUEUED for bulk importer: OFR repo series (NCCBR vol) -> https://data.financialresearch.gov/v1/series/timeseries?mnemonic=REPO-NCCBR_AR_TV-FRB&start_date=2000-01-01
- `20:48:54`   QUEUED for bulk importer: OFR FSI -> https://data.financialresearch.gov/v1/series/timeseries?mnemonic=FSI-OFR_FSI&start_date=2000-01-01
- `20:48:54`   QUEUED for bulk importer: NYFed PD 1998 (fails) -> https://markets.newyorkfed.org/api/pd/get/SBP2001/timeseries/PDFTD-UST.json
- `20:48:54`   QUEUED for bulk importer: NYFed tri-party (2010+) -> https://markets.newyorkfed.org/api/tripartyRepo/get/all/results/latest.json
- `20:48:54`   QUEUED for bulk importer: NYFed RRP history -> https://markets.newyorkfed.org/api/rp/reverserepo/all/results/lastTwoWeeks.json
- `20:48:54`   QUEUED for bulk importer: NYFed SRF/repo ops -> https://markets.newyorkfed.org/api/rp/repo/all/results/lastTwoWeeks.json
- `20:48:54`   NOTE: sandbox egress blocks OFR; these run inside AWS in the follow-up importer op
## verdict

- `20:48:54` ✅ banked history is delete-proof (policy-enforced, proven by a rejected delete), expiry rules stripped, 3 more ICE series recovered
