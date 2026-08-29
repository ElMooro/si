## P0 rule wiring

**Status:** success  
**Duration:** 665.7s  
**Finished:** 2026-08-29T14:47:51+00:00  

## Data

| eta_min | flows | pages | pages_per_min | series |
|---|---|---|---|---|
| 54 | 437 | 145697 | 6272 | 72848500 |

## Log
- `14:36:46`   justhodl-series-extractor-5min state=ENABLED schedule=rate(2 minutes)
- `14:36:46`   targets: 1
- `14:36:46`     id=t1 arn=arn:aws:lambda:us-east-1:857687956942:function:justhodl-series-extractor input={"provider": "eurostat"}
- `14:36:46`   target intact -- put_rule preserved the wiring
## P1 runtime

- `14:36:46`   mem=10240 MB timeout=900s lastmod=2026-08-29T14:21:26.000+0000
- `14:36:46`   reserved concurrency = 1 (interlock holding)
## P2 observe

- `14:36:47`   window opens: flows=411 pages=76389 series=38194500
- `14:40:28`   t+ 221s flows=421 (+10) pages=97951 (+21562) series=48975500 (+10781000)
- `14:44:09`   t+ 442s flows=432 (+21) pages=121091 (+44702) series=60545500 (+22351000)
- `14:47:51`   t+ 663s flows=437 (+26) pages=145697 (+69308) series=72848500 (+34654000)
- `14:47:51`   rate: 6272 pages/min  3136109 series/min
- `14:47:51`   flows 437 / 8147 (5.36%)   pages 145697 (30.0% of ~486000)
- `14:47:51`   ETA: ~0.9 hours (~54 min)
## P3 integrity of what the parallel writer produced

- `14:47:51`   write errors this run=0  holes=0  retired flows=0
- `14:47:51`   read-back page-145692.json: page=145692 count=500 rows=500
- `14:47:51`     sample id=eurostat:CENS_21A_R2:A.UNK.Y_LT1.T.NR.LIZ flow=CENS_21A_R2 geo=LIZ last_obs=2021 last_value=0.0
- `14:47:51`     fields: ['dims', 'engines', 'first_obs', 'flow', 'freq', 'geo', 'id', 'last_obs', 'last_value', 'name', 'raw_key', 'source_url', 'status', 'unit']
- `14:47:51`   -> data/ops/eurostat-backfill-progress.json
- `14:47:51` ops 5035 GREEN -- wiring verified, import landing, pages valid
