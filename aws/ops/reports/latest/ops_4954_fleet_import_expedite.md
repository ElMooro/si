- `19:34:52` baseline: 36 providers with freshest <= 1.0h (as_of 2026-08-23T18:48:53)
## A audit: schedules + rules -> functions
**Status:** failure  
**Duration:** 551.0s  
**Finished:** 2026-08-23T19:44:03+00:00  

## Error

```
SystemExit: 1
```

## Log

- `19:35:53` audit: 867 total scheduled entries · 78 import-relevant
- `19:35:53`   events    bea-economic-agent                         cron(15 14 * * ? *)    bea-economic-agent-daily
- `19:35:53`   events    bls-labor-agent                            cron(0 14 * * ? *)     bls-labor-agent-daily
- `19:35:53`   events    census-economic-agent                      cron(30 14 * * ? *)    census-economic-agent-daily
- `19:35:53`   events    cftc-futures-positioning-agent             cron(0 18 ? * FRI *)   cftc-cot-weekly-update
- `19:35:53`   events    ecb-auto-updater                           cron(0 6 ? * MON *)    ecb-weekly-update
- `19:35:53`   events    eia-energy-agent                           cron(40 11 * * ? *)    eia-energy-agent-daily
- `19:35:53`   scheduler justhodl-air-cargo                         cron(40 10 * * ? *)    justhodl-air-cargo-daily
- `19:35:53`   scheduler justhodl-beaters-grader                    cron(0 15 ? * SAT *)   justhodl-beaters-grader-weekly
- `19:35:53`   scheduler justhodl-bis-crossborder                   cron(40 9 ? * MON *)   justhodl-bis-crossborder-weekly
- `19:35:53`   events    justhodl-bis-gleif                         rate(7 days)           justhodl-bis-gleif-weekly
- `19:35:53`   events    justhodl-boj-detail                        cron(20 11 * * ? *)    boj-detail-daily
- `19:35:53`   scheduler justhodl-census-us                         rate(15 minutes)       justhodl-census-us-15min
- `19:35:53`   events    justhodl-cftc-full-datasets                cron(5 6 * * ? *)      justhodl-cftc-full-daily
- `19:35:53`   scheduler justhodl-data-census                       cron(45 12 * * ? *)    data-census-daily
- `19:35:53`   events    justhodl-ecb-derived                       cron(40 14 * * ? *)    justhodl-ecb-derived-daily
- `19:35:53`   events    justhodl-ecb-detail                        cron(0 11 * * ? *)     ecb-detail-daily
- `19:35:53`   events    justhodl-ecb-history                       cron(0 6 ? * SAT *)    justhodl-ecb-history-weekly
- `19:35:53`   events    justhodl-edgar-authority                   cron(0 13 * * ? *)     edgar-authority-daily
- `19:35:53`   events    justhodl-edgar-full-index                  cron(35 5 * * ? *)     justhodl-edgar-full-index-daily
- `19:35:53`   scheduler justhodl-etf-census                        cron(30 6 2,16 * ? *)  etf-census-sched
- `19:35:53`   events    justhodl-eurostat-oecd                     rate(7 days)           justhodl-eurostat-oecd-weekly
- `19:35:53`   scheduler justhodl-fi-census                         cron(0 7 2,16 * ? *)   fi-census-sched
- `19:35:53`   events    justhodl-fred-catalog                      rate(15 minutes)       justhodl-fred-catalog-5min
- `19:35:53`   events    justhodl-fred-tag-crawler                  cron(50 6 * * ? *)     justhodl-fred-catalog-daily
- `19:35:53`   scheduler justhodl-freight-pulse                     cron(50 11 * * ? *)    justhodl-freight-pulse-daily
- `19:35:53`   scheduler justhodl-fundamental-census                cron(0 6 1,15 * ? *)   fundamental-census-sched
- `19:35:53`   events    justhodl-gdelt-buzz                        cron(45 12 * * ? *)    gdelt-buzz-daily
- `19:35:53`   events    justhodl-gdelt-sentiment                   rate(30 minutes)       justhodl-gdelt-sentiment-30min
- `19:35:53`   scheduler justhodl-gov-sources                       cron(50 11 * * ? *)    gov-sources-daily
- `19:35:53`   scheduler justhodl-hot-money                         cron(50 9 * * ? *)     justhodl-hot-money-daily
- `19:35:53`   events    justhodl-hot-money                         cron(0 22 ? * MON-FRI  justhodl-hot-money-daily
- `19:35:53`   scheduler justhodl-import-sentinel                   rate(10 minutes)       justhodl-import-sentinel-10min
- `19:35:53`   events    justhodl-import-sentinel                   cron(5 0/6 * * ? *)    null
- `19:35:53`   events    justhodl-nyfed-dealer-survey               rate(7 days)           justhodl-nyfed-dealer-survey-weekly
- `19:35:53`   scheduler justhodl-nyfed-full-history                cron(40 4 * * ? *)     justhodl-nyfed-full-history-nightly
- `19:35:53`   events    justhodl-nyfed-full-history                cron(25 5 * * ? *)     justhodl-nyfed-full-history-daily
- `19:35:53`   scheduler justhodl-nyfed-markets-full                rate(1 hour)           justhodl-nyfed-markets-hourly-s
- `19:35:53`   scheduler justhodl-nyfed-markets-full                rate(1 hour)           justhodl-nyfed-markets-full-hourly
- `19:35:53`   events    justhodl-nyfed-markets-full                rate(1 hour)           justhodl-nyfed-markets-hourly
- `19:35:53`   events    justhodl-nyfed-pd                          cron(30 21 ? * THU,FRI justhodl-nyfed-pd-weekly
- `19:35:53`   events    justhodl-oecd-cli                          rate(7 days)           justhodl-oecd-cli-weekly
- `19:35:53`   scheduler justhodl-ofr-stfm                          cron(30 11 ? * TUE-SAT justhodl-ofr-stfm-daily
- `19:35:53`   events    justhodl-ofr-stfm                          rate(1 hour)           justhodl-ofr-stfm-hourly
- `19:35:53`   events    justhodl-polygon-daily-snapshot            cron(30 21 * * ? *)    justhodl-polygon-daily-2130
- `19:35:53`   events    justhodl-polygon-futures-curves            cron(20 13 * * ? *)    polygon-futures-curves-daily
- `19:35:53`   events    justhodl-polygon-fx-regime                 cron(10 13 * * ? *)    polygon-fx-regime-daily
- `19:35:53`   events    justhodl-polygon-options-flow              cron(15 14,15,16,17,18 justhodl-polygon-options-flow-hourly
- `19:35:53`   scheduler justhodl-portwatch                         cron(20 11 * * ? *)    portwatch-sched
- `19:35:53`   scheduler justhodl-portwatch                         cron(10 12 * * ? *)    justhodl-portwatch-daily
- `19:35:53`   events    justhodl-provider-catalog                  rate(1 hour)           justhodl-provider-catalog-hourly
- `19:35:53`   scheduler justhodl-sdmx-walker                       rate(1 hour)           justhodl-statcan-retry-hourly
- `19:35:53`   scheduler justhodl-sdmx-walker                       cron(15 3 ? * SUN *)   justhodl-ecb-rewalk-weekly
- `19:35:53`   scheduler justhodl-sdmx-walker                       rate(15 minutes)       justhodl-oecd-retry-hourly
- `19:35:53`   events    justhodl-sdmx-walker                       rate(5 minutes)        justhodl-sdmx-walker-hourly
- `19:35:53`   events    justhodl-sec-10kq                          rate(4 hours)          justhodl-sec-10kq-4h
- `19:35:53`   events    justhodl-sec-13f                           rate(1 day)            justhodl-sec-13f-daily
- `19:35:53`   events    justhodl-sec-8k                            rate(30 minutes)       justhodl-sec-8k-30min
- `19:35:53`   scheduler justhodl-sec-bulk                          cron(20 4 * * ? *)     justhodl-sec-bulk-daily
- `19:35:53`   scheduler justhodl-sec-bulk                          cron(0 9 ? * MON *)    justhodl-sec-bulk-weekly
- `19:35:53`   events    justhodl-sec-bulk                          rate(3 days)           justhodl-sec-bulk-hourly
- `19:35:53`   scheduler justhodl-sec-filing-diff                   cron(0 12 * * ? *)     justhodl-sec-filing-diff-daily
- `19:35:53`   events    justhodl-sec-filings-intel                 cron(7 21 * * ? *)     sec-filings-intel-3x-daily
- `19:35:53`   scheduler justhodl-sec-midas                         cron(30 4 ? * WED *)   justhodl-sec-midas-weekly
- `19:35:53`   events    justhodl-snb-detail                        cron(40 11 * * ? *)    snb-detail-daily
- `19:35:53`   events    justhodl-sovereign-fiscal                  cron(0 14 ? * TUE *)   justhodl-sovereign-fiscal-sched
- `19:35:53`   events    justhodl-sovereign-fiscal                  cron(10 7 * * ? *)     sovereign-fiscal-daily
- `19:35:53`   scheduler justhodl-spx-beaters                       cron(0 13 ? * SAT *)   justhodl-spx-beaters-weekly
- `19:35:53`   scheduler justhodl-src-mirror                        cron(5 5 * * ? *)      justhodl-src-mirror-daily
- `19:35:53`   scheduler justhodl-sympathetic-momentum              cron(30 23 * * ? *)    justhodl-sympathetic-momentum-daily
- `19:35:53`   events    justhodl-te-fred-mirror                    rate(1 hour)           justhodl-te-fred-mirror-hourly
- `19:35:53`   events    justhodl-tic-flows                         cron(15 14 ? * TUE *)  justhodl-tic-flows-sched
- `19:35:53`   events    justhodl-tic-flows                         cron(0 22 ? * THU *)   tic-flows-thu
- `19:35:53`   scheduler justhodl-tradingview                       cron(35 11 * * ? *)    tradingview-vault-daily
- `19:35:53`   events    justhodl-treasury-fiscal-full              cron(55 5 * * ? *)     justhodl-treasury-fiscal-daily
- `19:35:53`   scheduler justhodl-treasury-noise                    cron(45 21 ? * MON-FRI justhodl-treasury-noise-daily
- `19:35:53`   events    justhodl-treasury-noise                    cron(30 13 ? * TUE-SAT justhodl-treasury-noise-daily
- `19:35:53`   scheduler justhodl-treasury-rehypo                   cron(40 21 ? * MON-FRI treasury-rehypo-daily
- `19:35:53`   events    justhodl-vintage-fred                      cron(0 13 * * ? *)     justhodl-vintage-fred-daily
## B apply expedite map (tighten-only)

- `19:35:54`   TIGHTEN justhodl-ofr-stfm                        cron(30 11 ? * TUE-S -> rate(6 hours)
- `19:35:54`   TIGHTEN justhodl-sec-midas                       cron(30 4 ? * WED *) -> rate(6 hours)
- `19:35:54`   TIGHTEN justhodl-src-mirror                      cron(5 5 * * ? *)    -> rate(6 hours)
- `19:35:54`   TIGHTEN cftc-futures-positioning-agent           cron(0 18 ? * FRI *) -> rate(6 hours)
- `19:35:54`   TIGHTEN justhodl-bis-gleif                       rate(7 days)         -> rate(6 hours)
- `19:35:55`   TIGHTEN justhodl-cftc-full-datasets              cron(5 6 * * ? *)    -> rate(6 hours)
- `19:35:55`   TIGHTEN justhodl-eurostat-oecd                   rate(7 days)         -> rate(6 hours)
- `19:35:55`   TIGHTEN justhodl-gdelt-sentiment                 rate(30 minutes)     -> rate(15 minutes)
- `19:35:55`   TIGHTEN justhodl-oecd-cli                        rate(7 days)         -> rate(6 hours)
- `19:35:56`   TIGHTEN justhodl-sec-bulk                        rate(3 days)         -> rate(12 hours)
- `19:35:56` B: applied=10 skipped=68
- `19:35:56`   skip justhodl-data-census                     cron(45 12 * * ? *)  no-family
- `19:35:56`   skip justhodl-fundamental-census              cron(0 6 1,15 * ? *) no-family
- `19:35:56`   skip justhodl-census-us                       rate(15 minutes)     no-family
- `19:35:56`   skip justhodl-nyfed-full-history              cron(40 4 * * ? *)   no-family
- `19:35:56`   skip justhodl-treasury-rehypo                 cron(40 21 ? * MON-F cron-skip
- `19:35:56`   skip justhodl-sec-filing-diff                 cron(0 12 * * ? *)   no-family
- `19:35:56`   skip justhodl-treasury-noise                  cron(45 21 ? * MON-F cron-skip
- `19:35:56`   skip justhodl-fi-census                       cron(0 7 2,16 * ? *) no-family
- `19:35:56`   skip justhodl-air-cargo                       cron(40 10 * * ? *)  no-family
- `19:35:56`   skip justhodl-nyfed-markets-full              rate(1 hour)         already<=target
- `19:35:56`   skip justhodl-sdmx-walker                     rate(1 hour)         already<=target
- `19:35:56`   skip justhodl-portwatch                       cron(20 11 * * ? *)  no-family
- `19:35:56`   skip justhodl-portwatch                       cron(10 12 * * ? *)  no-family
- `19:35:56`   skip justhodl-sympathetic-momentum            cron(30 23 * * ? *)  no-family
- `19:35:56`   skip justhodl-gov-sources                     cron(50 11 * * ? *)  cron-skip
- `19:35:56`   skip justhodl-sdmx-walker                     cron(15 3 ? * SUN *) cron-skip
- `19:35:56`   skip justhodl-freight-pulse                   cron(50 11 * * ? *)  no-family
- `19:35:56`   skip justhodl-sec-bulk                        cron(20 4 * * ? *)   cron-skip
- `19:35:56`   skip justhodl-bis-crossborder                 cron(40 9 ? * MON *) cron-skip
- `19:35:56`   skip justhodl-spx-beaters                     cron(0 13 ? * SAT *) cron-skip
- `19:35:56`   skip justhodl-sec-bulk                        cron(0 9 ? * MON *)  cron-skip
- `19:35:56`   skip justhodl-etf-census                      cron(30 6 2,16 * ? * no-family
- `19:35:56`   skip justhodl-hot-money                       cron(50 9 * * ? *)   no-family
- `19:35:56`   skip justhodl-import-sentinel                 rate(10 minutes)     already<=target
- `19:35:56`   skip justhodl-beaters-grader                  cron(0 15 ? * SAT *) cron-skip
- `19:35:56`   skip justhodl-nyfed-markets-full              rate(1 hour)         already<=target
- `19:35:56`   skip justhodl-sdmx-walker                     rate(15 minutes)     already<=target
- `19:35:56`   skip justhodl-tradingview                     cron(35 11 * * ? *)  no-family
- `19:35:56`   skip bea-economic-agent                       cron(15 14 * * ? *)  cron-skip
- `19:35:56`   skip bls-labor-agent                          cron(0 14 * * ? *)   cron-skip
- `19:35:56`   skip justhodl-boj-detail                      cron(20 11 * * ? *)  cron-skip
- `19:35:56`   skip census-economic-agent                    cron(30 14 * * ? *)  no-family
- `19:35:56`   skip justhodl-ecb-detail                      cron(0 11 * * ? *)   cron-skip
- `19:35:56`   skip ecb-auto-updater                         cron(0 6 ? * MON *)  cron-skip
- `19:35:56`   skip justhodl-edgar-authority                 cron(0 13 * * ? *)   cron-skip
- `19:35:56`   skip eia-energy-agent                         cron(40 11 * * ? *)  cron-skip
- `19:35:56`   skip justhodl-gdelt-buzz                      cron(45 12 * * ? *)  cron-skip
- `19:35:56`   skip justhodl-ecb-derived                     cron(40 14 * * ? *)  cron-skip
- `19:35:56`   skip justhodl-ecb-history                     cron(0 6 ? * SAT *)  cron-skip
- `19:35:56`   skip justhodl-edgar-full-index                cron(35 5 * * ? *)   cron-skip
- `19:35:56` verify: 10/10 re-described == new (bad=[])
- `19:35:56` cost: ~991 -> ~1073 scheduled import invokes/day (+82)
## C kick sweep (freshness collapses now)

- `19:36:32` C: kicked 63 import functions
- `19:36:32` cadence-map artifact written: data/warm/_audit/cadence-map.json
## D board proof (fresh<=1h count must rise >=5)

- `19:44:03`   fresher: sec-midas          97.2h -> 0.0h
- `19:44:03`   fresher: sec-bulk           14.5h -> 0.0h
- `19:44:03` D FAIL fresh<=1h: 36 -> 21 (as_of 2026-08-23T19:36:25)
- `19:44:03`   sec-midas freshest now: -0.0h (schedule tightened; run may still be in flight)
- `19:44:03` ops 4954 RED: D board
