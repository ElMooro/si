- `19:48:55` baseline: 38 providers with freshest <= 1.0h (as_of 2026-08-23T19:43:32)
## A audit: schedules + rules -> functions
**Status:** failure  
**Duration:** 454.9s  
**Finished:** 2026-08-23T19:56:30+00:00  

## Error

```
SystemExit: 1
```

## Log

- `19:49:29` audit: 867 total scheduled entries · 78 import-relevant
- `19:49:29`   events    bea-economic-agent                         cron(15 14 * * ? *)    bea-economic-agent-daily
- `19:49:29`   events    bls-labor-agent                            cron(0 14 * * ? *)     bls-labor-agent-daily
- `19:49:29`   events    census-economic-agent                      cron(30 14 * * ? *)    census-economic-agent-daily
- `19:49:29`   events    cftc-futures-positioning-agent             rate(6 hours)          cftc-cot-weekly-update
- `19:49:29`   events    ecb-auto-updater                           cron(0 6 ? * MON *)    ecb-weekly-update
- `19:49:29`   events    eia-energy-agent                           cron(40 11 * * ? *)    eia-energy-agent-daily
- `19:49:29`   scheduler justhodl-air-cargo                         cron(40 10 * * ? *)    justhodl-air-cargo-daily
- `19:49:29`   scheduler justhodl-beaters-grader                    cron(0 15 ? * SAT *)   justhodl-beaters-grader-weekly
- `19:49:29`   scheduler justhodl-bis-crossborder                   cron(40 9 ? * MON *)   justhodl-bis-crossborder-weekly
- `19:49:29`   events    justhodl-bis-gleif                         rate(6 hours)          justhodl-bis-gleif-weekly
- `19:49:29`   events    justhodl-boj-detail                        cron(20 11 * * ? *)    boj-detail-daily
- `19:49:29`   scheduler justhodl-census-us                         rate(15 minutes)       justhodl-census-us-15min
- `19:49:29`   events    justhodl-cftc-full-datasets                rate(6 hours)          justhodl-cftc-full-daily
- `19:49:29`   scheduler justhodl-data-census                       cron(45 12 * * ? *)    data-census-daily
- `19:49:29`   events    justhodl-ecb-derived                       cron(40 14 * * ? *)    justhodl-ecb-derived-daily
- `19:49:29`   events    justhodl-ecb-detail                        cron(0 11 * * ? *)     ecb-detail-daily
- `19:49:29`   events    justhodl-ecb-history                       cron(0 6 ? * SAT *)    justhodl-ecb-history-weekly
- `19:49:29`   events    justhodl-edgar-authority                   cron(0 13 * * ? *)     edgar-authority-daily
- `19:49:29`   events    justhodl-edgar-full-index                  cron(35 5 * * ? *)     justhodl-edgar-full-index-daily
- `19:49:29`   scheduler justhodl-etf-census                        cron(30 6 2,16 * ? *)  etf-census-sched
- `19:49:29`   events    justhodl-eurostat-oecd                     rate(6 hours)          justhodl-eurostat-oecd-weekly
- `19:49:29`   scheduler justhodl-fi-census                         cron(0 7 2,16 * ? *)   fi-census-sched
- `19:49:29`   events    justhodl-fred-catalog                      rate(15 minutes)       justhodl-fred-catalog-5min
- `19:49:29`   events    justhodl-fred-tag-crawler                  cron(50 6 * * ? *)     justhodl-fred-catalog-daily
- `19:49:29`   scheduler justhodl-freight-pulse                     cron(50 11 * * ? *)    justhodl-freight-pulse-daily
- `19:49:29`   scheduler justhodl-fundamental-census                cron(0 6 1,15 * ? *)   fundamental-census-sched
- `19:49:29`   events    justhodl-gdelt-buzz                        cron(45 12 * * ? *)    gdelt-buzz-daily
- `19:49:29`   events    justhodl-gdelt-sentiment                   rate(15 minutes)       justhodl-gdelt-sentiment-30min
- `19:49:29`   scheduler justhodl-gov-sources                       cron(50 11 * * ? *)    gov-sources-daily
- `19:49:29`   scheduler justhodl-hot-money                         cron(50 9 * * ? *)     justhodl-hot-money-daily
- `19:49:29`   events    justhodl-hot-money                         cron(0 22 ? * MON-FRI  justhodl-hot-money-daily
- `19:49:29`   scheduler justhodl-import-sentinel                   rate(10 minutes)       justhodl-import-sentinel-10min
- `19:49:29`   events    justhodl-import-sentinel                   cron(5 0/6 * * ? *)    null
- `19:49:29`   events    justhodl-nyfed-dealer-survey               rate(7 days)           justhodl-nyfed-dealer-survey-weekly
- `19:49:29`   scheduler justhodl-nyfed-full-history                cron(40 4 * * ? *)     justhodl-nyfed-full-history-nightly
- `19:49:29`   events    justhodl-nyfed-full-history                cron(25 5 * * ? *)     justhodl-nyfed-full-history-daily
- `19:49:29`   scheduler justhodl-nyfed-markets-full                rate(1 hour)           justhodl-nyfed-markets-hourly-s
- `19:49:29`   scheduler justhodl-nyfed-markets-full                rate(1 hour)           justhodl-nyfed-markets-full-hourly
- `19:49:29`   events    justhodl-nyfed-markets-full                rate(1 hour)           justhodl-nyfed-markets-hourly
- `19:49:29`   events    justhodl-nyfed-pd                          cron(30 21 ? * THU,FRI justhodl-nyfed-pd-weekly
- `19:49:29`   events    justhodl-oecd-cli                          rate(6 hours)          justhodl-oecd-cli-weekly
- `19:49:29`   scheduler justhodl-ofr-stfm                          rate(6 hours)          justhodl-ofr-stfm-daily
- `19:49:29`   events    justhodl-ofr-stfm                          rate(1 hour)           justhodl-ofr-stfm-hourly
- `19:49:29`   events    justhodl-polygon-daily-snapshot            cron(30 21 * * ? *)    justhodl-polygon-daily-2130
- `19:49:29`   events    justhodl-polygon-futures-curves            cron(20 13 * * ? *)    polygon-futures-curves-daily
- `19:49:29`   events    justhodl-polygon-fx-regime                 cron(10 13 * * ? *)    polygon-fx-regime-daily
- `19:49:29`   events    justhodl-polygon-options-flow              cron(15 14,15,16,17,18 justhodl-polygon-options-flow-hourly
- `19:49:29`   scheduler justhodl-portwatch                         cron(20 11 * * ? *)    portwatch-sched
- `19:49:29`   scheduler justhodl-portwatch                         cron(10 12 * * ? *)    justhodl-portwatch-daily
- `19:49:29`   events    justhodl-provider-catalog                  rate(1 hour)           justhodl-provider-catalog-hourly
- `19:49:29`   scheduler justhodl-sdmx-walker                       rate(1 hour)           justhodl-statcan-retry-hourly
- `19:49:29`   scheduler justhodl-sdmx-walker                       cron(15 3 ? * SUN *)   justhodl-ecb-rewalk-weekly
- `19:49:29`   scheduler justhodl-sdmx-walker                       rate(15 minutes)       justhodl-oecd-retry-hourly
- `19:49:29`   events    justhodl-sdmx-walker                       rate(5 minutes)        justhodl-sdmx-walker-hourly
- `19:49:29`   events    justhodl-sec-10kq                          rate(4 hours)          justhodl-sec-10kq-4h
- `19:49:29`   events    justhodl-sec-13f                           rate(1 day)            justhodl-sec-13f-daily
- `19:49:29`   events    justhodl-sec-8k                            rate(30 minutes)       justhodl-sec-8k-30min
- `19:49:29`   scheduler justhodl-sec-bulk                          cron(20 4 * * ? *)     justhodl-sec-bulk-daily
- `19:49:29`   scheduler justhodl-sec-bulk                          cron(0 9 ? * MON *)    justhodl-sec-bulk-weekly
- `19:49:29`   events    justhodl-sec-bulk                          rate(12 hours)         justhodl-sec-bulk-hourly
- `19:49:29`   scheduler justhodl-sec-filing-diff                   cron(0 12 * * ? *)     justhodl-sec-filing-diff-daily
- `19:49:29`   events    justhodl-sec-filings-intel                 cron(7 21 * * ? *)     sec-filings-intel-3x-daily
- `19:49:29`   scheduler justhodl-sec-midas                         rate(6 hours)          justhodl-sec-midas-weekly
- `19:49:29`   events    justhodl-snb-detail                        cron(40 11 * * ? *)    snb-detail-daily
- `19:49:29`   events    justhodl-sovereign-fiscal                  cron(0 14 ? * TUE *)   justhodl-sovereign-fiscal-sched
- `19:49:29`   events    justhodl-sovereign-fiscal                  cron(10 7 * * ? *)     sovereign-fiscal-daily
- `19:49:29`   scheduler justhodl-spx-beaters                       cron(0 13 ? * SAT *)   justhodl-spx-beaters-weekly
- `19:49:29`   scheduler justhodl-src-mirror                        rate(6 hours)          justhodl-src-mirror-daily
- `19:49:29`   scheduler justhodl-sympathetic-momentum              cron(30 23 * * ? *)    justhodl-sympathetic-momentum-daily
- `19:49:29`   events    justhodl-te-fred-mirror                    rate(1 hour)           justhodl-te-fred-mirror-hourly
- `19:49:29`   events    justhodl-tic-flows                         cron(15 14 ? * TUE *)  justhodl-tic-flows-sched
- `19:49:29`   events    justhodl-tic-flows                         cron(0 22 ? * THU *)   tic-flows-thu
- `19:49:29`   scheduler justhodl-tradingview                       cron(35 11 * * ? *)    tradingview-vault-daily
- `19:49:29`   events    justhodl-treasury-fiscal-full              cron(55 5 * * ? *)     justhodl-treasury-fiscal-daily
- `19:49:29`   scheduler justhodl-treasury-noise                    cron(45 21 ? * MON-FRI justhodl-treasury-noise-daily
- `19:49:29`   events    justhodl-treasury-noise                    cron(30 13 ? * TUE-SAT justhodl-treasury-noise-daily
- `19:49:29`   scheduler justhodl-treasury-rehypo                   cron(40 21 ? * MON-FRI treasury-rehypo-daily
- `19:49:29`   events    justhodl-vintage-fred                      cron(0 13 * * ? *)     justhodl-vintage-fred-daily
## B apply expedite map (tighten-only)

- `19:49:29` B: applied=0 skipped=78
- `19:49:29`   skip justhodl-data-census                     cron(45 12 * * ? *)  no-family
- `19:49:29`   skip justhodl-fundamental-census              cron(0 6 1,15 * ? *) no-family
- `19:49:29`   skip justhodl-census-us                       rate(15 minutes)     no-family
- `19:49:29`   skip justhodl-nyfed-full-history              cron(40 4 * * ? *)   no-family
- `19:49:29`   skip justhodl-treasury-rehypo                 cron(40 21 ? * MON-F cron-skip
- `19:49:29`   skip justhodl-sec-filing-diff                 cron(0 12 * * ? *)   no-family
- `19:49:29`   skip justhodl-treasury-noise                  cron(45 21 ? * MON-F cron-skip
- `19:49:29`   skip justhodl-fi-census                       cron(0 7 2,16 * ? *) no-family
- `19:49:29`   skip justhodl-air-cargo                       cron(40 10 * * ? *)  no-family
- `19:49:29`   skip justhodl-nyfed-markets-full              rate(1 hour)         already<=target
- `19:49:29`   skip justhodl-sdmx-walker                     rate(1 hour)         already<=target
- `19:49:29`   skip justhodl-portwatch                       cron(20 11 * * ? *)  no-family
- `19:49:29`   skip justhodl-ofr-stfm                        rate(6 hours)        already<=target
- `19:49:29`   skip justhodl-portwatch                       cron(10 12 * * ? *)  no-family
- `19:49:29`   skip justhodl-sympathetic-momentum            cron(30 23 * * ? *)  no-family
- `19:49:29`   skip justhodl-gov-sources                     cron(50 11 * * ? *)  cron-skip
- `19:49:29`   skip justhodl-sdmx-walker                     cron(15 3 ? * SUN *) cron-skip
- `19:49:29`   skip justhodl-freight-pulse                   cron(50 11 * * ? *)  no-family
- `19:49:29`   skip justhodl-sec-bulk                        cron(20 4 * * ? *)   cron-skip
- `19:49:29`   skip justhodl-bis-crossborder                 cron(40 9 ? * MON *) cron-skip
- `19:49:29`   skip justhodl-sec-midas                       rate(6 hours)        already<=target
- `19:49:29`   skip justhodl-spx-beaters                     cron(0 13 ? * SAT *) cron-skip
- `19:49:29`   skip justhodl-sec-bulk                        cron(0 9 ? * MON *)  cron-skip
- `19:49:29`   skip justhodl-etf-census                      cron(30 6 2,16 * ? * no-family
- `19:49:29`   skip justhodl-hot-money                       cron(50 9 * * ? *)   no-family
- `19:49:29`   skip justhodl-import-sentinel                 rate(10 minutes)     already<=target
- `19:49:29`   skip justhodl-src-mirror                      rate(6 hours)        already<=target
- `19:49:29`   skip justhodl-beaters-grader                  cron(0 15 ? * SAT *) cron-skip
- `19:49:29`   skip justhodl-nyfed-markets-full              rate(1 hour)         already<=target
- `19:49:29`   skip justhodl-sdmx-walker                     rate(15 minutes)     already<=target
- `19:49:29`   skip justhodl-tradingview                     cron(35 11 * * ? *)  no-family
- `19:49:29`   skip bea-economic-agent                       cron(15 14 * * ? *)  cron-skip
- `19:49:29`   skip bls-labor-agent                          cron(0 14 * * ? *)   cron-skip
- `19:49:29`   skip justhodl-boj-detail                      cron(20 11 * * ? *)  cron-skip
- `19:49:29`   skip census-economic-agent                    cron(30 14 * * ? *)  no-family
- `19:49:29`   skip cftc-futures-positioning-agent           rate(6 hours)        already<=target
- `19:49:29`   skip justhodl-ecb-detail                      cron(0 11 * * ? *)   cron-skip
- `19:49:29`   skip ecb-auto-updater                         cron(0 6 ? * MON *)  cron-skip
- `19:49:29`   skip justhodl-edgar-authority                 cron(0 13 * * ? *)   cron-skip
- `19:49:29`   skip eia-energy-agent                         cron(40 11 * * ? *)  cron-skip
- `19:49:29` verify: 0/0 re-described == new (bad=[])
- `19:49:29` cost: ~1073 -> ~1073 scheduled import invokes/day (+0)
## C kick sweep (freshness collapses now)

- `19:49:59` C: kicked 63 import functions
- `19:49:59` cadence-map artifact written: data/warm/_audit/cadence-map.json
## D board proof (post-storm inventory only)

- `19:53:59`   post-storm mark 2026-08-23T19:53:59 -> catalog kicked; accepting only as_of >= mark (4954 v1 raced a mid-storm inventory)
- `19:54:29`   t+  30s inventory 2026-08-23T19:49:53 < mark
- `19:54:59`   t+  60s inventory 2026-08-23T19:49:53 < mark
- `19:55:29`   t+  90s inventory 2026-08-23T19:49:53 < mark
- `19:56:00`   t+ 120s inventory 2026-08-23T19:49:53 < mark
- `19:56:30`   fresher: tradingview-vault-live 8.0h -> 0.0h
- `19:56:30` D FAIL fresh<=1h: 38 -> 9 (as_of 2026-08-23T19:53:59) · sec-midas 0.1h
- `19:56:30` ops 4954 RED: B apply<8; D board
