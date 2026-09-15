# ops 5579 -- CryptoQuant + OpenFIGI: SSM truth, probes, feeds, labels (no tokens)

**Status:** success  
**Duration:** 10.8s  
**Finished:** 2026-09-15T04:24:25+00:00  

## Log
- `04:24:15` ✅ SSM cryptoquant  /justhodl/cryptoquant_api      sha=a1ac6eb761 len=40
- `04:24:15` ✅ SSM openfigi     /justhodl/openfigi/api-key     sha=60d263cfa7 len=36
- `04:24:24` ✅ env rows for CryptoQuant/OpenFIGI: 0 (none -- SSM only)
- `04:24:24` ✅ probe CQ mvrv http=200 n=2
- `04:24:24` ✅ probe CQ exchange netflow http=200 n=2
- `04:24:25` ✅ data/cryptoquant-onchain.json: 59938 bytes, generated_at=2026-09-14T21:05:07+00:00 status=EOD n_metrics=54 keys=['btc_exchange_netflow', 'btc_exchange_inflow', 'btc_exchange_outflow', 'btc_exchange_reserve', 'btc_exchange_addr_in', 'btc_mpi', 'btc_whale_ratio', 'btc_fund_flow_ratio', 'btc_stablecoins_ratio', 'btc_exchange_supply_ratio', 'btc_mvrv', 'btc_sopr']
- `04:24:25` ✅ data/cryptoquant-series.json: 994903 bytes, series={}
- `04:24:25` ✅ data/history/cryptoquant.json: 629337 bytes
- `04:24:25` ✅ probe OpenFIGI AAPL http=200 figi=BBG000B9XRY4
- `04:24:25` ✅ data/symbology/master.json: 3816209 bytes, 10426 tickers, figi mapped=9273 no_match=1153; AAPL fields=['cik', 'cusip', 'figi', 'figi_name', 'isin', 'lei', 'name', 'sedol', 'source', 'ticker']
- `04:24:25`   AAPL: figi=BBG000B9XRY4 shareClassFIGI=None type=None exch=None status=None
- `04:24:25` ✅ leak record: ops_4197_paid_keys.py literals matched LIVE SSM values (cryptoquant a1ac6eb761, te_api d33fcfd2ca) -> redacted in git; ROTATE in the vendor consoles, put new values in SSM only
- `04:24:25` ✅ GREEN -- CryptoQuant + OpenFIGI keyed from SSM, live, feeds present, labels honest
