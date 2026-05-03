# Verify 13F AUMs are realistic now

**Status:** failure  
**Duration:** 0.7s  
**Finished:** 2026-05-03T17:15:05+00:00  

## Error

```
Traceback (most recent call last):
  File "/home/runner/work/si/si/aws/ops/ops_report.py", line 97, in report
    yield r
  File "/home/runner/work/si/si/aws/ops/pending/_verify_13f_aum_real.py", line 42, in main
    r.log(f"    {x.get('ticker','?'):8s} {x.get('name','')[:30]:30s} {x.get('n_funds_holding')} hold  +{n_buy} buying  -{n_sell} selling  ${x.get('total_value', 0)/1e9:.1f}B")
                ^^^^^^^^^^^^^^^^^^^^^^^^
TypeError: unsupported format string passed to NoneType.__format__

```

## Log
- `17:15:05`   generated_at: 2026-05-03T17:13:13+00:00
- `17:15:05`   funds_parsed: 16 / 18
## Per-fund AUM (16 funds)

- `17:15:05`     CITADEL          6510 pos  AUM $    671.7B
- `17:15:05`     BERKSHIRE          42 pos  AUM $    274.2B
- `17:15:05`     AQR              3562 pos  AUM $    190.7B
- `17:15:05`     DURATION         2311 pos  AUM $    168.0B
- `17:15:05`     POINT72          2549 pos  AUM $     90.7B
- `17:15:05`     TWO_SIGMA        3534 pos  AUM $     77.2B
- `17:15:05`     RENAISSANCE      3185 pos  AUM $     72.7B
- `17:15:05`     COATUE             52 pos  AUM $     40.0B
- `17:15:05`     TIGER_GLOBAL       54 pos  AUM $     29.7B
- `17:15:05`     BRIDGEWATER      1040 pos  AUM $     27.4B
- `17:15:05`     PERSHING           11 pos  AUM $     15.5B
- `17:15:05`     BAUPOST            32 pos  AUM $     13.6B
- `17:15:05`     SOROS             237 pos  AUM $      8.6B
- `17:15:05`     LONE_PINE          22 pos  AUM $      5.3B
- `17:15:05`     GREENLIGHT         40 pos  AUM $      2.1B
- `17:15:05`     SCION               8 pos  AUM $      1.4B
## Top changes (most-bought + most-sold)

- `17:15:05`   Most bought (top 10):
- `17:15:05`     AAPL     Apple Inc                      14 hold  +14 buying  -0 selling  $93.5B
- `17:15:05`     AMZN     Amazon.com Inc                 14 hold  +14 buying  -0 selling  $32.7B
- `17:15:05`     GOOGL    Alphabet Inc Class A           12 hold  +12 buying  -0 selling  $30.4B
- `17:15:05`     PEP      PepsiCo                        12 hold  +12 buying  -0 selling  $3.2B
- `17:15:05`     NVDA     Nvidia                         11 hold  +11 buying  -0 selling  $53.4B
- `17:15:05`     MSFT     Microsoft                      11 hold  +11 buying  -0 selling  $31.6B
- `17:15:05`     AVGO     Broadcom Inc                   11 hold  +11 buying  -0 selling  $20.0B
