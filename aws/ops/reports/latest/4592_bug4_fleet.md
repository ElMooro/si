# ops 4592 — BUG-4 fleet gates (9 engines)

**Status:** failure  
**Duration:** 789.4s  
**Finished:** 2026-08-10T23:36:32+00:00  

## Error

```
SystemExit: 1
```

## Log
## 1. Settle + fire + poll

- `23:23:27`   fired 9 engines
- `23:23:39`   13f-price-divergence refreshed (12s)
- `23:23:39`   earnings-iv-crush refreshed (12s)
- `23:23:39`   forced-selling-bounce refreshed (12s)
- `23:23:39`   lockup-expiration refreshed (12s)
- `23:23:39`   post-earnings-mean-rev refreshed (12s)
- `23:23:39`   vvix-vov-regime refreshed (12s)
- `23:24:29`   ma-target-predictor refreshed (62s)
- `23:36:32` ⚠   justhodl-catalyst-skew-premove did not refresh in 780s
- `23:36:32` ⚠   justhodl-failed-pattern-reversal did not refresh in 780s
## 2. Gate contracts

- `23:36:32` ✅   [13f-price-divergence] data_sufficiency published
- `23:36:32` ✅   [13f-price-divergence] state=INSUFFICIENT_DATA (blind_by_numbers=True, ds={'feeder_loaded': True, 'n_feeder_tickers': 0})
- `23:36:32` ✗   [catalyst-skew-premove] CONTRACT MISS — data_sufficiency published
- `23:36:32` ✅   [catalyst-skew-premove] state=QUIET (blind_by_numbers=None, ds={})
- `23:36:32` ✅   [earnings-iv-crush] data_sufficiency published
- `23:36:32` ✅   [earnings-iv-crush] state=QUIET (blind_by_numbers=False, ds={'http_ok': 140, 'http_err': 0})
- `23:36:32` ✗   [failed-pattern-reversal] CONTRACT MISS — data_sufficiency published
- `23:36:32` ✅   [failed-pattern-reversal] state=QUIET (blind_by_numbers=None, ds={})
- `23:36:32` ✅   [forced-selling-bounce] data_sufficiency published
- `23:36:32` ✅   [forced-selling-bounce] state=QUIET (blind_by_numbers=False, ds={'feeds_ok': 10, 'feeds_miss': 2})
- `23:36:32` ✅   [lockup-expiration] data_sufficiency published
- `23:36:32` ✅   [lockup-expiration] state=QUIET (blind_by_numbers=False, ds={'http_ok': 1, 'http_err': 0})
- `23:36:32` ✅   [ma-target-predictor] data_sufficiency published
- `23:36:32` ✅   [ma-target-predictor] state=QUIET (blind_by_numbers=False, ds={'http_ok': 250, 'http_err': 0, 'n_results': 50})
- `23:36:32` ✅   [post-earnings-mean-rev] data_sufficiency published
- `23:36:32` ✅   [post-earnings-mean-rev] state=QUIET (blind_by_numbers=False, ds={'http_ok': 120, 'http_err': 0})
- `23:36:32` ✅   [vvix-vov-regime] data_sufficiency published
- `23:36:32` ✅   [vvix-vov-regime] state=NEUTRAL (blind_by_numbers=None, ds={})
## 3. Ledger evidence prints

- `23:36:32`   share-flows ATM warn: sec-filings-intel join fallback — feed_present=True n_tickers=0
- `23:36:32`   FRED import-health: status=['actions_this_sweep', 'duration_s', 'engine', 'generated_at', 'incidents', 'overall'] as_of=2026-08-10T23:35:04+00:00
## verdict

- `23:36:32` ✗ BUG-4 fleet gates: 4 red
