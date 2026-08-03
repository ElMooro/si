# ops 4302 -- rehypothecation + trend-reversal, live

**Status:** success  
**Duration:** 27.6s  
**Finished:** 2026-08-03T00:08:04+00:00  

## Data

| dir | latest | leg | n | score | signals | src | ticker | trend | z |
|---|---|---|---|---|---|---|---|---|---|
|  | 0.0 | sofr_iorb | 104 |  |  | FRED SOFR-IORB (bps) |  |  | 0.66 |
|  | -0.0 | rrp_drain_4w | 104 |  |  | FRED RRPONTSYD Δ4w ($bn) |  |  | 0.02 |
| TOP_FORMING |  |  |  | 31.3 | slope_flip, macd_flip, structure |  | NVDA | UP |  |
| BOTTOM_FORMING |  |  |  | 29.0 | ma_break, donchian_break |  | BSX | DOWN |  |
| TOP_FORMING |  |  |  | 28.0 | ma_break, structure |  | IWM | UP |  |
| TOP_FORMING |  |  |  | 25.0 | slope_flip, structure |  | EEM | UP |  |
| TOP_FORMING |  |  |  | 25.0 | slope_flip, structure |  | TSM | UP |  |
| BOTTOM_FORMING |  |  |  | 25.0 | slope_flip, structure |  | GILD | DOWN |  |

## Log
## 1. treasury-rehypo

- `00:07:42` function: env-repaired+verified
- `00:07:54` run: {"ok": true, "composite": 54.0, "band": "WATCH", "legs": ["sofr_iorb", "rrp_drain_4w"], "missing": ["ofr: gcf/tri unresolved even after catalog discovery"]}
- `00:07:55` COMPOSITE 54.0 (WATCH) · legs: ['sofr_iorb', 'rrp_drain_4w'] · missing: ['ofr: gcf/tri unresolved even after catalog discovery']
- `00:07:55` catalog picks: {'fails': 6, 'sec_in': 8, 'sec_out': 8, 'net_pos': 8}
- `00:07:55` schedule: present
## 2. trend-reversal

- `00:08:01` function: env-repaired+verified
- `00:08:03` run: {"ok": true, "n": 24, "hot": 1, "top": [["NVDA", 31.3], ["BSX", 29.0], ["IWM", 28.0]]}
- `00:08:04` universe 24 · hot(>=30) 1 · errors None
- `00:08:04` schedule: present
## 3. desk v2.3.3 -- both wired + RRG retry

## RESULT

- `00:08:04` ✗   rehypo legs 2 < 3 (missing=['ofr: gcf/tri unresolved even after catalog discovery'])
