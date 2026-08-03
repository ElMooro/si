# ops 4321 -- who is 1.10, really

**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-08-03T16:26:14+00:00  

## Log
- `16:26:13` fresh doc generated_at=2026-08-03T16:13:08.894678+00:00
- `16:26:13` ✅ valuation: {"pe_ttm": 27.47, "pe_5yr_avg": 23.5, "pb_ttm": 9.55, "ps_ttm": 13.84, "pfcf_ttm": 54.95, "ev_ebitda": 18.2, "peg_ratio": 0.52, "fcf_yield_pct": 1.82, "div_yield_pct": 0.93, "roe_ttm_pct": 39.35, "roic_ttm_pct": 27.15, "dcf_estimate": 147.76, "dcf_upside_pct": -63.4, "analyst_pt_median": 600.0, "analyst_pt_high": 700.0, "analyst_pt_low": 500.0, "analyst_pt_upside_pct": 48.8}
- `16:26:13` ✅ stored pe_ttm=27.47 -> live ratios-ttm fields equal to it: {'priceToEarningsRatioTTM': 27.471890576098296, 'priceToEarningsDilutedRatioTTM': 27.471890576098296}
- `16:26:14` key-metrics-ttm fields equal to it: {}
## call site + fetch definition

- `16:26:14` compute_valuation call:
1562-    }
1563-
1564-
1565:def compute_valuation(profile: dict, ratios_ttm: dict, key_ttm: dict,
1566-                       ratios_annual: list, dcf: dict, pt_consensus: dict,
1567-                       quote: dict) -> dict:
1568-    """Pull all the valuation metrics into one section."""
--
2980-    returns           = compute_returns(prices_eod, current_price)
2981-    balance_qual      = compute_balance_quality(balance_annual)
2982-    cf_qual           = compute_cf_quality(income_annual, cashflow_annual)
2983:    valuation         = compute_valuation(profile_obj, ratios_ttm, key_ttm,
2984-                                            ratios_annual, dcf, pt_consensus, quote_obj)
2985-    health            = compute_financial_health(scores, ratios_ttm, key_ttm,
2986-                                                   balance_qual, cf_qual,

- `16:26:14` ratios_ttm fetch def:
18-  - cash-flow-statement (20y annual)
19-  - ratios (15y annual)          → P/E, P/B, ROE, ROA, ROIC, margins
20:  - ratios-ttm                   → current TTM ratios
21-  - key-metrics (15y annual)     → market cap, EV, FCF yield, debt/equity
22-  - key-metrics-ttm              → current
--
1164-        "cashflow_annual":  ("cash-flow-statement", {"symbol": ticker, "period": "annual", "limit": 20}),
1165-        "ratios_annual":    ("ratios", {"symbol": ticker, "period": "annual", "limit": 15}),
1166:        "ratios_ttm":       ("ratios-ttm", {"symbol": ticker}),
1167-        "key_metrics":      ("key-metrics", {"symbol": ticker, "period": "annual", "limit": 15}),
1168-        "key_metrics_ttm":  ("key-metrics-ttm", {"symbol": ticker}),
--
1567-                       quote: dict) -> dict:
1568-    """Pull all the valuation metrics into one section."""
1569:    # FMP /stable/ratios-ttm field names (verified by ops 1139):
1570-    #   priceToEarningsRatioTTM, priceToBookRatioTTM, priceToSalesRatioTTM,
1571-    #   priceToFreeCashFlowRatioTTM (NO 's'), enterpriseValueMultipleTTM,
1572-    #   operatingProfitMarginTTM. priceEarningsToGrowthRatioTTM doesn't exist —
1573-    #   it's priceToEarningsGrowthRatioTTM.
1574:    # ROE/ROIC live in /stable/key-metrics-ttm not ratios-ttm.
1
- `16:26:14` raw dict keys around fetch table:
612-    q = raw.get("quote"); q = q[0] if isinstance(q, list) and q else (q or {})
613:    rt = raw.get("ratios_ttm"); rt = rt[0] if isinstance(rt, list) and rt else (rt or {})
614-    km = raw.get("key_metrics_ttm"); km = km[0] if isinstance(km, list) and km else (km or {})
--
805-    quote = _first(raw.get("quote")) or {}
806:    rt = _first(raw.get("ratios_ttm")) or {}
807-    px = _safe_num(quote, "price")
--
1138-                     ("liquidity", lambda: build_liquidity(income_annual, balance_annual,
1139:                                                            cashflow_annual, raw.get("ratios_ttm"), out.get("technicals") or {})),
1140-                     ("growth_vs_mcap", lambda: build_growth_vs_mcap(raw, income_annual, cashflow_annual)),
--
1165-        "ratios_annual":    ("ratios", {"symbol": ticker, "period": "annual", "limit": 15}),
1166:        "ratios_ttm":       ("ratios-ttm", {"symbol": ticker}),
1167-        "key_metrics":      ("key-metrics", {"symbol": ticker, "period": "annual", "limit": 15}),
--
2937-    ratios_annual    = raw.get("ratios_annual") if isinstance(raw.get("ratios_annual"), list) else []
2938:    ratios_ttm       = _first(raw.get("ratios_ttm")) or {}
2939-    key_metrics      = raw.get("key_metrics") if isinstance(raw.get("key_metrics"), lis
- `16:26:14` ✅ forensics complete -- patch ships against these lines
