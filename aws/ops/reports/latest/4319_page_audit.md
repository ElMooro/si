# ops 4319 v2 -- the real research docs, audited

**Status:** success  
**Duration:** 9.3s  
**Finished:** 2026-08-03T16:05:55+00:00  

## Log
- `16:05:46` prefix holds 81 docs; 6 freshest: [('VOO.json', '2026-08-03 08:06'), ('QQQ.json', '2026-08-03 08:05'), ('SPY.json', '2026-08-03 08:05'), ('T.json', '2026-08-03 08:05'), ('DIS.json', '2026-08-03 08:05'), ('VZ.json', '2026-08-03 08:05')]
- `16:05:54` ✅ MISPRICED DOC: TSM (S3 LastModified 2026-07-07 19:31:11+00:00, doc generated_at=2026-07-07T19:31:10.864762+00:00)
- `16:05:54` stored quote: {"price": 430.1, "change_pct": -4.8009, "volume": 11247737.0, "avg_volume": null, "day_low": 428.12, "day_high": 439.8, "year_low": 223.7, "year_high": 479.0}
- `16:05:54` valuation keys: ['pe_ttm', 'pe_5yr_avg', 'pb_ttm', 'ps_ttm', 'pfcf_ttm', 'ev_ebitda', 'peg_ratio', 'fcf_yield_pct', 'div_yield_pct', 'roe_ttm_pct', 'roic_ttm_pct', 'dcf_estimate', 'dcf_upside_pct', 'analyst_pt_median', 'analyst_pt_high', 'analyst_pt_low'] | pe_ttm=32.8 pe=None
- `16:05:54` ownership: {}
- `16:05:55` ✅ LIVE quote: price=403.65 day=398.2001-406.67 52w=223.7-479
- `16:05:55` ✅ LIVE ratios: peTTM=27.471890576098296 | fields matching stored pe_ttm=32.8 -> {}
## writer forensics (repo-wide)

- `16:05:55` writers of the prefix:
aws/lambdas/justhodl-research-backtest/source/lambda_function.py
aws/lambdas/justhodl-analytics-snapshot/source/lambda_function.py
aws/lambdas/justhodl-analytics-snapshot/config.json
aws/lambdas/justhodl-equity-research/source/lambda_function.py
aws/lambdas/justhodl-research-critique/source/lambda_function.py
aws/lambdas/justhodl-flows-ai-analysis/source/lambda_function.py

- `16:05:55` quote build:
171-            if entry_doc:
172-                gen_at = entry_doc.get("generated_at") or f"{oldest_date}T00:00:00+00:00"
173:                entry_price = (entry_doc.get("quote") or {}).get("price")
174-                # Verdict from oldest (the original call to evaluate)
175-                verdict = entry_doc.get("verdict") or {}
176-            else:
177-                gen_at = latest_doc.get("generated_at")
178:                entry_price = (latest_doc.get("quote") or {}).get("price")
179-                verdict = latest_doc.get("verdict") or {}
180-        else:
181-            # No history — use latest (will show 0% return)
182-            gen_at = latest_doc.get("generated_at")
183:            entry_price = (latest_doc.get("quote") or {}).get("price")
184-            verdict = latest_doc.get("verdict") or {}
185-
186-        if not entry_price or not gen_at:
187-            continue
188-

- `16:05:55` pe_ttm build:

- `16:05:55` ownership build:

- `16:05:55` cache policy:
135-
136-def build_per_call_attribution(now_prices: dict, spy_now: Optional[float],
137:                                spy_then_cache: dict) -> list:
138-    """For each research file, compute return + alpha attribution.
139-
140-    Returns list of dicts, one per (ticker, generated_at) pair.
141-
142-    Strategy: use the OLDEST historical snapshot as the "entry" point so
--
210-        days = days_between(gen_at, datetime.now(timezone.utc).isoformat())
211-
212:        spy_then = spy_then_cache.get(gen_at[:10])
213-        spy_ret = pct_change(spy_then, spy_now) if (spy_then and spy_now) else None
214-        alpha = round(ticker_ret - spy_ret, 2) if (ticker_ret is not None and spy_ret is not None) else None
215-
216-        # Capture regime stamp from entry snapshot (the regime active when
217-        # the call was made). Falls back to latest if no entry doc.
--
510-
511-    # 4. Build per-call attribution
512:    spy_then_cache = {}  # date -> SPY price
513-    for k in research_keys:
514-        doc = read_s3_json(k)
515-        if doc and doc.get("generated_at"):
516-        
- `16:05:55` ✅ AUDIT v2 COMPLETE
