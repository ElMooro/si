# ops 4650 — stock-buying recon

**Status:** success  
**Duration:** 68.7s  
**Finished:** 2026-08-13T16:34:18+00:00  

## Log
## census-store discovery

- `16:34:18` candidates: ['data/_state/fundamental-census-cursor.json', 'data/ai-commentary/fundamentals.json', 'data/ai-commentary/history/fundamentals/2026-06-02.json', 'data/ai-commentary/history/fundamentals/2026-06-30.json', 'data/ai-commentary/history/fundamentals/2026-07-01.json', 'data/ai-commentary/history/fundamentals/2026-07-02.json', 'data/ai-commentary/history/fundamentals/2026-07-03.json', 'data/ai-commentary/history/fundamentals/2026-07-06.json', 'data/ai-commentary/history/fundamentals/2026-07-07.json', 'data/ai-commentary/history/fundamentals/2026-07-08.json', 'data/ai-commentary/history/fundamentals/2026-07-09.json', 'data/ai-commentary/history/fundamentals/2026-07-10.json', 'data/ai-commentary/history/fundamentals/2026-07-13.json', 'data/ai-commentary/history/fundamentals/2026-07-14.json', 'data/ai-commentary/history/fundamentals/2026-07-15.json', 'data/ai-commentary/history/fundamentals/2026-07-16.json', 'data/ai-commentary/history/fundamentals/2026-07-17.json', 'data/ai-commentary/history/fundamentals/2026-07-20.json', 'data/ai-commentary/history/fundamentals/2026-07-21.json', 'data/ai-commentary/history/fundamentals/2026-07-22.json', 'data/ai-commentary/history/fundamentals/2026-07-23.json', 'data/ai-commentary/history/fundamentals/2026-07-24.json', 'data/ai-commentary/history/fundamentals/2026-07-27.json', 'data/ai-commentary/history/fundamentals/2026-07-28.json']
## shapes

- `16:34:18` data/fundamental-census.json -> {"ok": true, "version": "1.11.0", "generated_at": "2026-08-01T06:48:08.0182", "elapsed_s": 73.0, "cadence": "1st + 15th monthly, 06:0", "top_quality": ["len=50", {"t": "str", "sector": "str", "score": "int", "n_elite": "int", "n_green": "int", "n_red": "int", "top_elites": "list"}], "bottom_quality": ["len=50", {"t": "str", "sector": "str
- `16:34:18` data/census.json -> {"__err": "An error occurred (NoSuc"}
- `16:34:18` data/industry-boom.json -> {"engine": "industry-boom", "version": "1.1.0", "generated_at": "2026-08-13T10:50:04.3620", "n_industries": 132, "n_universe": 5239, "league": ["len=132", {"industry": "str", "sector": "str", "n": "int", "mcap_b": "float", "boom_score": "float", "n_component_families": "int", "coverage_w": "float"}], "trouble": ["len=10", {"industry": "st
- `16:34:18` data/deal-scanner.json -> {"engine": "deal-scanner", "version": "3.2.1", "generated_at": "2026-08-13T15:05:24.7168", "window": "rolling PR + news tape (", "summary": {"n_prs_scanned": 3599, "n_deals": 12, "n_with_size": 10, "n_small_cap": 9, "n_high_materiality": 3, "n_green": 6, "n_yellow": 0}, "deals": ["len=12", {"symbol": "str", "title": "str", "publisher": "s
- `16:34:18` data/_ma200/closes.json -> {"dates": ["len=235", "2025-09-04"], "series": {"IBKR": ["len=235", "float"], "ELF": ["len=235", "float"], "DIA": ["len=235", "float"], "XLB": ["len=235", "float"], "DOW": ["len=235", "float"], "XOP": ["len=235", "float"], "GEHC": ["len=235", "float"]}}
- `16:34:18` data/_state/fundamental-census-cursor.json -> {"cursor": 504, "universe": 498, "depth": 3, "version": "1.11.0", "at": "2026-08-01T06:42:54.6292"}
## census row columns (first row sample)

## FMP availability

- `16:34:18` FMP key present: False (len=0)
- `16:34:18` ✅ recon complete — build wires on the shapes above
