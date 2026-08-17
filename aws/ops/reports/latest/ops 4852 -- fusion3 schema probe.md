# catalyst

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-08-17T18:50:22+00:00  

## Log
- `18:50:21`   KEY=data/catalyst.json
- `18:50:21` ✅ catalyst: top keys ['as_of', 'by_ticker', 'class_census', 'doctrine', 'engine', 'macro', 'n_tickers', 'taxonomy']
- `18:50:21`   generated_at= v=None status=None
- `18:50:21`   dict 'class_census' n=6 sample key=MAJOR_CONTRACT
- `18:50:21`    1
- `18:50:21`   dict 'by_ticker' n=204 sample key=KBR
- `18:50:21`    {"catalysts": [{"class": "MAJOR_CONTRACT", "evidence": "KBR Awarded $60 Million Contract to Accelerate Air and Missile Defense Readiness", "src": "deal-scanner PR tape", "ts": "2026-08-17T18:05:50+00:00", "weight": 2.5}], "score": 2.5, "top_class": "MAJOR_CONTRACT"}
# readthrough

- `18:50:21`   KEY=data/readthrough.json
- `18:50:21` ✅ readthrough: top keys ['beneficiaries', 'by_beneficiary', 'caveats', 'chase_guard', 'consensus_coverage', 'data_source', 'degraded', 'elapsed_s', 'engine', 'events', 'generated_at', 'industry_boom_context', 'n_beneficiaries', 'n_events']
- `18:50:21`   generated_at=2026-08-17T13:20:26 v=None status=OK
- `18:50:21`   list 'events' n=3 row0:
- `18:50:21`    {"type": "BACKLOG_ORDERS", "propagation": 1.0, "order_value_usd": 465000000.0, "headline": "ERock: AI's Grid Bottleneck Can Become A Long-Lived Installed-Base Business", "published": "2026-08-17 08:28:09", "age_h": 4.9, "source": "seekingalpha.com", "ticker": "EROC", "move_pct": 6.1, "move_regular_pct": null, "move_extended_pct": null, "p...
- `18:50:21`   list 'beneficiaries' n=15 row0:
- `18:50:21`    {"ticker": "USAR", "tier": "T5_COMPETITOR_VALIDATION", "why": "same industry (Industrial Materials) — demand validated, not supplied", "edge_confidence": 0.45, "edge_source": "industry_peer", "named_edge": false, "bom_weight": 0.35, "capture_share": 0.0, "n_at_tier": 5, "company": "USA Rare Earth Inc", "tier_label": "Competitor — TAM vali...
- `18:50:21`   list 'chase_guard' n=6 row0:
- `18:50:21`    {"ticker": "SKE", "tier": "T5_COMPETITOR_VALIDATION", "why": "same industry (Industrial Materials) — demand validated, not supplied", "edge_confidence": 0.45, "edge_source": "industry_peer", "named_edge": false, "bom_weight": 0.35, "capture_share": 0.0, "n_at_tier": 5, "company": "Skeena Resources Limited", "tier_label": "Competitor — TAM...
- `18:50:21`   list 'by_beneficiary' n=10 row0:
- `18:50:21`    {"ticker": "TNC", "implied_order_usd_total": 0, "catalysts": ["EROC", "SERV"], "best_tier": "T5_COMPETITOR_VALIDATION", "max_score": 44.0, "quadrant": "CONSENSUS_NOT_DUE_YET"}
- `18:50:21`   dict 'quadrant_counts' n=8 sample key=TWICE_UNPRICED
- `18:50:21`    0
- `18:50:21`   dict 'industry_boom_context' n=131 sample key=Computer Hardware
- `18:50:21`    81.7
# backlog

- `18:50:21`   data/backlog-miner.json absent
- `18:50:22`   KEY=data/backlog.json
- `18:50:22` ✅ backlog: top keys ['accelerating', 'by_ticker', 'cap_distribution', 'cheap_vs_backlog', 'duration_s', 'engine', 'generated_at', 'ledger_size', 'method', 'n_covered', 'slice_this_run', 'sources', 'version']
- `18:50:22`   generated_at=2026-08-17T11:30:46 v=None status=None
- `18:50:22`   dict 'by_ticker' n=121 sample key=CETX
- `18:50:22`    {"ticker": "CETX", "sector": "Technology", "cap_bucket": "nano", "group": "Software/Semis", "cik": "0001435064", "deferred_rev": 2348384.0, "deferred_asof": "2026-03-31", "deferred_filed": "2026-05-15", "deferred_qoq": 52.3, "deferred_yoy": 22.0, "deferred_accelerating": true, "rev_yoy": -33.7}
- `18:50:22`   list 'accelerating' n=30 row0:
- `18:50:22`    {"ticker": "MU", "sector": "Technology", "cap_bucket": "mega", "group": "Software/Semis", "cik": "0000723125", "eps": 24.67, "eps_qoq": 104.4, "eps_yoy": 1368.5, "eps_asof": "2026-05-28", "rpo": 5000000000.0, "rpo_qoq": 2083.4, "rpo_yoy": 904.0, "rpo_tag": "RevenueRemainingPerformanceObligation", "rpo_asof": "2026-05-28", "rpo_filed": "20...
- `18:50:22`   list 'cheap_vs_backlog' n=25 row0:
- `18:50:22`    {"ticker": "HII", "sector": "Industrials", "cap_bucket": "large", "group": "Industrials", "cik": "0001501585", "rpo": 57300000000.0, "rpo_qoq": 6.1, "rpo_yoy": 0.7, "rpo_tag": "RevenueRemainingPerformanceObligation", "rpo_asof": "2026-06-30", "rpo_filed": "2026-07-30", "rpo_form": "10-Q", "rev_yoy": 10.9, "ev_to_rpo": 0.28, "rpo_minus_rev...
# est_revisions

- `18:50:22`   KEY=data/estimate-revisions.json
- `18:50:22` ✅ est_revisions: top keys ['caveats', 'data_source', 'direction_map', 'downward_revisions', 'elapsed_s', 'engine', 'estimate_strength_leaders', 'generated_at', 'horizon_days', 'n_fmp_enriched', 'n_state_keys', 'n_tracked', 'n_with_history', 'status']
- `18:50:22`   generated_at=2026-08-17T17:40:23 v=None status=LIVE
- `18:50:22`   dict 'direction_map' n=831 sample key=LIN
- `18:50:22`    "FLAT"
- `18:50:22`   list 'estimate_strength_leaders' n=40 row0:
- `18:50:22`    {"ticker": "ARQT", "company": "Arcutis Biotherapeutics", "earnings_date": "2026-10-27", "session": "BMO", "days_to_earnings": 71, "fiscal_period": "Q3", "fiscal_year": 2026, "importance": 3, "current_eps_est": 0.16, "baseline_eps_est": 0.15, "eps_rev_pct": 6.67, "eps_rev_recent_pct": 0.0, "rev_rev_pct": 0.0, "revenue_confirms": false, "ba...
- `18:50:22`   list 'upward_revisions' n=28 row0:
- `18:50:22`    {"ticker": "PHAT", "company": "Phathom Pharmaceuticals", "earnings_date": "2026-10-29", "session": "BMO", "days_to_earnings": 73, "fiscal_period": "Q3", "fiscal_year": 2026, "importance": 2, "current_eps_est": 0.04, "baseline_eps_est": 0.03, "eps_rev_pct": 33.33, "eps_rev_recent_pct": 0.0, "rev_rev_pct": 0.0, "revenue_confirms": false, "b...
- `18:50:22`   list 'downward_revisions' n=30 row0:
- `18:50:22`    {"ticker": "NTGR", "company": "Netgear", "earnings_date": "2026-10-28", "session": "AMC", "days_to_earnings": 72, "fiscal_period": "Q3", "fiscal_year": 2026, "importance": 2, "current_eps_est": -0.07, "baseline_eps_est": 0.01, "eps_rev_pct": -800.0, "eps_rev_recent_pct": 0.0, "rev_rev_pct": 0.0, "revenue_confirms": true, "baseline_date": ...
# verdict

- `18:50:22` ✅ schemas dumped -- Fusion 3 design binds ONLY what is printed above
