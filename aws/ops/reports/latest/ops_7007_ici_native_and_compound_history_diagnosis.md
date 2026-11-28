# ops 7007 - why data/ici-flows.json and data/compound-history.json stopped (read-only, 2026-11-28)

**Status:** success  
**Duration:** 2.8s  
**Finished:** 2026-11-28T11:23:12+00:00  

## Log
## 1. ICI native packet: replay the research model on the retained originals of the newest requests

- `11:23:09` 19 request journals; newest 6:
- `11:23:09`   aa1907fbac518836 11.6 h: status failed error_type ValueError attempts 2 sources {'combined_flows': (200, '2026-11-27T23:44:28.424855+00:00'), 'mmf': (200, '2026-11-27T23:44:28.732645+00:00')}
- `11:23:10`     original combined_flows: 355150 B, identity ok
- `11:23:10`     original mmf: 355057 B, identity ok
- `11:23:10`     parse_release(combined_flows): release 2026-11-25 observations ['2026-10-21', '2026-10-28', '2026-11-04', '2026-11-11', '2026-11-18']
- `11:23:10`     parse_release(mmf): release 2026-11-25 observations ['2026-11-11', '2026-11-18', '2026-11-24']
- `11:23:10`     reconcile(combined_flows): ok
- `11:23:10` ⚠     reconcile(mmf) -> ValueError: Weekly reported-change endpoints required
- `11:23:10`     mmf printed date columns: ['2026-11-24', '2026-11-18', '2026-11-11']
- `11:23:10` ⚠     compile_releases -> ValueError: Bounded acquisition window required
- `11:23:10`   4d7782061c7100d7 32.1 h: status failed error_type ValueError attempts 2 sources {'combined_flows': (200, '2026-11-27T03:16:20.937819+00:00'), 'mmf': (200, '2026-11-27T03:16:21.280507+00:00')}
- `11:23:10`     original combined_flows: 355123 B, identity ok
- `11:23:10`     original mmf: 355031 B, identity ok
- `11:23:10`     parse_release(combined_flows): release 2026-11-25 observations ['2026-10-21', '2026-10-28', '2026-11-04', '2026-11-11', '2026-11-18']
- `11:23:11`     parse_release(mmf): release 2026-11-25 observations ['2026-11-11', '2026-11-18', '2026-11-24']
- `11:23:11`     reconcile(combined_flows): ok
- `11:23:11` ⚠     reconcile(mmf) -> ValueError: Weekly reported-change endpoints required
- `11:23:11`     mmf printed date columns: ['2026-11-24', '2026-11-18', '2026-11-11']
- `11:23:11` ⚠     compile_releases -> ValueError: Bounded acquisition window required
- `11:23:11`   77470a19ca77353d 56.3 h: status failed error_type ValueError attempts 2 sources {'combined_flows': (200, '2026-11-26T03:04:17.678792+00:00'), 'mmf': (200, '2026-11-26T03:04:17.956611+00:00')}
- `11:23:11`     original combined_flows: 355123 B, identity ok
- `11:23:11`     original mmf: 355030 B, identity ok
- `11:23:11`     parse_release(combined_flows): release 2026-11-25 observations ['2026-10-21', '2026-10-28', '2026-11-04', '2026-11-11', '2026-11-18']
- `11:23:11`     parse_release(mmf): release 2026-11-25 observations ['2026-11-11', '2026-11-18', '2026-11-24']
- `11:23:11`     reconcile(combined_flows): ok
- `11:23:11` ⚠     reconcile(mmf) -> ValueError: Weekly reported-change endpoints required
- `11:23:11`     mmf printed date columns: ['2026-11-24', '2026-11-18', '2026-11-11']
- `11:23:11` ⚠     compile_releases -> ValueError: Bounded acquisition window required
- `11:23:11`   35fc3c60f2ed58f3 158.2 h: status complete error_type None attempts 2 sources {'combined_flows': (200, '2026-11-21T21:08:18.298480+00:00'), 'mmf': (200, '2026-11-21T21:08:18.848692+00:00')}
- `11:23:11`   3ab5119d6cb5cb21 181.0 h: status complete error_type None attempts 2 sources {'combined_flows': (200, '2026-11-20T22:21:15.201375+00:00'), 'mmf': (200, '2026-11-20T22:21:15.468806+00:00')}
- `11:23:11`   ea4c168fa3974824 201.5 h: status complete error_type None attempts 2 sources {'combined_flows': (200, '2026-11-20T01:53:57.621881+00:00'), 'mmf': (200, '2026-11-20T01:53:57.934654+00:00')}
- `11:23:11` ⚠ ICI native packet refusal(s) reproduced: ['ValueError: Bounded acquisition window required', 'ValueError: Weekly reported-change endpoints required']
## 2. compound-aggregator: which input withholds the history write

- `11:23:12` data/compound-signals.json 14.1 h old: generated_at 2026-11-27T21:15:51+00:00 | top-level keys ['activist_research_exclusion', 'calls_eligible', 'compound', 'duration_s', 'feed_stats', 'feed_stats_definition', 'forecast_qualified', 'generated_at', 'history_comparability', 'holdings_exclusions', 'input_evidence', 'momentum_research_exclusion', 'new_alerts', 'notifications_suppressed', 'numeric_contract', 'overlay_contract', 'overlay_evidence', 'schema_version', 'score_basis', 'sizing_eligible', 'stats', 'volatility_research_exclusion', 'withheld']
- `11:23:12`   withheld: []
- `11:23:12`   overlay_evidence: {"contract": "compound-overlays.v1", "evaluation_at": "2026-11-27T21:15:51.485603+00:00", "sources": {"data/trend-reversal.json": {"source": "data/trend-reversal.json", "status": "received", "original_bytes_retained": false, "source_qualified": false, "source_sha256": "97949f16f8cc2bf2bba23657a7ccda7dd803c22192e75e2c9177d7c8a27bf69e", "source_bytes": 587319, "received_at": "2026-11-27T21:15:51.174349+00:00", "whole_source_parsed": true, "reported_generated_at": "2026-11-27T20:53:07+00:00"}, "data/compound-firstseen.json": {"source": "data/compound-firstseen.json", "status": "received", "original_bytes_retained": false, "source_qualified": false, "source_sha256": "5976d6cb47b9b6ea3ae6cdcb42a01fdad32c2fcabd01ba9e198dc234445bba20", "source_bytes": 30809, "received_at": "2026-11-27T21:15:51.301995+00:00", "whole_source_parsed": true, "reported_generated_at": null}, "data/risk-gate.json": {"source": "data/risk-gate.json", "status": "received", "original_bytes_retained": false, "source_qualified": false, "source_sha256": "3746e7b9ffc70870eefd8fefe3e0e1f104d2a791ffb547ae2dab478f509332d4", "source_bytes": 424486, "received_at": "2026-11-27T21:15:51.405469+00:00", "whole_source_parsed": tru
- `11:23:12`   input_evidence: 13 inputs, 1 not usable/empty/excluded
- `11:23:12` ⚠   incomplete input options_flow: {"source": "data/options-flow-scanner.json", "collection": "summary.top_25_overall", "status": "unavailable", "reason": "source_read_or_validation_failed", "occurrences": [], "selected_count": null, "usable_count": 0, "selection_scope": "declared_collection_only", "source_qualified": false}
- `11:23:12`   data/compound-history.json 182.1 h old, 79691 B
- `11:23:12` ✅ read-only; nothing invoked, nothing written
