# Census discovery from All Providers

Source commit: 86d95dae568d0993837614bba0f3505d2e187e99. The chart now exposes the reviewed Census economic histories from its All Providers picker. The original Census warehouse entry remains available and links to the same reviewed histories. Stored-file requests use the real `census-us` catalogue; exact chart series keep their `census` identity.

The additional chart-adapter entry is labelled separately from published warehouse metadata. Original metadata downloads and hashes are unchanged. Directory listing or receipt time does not establish observation freshness or historical coverage.

All 4,789 frontend tests, 280 native tests and 1,075 deployment tests plus 15 shell checks passed. A browser test timing defect was corrected by waiting for the asynchronous search render; production code and every assertion were retained. Whole served chart HTML, 49 static files and 20 complete chart modules match the intended build. Sixteen isolated browser suites passed on the served source, including the actual provider picker, dataset search, chart points, stored-file pagination, mismatched identity rejection and endpoint denial.

Post-release verification independently reconstructed three public Census histories from their complete original ZIP sources and checked 11 public metadata responses. The native engine remains the already accepted 9253a12e74e5fb76bb4c0af59a4669e0fb9eaddf; this was a page-only change. No provider substitution, source-unit conversion, history backfill, producer invocation or trading authority was introduced. The watchlist routing audit remains 7,028 routed / 3,717 unresolved; routing is not historical coverage.
