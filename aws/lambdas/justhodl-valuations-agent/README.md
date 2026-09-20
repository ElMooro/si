# Valuation research

The native producer reconstructs available declared FRED inputs from the canonical original-source archive. It publishes `valuations-data.json`, immutable runs/inputs/outputs/compiler artifacts under `data/valuation-research/`, and idempotent request status. Provider originals and whole predecessors remain protected. HTTP requests only read the current publication.

Observation periods, source units, acquisition clocks and exact historical row indices survive. Historical sample means, standard deviations and midrank percentiles are descriptive; same-date spreads preserve their units. Seasonally adjusted CPI change needs the exact twelve-month baseline. Negative energy prices and zero rates are legitimate values. Missing, stale or unqualified inputs cannot become fixed prices, fair-value defaults, trade scores or forecast probabilities.

CAPE, a properly scoped equity-market-cap/GDP numerator, physical metal spot sources, reconciled ETF backing and the crypto market-cap source still require separate original-source qualification. The complete 18,441-byte predecessor is preserved unimported as `source/legacy_valuations_agent.py`; it is not an executable fallback. No paid AI, credential lookup, account read, notification, signal-table or portfolio write occurs in the native producer.

Ops 5944 observed no active valuation schedule. The old imported `valuations-monthly-update` reference did not represent a live binding. The native config installs a named AWS Scheduler binding every four hours at minute 35 UTC, using the existing scheduler role. Source freshness never resets just because this producer ran.

Run `tests/run_tests.py` before deployment. The frontend fixture contains a complete Python output and retained run, above the old connector write cap; ship related files atomically through native git. On the AWS runner, `python3 scripts/replay_valuation_research.py` reconstructs the current output without invoking the producer or writing data. Exact release receipts and original-source/live-page acceptance are required for shipment; a green job alone is insufficient.
