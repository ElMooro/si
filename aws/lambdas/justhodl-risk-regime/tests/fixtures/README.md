Original public-provider responses acquired by `ops_5902_risk_regime_preflight` on 2026-09-20 around 06:44 UTC. The report on main records the content hashes and retained evidence paths. Files are exact response bytes; no credentials are present.

FRED definitions and 800-calendar-day histories use current vintage 2026-09-20, untransformed levels, ascending dates and limit 10000. SPY/HYG options query 21–45 calendar days to expiry and strikes within ±12% of the previous reported close. The SPY fixture is deliberately the first page of an incomplete chain; HYG ends after 158 contracts. It is a regression failure to call the SPY fixture complete or to turn missing HYG daily volume into zero. AUD/JPY is the 65-calendar-day provider aggregate range ending 2026-09-19.

Tests construct explicit authenticated-request-free bindings for those requests. Mutated fixtures are only constructed in memory to exercise failures and are never published as originals.
