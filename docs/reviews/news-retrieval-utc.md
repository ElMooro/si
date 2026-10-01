# News Flow retrieval timezone clarity

The retrieval label is now `Page retrieved (UTC)` and its value uses `new Date().toISOString()` rather than browser-local `toLocaleString()`. This changes exactly two lines in `news.html`: the loading label and successful retrieval timestamp. Source clocks, event times, availability states, all data/request logic and the five-minute interval are unchanged.

The focused regression supplies a deterministic clock whose locale formatter throws, checks the exact UTC output on two loads, verifies unchanged publication/observation clocks, and retains eight requests per load. Synthetic Chromium at 1440/390 now uses America/New_York and asserts that retrieval remains explicitly UTC. No provider, producer, guarded-data, privacy or freshness-policy changes.

Parent-managed normal live PR58 acceptance (1170×751) is complete on build `fac700530c774f3615cc3f03b0befe85aa0d3678`: alert history remains unavailable/withheld HTTP503 with unknown cause, the Alerts filter reports no complete count, received-empty Earnings reports no events, and 54 partial events/13 partial high-severity/50 insider buys remain visible. Normal reload advances only retrieval. GitHub ancestry/blob proof confirmed that build preserved the accepted PR58 page and all nine shared scripts. This supplied parent evidence closes the original live-browser acceptance gap; it does not yet verify this timestamp follow-up.

Independent exact-head review, required gates and release proof are pending. This executor will not revisit its earlier certificate-failing Chromium route or disable certificate verification. Normal live timestamp-label acceptance will use the parent's existing trusted browser; served-byte verification uses the already functioning ordinary static HTTPS path.
