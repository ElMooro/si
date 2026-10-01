# Chart render ownership across asynchronous work

Each main-chart paint captures its own sequence, symbol, interval and acquisition generation. Every awaited calendar, dividend/news, insider/buyback, intraday-session, comparison or benchmark response must still belong to that paint before it can modify series, overlays or labels. Both successful responses and caught failures pass the same ownership check. Deferred marker and visible-range callbacks use the captured ownership as well.

Daily and intraday benchmark helpers accept an optional current-owner predicate. Obsolete responses cannot replace shared benchmark rows or the displayed benchmark identity, nor initiate a fallback request after cancellation. Current requests retain their previous source selection and calculation rules. This does not establish benchmark source quality, timing comparability or a causal interpretation of relative returns.

The initial load, symbol-selection load and live-refresh callers recheck ownership after painting. A superseded load cannot open an old destination, close the current search, set its view policy or initiate follow-on pane/tape work. An unsuccessful optional benchmark for the current paint does not prevent the current market frame from rendering.

The retained predecessor reproduction holds one response, completes a different invented symbol, then releases the old response. It demonstrates a 305 price in the chart with a 105 price and old volume in the quote. Current browser cases retain the entire new frame after fulfilled and rejected benchmark responses and after a delayed calendar response. All network requests are intercepted; the original pinned drawing library is used. Completion instrumentation records settlement only and does not replace a computation or resolve a promise.

This bounded repair does not qualify every auxiliary panel, external annotation module, side-channel acquisition, source cache, original measurement, investment policy or portfolio result. No provider requests, producer invocations, schedules, paid AI or private/account/current-consumer records are involved in acceptance. All ten institutional workstreams remain open.
