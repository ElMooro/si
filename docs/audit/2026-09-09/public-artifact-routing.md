# Exact public artifact delivery

The shared engine inspector requested every key from the Pages domain. Production has a same-origin Worker route for `/data/*`, while keys such as `cot/extremes/current.json`, `backtest/results.json` and `screener/data.json` are stored outside that prefix. The COT browser check reproduced a Pages 404 despite the verified engine having published 36 contracts and 9,300 history rows.

The inspector now uses the existing data-proxy Worker for approved public keys outside `/data/`. It requests `exact=1&nogen=1` and requires the response's `X-JH-Artifact-Key` to match the requested key. Cross-origin public requests omit credentials. Owner-authenticated records continue through the owner client; unknown access classifications are rejected.

Exact mode uses a separate Worker cache namespace, so legacy alias responses cannot be reused as exact objects. A missing root object is not retried under `/data/`. A missing equity-research object cannot trigger a paid generation request. The Worker attests the requested key only after a successful exact origin read; the browser can read that header through CORS. Private containment executes before all public cache and origin access. Existing calls that do not request exact mode retain their prior routing.

Verification covers positive exact reads, cache reuse, missing-object behavior, no research-generation side effect, owner containment, unknown/malformed paths, wrong/missing identity headers, full field and row inspection, and the complete frontend/Worker regressions. Engine publication receipts remain separate from successful browser delivery. This correction does not certify every historical object or remove the explicit partial page contracts.
