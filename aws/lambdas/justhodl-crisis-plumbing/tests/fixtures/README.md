Original public-market fixtures for the native Crisis Plumbing migration.

`macro.json` and `macro-manifest.json` retain the canonical 2026-09-20T10:26:38.679740Z source packet/run. `selected-macro-inputs.json` identifies the 45 requested series already in that run. `originals/` contains their complete definition and observation response bytes, named by SHA-256. Unavailable identities remain absent in this historical fixture; the live catalog adds all eight remaining requested IDs without substituting identities.

`funding.json`, its run manifest and complete retained input bind the canonical 2026-09-19T13:14:03.738433Z funding packet. The original 514,973-byte OFR CSV is retained by hash. All nine publisher columns are independently reconstructed from it; the test does not claim to re-run unrelated Funding calculations.

The pure fixture clock is 2026-09-20T11:00:00Z. `expected-output.sha256` is the exact full output digest, not a tolerance or a subset. The same fixture must pass on Windows and Linux before a public producer is invoked. Histories are separately checked byte-for-byte by replay. Request URLs have no credentials. No private accounts, consumer invocations, notifications, or paid AI are used.
