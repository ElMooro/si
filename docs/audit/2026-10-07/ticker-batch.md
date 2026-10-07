# Ticker 360 timeout repair

Diagnostic 6490 found repeated 600-second production timeouts and two enabled
bindings with the same 06:20/18:20 UTC cadence. The old producer revalidated
large complete packets once per ticker. A 50-ticker profile spent 8.22 of its
9.50 seconds repeatedly serializing packets for context digest checks.

The producer now prepares each domain once per invocation and indexes only the
registered context's projected rows. The existing single-ticker hub remains
unchanged. First-match precedence (including null), failed context holds,
fallback identity, source clocks and all decision restrictions are preserved.
No data source, Lambda resource, schedule or decision policy is changed.

The complete native legacy replay covered 3145 tickers.
All output fields match after canonicalization, excluding only elapsed_s.
Digest: `bb1b4f3cb669c21bf024ea2620453f4f91d4f80f0c02429d16307043662da157`.
Legacy duration: 688.399 seconds before and
1.213 seconds after. The actual candidate handler,
including the real network publisher, took 15.437 seconds and
peaked at 361.58 MiB locally. These measurements
do not establish AWS runtime capacity or successful scheduled publication.

Validation: 110 engine tests, 1,075 deployment static checks and 15 deployment
candidate/rollback checks passed. Source/config validation, public-boundary and
secret checks passed. Dependency detection selects only justhodl-ticker-360.
Eight new tests include 250 seeded mixed-schema equivalence cases, all eleven
real contexts, failed/invalid contexts, fallback and malformed transports,
complete producer equality and one projection per source across 3,145 tickers.
Historical fixtures/hashes remain unchanged: the new exact transition is
reversed before historical assertions.

Reproduce offline with `scripts/replay_ticker_batch.py --snapshots PATH
--extra-inventory PATH --output PATH`, using the retained complete public
captures and their digests. Output must be outside the repository. Denied or
missing inputs remain unavailable; the replay makes no network calls.

Release acceptance still requires an exact code receipt and a successful
natural publication. Duplicate triggers are pre-existing and remain unchanged
in this code repair. No producer invocation or private-account probe occurred.
