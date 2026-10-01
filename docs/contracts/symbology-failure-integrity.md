# Symbol mapping failure integrity

The existing SEC equity spine and its source paths are preserved. The optional
bond queue still uses ID_CUSIP and writes the existing bond master on the normal
05:15 UTC schedule. Native resources stay 1024 MB / 120 seconds / 512 MB temporary
storage. No provider probes or native invocations are part of acceptance.

## Resolution contract

`openfigi-resolution.v1` retains the query and complete per-job provider response.
A response must have exactly as many positions as its request. Only a documented
nonempty warning means `no_match`; request errors, missing data, invalid framing,
duplicate JSON fields and malformed candidates stay errors. Several candidates
remain `ambiguous`; neither issuer preference nor a first-row heuristic proves
security identity. A unique valid FIGI row provides descriptors. Every unresolved
outcome remains visible and retryable. Confirmed negative results expire after
seven days. The compatibility projection returns None for all non-resolved states;
None must never be written as a confirmed negative.

Provider contract: https://www.openfigi.com/api/documentation (reviewed September
30, 2026). Its two anonymous mapping limits disagree; this repair does not increase
the existing batch sizes or request rate. Mapping and search are distinct APIs.
Throttle delays are honored or deferred; an excessive Retry-After is never
shortened to retry early. No paid API or new credential is introduced.

## Publication and recovery

The complete prior master is required before replacement. Only an explicit
NoSuchKey/404 is absence; denied, timed out, truncated, unversioned, malformed,
null or duplicate-key state prevents publication. Stored bodies have a 16 MiB
whole-object bound; exceeding it defers work instead of truncating the population.
Unknown prior fields and unrelated mappings carry forward. Legacy negative and
resolved rows remain retained in `legacy_record` when rechecked, with new structured
qualification separated. The existing queue is never altered by this module.

The final write requires the observed ETag, or IfNoneMatch on genuine creation.
A conflicting writer prevents publication; it never causes an unconditional retry.
No publication time advances without an attempted result and successful write.
Attempt counts describe work, while `published` says whether it reached storage.
Missing/error outcomes never grant investment or sizing authority.

Unattempted entries precede older failed/ambiguous attempts so a failing queue head
cannot permanently starve later entries. The existing per-run cap remains 100.
Native optional enrichment requires the actual remaining-time clock, reserves
20 seconds for the primary publication and caps its own work at 40 seconds. S3 and
SSM have bounded requests/retries; provider reads/backoff observe a cooperative
deadline. This is not a deterministic wall-clock guarantee. Any optional failure
returns a stable reason and leaves the primary equity write available.

## Evidence and remaining scope

Complete predecessor source files and complete invented fixtures reproduce the
original three failures and verify the repaired behavior without actual account,
queue, master, current-consumer or provider reads. Read-only 6397 records the native
predecessor; 6398 requires exact repaired source/ZIP/receipt and unchanged controls.
Neither proves a normal new-code publication or source-to-master historical replay.

Existing equity FIGI first-row resolution, equity prior-read recovery, SEC timing,
CUSIP/ISIN/LEI matching and historical original-response capture are separate,
unqualified work. This patch does not certify those inherited paths, instrument
coverage, first-release availability or portfolio consequences. The legacy static
coverage denominator is also not an independently validated market universe.
