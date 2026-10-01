# Source-scoped coverage inventory

The report at data/audit/coverage-gap.json describes four existing platform
artifacts. It is not a global market census, provider-history completeness proof,
freshness assessment, Bloomberg comparison or investment signal. Its original
six metric names, actual/target/pct_of_target/method fields and output path remain.

Ticker count comes from the complete received by_ticker dictionary, reconciled
separately against declared n_tickers. Malformed populations are unknown, never a
filtered subset presented as complete. FIGI and CUSIP counts are distinct stored
strings matching explicit character patterns. Non-null rows, matching rows and
nonmatching rows are separate. Checksums, assignment and issuer/security links
remain unqualified. The CUSIP compatibility pattern accepts eight alphanumeric or
*@# characters plus a final digit; this is not full identifier validation.
The FIGI pattern follows v30 section 1.1.2 and does not assume every provider uses
the BB prefix. Prior global targets survive as legacy_unqualified_target; active
denominators and percentages are null when no comparable population is defined.

EDGAR n_filings and the rollup FRED feed count remain explicitly reported summary
counts. Neither proves current-quarter completeness or a unique series census.
NYFed counts only SOFR/EFFR/OBFR/TGCR/BGCR slots with positive integer n_obs, after
all five have known counts. The producer uses lowercase keys; uppercase aliases
are supported, duplicates are ambiguous, and data_unavailable prevents a known
count. Partial known counts are separate from the unknown headline. Generated
time is distinct from each source's reported clocks; no freshness is inferred.

The native store reads only those four inputs and the prior report. Complete raw
inputs, compiler files, input manifest, prior reports and prepared outputs are
retained by content hash under the existing protected audit-private namespace.
Only references and scoped counts appear in the report; no source bodies are
exposed. This supports later replay, not a claim that independent replay happened.
Source platform artifacts are not original provider releases or first-availability
evidence. Unknown prior fields and the whole predecessor report are preserved.

Only explicit NoSuchKey/404 permits creation; denial, timeout, malformed prior or
missing ETag abort replacement. Writes require IfMatch or IfNoneMatch; conflicts
and lost acknowledgements do not cause unconditional retry or a rollback claim.
Strict JSON rejects duplicate/nonfinite values. Complete bounded gzip is accepted;
corrupt gzip is retained as failed input without destroying other available counts.
Short bodies are not represented as complete originals. Limits are 64 MiB per
object/decompressed member and 128 MiB total received inputs; exceeding aggregate
bounds aborts publication. These byte limits are not a memory-use guarantee.
Native remaining-time checks cap cooperative work at 95 seconds with 20 seconds
reserved; client connection/read bounds are 2/3 seconds with one total attempt.
This is not a hard end-to-end wall-clock guarantee.

Original resources (1024 MiB, 120 seconds, 512 MiB temporary storage, Python 3.12,
x86_64) and the existing daily 06:45 UTC rule are unchanged. Operation 6402 checks
only package bytes, exact public receipt and selected native controls twice.
It neither invokes the producer nor reads current/private/account/consumer data.
Normal scheduled publication and independent real-source replay remain unverified.
Complete invented fixtures and whole predecessor reproductions establish the
regression evidence. No investment or sizing authority is granted.
