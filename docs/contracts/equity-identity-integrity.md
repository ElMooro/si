# SEC equity identity and recovery

The whole prior equity master must be readable, valid and versioned before any
replacement. Only explicit NoSuchKey/404 permits creation. Denial, timeout,
null/malformed JSON, duplicate fields, short bodies and missing ETags stop the
replacement. Whole equity objects may use 64 MiB; bond defaults remain 16 MiB.
Writes require the observed ETag, or IfNoneMatch on true absence. Concurrent
replacement is never retried unconditionally.

SEC CIK must be a positive decimal identity, not a fabricated zero. The source
population must be nonempty and complete; conflicting CIKs for a ticker reject
the population. Repeated same-issuer records retain every original source row.
Conflicting labels stay unavailable. A greater-than-half population contraction
retains the prior master for review; this heuristic is not a completeness proof.

Only matching validated ticker/CIK identities carry prior identifiers. Changed
or unverified issuers retain their whole predecessor separately, without borrowing
its identifiers. Absent prior ticker rows and unknown fields are retained. Their
absence from this particular SEC file does not prove delisting. Flat legacy
provenance avoids recursively nesting each normal daily refresh.

Optional keyed FIGI work retains structured resolved/no_match/ambiguous/error
responses and every candidate, exact request, SEC CIK and attempted-at timestamp.
Only a unique matching equity ticker can populate the current lookup result.
Positive and negative cached results expire after seven days. Failed requests,
wrong response cardinality and ambiguous candidates remain retryable. A retained
identifier may remain visible after a failed lookup, but its current resolution
flag is false. A ticker lookup never proves the historical issuer-security link.
Work requires the real remaining-time clock, caps FIGI work at 40 seconds and
reserves 30 seconds. This cooperative budget is not a wall-clock guarantee.

Legacy CUSIP candidates are explicit. Conflicting direct matches do not promote;
a new incompatible CUSIP cannot attach a different ISIN or LEI to a retained
identifier. Name-only joins and US-country ISIN assumptions remain candidates.
Unexpected optional exceptions cannot leave half-applied row changes. Existing
direct CUSIP/GLEIF values remain unqualified: complete transport, schema/checksum,
country and issuer-security evidence are separate outstanding work. The legacy
CUSIP/GLEIF operation does not yet have an end-to-end deadline. SEC HTTP capture
and independent source replay also remain unqualified. The 320k denominator is a
legacy unvalidated comparison, not a measured coverage benchmark.

No investment or sizing authority is granted. Consumer qualification migration
remains open for catalog consumers. The former 13F symbology guesser is
unreachable from the canonical handler and is not rebuilt here. Normal publication, private/current objects
and provider originals are not read during acceptance. Operation 6399 checks only
the exact native public receipt/package and unchanged original resources/schedule;
it never invokes the producer. Complete invented inputs and whole predecessor
source establish regression evidence, not real-data economic qualification.
