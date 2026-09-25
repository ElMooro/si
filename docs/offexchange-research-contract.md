# Off-exchange research contract

This contract defines the source and publication requirements for replacing the
legacy dark-pool engine. It is not a claim that native deployment is complete.

## Measurements and identity

- Daily CNMS is FINRA-disseminated regular-session volume in exchange-listed
  securities reported through the TRFs and ADF. ShortVolume already includes
  ShortExemptVolume. Preserve fractional shares and reconcile the file trailer,
  date, unique symbol, facility codes and exempt <= short <= total.
- Weekly ATS and non-ATS activity is grouped by reported symbol, issue name,
  week and tier. Preserve initial publication, update and last-reported dates.
  Join legs only when these identifying dimensions agree. Missing legs are null.
- An ATS is not necessarily a dark venue. A mean reported trade size cannot
  identify the distribution of trade sizes, retail participation or block trades.
- ATS / (ATS + non-ATS) is a share of reported off-exchange activity, not a share
  of consolidated market volume. No Polygon denominator is used until its
  dates, sessions, adjustments, coverage and security identity are qualified.
- Monthly concentration is within one reported symbol/issue name/month/tier.
  CRD 0 is the aggregated De Minimis Firms bucket, not one known firm. Show
  named-firm contribution and an interval for reporting-activity HHI. The
  undisclosed bucket's squared share contributes to the upper bound only.
- A ticker plus an issue description is a source record identity. It is not a
  verified security-master mapping. June 2026 NVA records explicitly demonstrate
  why different issue descriptions must remain separate. Do not infer share
  conversions, returns or ownership continuity across those descriptions.

## Capture and replay

Retain each complete HTTP response, including empty/error bodies, with its exact
request, acquisition clock, response status, pagination headers and digest.
Transport failure has no fabricated original. Byte-limit failures never retain
truncated prefixes as complete originals. Each response has a durable journal;
one worker failure must not obscure another worker's completed evidence.

Select explicit dated partitions, use equality filters on all sorting partition
fields, advance offsets by actual parsed row counts, and reconcile reported
totals and unique record grain. A short page is not evidence of completion.
Total-Records-On-Page is optional in the reviewed live API; check it if present.
Reject changing totals, duplicate exact grain and a changed first-page recheck.
Disclose that this is a multi-request acquisition, not an atomic API snapshot.

Whole original responses stay protected. Public records must bind their derived
values to original digests and source row/line locations, and to the exact frozen
compiler. Retain acquired vintages; never overwrite a historical week's source.
Publication must replay from originals before conditionally replacing a current
head, with protection against concurrent changes and observation rollback.

## Permissions and consumer boundaries

These are descriptive observations. Call, score, direction, ownership flow,
beneficial-owner concentration and position size remain unavailable. Calls,
sizing, execution and forecast qualification are false. A missing measurement
does not become a zero or a neutral vote.

Retain the complete previous handler inactive before native replacement. Preserve
compatibility keys with null or empty unqualified outputs where required, but
inspect consumers first. In particular, Ignition's direct FINRA fallback must
not bypass the new qualification boundary, and DarkPoolAdapter must not invent
confidence from mean trade size or acceleration. No consumer invocation, order,
notification, paid AI call or private-account read is part of acceptance.

## Page and acceptance requirements

The page must separate daily, weekly and monthly periods, expose coverage and
source lags, and make ambiguous issue descriptions visible. Immutable run links
must reproduce the same records; an unavailable pinned run must not substitute
the latest data. Render provider text safely and distinguish data unavailability
from an empty result. A hypothetical participation or cost calculation must use
explicit user assumptions and a dated measurement, and cannot promise a fill or
recommend a position size.

Accept only after exact release receipts and actual packaged source match the
intended commit, raw-input reconstruction and independent arithmetic pass, live
JSON and built HTML/assets match, protected originals remain private, and desktop
and mobile browser tests cover valid/invalid pinned records and stale scenarios.

## Primary definitions

- [FINRA OTC transparency](https://www.finra.org/filing-reporting/otc-transparency)
- [Daily short-sale files](https://www.finra.org/finra-data/browse-catalog/short-sale-volume-data/daily-short-sale-volume-files)
- [Current short-volume data guide](https://www.finra.org/sites/default/files/2020-12/short-sale-volume-user-guide.pdf)
- [FINRA API pagination, partitioning and sorting](https://developer.finra.org/docs)
