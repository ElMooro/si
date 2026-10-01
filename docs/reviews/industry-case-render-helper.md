# Industry-case renderer helper — separate draft

The August industry UI calls `cls` for league returns, industry summary cards,
and member returns without declaring it. With a LIVE populated industry packet,
the first call throws before the league HTML is committed. Its section wrapper
logs the warning but leaves all rows blank. Direct `?ind=` navigation also throws
before cards and members commit. The ordinary ticker Q&A survives separately.
The defect originates in e7dfb8446ca7a935073f13d01770037a653bd4b7 and predates PR43.

This change adds the missing finite-number sign-class helper beside `fN`, using
existing `.pos`/`.neg` CSS. Zero, null, nonfinite and wrong-type values get no
sign class. The existing formatter also needs the same finite-number check:
otherwise wrong-type values throw at `.toFixed`, or nonfinite values appear as
measurements once the missing helper is repaired. Valid number formatting is
unchanged; unavailable values use the existing dash, never a coerced zero.

The production diff is only these two helper lines in industry-case.html. All
rendering structure, calculations, source dates, sorting/navigation code, tape
qualification guards, backend source, shared helpers and policies are unchanged.
No heading-entity cosmetic repair is included. PR43 stays immutable; this is a
separate draft, with no merge/deploy authorization until independent acceptance.

## Evidence and tests

`tests/fixtures/industry-case-public-20260818.json` is an explicitly documented
projection of the public packet captured during PR43 release verification. It
preserves all 149 industry summaries/top-five lists, the complete 105-member
Semiconductors cohort, and original NVDA/AMD cases. Other member arrays and cases
are omitted to keep the regression bounded. Original source URL, publication
timestamp, raw byte count and SHA-256 are recorded in its provenance. Retained
source values are not edited; source counts describe the original full packet.

`node --test tests/industry-case-renderer.test.js` executes the actual inline
page script and shared qualification code. The five tests fail on the predecessor
with the missing helper and pass after the repair. They verify 149 league rows,
four industry cards, source coverage, all 105 members, two-way sorting and
navigation, finite signed/zero and invalid returns through actual renderers,
unchanged packets, publication dates and legacy/qualified tape withholding.
The qualified fixture comes from the existing intercepted producer/projection
harness. NaN/Infinity cases are in-memory synthetic inputs, not JSON values.

`node tests/industry-case-browser.cjs` intercepts every browser request and uses
the actual page at 1440/390 widths. It exercises league clicks, direct industry
URLs, four cards/coverage, member sorting, real member anchor navigation, and
Enter ticker switching, with zero page errors and console warnings. Captured
August source dates and legacy tape withholding remain visible. This is local
browser acceptance; no live publication is claimed.

The existing tape renderer/browser suites and full frontend/page gates must also
pass. No private data/provider requests or production invocations are used.

## Draft validation

Five actual-renderer regressions passed (all five fail on the uncorrected page),
including 15 signed/zero/missing/invalid value cases. Intercepted Chromium passed
at both widths with zero console warnings/page errors. The full frontend suite
passed 2,545 tests. Page syntax/regression, wiring, offline-build boundary,
sovereign asset dependency, secret and Brain-publication boundary gates passed.
The existing tape browser suite passed; its obsolete success-log warning note is
removed now that this separate repair fixes that warning.

The complete page outside `fN` and `cls` definitions is unchanged. No production
backend/shared/navigation files changed. The seven-file PR inventory consists of
the session claim, this note, industry-case.html, the captured fixture, the two new
industry renderer/browser tests, and the existing tape browser's log-note update.
No live/deployment acceptance is claimed; independent exact-head review is next.
