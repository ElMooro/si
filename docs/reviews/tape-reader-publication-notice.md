# Tape-reader publication notice — presentation-only draft

Legacy or unknown measurement contracts intentionally withhold activity, rather
than reporting a fetch failure. Table and enhancement now say “Qualification
withheld — awaiting a compatible publication.” A known contract without its
expected row array instead reports incompatible activity rows. All five summary
metrics, rows and charts remain unavailable for those packets.

Only a strictly parsed, calendar-valid, timezone-qualified ISO timestamp is
shown, with the original value labelled packet publication time, not observation
freshness. Missing/invalid clocks are unavailable; future clocks are explicitly
unavailable. This does not add a freshness rule or change measurement eligibility
for a compatible packet. Browser timestamp comparisons have millisecond precision;
sub-millisecond differences grant no freshness or authority. Fetch/HTTP errors, JSON parse errors and display errors
remain distinct from intentional contract withholding. Output is escaped.

The tape-reader-only enhancement opt-in uses the main page's existing fetch
result and a local state event. It updates with the table, removes a duplicate
request and adds no requests/assets. It waits honestly if the initial request is
pending. Generation checks prevent an older completion overwriting a newer
result. Other pages keep the original fetch/fallback and rendering behavior.

Tests exercise old/absent/unknown/current contracts, malformed rows, missing,
invalid and future clocks, escaping, repeated updates/reloads, recovery and
out-of-order completion. The frozen enhancement predecessor is compared against
all 59 other actual importer configurations across 413 rendered-output/request
comparisons. The intercepted Chromium test verifies table/enhancement agreement
and one feed request per main update, preserving existing sort/keyboard/mobile
checks. No backend, tape-truth, schedule, invocation or deployment change.

Commands: `node --test tests/tape-reader-counts.test.js tests/tape-reader-notice.test.js`
and `node tests/tape-reader-browser.cjs`, plus the full frontend, page syntax,
wiring, asset/offline/boundary and secret gates. This remains a draft for review;
no production acceptance is claimed. PR43 is separate and unchanged by this PR.
