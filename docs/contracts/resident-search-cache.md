# Resident search generation checks

Each active native search process checks the directory manifest at most once per
five-minute success window during ordinary requests. A warm invocation checks
explicitly; it is not assumed to reach every serving process. A changed content
identity, including a same-clock replacement, enters the existing validated
generation download and rollback path. Unchanged heads do not download populations.
This introduces no new schedule, provider request, resource or acquisition source.

UTC wall time and monotonic elapsed time both bound the window. Either advancing
expires it; either regressing requires a new check. The window begins before the
manifest acquisition, not after a potentially long download. Future/regressing
heads and downgrades to unbound generations remain rejected. Cold and explicit
refresh/check failures raise. No error can report a usable cache if recovery lost
the population.

Ordinary searches can retain a previously validated index during a failed check.
The existing graph and its generation metadata are unchanged. Separate head-check
metadata exposes the failed attempt, last successful check, cached status and a
typed error without echoing transport text. Retries are limited to one attempt per
30 seconds per resident process after failure. An explicit check bypasses this
delay. Serving a cached generation is not evidence that it matches the current
manifest. HTTP responses have zero cache lifetime when the check is unavailable
or due; successful responses cannot extend the five-minute check window.

The additive `index_integrity.head_check` is storage-selection evidence. Neither
its timestamp nor `hash_bound_generation` qualifies observation freshness,
identifier relationships, completeness, independent replay or investment authority.
Clients that ignore the new metadata may still present cached results without a
visible warning; that UI migration remains separate. Browser catalog lifetime,
legacy instrument-copy readers, native traffic/capacity and normal production
execution remain unverified. Frozen whole native fixtures block real HTTP.

The complete accepted predecessor and both complete invented generations are
retained under `tests/fixtures/symbol-directory/`. Read-only operation 6409 checks
eight packaged sources, the exact receipt, original resources and seven schedules.
It never invokes a producer or reads current/private/account/consumer packets.

The related proxy change prevents `/symsearch` from replacing native zero-cache
responses with its previous fixed two-minute lifetime. It uses a new search-only
cache namespace, accepts only explicit valid public lifetime directives and
accounts for upstream Age and transport time. Cache hits carry an internal expiry;
their client lifetime decreases instead of restarting. Expired, malformed or
future cache metadata causes a new origin request. Errors, bypass requests,
private/no-cache/no-store policies, cookies and `Vary: *` are not stored. Whole
response bytes are preserved. Other directory routes and worker controls remain
unchanged. The native and worker changes share one reviewed Git batch but have
separate deployment jobs; both exact receipts are required for acceptance.

Cloudflare's [Cache API](https://developers.cloudflare.com/workers/runtime-apis/cache/)
honors response cache-control directives. The code also enforces its own fixed
expiry on hits, so a stale fixture entry cannot extend native check lifetime.
The whole previous worker is retained; regressions execute both real handlers
with complete invented responses and intercepted network calls. Exact deployed
worker source/configuration evidence is distinct from a live route invocation.
