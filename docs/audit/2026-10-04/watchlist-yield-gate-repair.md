# Treasury alias Pages gate repair

Claim: **S-shopiz#wly1004p9**, 2026-10-04 UTC. Root is the sole writer.
The independent watchlist reviewer is read-only. No ops number is reserved.
This standalone claim preserves `docs/SESSION_CLAIMS.md`, which belongs to the
pending PR98 containment work. Explicit parent direction protects every PR98
path and leaves the four-chart-file takeover unauthorised.

Scope: the qualification routing fixture, identity unit/browser tests, and this
receipt only. No renderer, chart, CFTC, Worker, storage, workflow or schedule
change. Current owner S-codex#symdir1001a and all other claims remain intact.

At main `0ca8957026d3e31d7a0dfc0a5281c3d734189605`, local identity tests
pass 1,248/1,249. The sole failure compares the 685-entry `providerRest` map
against the 676-entry economic qualification fixture. Compared with the last
successful Pages source `3a16452fa36f969662717ae5ab3d4eebf95ccb19`, the map
adds precisely nine Treasury Y aliases, changes/removes no prior pair, and
preserves all 461 extra routes and 36 economic qualification records. Each new
alias targets the same existing FRED DGS maturity as its unsuffixed counterpart.
This verifies the routing contract, not financial equivalence or live data.

The standalone synthetic browser script also has a pre-existing 318-route count
that is stale for both those sources. Keep its exact full-map assertion and
update the explicit count only after the verified 676-to-685 contract delta.

Structured GitHub metadata identifies the frontend/Worker behavioural step as
failed in Pages runs 37230643633 and 37231323793; deployment was skipped. No raw
Actions logs were fetched. Intended repair adds only the nine missing fixture
pairs, explicit alias/maturity checks, and synthetic 1440/390 click/keyboard
coverage. Existing qualification metadata and all production source bytes stay
unchanged. Independent exact-head review and required gates precede merge.

Production edge/nav/click acceptance remains HOLD after the retained HTTP403.
Direct FRED access and raw Actions logs also remain stopped. No alternate access
or actual user storage is used; all browser packets and stores are synthetic.
