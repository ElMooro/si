# Source-only Pages builds

The Pages build compiles checked-in source. It does not query current engine
packets, account state, provider catalogs or remote file metadata to decorate a
static page. A successful site deployment proves neither runtime availability
nor observation freshness.

The engine directory uses the complete generated engine manifest and exact page
references. Every concrete output has `state: not_checked_during_build`, with
`present`, `valid`, `fresh`, `age_h` and the row's `n_present` left null. Pattern
outputs retain `pattern_requires_index`; unresolved writes stay visible. A
missing, invalid or duplicate build manifest fails rather than substituting the
working directory's inventory. The generated registry has source ownership only;
remote registry entries cannot silently augment it.

Right rails retain source links, descriptions and navigation, with unknown
modification times. The retired homepage snapshot and provider inventory baker
leave their source pages untouched in offline mode. Client page behavior is
unchanged. No cached current packet is supplied as a replacement build input.

All four CLI and library defaults are offline. Pages invokes them through
`scripts/run_offline_bake.py` with the required `--offline` flag. Its Python audit
hook rejects socket/HTTP and child-process attempts, including attempts hidden
by legacy exception handlers. The sole permitted child is the exact reviewed
`node scripts/js_source_refs.cjs` source parser with no Node preload options.
It parses page JavaScript with bundled Acorn and does not execute page code.
The guard is a regression boundary, not an operating-system security sandbox.
Legacy explicitly requested library probe branches remain for injected-response
regressions; they are not reachable through the bake CLI and are not an approved
deployment acceptance path.

The original site schedule and cache invalidation remain unchanged. Static
acceptance checks an exact source commit, the complete served manifest/contract,
and one unchanged build manifest before and after the asset reads. Separate
runtime acceptance requires its own permitted evidence; this change grants no
engine sizing authority and does not certify current/private delivery.
