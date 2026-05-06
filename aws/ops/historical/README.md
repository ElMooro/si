# Historical ops scripts

Underscore-prefixed scripts (`_create_*.py`, `_phase_*.py`, etc.) from the
pre-numbered era of the JustHodl ops harness. Moved here from `pending/`
2026-05-06 because:

- They were one-shot creation/probe scripts for features that have long shipped
- The run-ops.yml workflow only runs scripts that change in a push,
  so dormant scripts in `pending/` never re-fire — but they clutter
  the directory and make it hard to find current pending work
- Their git history is intact, so they're recoverable if needed

If you need to re-run one of these for archeological purposes, copy it
back to `pending/` and push.
