# Pages static producer release

[PR94](https://github.com/ElMooro/si/pull/94) merged normally at
`ba0bac8464d2542429be837e0448342bc1c3ba41` on October 4 at 10:23:38 UTC.
The actual merge parents are main
`4c3aeb227caeb79e27195ed9cd10261350ca7dea` and reviewed source
`befb348a59d321667ffb1bc5102d982a10bf2ce6`. Its tree is exactly the
independently accepted tree `0d15e47174df20b7e785c4b8ff8b7b861a5b87d2`.
There was no current-main drift, conflict resolution, source change or broad
deployment dispatch during release qualification.

The independent review was explicitly extended from draft readiness to normal
source release of the exact prospective integration
`fec53f32cb37b36b90e0c8e48deea88183c98be9`, with the same parents and tree.
Historical bytes and all three provenance-path replacements were reverified.
The complete 600-page / 910-engine contract equivalence and passing checks
remain applicable because the source tree is identical. Ordinary ops/recovery
selection and patcher globs exclude the retained fixture; the direct ops lane
is dispatch-only. This qualifies automatic lane exclusion, not a guarantee
against a deliberate arbitrary interpreter/path invocation.

PR94 was marked ready before merge. Its applicable GitHub status was terminal
CodeRabbit SUCCESS at 10:22:12 UTC; the bot explicitly reported that automatic
review was skipped because the repository has fewer than ten stars. This is
not a CodeRabbit source approval. The independent source review supplied the
release qualification; no failing check was waived and no settings changed.
The normal merge was guarded against a different PR head.

[Automatic Pages run 37195222637](https://github.com/ElMooro/si/actions/runs/37195222637)
has event `push`, head SHA `ba0bac8464d2542429be837e0448342bc1c3ba41`,
attempt 1, and terminal conclusion SUCCESS at 10:27:45 UTC. Permitted GitHub
run/job/step metadata establishes the following actual outcomes:

| Job or step | Result | Completed UTC |
|---|---|---|
| Build job | SUCCESS | 10:27:29 |
| Build complete producer-to-page data contracts (step 24) | SUCCESS | 10:27:19 |
| Final built page syntax gate | SUCCESS | 10:27:24 |
| Block repository-only directories in public artifact | SUCCESS | 10:27:24 |
| Stamp every page and asset in commit-bound build manifest | SUCCESS | 10:27:24 |
| Upload Pages artifact | SUCCESS | 10:27:26 |
| Deploy job | SUCCESS | 10:27:44 |
| actions/deploy-pages@v4 | SUCCESS | 10:27:41 |
| Existing Cloudflare cache purge | SUCCESS | 10:27:41 |
| Self-heal job | SKIPPED | no retry |

The build's credential, Brain-publication, frontend/worker behavior, source
syntax, sovereign asset, wiring, offline-build and public-artifact regression
steps all succeeded. Console counts are not asserted from inaccessible logs.
The formerly failing contract compilation is resolved in the actual runner,
and the deployment action itself succeeded, rather than merely the containing
workflow reporting green after a nonfatal deployment error.

[Automatic Lambda run 37195222665](https://github.com/ElMooro/si/actions/runs/37195222665)
also completed SUCCESS, attempt 1, for the exact merge commit at 10:24:04 UTC.
The unchanged path detector selects no Lambda/shared source targets. Existing
AWS credential setup ran; deployment preflight, alias priming, long-validation
connection setup and code deployment were all SKIPPED. No Lambda code release,
historical producer execution, manual AWS call, ops dispatch or producer invoke
was performed by this repair. No new workflow, schedule, settings, user-data
retention or market-value changes were introduced.

The [preparation record](pages-static-producer-retention.md), original
`ae079063` retention commit/archive and
[failed-run receipt at 960dd1b8](https://github.com/ElMooro/si/blob/960dd1b8c84ed60903a63e3023a9e429816c8470/aws/ops/reports/latest/audit_release_progress.json)
are preserved unchanged. Source validation includes six retention regressions,
1,075 deployment static tests, 15 mocked shell checks, 3,133 frontend/worker
tests, 36 boundary regressions, full secret scan and staged inventory. The
independent reviewer passed 48 direct test functions and separately verified
all 1,045 local artifact hashes / 600 page stamps. These are local counts;
actual runner step verdicts are recorded separately above.

The raw Actions-log Forbidden, annotation-endpoint 400 and public build-manifest
403 stops remain in force. No manual retry, alternate access route, new live
verification request or extra workflow dispatch was made. Successful Actions
deployment establishes the runner release outcome; served-byte and live/browser
acceptance remain UNVERIFIED. This release does not qualify current Alpha Atlas
data availability, freshness or market values.

Only session `S-shopiz#pspr1004n6` is moved to Done. All other claims and prior
failure records remain unchanged. This documentation closeout uses reviewed
docs-only `[skip-deploy] [skip-ops]` tags and does not initiate another release.
