# Brief future timestamps — draft review only

The shared `freshness()` helper classified negative ages as FRESH because they
satisfied `age_h <= ttl_h`. It now returns the existing EXPIRED enum for any
negative age. Missing/malformed timestamps retain EXPIRED, naive input timestamps
retain UTC interpretation, now remains FRESH, and exact TTL / twice-TTL remain
FRESH / STALE respectively. No new clock-skew tolerance is introduced: none was
found in brief_contract, brief_compiler, the brief adapters, or the warehouse
engine-data contract. Other subsystems' skew allowances are not this contract.

The existing compiler consequently holds future-dated required inputs across
all five modes. Optional inputs retain their existing non-blocking behavior.
Inputs and fields remain present; schemas and validators are unchanged.

Review risk: downstream consumers which previously accepted invalid future-dated
required data may now receive HELD and omit a vote. Even a small positive skew is
rejected. This can reduce coverage; it does not authorize changing scoring,
adapters, registry, capital allocation or veto behavior. The helper's dependency
scan (`python scripts/shared_dependents.py aws/shared/brief_contract.py`) selected
only `justhodl-brief-compiler`. Its existing schedules and deployment configuration
are unchanged. There is no new recurring cost.

## Source and ownership audit

Base: `ee3f535583aeee9129c34e885a81f1f683c31c2d` (main, 2026-09-30).
Reviewed current DEPLOY_LANE, actual deploy-lambdas/guard/page workflow triggers,
CLAUDE/AUTONOMY, STATE, claims, and the 72-hour main change inventory (533 commits).
No changes in that window touched brief_contract, brief_compiler, the compiler
Lambda, or SESSION_CLAIMS. No matching branch, open PR, or conflicting claim was
found before reserving the named branch. No AGENTS.md or local .agents/skills
files were exposed in this workspace. The branch was published at the base first;
the nonce-tagged claim is included with this single atomic fix commit to honor
the user's one-commit constraint. No main claim-only push or deployment was made.
The separate bonds.html workstream is untouched.

Task 0 remains the prior green `74a51be25` / run `36746204181`; this work does not
repeat that operation or fetch private Brain. No AWS mutation or workflow dispatch
was performed.

## Canonical public artifact inspection

On 2026-09-30, GETs to `https://justhodl.ai/data/<mode>-brief.json` succeeded using
an explicit review User-Agent, as required by AUTONOMY's edge-polling guidance.
The default Python User-Agent had returned HTTP 403. No alternate host was used.
All five artifacts had schema `brief-1.0`, mode, status, generated_at, and input
metadata with required, last_modified, as_of, freshness and error.

| Artifact | Observed status | generated_at (UTC) | Input detail |
|---|---|---|---|
| plumbing-brief | LIVE | 2026-09-30 12:20:03 | Required plumbing-stress FRESH |
| official-stats-brief | HELD | 2026-09-30 01:40:20 | Required fed-nowcast-join EXPIRED, as_of 2026-09-12 |
| market-tape-brief | LIVE | 2026-09-30 12:25:48 | Both required inputs FRESH |
| positioning-brief | LIVE | 2026-09-30 01:50:11 | Required 13F FRESH; optional CFTC EXPIRED |
| event-brief | LIVE | 2026-09-30 12:35:36 | Required finviz-signals FRESH |

These observations establish the existing public shape, not deployment acceptance
for this patch or a claim of current future-dated corruption.

## Validation

- Before production edit: shared contract tests **24 failed, 49 passed**. Failures
  reproduced +1 microsecond, +1 hour, +365 days and offset-normalized future dates,
  including compiler required-input behavior in all five modes.
- After: shared contract tests **73 passed**. Combined with existing bridge brief
  adapter and official-stats tests: **90 passed**.
- Full unscoped `python3 tests/deployment/run_tests.py`: **902 static tests passed**;
  validated-candidate shell suite: **15 passed**. No test or unrelated gate bypass.
- Compiler importer `tests/run_tests.py`: **12 passed**.
- `tests/test_brain_public_boundaries.py`: **15 passed**, local fixtures only.
- Selected Lambda source/config validation, py_compile, ops preflight: PASS.
- Repository secret scan: **12,999 tracked files, zero findings** before adding
  this review note; final staged inventory and scan are checked before commit.
- `git diff --check`: PASS. Code review confirms only a negative-age guard changes
  production behavior; parsing, TTL thresholds and consumer policies are untouched.

The repository's only pull_request workflow is path-filtered to Lambda source
files; this shared-module-only patch does not match. Deployment workflows run on
main or explicit dispatch, neither authorized here. Check the draft PR's actual
checks/runs separately; absent CI is not a successful deployment. Reading main's
branch-protection settings returned HTTP 403, so required-check policy is unknown.

This is a draft review candidate only. No merge, deploy, schedule change, or live
producer invocation is included; later runtime acceptance requires an authorized
release and exact receipt verification.
