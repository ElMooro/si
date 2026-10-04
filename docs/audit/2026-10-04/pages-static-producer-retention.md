# Pages static producer retention repair

The Pages contract compiler on main `4c3aeb227caeb79e27195ed9cd10261350ca7dea`
fails because its Alpha Atlas static-output proof points into the age-retained
ops directory. Retention commit
`ae07906330448dc99e36d284f6bf36eacb7dc10a` archived that source.
[Committed receipt](https://github.com/ElMooro/si/blob/960dd1b8c84ed60903a63e3023a9e429816c8470/aws/ops/reports/latest/audit_release_progress.json)
records Pages run `37190098591`, failed step "Build complete producer-to-page
data contracts" at 08:52:59 UTC on October 4, and `FileNotFoundError` at
`build_page_data_contracts.py` lines 418, 398 and 310. The unchanged main
checkout reproduced those locations and the exact missing path locally.

The repair retains the complete historical
`aws/ops/ran/ops_4281_alpha_atlas.py` from the last passing source commit
`77ce5dad1887b0a36b1244ecd35d0a1d4531eb92` as
`scripts/static-producers/ops_4281_alpha_atlas.py.txt`. Its 9,618 bytes match
Git blob `54c67fee8ab8b602dae81dcdf11e8a01f753e5c4` and the retention tarball
byte for byte; SHA-256 is
`522046df7f369156caa8344b51a446df29e72c91623e655d64e029bc4b87a6b3`.
The normal secret scanner reports no findings in this source.

Only its provenance location changes: the role's writer evidence and static
dependency, and the checked-in contract's ownership evidence, point to the
retained file. Both JSON files otherwise remain byte-identical. The contract
builder still parses the actual write argument and fails for missing,
malformed, unrelated or private output proof. It is unchanged. The existing
subsequent-writer historical citation to ops4289 is unchanged; it is not an
input read by this static contract compiler.

The file is outside the retention candidates (`aws/ops/reports` and
`aws/ops/ran`), outside executable ops lanes and inert under its `.py.txt`
suffix. It is excluded from the public artifact with `scripts/`. No historical
operation was executed, and no current output was fetched or manufactured.
Market values, the page, production source, user-data retention, ops execution,
schedules, workflows and security settings are unchanged. There are no
repository deletions. No AGENTS.md or populated .agents skill directory was
present in this checkout or the provided workspace; DEPLOY_LANE, AUTONOMY,
STATE, the system catalog and current claims were inspected. The isolated
claim is `S-shopiz#pspr1004n6`; other lanes are preserved.

Local validation:

- Six new retention regressions pass. They execute the existing workflow's
  candidate selection, archive and removal commands in a temporary Git
  repository, stopping before its publication tail. An age-eligible retained
  source survives while old ops/report files are archived and removed; recent
  ops and the live run log survive. The original path reproduces the failure.
  Missing, malformed, wrong-output and private-output proofs still fail.
- 1,075 deployment static tests and 15 mocked deployment shell checks pass,
  with dummy AWS credentials, metadata disabled and outbound Python networking
  blocked. The first environment setup attempts lacked boto3 and the runner's
  module search path; the complete corrected run passed without source changes.
- All 3,133 frontend/worker tests pass. The 36 Brain, page syntax, sovereign
  asset, offline-build and public-artifact regressions pass. All 600 source
  page graphs parse; wiring checks preserve 36 pages and 143 references.
- The exact existing Pages assembly commands through final syntax, public
  artifact and manifest stamping pass locally in a disposable source snapshot.
  All 600 built page graphs parse; all 1,045 manifest hashes verify; neither
  `scripts/` nor `aws/` enters the artifact. The regenerated contract passes
  `--check`. This is a local build, not a deployed artifact.
- All 600 page and 910 engine contracts compare equal with the same source
  tree using the historical producer path after normalizing that single path.
  Coverage, output keys, dependencies, access rules and runtime-unknown fields
  are identical. The normalized contract SHA-256 is
  `b47a36ef1ae2ce24b1ff1517824610a76e93c7815c2545464db12584b1cfa2a1`.
  Existing generated-registry drift on main is preserved; the tracked registry
  is not broadly regenerated as part of this repair.

Preflight, the complete staged inventory, full tracked-file secret scan and
independent review are release gates; the final head and review outcome belong
in the draft PR. The draft is for parent reconciliation before merge.

A normal accepted main merge is eligible for Pages through `scripts/**`; the
existing 15-minute schedule is also unchanged. The deployment-test path can
trigger the Lambda workflow, but this repair contains no Lambda/shared source
targets. No dispatch, merge, AWS call, producer invoke or deployment was made.
Do not use this repair as a broad release of unrelated main source. Returning
the dependency to the archived path would reproduce the build failure.

The raw Actions log denial, annotation-endpoint 400 and public build-manifest
403 remain stopped. No retry or alternate access was used. Local compilation
resolves the proven missing-source defect; actual runner release and served
bytes remain unverified.
