# JustHodl Compound Factory — Gear A

Owner: Khalid. Region: `us-east-1`. Repository: `ElMooro/si`.

The factory is a pane of the existing `/ai.html` SageMaker desk. It preserves the Brain renderer, controls, and `data/ai.json`. Its objective is to improve independently checked usefulness, reliability, and efficiency within owner-set limits. This release is a bounded experiment controller, not ASI or an endlessly self-training model.

## Running components

| Component | Responsibility |
|---|---|
| `justhodl-student-rsi` | One-minute Gear A tick, external evidence, finite coding search, training-eligible pass archive, research baseline forecasts, desk views and state repair |
| `justhodl-factory-grader` | Private frozen coding exam, exact decimal guest checker, official-print market grading |
| Existing `justhodl-ai` | Service-authenticated admission handler; retains all existing Brain behavior |
| Existing data proxy | Verifies the user's identity and admits only the five named factory API routes |
| Existing S3 buckets | Private controls/exams/evidence and authenticated desk state |
| `justhodl-student-rsi-1m` | EventBridge **Scheduler** `rate(1 minute)`; classic rule quota was already full |

The student uses 512 MB with a 120-second Lambda hard timeout, a 40-second work deadline, one coding experiment per day, 15-minute source refreshes and no GPU jobs. Time spent in in-flight network requests can exceed the work deadline. Cloud service charges remain billable; work-unit limits are not a billing guarantee. A private pause switch stops research work. The student cannot change it, its budget, IAM, schedules, endpoints, exams, or the champion.

## What is live-capable in this release

- External observations from the existing official OFR/NY Fed funding warehouse, completed delayed SPY/QQQ/IWM/TLT/GLD/BTC bars when present, and allowlisted GitHub metadata. Source errors retain values and original observation dates. No private notes enter the student feed.
- A finite typed program search over eight candidates for a real deployment bug: the create path previously ignored the configured Lambda role and handler. The candidate is rendered to `scripts/lambda_identity.py`; `scripts/deploy_lambdas.sh` uses its validated values. This is one repair family, not arbitrary software generation.
- The independent grader compares that candidate and the old behavior on 64 cases generated from a private frozen seed. The student gets aggregate results once per family. Passing traces enter the skillbook and release queue; failures enter `_reject`. A score cannot stand in for code release or model promotion.
- An invite-only salon. The current guest checker verifies exact bounded decimal addition, subtraction and multiplication. Other trace domains are admitted to quarantine and rejected until independently checkable task types exist. No submitted Python, shell, IAM call, browser command, or URL is executed.
- A transparent five-session momentum **research baseline** posts for the student if valid prior-session data is available at the Monday window. The baseline's crisis probability is uncalibrated. It is explicitly not a trained model.
- A frozen 13-week season beginning September 14, 2026. NYSE calendar verified against <https://www.nyse.com/markets/hours-calendars>. The November 27 early close is 13:00 ET. The next season requires another frozen owner-reviewed calendar; the student cannot invent one.

## Deliberately unavailable until the dependency exists

- **General generative coding:** inventory found two existing SageMaker endpoints but no verified compatible generative coding endpoint. No new endpoint is provisioned. `model.status` reports `blocked_no_verified_generative_model`.
- **Official price grading adapter:** the current warehouse contains research bars, not verified official grading prints. The grader remains pending instead of silently treating these as official. A runner-owned adapter must write provenance-checked prints to the private `factory/official-prints/<week>/<symbol>.json` contract. The required schema is enforced by `grade_market`; the student has no write access to this prefix. BTC needs the specified venue and session boundary prints, not a daily UTC bar substituted for New York boundaries.
- **Historical crisis promotion exam:** this release collects prospective weeks. It does not claim a validated crisis-week split since 2020. Promote only after a separately frozen chronological, purged exam with crisis/base-rate strata, data availability timestamps, regime calibration, and sufficient independent weeks. Six correlated tickers are not six independent weeks.
- **Weight training/Gear B:** disabled. Thousands of passes alone will not suffice: provenance, licensing, diversity, deduplication, protected evaluation, a compatible open-weight model and an owner-approved spot budget are still required. No paid teacher APIs or new endpoint.
- **General code sandbox:** no arbitrary execution is available. The finite interpreter is the sandbox for the current coding family. General proposals require a separately isolated, network-disabled runner with no AWS credentials.

## Market wall

Entries open on the first NYSE session of the week at 09:30 New York time and lock five minutes later. An exact `(week, agent, symbol)` can be written once. Official server receipt time decides eligibility. Forecasts require direction and regime probability vectors, a crisis probability, data cutoff, pinned source and model/method revision.

Direction bands are ±0.3% for SPY/QQQ/IWM/TLT/GLD and ±1.5% for BTC. Regime uses a frozen weekly path-efficiency definition. The crisis label is explicitly a **weekly daily-close drawdown proxy**, not a claim to detect systemic crises. The thresholds are 5% for equities/gold, 3% for TLT and 12% for BTC. Corporate action weeks are void; missing provenance or prices remain pending.

The display score weights direction/regime/crisis 0.5/0.3/0.2. Additional outputs include Brier losses, no-crisis baseline loss, misses, false alarms and independent week count. Elo moves once per graded week against a fixed 1000 reference and is display-only. No market promotion is authorized by this release.

Individual immutable `factory/salon/events/<event-id>.json` objects are the source of truth. The JSONL files append previously unpublished events while preserving the exact bytes of old lines. Event permalink: `/ai.html#factory-event=<event-id>`. S3 normal objects are not treated as if they supported atomic append.

## Storage and authority

Site-bucket keys (factory evidence requires owner/invited access; anonymous S3 publication was not approved):

- `ai.html`, `factory-desk.js`, `factory-desk.css`
- `student-state.json`, `data/student-state.json`, existing `data/ai.json`
- `factory/salon/season.json`, `board.json`, `wall.jsonl`, `events.jsonl`, `events/*.json`
- `factory/scoreboard.json`, `factory/champions/current.json`, `factory/invites.json`
- `factory/teachers/*.json`, `factory/exams/index.json`, `factory/traces/code/*.json`

Private prefixes in existing `justhodl-ai-857687956942`:

- `factory/control/`: owner-only policy, season and invitations
- `factory/exams/`: grader-only protected fixtures
- `factory/runtime/`: conditional lease, authoritative state, immutable snapshots
- `factory/sources/`, `experiments/`, `skillbook/`, `queue/`
- `factory/quarantine/`, `traces/{code,math,tape,sec}/`, `traces/_reject/`
- `factory/verdicts/`, `salon/accepted/`, `salon/results/`, `official-prints/`

All mutable writes require an ETag or a create-if-absent condition. Evidence writes use create-if-absent permissions. The private state commits before the mirrors. Checksums and increasing versions protect the read model; last-good mirrors and snapshots support recovery. Corruption cannot cause an empty `{}` initialization over existing state. The student is not allowed to write the model champion.

The initial pilot is capped at ten invited accounts. Only the owner adds verified user IDs. No invitation messages are sent. Views contain aliases, not user IDs, and require owner/invited authentication. Public expansion requires three completed seasons and a separate owner release.

## API and operator controls

Authenticated same-origin routes: `GET /api/v1/factory/sandbox` and `/view?kind=state|mirror|board|season|scoreboard|exams|wall|event|trace`; `POST /api/v1/factory/traces`, `/predictions`, `/control`, `/invites`. The last two are owner-only. The desk supplies examples and uses the existing sign-in token. The proxy overwrites identity headers with verified identity; the Brain service token is never returned to a guest or student.

Pause: owner opens Salon, connects, and presses **Pause factory**. Resume uses the same control. A hard stop can additionally disable `justhodl-student-rsi-1m` through an owner-authored GitHub runner operation. Future deployment candidates remain queued for human review.

## Release and verification

AWS changes use the existing GitHub Actions runner only. Do not run AWS locally or create another bucket/region. Deploy the functions and shared modules using the repository pipeline; do not overwrite its shared-module ZIPs with source-only ad hoc packages.

- Factory tests: `python tests/factory/test_factory.py`
- Existing deployment/Brain/page/worker gates remain required.
- Require Lambda package-parity receipts and verify the Scheduler target.
- Invoke one bounded tick; check real grader result, state mirrors, health, private exam denial, forbidden capabilities, and unchanged Brain feed availability.
- Hard refresh `/ai.html`, confirm the original SageMaker desk still paints and the Student factory/CLUB WALL pane displays real state.

Guest admission uses a bounded ten-key cursor and immutable terminal receipts. Season scans stop explicitly rather than truncate if a weekly prefix exceeds 1,000 items. The JSONL read guard is 4 MiB. Before broadening the pilot, release archival views that preserve original event bytes. No old event is deleted to get around those limits.

Anonymous S3 publication was rejected by automatic approval review. No bucket policy was expanded. The factory pane uses the existing authenticated service to read an exact allowlist of views; signed-out visitors see role definitions and an explicit sign-in message, never simulated metrics.

## Verified deployment — September 13, 2026

The live runner receipt is `aws/ops/reports/5510.json`; the factory access boundary receipt is `aws/ops/reports/5512.json`.

- Student ARN: `arn:aws:lambda:us-east-1:857687956942:function:justhodl-student-rsi`.
- Grader ARN: `arn:aws:lambda:us-east-1:857687956942:function:justhodl-factory-grader`.
- Scheduler: `justhodl-student-rsi-1m`, enabled at `rate(1 minute)`.
- First coding family: candidate 64/64, old behavior 16/64. Generation 1 and both checksummed state mirrors verified. These results apply only to the configured role/handler repair family.
- Package parity passed. Eleven prohibited permission probes were denied. Existing Brain feed version 2.2.2 and two in-service SageMaker endpoints were preserved.
- Protected handler checks returned 401 without service identity, 200 for the owner, and 403 for an uninvited identity. Anonymous S3 access to the new factory keys is denied; the existing Brain feed remains accessible.
- A fresh browser load showed nine role cards and CLUB WALL on the same gold desk, with no duplicate section badges. The browser was signed out, so live authenticated metrics were verified through the runner handler check, not through an owner browser session. Cloudflare blocked the runner's website HTTP probe with 403/1010; its browser controls were not bypassed.

The visible role cards are responsibilities and principle cards. They are not nine independently running language models. The current deployment does not establish a general generative coder, a working official price adapter, a validated historical crisis exam, or any weight-training run.

## Discipline in the tick + official prints — September 13, 2026 (Claude, ops 5520)

- **Tick repair.** Since 26c4ac6 the fleet drain in `student_lambda.tick` reused the state ETag variable for
  `factory/fleet/meta.json`, so `commit_state` raised `Conflict("state_changed")` on every tick. The drain is
  gone (the gateway owns `factory/fleet/*` per factory-doctrine.v1); the tick commits again. Public mirrors that
  the student role cannot write are reported in `health.errors[phase=projection]`, never fatal.
- **Chain of command enforced.** `aws/shared/factory_discipline.py` runs inside the tick every 15 minutes:
  one card per alias built from warehouse-graded evidence only (market results on official prints, grader
  verdicts on traces and code), a 21-day window versus the prior one, `factory_doctrine.verdict` →
  promote / hold / retire. Ranks live in the authoritative state (`factory/runtime/current.json → ranks`) and
  in the public projection as aliases only; every promote/retire is an immutable `discipline` event under
  `factory/events/`. Voids count as errors; the supervisor card (`student`) is never auto-retired — that is an
  owner control. Recruits are materialized from gateway spawn requests (`factory/queue/spawn-*.json`), bounded
  by the parent's `SPAWN_CAP` and a roster of 48 active cards; they start recruit and retire as `stalled`
  after 14 days without a graded task. `spawn_workers` takes the parent's rank from the chain of command,
  never from the request body; retired cards cannot spawn.
- **Evidence contract.** `aws/shared/factory_evidence.py`: `factory-evidence.v1` (claim + falsifier, warehouse
  keys with sha256, holdout untouched, checker id, grade window; grades are the grader's only) and
  `factory-reading.v1` receipts for anything read outside (citable, `trainable: false`, never a lesson). The
  gateway's learn tracks and research banks now write receipts under `factory/fleet/reading/`.
- **Outside voice.** `factory_gateway._public_think` no longer calls z.ai directly; it goes through
  `llm_router.complete(tier="reason", on_demand=True)` (daily budget, on-demand gate, cost attribution) and is
  a chat voice only — nothing it says is written as evidence.
- **Official prints (Ship 1).** `scripts/factory_official_prints.py`, run by `factory-official-prints.yml`
  (Sat 04:45 UTC, Sun 12:00 UTC retry, dispatchable): SPY/QQQ/IWM/TLT/GLD from the warehouse's Polygon
  grouped-daily session files (first-session open, per-session official closes; adjusted=true is split-adjusted
  and split weeks are void); BTC from Coinbase Exchange minute candles at the New York boundaries (09:30 ET
  open of the first session, 16:00 ET or the season's early close per session), banked to
  `data/warm/coinbase/BTC-USD/1m/<week>/` first so provenance is a warehouse key — no UTC daily bar is ever
  substituted. Corporate-action check = Polygon reference splits for the week (banked to
  `data/warm/polygon-full/reference/<week>/`); dividends are reported in `dividends_in_window` and do not void
  (season-1 price definition is unadjusted OHLC; SPY/QQQ/IWM go ex-div in week 1 and TLT monthly). Prints are
  written create-if-absent to the private `factory/official-prints/<week>/<symbol>.json` with
  `verified_by: owner_runner` only when they pass the grader's own shape checks; a missing input leaves the
  symbol unwritten and the wall entry pending. Season 2 should re-freeze with dividend-adjusted closes.
- **Not done here.** No holdout manifest yet (Ship 2), no generative model, Gear B off, xai tier
  (`grok_xai_tier`) not live fleet-wide — its shared-module deploy failed preflight on the importer closure.
