# Recovery release evidence — 2026-09-09

Core run34312891397 at0c82670 completed with two failed engines. All other selected engines reported deployment success. This document records source fixes; it is not a production completion receipt.

- Factor risk: runner diagnostics identify UpdateSchedule / ValidationException / target.roleArn. Configuration now supplies the established execution role. Direct schedule replacement preserves existing input, disabled state, retry, DLQ and other supported settings. Identity conflicts, read failures and concurrent changes stop mutation; readback is required.
- Research backtest: validation reached a pinned candidate and then its client connection closed. The runner enables OS and AWS CLI TCP keepalive as recommended by AWS for long synchronous Lambda invocations. Duplicate source reads are cached within one invocation and provider calls use four workers; source prefetch uses eight. Every source row remains eligible for computation. Deadline exhaustion stops publication. Successful validation remains mandatory for live alias promotion.
- Fed Liquidity Terminal (`census.html`): complete metadata discovery5241 enumerated884 functions and152 Function URL configurations with zero errors; the former hostname has no owner. The page now uses fedliquidityapi's configured hostname and actual summary/catalog/series schema. Summary/calendar comparison dates, zero values, partial coverage, all catalog entries, categories, returned observations and metadata are accessible. HTML uses escaped values or textContent. No risk probabilities are invented.
- Fed engine: comparisons use calendar cutoffs, finite observations, explicit missing-series coverage, bounded parallel summary reads and fixed provider-error categories. Rerun5240 verifies corrected URL identities.
- Provider privacy and contract completeness fixes from798e2da and page roles/observability fixes fromb4572ae are included in this recovery range. Static route coverage is362 valid,113 partial and42 not applicable across517 routes. Remaining route reasons are committed in page-coverage-remaining.json; static accessibility is not proof of live availability.

Recovery5243 selects changes since0c82670, including explicit changes to both failed engines. Final order remains core deployment, layer reconcile5234, privacy migration5230, then final verifier5231. Pending production checks must remain pending until their receipts exist.

AWS primary references: https://docs.aws.amazon.com/cli/latest/reference/lambda/invoke.html and https://aws.amazon.com/blogs/networking-and-content-delivery/implementing-long-running-tcp-connections-within-vpc-networking/ .

Local verification:214 deployment checks,11 validated-candidate shell scenarios,233 frontend tests,32 selected-engine runners (all passed),14 Brain boundary tests and a9,144-file secret scan with zero findings. Offline Pages assembly stamped517 pages and647 artifacts. Live data baking and final production verification remain pending. Recovery selection contains71 functions.

The queued follow-up also applies the Fed engine’s explicit Python3.12 runtime upgrade. Legacy runtime metadata remains create-time-only unless `update_runtime=true`; this prevents old imports from silently downgrading unrelated functions. The code/source tests run before the upgrade, and final metadata parity must confirm it.

The follow-up explicitly applies fedliquidityapi's configured Python3.12 runtime. General deployment previously updated timeout/memory but ignored runtime changes. Runtime upgrades now require update_runtime=true so an old imported config cannot silently downgrade other functions. Risk Sizer and Fed Liquidity are the only follow-up selections since76e1bc1.

Live Fed page verification at76e1bc1 exposed252 catalog series and12 summary series with full response capture. It also exposed obsolete RESBALNS data from2020 and a monthly observation incorrectly represented as a weekly change. The follow-up uses the active WRBWFRBL reserve series in the current summary (retains historical RESBALNS in the catalog), obtains units/native frequency/provider metadata, rejects cadence-inappropriate or distant comparison observations, and exposes age/quality. Currency magnitudes now convert explicit native million/billion-dollar units; absent values remain unavailable. FRED references: https://fred.stlouisfed.org/series/RESBALNS and https://fred.stlouisfed.org/series/WRBWFRBL . Five Fed handler and five page behavior checks pass.

Final local follow-up gate:216 deployment checks,11 candidate scenarios,234 frontend tests,19 Risk Sizer scenarios,5 Fed handler checks;9,157 scanned files with zero credential findings. These counts supersede earlier local follow-up counts only; they do not certify runtime deployment.

Recovery34349581247 deployed70 of71 selected functions. Research Backtest's
numbered candidate3 exhausted its1024MB allocation before constructing the
research universe. Its previous live version1 remained active; metadata5247
also records that older version's scheduled timeouts. The fee895b follow-up
34350706350 successfully deployed Risk Sizer and Fed Liquidity.

The research retry retains only fields consumed by attribution arithmetic in
its per-invocation cache. It preserves every decision, entry price, verdict,
regime and temporally eligible critique while leaving large narratives/provider
tables in their original source objects. A full-versus-projected input test
asserts identical attribution rows and values. Memory is explicitly2048MB.
Deadline exhaustion still prevents publication, and pinned validation remains
mandatory. The deployment diagnostic now distinguishes FunctionError from an
executed-version mismatch; both paths stop before promotion. Local research
checks9 and validated-candidate scenarios12 pass. This is a source-fix record;
the next numbered candidate must pass production validation.

The live Fed check after fee895b confirmed correct currency magnitudes, dates,
zero values and explicit Partial coverage. It exposed STLFSI3's last2022
observation in the current summary. FRED identifies STLFSI4 as the current
replacement; the follow-up uses it for current summaries/categories and keeps
STLFSI3 explicitly discontinued in the historical catalog. Weekly month
comparisons now admit the last observation within one weekly cadence before
the calendar cutoff (at most6 days), without using an observation after that
cutoff. Validated series metadata is cached in each warm process for at most6
hours to reduce repeated provider requests. Missing metadata still produces
UNKNOWN/Partial; no frequency or units are guessed. Eight Fed handler checks
pass. Primary references: https://fred.stlouisfed.org/series/STLFSI3 and
https://fred.stlouisfed.org/series/STLFSI4 .
