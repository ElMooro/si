# News Flow retrieval timezone clarity

The retrieval label is now `Page retrieved (UTC)` and its value uses `new Date().toISOString()` rather than browser-local `toLocaleString()`. This changes exactly two lines in `news.html`: the loading label and successful retrieval timestamp. Source clocks, event times, availability states, all data/request logic and the five-minute interval are unchanged.

The focused regression supplies a deterministic clock whose locale formatter throws, checks the exact UTC output on two loads, verifies unchanged publication/observation clocks, and retains eight requests per load. Synthetic Chromium at 1440/390 now uses America/New_York and asserts that retrieval remains explicitly UTC. No provider, producer, guarded-data, privacy or freshness-policy changes.

Parent-managed normal live PR58 acceptance (1170×751) is complete on build `fac700530c774f3615cc3f03b0befe85aa0d3678`: alert history remains unavailable/withheld HTTP503 with unknown cause, the Alerts filter reports no complete count, received-empty Earnings reports no events, and 54 partial events/13 partial high-severity/50 insider buys remain visible. Normal reload advances only retrieval. GitHub ancestry/blob proof confirmed that build preserved the accepted PR58 page and all nine shared scripts. This supplied parent evidence closes the original live-browser acceptance gap; it does not yet verify this timestamp follow-up.

## Reviewed release proof

[PR59](https://github.com/ElMooro/si/pull/59) was opened as a draft. Independent review accepted exact head `7d6227779a95c8577e73ae84bc9f5bcb54978a11`, tree `6fbbb380eb268be73ee377ebfbfd3abc8009dd98`, with no blockers. The reviewer independently ran all 11 focused tests, Chromium 1440/390 in America/New_York, 599-page syntax and 36-page/143-wire checks. Full frontend/worker suite: **2,661 passed**. Secret, publication-boundary, page-regression, dependency and offline-build checks passed.

Merged at 16:30:00 UTC as **`cda961504c2156afdc427862e84b7ed5557bb329`**; its tree exactly equals the independently accepted tree. [Pages 36892457019](https://github.com/ElMooro/si/actions/runs/36892457019) and [page-gate 36892456985](https://github.com/ElMooro/si/actions/runs/36892456985) succeeded. No manual dispatch or unrelated deployment was needed.

At **2026-10-01 16:34:16 UTC**, the served manifest and HTML identified that exact merged release. Both loading/rendered-source labels explicitly say `Page retrieved (UTC)` and runtime formatting is `new Date().toISOString()`. Served HTML SHA256 `d02770566ad6c06b9a3ce7e75acd68a77d1a377719230e494a086f96b8e70955`; removing exactly one Cloudflare-injected beacon and its trailing newline produced **`655d5a1310633233d418295260d790bd11f90698dc9c12b1ce4b769c673804a5`**, matching the manifest. All nine shared scripts matched their manifest hashes. Details and workflow steps: `news-retrieval-utc-release-evidence.json`.

The earlier certificate-failing Chromium route was not retried or bypassed. **Remaining handoff:** in the parent's existing trusted browser, confirm the visible explicit UTC timestamp on this build and that a normal reload advances only retrieval, preserving source clocks. Original PR58 live acceptance is already complete as recorded above; this follow-up's served-byte proof and synthetic timezone/browser checks do not replace that final normal live interaction.

No new provider/backend/recurring page requests, invokes or test sends. Standard CI/Pages/static verification cost was not measured. Implementation/release claim is closed with the parent live-label confirmation explicitly pending.
