# Warehouse scalar observations

Stage 502 extends the existing chart-observations.v1 and observation-cache.v1 contracts to the native directory's existing /series route. It does not change provider acquisition, engines, schedules or investment permissions.

## Identity and evidence

FRED, NYFED, ECB and other declared warehouse namespaces render as scalar histories. The full requested native id must match the packet id case-insensitively; suffixes and neighboring providers cannot substitute. Existing explicit chart aliases retain the requested and resolved ids. Named exchange symbols cannot inherit an unqualified macro alias. Initial URL loading preserves scalar identifier spelling. Classification terminates without recursing through the market ticker resolver. Missing parser/catalog modules cannot route these measurement ids into market data fallbacks.

The complete received packet and every evaluated obs row remain inspectable and exportable. Decimal finite numbers, explicit zero and signed measurements are preserved. Nulls, booleans, blank strings, containers, nonfinite/unrepresentable numbers and impossible calendars are unavailable. Equal duplicate dates plot once with all contributing ordinals; conflicting values on one date are withheld, with every row retained. One accepted observation is enough for descriptive display. No valid points produces a diagnostic frame, not a substitute series.

## Calendar and source limits

The native directory already normalizes provider periods to ISO days and may collapse duplicate dates. Browser validation cannot recover discarded original precision or provider rows. Received days remain those days, including month-start normalized dates; they are not guessed to be month-end or publication dates. Reported frequency, unit, provider and source remain reported metadata. Packet as_of is construction metadata, not first publication or acquisition. Original-provider equivalence, observation freshness, predictive validity and portfolio consequences remain unverified. No Calls or sizing permission is granted.

The line and grouped display reuse the existing scalar rules. Market OHLC, trading volume, logarithmic price controls, price-pattern recommendations and replay/trading panels stay unavailable for these measurements. The full transform retains every contributing ordinal. Gap-aware line presentation remains separate work.

## Download cache

Each requested warehouse identifier has an isolated cache. At most eight complete packet caches are retained in the catalog; least-recently-used eviction aborts and supersedes a pending generation. Case spelling may occupy separate entries, still within the same capacity. The existing shared transport policy applies: five-minute successful-read reuse, thirty-second failed-read retry and a ten-second request-plus-decode deadline. Reads trigger downloads, not background producer invocations. Concurrent requests share a pending promise. Late responses cannot republish an evicted generation.

A failed refresh retains the previous accepted packet with visible failure state and the whole decoded rejected replacement, when available. An initially wrong or malformed packet has no selected series; its decoded body stays in failure evidence. HTTP error bodies are not parsed. Transport success does not prove source freshness. Generalized custom cache definitions are copied and validated; the seven existing fixed sources are unchanged. Scalar arrays no longer accumulate full-packet references in the separate unused market bar cache.

## Validation boundary

Tests use complete invented packets, complete prior source and the selected whole browser modules at desktop/mobile widths, including the pinned chart library. Actual application/private/current-consumer bodies are not read; no producer runs and no paid provider is used. Exact served static-byte acceptance is required after deployment. These checks qualify this repair, not the entire chart plugin set, native normalization, the full platform, or any investment recommendation. All ten institutional acceptance workstreams remain open.
