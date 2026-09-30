# Portfolio retained-research inspection

The portfolio page presents `snapshot-research.v1` from its existing authenticated snapshot. It does not request original research feeds again, write account records, or qualify an engine for sizing. The native producer remains the previously accepted portfolio-snapshot release.

## Evidence and interpretation

- All four known documents appear, including unavailable ones. Additional keys remain accessible through source pagination. A source-declared date is separate from acquisition start/end; neither clock is called current by this component.
- The recorded aggregate complete-body byte count must equal the individual records and the producer's eight-MiB bound. An inconsistency is visible. A consistent total is metadata validation, not independent source verification.
- Inspection is explicit and local. Supported complete-body metadata, canonical base64, exact byte length and SHA-256 must pass before text or a download is offered. Each original is bounded at eight MiB. Invalid UTF-8 remains downloadable as exact bytes without replacement text.
- Original text is literal, not reparsed JSON. This preserves numeric spelling and integers outside JavaScript's safe range. `textContent` keeps source markup inert. The `.original` download is an octet-stream Blob containing the same verified bytes.
- Six joins are reported views of four sources, not independent votes. Their population counts and complete row references are inspectable. Join diagnostics are reported metadata rather than a browser recomputation. Full references are rendered only on demand; no row count or text-length truncation is applied.
- A new or unavailable snapshot, source switch, close action or page hide invalidates pending inspection and revokes the old download URL. Repeated age-only rendering of the same loaded object preserves the open view. Normal five-minute refresh and authentication remain unchanged.

## Source-contract boundary

The pure verifier's four path labels identify documents retained inside the snapshot. `scripts/page_sources.py` classifies that asset as a non-consumer, as it already does for other local evidence renderers. Its script is still included and parsed; imported consumers are still traversed. The portfolio page's original primary producers and output inventory must remain unchanged. This prevents source labels from turning into newly advertised current-feed dependencies.

## Acceptance and limits

Current reviewed handlers produce complete invented frames with all book, mark, research and publication I/O mocked. Browser QA checks exact long-integer text, duplicate joins, missing and legacy evidence, corrupt hashes, invalid UTF-8, refresh failure/recovery, keyboard focus, 105 watchlist references and a 360-pixel viewport. Unit tests additionally exercise eight-MiB originals, stale asynchronous completion, extra-source pagination and production Blob bytes.

The in-app browser's two documented download mechanisms timed out. Actual file-save completion is therefore **unverified**, while literal text, download availability and exact production Blob contents are verified separately. No actual account, provider, recipient or current-consumer data is used for acceptance. No native producer invocation is performed. Investment qualification and actual private publication remain unverified.
