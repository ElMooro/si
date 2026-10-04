# BIS reference FX chart release

Source commit: 75a0ecfa11d0784e0e6181d1381ac70b6757162d. The native receipt, all 18 deployed source/shared files, and the full original symdir schedule/runtime controls were checked twice by read-only operation 6479 (run 37240716792). The complete served chart and 49 static files match one commit-bound build manifest. Desktop and mobile browser checks used the served modules, with real public history packets in the source-replay scenario.

The provider picker now exposes 1,234 exact WS_XRU bilateral exchange-rate definitions alongside all 98 preceding policy-rate definitions. Every exact identifier was checked through the public directory. Fifty-nine previously unrouted FX_IDC requests have a separately named BIS reference-rate ratio, retaining the original requested identifier and an explicit equivalence warning. Every previous provider mapping is retained.

Ratios use quote currency per USD divided by base currency per USD on the same reference date. Missing legs stay null, with no forward fill. Different source fixing times, averages and redenominations remain material limitations. These are reference observations, not executable market quotes. Neither Calls nor position sizing is enabled. The original complete CSV responses and source-row evidence remain in each returned packet; larger row-evidence sections use lossless gzip encoding. Historical point-in-time vintages and complete upstream history are not certified.

Public history audit: {"public_history_independently_replayed": 1293}. Each successful packet was independently reconstructed from its embedded original CSV responses; derived ratios were then recalculated. Failed or unavailable histories remain explicitly recorded and are not counted as verified history. Definition availability is never used as proof of observed data.

Validation passed: 4,491 frontend tests, 170 native symdir tests, acceptance-operation checks, page syntax, source contracts, engine wiring, desktop/mobile chart regressions and the deployment suite. The exact test logs/hashes and accepted public packets' hashes are linked in the JSON files alongside this note.

Coverage remains incomplete: 6,788 of 10,745 identifiers have routes, including explicitly unqualified alternatives; 3,957 still do not. The previously recorded TradingView HTTP 400 connection refusal remains unresolved and was not retried. Four additional monthly FX alternatives remain research candidates outside this release. The remaining-items audit retains every unresolved identifier.
