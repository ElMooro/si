# Monthly reference FX chart release

Frontend source commit: 1ea240c5b6dcabd2b3554287fb53c4a8a5881ca4. All 49 served static files and the chart HTML match that commit's build manifest. The unchanged native backend is 75a0ecfa11d0784e0e6181d1381ac70b6757162d, rechecked through its exact release receipt. No Lambda code, schedule, role or producer was changed or invoked by this release.

Four previously unresolved watchlist identifiers now open explicitly qualified historical BIS monthly reference ratios: JPY/VND, JPY/EGP, JPY/ETB and XDR/BWP. The latest usable observations are July 2026, July 2026, September 2025 and August 2026 respectively. Month-start dates are plotting coordinates. These are neither daily executable prices nor certified equivalents of FX_IDC quotes. Missing legs stay null. The quote column still reports unavailable while explaining that a reference-history route exists.

Each public packet was independently reconstructed from its original source receipts. Desktop and mobile tests replayed every accepted observation through the served chart and compared every plotted point. Both screenshots were inspected. A pre-existing initial SPY error toast remains visible briefly in the isolated acceptance scenario; it does not alter the selected series and needs a separate asynchronous-status review. Long provenance banners also remain a mobile usability limitation.

Validation passed: 4,498 frontend tests, 170 native regression tests, 1,075 deployment checks plus 15 shell cases, page syntax and contracts, and desktop/mobile chart regressions. No personal browser profile, private account data or actual user storage was read or changed.

Coverage remains incomplete: 6,792 routed of 10,745; 3,953 unresolved. Unverified substitutions remain explicitly unqualified, and routing is not full historical coverage proof. OECD source research is in progress outside this accepted release.
