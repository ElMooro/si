# FMP rename + /api/v3 death — fleet sweep (2026-07-17)

Follow-up to the 3364–65 partial-quarter audit. Swept every Lambda for the
2026 /stable renames (grades-news → grades-latest-news, price-target-news →
price-target-latest-news, earnings-surprises → earnings) plus lingering dead
/api/v3|v4 bases and old field names (actualEarningResult / estimatedEarning).

## Verdict: CLEAN — zero live defects, nothing deployed, no ops number used.

| Engine | Finding |
|---|---|
| justhodl-analyst-consensus | ✓ live call = grades-latest-news (migrated 3311–23); `earnings` endpoint; old names appear only in comments |
| justhodl-sellside-views | ✓ live call = grades-latest-news (line 129); docstrings stale but harmless |
| justhodl-52wk-quality-breakout | ✓ `earnings` endpoint; "earnings-surprises" only in a sources label string |
| justhodl-starmine | ✓ `earnings` endpoint; field access via or-chain (old∨new names) |
| justhodl-earnings-pead | ✓ `earnings` endpoint; or-chain fields epsActual∨actualEarningResult∨actualEps |
| justhodl-apac-flows | ✓ /stable/stock-price-change is PRIMARY; the /api/v3 URL is a dead second-choice fallback that 400s into except — harmless dead weight, left as-is |
| fmp-fundamentals-agent | RETIRED stub (65 lines, BASE=/api/v3): listed in 1025 dead-lambda triage, writes no S3, zero consumers, no schedule. Superseded by /stable fleet + FinViz. Left untouched per never-rebuild-retired doctrine. |

Rule of thumb confirmed fleet-wide: every live consumer either already calls
the renamed endpoint or reads fields through or-chains, so the 2026 renames
cannot silently empty any active feed.
