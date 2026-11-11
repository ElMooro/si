# ICI weekly releases through Chrome (the lane that replaced the Lambda's transport)

**Since:** 2026-11-11 · **Workflow:** `.github/workflows/ici-flows-lane.yml` (Wed/Thu 20:40 UTC, Fri/Sat 15:40 UTC, `workflow_dispatch`) ·
**Code:** `aws/ops/checks/ici_flows_lane.py`, `aws/ops/checks/run_ici_flows_lane.py` · **Tests:** `tests/test_ici_flows_lane.py`

## Why

ici.org sits behind an Akamai bot wall that answers **403 to every datacenter HTTP client**. The native
`justhodl-ici-flows` engine (deployed 2026-09-30) acquired one release by hand on 2026-09-26 and then nothing: its
request journals show 403 on every scheduled run (Sep 30, Oct 1, 7, 8, 14, 15). With no `data/ici-flows.json` ever
published, the 2026-10-20 retire plan (ops 6563) classed it a broken writer and disabled `justhodl-ici-flows-weekly`;
`config/schedule-manifest.json` pins that state. Meanwhile the desk feed `data/ici-fund-flows.json` was a one-off write
(2026-10-02) of an older seeded variant with 86 hard-coded weeks ending 2026-09-23. Diagnosis: ops 6779, 6780.

Headless Chrome on a GitHub runner is let through: ops 6781 loaded all three release pages (HTTP 200) and both engines'
parsers were green, every reconciliation identity within rounding.

## What the lane does

1. **Acquire** `/research/stats/flows`, `/research/stats/combined_flows`, `/research/stats/mmf` with Playwright's real
   Chrome (`channel="chrome"`): the exact main-document bytes, status and final URL. If the server bytes do not carry
   the table, the rendered document is retained instead and the source says `transport: chrome-rendered-document`.
2. **Native engine, unchanged.** `ici_store.run()` is executed on the runner with its `acquire(kind)` answered from
   the browser originals (`browser_transport`): private retention, sealed packet, independent replay, CAS publication
   of `data/ici-flows.json` (`ici-research.v1`). The request journal lands under the usual private prefix with
   `request_id = chrome-lane:<github run id>`. A native failure is reported, never hidden, and never blocks step 3.
3. **Desk feed.** `/flows` (Estimated Long-Term Mutual Fund Flows, $M) is read with the same strict table reader and
   merged into `data/ici-fund-flows.json` (`history`, fresh release wins per week; the seeded weeks stay). The
   combined table (`combined_history`), the money-market table (`money_market_history`, $B) and ETF net issuance
   (`etf_net_issuance_history` = combined − mutual funds for the same week, ICI's own identity) accumulate weekly.
   The Inflows & outflows desk reads all four through `MacroData.FLOW_FEEDS` (`flows:ici/…`, `flows:ici-combined/…`,
   `flows:ici-etf/…`, `flows:ici-mmf/…`).

## Rules

- The Lambda trigger stays **DISABLED** by design; the lane is the writer. Do not re-enable the rule: the function
  cannot pass the wall and would only fail every Wednesday.
- Nothing is estimated or carried forward. Every number is a cell of an ICI release table; weeks ICI revises are
  replaced in place.
- `ici_research.py` / `verify_ici_research.py` are hash-pinned by `ici_qualification.py`; the lane does not touch them.
- The runner prints only known literal error messages (`known_messages()`), like the other source-refresh lanes.
