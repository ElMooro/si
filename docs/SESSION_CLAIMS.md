# SESSION CLAIMS — parallel-session coordination (est. ops 4830)

Two-plus Claude sessions build concurrently. ops-number collisions are
caught by preflight, but WORKSTREAM collisions (same feature built
twice: 4819 odds chips, 4827 CSLT probe) waste runs. Protocol:

1. BEFORE starting a workstream: `git pull`, read this file.
2. Claim = add a row (workstream, ops range guess, timestamp UTC),
   commit + push IMMEDIATELY (tiny commit, [skip-deploy] fine).
3. Release = move the row to Done with the final ops numbers.
4. If your intended work is already claimed: pick the next free
   workstream from the queue instead. Never build a claimed row.
5. Stale claims (>6h, no matching ops activity in git log) may be
   taken over — note the takeover.

## Active claims
| workstream | ops | session | claimed (UTC) |
|---|---|---|---|
| WORLDWIDE hot-money: IMF sdmx3/BIS/TWSE-rwd probe + Taiwan daily hot-money wire + generic country card | 4837-4840 | S-fable-A | 2026-08-17 17:3x |
| global-flows micro-probes (CBC shape / TWSE rwd daily / IMF BOP dataflow) + v1.1 Taiwan wiring + page generic country render | 4837-4841 | S-fable-A | 2026-08-17 17:3x |

## Done (this arc)
| workstream | ops | session |
|---|---|---|
| base-rates spine (Fusion 1) | 4818 | S-fable-A |
| odds chips consumers (Fusion 1) | 4819-4820 | S-B |
| plumbing composite + risk-gate v2.4 (Fusion 2) | 4821-4823 | S-fable-A |
| foreign-flows engine v1.0 (TIC/CSLT) | 4824-4826 | S-fable-A |
| CSLT official/private + countries v1.1 | 4827-4829 | S-B |
| capital-flow.html TIC card + verify | 4830-4831 | S-fable-A |
| global-flows engine (Peru live) + capital-flow TOP restructure | 4832-4836 | S-fable-A |

## Open queue (unclaimed)
- Fusion 3: catalyst chain (catalyst x readthrough x backlog-miner x
  estimate-revisions, second-order propagation, ledger-graded)
- Fusion 4: learned-weights grader for beaters legs / invest tiers
  (ship grader now, activation at >=4 graded weeks)
- risk-gate dollar-leg input from foreign-flows (official-outflow z +
  safe-haven spike, STRESS-ONLY) — GATED: wait >=1 verified monthly
  TIC cycle (earliest after next new_release flip)
- FRED-window sentinel for tic-cslt banks (weekly diff vs bank)
- global-flows micro-probes: CBC BPP2Q01en data[] shape map; TWSE
  rwd/en/fund JSON daily-flow endpoints; IMF sdmx/2.1 BOP dataflow
  id (worldwide layer)
- global-flows key-gated: Korea (BOK ECOS + KRX keys) and Chile
  (BCCh token) -- BLOCKED ON KHALID
- Fusion 5/6 (sector triangle + mispriced-boom) — wave 2, after Sat
