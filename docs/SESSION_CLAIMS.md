# SESSION CLAIMS — parallel-session coordination (est. ops 4830)

Two-plus Claude sessions build concurrently. ops-number collisions are
caught by preflight, but WORKSTREAM collisions (same feature built
twice: 4819 odds chips, 4827 CSLT probe) waste runs. Protocol:

1. BEFORE starting a workstream: `git pull`, read this file.
2. Claim = add a row (workstream, ops range guess, timestamp UTC).
   Session tag MUST carry a unique nonce (e.g. S-A#k7q2) -- two
   sessions reused the same tag on 2026-08-17.
   commit + push IMMEDIATELY (tiny commit, [skip-deploy] fine).
3. Release = move the row to Done with the final ops numbers.
4. If your intended work is already claimed: pick the next free
   workstream from the queue instead. Never build a claimed row.
5. Stale claims (>6h, no matching ops activity in git log) may be
   taken over — note the takeover.

## Active claims
| workstream | ops | session | claimed (UTC) |
|---|---|---|---|
| IMF BOP worldwide layer: structure probe -> multi-country portfolio+ST-other liabilities wire -> macro hot-money composite (+BIS v2 probe folded in) | 4843-4846 | S-A#k7q2 | 2026-08-17 17:4x |

## Done (this arc)
| workstream | ops | session |
|---|---|---|
| base-rates spine (Fusion 1) | 4818 | S-fable-A |
| odds chips consumers (Fusion 1) | 4819-4820 | S-B |
| plumbing composite + risk-gate v2.4 (Fusion 2) | 4821-4823 | S-fable-A |
| foreign-flows engine v1.0 (TIC/CSLT) | 4824-4826 | S-fable-A |
| CSLT official/private + countries v1.1 | 4827-4829 | S-B |
| capital-flow.html TIC card + verify | 4830-4831 | S-fable-A |
| global-flows engine (Peru live) + capital-flow TOP restructure | 4832-4836 | S-A#k7q2 |
| micro-probes + Taiwan CBC+TWSE hot-money wire + generic world card + throttle fix | 4837-4842 | S-B |

## Open queue (unclaimed)
- catalyst-chain v1.1: S3 freshness gate (RPO/deferred as-of must be
  <200d old -- AMZN chained on a 2020 as-of day-one, honest bug),
  UNKNOWN-direction coverage expansion, chain grading via signals
  ledger, catalyst.html card
- risk-gate dollar-leg input from foreign-flows (official-outflow z +
  safe-haven spike, STRESS-ONLY) — **UN-GATED 2026-08-17 21:30**: June
  cycle verified (new_release fired, data month 2026-06-01)
- global-flows key-gated: Korea (BOK ECOS + KRX keys) and Chile
  (BCCh token) -- BLOCKED ON KHALID
- Fusion 5/6 (sector triangle + mispriced-boom) — wave 2, after Sat
