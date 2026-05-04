# 1) Redeploy ai-brief with enriched eurodollar compressor

**Status:** success  
**Duration:** 34.1s  
**Finished:** 2026-05-04T20:00:31+00:00  

## Log
- `19:59:57`   zip size: 5,347b
- `20:00:00` ✅   ✓ deployed at 2026-05-04T19:59:57.000+0000
# 2) Trigger fresh AI brief

- `20:00:30`   status: 200  duration: 30.2s
# 3) Eurodollar snapshot in brief

- `20:00:30`   score:           39.01
- `20:00:30`   severity:        CALM
- `20:00:30`   regime:          CALM
- `20:00:30`   n_signals_used:  8
- `20:00:30`   hot_signals:     [{'id': 'repo_spread', 'label': 'SOFR – Fed Funds Spread', 'score': 88.1}]
- `20:00:30`   cold_signals:    [{'id': 'hy_oas', 'label': 'HY Credit OAS', 'score': 11.2}, {'id': 'ig_oas', 'label': 'IG Credit OAS', 'score': 22.8}, {'id': 'ofr_fsi', 'label': 'St Louis Fed FSI', 'score': 28.9}]
- `20:00:30` 
# 4) End-to-end pipeline status

- `20:00:30` ✅   ✓ Lambda Active        — last modified 2026-05-04T19:51:41.000+0000
- `20:00:30` ✅   ✓ Schedule wired       — rate(1 hour) state=ENABLED
- `20:00:31` ✅   ✓ S3 output produced   — 2026-05-04T19:51:50+00:00 (2,602b)
- `20:00:31`   ✓ Wave-logger          — last modified 2026-05-04T19:54:22.000+0000 (eurodollar in dispatch)
- `20:00:31` ✅   ✓ AI Brief reads it    — score=39.01/100, severity=CALM
# 5) Brief mentions of eurodollar/repo/FSI/HY/IG

- `20:00:31`   > | **HY Credit OAS** | 277 bps | 11.2/100 (tight, calm) | ✓ |
- `20:00:31`   > | **SOFR–Fed Funds Spread** | — | 88.1/100 (elevated) | ⚠️ Repo stress |
- `20:00:31`   > - **Credit**: Deceptively calm (HY OAS 277bps, IG OAS tight) but repo stress visible (SOFR spread 88/100)
- `20:00:31`   > | **Liquidity crisis / forced deleveraging** | **70%** | **1–2 weeks** | RRP hits $0; SOFR blows out >100bps; HY OAS widens >400bps |
- `20:00:31`   > | **SOFR–FF spread** | 88.1/100 | **> 95/100** | Cut equity 50%, raise cash to 50%+ |
- `20:00:31`   > | **HY OAS blowout** | 277 bps | **> 400 bps** | Crisis_HY_OAS signal triggers; sell risk 40% |
- `20:00:31`   > 1. **RRP stabilizes above $5B AND SOFR spread falls below 50/100** → Add 20pp to QQQ/SPY
- `20:00:31`   > 3. **HY OAS remains < 300bps for 5 consecutive trading days** → Cautiously add 10pp back to equities
- `20:00:31`   > - SOFR > 100/100 (repo panic)
- `20:00:31`   > - HY OAS > 450 bps (credit event)
