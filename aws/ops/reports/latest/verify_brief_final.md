# 1) Pull live ai-brief.json — all 4 fixed snapshot tiles

**Status:** success  
**Duration:** 0.1s  
**Finished:** 2026-05-04T20:17:50+00:00  

## Log
- `20:17:50`   Tile render preview (what brief.html shows):
- `20:17:50` 
- `20:17:50`   ┌─ Eurodollar Stress    : 39.01/100 · CALM (1🔴)
- `20:17:50`   ├─ Asymmetric Setups    : 94 setups · top INCY · 21 traps
- `20:17:50`   ├─ Risk Sizer           : 75.0% cap · ¼Kelly · DD -0.2%
- `20:17:50`   └─ Allocator            : BALANCED_NEUTRAL · 20.0% cash
# 2) Brief metadata

- `20:17:50`   generated_at: 2026-05-04T20:15:00.602727+00:00
- `20:17:50`   model:        claude-haiku-4-5-20251001
- `20:17:50`   duration_s:   27.16
- `20:17:50`   brief chars:  5912
- `20:17:50`   tokens: in=5428 out=2069
- `20:17:50`   cost:   ~$0.0158/run = ~$2.84/month
# 3) DECISIVE CALL section from brief

- `20:17:50`   DECISIVE CALL
- `20:17:50`   
- `20:17:50`   ### **TRIM / TACTICAL HEDGE — 60% long, 15% long vol hedge, 25% cash**
- `20:17:50`   
- `20:17:50`   **Rationale:**  
- `20:17:50`   - **edge_regime** (perfect 1.0 accuracy, 0.75 weight) + **crisis_hy_oas_vs_hyg** (92.3% acc, 1.42 weight) both flag elevated downside tail despite positive macro surprise (growth_surprise +1.1σ).
- `20:17:50`   - RRP at $0.6B is **critical**: below $1B is systemic warning; below $0.5B triggers forced deleveraging. Khalid Index (82.09% acc, 0.31 weight) at 48 is neutral but underweights crisis_distance (42 = moderate-to-high tail risk).
- `20:17:50`   - Narrow leadership (XLK +8.39%, 7 laggards) + AAII extreme bullish (+50% spread) = classic distribution setup into rally.
- `20:17:50`   - Allocator correctly holding 20% cash; system recommends increase.
- `20:17:50`   
- `20:17:50`   **Concrete allocation (from $100M base):**
- `20:17:50`   
- `20:17:50`   | Position | Size | Rationale |
- `20:17:50`   |----------|------|-----------|
- `20:17:50`   | **QQQ (long)** | 20% | Momentum leaders; maintain clip but reduce from allocator 32.9% |
- `20:17:50`   | **MU / NEM / INCY** | 10% (3.8 + 3.7 + 2.5) | Asymmetric setups; highest conviction quality |
- `20:17:50`   | **Energy (EXE/FSLR blend)** | 8% | Sector leader momentum; DXY hedge; sized 7.1% in risk_sizer |
- `20:17:50`   | **TLT / LQD long vol** | 15% | Duration hedge; correlation breaks suggest bond unwind risk; protect downside |
- `20:17:50`   | **Cash / Treasury bills** | 47% | RAISE from 20% to 47%; liquidity buffer for RRP <$500M event |
- `20:17:50`   
- `20:17:50`   **Exit triggers (change to HEDGE or EXIT ALL RISK):**
- `20:17:50`   - RRP closes <$500M → REDUCE to 40% gross long, add 10% short SPY hedge
- `20:17:50`   - HY OAS >290 bps **AND** Khalid Index >55 → EXIT all risk, move to 70% cash
- `20:17:50`   - LITE closes <$80 → Sell 50% QQQ, reallocate to defensive
- `20:17:50`   - DXY closes >120 → EXIT EEM/commodity longs, raise cash to 60%
- `20:17:50`   
- `20:17:50`   **Rebalance cadence:** Daily monitor RRP; weekly adjust on Khalid Index; daily stop on LITE momentum >30-day vol.
- `20:17:50`   
- `20:17:50`   ---
- `20:17:50`   
- `20:17:50`   **STATUS: YELLOW ALERT — Maintain positions but prepare for liquidity event in 7–14 days. Raise cash aggressively. Khalid prefers optionality over yield in pre-crisis windows.**
# 4) brief.html status

- `20:17:50`   status: 200, size: 17,330b
- `20:17:50`   has 'd?.score' eurodollar reader:  True
- `20:17:50`   has 'top_5_setups' asymmetric:     True
- `20:17:50`   has 'max_gross_exposure_pct':      True
