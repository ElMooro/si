# 1) Force redeploy ai-brief w/ horizon-aware prompt

**Status:** success  
**Duration:** 38.5s  
**Finished:** 2026-05-05T11:46:27+00:00  

## Log
- `11:45:48`   zip size: 12,889b
- `11:45:53` ✅   ✓ deployed, mod=2026-05-05T11:45:49.000+0000
# 2) Inspect deployed source for horizon-aware code

- `11:45:54`   ✓ recommended_horizon read
- `11:45:54`   ✓ best_horizon in row
- `11:45:54`   ✓ horizon_lifts compute
- `11:45:54`   ✓ horizons_tracked field
- `11:45:54`   ✓ HORIZON-AWARE WEIGHTING in prompt
- `11:45:54`   ✓ Match weight to call horizon
# 3) Invoke ai-brief with horizon-aware synthesis

- `11:46:26`   status: 200, duration: 32.7s
- `11:46:26`   brief_chars: 6485
# 4) Verify snapshot has horizon fields

- `11:46:27`   n_signals_with_horizon_data: 21
- `11:46:27`   horizons_tracked: ['day_1', 'day_14', 'day_3', 'day_30', 'day_5', 'day_7']
- `11:46:27` 
- `11:46:27`   Top 5 signals with best_horizon attribution:
- `11:46:27`     carry_risk                    flat_w=1.453  best=day_30: w=1.453 acc=1.0 n=30
- `11:46:27`     screener_top_pick             flat_w=1.338  best=day_30: w=1.338 acc=0.832 n=555
- `11:46:27`     ml_risk                       flat_w=1.298  best=day_30: w=1.453 acc=1.0 n=30
- `11:46:27`     momentum_spy                  flat_w=1.056  best=day_7: w=1.31 acc=1.0 n=8
- `11:46:27`     plumbing_stress               flat_w=0.992  best=day_14: w=1.414 acc=0.92 n=25
- `11:46:27` 
- `11:46:27`   horizon_lifts (mis-priced by flat lens):
- `11:46:27`     edge_composite                flat=0.508 → day_1: w=1.292  acc=0.795 n=44  Δ+0.785
- `11:46:27`     crypto_fear_greed             flat=0.861 → day_14: w=1.442  acc=0.973 n=37  Δ+0.581
- `11:46:27`     plumbing_stress               flat=0.992 → day_14: w=1.414  acc=0.92 n=25  Δ+0.422
- `11:46:27`     crisis_hy_oas_vs_hyg          flat=0.96 → day_3: w=1.222  acc=0.75 n=20  Δ+0.262
- `11:46:27`     momentum_spy                  flat=1.056 → day_7: w=1.31  acc=1.0 n=8  Δ+0.254
- `11:46:27`     ml_risk                       flat=1.298 → day_30: w=1.453  acc=1.0 n=30  Δ+0.155
# 5) Scan brief markdown for horizon citations

- `11:46:27`   brief size: 6,485b
- `11:46:27`   horizon-keyword hits:
- `11:46:27`     'day_30': 4 mentions
- `11:46:27`     'day_14': 2 mentions
- `11:46:27`     'day_7': 4 mentions
- `11:46:27`     'day_3': 5 mentions
- `11:46:27`     'day_1': 2 mentions
- `11:46:27`     'at day': 1 mentions
- `11:46:27` 
- `11:46:27`   Last 1500 chars of brief (where DECISIVE CALL lives):
- `11:46:27`   ────────────────────────────────────────────────────────────
- `11:46:27`     ctical rotation; raise cash to 40%+ immediately)
- `11:46:27`     
- `11:46:27`     **Rationale:**
- `11:46:27`     - **RRP at $0.6B** is a **CRITICAL structural signal**. This is lower than Sept 2019 (repo crisis at ~$200B) and March 2020 (pandemic panic). System liquidity i
- `11:46:27`     - **Paper portfolio is underwater (-0.28% alpha vs buy-and-hold over 9 days)** despite SPY at +28.5% YTD. Regime allocation is broken.
- `11:46:27`     - **AAII sentiment is extreme bullish (+50% spread)** — classic contrarian headwind into liquidity crunch.
- `11:46:27`     - **Narrow leadership (Tech only; 7 sectors lag)** + momentum acceleration into exhaustion zone (INTC accel 824.55, AMD 626.37 — unsustainable).
- `11:46:27`     - **DXY at 118.39** (>115 EM crisis threshold) is crushing laggards; multinationals exposed.
- `11:46:27`     - VIX at 16.99 is **dangerously complacent** given plumbing stress and RRP depletion — false calm before volatility repricing.
- `11:46:27`     
- `11:46:27`     **ALLOCATION:**
- `11:46:27`     
- `11:46:27`     | Asset | Current (Allocator) | **NEW (EXIT Call)** | Rationale |
- `11:46:27`     |-------|---|---|---|
- `11:46:27`     | **SPY/QQQ** | 43.6% (27 QQQ + 16.6 SPY) | **15%** | Cut 2/3 of equity; take profits on YTD +28.5% run |
- `11:46:27`     | **UUP** | 20.7% | **25%** | Raise USD hedge (DXY strength = EM stress amplifier) |
- `11:46:27`     | **CASH** | 15.0% | **40%** | Liquidity buffer for RRP collapse; dry powder for bargain entry |
- `11:46:27`     | **EEM** | 10.4% | **5%** | Dollar strength hammering; cut EM exposure in half |
- `11:46:27`     | **DBC** | 10.4% | **8%** | Commodity rebound possible but secondary; hold core |
- `11:46:27`     
- `11:46:27`     **Paper Portfolio Action:**
- `11:46:27`     - **CLOSE** all 6 short-squeeze positions (ABBV, LLY, ROKU, and 3 others) — liquidate into strength, redeploy to CASH.
- `11:46:27`     - **TRIM 40% of earnings_pead longs** (MSFT, QCOM, TMUS, NOW, ELV) — take partial profits; reset stops at +10% above current.
- `11:46:27`     - **STOP: MU, N
- `11:46:27`   ────────────────────────────────────────────────────────────
