# ops 3907 — full by_verdict breakdown + the engine's own deterministic analysis

**Status:** success  
**Duration:** 0.4s  
**Finished:** 2026-07-26T04:43:42+00:00  

## Log
## 1. FULL by_verdict — every conviction label, ranked by win rate

- `04:43:42`   HIGH RISK            n=10408   win_rate=71.3% avg=4.34% median=3.36% hit_5pct=40.2% best=195.6% worst=-38.0%
- `04:43:42`   FAIR VALUE           n=4734    win_rate=63.1% avg=2.13% median=2.23% hit_5pct=36.3% best=42.6% worst=-53.0%
- `04:43:42`   STRONG OPPORTUNITY   n=1537    win_rate=59.0% avg=2.23% median=2.06% hit_5pct=37.7% best=44.1% worst=-25.5%
- `04:43:42`   OPPORTUNITY          n=8845    win_rate=56.4% avg=1.57% median=1.16% hit_5pct=32.4% best=51.8% worst=-38.9%
- `04:43:42`   HOLD / NEUTRAL       n=27315   win_rate=50.9% avg=-0.68% median=0.16% hit_5pct=27.8% best=1838.0% worst=-77.4%
- `04:43:42`   EXPENSIVE            n=6182    win_rate=43.3% avg=-3.34% median=-1.49% hit_5pct=22.5% best=85.8% worst=-90.2%
## 2. the engine's own deterministic analysis (no LLM needed, always-on)

- `04:43:42`   headline: Conviction labels are inverted — HIGH RISK leads at 71.3% while STRONG OPPORTUNITY trails at 59.0%; the buy labels are not predictive yet.
- `04:43:42`   full ai_analysis: {"headline": "Conviction labels are inverted \u2014 HIGH RISK leads at 71.3% while STRONG OPPORTUNITY trails at 59.0%; the buy labels are not predictive yet.", "diagnosis": "Across 6 verdicts the win-rate spread is 28.0pp (best HIGH RISK, worst EXPENSIVE); conviction-vs-outcome rank correlation is -0.03 (1.0 = perfectly ordered, <=0 = inverted/noisy). Top-quality names (compounder_80+) win 52.4%, so the quality axis carries more signal than the verdict axis right now.", "verdict_notes": {"HIGH RISK": "71.3% win; driven by DD +114.9%, TRV +23.0%; dragged by ALB -23.5%, SATS -21.4%", "FAIR VALUE": "63.1% win; driven by NTAP +39.0%, CRL +27.3%; dragged by ORCL -46.8%, FSLR -30.7%", "STRONG OPPORTUNITY": "59.0% win; driven by BBY +40.4%, TMO +24.8%; dragged by TTD -22.0%, NFLX -17.3%", "OPPORTUNITY": "56.4% win; driven by BBY +31.7%, PYPL +27.0%; dragged by ORCL -33.7%, IBM -17.7%", "HOLD / NEUTRAL": "50.9% win; driven by MQ +338.3%, ATAI +69.0%; dragged by HTZ -64.9%, WLFC -63.9%", "EXPENSIVE": "43.3% win; driven by TECH +24.9%, DELL +17.8%; dragged by CRWD -55.4%, KLAC -51.1%"}, "patterns": ["The high-conviction buy bucket underperforms lower-conviction ones \u2014 verdict scoring is currently anti-correlated with forward returns (rank corr -0.03).", "Systematic losers dragging multiple verdicts: ORCL, ROL, FSLR, TTD, PNR, LVS \u2014 these lose regardless of the label assigned.", "Systematic winners across verdicts: PSX, VLO, BBY, TMO, WELL, BALL \u2014 consistently positive wh
## 3. by_compounder_bucket — does the QUALITY axis fare better than the verdict axis

- `04:43:42`   compounder_<70       n=27963   win_rate=52.8% avg=-0.37%
- `04:43:42`   compounder_80+       n=2475    win_rate=52.4% avg=0.45%
- `04:43:42`   compounder_70-80     n=3513    win_rate=42.4% avg=-2.72%
- `04:43:42` ✅ PROBE COMPLETE
