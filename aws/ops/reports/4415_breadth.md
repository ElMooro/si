# ops 4415 — breadth-thrust fix — PARTIAL — n_history=0, diag=[]
- deployed: True | invoke: {"code": 200, "fn_err": null}
- diagnostics: []
- result: {
 "n_history": 0,
 "distinct_trigger_prices": 0,
 "trigger_sample": [],
 "fwd_12m_sample": [],
 "forward_expectations": {
  "1m": {
   "return_pct": 3.18,
   "win_rate_pct": 62.5,
   "n": 8,
   "median_pct": 4.85,
   "best_pct": 6.72,
   "worst_pct": 0.0,
   "basis": "SPY next 30 calendar days"
  },
  "3m": {
   "return_pct": 6.04,
   "win_rate_pct": 62.5,
   "n": 8,
   "median_pct": 8.59,
   "best_pct": 14.51,
   "worst_pct": 0.0,
   "basis": "SPY next 90 calendar days"
  },
  "6m": {
   "return_pct": 10.94,
   "win_rate_pct": 62.5,
   "n": 8,
   "median_pct": 15.06,
   "best_pct": 29.47,
   "worst_pct": 0.0,
   "basis": "SPY next 180 calendar days"
  },
  "12m": {
   "return_pct": 19.72,
   "win_rate_pct": 62.5,
   "n": 8,
   "median_pct": 26.62,
   "best_pct": 49.08,
   "worst_pct": 0.0,
   "basis": "SPY next 365 calendar days"
  }
 },
 "state": "NULL",
 "signal_strength": 25
}
