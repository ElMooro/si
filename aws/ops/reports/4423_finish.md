# ops 4423 — finish queue + batch verification — PARTIAL — posted={'ok': False, 'err': 'rejected_no_evidence'}, stages=3/3
- evidence settled: True | precheck: [{"ref": "liquidity-data.json", "resolves": true}, {"ref": "data/plumbing-stress.json", "resolves": true}, {"ref": "data/bond-vol.json", "resolves": true}, {"ref": "data/crisis-plumbing.json", "resolves": true}]
- posted: {"ok": false, "err": "rejected_no_evidence"}
- batch:
{
 "A_four_canary_4of4": {
  "verdict": "CALM",
  "n_firing": 0,
  "canaries": {
   "sofr_iorb": {
    "value": 1.0,
    "state": "CALM",
    "source": null,
    "pending": null
   },
   "hy_oas": {
    "value": 2.73,
    "state": "CALM",
    "source": null,
    "pending": null
   },
   "move": {
    "value": 70.88,
    "state": "CALM",
    "source": "bond-vol.json:move.value",
    "pending": null
   },
   "on_off_run": {
    "value": 2.795,
    "state": "CALM",
    "source": "treasury-noise.json:noise_bps",
    "pending": null
   }
  }
 },
 "B_real_MOVE": {
  "value": 70.88,
  "z": -0.92,
  "pctile_2y": 21.9,
  "state": "CALM",
  "source": "real ^MOVE index (public quote)",
  "is_proxy": false,
  "n_obs": 485
 },
 "C_dxy_hero": {
  "regime": "NEUTRAL",
  "level": 119.7034,
  "z": 0.14,
  "pctile_5y": 55.2,
  "note": "brain: DXY is the most important chart \u2014 promoted to hero"
 },
 "D_credit_first_sequence": {
  "verdict": "CLEAR \u2014 no stage firing",
  "current_stage": 0,
  "stages": [
   {
    "stage": 1,
    "name": "Credit stress (HY OAS)",
    "value": 2.73,
    "z": -0.77,
    "fired": false
   },
   {
    "stage": 2,
    "name": "Dollar spike (DXY)",
    "value": 119.7034,
    "z": 0.14,
    "fired": false
   },
   {
    "stage": 3,
    "name": "Equity drawdown (SPX 60d)",
    "value": 5.44,
    "z": null,
    "fired": false
   }
  ]
 },
 "E_global_4cb_stack": {
  "stack": {
   "Fed": 6738.2,
   "ECB": 6843.7,
   "BOJ": 4048.1
  },
  "total_usd_bn": null,
  "china_credit_impulse": null,
  "join_notes": null
 },
 "F_liquidity_catalog": {
  "n_series": 63,
  "categories": [
   "dollar",
   "fed_balance_sheet",
   "funding",
   "money_supply",
   "other",
   "reserves",
   "rrp",
   "soma",
   "tga",
   "yields"
  ]
 },
 "G_units_fixed": {
  "fed_balance_sheet": 6738.19,
  "tga": 910.776,
  "net_liquidity": 5825.16
 },
 "H_crisis_enrichment": {
  "n_series": 32,
  "derived": {
   "hy_ig_spread": {
    "label": "HY \u2212 IG spread",
    "unit": "pp",
    "value": 2.06,
    "note": "credit-quality dispersion"
   },
   "ccc_bb_spread": {
    "label": "CCC \u2212 BB spread",
    "unit": "pp",
    "value": 8.61,
    "note": "internal-credit dispersion tell"
   }
  }
 },
 "I_plumbing_enrichment": {
  "n_series": 25
 },
 "J_breadth_thrust_fix": {
  "forward_12m": {
   "return_pct": 31.55,
   "win_rate_pct": 100.0,
   "n": 5,
   "median_pct": 31.08,
   "best_pct": 49.08,
   "worst_pct": 8.17,
   "basis": "SPY next 365 calendar days"
  },
  "triggers": 
