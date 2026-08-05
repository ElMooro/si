# ops 4420 — handshake agreed + fixes — PASS — hybrid handshake answered, ACK dedupe + ceiling 48 + inline evidence live
- deploys: bus=True agent=True
- answer posted: {"ok": true, "err": null} | verify repost: {"ok": false, "err": "rejected_no_evidence"}
- inline evidence sent:
{
 "V4_units": {
  "fed_bs_bn": 6738.19,
  "tga_bn": 910.776,
  "net_liquidity_bn": 5825.16
 },
 "V5_catalog_series": 63,
 "V5_categories": [
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
 ],
 "V6_crisis_enrichment": 32,
 "V6_derived": {
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
 },
 "V7_plumbing_enrichment": 25,
 "V7_four_canary": {
  "canaries": {
   "sofr_iorb": {
    "label": "SOFR \u2212 IORB",
    "value_bp": 1.0,
    "state": "CALM",
    "thresholds": {
     "amber": 5,
     "red": 10,
     "note": "brain: >+5bp amber, >+10bp red"
    }
   },
   "hy_oas": {
    "label": "HY OAS",
    "value": 2.73,
    "z": -0.77,
    "state": "CALM"
   },
   "move": {
    "label": "MOVE (bond vol)",
    "pending_source": "MOVE is not on FRED \u2014 join from bond-vol.json fleet feed",
    "thresholds": {
     "amber": 120,
     "red": 140
    }
   },
   "on_off_run": {
    "label": "On/off-the-run 10Y spread",
    "pending_source": "NY Fed, not FRED \u2014 join from treasury-noise.json"
   }
  },
  "n_firing": 0,
  "firing": [],
  "verdict": "CALM",
  "brain_rule": "If all four move together on a non-quarter-end day, that is the signature of a basis-trade / repo unwind begi
