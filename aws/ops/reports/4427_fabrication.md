# ops 4427 — SPEC F fabrication audit — PASS — 898 fabrication sites across 235 engines measured, report published
- stats: {
 "engines_total": 785,
 "with_numeric": 731,
 "with_provenance": 178,
 "with_mock": 63,
 "with_hedge": 276,
 "fallback_sites": 898,
 "engines_with_fallback": 235
}
- provenance coverage: 24.4%
- top offenders: [
 {
  "engine": "aiapi-market-analyzer",
  "fallback_count": 0,
  "mock_count": 39,
  "risk_score": 117,
  "sample_fallbacks": [],
  "sample_mocks": [
   "L50: \"market_phase\": random.choice([\"bull\", \"bear\", \"transition\"]),",
   "L51: \"risk_level\": random.choice([\"low\", \"moderate\", \"high\"]),"
  ]
 },
 {
  "engine": "justhodl-signal-board",
  "fallback_count": 53,
  "mock_count": 0,
  "risk_score": 106,
  "sample_fallbacks": [
   "L69: uv, ov = s.get(\"n_undervalued\") or 0, s.get(\"n_overvalued\") or 0",
   "L88: b = d.get(\"n_pressure_building\") or 0",
   "L89: c = d.get(\"n_shorts_covering\") or 0"
  ],
  "sample_mocks": []
 },
 {
  "engine": "justhodl-prepump-alerts-router",
  "fallback_count": 41,
  "mock_count": 0,
  "risk_score": 82,
  "sample_fallbacks": [
   "L107: days = ticket.get(\"expected_horizon_days\") or 0",
   "L127: entry = t.get(\"entry\") or 0",
   "L128: stop = t.get(\"stop_loss\") or 0"
  ],
  "sample_mocks": []
 },
 {
  "engine": "justhodl-stock-screener",
  "fallback_count": 34,
  "mock_count": 0,
  "risk_score": 68,
  "sample_fallbacks": [
   "L703: chg_pct = inst_shares_change_pct or 0",
   "L704: inv_chg_pct = inst_investors_chg_pct or 0",
   "L1513: prior_streak = prior_s.get(\"beatStreak\") or 0"
  ],
  "sample_mocks": []
 },
 {
  "engine": "justhodl-cds-monitor",
  "fallback_count": 5,
  "mock_count": 16,
  "risk_score": 58,
  "sample_fallbacks": [
   "L214: ltd = bs.get(\"longTermDebt\") or 0",
   "L215: std = bs.get(\"shortTermDebt\") or 0",
   "L216: total_debt = bs.get(\"totalDebt\") or 0"
  ],
  "sample_mocks": [
   "L23: probability and a synthetic CDS spread for the global systemically-",
   "L28: and bank spreads, so the engine leads with DD and treats the synthetic"
  ]
 },
 {
  "engine": "justhodl-crypto-opportuni
- posted: {"ok": true, "err": null}
