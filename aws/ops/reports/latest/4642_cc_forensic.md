# ops 4642 — cryptocap forensic

**Status:** success  
**Duration:** 47.7s  
**Finished:** 2026-08-12T21:38:52+00:00  

## Log
## deployed-source CC context

- `21:38:05` marker: """justhodl-liquidity-reversal v1.4.3 (ops 4641)
- `21:38:05`   597|     self-building daily history (institutional pattern: the
- `21:38:05`   598|     series grows itself into trend basis)."""
- `21:38:05`   599|     if sym not in CC_MAP:
- `21:38:05`   600|         return None
- `21:38:05`   601|     kind, key = CC_MAP[sym]
- `21:38:05`      ---
- `21:38:05`   652|     if sid:
- `21:38:05`   653|         return fred_fallback("FRED:" + sid, budget)
- `21:38:05`   654|     if leg in CC_MAP:
- `21:38:05`   655|         return cryptocap_fallback(leg, CC_BUDGET)
- `21:38:05`   656|     if leg in CURATED_LEG_YH:
- `21:38:05`      ---
- `21:38:05`  1556|             if (node is None or not extract_series(node)) \
- `21:38:05`  1557|                     and any(op in sym for op in "-/+") \
- `21:38:05`  1558|                     and ":" in sym and sym not in CC_MAP:
- `21:38:05`  1559|                 fb = eval_composite(sym, budget)
- `21:38:05`  1560|                 if fb and fb.get("series"):
- `21:38:05`      ---
- `21:38:05`  1623|                     if fb:
- `21:38:05`  1624|                         node = fb
- `21:38:05`  1625|                 elif sym in CC_MAP:
- `21:38:05`  1626|                     fb = cryptocap_fallback(sym, CC_BUDGET)
- `21:38:05`  1627|                     if fb:
- `21:38:05`      ---
## provider egress from runner

- `21:38:05` api.coingecko.com/api/v3/global -> 200 {"data":{"active_cryptocurrencies":18398,"upcoming_icos":0,"ongoing_icos":49,"ended_icos":3376,"markets":1488,
- `21:38:05` api.coinpaprika.com/v1/global -> 200 {"market_cap_usd":2273636285892,"volume_24h_usd":359143516909,"bitcoin_dominance_percentage":55.98,"cryptocurr
- `21:38:05` api.coincap.io/v2/assets?ids=tet -> ERR <urlopen error [Errno -2] Name or service not known>
## warm cc_* state

- `21:38:05` cc_TOTAL.json (150B): {"series": [{"date": "2026-08-12", "value": 2263464565193.103}], "fetched_at": "2026-08-12T21:29:13+00:00", "via": "CoinGecko global (self-b
- `21:38:05` cc_USDC_D.json (141B): {"series": [{"date": "2026-08-12", "value": 3.182656}], "fetched_at": "2026-08-12T21:28:45+00:00", "via": "CoinGecko global (self-building)"
- `21:38:05` cc_USDT_D.json (141B): {"series": [{"date": "2026-08-12", "value": 8.082385}], "fetched_at": "2026-08-12T21:28:45+00:00", "via": "CoinGecko global (self-building)"
- `21:38:06` cc_USDT_D_CRYPTOCAP_USDC_D.json (142B): {"series": [{"date": "2026-08-12", "value": 11.265041}], "fetched_at": "2026-08-12T21:32:04+00:00", "via": "CoinGecko global (self-building)
## invoke + full row JSON

- `21:38:51` fn_error=None
- `21:38:52` row: {"symbol": "CRYPTOCAP:USDT.D+CRYPTOCAP:USDC.D", "name": "Composite: CRYPTOCAP:USDT.D+CRYPTOCAP:USDC.D", "resolved": false, "move_state": "UNRESOLVED", "range_state": "UNRESOLVED"}
- `21:38:52` ✅ forensic complete — evidence above
