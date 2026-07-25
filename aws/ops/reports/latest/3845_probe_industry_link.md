# ops 3845 — PROBE: port -> industry percentage feasibility

**Status:** success  
**Duration:** 0.3s  
**Finished:** 2026-07-25T02:51:52+00:00  

## Data

| countries | industries | lines | per_country_shares |
|---|---|---|---|
| 10 | 16 | 26 | False |

## Log
## 1. import-canary live shape

- `02:51:52`   generated_at=2026-07-24T13:40:06.090772+00:00 top-level=['version', 'generated_at', 'data_month', 'lag_note', 'n_lines', 'lines', 'naics_lines', 'signals', 'industry_rollup', 'degraded', 'coverage', 'scope_note', 'attribution', 'source_url']
- `02:51:52` ✅   lines=26
- `02:51:52`   line keys: ['level', 'month', 'yoy_pct', 'yoy_prev_pct', 'accel_pp', 'mom_3m_pct', 'n_months', 'level_code', 'code', 'label', 'industry', 'basis', 'concentration', 'source_shift', 'z_yoy', 'hist_n']
- `02:51:52`   sample: {"level": 27608702.0, "month": "2026-05", "yoy_pct": -30.41, "yoy_prev_pct": -23.39, "accel_pp": -7.02, "mom_3m_pct": 16.65, "n_months": 15, "level_code": "HS4", "code": "2836", "label": "Carbonates (incl. lithium carbonate)", "industry": "Chemicals", "basis": "HS", "concentration": {"top_source": "Germany", "top_share_pct": 29.5, "hhi": 0.142, "fragile": false, "shares_pct": {"Germany": 29.5, "China": 11.4, "Canada": 10.6, "Korea, South": 9.6, "Singapore": 7.0, "Ireland": 7.0, "Indonesia": 5.5, "Japan": 5.4}, "covered_usd": 10559078.0}, "source_shift": {"gainer": "Singapore", "gainer_pp": 7.0
## 2. Is a PER-COUNTRY SHARE published?

- `02:51:52`   [Chemicals] Carbonates (incl. lithium carbonate)
- `02:51:52`     concentration keys: ['top_source', 'top_share_pct', 'hhi', 'fragile', 'shares_pct', 'covered_usd']
- `02:51:52`     'shares_pct' dict -> ['Germany', 'China', 'Canada', 'Korea, South', 'Singapore', 'Ireland', 'Indonesia', 'Japan']
- `02:51:52`     source_shift: {"gainer": "Singapore", "gainer_pp": 7.05, "loser": "Korea, South", "loser_pp": -15.88, "all_pp": {"Singapore": 7.05, "Canada": 4.55, "China": 3.66, "India": 2.18, "Ireland": 1.82, "Japan": 1.5, "Mexico": 1.22, "Netherla
- `02:51:52`   [Uranium] Radioactive elements (uranium)
- `02:51:52`     concentration keys: ['top_source', 'top_share_pct', 'hhi', 'fragile', 'shares_pct', 'covered_usd']
- `02:51:52`     'shares_pct' dict -> ['Canada', 'Germany', 'Netherlands', 'Ireland', 'Japan']
- `02:51:52`     source_shift: {"gainer": "Canada", "gainer_pp": 61.63, "loser": "China", "loser_pp": -27.78, "all_pp": {"Canada": 61.63, "Ireland": 0.2, "Japan": 0.0, "Netherlands": -10.27, "Germany": -23.78, "China": -27.78}}
- `02:51:52`   [Drug Manufacturers] Medicaments, dosed
- `02:51:52`     concentration keys: ['top_source', 'top_share_pct', 'hhi', 'fragile', 'shares_pct', 'covered_usd']
- `02:51:52`     'shares_pct' dict -> ['Switzerland', 'India', 'China', 'Ireland', 'Germany', 'Canada', 'Malaysia', 'Japan']
- `02:51:52`     source_shift: {"gainer": "China", "gainer_pp": 7.94, "loser": "India", "loser_pp": -9.31, "all_pp": {"China": 7.94, "Switzerland": 7.19, "Ireland": 4.48, "Canada": 2.38, "Japan": 0.89, "Korea, South": 0.11, "Thailand": 0.09, "Indonesi
- `02:51:52`   [Footwear & Accessories] Leather footwear
- `02:51:52`     concentration keys: ['top_source', 'top_share_pct', 'hhi', 'fragile', 'shares_pct', 'covered_usd']
- `02:51:52`     'shares_pct' dict -> ['Indonesia', 'Singapore', 'China', 'Mexico', 'Germany', 'India', 'Vietnam', 'Ireland']
- `02:51:52`     source_shift: {"gainer": "Germany", "gainer_pp": 1.77, "loser": "China", "loser_pp": -4.91, "all_pp": {"Germany": 1.77, "Indonesia": 1.57, "Singapore": 0.73, "Mexico": 0.57, "Vietnam": 0.2, "Ireland": 0.18, "Japan": 0.1, "Korea, South
- `02:51:52`   [Metals & Mining] Refined copper
- `02:51:52`     concentration keys: ['top_source', 'top_share_pct', 'hhi', 'fragile', 'shares_pct', 'covered_usd']
- `02:51:52`     'shares_pct' dict -> ['Canada', 'Mexico', 'Japan', 'Korea, South', 'Germany', 'India', 'China', 'Ireland']
- `02:51:52`     source_shift: {"gainer": "Canada", "gainer_pp": 31.93, "loser": "India", "loser_pp": -14.96, "all_pp": {"Canada": 31.93, "Mexico": 6.3, "Germany": 1.47, "China": 0.01, "Ireland": 0.0, "Thailand": 0.0, "Netherlands": 0.0, "Singapore": 
- `02:51:52`   [Metals & Mining] Unwrought aluminium
- `02:51:52`     concentration keys: ['top_source', 'top_share_pct', 'hhi', 'fragile', 'shares_pct', 'covered_usd']
- `02:51:52`     'shares_pct' dict -> ['Canada', 'India', 'Mexico', 'Korea, South', 'Ireland', 'Singapore', 'Vietnam', 'Netherlands']
- `02:51:52`     source_shift: {"gainer": "India", "gainer_pp": 8.89, "loser": "Canada", "loser_pp": -12.58, "all_pp": {"India": 8.89, "Mexico": 1.8, "Korea, South": 0.74, "Ireland": 0.66, "Singapore": 0.37, "Germany": 0.04, "Netherlands": 0.03, "Chin
- `02:51:52`   [Computer Hardware] Computers & data-processing units
- `02:51:52`     concentration keys: ['top_source', 'top_share_pct', 'hhi', 'fragile', 'shares_pct', 'covered_usd']
- `02:51:52`     'shares_pct' dict -> ['Mexico', 'Taiwan', 'Indonesia', 'Vietnam', 'China', 'Switzerland', 'Thailand', 'Malaysia']
- `02:51:52`     source_shift: {"gainer": "Vietnam", "gainer_pp": 4.79, "loser": "Indonesia", "loser_pp": -5.89, "all_pp": {"Vietnam": 4.79, "Mexico": 2.15, "Switzerland": 1.01, "Malaysia": 0.25, "Thailand": 0.14, "Korea, South": 0.04, "Germany": 0.01
- `02:51:52`   [Computer Hardware] Computer parts & accessories
- `02:51:52`     concentration keys: ['top_source', 'top_share_pct', 'hhi', 'fragile', 'shares_pct', 'covered_usd']
- `02:51:52`     'shares_pct' dict -> ['Taiwan', 'Indonesia', 'Thailand', 'Korea, South', 'China', 'Vietnam', 'Mexico', 'Singapore']
- `02:51:52`     source_shift: {"gainer": "Indonesia", "gainer_pp": 11.88, "loser": "Taiwan", "loser_pp": -21.62, "all_pp": {"Indonesia": 11.88, "Thailand": 9.7, "Korea, South": 5.21, "Vietnam": 1.3, "Malaysia": 0.06, "Japan": -0.0, "Netherlands": -0.
## 3. Industry universe available

- `02:51:52`   16 industries: ['Auto Manufacturers', 'Auto Parts', 'Chemicals', 'Computer Hardware', 'Drug Manufacturers', 'Electrical Equipment', 'Electronic Components', 'Footwear & Accessories', 'Furnishings', 'Industrial Machinery', 'Medical Devices', 'Metals & Mining', 'Semiconductor Equipment', 'Semiconductors', 'Solar', 'Uranium']
## 4. Countries named in the trade data

- `02:51:52`   10 distinct country strings: ['Canada', 'China', 'Germany', 'Indonesia', 'Japan', 'Korea, South', 'Mexico', 'Switzerland', 'Taiwan', 'Vietnam']
## 5. Verdict — what percentage is honestly computable?

- `02:51:52` ⚠   Only a single top_source per line is published. Honest
- `02:51:52` ⚠   output is then a RANKED EXPOSURE (which industries this
- `02:51:52` ⚠   country dominates) with the DIRECTION of impact — and NO
- `02:51:52` ⚠   fabricated percentage. Widening the country split would
- `02:51:52` ⚠   require extending import-canary to publish full shares.
- `02:51:52` ✅ PROBE COMPLETE — no code written
