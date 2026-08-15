# ops 4718 — deep structural probe for justhodl-invest fixes

**Status:** success  
**Duration:** 1.5s  
**Finished:** 2026-08-15T21:00:30+00:00  

## Log
## forward-returns.json — the get_spx_er() crash site

- `21:00:29`   data/forward-returns.json :: assets =
{
  "SPY": {
    "ticker": "str: 'SPY'",
    "name": "str: 'US Large-Cap Stocks (S&P 500)'",
    "current_price": 773.26,
    "trailing_dividend_yield_pct": 0.97,
    "buyback_yield_assumption_pct": 2.0,
    "nominal_growth_assumption_pct": 4.25,
    "forward_er_10y_pct": 7.22,
    "history_30y": {
      "realized_cagr_pct": "<float>",
      "er_median_pct": "<float>",
      "er_p10_pct": "<float>",
      "er_p90_pct": "<float>"
    },
    "current_vs_history_percentile": 53,
    "verdict": "str: 'FAIR'",
    "verdict_color": "str: '#fbbf24'",
    "verdict_text": "str: 'Near the historical median (53th pct) \u2014 fairly priced.'",
    "risk": {
      "vol_pct_annualized": "<float>",
      "worst_12mo_drawdown_pct": "<float>",
      "prob_negative_10y_pct": "<float>",
      "sharpe_vs_cash": "<float>"
    },
    "ten_k_in_10yr_usd": {
      "central": "<int>",
      "p10_bear": "<int>",
      "p90_bull": "<int>"
    },
    "explainer_retail": "str: \"What you get from owning America's biggest companies. The lo\"",
    "model_inputs": {
      "macro": "<dict>"
    }
  },
  "QQQ": {
    "ticker": "str: 'QQQ'",
    "name": "str: 'US Tech / NASDAQ-100'",
    "current_price": 723.03,
    "trailing_dividend_yield_pct": 0.42,
    "buyback_yield_assumption_pct": 2.5,
    "nominal_growth_assumption_pct": 5.25,
    "forward_er_10y_pct": 8.17,
    "history_30y": {
      "realized_cagr_pct": "<float>",
      "er_median_pct": "<float>",
      "er_p10_pct": "<float>",
      "er_p90_pct": "<float>"
    },
    "current_vs_history_percentile": 54,
    "verdict": "str: 'FAIR'",
    "verdict_color": "str: '#fbbf24'",
    "verdict_text": "str: 'Near the historical median (54th pct) \u2014 fairly priced.'",
    "risk": {
      "vol_pct_annualized": "<float>",
      "worst_12mo_drawdown_pct": "<float>",
      "prob_negative_10y_pct": "<float>",
      "sharpe_vs_cash": "<float>"
    },
    "ten_k_in_10yr_usd": {
      "central": "<int>",
      "p10_bear": "<int>",
      "p90_bull": "<int>"
    },
    "explainer_retail": "str: 'Concentrated tech bet. Higher long-run growth than SPY but w'",
    "model_inputs": {
      "macro": "<dict>"
    }
  },
  "IWM": {
    "ticker": "str: 'IWM'",
    "name": "str: 'US Small-Cap (Russell 2000)'",
    "current_price": 301.56,
    "trailing_dividend_yield_pct": 0.88,
    "buyback_yield_assumption_pct": 1.0,
    "nominal_growth_assumption_pct": 4.75,
    "forward_er_10y_pct": 6.63,
    "history_30y": {
      "realized_cagr_pct": "<float>",
      "er_median_pct": "<float>",
      "er_p10_pct": "<float>",
      "er_p90_pct": "<float>"
    },
    "current_vs_history_percentile": 34,
    "verdict": "str: 'POOR'",
    "verdict_color": "str: '#f97316'",
    "verdict_text": "str: 'Bottom 66% of history \u2014 paying you less than usual.'",
    "risk": {
      "vol_pct_annualized": "<float>",
      "worst_12mo_drawdown_pct": "<float>",
      "prob_negative_10y_pct": "<float>",
      "sharpe_vs_cash": "<float>"
    },
    "ten_k_in_10yr_usd": {
- `21:00:29`   data/forward-returns.json :: rankings =
{
  "by_forward_er": [
    "str: 'BTC'",
    "str: 'ETH'",
    "str: 'QQQ'",
    "str: 'EEM'",
    "...+13 more"
  ],
  "by_sharpe": [
    "str: 'BTC'",
    "str: 'EFA'",
    "str: 'EEM'",
    "str: 'SPY'",
    "...+13 more"
  ],
  "by_opportunity_percentile": [
    "str: 'BIL'",
    "str: 'GLD'",
    "str: 'SLV'",
    "str: 'QQQ'",
    "...+13 more"
  ]
}
## canary-grid.json — signals + sub_grids (copper, lumber, korea)

- `21:00:29`   data/canary-grid.json :: signals =
[
  {
    "key": "str: 'korea_exports'",
    "name": "str: 'South Korea exports'",
    "sub_grid": "str: 'trade_shipping'",
    "lead_months": 3,
    "unit": "str: '%YoY'",
    "available": false,
    "as_of": "str: '2026-04-01'",
    "age_days": 136,
    "fred_series": "str: 'XTEXVA01KRM664S'",
    "reason": "str: 'stale \u2014 latest reading is 136d old (>95d); excluded to keep '"
  },
  {
    "key": "str: 'china_exports'",
    "name": "str: 'China exports'",
    "sub_grid": "str: 'trade_shipping'",
    "lead_months": 3,
    "unit": "str: '%YoY'",
    "available": false,
    "as_of": "str: '2026-04-01'",
    "age_days": 136,
    "fred_series": "str: 'XTEXVA01CNM664S'",
    "reason": "str: 'stale \u2014 latest reading is 136d old (>95d); excluded to keep '"
  },
  {
    "key": "str: 'singapore_nodx'",
    "name": "str: 'Singapore NODX (trade + chip hub)'",
    "sub_grid": "str: 'trade_shipping'",
    "lead_months": 2,
    "unit": "str: '%YoY'",
    "available": true,
    "value": 20.71,
    "as_of": "str: '2026-06-28'",
    "age_days": 48,
    "stale_warning": false,
    "fred_series": "str: 'feed:singapore-nodx:nodx_total.history'",
    "transform": "str: 'yoy'",
    "zscore": 1.48,
    "stress": 17.4,
    "read": "str: \"Singapore's NODX is growing \u2014 global trade and the electroni\""
  },
  {
    "key": "str: 'semiconductor_ip'",
    "name": "str: 'Semiconductor production (chip cycle)'",
    "sub_grid": "str: 'trade_shipping'",
    "lead_months": 3,
    "unit": "str: '%YoY'",
    "available": true,
    "value": 11.47,
    "as_of": "str: '2026-06-01'",
    "age_days": 75,
    "stale_warning": true,
    "fred_series": "str: 'IPG3344S'",
    "transform": "str: 'yoy'",
    "zscore": 0.54,
    "stress": 38.2,
    "read": "str: 'Semiconductor output is expanding \u2014 the chip cycle is in an '"
  },
  "...+61 more"
]
- `21:00:29`   data/canary-grid.json :: sub_grids =
{
  "trade_shipping": {
    "label": "str: 'Trade & Shipping'",
    "score": 21.4,
    "band": "str: 'WATCH'",
    "n_signals": 4,
    "lead_months": 2.9
  },
  "commodity_cycle": {
    "label": "str: 'Commodity Cycle'",
    "score": 43.4,
    "band": "str: 'ELEVATED'",
    "n_signals": 9,
    "lead_months": 2.5
  },
  "funding_plumbing": {
    "label": "str: 'Funding Plumbing'",
    "score": 44.8,
    "band": "str: 'ELEVATED'",
    "n_signals": 14,
    "lead_months": 1.5
  },
  "labor_industrial": {
    "label": "str: 'Labour & Industrial'",
    "score": 41.0,
    "band": "str: 'ELEVATED'",
    "n_signals": 8,
    "lead_months": 2.1
  },
  "rates_credit": {
    "label": "str: 'Rates & Credit'",
    "score": 34.7,
    "band": "str: 'WATCH'",
    "n_signals": 14,
    "lead_months": 4.6
  },
  "global_risk": {
    "label": "str: 'Global Risk Appetite'",
    "score": 45.7,
    "band": "str: 'ELEVATED'",
    "n_signals": 10,
    "lead_months": 0.9
  }
}
## portwatch.json — where does Korea live?

- `21:00:29`   data/portwatch.json :: ports =
[
  {
    "id": "str: 'port1188'",
    "name": "str: 'Shanghai'",
    "country": "str: 'China'",
    "latest_7d_avg": 67.6,
    "prev_30d_avg": 80.4,
    "baseline_1y": 108.8,
    "z": -1.95,
    "vs_baseline_pct": -37.9,
    "yoy_pct": -49.6,
    "n_days": 393,
    "last_date": "str: '2026-08-07'",
    "status": "str: 'DISRUPTED'",
    "industry_exposure": {
      "available": "<bool>",
      "country_matched": "<str>",
      "port_yoy_pct": "<float>",
      "n_industries": "<int>",
      "top_industry": "<str>",
      "industries": "<list>",
      "method": "<str>",
      "limits": "<str>"
    }
  },
  {
    "id": "str: 'port518'",
    "name": "str: 'Jeddah'",
    "country": "str: 'Saudi Arabia'",
    "latest_7d_avg": 3.0,
    "prev_30d_avg": 7.3,
    "baseline_1y": 8.7,
    "z": -1.88,
    "vs_baseline_pct": -65.4,
    "yoy_pct": -66.1,
    "n_days": 393,
    "last_date": "str: '2026-08-07'",
    "status": "str: 'DISRUPTED'",
    "industry_exposure": {
      "available": "<bool>",
      "reason": "<str>"
    }
  },
  {
    "id": "str: 'port2027'",
    "name": "str: 'Shanghai (Yangshan)'",
    "country": "str: 'China'",
    "latest_7d_avg": 9.1,
    "prev_30d_avg": 10.9,
    "baseline_1y": 15.5,
    "z": -1.8,
    "vs_baseline_pct": -40.9,
    "yoy_pct": -46.7,
    "n_days": 393,
    "last_date": "str: '2026-08-07'",
    "status": "str: 'DISRUPTED'",
    "industry_exposure": {
      "available": "<bool>",
      "country_matched": "<str>",
      "port_yoy_pct": "<float>",
      "n_industries": "<int>",
      "top_industry": "<str>",
      "industries": "<list>",
      "method": "<str>",
      "limits": "<str>"
    }
  },
  {
    "id": "str: 'port570'",
    "name": "str: 'Yanbu (King Fahd Port)'",
    "country": "str: 'Saudi Arabia'",
    "latest_7d_avg": 0.3,
    "prev_30d_avg": 4.8,
    "baseline_1y": 4.4,
    "z": -1.66,
    "vs_baseline_pct": -93.5,
    "yoy_pct": -92.3,
    "n_days": 393,
    "last_date": "str: '2026-08-07'",
    "status": "str: 'DISRUPTED'",
    "industry_exposure": {
      "available": "<bool>",
      "reason": "<str>"
    }
  },
  "...+85 more"
]
- `21:00:29`   data/portwatch.json :: exporters =
[
  {
    "code": "str: 'SAU'",
    "country": "str: 'Saudi Arabia'",
    "n_ports": 6,
    "ports": [
      "<str>",
      "<str>",
      "<str>",
      "<str>"
    ],
    "avg_vs_baseline_pct": -72.5,
    "avg_z": -1.12,
    "verdict": "str: 'SLOWING'"
  },
  {
    "code": "str: 'ARE'",
    "country": "str: 'UAE'",
    "n_ports": 2,
    "ports": [
      "<str>",
      "<str>"
    ],
    "avg_vs_baseline_pct": -49.5,
    "avg_z": -0.72,
    "verdict": "str: 'SLOWING'"
  },
  {
    "code": "str: 'MEX'",
    "country": "str: 'Mexico'",
    "n_ports": 3,
    "ports": [
      "<str>",
      "<str>",
      "<str>"
    ],
    "avg_vs_baseline_pct": -32.6,
    "avg_z": -0.11,
    "verdict": "str: 'SLOWING'"
  },
  {
    "code": "str: 'FIN'",
    "country": "str: 'Finland'",
    "n_ports": 3,
    "ports": [
      "<str>",
      "<str>",
      "<str>"
    ],
    "avg_vs_baseline_pct": -26.9,
    "avg_z": -0.41,
    "verdict": "str: 'SLOWING'"
  },
  "...+20 more"
]
- `21:00:29`   data/portwatch.json :: chokepoints =
[
  {
    "id": "str: 'chokepoint28'",
    "name": "str: 'Kerch Strait'",
    "latest_7d_avg": 0.1,
    "prev_30d_avg": 2.2,
    "baseline_1y": 13.7,
    "z": -1.58,
    "vs_baseline_pct": -99.0,
    "yoy_pct": -99.2,
    "n_days": 395,
    "last_date": "str: '2026-08-09'",
    "status": "str: 'DISRUPTED'"
  },
  {
    "id": "str: 'chokepoint4'",
    "name": "str: 'Bab el-Mandeb Strait'",
    "latest_7d_avg": 25.1,
    "prev_30d_avg": 32.6,
    "baseline_1y": 35.0,
    "z": -1.49,
    "vs_baseline_pct": -28.1,
    "yoy_pct": -29.2,
    "n_days": 395,
    "last_date": "str: '2026-08-09'",
    "status": "str: 'DISRUPTED'"
  },
  {
    "id": "str: 'chokepoint3'",
    "name": "str: 'Bosporus Strait'",
    "latest_7d_avg": 62.9,
    "prev_30d_avg": 71.7,
    "baseline_1y": 87.6,
    "z": -1.4,
    "vs_baseline_pct": -28.2,
    "yoy_pct": -27.3,
    "n_days": 395,
    "last_date": "str: '2026-08-09'",
    "status": "str: 'DISRUPTED'"
  },
  {
    "id": "str: 'chokepoint6'",
    "name": "str: 'Strait of Hormuz'",
    "latest_7d_avg": 4.4,
    "prev_30d_avg": 6.4,
    "baseline_1y": 39.9,
    "z": -1.05,
    "vs_baseline_pct": -88.9,
    "yoy_pct": -93.8,
    "n_days": 395,
    "last_date": "str: '2026-08-09'",
    "status": "str: 'DISRUPTED'"
  },
  "...+24 more"
]
## asia-leads.json — taiwan_orders full shape (korea_exports already proven working)

- `21:00:29`   data/asia-leads.json :: taiwan_orders =
{
  "source": "str: 'https://eng.stat.gov.tw/Point.aspx?sid=t.6&n=4205&sms=11713'",
  "label": "str: 'Taiwan export orders (MOEA via DGBAS point page)'",
  "latest_usd_bn": 95.26,
  "yoy_pct": null,
  "period": "str: 'Jun. 2026'",
  "error": null,
  "via_stage1": "str: 'edge'",
  "stage2": "str: 'https://eng.stat.gov.tw/Point.aspx?sid=t.6&n=4205&sms=11713&'",
  "via_stage2": "str: 'edge'",
  "raw_head": "str: '       Import on customs basis Growth Rate       Export on c'",
  "stage3_candidates": [
    {
      "u": "<str>",
      "label": "<str>"
    },
    {
      "u": "<str>",
      "label": "<str>"
    },
    {
      "u": "<str>",
      "label": "<str>"
    },
    {
      "u": "<str>",
      "label": "<str>"
    },
    "...+2 more"
  ],
  "stage3_tried": [
    {
      "u": "<str>",
      "label": "<str>",
      "via": "<str>",
      "bytes": "<int>",
      "hit": "<bool>"
    },
    {
      "u": "<str>",
      "label": "<str>",
      "via": "<str>",
      "bytes": "<int>",
      "hit": "<bool>"
    },
    {
      "u": "<str>",
      "label": "<str>",
      "via": "<str>",
      "bytes": "<int>",
      "hit": "<bool>"
    }
  ],
  "stage4_tried": [
    {
      "u": "<str>",
      "via": "<str>",
      "bytes": "<int>",
      "hit": "<bool>"
    },
    {
      "u": "<str>",
      "via": "<str>",
      "bytes": "<int>",
      "hit": "<bool>"
    }
  ],
  "stage5_endpoints": [
    {
      "u": "<str>",
      "src_frame": "<str>"
    },
    {
      "u": "<str>",
      "src_frame": "<str>"
    }
  ],
  "stage5_tried": [
    {
      "u": "<str>",
      "via": "<str>",
      "bytes": "<int>",
      "json": "<bool>",
      "hit": "<bool>",
      "head": "<str>"
    },
    {
      "u": "<str>",
      "via": "<str>",
      "bytes": "<int>",
      "json": "<bool>",
      "hit": "<bool>",
      "head": "<str>"
    }
  ],
  "stage6_sitesns": [
    "str: '464'"
  ],
  "stage6_tried": [
    {
      "sn": "<str>",
      "via": "<str>",
      "bytes": "<int>",
      "hit": "<bool>",
      "titles": "<list>",
      "item_blob": "<str>"
    }
  ],
  "v17": true,
  "stage6_hit": {
    "sn": "str: '464'",
    "title": "str: 'Value of Export Orders'"
  },
  "levels_cached": 2
}
- `21:00:29`   data/asia-leads.json :: taiwan_exports =
{
  "source": "str: 'FRED VALEXPTWM052N'",
  "label": "str: 'Taiwan goods exports (monthly)'",
  "frequency": "str: 'M'",
  "last_period": "str: '2026-05-01'",
  "last_value": 78596.0,
  "yoy_pct": 48.33,
  "chg_3m_pct": 57.36,
  "n_obs": 137,
  "history_24m": [
    {
      "p": "<str>",
      "v": "<float>"
    },
    {
      "p": "<str>",
      "v": "<float>"
    },
    {
      "p": "<str>",
      "v": "<float>"
    },
    {
      "p": "<str>",
      "v": "<float>"
    },
    "...+20 more"
  ],
  "note": "str: 'Proxy for MOEA export ORDERS (orders lead shipments); direct'"
}
- `21:00:29`   data/asia-leads.json :: korea_exports =
{
  "source": "str: 'FRED XTEXVA01KRM667N'",
  "label": "str: 'Korea merchandise exports (monthly, NSA)'",
  "frequency": "str: 'M'",
  "last_period": "str: '2026-04-01'",
  "last_value": 85867480000.0,
  "yoy_pct": 47.96,
  "chg_3m_pct": 30.41,
  "n_obs": 136,
  "history_24m": [
    {
      "p": "<str>",
      "v": "<float>"
    },
    {
      "p": "<str>",
      "v": "<float>"
    },
    {
      "p": "<str>",
      "v": "<float>"
    },
    {
      "p": "<str>",
      "v": "<float>"
    },
    "...+20 more"
  ],
  "note": "str: '20-day customs flash (the true nowcast) requires a free Bank'"
}
## china-liquidity.json — tsf

- `21:00:29`   data/china-liquidity.json :: tsf =
{
  "source_annual": "str: 'DBnomics NBS/A_A0L08'",
  "annual_composition": [
    {
      "code": "<str>",
      "name": "<str>",
      "frequency": "<str>",
      "last_period": "<str>",
      "last_value": "<float>",
      "yoy_pct": "<float>"
    },
    {
      "code": "<str>",
      "name": "<str>",
      "frequency": "<str>",
      "last_period": "<str>",
      "last_value": "<float>",
      "yoy_pct": "<float>"
    },
    {
      "code": "<str>",
      "name": "<str>",
      "frequency": "<str>",
      "last_period": "<str>",
      "last_value": "<float>",
      "yoy_pct": "<float>"
    },
    {
      "code": "<str>",
      "name": "<str>",
      "frequency": "<str>",
      "last_period": "<str>",
      "last_value": "<float>",
      "yoy_pct": "<float>"
    },
    "...+4 more"
  ],
  "monthly": null,
  "note": "str: 'Real NBS Total Social Financing \u2014 replaces the money-acceler'",
  "pboc_monthly": {
    "source": "str: 'http://www.pbc.gov.cn/en/3688247/3688978/3709140/index.html'",
    "latest_report": {
      "title": "<str>",
      "url": "<str>",
      "attachment": "<NoneType>",
      "header": "<NoneType>",
      "n_rows_parsed": "<int>"
    },
    "series": null,
    "error": null,
    "note": "str: 'PBoC EN monthly AFRE Flow report (units: 100 million yuan). '",
    "rows": []
  },
  "pboc_cn": {
    "label": "str: 'PBoC CN TSF monthly flow (\u793e\u4f1a\u878d\u8d44\u89c4\u6a21\u589e\u91cf)'",
    "period": "str: '2025-01..08 (cum)'",
    "flow_trn_cny": 26.56,
    "yoy_delta_trn": 4.66,
    "title": "str: '2025\u5e748\u6708\u793e\u4f1a\u878d\u8d44\u89c4\u6a21\u589e\u91cf\u7edf\u8ba1\u6570\u636e\u62a5\u544a'",
    "url": "str: 'http://www.pbc.gov.cn/diaochatongjisi/116219/116225/523c260b'",
    "via": "str: 'edge'",
    "candidates": [
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "...+3 more"
    ],
    "error": null,
    "js_files": [],
    "body_probe": "str: '<!DOCTYPE html PUBLIC \"-//W3C//DTD XHTML 1.0 Transitional//E'",
    "content_refs": [
      "<str>",
      "<str>",
      "<str>",
      "<str>",
      "...+4 more"
    ],
    "cumulative": true
  }
}
- `21:00:29`   data/china-liquidity.json :: credit_impulse =
{
  "value_pp": -5.52,
  "is_proxy": true,
  "definition": "str: 'acceleration of money-supply YoY growth (pp change). A free '",
  "signal": "str: 'credit decelerating \u2014 forward headwind'"
}
## port-cargo.json — global_pulse

- `21:00:29`   data/port-cargo.json :: global_pulse =
{
  "import_tpd_7d": 32038460,
  "export_tpd_7d": 29811782,
  "total_chg_pct": -3.89
}
## freight-pulse.json — composite

- `21:00:29`   data/freight-pulse.json :: composite =
47.4
## grid-queue.json — national, queue_velocity, planned_capacity

- `21:00:29`   data/grid-queue.json :: national =
{
  "primary_metric": "str: 'mw_with_executed_ia'",
  "mw_with_executed_ia": 202469.5,
  "ia_measured_isos": [
    "str: 'CAISO'",
    "str: 'NYISO'",
    "str: 'MISO'",
    "str: 'SPP'",
    "...+1 more"
  ],
  "headline_queue_mw": 1195315.0,
  "headline_risk_adjusted_mw": 155391.0,
  "isos_live": [
    "str: 'CAISO'",
    "str: 'ERCOT'",
    "str: 'MISO'",
    "str: 'NYISO'",
    "...+1 more"
  ],
  "isos_missing": [
    "str: 'ISO-NE'",
    "str: 'PJM'"
  ],
  "n_isos_live": 5,
  "assumption": "str: \"headline_risk_adjusted applies LBNL's ~13%% all-request comp\"",
  "blind_spot": "str: 'large-LOAD (datacenter) interconnection is handled outside g'"
}
- `21:00:29`   data/grid-queue.json :: queue_velocity =
{
  "status": "str: 'LIVE'",
  "n_snapshots": 6,
  "span_days": 5,
  "national_ia_mw_per_month": 913.2,
  "national_headline_mw_per_month": 8317.4,
  "per_iso_ia_mw_per_month": {
    "CAISO": -1522.0,
    "NYISO": 0.0,
    "MISO": 0.0,
    "SPP": 2435.2,
    "ERCOT": 0.0
  },
  "method": "str: '\u0394 vs oldest archived snapshot, scaled to MW/month; the archi'"
}
- `21:00:29`   data/grid-queue.json :: planned_capacity =
{
  "period": "str: '2026-05'",
  "uprate_by_state": {
    "NJ": 206.7,
    "WA": 162.0,
    "CA": 61.3,
    "AL": 55.9,
    "SC": 53.0,
    "OR": 51.6,
    "OK": 34.0,
    "MO": 22.0,
    "KS": 18.0,
    "AZ": 17.0,
    "ID": 12.0,
    "MT": 10.1,
    "CT": 7.5,
    "AK": 0.0,
    "TX": 0.0,
    "MN": 0.0,
    "KY": 0.0,
    "NE": 0.0,
    "NY": 0.0,
    "FL": 0.0
  },
  "uprate_by_sector": {
    "electric-utility": 496.89999999999986,
    "ipp-non-chp": 214.2,
    "commercial-non-chp": 0.0,
    "ipp-chp": 0.0,
    "industrial-chp": 0.0,
    "commercial-chp": 0.0
  },
  "industrial_plants": [
    {
      "state": "<str>",
      "entity": "<str>",
      "plant": "<str>",
      "county": "<NoneType>",
      "tech": "<str>",
      "capacity_mw": "<float>",
      "sector": "<str>"
    },
    {
      "state": "<str>",
      "entity": "<str>",
      "plant": "<str>",
      "county": "<NoneType>",
      "tech": "<str>",
      "capacity_mw": "<float>",
      "sector": "<str>"
    },
    {
      "state": "<str>",
      "entity": "<str>",
      "plant": "<str>",
      "county": "<NoneType>",
      "tech": "<str>",
      "capacity_mw": "<float>",
      "sector": "<str>"
    },
    {
      "state": "<str>",
      "entity": "<str>",
      "plant": "<str>",
      "county": "<NoneType>",
      "tech": "<str>",
      "capacity_mw": "<float>",
      "sector": "<str>"
    },
    "...+1 more"
  ],
  "n_industrial": 5,
  "upcoming_uprates": []
}
## pjm-grid.json — load, forecast, ai_demand_read

- `21:00:29`   data/pjm-grid.json :: load =
{
  "current_gw": 112.52,
  "current_ts_ept": "str: '2026-08-13T23:05'",
  "avg_24h_gw": 113.19,
  "momentum_8d_pct": -4.79,
  "n_obs": 2000
}
- `21:00:29`   data/pjm-grid.json :: forecast =
{
  "peak_gw": 137.42,
  "peak_at_ept": "str: '2026-08-17T17:00:00'",
  "current_vs_peak_pct": 81.9
}
- `21:00:29`   data/pjm-grid.json :: ai_demand_read =
"str: 'RTO demand -4.79% over 8 days \u2014 contracting electricity pull'"
## construction-housing.json — signals, cycle_score

- `21:00:30`   data/construction-housing.json :: signals =
[
  "str: 'Starts +3.5% YoY'",
  "str: 'New-home sales -5.6% YoY \u2014 demand soft'",
  "str: \"Months' supply 9.3 \u2014 inventory heavy\"",
  "str: 'Builder input costs +10.5% YoY'"
]
- `21:00:30`   data/construction-housing.json :: cycle_score =
-1
## taiwan-moea.json — export_orders, semiconductor

- `21:00:30`   data/taiwan-moea.json :: export_orders =
{
  "latest_period": "str: '2026-06'",
  "latest_value": 95262.0,
  "yoy_pct": 59.43,
  "yoy_3mma_pct": 51.57,
  "yoy_z_5y": 2.24,
  "history": [
    {
      "p": "<str>",
      "v": "<float>"
    },
    {
      "p": "<str>",
      "v": "<float>"
    },
    {
      "p": "<str>",
      "v": "<float>"
    },
    {
      "p": "<str>",
      "v": "<float>"
    },
    "...+56 more"
  ],
  "n": 510,
  "unit": "str: '\u767e\u842c\u7f8e\u5143'",
  "series": "str: 'Taiwan export orders \u2014 grand total (US$)'",
  "read": "str: 'EXPANDING \u2014 global tech demand firm'"
}
- `21:00:30`   data/taiwan-moea.json :: semiconductor =
{
  "production": {
    "latest_period": "str: '2026-05'",
    "latest_value": 855718330.0,
    "yoy_pct": 27.91,
    "yoy_3mma_pct": 26.15,
    "yoy_z_5y": 1.0,
    "history": [
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "...+56 more"
    ],
    "n": 533,
    "series": "str: 'Taiwan electronic-components production (semiconductor bellw'",
    "read": "str: 'UP-LEG \u2014 chip cycle expanding'"
  },
  "inventory": {
    "latest_period": "str: '2026-05'",
    "latest_value": 586417422.0,
    "yoy_pct": 64.4,
    "yoy_3mma_pct": 52.29,
    "yoy_z_5y": 1.38,
    "history": [
      "<dict>",
      "<dict>",
      "<dict>",
      "<dict>",
      "...+56 more"
    ],
    "n": 533,
    "series": "str: 'Taiwan electronic-components inventory (scarcity/glut tell)'"
  }
}
## industry-boom.json — full label list (need Homebuilding/Industrial-Machinery equivalents)

- `21:00:30`   132 labels:
Advertising Agencies
Aerospace & Defense
Agricultural - Machinery
Agricultural Farm Products
Agricultural Inputs
Airlines, Airports & Air Services
Apparel - Footwear & Accessories
Apparel - Manufacturers
Apparel - Retail
Asset Management - Cryptocurrency
Auto - Dealerships
Auto - Manufacturers
Auto - Parts
Banks - Diversified
Banks - Regional
Beverages - Alcoholic
Beverages - Non-Alcoholic
Beverages - Wineries & Distilleries
Biotechnology
Broadcasting
Chemicals
Chemicals - Specialty
Communication Equipment
Computer Hardware
Conglomerates
Construction
Construction Materials
Consulting Services
Consumer Electronics
Copper
Department Stores
Discount Stores
Diversified Utilities
Drug Manufacturers - General
Drug Manufacturers - Specialty & Generic
Education & Training Services
Electrical Equipment & Parts
Electronic Gaming & Multimedia
Engineering & Construction
Entertainment
Financial - Capital Markets
Financial - Credit Services
Financial - Data & Stock Exchanges
Financial - Mortgages
Food Confectioners
Food Distribution
Furnishings, Fixtures & Appliances
Gambling, Resorts & Casinos
Gold
Grocery Stores
Hardware, Equipment & Parts
Home Improvement
Household & Personal Products
Independent Power Producers
Industrial - Distribution
Industrial - Machinery
Industrial - Pollution & Treatment Controls
Industrial Materials
Information Technology Services
Insurance - Brokers
Insurance - Diversified
Insurance - Life
Insurance - Property & Casualty
Insurance - Reinsurance
Insurance - Specialty
Integrated Freight & Logistics
Internet Content & Information
Investment - Banking & Investment Services
Leisure
Luxury Goods
Manufacturing - Metal Fabrication
Manufacturing - Tools & Accessories
Marine Shipping
Medical - Care Facilities
Medical - Devices
Medical - Diagnostics & Research
Medical - Distribution
Medical - Equipment & Services
Medical - Healthcare Information Services
Medical - Healthcare Plans
Medical - Instruments & Supplies
Medical - Pharmaceuticals
Medical - Specialties
Oil & Gas Equipment & Services
Oil & Gas Exploration & Production
Oil & Gas Integrated
Oil & Gas Midstream
Oil & Gas Refining & Marketing
Other Precious Metals
Packaged Foods
Packaging & Containers
Paper, Lumber & Forest Products
Personal Products & Services
Publishing
REIT - Diversified
REIT - Healthcare Facilities
REIT - Hotel & Motel
REIT - Industrial
REIT - Mortgage
REIT - Office
REIT - Residential
REIT - Retail
REIT - Specialty
Railroads
Real Estate - Development
Real Estate - Services
Regulated Electric
Regulated Gas
Regulated Water
Renewable Utilities
Rental & Leasing Services
Residential Construction
Restaurants
Security & Protection Services
Semiconductors
Silver
Software - Application
Software - Infrastructure
Software - Services
Solar
Specialty Business Services
Specialty Retail
Staffing & Employment Services
Steel
Technology Distributors
Telecommunications Services
Tobacco
Travel Lodging
Travel Services
Trucking
Uranium
Waste Management
## Done

- `21:00:30` ✅ deep shape probe complete — fix causal_graph.py / lambda_function.py from the Log above
