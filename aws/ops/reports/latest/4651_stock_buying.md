# ops 4651 — stock-buying flagship

**Status:** failure  
**Duration:** 296.6s  
**Finished:** 2026-08-13T18:51:51+00:00  

## Error

```
SystemExit: 1
```

## Data

| n_cols |
|---|
| 293 |

## Log
## FMP key donor -> engine env

- `18:46:54` key from fmp-fundamentals-agent.FMP_API_KEY (len=32)
## authority probes

- `18:46:54` S3: data/fundamental-census-history.json (226 B, 2026-08-01 06:48)
- `18:46:54` S3: data/fundamental-census-matrix.json (1180010 B, 2026-08-01 06:48)
- `18:46:54` S3: data/fundamental-census.json (78138 B, 2026-08-01 06:48)
- `18:46:54` S3: data/fundamentals-decisive-call.json (657 B, 2026-08-13 13:20)
- `18:46:54` S3: data/fundamentals.json (13646 B, 2026-08-13 13:00)
- `18:46:54` ⚠ census_idx replica: '{' was never closed (cidx, line 56)
## verbatim matrix truth

- `18:46:54` top keys: ['generated_at', 'n_tickers', 'n_metrics', 'tickers', 'sectors', 'industries', 'quality', 'turn', 'flagged', 'metrics', 'cols']
- `18:46:54`   generated_at -> str len=32
- `18:46:54`   n_tickers -> int len=-
- `18:46:54`   n_metrics -> int len=-
- `18:46:54`   tickers -> list len=498
- `18:46:54`   sectors -> list len=498
- `18:46:54`   industries -> list len=498
- `18:46:54`   quality -> list len=498
- `18:46:54`   turn -> list len=498
## column census (293) + concept samples

- `18:46:55` cols: above_ma40w | accountsPayable | acquisitions | altman_z | altman_z_prime | aoci | asset_turnover | asset_turnover_ttm | assets_per_employee | beneish_m
- `18:46:55` cols: beta_2y | book_value_per_share | book_value_ps | breakout_20w | buyback_yield_gross_pct | buyback_yield_pct | bvps_yoy_pct | capLeaseObligations | capex | capex_to_da
- `18:46:55` cols: capex_to_revenue_pct | capex_yoy_pct | cash | cashSTI | cash_conversion_pct | cash_ps | cash_ratio | cash_to_debt | ccc_days | cff
- `18:46:55` cols: cfi | cfo | cfo_ps_ttm | cfo_to_debt_pct | cfo_ttm | cfo_yoy_pct | cogs_to_revenue_pct | combo_score | commonStockPar | conviction_score
- `18:46:55` cols: costAndExpenses | costOfRevenue | current_ratio | dInventory | dPayables | dReceivables | dWorkingCapital | da | days_inventory | days_payable
- `18:46:55` cols: debtRepayment | debt_paydown_yield_pct | debt_per_employee | debt_to_assets | debt_to_assets_pct | debt_to_capital | debt_to_equity | debt_to_revenue | deferredIncomeTaxCF | deferredRevenue
- `18:46:55` cols: deferredRevenueNC | deferredTaxAssets | deferredTaxLiabNC | dio_days | dist_52w_high_pct | dist_52w_low_pct | dividend_yield_pct | dividendsPaid | double_bottom | double_top
- `18:46:55` cols: downside_pct | dpo_days | dps_ttm | dps_yoy_pct | dso_days | earnings_in_days | earnings_yield_ebit_pct | earnings_yield_pct | ebit_ttm | ebitda
- `18:46:55` cols: ebitda_interest_coverage | ebitda_margin_pct | ebitda_per_employee | ebitda_ttm | ebitda_yoy_pct | effective_interest_rate_pct | effective_tax_rate_pct | employees | eps | epsDiluted
- `18:46:55` cols: eps_cagr_3y_pct | eps_cagr_5y_pct | eps_ttm | eps_yoy_pct | equity | equity_multiplier | equity_to_assets | equity_to_assets_pct | ev | ev_ebit_fwd
- `18:46:55` cols: ev_ebit_ttm | ev_ebitda_fwd | ev_ebitda_ttm | ev_fcf_ttm | ev_gp_ttm | ev_revenue_fwd | ev_sales_ttm | factor_growth | factor_momentum | factor_quality
- `18:46:55` cols: factor_safety | factor_value | fcf | fcf_cagr_3y_pct | fcf_cagr_5y_pct | fcf_conversion_pct | fcf_ev_yield_pct | fcf_margin_pct | fcf_per_employee | fcf_per_share
- `18:46:55` cols: fcf_ps_ttm | fcf_to_debt_pct | fcf_to_ni | fcf_ttm | fcf_yield_pct | fcf_yoy_pct | forexCash | fulmer_h | golden_cross_10_40w | goodwill
- `18:46:55` cols: goodwill_to_assets_pct | gp_to_assets_pct | graham_number | grossProfit | gross_debt_to_ebitda | gross_margin_pct | gross_profit_ttm | gross_profit_yoy_pct | gwIntang | implied_fcf_growth_pct
- `18:46:55` cols: implied_vs_actual_gap_pct | in_long_book | in_short_book | income_quality | industry_risk_score | insider_own_pct | insider_trans_pct | inst_buy_usd_m | inst_net_usd_m | inst_sell_usd_m
- `18:46:55` cols: intangibles | intangibles_to_assets_pct | interestExpense | interestIncome | interest_coverage_ttm | inventory | inventory_to_revenue_pct | inventory_turnover | inventory_turnover_ttm | kz_index
- `18:46:55` cols: liab_to_assets | longTermDebt | ltInvestments | lt_debt_to_assets_pct | lt_debt_to_equity | mcap | minorityInterest | mom_12_1_pct | mom_6m_pct | ncav
- `18:46:55` cols: ncav_ps | netChangeInCash | netDebt | netIncome | net_buyback_ttm | net_buyback_yield_pct | net_debt_calc | net_income_per_employee | net_income_ttm | net_income_yoy_pct
- `18:46:55` cols: net_margin_pct | net_shareholder_yield_pct | netdebt_to_ebitda_ttm | netdebt_to_fcf | nonOpIncomeTotal | op_income_per_employee | operatingIncome | operating_income_yoy_pct | operating_margin_pct | opex
- `18:46:55` cols: otherCFF | otherCFI | otherCurrentAssets | otherCurrentLiabilities | otherEquity | otherNonCash | otherNonCurrentAssets | otherNonCurrentLiabilities | otherOpex | otherWC
- `18:46:55` cols: p_cfo_ttm | p_fcf_ttm | payout_ratio_pct | pb | pe_fwd | pe_ttm | peg_ttm | piotroski_f | ppeNet | preferredStock
- `18:46:55` cols: pretaxIncome | pretax_margin_pct | price_to_book | price_to_cfo_ttm | price_to_tangible_book | ps_fwd | ps_ttm | ptb | purchInvest | quick_ratio
- `18:46:55` cols: receivables | retail_accum | retail_dp_pct | retail_dp_score | retainedEarnings | retention_pct | revenue | revenue_cagr_3y_pct | revenue_cagr_5y_pct | revenue_per_employee
- `18:46:55` cols: revenue_ps_ttm | revenue_ttm | revenue_yoy_pct | risk_score | rnd | rnd_per_employee | rnd_to_revenue_pct | roa_pct | roc_greenblatt_pct | roce_pct
- `18:46:55` cols: roe_pct | roic_pct | rota_pct | rote_pct | rr_ratio | rsi_14w | rule_of_40 | saleInvest | sbc | sbc_to_revenue_pct
- `18:46:55` cols: sbc_yoy_pct | sellingMarketing | sga_to_revenue_pct | sgna | share_count_yoy_pct | shareholder_yield_pct | shortTermDebt | short_float_pct | shs | shsDil
- `18:46:55` cols: sloan_accruals_pct | springate | springate_s | sti | stockIssued | stockRepurchased | sustainable_growth_pct | tangible_bv_ps | tangible_bvps | tangible_ce_ratio
- `18:46:55` cols: tangible_common_equity_pct | tangible_equity | taxExpense | taxPayables | tech_score | tobins_q | totalAssets | totalCurrentAssets | totalCurrentLiabilities | totalDebt
- `18:46:55` cols: totalInvestments | totalLiabilities | totalNonCurrentAssets | totalNonCurrentLiabilities | total_yield_pct | upside_pct | vol_52w_pct | wc_to_revenue_pct | whale_buy_usd_m | whale_net_usd_m
- `18:46:55` cols: whale_sell_usd_m | working_capital | zmijewski_x
- `18:46:55` [peg] hits=['peg_ttm'] · peg_ttm=1.0
- `18:46:55` [issuance] hits=['buyback_yield_gross_pct', 'buyback_yield_pct', 'net_buyback_ttm', 'net_buyback_yield_pct', 'stockIssued', 'stockRepurchased'] · buyback_yield_gross_pct=1.964 ; buyback_yield_pct=1.964 ; net_buyback_ttm=82226000000.0
- `18:46:55` [basicshares] hits=['book_value_per_share', 'fcf_per_share', 'net_shareholder_yield_pct', 'share_count_yoy_pct', 'shareholder_yield_pct'] · book_value_per_share=7.289 ; fcf_per_share=9.727 ; net_shareholder_yield_pct=2.338
- `18:46:55` [qeps] hits=[] · 
- `18:46:55` [qrev] hits=[] · 
- `18:46:55` [roic] hits=['roic_pct'] · roic_pct=98.946
- `18:46:55` US10Y: found=True last=4.7 age=2d src-rows
## deploy (create-capable) + schedule

- `18:51:51` ✗   [deploy] CONTRACT MISS — v1.0.5 live (created=False)
