/* FG_CATALOG_OPS3476 + OPS3482 + FG_SHARED_OPS3492 (macro registry + wk2d) — single source of truth for the Fundamental Graphs
   metric catalog. Consumers: /fundamental-graphs.html + why.html module.
   Keys MUST mirror engine justhodl-fundamental-graphs output. */
window.FG_CAT=[
/* Income statement */
['revenue','Total revenue','IS','$',1],['costOfRevenue','Cost of revenue','IS','$',1],
['grossProfit','Gross profit','IS','$',1],['opex','Operating expenses','IS','$',1],
['rnd','Research & development','IS','$',1],['sgna','SG&A expense','IS','$',1],
['operatingIncome','Operating income (EBIT)','IS','$',1],['ebitda','EBITDA','IS','$',1],
['da','Depreciation & amortization','IS','$',1],['interestExpense','Interest expense','IS','$',1],
['interestIncome','Interest income','IS','$',1],['pretaxIncome','Pretax income','IS','$',1],
['taxExpense','Income tax expense','IS','$',1],['netIncome','Net income','IS','$',1],
['eps','EPS (basic)','IS','c',1],['epsDiluted','EPS (diluted)','IS','c',1],
['shs','Shares outstanding (basic wtd)','IS','s',0],['shsDil','Shares outstanding (diluted wtd)','IS','s',0],
['revenue_ttm','Revenue (TTM)','IS','$',0],['gross_profit_ttm','Gross profit (TTM)','IS','$',0],
['ebitda_ttm','EBITDA (TTM)','IS','$',0],['ebit_ttm','EBIT (TTM)','IS','$',0],
['net_income_ttm','Net income (TTM)','IS','$',0],
/* Balance sheet */
['cash','Cash & equivalents','BS','$',0],['sti','Short-term investments','BS','$',0],
['cashSTI','Cash & ST investments','BS','$',0],['receivables','Net receivables','BS','$',0],
['inventory','Inventory','BS','$',0],['totalCurrentAssets','Total current assets','BS','$',0],
['ppeNet','PP&E (net)','BS','$',0],['goodwill','Goodwill','BS','$',0],
['intangibles','Other intangibles','BS','$',0],['gwIntang','Goodwill + intangibles','BS','$',0],
['ltInvestments','Long-term investments','BS','$',0],['totalAssets','Total assets','BS','$',0],
['accountsPayable','Accounts payable','BS','$',0],['shortTermDebt','Short-term debt','BS','$',0],
['deferredRevenue','Deferred revenue','BS','$',0],['totalCurrentLiabilities','Total current liabilities','BS','$',0],
['longTermDebt','Long-term debt','BS','$',0],['totalLiabilities','Total liabilities','BS','$',0],
['retainedEarnings','Retained earnings','BS','$',0],['equity','Shareholders\u2019 equity','BS','$',0],
['totalDebt','Total debt','BS','$',0],['netDebt','Net debt','BS','$',0],
['net_debt_calc','Net debt (calc)','BS','$',0],['working_capital','Working capital','BS','$',0],
['tangible_equity','Tangible equity','BS','$',0],['minorityInterest','Minority interest','BS','$',0],
/* Cash flow */
['cfo','Cash from operations','CF','$',1],['sbc','Stock-based compensation','CF','$',1],
['dWorkingCapital','Δ working capital','CF','$',1],['capex','Capital expenditures','CF','$',1],
['acquisitions','Acquisitions (net)','CF','$',1],['purchInvest','Purchases of investments','CF','$',1],
['saleInvest','Sale/maturity of investments','CF','$',1],['cfi','Cash from investing','CF','$',1],
['debtRepayment','Debt issuance/repayment (net)','CF','$',1],['stockIssued','Stock issued','CF','$',1],
['stockRepurchased','Stock repurchased','CF','$',1],['dividendsPaid','Dividends paid','CF','$',1],
['cff','Cash from financing','CF','$',1],['fcf','Free cash flow','CF','$',1],
['cfo_ttm','CFO (TTM)','CF','$',0],['fcf_ttm','Free cash flow (TTM)','CF','$',0],
/* Statistics */
['mcap','Market capitalization','ST','$',0],['ev','Enterprise value','ST','$',0],
['pe_ttm','P/E (TTM)','ST','x',0],['ps_ttm','P/S (TTM)','ST','x',0],
['pb','Price / book','ST','x',0],['ptb','Price / tangible book','ST','x',0],
['p_fcf_ttm','Price / FCF (TTM)','ST','x',0],['p_cfo_ttm','Price / CFO (TTM)','ST','x',0],
['ev_ebitda_ttm','EV / EBITDA (TTM)','ST','x',0],['ev_ebit_ttm','EV / EBIT (TTM)','ST','x',0],
['ev_sales_ttm','EV / Sales (TTM)','ST','x',0],['ev_fcf_ttm','EV / FCF (TTM)','ST','x',0],
['earnings_yield_pct','Earnings yield %','ST','%',0],['fcf_yield_pct','FCF yield %','ST','%',0],
['dividend_yield_pct','Dividend yield %','ST','%',0],['buyback_yield_pct','Buyback yield %','ST','%',0],
['shareholder_yield_pct','Shareholder yield %','ST','%',0],['payout_ratio_pct','Dividend payout ratio %','ST','%',0],
['gross_margin_pct','Gross margin %','ST','%',0],['operating_margin_pct','Operating margin %','ST','%',0],
['ebitda_margin_pct','EBITDA margin %','ST','%',0],['net_margin_pct','Net margin %','ST','%',0],
['fcf_margin_pct','FCF margin %','ST','%',0],
['roe_pct','Return on equity %','ST','%',0],['roa_pct','Return on assets %','ST','%',0],
['roic_pct','Return on invested capital %','ST','%',0],['rota_pct','Return on tangible assets %','ST','%',0],
['debt_to_equity','Debt / equity','ST','x',0],['debt_to_assets','Debt / assets','ST','x',0],
['equity_to_assets','Equity / assets','ST','x',0],['liab_to_assets','Liabilities / assets','ST','x',0],
['netdebt_to_ebitda_ttm','Net debt / EBITDA (TTM)','ST','x',0],['interest_coverage_ttm','Interest coverage (TTM)','ST','x',0],
['current_ratio','Current ratio','ST','x',0],['quick_ratio','Quick ratio','ST','x',0],
['cash_ratio','Cash ratio','ST','x',0],['asset_turnover_ttm','Asset turnover (TTM)','ST','x',0],
['inventory_turnover_ttm','Inventory turnover (TTM)','ST','x',0],
['dso_days','Days sales outstanding','ST','d',0],['dio_days','Days inventory','ST','d',0],
['dpo_days','Days payable','ST','d',0],['ccc_days','Cash conversion cycle','ST','d',0],
['income_quality','Income quality (CFO/NI)','ST','x',0],
['sbc_to_revenue_pct','SBC / revenue %','ST','%',0],['capex_to_revenue_pct','Capex / revenue %','ST','%',0],
['rnd_to_revenue_pct','R&D / revenue %','ST','%',0],['sga_to_revenue_pct','SG&A / revenue %','ST','%',0],
['effective_tax_rate_pct','Effective tax rate %','ST','%',0],['sloan_accruals_pct','Sloan accruals %','ST','%',0],
['share_count_yoy_pct','Share count YoY % (dilution)','ST','%',0],
['altman_z','Altman Z-score','ST','n',0],['piotroski_f','Piotroski F-score','ST','n',0],
['beneish_m','Beneish M-score','ST','n',0],['implied_fcf_growth_pct','Market-implied FCF growth % (rev-DCF, r=9%)','ST','%',0],
['implied_vs_actual_gap_pct','Expectations gap % (implied \u2212 3y FCF CAGR)','ST','%',0],
['graham_number','Graham number','ST','c',0],
/* Per share */
['eps_ttm','EPS (TTM)','PS','c',0],['fcf_ps_ttm','FCF per share (TTM)','PS','c',0],
['cfo_ps_ttm','CFO per share (TTM)','PS','c',0],['revenue_ps_ttm','Revenue per share (TTM)','PS','c',0],
['book_value_ps','Book value per share','PS','c',0],['tangible_bv_ps','Tangible book per share','PS','c',0],
['dps_ttm','Dividends per share (TTM)','PS','c',0],['cash_ps','Cash per share','PS','c',0],
/* Forecasts */
['est_revenue_avg','Revenue estimate (avg)','FC','$',1],['est_revenue_low','Revenue estimate (low)','FC','$',1],
['est_revenue_high','Revenue estimate (high)','FC','$',1],['est_eps_avg','EPS estimate (avg)','FC','c',1],
['est_eps_low','EPS estimate (low)','FC','c',1],['est_eps_high','EPS estimate (high)','FC','c',1],
['est_ebitda_avg','EBITDA estimate (avg)','FC','$',1],['est_ebit_avg','EBIT estimate (avg)','FC','$',1],
['est_net_income_avg','Net income estimate (avg)','FC','$',1],['est_sga_avg','SG&A estimate (avg)','FC','$',1],
['est_num_analysts','# analysts (revenue)','FC','n',0],
/* Growth */
['revenue_yoy_pct','Revenue growth YoY %','GR','%',0],['gross_profit_yoy_pct','Gross profit growth YoY %','GR','%',0],
['operating_income_yoy_pct','Operating income growth YoY %','GR','%',0],['ebitda_yoy_pct','EBITDA growth YoY %','GR','%',0],
['net_income_yoy_pct','Net income growth YoY %','GR','%',0],['eps_yoy_pct','EPS growth YoY %','GR','%',0],
['cfo_yoy_pct','CFO growth YoY %','GR','%',0],['fcf_yoy_pct','FCF growth YoY %','GR','%',0],
['dps_yoy_pct','Dividend/share growth YoY %','GR','%',0],['bvps_yoy_pct','Book value/share growth YoY %','GR','%',0],
['capex_yoy_pct','Capex growth YoY %','GR','%',0],['sbc_yoy_pct','SBC growth YoY %','GR','%',0],
['revenue_cagr_3y_pct','Revenue CAGR 3Y %','GR','%',0],['revenue_cagr_5y_pct','Revenue CAGR 5Y %','GR','%',0],
['eps_cagr_3y_pct','EPS CAGR 3Y %','GR','%',0],['eps_cagr_5y_pct','EPS CAGR 5Y %','GR','%',0],
['fcf_cagr_3y_pct','FCF CAGR 3Y %','GR','%',0],['fcf_cagr_5y_pct','FCF CAGR 5Y %','GR','%',0],
['sustainable_growth_pct','Sustainable growth rate %','GR','%',0],['retention_pct','Earnings retention %','GR','%',0],
['rule_of_40','Rule of 40 (rev YoY + FCF mgn)','GR','n',0],
/* HF quality / capital return */
['gp_to_assets_pct','Gross profits / assets % (Novy-Marx)','ST','%',0],
['earnings_yield_ebit_pct','EBIT / EV yield % (Greenblatt)','ST','%',0],
['roc_greenblatt_pct','Return on capital % (Greenblatt)','ST','%',0],
['ev_gp_ttm','EV / gross profit (TTM)','ST','x',0],['fcf_ev_yield_pct','FCF / EV yield %','ST','%',0],
['fcf_conversion_pct','FCF conversion % (FCF/EBITDA)','ST','%',0],
['cash_conversion_pct','Cash conversion % (CFO/EBITDA)','ST','%',0],
['fcf_to_ni','FCF / net income','ST','x',0],['capex_to_da','Capex / D&A','ST','x',0],
['net_buyback_yield_pct','Net buyback yield % (net of issuance)','ST','%',0],
['net_shareholder_yield_pct','Net shareholder yield %','ST','%',0],
['debt_paydown_yield_pct','Debt paydown yield %','ST','%',0],
['total_yield_pct','Total yield % (div+netBB+debt)','ST','%',0],
['peg_ttm','PEG (trailing)','ST','x',0],['tobins_q','Tobin\u2019s Q (approx)','ST','x',0],
['pretax_margin_pct','Pretax margin %','ST','%',0],
['cogs_to_revenue_pct','COGS / revenue %','ST','%',0],['wc_to_revenue_pct','Working capital / revenue %','ST','%',0],
['debt_to_revenue','Debt / revenue','ST','x',0],['cash_to_debt','Cash / debt','ST','x',0],
['goodwill_to_assets_pct','Goodwill / assets %','ST','%',0],['intangibles_to_assets_pct','GW+intangibles / assets %','ST','%',0],
['tangible_ce_ratio','Tangible common equity ratio','ST','x',0],['equity_multiplier','Equity multiplier (assets/equity)','ST','x',0],
['debt_to_capital','Debt / capital','ST','x',0],['gross_debt_to_ebitda','Gross debt / EBITDA (TTM)','ST','x',0],
['ebitda_interest_coverage','EBITDA / interest (TTM)','ST','x',0],
['fcf_to_debt_pct','FCF / total debt %','ST','%',0],['cfo_to_debt_pct','CFO / total debt %','ST','%',0],
['netdebt_to_fcf','Net debt / FCF (yrs)','ST','x',0],
['altman_z_prime','Altman Z\u2033 (non-mfg)','ST','n',0],['springate','Springate score','ST','n',0],
['zmijewski_x','Zmijewski X-score','ST','n',0],['fulmer_h','Fulmer H factor','ST','n',0],
['kz_index','KZ index (fin. constraint)','ST','n',0],
['employees','Number of employees','ST','s',0],
['revenue_per_employee','Revenue per employee (TTM)','ST','$',0],
['net_income_per_employee','Net income per employee (TTM)','ST','$',0],
['net_buyback_ttm','Net buybacks (TTM, net of issuance)','CF','$',0],
['ncav','Net current asset value (NCAV)','BS','$',0],
['ncav_ps','NCAV per share','PS','c',0],
];
window.FG_TABS=[['FAV','\u2605 Favorites'],['IN','Institutional'],['IS','Income statement'],['BS','Balance sheet'],['CF','Cash flow'],['GR','Growth'],['ST','Statistics'],['PS','Per share'],['FC','Forecasts']];
window.FG_INST=['implied_fcf_growth_pct','roic_pct','fcf_yield_pct','earnings_yield_ebit_pct','roc_greenblatt_pct','ev_ebitda_ttm','ev_gp_ttm','ev_sales_ttm','pe_ttm','p_fcf_ttm','peg_ttm','gp_to_assets_pct','gross_margin_pct','operating_margin_pct','fcf_margin_pct','rule_of_40','revenue_yoy_pct','revenue_cagr_3y_pct','eps_yoy_pct','fcf_yoy_pct','fcf_conversion_pct','income_quality','sloan_accruals_pct','sbc_to_revenue_pct','share_count_yoy_pct','net_buyback_yield_pct','dividend_yield_pct','net_shareholder_yield_pct','total_yield_pct','netdebt_to_ebitda_ttm','gross_debt_to_ebitda','ebitda_interest_coverage','fcf_to_debt_pct','debt_to_capital','current_ratio','capex_to_da','tobins_q','roe_pct','ccc_days','dso_days','altman_z','piotroski_f','beneish_m','kz_index','sustainable_growth_pct'];

/* ── FG_SHARED_OPS3492: macro bridge config (single source for flagship + why module) ── */
window.FG_WLAPI='https://nu4umjskc25osscrbmqh3o2gte0utlkx.lambda-url.us-east-1.on.aws';
window.FG_wk2d=function(w){var q=String(w).split('-'),y=+q[0],n=+q[1];if(!n||String(w).length>7)return String(w).slice(0,10);var d=new Date(Date.UTC(y,0,4));var dow=(d.getUTCDay()||7);d.setUTCDate(d.getUTCDate()-dow+1+(n-1)*7);return d.toISOString().slice(0,10);};
window.FG_MACROS=[
 {k:'US10Y',label:'US 10Y yield',u:'%',ids:['TVC:US10Y','FRED:DGS10','DGS10']},
 {k:'US02Y',label:'US 2Y yield',u:'%',ids:['TVC:US02Y','FRED:DGS2','DGS2']},
 {k:'T10Y2Y',label:'2s10s spread (10Y\u22122Y)',u:'%',derived:['US10Y','US02Y']},
 {k:'HYOAS',label:'HY OAS',u:'%',ids:['FRED:BAMLH0A0HYM2','BAMLH0A0HYM2']},
 {k:'FEDFUNDS',label:'Fed Funds',u:'%',ids:['FRED:FEDFUNDS','FEDFUNDS','FRED:DFF']},
 {k:'UNRATE',label:'Unemployment',u:'%',ids:['FRED:UNRATE','UNRATE']},
 {k:'DXY',label:'US Dollar (DXY)',u:'x',ids:['TVC:DXY','CAPITALCOM:DXY','DXY','FRED:DTWEXBGS']}];

/* FG_RADAR_OPS3508: tiny dependency-free radar. axes=[{label,pct,val}] */
window.FG_RADAR=function(axes,size){
  var S=size||190,cx=S/2,cy=S/2,R=S/2-34,n=axes.length,NS='http://www.w3.org/2000/svg';
  var pt=function(i,r){var a=-Math.PI/2+i*2*Math.PI/n;return [cx+r*Math.cos(a),cy+r*Math.sin(a)];};
  var h='<svg width="'+S+'" height="'+S+'" viewBox="0 0 '+S+' '+S+'">';
  [0.33,0.66,1].forEach(function(f){
    h+='<polygon points="'+axes.map(function(_,i){return pt(i,R*f).join(',');}).join(' ')+'" fill="none" stroke="#1e293b" stroke-width="1"/>';});
  axes.forEach(function(_,i){var p=pt(i,R);h+='<line x1="'+cx+'" y1="'+cy+'" x2="'+p[0]+'" y2="'+p[1]+'" stroke="#1e293b" stroke-width="1"/>';});
  h+='<polygon points="'+axes.map(function(a,i){return pt(i,R*Math.max(0.02,(a.pct||0)/100)).join(',');}).join(' ')+'" fill="#22d3ee2e" stroke="#22d3ee" stroke-width="1.6"><title>'+axes.map(function(a){return a.label+' p'+a.pct;}).join(' \u00b7 ')+'</title></polygon>';
  axes.forEach(function(a,i){var p=pt(i,R+16);
    h+='<text x="'+p[0]+'" y="'+p[1]+'" fill="#8b98ad" font-size="8.5" text-anchor="middle" dominant-baseline="middle">'+a.label+' <tspan fill="#22d3ee">p'+Math.round(a.pct)+'</tspan></text>';});
  return h+'</svg>';};
// RAW STATEMENTS (ops 3561): TV-parity pass-through line items
window.FG_CAT.push(["otherOpex","Other operating expenses","IS","$",0]);
window.FG_CAT.push(["costAndExpenses","Total costs & expenses","IS","$",0]);
window.FG_CAT.push(["sellingMarketing","Selling & marketing exp","IS","$",0]);
window.FG_CAT.push(["nonOpIncomeTotal","Non-operating income (total)","IS","$",0]);
window.FG_CAT.push(["otherCurrentAssets","Other current assets","BS","$",0]);
window.FG_CAT.push(["otherNonCurrentAssets","Other non-current assets","BS","$",0]);
window.FG_CAT.push(["totalNonCurrentAssets","Total non-current assets","BS","$",0]);
window.FG_CAT.push(["deferredTaxAssets","Deferred tax assets","BS","$",0]);
window.FG_CAT.push(["taxPayables","Income tax payable","BS","$",0]);
window.FG_CAT.push(["otherCurrentLiabilities","Other current liabilities","BS","$",0]);
window.FG_CAT.push(["otherNonCurrentLiabilities","Other non-current liabilities","BS","$",0]);
window.FG_CAT.push(["totalNonCurrentLiabilities","Total non-current liabilities","BS","$",0]);
window.FG_CAT.push(["deferredRevenueNC","Deferred income (non-current)","BS","$",0]);
window.FG_CAT.push(["deferredTaxLiabNC","Deferred tax liabilities","BS","$",0]);
window.FG_CAT.push(["commonStockPar","Common stock par/carrying","BS","$",0]);
window.FG_CAT.push(["aoci","Accum. other comprehensive income","BS","$",0]);
window.FG_CAT.push(["otherEquity","Other equity","BS","$",0]);
window.FG_CAT.push(["preferredStock","Preferred stock (carrying)","BS","$",0]);
window.FG_CAT.push(["totalInvestments","Total investments","BS","$",0]);
window.FG_CAT.push(["capLeaseObligations","Capitalized lease obligations","BS","$",0]);
window.FG_CAT.push(["deferredIncomeTaxCF","Deferred taxes (cash flow)","CF","$",0]);
window.FG_CAT.push(["dReceivables","Δ accounts receivable","CF","$",0]);
window.FG_CAT.push(["dInventory","Δ inventories","CF","$",0]);
window.FG_CAT.push(["dPayables","Δ accounts payable","CF","$",0]);
window.FG_CAT.push(["otherWC","Δ other working capital","CF","$",0]);
window.FG_CAT.push(["otherNonCash","Other non-cash items","CF","$",0]);
window.FG_CAT.push(["otherCFI","Other investing cash flow","CF","$",0]);
window.FG_CAT.push(["otherCFF","Other financing cash flow","CF","$",0]);
window.FG_CAT.push(["netChangeInCash","Net change in cash","CF","$",0]);
window.FG_CAT.push(["forexCash","FX effect on cash","CF","$",0]);
// TV STATS (ops 3563)
window.FG_CAT.push(["price_to_book","Price / book","ST","x",0]);
window.FG_CAT.push(["price_to_cfo_ttm","Price / cash flow (TTM)","ST","x",0]);
window.FG_CAT.push(["price_to_tangible_book","Price / tangible book","ST","x",0]);
window.FG_CAT.push(["book_value_per_share","Book value per share","ST","x",0]);
window.FG_CAT.push(["tangible_bvps","Tangible book value / share","ST","x",0]);
window.FG_CAT.push(["fcf_per_share","FCF per share","ST","x",0]);
window.FG_CAT.push(["roce_pct","Return on capital employed %","ST","x",0]);
window.FG_CAT.push(["rote_pct","Return on tangible equity %","ST","x",0]);
window.FG_CAT.push(["debt_to_assets_pct","Debt / assets %","ST","x",0]);
window.FG_CAT.push(["lt_debt_to_assets_pct","LT debt / assets %","ST","x",0]);
window.FG_CAT.push(["lt_debt_to_equity","LT debt / equity","ST","x",0]);
window.FG_CAT.push(["effective_interest_rate_pct","Effective interest rate %","ST","x",0]);
window.FG_CAT.push(["equity_to_assets_pct","Equity / assets %","ST","x",0]);
window.FG_CAT.push(["inventory_to_revenue_pct","Inventory / revenue %","ST","x",0]);
window.FG_CAT.push(["days_inventory","Days inventory","ST","x",0]);
window.FG_CAT.push(["days_payable","Days payable","ST","x",0]);
window.FG_CAT.push(["buyback_yield_gross_pct","Buyback yield (gross) %","ST","x",0]);
window.FG_CAT.push(["tangible_common_equity_pct","Tangible common equity %","ST","x",0]);
window.FG_CAT.push(["fcf_per_employee","FCF per employee","ST","x",0]);
window.FG_CAT.push(["ebitda_per_employee","EBITDA per employee","ST","x",0]);
window.FG_CAT.push(["op_income_per_employee","Operating income per employee","ST","x",0]);
window.FG_CAT.push(["debt_per_employee","Total debt per employee","ST","x",0]);
window.FG_CAT.push(["assets_per_employee","Total assets per employee","ST","x",0]);
window.FG_CAT.push(["rnd_per_employee","R&D per employee","ST","x",0]);
window.FG_CAT.push(["springate_s","Springate S-score","ST","x",0]);
window.FG_CAT.push(["pe_fwd","P/E forward","ST","x",0]);
window.FG_CAT.push(["ps_fwd","P/S forward","ST","x",0]);
window.FG_CAT.push(["ev_ebitda_fwd","EV/EBITDA forward","ST","x",0]);
window.FG_CAT.push(["ev_ebit_fwd","EV/EBIT forward","ST","x",0]);
window.FG_CAT.push(["ev_revenue_fwd","EV/Revenue forward","ST","x",0]);
