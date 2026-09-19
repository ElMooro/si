"""Requested Risk Gate identities, preserved independently of provider availability."""
SERIES={
 'RRPONTSYD':('funding','Overnight reverse repurchase agreements'),
 'WRESBAL':('funding','Reserve balances'), 'TOTRESNS':('funding','Total reserves'),
 'BOGMBBM':('funding','Monetary base'), 'CASACBW027SBOG':('funding','Commercial-bank cash assets'),
 'SOFR':('funding','Secured Overnight Financing Rate'), 'IORB':('funding','Interest on reserve balances'),
 'BAMLH0A3HYC':('credit','CCC and lower option-adjusted spread'),
 'BAMLC0A4CBBB':('credit','BBB option-adjusted spread'),
 'BAMLH0A0HYM2':('credit','High-yield option-adjusted spread'),
 'BAMLHE00EHYIOAS':('credit','Euro high-yield option-adjusted spread'),
 'DTWEXBGS':('dollar','Broad trade-weighted US dollar index'), 'DGS10':('dollar','10-year constant-maturity Treasury yield'),
 'DEXJPUS':('carry','Japanese yen per US dollar'), 'INDPRO':('growth','Industrial production'),
 'VIXCLS':('structure','CBOE volatility index'), 'DCPN3M':('funding','Nonfinancial commercial paper, three months'),
 'DFF':('funding','Effective federal funds rate'), 'RIFSPPNA2P2D90NB':('funding','Requested A2/P2 nonfinancial commercial paper identity'),
 'BAMLC0A0CM':('credit','Investment-grade option-adjusted spread'), 'VXVCLS':('structure','CBOE three-month volatility index'),
 'DGS2':('dollar','Two-year constant-maturity Treasury yield'), 'UNRATE':('growth','Unemployment rate'),
 'TRUCKD11':('growth','Truck tonnage index'), 'SP500':('structure','S&P 500 price index')}


def extend_catalog(catalog):
    # Requested labels never override official metadata or existing desk taxonomy.
    requested={sid:{'category':'risk_gate_'+cat,'display_name':name} for sid,(cat,name) in SERIES.items()}
    return {**requested,**catalog}
