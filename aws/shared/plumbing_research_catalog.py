"""All 53 existing Crisis Plumbing requests; no identity substitution or score."""
GROUPS={
 'official_conditions':('STLFSI4','NFCI','ANFCI','KCFSI','OFRFSI','NFCICREDIT','NFCILEVERAGE','NFCIRISK','NFCINONFINLEVERAGE'),
 'credit':('BAMLH0A0HYM2','BAMLH0A3HYC','BAMLH0A2HYB','BAMLH0A1HYBB','BAMLC0A4CBBB','BAMLC0A0CM','BAMLEMCBPIOAS','BAMLEM4BRRBLCRPIOAS'),
 'bank_credit':('DPSACBW027SBOG','BUSLOANS','TOTBKCR','DRTSCILM','DRTSCLCC','DRTSCLM','DPCREDIT'),
 'funding_rates':('SOFR','IORB','OBFR','DGS3MO','DTB3','IR3TIB01JPM156N','ECBESTRVOLWGTTRMDMNRT'),
 'liquidity_stocks':('RRPONTSYD','WTREGEN','WCBSL'),
 'foreign_exchange':('DTWEXBGS','DTWEXAFEGS','DTWEXEMEGS','DEXJPUS','DEXUSEU','DEXCHUS','DEXKOUS','DEXBZUS'),
 'real_activity':('ICSA','CCSA','PERMIT','USSLIND','RECPROUSM156N','SAHMREALTIME'),
 'yield_curves':('DGS10','T10Y2Y','T10Y3M','T10YIE','DFII10'),
}
SERIES=tuple(sid for group in GROUPS.values() for sid in group)
NOTES={
 'DPSACBW027SBOG':'Seasonally adjusted deposit stock, in the publisher\'s native USD billions. A decline does not identify a bank run or destination.',
 'BUSLOANS':'Monthly C&I loan stock. A monthly label is not a weekly observation or a measured flow.',
 'DPCREDIT':'Primary credit interest rate, not the dollar amount of discount-window borrowing.',
 'RRPONTSYD':'Operation amount in USD billions. Its change does not identify prime-to-government money-fund migration.',
 'WTREGEN':'Weekly average in USD millions. Do not combine with USD billions without explicit conversion.',
 'IR3TIB01JPM156N':'Monthly Japanese three-month interbank rate. Do not forward-fill into a current daily funding basis.',
 'ECBESTRVOLWGTTRMDMNRT':'Euro unsecured overnight borrowing benchmark indexed by transaction date; publication occurs later.',
 'RECPROUSM156N':'Publisher smoothed recession estimate, subject to revision. Not a forward recession forecast or JustHodl-calibrated probability.',
 'SAHMREALTIME':'Publisher real-time Sahm indicator in percentage points. Historical labels do not establish portfolio predictive performance.',
 'USSLIND':'Retain discontinued history with its actual final observation. It cannot provide a current leading signal.',
 'OFRFSI':'This requested FRED identity is unavailable. The actual OFR publisher series is separately identified and reconstructed.',
 'WCBSL':'Unresolved requested FRED identity. Do not replace it silently with another balance-sheet series.',
 'DRTSCLM':'Unresolved requested FRED identity. Do not replace it silently with another survey population.',
}


def extend_catalog(catalog):
    result=dict(catalog)
    for sid in SERIES:result.setdefault(sid,{'category':'crisis_plumbing','display_name':sid})
    return result
