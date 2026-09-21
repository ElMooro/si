"""Source identities, native periods and explicit overlap; no independent-vote claim."""
SPECS={
 'WEI':('Weekly Economic Index (Lewis-Mertens-Stock)','Index','W','NSA'),
 'ICSA':('Initial Claims','Number','W','SA'),
 'CCSA':('Continued Claims (Insured Unemployment)','Number','W','SA'),
 'NFCI':('Chicago Fed National Financial Conditions Index','Index','W','NSA'),
 'STLFSI4':('St. Louis Fed Financial Stress Index','Index','W','NSA'),
 'BAA10Y':("Moody's Seasoned Baa Corporate Bond Yield Relative to Yield on 10-Year Treasury Constant Maturity",'Percent','D','NSA'),
 'GDPNOW':('GDPNow','Percent Change at Annual Rate','Q','SAAR'),
 'T10Y3M':('10-Year Treasury Constant Maturity Minus 3-Month Treasury Constant Maturity','Percent','D','NSA'),
}
SERIES=tuple(SPECS);CORE=SERIES[:6]
WEEKDAY={'WEI':5,'ICSA':5,'CCSA':5,'NFCI':4,'STLFSI4':4}
NOTES={
 'WEI':'A source-produced composite of ten indicators, scaled to four-quarter GDP growth. It includes initial and continuing claims. It is not an official Federal Reserve forecast or a separate independent vote alongside those claims.',
 'ICSA':'New claims for unemployment insurance, seasonally adjusted; reference week ends Saturday. Initial claims are already represented in WEI.',
 'CCSA':'Insured unemployment claims for continued benefits, seasonally adjusted; reference week ends Saturday. Continued claims are already represented in WEI and arrive for a different latest reference week.',
 'NFCI':'Source-standardized financial conditions: positive means tighter than average and negative means looser. It measures conditions, not an independently identified GDP contribution.',
 'STLFSI4':'Source-standardized financial stress from eighteen series. Positive means above-average stress. Rate and spread inputs overlap the financial-conditions evidence family.',
 'BAA10Y':'Baa corporate yield minus ten-year Treasury yield in percentage points, stored by FRED as Percent. This is not an option-adjusted spread or a duration-matched hedge quote.',
 'GDPNOW':'Atlanta Fed model estimate of annualized quarterly real GDP growth. The observation identifies the target quarter, not the forecast publication day. FRED current-quarter values revise; historical quarters contain final model estimates, not a complete forecast-vintage path.',
 'T10Y3M':'Ten-year minus three-month Treasury constant-maturity yield, in percentage points. A yield spread is not the Cleveland Fed recession probability model.',
}
