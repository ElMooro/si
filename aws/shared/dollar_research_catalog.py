"""Reviewed Dollar observations; source identities are not investment votes."""
INDICES={
    'DTWEXBGS':('Fed nominal broad dollar','D'),
    'DTWEXAFEGS':('Fed nominal dollar versus advanced foreign economies','D'),
    'DTWEXEMEGS':('Fed nominal dollar versus emerging market economies','D'),
    'RTWEXBGS':('Fed real broad dollar','M')}
# Currency, original numerator, original denominator, exact provider unit.
FX={
    'DEXUSEU':('EUR','USD','EUR','U.S. Dollars to One Euro'),
    'DEXUSUK':('GBP','USD','GBP','U.S. Dollars to One U.K. Pound Sterling'),
    'DEXJPUS':('JPY','JPY','USD','Japanese Yen to One U.S. Dollar'),
    'DEXCHUS':('CNY','CNY','USD','Chinese Yuan Renminbi to One U.S. Dollar'),
    'DEXCAUS':('CAD','CAD','USD','Canadian Dollars to One U.S. Dollar'),
    'DEXMXUS':('MXN','MXN','USD','Mexican Pesos to One U.S. Dollar'),
    'DEXKOUS':('KRW','KRW','USD','South Korean Won to One U.S. Dollar'),
    'DEXSZUS':('CHF','CHF','USD','Swiss Francs to One U.S. Dollar'),
    'DEXINUS':('INR','INR','USD','Indian Rupees to One U.S. Dollar'),
    'DEXBZUS':('BRL','BRL','USD','Brazilian Reals to One U.S. Dollar'),
    'DEXUSAL':('AUD','USD','AUD','U.S. Dollars to One Australian Dollar'),
    'DEXSIUS':('SGD','SGD','USD','Singapore Dollars to One U.S. Dollar'),
    'DEXTAUS':('TWD','TWD','USD','Taiwan Dollars to One U.S. Dollar'),
    'DEXSDUS':('SEK','SEK','USD','Swedish Kronor to One U.S. Dollar')}
CONTEXT={
    'WALCL':('Federal Reserve total assets, Wednesday stock','Millions of U.S. Dollars','W','fed_h41'),
    'WRESBAL':('Reserve balances, weekly average','Millions of U.S. Dollars','W','fed_h41'),
    'RRPONTSYD':('Overnight reverse-repo outstanding','Billions of US Dollars','D','nyfed_operations'),
    'WTREGEN':('Treasury General Account, weekly average','Millions of U.S. Dollars','W','fed_h41'),
    'DFII10':('10-year Treasury inflation-indexed yield','Percent','D','fed_h15'),
    'DGS10':('10-year Treasury constant-maturity yield','Percent','D','fed_h15'),
    'DGS2':('2-year Treasury constant-maturity yield','Percent','D','fed_h15'),
    'IRLTLT01DEM156N':('German long-term government yield, monthly','Percent','M','oecd_interest_rates'),
    'VIXCLS':('Cboe VIX close','Index','D','cboe_volatility'),
    'BAMLH0A0HYM2':('ICE BofA US high-yield index OAS','Percent','D','ice_credit_indices'),
    'DCOILWTICO':('WTI Cushing crude-oil spot price','Dollars per Barrel','D','eia_spot_prices'),
    'SWPT':('Federal Reserve central-bank liquidity swaps, Wednesday stock','Millions of U.S. Dollars','W','fed_h41'),
    'NFCI':('Chicago Fed National Financial Conditions Index','Index','W','chicago_fed_conditions'),
    'T10YIE':('10-year Treasury breakeven inflation rate','Percent','D','stlouis_fed_yield_derivation')}
SPECS={**{sid:(label,'Index Jan 2006=100',freq,'fed_h10') for sid,(label,freq) in INDICES.items()},
    **{sid:(currency+' / USD original H.10 quote',unit,'D','fed_h10') for sid,(currency,num,den,unit) in FX.items()},**CONTEXT}
SERIES=tuple(SPECS)
CONTEXT_KEYS=('data/eurodollar-stress.json','data/cb-stance.json','data/china-liquidity.json',
    'data/cftc-all-cache.json','data/bond-vol.json','data/repo-market.json','data/indicator-bus.json')
METHOD_SOURCES=(
    'https://www.federalreserve.gov/releases/h10/h10_technical_qa.htm',
    'https://www.federalreserve.gov/releases/H10/Weights/',
    'https://fred.stlouisfed.org/series/IRLTLT01DEM156N')
