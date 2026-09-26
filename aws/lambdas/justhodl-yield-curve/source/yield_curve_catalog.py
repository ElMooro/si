"""Complete predecessor scope and reviewed definitions, not trading factors."""
NOMINAL = ('DGS1MO','DGS3MO','DGS6MO','DGS1','DGS2','DGS3','DGS5','DGS7','DGS10','DGS20','DGS30')
REAL = ('DFII5','DFII7','DFII10','DFII20','DFII30')
BREAKEVEN = ('T5YIE','T10YIE','T5YIFR')
CONTEXT = ('DFF','DFEDTARU','DFEDTARL','SOFR30DAYAVG')
SERIES = NOMINAL + REAL + BREAKEVEN + CONTEXT
TENOR_MONTHS = dict(zip(NOMINAL, (1,3,6,12,24,36,60,84,120,240,360)))
TENOR_MONTHS.update(dict(zip(REAL, (60,84,120,240,360))))
SPECS = {sid: {'group': group, 'tenor_months': TENOR_MONTHS.get(sid),
    'reviewed_definition': ('Percent','D','Daily, 7-Day' if sid in ('DFF','DFEDTARU','DFEDTARL') else 'Daily','Not Seasonally Adjusted')}
    for group, members in (('nominal',NOMINAL),('real',REAL),('inflation_compensation',BREAKEVEN),('policy_and_funding',CONTEXT))
    for sid in members}

# Integer coefficients and one denominator express the exact arithmetic.
# All spread/curvature quantities are basis points; the full nominal mean is percent.
METRICS = {
    '2s10s': {'coefficients': {'DGS10':100,'DGS2':-100}, 'divisor':1, 'unit':'basis_points'},
    '3M10Y': {'coefficients': {'DGS10':100,'DGS3MO':-100}, 'divisor':1, 'unit':'basis_points'},
    '5s30s': {'coefficients': {'DGS30':100,'DGS5':-100}, 'divisor':1, 'unit':'basis_points'},
    '2s5s': {'coefficients': {'DGS5':100,'DGS2':-100}, 'divisor':1, 'unit':'basis_points'},
    '10s30s': {'coefficients': {'DGS30':100,'DGS10':-100}, 'divisor':1, 'unit':'basis_points'},
    'dff_to_10y': {'coefficients': {'DGS10':100,'DFF':-100}, 'divisor':1, 'unit':'basis_points'},
    'butterfly_2_5_10': {'coefficients': {'DGS5':200,'DGS2':-100,'DGS10':-100}, 'divisor':2, 'unit':'basis_points'},
    'curvature_2_5_10': {'coefficients': {'DGS5':200,'DGS2':-100,'DGS10':-100}, 'divisor':1, 'unit':'basis_points'},
    'nominal_mean': {'coefficients': {sid:1 for sid in NOMINAL}, 'divisor':11, 'unit':'percent'},
    'real_5s30s': {'coefficients': {'DFII30':100,'DFII5':-100}, 'divisor':1, 'unit':'basis_points'},
    'nominal_real_breakeven_residual_10y': {'coefficients': {'DGS10':100,'DFII10':-100,'T10YIE':-100}, 'divisor':1, 'unit':'basis_points'},
    'target_band_width': {'coefficients': {'DFEDTARU':100,'DFEDTARL':-100}, 'divisor':1, 'unit':'basis_points'},
}
LAGS = (1,5,20,60)
MAX_OBSERVATION_DAYS = 7
MAX_ACQUISITION_SECONDS = 26*3600
METHOD_SOURCES = [
    'https://home.treasury.gov/policy-issues/financing-the-government/interest-rate-statistics/treasury-yield-curve-methodology',
    'https://fred.stlouisfed.org/series/T10YIE',
    'https://fred.stlouisfed.org/series/T5YIFR',
    'https://www.federalreserve.gov/econres/notes/feds-notes/tips-from-tips-update-and-discussions-20190521.html',
]
DEFINITION_NOTES = {
    'nominal': 'Treasury constant-maturity par yields, not zero-coupon yields or discount factors.',
    'real': 'Inflation-indexed constant-maturity Treasury yields; nominal and real tenors remain separate.',
    'inflation_compensation': 'Market inflation compensation incorporates risk and liquidity premia; it is not a pure inflation forecast.',
    'T5YIFR': 'Original published forward inflation-compensation series; not a recomputed linear approximation.',
    'nominal_mean': 'Equal-weight descriptive average of all eleven declared nominal tenors, not a fitted level factor or PCA.',
    'nominal_real_breakeven_residual_10y': 'Reconciliation residual, not a term premium estimate.',
    'term_premium': 'The complete separate ACM packet is retained as unqualified context; no residual fallback.',
    'comparisons': 'Changes over 1, 5, 20 or 60 matched numeric observations; endpoints and elapsed calendar days disclosed. Not fixed calendar periods.',
}
