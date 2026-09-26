"""Reviewed measurement identities; these are not independent trading factors."""
SERIES=('DGS10','DGS2','DGS30','DGS5','T10Y2Y','T10Y3M','BAMLH0A0HYM2','BAMLC0A4CBBB','DFII10','T10YIE')
ROOTS={
    'DGS10':('H15:nominal',),'DGS2':('H15:nominal',),'DGS30':('H15:nominal',),'DGS5':('H15:nominal',),
    'T10Y2Y':('H15:nominal',),'T10Y3M':('H15:nominal',),
    'BAMLH0A0HYM2':('ICE:HY-OAS',),'BAMLC0A4CBBB':('ICE:BBB-OAS',),
    'DFII10':('H15:real',),'T10YIE':('H15:nominal','H15:real'),
}
SPECS={sid:{'reviewed_definition':('Percent','D','Daily, Close' if sid.startswith('BAML') else 'Daily','Not Seasonally Adjusted'),
    'dependency_roots':list(ROOTS[sid]),'source_url':'https://fred.stlouisfed.org/series/'+sid} for sid in SERIES}
WINDOW=30
BASELINE=252
ANNUAL_STEPS=252
MAX_GAP_DAYS=7
MAX_OBSERVATION_DAYS=7
MAX_SOURCE_SECONDS=26*3600
MOVE_URL='https://query1.finance.yahoo.com/v8/finance/chart/%5EMOVE?range=2y&interval=1d'
MOVE_NAMES=('ICE BofA MOVE Index','ICE BofA U.S. Bond Market Option Volatility Estimate Index','Merrill Lynch Option Volatility Estimate')
METHOD={
    'dispersion':'Sample standard deviation of 30 successive numeric-observation changes, in basis points per observation step; dates and missing rows remain explicit.',
    'annualization':'Multiply variance by 252, then take its square root. Assumes 252 comparable observation intervals a year; trading-calendar and equal-interval assumptions are not verified.',
    'baseline':'252 preceding rolling-window estimates, excluding the current estimate. Overlapping windows are dependent; this z-score is descriptive, not a Gaussian probability.',
    'percentile':'100 * (strictly lower baseline values + half the equal values) / baseline count. No clipped scores; a flat baseline has undefined z-score.',
    'precision':'Original decimal strings retained. Variances use exact rational centering; decimal calculations use 72 significant digits, ROUND_HALF_EVEN. Square roots are rounded calculations, not exact irrational numbers.',
    'scope':'Every returned original row is retained. Acquired current-vintage history is not a point-in-time backtest or a complete history guarantee.',
    'move':'MOVE is a separate option-implied index. A realized-change statistic can never replace its index level. Public quote identity does not establish parity with the official ICE feed.',
    'authority':'No composite stress vote, crisis label, causal playbook, position size, notification or execution permission is qualified by these descriptive measurements.',
}
METHOD_SOURCES=('https://developer.ice.com/fixed-income-data-services/catalog/ice-data-indices-move-index',
    'https://fred.stlouisfed.org/series/DGS10','https://fred.stlouisfed.org/series/BAMLH0A0HYM2')
