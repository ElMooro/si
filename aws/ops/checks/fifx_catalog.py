"""Reviewed identities and descriptive units, never investment permissions."""
FRED = ('VIXCLS', 'DGS10', 'DEXUSEU', 'DEXJPUS', 'DEXUSUK', 'DTWEXBGS')
QUOTES = ('^MOVE', '^KS11', '^HSI', '^N225', '^GDAXI', '^FTSE', '^FCHI',
          '000001.SS', '^BSESN', '^BVSP', '^AXJO', '^VHSI')
SOURCES = FRED + QUOTES
BASELINE = 504
ANNUAL_STEPS = 252
MAX_GAP_DAYS = 7
MAX_ACQUISITION_SECONDS = 26 * 3600
AUTHORITY = {k: False for k in ('calls_eligible', 'sizing_eligible', 'execution_eligible',
    'forecast_qualified', 'point_in_time_backtest_qualified', 'publication_eligible')}
FRED_UNITS = {'VIXCLS': 'Index', 'DGS10': 'Percent', 'DEXUSEU': 'U.S. Dollars to One Euro',
    'DEXJPUS': 'Japanese Yen to One U.S. Dollar', 'DEXUSUK': 'U.S. Dollars to One British Pound',
    'DTWEXBGS': 'Index Jan 2006=100'}
QUOTE_IDENTITIES = {
    '^MOVE': ('USD', 'NYS', 'America/New_York', ('ICE BofA MOVE Index',
        'ICE BofA U.S. Bond Market Option Volatility Estimate Index', 'Merrill Lynch Option Volatility Estimate')),
    '^KS11': ('KRW', 'KSC', 'Asia/Seoul', ('KOSPI Composite Index',)),
    '^HSI': ('HKD', 'HKG', 'Asia/Hong_Kong', ('HANG SENG INDEX',)),
    '^N225': ('JPY', 'OSA', 'Asia/Tokyo', ('Nikkei 225',)),
    '^GDAXI': ('EUR', 'GER', 'Europe/Berlin', ('DAX P',)),
    '^FTSE': ('GBP', 'FGI', 'Europe/London', ('FTSE 100',)),
    '^FCHI': ('EUR', 'PAR', 'Europe/Paris', ('CAC 40',)),
    '000001.SS': ('CNY', 'SHH', 'Asia/Shanghai', ('SSE Composite Index',)),
    '^BSESN': ('INR', 'BSE', 'Asia/Kolkata', ('S&P BSE SENSEX',)),
    '^BVSP': ('BRL', 'SAO', 'America/Sao_Paulo', ('IBOVESPA',)),
    '^AXJO': ('AUD', 'ASX', 'Australia/Sydney', ('S&P/ASX 200 [XJO]',)),
    # No successful original identity was obtained; do not guess an alias.
    '^VHSI': ('HKD', 'HKG', 'Asia/Hong_Kong', ()),
}
SPECS = {}
for sid in SOURCES:
    level = sid in ('VIXCLS', '^MOVE', '^VHSI')
    fx = sid in ('DEXUSEU', 'DEXJPUS', 'DEXUSUK', 'DTWEXBGS')
    SPECS[sid] = {
        'provider': 'fred_csv' if sid in FRED else 'yahoo_chart',
        'measurement': 'reported_index_level' if level else 'yield_change_dispersion' if sid == 'DGS10' else 'log_return_dispersion',
        'source_unit': FRED_UNITS.get(sid, 'index_points'),
        'measurement_unit': 'index_points' if level else 'basis_points' if sid == 'DGS10' else 'percent_log_return',
        'window_changes': 0 if level else 30 if sid == 'DGS10' else 20,
        'max_observation_age_days': 12 if fx else 7,
        'release_cadence': 'H.10: previous business week, Monday 16:15 ET or next federal business day' if fx else 'provider daily observations',
        'dependency_roots': ['H10:USD'] if fx else ['H15:nominal'] if sid == 'DGS10' else ['CBOE:VIX'] if sid == 'VIXCLS' else ['quote:' + sid],
        'return_basis': 'provider-reported index close; return variant and official-feed parity unverified' if sid in QUOTES else None,
        'source_url': 'https://fred.stlouisfed.org/series/' + sid if sid in FRED else None,
    }
METHOD = {
    'precision': 'Original numeric lexemes retained. Log changes use 72-digit Decimal ln; sample variances use exact rational centering of those rounded changes. No display rounding enters calculations.',
    'dispersion': '30 successive numeric-observation yield changes in basis points, or 20 log price/FX changes in percent. Nonpositive log inputs invalidate windows rather than being skipped.',
    'annualization': 'Square root of 252 times sample variance. This assumes comparable observation intervals, not a verified trading calendar or portfolio covariance.',
    'baseline': '504 preceding valid estimates, excluding current. Missing or invalid estimates inside the window prevent ranking; overlapping rolling windows are dependent.',
    'percentile': 'Midrank: 100 * (lower + half equal) / 504. Flat baselines have null z-scores. No clipping or normal-distribution probability claim.',
    'dates': 'All source rows retained, including missing and future rows. Source acquisition fixes the eligible cutoff. Quoted current-day sessions remain provisional. No date backfill across markets.',
    'vintage': 'Full returned current-vintage histories, not historical information sets. Revisions and look-ahead prevent point-in-time forecast claims.',
    'scope': 'Separate measurement units and instruments. No MOVE fallback, averaged FX volatility, migration state, canary call or cheap-hedge inference.',
    'quality': 'Observation lag and acquisition age are separate. Age ceilings are review policies, not proof that the latest scheduled release arrived.',
}
