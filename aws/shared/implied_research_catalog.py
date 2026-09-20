"""Declared rate/recession/volatility sources; no proxy or probability authority."""
SERIES=('DFEDTARU','DFEDTARL','FEDFUNDS','DFF','DGS1MO','DGS3MO','DGS6MO','DGS1',
        'RECPROUSM156N','T10Y3M','BAMLH0A0HYM2','VIXCLS','VXVCLS','VXNCLS','RVXCLS','VXDCLS','GVZCLS','OVXCLS')

def extend_catalog(catalog):
    result=dict(catalog)
    for sid in SERIES:result.setdefault(sid,{'category':'rates_probability_research','display_name':sid})
    return result
