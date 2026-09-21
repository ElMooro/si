"""Complete existing Dollar Radar source identities in canonical FRED collection.

Definitions and untransformed observations still come from the original provider.
No basket weights, forecast scores, currency inversion or authority are added.
"""
SERIES=('DEXUSAL','DEXSIUS','DEXTAUS','DEXSDUS','IRLTLT01DEM156N')


def extend_catalog(catalog):
    result=dict(catalog)
    for sid in SERIES:result.setdefault(sid,{'category':'dollar_research','display_name':sid})
    return result
