"""Add source-defined activity and real-sales context without changing existing entries."""
SERIES=('WEI','GDPNOW','RRSFS')

def extend_catalog(catalog):
    result=dict(catalog)
    for sid in SERIES:result.setdefault(sid,{'category':'activity_research','display_name':sid})
    return result
