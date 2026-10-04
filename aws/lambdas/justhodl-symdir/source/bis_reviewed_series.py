"""Dispatch exact reviewed BIS datasets without fuzzy matching or provider fallback."""
import bis_policy_series as policy
import bis_fx_series as fx

ADAPTERS={'WS_CBPOL':policy,'WS_XRU':fx}

def adapter(sid):
    parts=str(sid).split(':')
    if len(parts)!=3 or parts[0].lower()!='bis' or parts[1].upper() not in ADAPTERS:
        raise ValueError('Select an exact reviewed BIS dataset and series key')
    return ADAPTERS[parts[1].upper()]

def definition(sid):return adapter(sid).definition(sid)
def fetch(sid):return adapter(sid).fetch(sid)
def cache_valid(packet,sid):
    try:return adapter(sid).cache_valid(packet,sid)
    except ValueError:return False

def directory(q='',limit=50,offset=0,flow=None):
    if flow is not None:
        if flow.upper() not in ADAPTERS:raise ValueError('Unreviewed BIS dataset')
        return ADAPTERS[flow.upper()].directory(q,limit,offset,dataset=True)
    if type(limit) is not int or not 1<=limit<=500 or type(offset) is not int or offset<0:
        raise ValueError('Invalid BIS directory page')
    rows=[]
    for module in ADAPTERS.values():
        first=module.directory(q,500,0);rows.extend(first['rows'])
        for start in range(500,first['total'],500):rows.extend(module.directory(q,500,start)['rows'])
    rows.sort(key=lambda row:row['id'])
    return {'provider':'bis','provider_name':'BIS','rows':rows[offset:offset+limit],
            'total':len(rows),'offset':offset,'limit':limit,'history_verified':False,
            'definition_hashes':{flow:module.CATALOG_HASH for flow,module in ADAPTERS.items()},
            'catalogue_scope':'Reviewed WS_CBPOL policy rates and WS_XRU bilateral reference rates; other datasets remain catalogue records'}
