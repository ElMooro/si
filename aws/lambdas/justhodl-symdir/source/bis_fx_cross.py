"""Explicit ratios of BIS reference rates; never a same-time tradable FX quote."""
from collections import defaultdict
from decimal import Decimal
from datetime import datetime,timezone
import base64,gzip,hashlib,json,re
import bis_fx_series as series

CONTRACT='bis-reference-cross.v1'

def _legs():
    groups=defaultdict(list)
    for key,d in series.CATALOG['series'].items():
        if (d['freq']=='D' and d['collection']=='A') or (d['freq']=='M' and d['collection']=='E'):
            groups[(d['currency'],d['freq'])].append(key)
    out={}
    # Explicit currency-area choices; other ambiguous currency-area sets remain unavailable.
    area={'EUR':'XM','USD':'US','AUD':'AU'}
    for pair,keys in groups.items():
        if len(keys)==1:out[pair]=keys[0]
        elif pair[0] in area:
            chosen=[key for key in keys if key.split('.')[1]==area[pair[0]]]
            if len(chosen)==1:out[pair]=chosen[0]
    return out

LEGS=_legs()

def definition(identifier):
    if not isinstance(identifier,str):raise ValueError('Exact BIS reference cross required')
    match=re.fullmatch(r'(?i)bisfx:([A-Z]{3}):([A-Z]{3}):([DM])',identifier)
    if not match:raise ValueError('Exact base, quote and frequency required')
    base,quote,freq=[s.upper() for s in match.groups()]
    if base==quote or (base,freq) not in LEGS or (quote,freq) not in LEGS:raise ValueError('Unambiguous reviewed BIS reference legs unavailable')
    numerator='bis:WS_XRU:'+LEGS[(quote,freq)];denominator='bis:WS_XRU:'+LEGS[(base,freq)]
    return {'id':'bisfx:'+base+':'+quote+':'+freq,'base':base,'quote':quote,'freq':freq,
      'numerator':numerator,'denominator':denominator,'unit':quote+' per '+base,
      'name':'BIS reference-rate ratio '+base+'/'+quote,
      'formula':'(quote currency per USD) / (base currency per USD)',
      'warning':'Derived ratio of BIS reference observations, not a traded FX cross or a verified same-time fixing. Daily labels may conceal different source fixing times; ratio of averages is not an average cross rate. Historical sources and redenominations may change.'}

def fetch(identifier,fetcher=None):
    d=definition(identifier);load=fetcher or series.fetch
    numerator=load(d['numerator'])
    # A denial or invalid leg stops the operation; no alternative series/provider is tried.
    def allowed(packet,sid):
        if not isinstance(packet,dict) or not series.cache_valid(packet,sid):raise ValueError('BIS reference leg did not complete with the reviewed identity')
    allowed(numerator,d['numerator']);denominator=load(d['denominator']);allowed(denominator,d['denominator'])
    left=dict(numerator['obs']);right=dict(denominator['obs']);dates=sorted(set(left)|set(right));obs=[];evidence=[]
    for period in dates:
        a,b=left.get(period),right.get(period);reason=None;value=None
        if a is None or b is None:reason='missing_source_leg'
        elif b<=0 or a<=0:reason='invalid_reference_rate'
        else:
            exact=Decimal(str(a))/Decimal(str(b));value=float(exact)
            if not (0 < value < float('inf')):value=None;reason='invalid_derived_rate'
        obs.append([period,value]);evidence.append([period,a,b,value,reason])
    receipts=numerator['source_receipts']+denominator['source_receipts']
    result={'contract':CONTRACT,'id':d['id'],'requested_id':identifier,'provider':'bisfx','provider_name':'BIS derived reference rates',
      'definition':d,'definition_sha256':series.CATALOG_HASH,'name':d['name'],'freq':d['freq'],'unit':d['unit'],
      'obs':obs,'n':sum(v is not None for _,v in obs),'first':obs[0][0] if obs else None,'last':obs[-1][0] if obs else None,
      'acquired_at':datetime.now(timezone.utc).isoformat(),'source_published_at':None,
      'source':'BIS WS_XRU source observations / explicit reference-rate ratio',
      'source_definitions':[numerator['definition'],denominator['definition']],
      'source_receipts':receipts,'measurement_evidence':{'columns':['period','numerator','denominator','ratio','rejection'],'rows':evidence},
      'source_period_evidence':{'encoding':'gzip+base64','content_type':'application/json','body':base64.b64encode(gzip.compress(json.dumps([numerator['measurement_evidence'],denominator['measurement_evidence']],separators=(',',':')).encode(),mtime=0)).decode()},
      'quality':{'status':'unavailable' if not any(v is not None for _,v in obs) else 'partial' if any(r[-1] for r in evidence) else 'observations','rejected_rows':sum(r[-1] is not None for r in evidence)},
      'history':{'response_complete':True,'missing_periods_filled':False,'full_upstream_history_verified':False,'point_in_time_vintages_verified':False,'same_time_fix_verified':False},
      'equivalence_to_watchlist_provider_verified':False,'calls_eligible':False,'sizing_eligible':False}
    if len(json.dumps(result).encode())>3900000:raise ValueError('Both complete source receipts exceed chart response bound; use the component series')
    return result


def cache_valid(packet,sid):
    try:d=definition(sid)
    except ValueError:return False
    return (isinstance(packet,dict) and packet.get('contract')==CONTRACT and packet.get('id')==d['id']
            and packet.get('definition_sha256')==series.CATALOG_HASH and packet.get('definition')==d
            and isinstance(packet.get('history'),dict) and packet['history'].get('response_complete') is True
            and packet.get('source_definitions')==[series.definition(d['numerator']),series.definition(d['denominator'])])
