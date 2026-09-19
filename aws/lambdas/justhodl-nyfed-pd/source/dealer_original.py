"""Reviewed dealer identities and strict original-response parsing.

Each identity retains its own value basis and definition-review status.
Historical response rows are current provider vintage, never point-in-time truth.
"""
from datetime import date, datetime, timezone
import hashlib
import json
import re

BASE = 'https://markets.newyorkfed.org/api/pd/'
CATALOG_URL = BASE+'list/timeseries.json'
BREAKS_URL = BASE+'list/seriesbreaks.json'
RELEASE_URL = 'https://www.newyorkfed.org/markets/counterparties/primary-dealers-statistics'
DEFINITION_BASE = 'https://www.federalreserve.gov/apps/reportingforms/Download/DownloadAttachment?guid='
from pathlib import Path
REGISTRY_PATH = Path(__file__).with_name('dealer-native-registry.json')
REGISTRY = json.loads(REGISTRY_PATH.read_bytes())
SERIES = REGISTRY['series']
KEYS = tuple(sorted(SERIES))
DATA_URL = BASE+'get/'+'_'.join(KEYS)+'.json'
# Reporting-definition snapshots reviewed against official FR2004C instructions.
# One-based PDF pages identify the accounting rules; source-period boundaries
# come from the independently captured NY Fed series-break catalog.
DEFINITIONS = {
    'SBN2013': {'start': '2013-04-01', 'end': '2014-12-31', 'page': 23,
                'guid': 'f555fe11-e149-479b-8088-4ed4e9ae1206', 'sha256': '7fc2d9f7f4f0cd5ffad94fc328acd6a2115f96fed16b9b0fd559344e0ff05ac3'},
    'SBN2015': {'start': '2015-01-01', 'end': '2022-01-04', 'page': 23,
                'guid': '0ead3d17-ff78-4ffe-9c62-a5c0b9784c82', 'sha256': '504cfdd48966d6499f70eb9da355388b3ac9597e4ecedf50d43b3f4ceeeacd11'},
    'SBN2022': {'start': '2022-01-05', 'end': '2024-07-02', 'page': 25,
                'guid': '978610fa-a9ee-4a6c-ac90-8055546b77ea', 'sha256': 'a83b6ee7027cd8f1b6a298e25b4000e0ca7f702984d4cee8a1699cdcab3c10ab'},
    'SBN2024': {'start': '2024-07-03', 'end': '9999-12-31', 'page': 25,
                'guid': '950ab256-b485-4586-99b0-b27eb205fdfa', 'sha256': '3184245a8da570b14d34cbe6a2bf4fafb76acfc487c70c7642e8e4c3da755aeb'},
}
SECTION_PAGES = {
    'SBN2013': {'net_outright_position':13,'transactions':17,'financing':23,'specific_issue_position':29},
    'SBN2015': {'net_outright_position':12,'transactions':17,'financing':23,'specific_issue_position':29},
    'SBN2022': {'net_outright_position':13,'transactions':19,'financing':25,'specific_issue_position':31},
    'SBN2024': {'net_outright_position':13,'transactions':19,'financing':25,'specific_issue_position':33},
}
URLS = {'catalog':CATALOG_URL,'breaks':BREAKS_URL,'observations':DATA_URL,
        'release_schedule':RELEASE_URL,'historical_release':REGISTRY['reference_release']['url'],
        **{k:DEFINITION_BASE+v['guid'] for k,v in DEFINITIONS.items()}}



def encoded(value): return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False, allow_nan=False).encode()
def digest(value): return hashlib.sha256(encoded(value)).hexdigest()


def day(value):
    if not isinstance(value, str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}', value): raise ValueError('exact observation date required')
    return date.fromisoformat(value)


def clock(value):
    if not isinstance(value, str): raise ValueError('dated source clock required')
    result = datetime.fromisoformat(value.replace('Z', '+00:00'))
    if result.tzinfo is None: raise ValueError('source clock requires timezone')
    return result.astimezone(timezone.utc)


def strict_json(raw):
    def pairs(items):
        out = {}
        for k,v in items:
            if k in out: raise ValueError('duplicate JSON member')
            out[k] = v
        return out
    def invalid(_): raise ValueError('nonfinite JSON value')
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=invalid)


def original(descriptor, expected_url, read, at):
    if not isinstance(descriptor, dict) or descriptor.get('url') != expected_url: raise ValueError('source request differs')
    acquired = clock(descriptor.get('acquired_at'))
    if acquired > clock(at): raise ValueError('future source acquisition')
    receipt = descriptor.get('evidence') or {}
    public_url = expected_url.split('?')[0]  # Complete, fixed public request remains in descriptor.url.
    if receipt.get('source_url') != public_url or receipt.get('provider') != 'fr2004': raise ValueError('source identity differs')
    if receipt.get('contract') != 'source-evidence.v1' or receipt.get('captured') is not True: raise ValueError('captured original required')
    sha = receipt.get('sha256'); request_sha = hashlib.sha256(public_url.encode()).hexdigest()
    if not isinstance(sha,str) or not re.fullmatch('[a-f0-9]{64}',sha): raise ValueError('original hash required')
    if receipt.get('key') != 'data/evidence/fr2004/'+request_sha+'/'+sha+'.bin.gz': raise ValueError('original path differs')
    if clock(receipt.get('first_received_at')) > acquired: raise ValueError('original receipt is later than acquisition')
    raw = read(receipt['key'])
    if not isinstance(raw,bytes) or not 0<len(raw)<=8*1024*1024: raise ValueError('original response bound')
    if len(raw)!=receipt.get('bytes') or hashlib.sha256(raw).hexdigest()!=sha: raise ValueError('original bytes differ')
    return raw


def period(observed):
    matches = [k for k,v in DEFINITIONS.items() if v['start'] <= observed <= v['end']]
    if len(matches)!=1: raise ValueError('unreviewed reporting period')
    return matches[0]


def metadata(raws):
    for key, definition in DEFINITIONS.items():
        if not raws[key].startswith(b'%PDF-') or hashlib.sha256(raws[key]).hexdigest()!=definition['sha256']:
            raise ValueError('reporting instructions changed; review required')
    breaks = strict_json(raws['breaks']).get('pd',{}).get('seriesbreaks')
    if not isinstance(breaks,list): raise ValueError('series-break catalog unavailable')
    found = {}
    for row in breaks:
        key = row.get('seriesbreak')
        if key in found: raise ValueError('duplicate series break')
        found[key] = row
    for key,definition in DEFINITIONS.items():
        row = found.get(key,{})
        if (row.get('startdate'),row.get('enddate'))!=(definition['start'],definition['end']):
            raise ValueError('reporting-period boundaries changed')
    catalog = strict_json(raws['catalog']).get('pd',{}).get('timeseries')
    if not isinstance(catalog,list): raise ValueError('series catalog unavailable')
    selected = {}
    for index,row in enumerate(catalog):
        key = row.get('keyid')
        if key not in KEYS: continue
        if key in selected: raise ValueError('duplicate catalog identity')
        if row!=SERIES[key]['catalog']:
            raise ValueError('source definition changed; review required')
        selected[key] = {**row,'row_index':index}
    if set(selected)!=set(KEYS): raise ValueError('missing source catalog identity')
    if hashlib.sha256(raws['historical_release']).hexdigest()!=REGISTRY['reference_release']['sha256']:
        raise ValueError('historical definition reference changed; review required')
    # A normal weekly timetable is a policy reference, not an actual print timestamp.
    text = raws['release_schedule'].decode('utf-8')
    if not all(s in text for s in ('Thursdays','4:15','previous week')): raise ValueError('normal publication timetable changed')
    return selected


def parse(raw, at):
    rows = strict_json(raw).get('pd',{}).get('timeseries')
    if not isinstance(rows,list) or not 1<=len(rows)<=100000: raise ValueError('bounded original observations required')
    out = {key:{} for key in KEYS}
    for index,row in enumerate(rows):
        if not isinstance(row,dict) or row.get('keyid') not in out: raise ValueError('unexpected original series identity')
        key = row['keyid']; observed = row.get('asofdate'); d=day(observed)
        if d>clock(at).date(): raise ValueError('future observation')
        if observed in out[key]: raise ValueError('duplicate original observation')
        value=row.get('value')
        if value in ('*','',None): amount=None
        elif isinstance(value,str) and re.fullmatch(r'-?(?:0|[1-9]\d{0,14})',value):
            amount=int(value)
            if amount<0 and SERIES[key]['family'] not in ('net_outright_position','specific_issue_position'):
                raise ValueError('negative financing or transaction amount')
            if abs(amount)>(2**53-1)//100:raise ValueError('reported amount exceeds exact downstream integer bound')
        else: raise ValueError('dealer value must be an exact signed integer or explicitly missing')
        out[key][observed] = {'date':observed,'usd_mn':amount,'row_index':index,
                             'source_value':value,'series_id':key,'seriesbreak':period(observed),
                             'status':'suppressed' if value=='*' else 'missing' if amount is None else 'observed'}
    if any(not rows for rows in out.values()): raise ValueError('requested source series missing')
    return out


def load(inputs, read):
    if inputs.get('contract')!='dealer-original-inputs.v1' or set(inputs.get('sources',{}))!=set(URLS):
        raise ValueError('complete original input set required')
    at=inputs['generated_at'];clock(at)
    raws={key:original(inputs['sources'][key],url,read,at) for key,url in URLS.items()}
    definitions=metadata(raws)
    return parse(raws['observations'],at),definitions
