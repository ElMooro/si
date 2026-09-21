"""Bind selected tape sessions to retained originals and reviewed source compilers."""
from datetime import timedelta
from decimal import Decimal
import json,re
import money_volume_sessions as sessions
from money_volume_pins import BREADTH_COMPILERS

PREFIX='data/breadth-research/'
PRIVATE='audit-private/20260909-originals/market-internals/'


def artifact(key):
    return isinstance(key,str) and bool(re.fullmatch(re.escape(PRIVATE)+r'[a-f0-9]{64}\.bin|'+re.escape(PREFIX)+r'(?:inputs|outputs|runs|compilers)/[a-f0-9]{64}\.(?:json|py)',key))


def checked(ref,kind,read):
    digest=ref.get('sha256','')
    if not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PREFIX+kind+'/'+digest+'.json':raise ValueError('Upstream artifact identity differs')
    raw=read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw)!=ref['bytes'] or sessions.sha(raw)!=digest:raise ValueError('Upstream artifact bytes differ')
    return json.loads(raw)


def prepare(packet,read):
    if packet.get('contract')!='breadth-native-research.v1':raise ValueError('Native breadth source required')
    ref=packet['replay'];key=ref.get('manifest_key','')
    if not re.fullmatch(re.escape(PREFIX)+r'runs/[a-f0-9]{64}\.json',key):raise ValueError('Upstream run path differs')
    raw=read(key);manifest=json.loads(raw)
    if key!=PREFIX+'runs/'+sessions.sha(raw)+'.json' or manifest.get('contract')!='breadth-native-replay.v1':raise ValueError('Upstream run differs')
    if set(manifest['compilers'])!=set(BREADTH_COMPILERS):raise ValueError('Upstream compiler inventory differs')
    for name,digest in BREADTH_COMPILERS.items():
        expected={'key':PREFIX+'compilers/'+digest+'.py','sha256':digest}
        if manifest['compilers'][name]!=expected or sessions.sha(read(expected['key']))!=digest:raise ValueError('Reviewed upstream compiler differs')
    output=checked(manifest['output'],'outputs',read)
    if output!={k:v for k,v in packet.items() if k!='replay'} or sessions.sha(sessions.encoded(output))!=ref.get('output_sha256') or manifest['output_sha256']!=ref['output_sha256']:raise ValueError('Upstream current body differs')
    inputs=checked(manifest['input'],'inputs',read)
    if inputs.get('contract')!='breadth-native-inputs.v1' or inputs['generated_at']!=manifest['generated_at'] or output['generated_at']!=manifest['generated_at']:raise ValueError('Upstream input clocks differ')
    if not 0<=(sessions.stamp(inputs['generated_at'])-sessions.stamp(inputs['started_at'])).total_seconds()<=720:raise ValueError('Upstream collection interval differs')
    days=sessions.sessions(inputs['generated_at'],6)
    if days!=inputs['expected_days'][-6:] or set(inputs['sources'])!=set(inputs['expected_days']):raise ValueError('Selected source session inventory differs')
    return {'generated_at':inputs['generated_at'],'started_at':inputs['started_at'],'days':days,
        'sources':{day:inputs['sources'][day] for day in days},'replay':ref}


def restore_day(plan,day,read):
    item=plan['sources'][day]
    if item.get('error'):
        if set(item)!={'error'} or not re.fullmatch('[a-z0-9_]{1,80}',item['error']):raise ValueError('Unsafe source failure descriptor')
        return None,{'status':'unavailable','reason':item['error']}
    ref=item['evidence'];digest=ref.get('sha256','');acquired=item['acquired_at']
    expected='https://api.massive.com/v2/aggs/grouped/locale/us/market/stocks/'+day+'?adjusted=true&include_otc=false'
    if not re.fullmatch('[a-f0-9]{64}',digest) or ref.get('key')!=PRIVATE+digest+'.bin' or ref.get('source_url')!=expected or ref.get('provider')!='massive':raise ValueError('Original source identity differs')
    if not sessions.stamp(plan['started_at'])<=sessions.stamp(acquired)<=sessions.stamp(plan['generated_at']):raise ValueError('Original acquisition clock differs')
    raw=read(ref['key'])
    if type(ref.get('bytes')) is not int or len(raw)!=ref['bytes'] or sessions.sha(raw)!=digest:raise ValueError('Original source bytes differ')
    parsed=sessions.parse_session(raw,day,acquired)
    document=json.loads(raw,parse_float=Decimal)
    for ticker,row in parsed.items():
        original=document['results'][row['source_row_index']]
        try:
            vw=sessions.number(original.get('vw'))
            if vw<=0:raise ValueError('Nonpositive VWAP')
        except ValueError:vw=None
        # Zero reported volume implies zero turnover, without inventing a VWAP.
        row['vwap']=vw
        row['turnover']=Decimal(0) if row['volume']==0 else row['volume']*vw if vw is not None else None
    return parsed,{'status':'original_replayed','source':ref,'acquired_at':acquired,'returned_rows':len(parsed)}
