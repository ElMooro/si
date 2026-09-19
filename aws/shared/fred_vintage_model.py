"""Original ALFRED periods and dated definitions, without first-release claims.

The archive is reconstructed as retrieved today. A real-time interval is not an
intraday release timestamp or proof that JustHodl actually possessed it then.
"""
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
import math
import re

CONTRACT = 'fred-vintage-periods.v1'
PREFIX = 'data/vintage-research/'
EARLIEST = '1776-07-04'
MAX_DATE = '9999-12-31'
PAGE_SIZE = 10000
SERIES = ('GDPC1','GDP','INDPRO','PAYEMS','UNRATE','RSAFS','PCEC96',
          'CPIAUCSL','CPILFESL','PCEPI','PCEPILFE','DGS10','DGS2','DGS3MO',
          'FEDFUNDS','T10Y2Y','BAMLH0A0HYM2','NFCI','STLFSI4','M2SL','WALCL',
          'RRPONTSYD','HOUST','UMCSENT','ICSA','WTREGEN')


def encoded(value):
    return json.dumps(value,sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()


def digest(value):return hashlib.sha256(encoded(value)).hexdigest()


def day(value):
    if not isinstance(value,str) or not re.fullmatch(r'\d{4}-\d{2}-\d{2}',value):
        raise ValueError('calendar date required')
    return date.fromisoformat(value)


def clock(value):
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    if stamp.tzinfo is None:raise ValueError('timezone-aware clock required')
    return stamp.astimezone(timezone.utc)


def request(sid,endpoint='series',start=EARLIEST,end=MAX_DATE,offset=0):
    if sid not in SERIES:raise ValueError('unreviewed series identity')
    if endpoint not in ('series','series/observations'):raise ValueError('unreviewed endpoint')
    if day(start)>day(end):raise ValueError('reversed request interval')
    params={'series_id':sid,'file_type':'json','realtime_start':start,'realtime_end':end}
    if endpoint.endswith('observations'):
        if type(offset) is not int or offset<0:raise ValueError('invalid page offset')
        params.update(observation_start=EARLIEST,observation_end=MAX_DATE,
                      units='lin',output_type=1,sort_order='asc',limit=PAGE_SIZE,offset=offset)
    return {'endpoint':endpoint,'params':params}


def windows(first,last):
    """At most 1,461 calendar days: below FRED's 2,000 JSON vintage dates."""
    start=day(first);end=day(last);out=[]
    while start<=end:
        stop=min(start+timedelta(days=1460),end)
        out.append((start.isoformat(),stop.isoformat()))
        start=stop+timedelta(days=1)
    return out


def original(record,sid,endpoint):
    """Rebind the actual bytes, full public request and acquisition timestamp."""
    req=record['request'];params=req['params'];raw=record['raw'];receipt=record['evidence']
    if req!=request(sid,endpoint,params['realtime_start'],params['realtime_end'],params.get('offset',0)):
        raise ValueError('source request differs from reviewed contract')
    sha=hashlib.sha256(raw).hexdigest()
    key=PREFIX+'originals/'+digest(req)+'/'+sha+'.json.gz'
    if receipt!={'key':key,'sha256':sha,'bytes':len(raw),'request_sha256':digest(req)}:
        raise ValueError('original bytes or request binding differs')
    stamp=clock(record['acquired_at']);body=json.loads(raw)
    if body.get('realtime_start')!=params['realtime_start'] or body.get('realtime_end')!=params['realtime_end']:
        raise ValueError('provider real-time query differs')
    if 'error_code' in body:raise ValueError('provider error is not data')
    return body,{'request':req,'evidence':receipt,'acquired_at':stamp.isoformat()}


def decimal_value(value):
    if value=='.':return None
    if not isinstance(value,str) or not value:raise ValueError('source decimal text required')
    try:number=Decimal(value)
    except InvalidOperation:raise ValueError('invalid source number') from None
    if not number.is_finite() or not math.isfinite(float(number)):raise ValueError('non-finite source number')
    return value


def compile_series(sid,definition,pages,generated_at,collection_id,archive_end,collection_started_at):
    generated=clock(generated_at);day(archive_end)
    if clock(collection_started_at)>generated:raise ValueError('collection begins after completion')
    meta,meta_ref=original(definition,sid,'series')
    definitions=meta.get('seriess')
    if not isinstance(definitions,list) or not definitions:raise ValueError('no archived definition')
    for row in definitions:
        if row.get('id')!=sid or day(row['realtime_start'])>day(row['realtime_end']):
            raise ValueError('definition identity or interval differs')
        if not row.get('units') or not row.get('frequency_short'):raise ValueError('definition units/frequency missing')
    first=max('1990-01-01',min(row['realtime_start'] for row in definitions))
    planned=windows(first,archive_end);grouped={span:[] for span in planned}
    references=[];records=[];all_clocks=[clock(meta_ref['acquired_at'])]
    for page in pages:
        body,ref=original(page,sid,'series/observations');p=ref['request']['params']
        span=(p['realtime_start'],p['realtime_end'])
        if span not in grouped:raise ValueError('unexpected source window')
        grouped[span].append((body,ref));all_clocks.append(clock(ref['acquired_at']))
    if any(stamp>generated for stamp in all_clocks):raise ValueError('source acquisition follows build clock')
    for span,batch in grouped.items():
        offset=0;count=None
        if not batch:raise ValueError('missing archive window')
        for body,ref in sorted(batch,key=lambda pair:pair[1]['request']['params']['offset']):
            p=ref['request']['params'];observations=body.get('observations');n=body.get('count')
            if type(n) is not int or n<0 or not isinstance(observations,list):raise ValueError('invalid provider pagination')
            if count is None:count=n
            if n!=count or p['offset']!=offset or body.get('offset')!=offset or body.get('limit')!=PAGE_SIZE:
                raise ValueError('incomplete or shifting pagination')
            if body.get('units')!='lin' or body.get('output_type')!=1 or body.get('sort_order')!='asc':
                raise ValueError('transformed or wrong source output type')
            if body.get('observation_start')!=EARLIEST or body.get('observation_end')!=MAX_DATE:
                raise ValueError('observation range differs')
            if len(observations)!=min(PAGE_SIZE,count-offset):raise ValueError('truncated or excess source page')
            page_index=len(references);references.append(ref)
            for row_index,row in enumerate(observations):
                observed=day(row['date']);start=day(row['realtime_start']);end=day(row['realtime_end'])
                if not day(span[0])<=start<=end<=day(span[1]):raise ValueError('row outside requested real-time window')
                value=decimal_value(row.get('value'))
                records.append({'date':observed.isoformat(),'value_decimal':value,
                    'value':float(value) if value is not None else None,
                    'valid_from':start.isoformat(),'valid_through':end.isoformat(),
                    # A clipped interval boundary must never masquerade as a release.
                    'known_on':None if start.isoformat()==span[0] else start.isoformat(),
                    'start_left_censored':start.isoformat()==span[0],
                    'source_page':page_index,'source_row':row_index})
            offset+=len(observations)
        if offset!=count:raise ValueError('missing final observation page')
    records.sort(key=lambda row:(row['date'],row['valid_from'],row['valid_through']))
    previous=None
    for row in records:
        if previous and previous['date']==row['date'] and row['valid_from']<=previous['valid_through']:
            raise ValueError('overlapping archive periods for one observation')
        previous=row
    return {'contract':CONTRACT,'version':'2.0.0','series':sid,'generated_at':generated_at,'updated':generated_at,
        'collection_id':collection_id,'collection_started_at':collection_started_at,'acquired_at':min(all_clocks).isoformat(),
        'acquisition_completed_at':max(all_clocks).isoformat(),'definitions':definitions,
        'definition_source':meta_ref,'observation_sources':references,'vintages':records,'n_vintages':len(records),
        'coverage':{'status':'complete_requested_windows','archive_start':first,'archive_end':archive_end,
            'windows':len(planned),'pages':len(references),'observation_start':EARLIEST,'observations':len({r['date'] for r in records}),
            'missing_periods':sum(r['value_decimal'] is None for r in records),'all_time_archive_claim':False},
        'point_in_time':False,'historical_feature_replay_ready':False,'sizing_eligible':False,'calls_eligible':False,
        'scope':'Provider archive reconstruction captured now; interval starts may be clipped. Not first-release-only, actual intraday availability, a historical JustHodl decision or a qualified investment model.',
        'availability_rule':'A provider archive date is usable from 12:00 UTC on the following calendar day; this conservative research delay is a policy, not a measured release timestamp.'}


def validate_packet(doc):
    if not isinstance(doc,dict) or doc.get('contract')!=CONTRACT:raise ValueError('original-bound archive required')
    ref=doc.get('replay') or {};payload={k:v for k,v in doc.items() if k!='replay'}
    if ref.get('output_sha256')!=digest(payload):raise ValueError('archive content binding differs')
    if not re.fullmatch(PREFIX+r'runs/[a-f0-9]{64}\.json',ref.get('manifest_key','')):
        raise ValueError('archive replay reference missing')
    if doc.get('coverage',{}).get('status')!='complete_requested_windows':raise ValueError('incomplete archive')
    return payload


def select_asof(doc,decision_at,validate=True):
    """Select the latest observation valid on a completed archive day, including null."""
    if validate:validate_packet(doc)
    decision=clock(decision_at)
    archive_day=(decision-timedelta(hours=12,days=1)).date().isoformat()
    coverage=doc['coverage']
    if archive_day<coverage['archive_start'] or archive_day>coverage['archive_end']:
        return {'status':'outside_retained_archive','selected':None}
    rows=[(i,row) for i,row in enumerate(doc['vintages'])
          if row['date']<=archive_day and row['valid_from']<=archive_day<=row['valid_through']]
    if not rows:return {'status':'unavailable','selected':None}
    latest=max(row['date'] for _,row in rows);chosen=[(i,row) for i,row in rows if row['date']==latest]
    if len(chosen)!=1:raise ValueError('ambiguous observation period')
    index,row=chosen[0]
    definitions=[(i,d) for i,d in enumerate(doc['definitions']) if d['realtime_start']<=archive_day<=d['realtime_end']]
    if len(definitions)!=1:return {'status':'historical_definition_unavailable','selected':None}
    mi,meta=definitions[0]
    return {'status':'missing' if row['value_decimal'] is None else 'archived_value',
        'archive_day':archive_day,'decision_at':decision_at,'selected':{
            **row,'units':meta['units'],'frequency':meta['frequency_short'],
            'seasonal_adjustment':meta.get('seasonal_adjustment'),'title':meta['title'],
            'series':doc['series'],'row_index':index,'definition_index':mi,
            'source':doc['observation_sources'][row['source_page']],
            'definition_source':doc['definition_source'],'replay':doc.get('replay')},
        'intraday_release_verified':False,'actual_system_possession_verified':False,'historical_feature_replay_ready':False}


def net_liquidity(docs,now=None):
    """Dated archival balance-sheet context, never automatic event-study authority."""
    stamp=now or datetime.now(timezone.utc)
    if stamp.tzinfo is None:raise ValueError('timezone-aware evaluation clock required')
    required=('WALCL','WTREGEN','RRPONTSYD');contracts={};missing=[];valid={}
    out={'schema_version':'2.0','status':'BLOCKED','point_in_time':False,'publication_eligible':False,
        'historical_feature_replay_ready':False,'contracts':contracts,'missing_series':missing,
        'units':'USD millions','series':{},'series_decimal':{},'components':{},'excluded_days':{},
        'availability_rule':'Daily research observations are evaluated at 12:00 UTC; archive dates become usable the next calendar day.',
        'history_semantics':'Original-bound provider archive reconstruction; historical decision rules and event-study execution timing are not qualified.'}
    for sid in required:
        try:
            doc=docs.get(sid);validate_packet(doc)
            if doc['series']!=sid:raise ValueError('series identity differs')
            age=(stamp-clock(doc['generated_at'])).total_seconds()
            if age<0 or age>8*86400:raise ValueError('archive publication expired or future')
            valid[sid]=doc;contracts[sid]={'usable':True,'status':'ORIGINAL_BOUND_ARCHIVE','replay':doc['replay']}
        except (ValueError,KeyError,TypeError,AttributeError) as exc:
            missing.append(sid);contracts[sid]={'usable':False,'status':'BLOCKED','reason':str(exc)}
    if missing:return out
    if len({d['collection_id'] for d in valid.values()})!=1:
        out['reason']='Component archives belong to different collections';return out
    multipliers={'Millions of Dollars':Decimal(1),'Millions of U.S. Dollars':Decimal(1),'Millions of US Dollars':Decimal(1),
                 'Billions of Dollars':Decimal(1000),'Billions of U.S. Dollars':Decimal(1000),'Billions of US Dollars':Decimal(1000)}
    first=max(day(d['coverage']['archive_start']) for d in valid.values())+timedelta(days=1)
    last=min(min(day(d['coverage']['archive_end']) for d in valid.values())+timedelta(days=1),stamp.date())
    current=first
    while current<=last:
        instant=datetime.combine(current,datetime.min.time(),timezone.utc)+timedelta(hours=12)
        key=current.isoformat();current+=timedelta(days=1)
        if instant>stamp or instant.weekday()>4:continue
        selected={sid:select_asof(doc,instant.isoformat(),validate=False) for sid,doc in valid.items()}
        bad=[sid+': '+s['status'] for sid,s in selected.items() if s['status']!='archived_value']
        if bad:out['excluded_days'][key]=bad;continue
        rows={sid:s['selected'] for sid,s in selected.items()}
        bad=[sid+': stale observation or unreviewed historical unit' for sid,row in rows.items()
             if (instant.date()-day(row['date'])).days>14 or row['units'] not in multipliers]
        if bad:out['excluded_days'][key]=bad;continue
        levels={sid:Decimal(row['value_decimal'])*multipliers[row['units']] for sid,row in rows.items()}
        value=levels['WALCL']-levels['WTREGEN']-levels['RRPONTSYD']
        out['series'][key]=float(value);out['series_decimal'][key]=str(value)
        out['components'][key]={sid:{'row_index':row['row_index'],'definition_index':row['definition_index'],
            'observation_date':row['date'],'original_value_decimal':row['value_decimal'],'original_unit':row['units'],
            'value_usd_mn_decimal':str(levels[sid])} for sid,row in rows.items()}
    out['n_points']=len(out['series']);out['collection_id']=next(iter(valid.values()))['collection_id']
    out['status']='ARCHIVE_RESEARCH' if out['series'] else 'BLOCKED'
    out['reason']='Historical feature definitions, decision timing and out-of-sample investment protocol still require qualification.'
    return out

