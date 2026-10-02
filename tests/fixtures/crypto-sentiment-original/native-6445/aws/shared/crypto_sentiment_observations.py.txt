"""Pure reported Alternative.me observations, no synthetic history/vote."""
from base64 import b64encode
from datetime import datetime,timedelta,timezone
from decimal import Decimal,ROUND_HALF_EVEN,localcontext
import hashlib,json,re

CONTRACT='reported-bitcoin-sentiment.v1'
DENIED={'calls_eligible':False,'sizing_eligible':False,'execution_eligible':False,
        'forecast_qualified':False,'independent_investment_votes':0,
        'source_authenticity_verified':False,'point_in_time_qualified':False}
SOURCES=('https://api.alternative.me/fng/?limit=31','https://api.alternative.me/fng/?limit=0')
LIMIT=8*1024*1024
MAX_ROWS=50000

def clock(value):
    if not isinstance(value,str):raise ValueError('Explicit UTC acquisition clock required')
    stamp=datetime.fromisoformat(value.replace('Z','+00:00'))
    if stamp.tzinfo is None or stamp.utcoffset()!=timedelta(0):raise ValueError('UTC acquisition required')
    return stamp

def pairs(items):
    out={}
    for key,value in items:
        if key in out:raise ValueError('Duplicate JSON member')
        out[key]=value
    return out

def integer(value,upper):
    if isinstance(value,str):
        if not re.fullmatch(r'(?:0|[1-9][0-9]{0,11})',value):return None
        value=int(value)
    elif isinstance(value,Decimal):
        if not value.is_finite() or value!=value.to_integral_value():return None
        if not 0<=value<=upper:return None
        value=int(value)
    elif type(value) is not int:return None
    return value if 0<=value<=upper else None

def average(series,anchor,days,undated):
    result={'status':'unavailable','value':None,'unit':'index_points','calendar':'UTC',
            'calendar_days':days,'source_dates':[],'sum_index_points':None,
            'denominator':None,'decimal_places':2,'rounding':'ROUND_HALF_EVEN',
            'reason':'complete_daily_window_unavailable'}
    if anchor is None or undated:return result
    end=datetime.fromisoformat(anchor).date()
    dates=[(end-timedelta(days=i)).isoformat() for i in range(days-1,-1,-1)]
    by_date={day:[row for row in series if row['date']==day] for day in dates}
    result['source_dates']=dates
    if any(len(rows)!=1 or rows[0]['value'] is None for rows in by_date.values()):return result
    total=sum(rows[0]['value'] for rows in by_date.values())
    with localcontext() as ctx:
        ctx.prec=40
        rounded=(Decimal(total)/days).quantize(Decimal('0.01'),rounding=ROUND_HALF_EVEN)
    return {**result,'status':'complete_daily_window','value':float(rounded),
            'sum_index_points':total,'denominator':days,'reason':'reported_UTC_date_window_only',
            'observation_references':[row['source_rows'] for rows in by_date.values() for row in rows],
            'historical_availability_verified':False}

def build(raw,*,source_url,acquired_at):
    acquired=clock(acquired_at)
    if source_url not in SOURCES:raise ValueError('Named existing public source required')
    if not isinstance(raw,bytes) or not 0<len(raw)<=LIMIT:raise ValueError('Complete bounded original required')
    out={'contract':CONTRACT,'status':'unavailable','source_url':source_url,
         'acquired_at':acquired_at,'unit':'index_points','scale':[0,100],
         'subject':'provider Bitcoin sentiment index','current':None,'label':None,
         'source_observation_at':None,'first_publication_at':None,
         'history':[],'series':[],'returned_rows':None,'resolved_rows':0,'unresolved_rows':None,
         'avg_7d':None,'avg_30d':None,'averages':{},
         'original_response_base64':b64encode(raw).decode('ascii'),
         'original_response_bytes':len(raw),'original_response_sha256':hashlib.sha256(raw).hexdigest(),
         'synthetic_rows_added':0,'sampled_rows_removed':0,
         'note':'Reported provider observations only. Provider timestamps are not independently verified publication times. No reconstructed prehistory, sentiment forecast or investment vote.',**DENIED}
    try:
        doc=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_int=Decimal,parse_float=Decimal,
                       parse_constant=lambda _:(_ for _ in ()).throw(ValueError('Nonfinite JSON')))
        if not isinstance(doc,dict) or not isinstance(doc.get('data'),list):raise ValueError('Whole data array required')
        if doc.get('name')!='Fear and Greed Index':raise ValueError('Named provider series required')
        if not isinstance(doc.get('metadata'),dict) or doc['metadata'].get('error','missing') is not None:raise ValueError('Explicit provider response state required')
    except (ValueError,TypeError,UnicodeError,RecursionError):return {**out,'reason':'invalid_original_response'}
    rows=doc['data']
    if len(rows)>MAX_ROWS:return {**out,'reason':'complete_population_exceeds_projection_budget','received_rows':len(rows),'projection_complete':False}
    groups={};undated=False
    for index,row in enumerate(rows):
        point={'source_row':'/data/'+str(index),'value':None,'label':None,'timestamp':None,
               'observation_at':None,'date':None,'unit':'index_points','status':'unavailable'}
        if isinstance(row,dict):
            value=integer(row.get('value'),100);timestamp=integer(row.get('timestamp'),253402300799)
            point['label']=row.get('value_classification') if isinstance(row.get('value_classification'),str) else None
            if timestamp is not None:
                observed=datetime.fromtimestamp(timestamp,timezone.utc)
                point.update(timestamp=timestamp,observation_at=observed.isoformat(),date=observed.date().isoformat())
                if observed>acquired:point['reason']='future_source_observation'
                elif value is None:point['reason']='invalid_or_missing_index_value'
                else:point.update(value=value,status='reported')
            else:point['reason']='invalid_or_missing_observation_clock'
        else:point['reason']='invalid_source_row'
        if point['timestamp'] is None:undated=True
        else:groups.setdefault(point['timestamp'],[]).append(point)
        out['history'].append(point)
    for timestamp,points in sorted(groups.items()):
        agreed=all(row['status']=='reported' for row in points) and len({row['value'] for row in points})==1
        labels={row['label'] for row in points}
        first=points[0]
        out['series'].append({'timestamp':timestamp,'observation_at':first['observation_at'],'date':first['date'],
                              'value':first['value'] if agreed else None,
                              'label':first['label'] if agreed and len(labels)==1 else None,
                              'source_rows':[row['source_row'] for row in points],
                              'source_occurrences':len(points),'status':'reported' if agreed else 'unresolved_source_occurrences'})
    resolved=sum(row['status']=='reported' for row in out['history'])
    out.update(status='descriptive',returned_rows=len(rows),resolved_rows=resolved,unresolved_rows=len(rows)-resolved,
               source_order_preserved=True,undated_source_rows=undated,
               unresolved_clock_groups=sum(row['status']!='reported' for row in out['series']),
               source_series_name=doc['name'])
    if out['series'] and not undated:
        current=out['series'][-1]
        out.update(current=current['value'],label=current['label'] if current['value'] is not None else None,
                   source_observation_at=current['observation_at'])
    for days in (7,30):
        window=average(out['series'],out['source_observation_at'],days,undated)
        out['averages'][str(days)+'d']=window;out['avg_'+str(days)+'d']=window['value']
    return out


def context(packet):
    """Recompute the whole retained projection; this consumer never reads storage."""
    result={'contract':'crypto-sentiment-consumer-context.v1','status':'unavailable',
            'current':None,'label':None,'source_observation_at':None,'first_publication_at':None,
            'avg_7d':None,'avg_30d':None,'unit':'index_points','scale':[0,100],
            'source_url':SOURCES[0],'attribution_url':'https://alternative.me/crypto/fear-and-greed-index/',
            'original_projection_checked':False,'stored_archive_read_verified':False,
            'note':'Reported Bitcoin sentiment only; no independently verified publication time, forecast or portfolio vote.',**DENIED}
    try:
        from crypto_sentiment_transport import project
        from types import SimpleNamespace
        if not isinstance(packet,dict):return result
        model=SimpleNamespace(LIMIT=LIMIT,CONTRACT=CONTRACT,DENIED=DENIED,build=build,clock=clock)
        expected=project(packet.get('source_attempts'),model)
        if set(packet)!=set(expected)|{'original_capture'}:return result
        def canonical(value):return (json.dumps(value,sort_keys=True,ensure_ascii=True,allow_nan=False,separators=(',',':'))+'\n').encode()
        if canonical(expected)!=canonical({k:packet[k] for k in expected}):return result
        proof=packet['original_capture']
        if not isinstance(proof,dict) or set(proof)!={'contract','manifest','capture','complete_capture_replayed','source_qualified','point_in_time_qualified','investment_authority'}:return result
        if proof['contract']!='crypto-sentiment-original-replay.v1' or proof['complete_capture_replayed'] is not True:return result
        for key in ('source_qualified','point_in_time_qualified','investment_authority'):
            if proof[key] is not False:return result
        for name,kind in (('manifest','runs'),('capture','captures')):
            ref=proof[name]
            if not isinstance(ref,dict) or set(ref)!={'key','bytes','sha256'}:return result
            if type(ref['bytes']) is not int or not 0<ref['bytes']<=64*1024*1024:return result
            if not isinstance(ref['sha256'],str) or not re.fullmatch('[a-f0-9]{64}',ref['sha256']):return result
            if ref['key']!='data/crypto-sentiment-research/'+kind+'/'+ref['sha256']+'.json':return result
        raw=canonical(expected)
        if proof['capture']['sha256']!=hashlib.sha256(raw).hexdigest() or proof['capture']['bytes']!=len(raw):return result
        return {**result,'status':'descriptive' if expected['current'] is not None else 'unavailable',
                **{k:expected[k] for k in ('current','label','source_observation_at','avg_7d','avg_30d','current_reason','averages','source_disagreements')},
                'original_projection_checked':True,'original_manifest_reference':proof['manifest'],
                'acquisition_completed_at':expected['source_attempts'][0]['acquisition_completed_at'],
                'source_response_sha256':expected['source_attempts'][0]['received_sha256']}
    except (ValueError,TypeError,KeyError,UnicodeError,OverflowError,RecursionError):return result
