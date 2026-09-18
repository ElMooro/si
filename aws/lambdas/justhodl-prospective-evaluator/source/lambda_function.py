"""Scheduled, bounded evaluation of prospective public research registrations.

No legacy outcome rewrites, AI calls, private account reads, orders or alerts.
Successful measurements retain exact source bytes and can be reproduced.
"""
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import re
import time
import urllib.parse
import urllib.request
from datetime import datetime, timezone, timedelta
import boto3
from managed_secret import managed_secret
from evidence_store import capture
from prospective_journal import PREFIX, canonical, digest, read_record, persist_once, stamp
from forward_price_measurement import CONTRACT, evaluate, marks
from outcome_price_evidence import PriceEvidenceVerifier, EASTERN
from calls_research_replay import publish_current

BUCKET=os.environ.get('S3_BUCKET','justhodl-dashboard-live')
STATE=PREFIX+'evaluator-state.json'
SUMMARY='data/prospective-outcomes.json'
s3=boto3.client('s3',region_name='us-east-1')


def optional_json(key):
    try:return json.loads(s3.get_object(Bucket=BUCKET,Key=key)['Body'].read())
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code','')) in ('NoSuchKey','404'):return None
        raise


def source_packet(symbol,start,end,credential):
    if not credential:return None
    path='/v2/aggs/ticker/'+urllib.parse.quote(symbol,safe='')+'/range/1/day/'+start+'/'+end
    url='https://api.polygon.io'+path+'?adjusted=true&sort=asc&limit=500&apiKey='+urllib.parse.quote(credential,safe='')
    try:
        with urllib.request.urlopen(urllib.request.Request(url,headers={'User-Agent':'JustHodl-ProspectiveResearch/1.0'}),timeout=10) as response:
            raw=response.read(2_000_001)
        if len(raw)>2_000_000 or credential.encode() in raw:raise ValueError('invalid provider response')
        doc=json.loads(raw)
        if not isinstance(doc,dict) or doc.get('ticker')!=symbol or doc.get('status') not in ('OK','DELAYED') or doc.get('adjusted') is not True or doc.get('next_url'):
            raise ValueError('incompatible provider response')
        receipt=capture(s3,BUCKET,'polygon',url,raw)
        return {'raw':raw,'evidence':receipt}
    except Exception:
        # No exception body or credential-bearing URL enters logs/public output.
        return None


def retained_packet(mark):
    receipt=mark['evidence']
    if not re.fullmatch(r'data/evidence/polygon/[0-9a-f]{64}/[0-9a-f]{64}\.bin\.gz',str(receipt.get('key',''))):
        raise ValueError('invalid price evidence path')
    obj=s3.get_object(Bucket=BUCKET,Key=receipt['key'])
    packed=obj['Body'].read(2_000_001)
    if len(packed)>2_000_000:raise ValueError('retained source size invalid')
    with gzip.GzipFile(fileobj=io.BytesIO(packed)) as stream:raw=stream.read(2_000_001)
    if len(raw)>2_000_000 or hashlib.sha256(raw).hexdigest()!=receipt['sha256']:raise ValueError('retained source bytes mismatch')
    return {'raw':raw,'evidence':receipt}


def compiler_identity():
    import forward_price_measurement,prospective_journal,outcome_price_evidence,instrument_identity
    return {Path(module.__file__).name:hashlib.sha256(Path(module.__file__).read_bytes()).hexdigest()
            for module in (forward_price_measurement,prospective_journal,outcome_price_evidence,instrument_identity)}


def replay_measurement(bundle, record, verifier, compiler):
    if (bundle.get('contract')!=CONTRACT or bundle.get('compiler')!=compiler or bundle.get('forecast_id')!=record['forecast_id']
            or (bundle.get('forecast_ref') or {}).get('sha256')!=digest(record)):
        raise ValueError('retained measurement compiler or forecast mismatch')
    output=bundle['output']
    reproduced=evaluate(record,output['horizon_sessions'],retained_packet(output['entry_marks']['asset']),
                        retained_packet(output['entry_marks']['benchmark']),stamp(output['measured_at']),verifier)
    if canonical(reproduced)!=canonical(output):raise ValueError('retained measurement did not reproduce')
    return output


def run(context=None):
    started=datetime.now(timezone.utc);deadline=time.monotonic()+210
    old=optional_json(STATE) or {};after=old.get('next_after')
    if after and not re.fullmatch(re.escape(PREFIX)+r'records/[0-9a-f]{64}\.json',after):raise ValueError('invalid evaluation cursor')
    args={'Bucket':BUCKET,'Prefix':PREFIX+'records/','MaxKeys':100}
    if after:args['StartAfter']=after
    page=s3.list_objects_v2(**args);objects=page.get('Contents',[])
    credential=managed_secret(('POLYGON','POLYGON_KEY','POLYGON_API_KEY','POLY_KEY'),('/justhodl/polygon/api-key',))
    compiler=compiler_identity();verifier=PriceEvidenceVerifier(s3,BUCKET)
    packets={};request_count=0;rows=[];processed=0;last=after;errors=0
    for item in objects:
        if time.monotonic()>deadline or (context and context.get_remaining_time_in_millis()<40000):break
        key=item['Key']
        if not re.fullmatch(re.escape(PREFIX)+r'records/[0-9a-f]{64}\.json',key):raise ValueError('unexpected journal key')
        fid=key.rsplit('/',1)[-1][:-5]
        raw=s3.get_object(Bucket=BUCKET,Key=key)['Body'].read(100001)
        ref={'key':key,'forecast_id':fid,'sha256':hashlib.sha256(raw).hexdigest()}
        deferred=False
        try:
            record=read_record(s3,BUCKET,ref)
            first=(datetime.fromisoformat(record['registration_date_et'])+timedelta(days=1)).date().isoformat()
            end=(datetime.now(timezone.utc).astimezone(EASTERN).date()-timedelta(days=1)).isoformat()
            for horizon in record['protocol']['horizons_sessions']:
                result_key=PREFIX+'measurements/'+fid+'/s'+str(horizon)+'.json'
                previous=optional_json(result_key)
                if previous:
                    output=replay_measurement(previous,record,verifier,compiler)
                    rows.append({'forecast_id':fid,'horizon_sessions':horizon,'status':output['status'],
                                 'key':result_key,'sha256':digest(previous),'replayed':True})
                    continue
                if end<first:
                    rows.append({'forecast_id':fid,'horizon_sessions':horizon,'status':'PENDING_FORWARD_WINDOW'})
                    continue
                needed=[(sym,first,end) for sym in (record['observation']['instrument']['symbol'],'SPY')]
                for cache_key in needed:
                    if cache_key not in packets and request_count<30 and time.monotonic()<deadline:
                        packets[cache_key]=source_packet(*cache_key,credential);request_count+=1
                if any(cache_key not in packets for cache_key in needed):
                    rows.append({'forecast_id':fid,'horizon_sessions':horizon,'status':'DEFERRED_REQUEST_BUDGET'})
                    deferred=True
                    break
                output=evaluate(record,horizon,packets[needed[0]],packets[needed[1]],datetime.now(timezone.utc),verifier)
                result={'forecast_id':fid,'horizon_sessions':horizon,'status':output['status']}
                if output['status']=='MEASURED_PRICE_ONLY':
                    bundle={'contract':CONTRACT,'forecast_id':fid,'forecast_ref':ref,'compiler':compiler,'output':output}
                    result.update(persist_once(s3,BUCKET,result_key,bundle))
                    replay_measurement(bundle,record,verifier,compiler)
                    result['replayed']=True
                rows.append(result)
        except Exception:
            # Keep this forecast in the traversal; do not manufacture a grade.
            errors+=1;rows.append({'forecast_id':fid,'status':'EVIDENCE_OR_REPLAY_REJECTED'})
        if deferred:break
        processed+=1;last=key
    at_end=processed==len(objects) and not page.get('IsTruncated')
    status_counts={status:sum(row['status']==status for row in rows) for status in sorted({row['status'] for row in rows})}
    completed=datetime.now(timezone.utc).isoformat()
    batch={'schema_version':'prospective-outcome-batch.v1','generated_at':completed,'started_at':started.isoformat(),
           'scope':'This bounded traversal batch only; not lifetime totals or independent sample counts',
           'forecasts_checked':processed,'status_counts':status_counts,'results':rows,'compiler':compiler,
           'coverage':{'max_forecasts_per_run':100,'continued_from':after,'reached_end':at_end,
                       'scan_complete_in_this_run':not after and at_end,'request_budget':30,'provider_requests':request_count},
           'archive_checks':verifier.stats,'sizing_eligible':False,'promotion_eligible':False,
           'net_return_pct':None,'portfolio_pnl':None,'evidence_errors':errors}
    batch_ref=persist_once(s3,BUCKET,PREFIX+'evaluation-runs/'+digest(batch)+'.json',batch)
    publish_current(s3,BUCKET,SUMMARY,{**batch,'batch':batch_ref})
    publish_current(s3,BUCKET,STATE,{'generated_at':completed,'next_after':None if at_end else last,'batch':batch_ref})
    return {'statusCode':200,'forecasts_checked':processed,'status_counts':status_counts,'batch':batch_ref,
            'sizing_eligible':False,'legacy_ledger_writes':0}


def lambda_handler(event,context):
    if (event or {}).get('validation_only') is True:
        now=datetime.now(timezone.utc)
        end=(now.astimezone(EASTERN).date()-timedelta(days=1)).isoformat()
        start=(now.astimezone(EASTERN).date()-timedelta(days=14)).isoformat()
        credential=managed_secret(('POLYGON','POLYGON_KEY','POLYGON_API_KEY','POLY_KEY'),('/justhodl/polygon/api-key',))
        packet=source_packet('SPY',start,end,credential)
        if packet is None:raise ValueError('validation market source unavailable')
        selected=marks(packet,'SPY',datetime.now(timezone.utc));verifier=PriceEvidenceVerifier(s3,BUCKET)
        if not selected:raise ValueError('validation market source empty')
        for mark in selected.values():
            if verifier(mark):raise ValueError('validation market evidence rejected')
        return {'statusCode':200,'validation_only':True,'kind':'real_market_source_probe_not_forecast',
                'observation_count':len(selected),'as_of':max(selected),'evidence':packet['evidence'],
                'archive_checks':verifier.stats,'forecast_writes':0,'legacy_ledger_writes':0,'sizing_eligible':False}
    return run(context)
