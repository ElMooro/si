"""Inspect one failed scheduled sweep; never retry requests or alter its control."""
from pathlib import Path
import json,re,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared')]
from ops_report import report
import capital_structure_refresh as refresh
import share_structure_sources as capture
import share_structure_campaign as campaign
from market_runtime_evidence import bounded

RUN='36269293338'


def diagnostic(client):
    state,etag,raw=refresh.load_control(client)
    request=refresh.request_id(RUN)
    if not state or state.get('request_id')!=request or state.get('status')!='failed' or state.get('active_phase')!='part-1':
        raise ValueError('Exact reviewed failed scheduled sweep required')
    plan=json.loads(capture.read(client,state['plan']))
    specs={s['url']:s for s in campaign.specifications(plan,1)}
    batch_key=capture.request_key(request,'batch:1')
    batch=campaign.read_journal(client,batch_key)
    if batch.get('request_id')!=request or batch.get('status')!='failed' or batch.get('plan')!=state['plan']:
        raise ValueError('Exact failed batch required')
    errors=batch.get('source_errors',{});complete=batch.get('captures',{})
    if not errors or not set(errors)<=set(specs) or not set(complete)<=set(specs) or set(errors)&set(complete):
        raise ValueError('Disjoint complete/error source coordinates required')
    result=[]
    for url in sorted(errors):
        item=campaign.read_journal(client,capture.request_key(request,url))
        if item.get('request_id')!=request or item.get('spec')!=specs[url] or item.get('status')!='failed':
            raise ValueError('Failed request identity differs')
        original=item.get('original');body=capture.read(client,original) if original else None
        # Never print provider bodies or exception text: either can echo credentials.
        text=body.decode('utf-8','replace').lower() if body else ''
        symptoms=[name for name,pattern in (
            ('rate_limit',r'too many requests|rate limit|limit reach'),
            ('provider_quota',r'quota|bandwidth|daily limit'),
            ('authorization',r'unauthori|invalid api|forbidden|subscription'),
            ('server_failure',r'internal server|service unavailable|gateway')) if re.search(pattern,text)]
        result.append({'symbol':specs[url]['symbol'],'endpoint':specs[url]['endpoint'],'period':specs[url]['period'],
            'http_status':item.get('http_status'),'error_type':item.get('error_type'),
            'transport_attempted':item.get('transport_attempted'),'requested_at':item.get('requested_at'),
            'received_at':item.get('received_at'),'original':original,'classified_body_symptoms':symptoms})
    again,new_etag,new_raw=refresh.load_control(client)
    if new_etag!=etag or new_raw!=raw:raise ValueError('Control changed during diagnosis')
    return {'run_id':RUN,'request_id':request,'control_sha256':capture.sha(raw),
        'status':state['status'],'completed_phases':state['completed_phases'],
        'planned_batch_sources':len(specs),'complete_captures':len(complete),'failed_requests':result,
        'unattempted_batch_sources':len(specs)-len(complete)-len(errors),
        'provider_requests':0,'native_invocations':0,'control_writes':0,'readiness_writes':0,
        'public_writes':0,'account_reads':0,'notifications_sent':0,'schedules_changed':0,
        'scope':'Read-only diagnosis of retained scheduled-run failures; no recovery or new acquisition.'}


def main():
    with report('ops_6173_capital_refresh_failure') as r:r.kv(**diagnostic(boto3.client('s3',region_name='us-east-1')))


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
