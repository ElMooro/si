"""Complete daily accounting capture/qualification on the AWS-authorized runner.

No current research packet, Lambda, account, signal or notification access.
Only a complete source replay and independent arithmetic check can advance
the protected ready pointer consumed by the native publisher.
"""
from datetime import datetime, timezone
import gc, json, time
import financial_statement_campaign as campaign
import financial_statement_source as acquisition
import statement_research_source as source
import statement_research_v2 as model
import statement_research_store_v2 as store
import statement_research_arithmetic_v2 as arithmetic
import statement_identity_capture as identity_capture
import statement_producer as producer

BUCKET=campaign.BUCKET
UNIVERSE='screener/data.json'


def now():return datetime.now(timezone.utc).isoformat()


def original_universe(client):
    response=client.get_object(Bucket=BUCKET,Key=UNIVERSE)
    body=store.bounded(response['Body']);parsed=source.strict(body)
    if not isinstance(parsed,dict):raise ValueError('Complete screener universe required')
    choices=[parsed[k] for k in ('rows','stocks','data') if isinstance(parsed.get(k),list)]
    if len(choices)!=1 or not all(isinstance(row,dict) for row in choices[0]):
        raise ValueError('Unambiguous complete universe records required')
    labels=[row.get('symbol') or row.get('ticker') for row in choices[0]]
    if len(labels)!=500 or any(not isinstance(s,str) for s in labels) or len(set(labels))!=500:
        raise ValueError('The entire reviewed 500-name universe is required; never silently truncate')
    for label in labels:source.spec(label,source.ENDPOINTS[0],'annual')
    retained=campaign.retain(client,body)
    return sorted(labels),retained,{'source_key':UNIVERSE,'original':retained,'etag':response['ETag'],
        'captured_at':now(),'source_generated_at':parsed.get('generated_at'),'index_membership_verified':False}


def advance_ready(client,document,old_etag):
    body=source.encoded(document)
    try:
        client.put_object(Bucket=BUCKET,Key=producer.READY,Body=body,ContentType='application/json',CacheControl='no-store',
            **({'IfMatch':old_etag} if old_etag else {'IfNoneMatch':'*'}))
    except Exception as exc:
        if producer.conflict(exc):return False
        raise
    if store.bounded(client.get_object(Bucket=BUCKET,Key=producer.READY)['Body'])!=body:
        raise ValueError('Qualified ready pointer readback differs')
    return True


def run(client,request_id,credential,remaining_seconds=3000,clock=now,fetch=None,fetch_identity=None):
    if not isinstance(credential,str) or not credential:raise ValueError('Existing managed provider credential required')
    if remaining_seconds<1800:raise ValueError('Full source capture and qualification budget required')
    deadline=time.monotonic()+remaining_seconds-120
    status=campaign.request_key(request_id,'refresh')
    try:existing=source.strict(store.bounded(client.get_object(Bucket=BUCKET,Key=status)['Body']))
    except Exception as exc:
        if not producer.missing(exc):raise
        existing=None
    if existing:
        if existing.get('status')!='complete':raise ValueError('Accounting refresh already attempted; inspect and adopt retained work explicitly')
        return existing['result']
    progress={'request_id':request_id,'contract':'statement-source-refresh.v2','status':'claimed','started_at':clock()}
    campaign.journal(client,status,progress,True)
    try:
        try:
            old=client.get_object(Bucket=BUCKET,Key=producer.READY);old_bytes=store.bounded(old['Body']);old_etag=old['ETag']
            previous_ready=source.strict(old_bytes);progress['previous_ready']=campaign.retain(client,old_bytes)
        except Exception as exc:
            if not producer.missing(exc):raise
            old_etag,previous_ready=None,None
        labels,universe,universe_capture=original_universe(client);symbols=frozenset(labels)
        specs=[acquisition.request_spec(label,endpoint,period,symbols) for label in labels
            for period in ('annual','quarter') for endpoint in acquisition.ENDPOINTS]
        baseline=campaign.retain(client,source.encoded({'contract':'statement-refresh-universe.v1','captures':{UNIVERSE:universe_capture}}))
        base={'contract':'financial-statement-complete-source-campaign.v1','request_id':request_id,
            'generated_at':clock(),'universe':universe,'baseline':baseline,'reported_symbols':labels,'planned_sources':len(specs),
            'snapshot_atomic':False,'index_membership_verified':False,'historical_availability_verified':False,
            'max_request_starts_per_second':2.5,'workers':3,'accounting_calculations_qualified':False,
            'forecast_qualified':False,'sizing_qualified':False}
        identity_ref=(fetch_identity or (lambda:identity_capture.capture(client,request_id,clock)))()
        progress.update(status='capturing',universe=universe,baseline=baseline,identity_capture=identity_ref)
        campaign.journal(client,status,progress)
        rate=campaign.Rate()
        def capture(spec):
            if time.monotonic()>=deadline:raise TimeoutError('Accounting refresh budget exhausted; no new source request')
            return (fetch or (lambda s:campaign.capture(client,request_id,s,symbols,credential,rate,clock)))(spec)
        def checkpoint(complete,errors):
            progress.update(status='source_failure' if errors else 'capturing',
                captures={key:value['retained_capture'] for key,value in complete.items()},source_errors=errors)
            campaign.journal(client,status,progress)
            print(json.dumps({'refresh':request_id,'complete_sources':len(complete),'planned_sources':len(specs),'source_errors':len(errors)}),flush=True)
        captures=campaign.collect(specs,{},capture,checkpoint)
        counts={'complete_sources':len(captures),'provider_rows':sum(v['inventory']['rows'] for v in captures.values()),
            'provider_bytes':sum(v['original']['bytes'] for v in captures.values()),
            'unavailable_empty_responses':sum(v['inventory']['rows']==0 for v in captures.values())}
        manifest=campaign.retain(client,source.encoded({**base,'status':'complete','completed_at':clock(),
            'captures':{key:value['retained_capture'] for key,value in captures.items()},'counts':counts}))
        progress.update(status='sources_complete',source_manifest=manifest);campaign.journal(client,status,progress)
        if time.monotonic()>=deadline:raise TimeoutError('Qualification reserve exhausted; captured originals retained')
        def deadline_check():
            if time.monotonic()>=deadline:raise TimeoutError('Accounting qualification budget exhausted')
        read=store.reader(client,BUCKET,before_read=deadline_check)
        compiled=model.compile_output(manifest,identity_ref,read)
        proof=arithmetic.verify(manifest,identity_ref,compiled,read)
        reference=store.retain(client,BUCKET,manifest,identity_ref,compiled)
        expected=source.sha(source.encoded(compiled['packet']))
        progress.update(status='retained',replay=reference,qualification=proof);campaign.journal(client,status,progress)
        del compiled;gc.collect()
        # Exact immutable source bodies are rechecked by replay. Reusing this
        # bounded reader avoids fetching unchanged originals from S3 twice.
        replayed=store.replay(reference,read)
        if source.sha(source.encoded(replayed['packet']))!=expected:raise ValueError('Accounting qualification replay differs')
        ready={'contract':'financial-statement-qualified-ready.v2','status':'qualified','request_id':request_id,
            'qualified_at':clock(),'source_manifest_sha256':manifest['sha256'],'generated_at':replayed['packet']['generated_at'],
            'replay':reference,'qualification':proof,'fmp_requests':len(specs),'sec_identity_requests':1,'provider_requests':len(specs)+1,'producer_invocations':0,'consumer_invocations':0,
            'private_account_reads':0,'signal_writes':0,'notifications_sent':0,'paid_ai_calls':0}
        if previous_ready and source.clock(previous_ready['generated_at'])>=source.clock(ready['generated_at']):
            result={'ready_advanced':False,'reason':'source_time_rollback','replay':reference}
        else:
            if time.monotonic()>=deadline:raise TimeoutError('Ready publication reserve reached')
            advanced=advance_ready(client,ready,old_etag)
            result={'ready_advanced':advanced,'reason':'qualified' if advanced else 'concurrent_ready_publication','replay':reference}
        campaign.journal(client,status,{**progress,'status':'complete','completed_at':clock(),'result':result})
        return result
    except Exception as exc:
        campaign.journal(client,status,{**progress,'status':'failed','error_type':type(exc).__name__})
        raise
