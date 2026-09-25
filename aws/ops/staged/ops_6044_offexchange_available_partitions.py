"""Retain FINRA's advertised partitions before choosing native release periods."""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor,as_completed
import sys,urllib.request,urllib.error
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
import offexchange_research_model as model
import ops_6038_offexchange_journalled_source_inventory as source
from ops_5998_option_population_retained_acceptance import denied_with_retry
prior=source.prior;BUCKET=source.BUCKET;REQUEST='chatgpt-offexchange-available-partitions-6044'
STATUS=model.PRIVATE+'requests/'+model.sha(REQUEST.encode())+'.json'

def inspect(raw,dataset):
    doc=model.strict(raw)
    if not isinstance(doc,dict):raise ValueError('Partition object required')
    fields=doc.get('partitionFields');rows=doc.get('availablePartitions')
    expected=['weekStartDate','tierIdentifier'] if dataset=='weeklySummary' else ['monthStartDate','tierIdentifier']
    if fields!=expected or not isinstance(rows,list) or len(rows)>20000:raise ValueError('Partition schema or count differs')
    values={};other_fields=set();seen=set()
    for row in rows:
        other_fields.update(set(row)-{'partitions'});part=row.get('partitions')
        if not isinstance(part,list) or len(part)!=2 or any(not isinstance(v,str) for v in part):raise ValueError('Partition dimensions differ')
        model.measures.day(part[0]);seen.add(tuple(part));values.setdefault(part[1],set()).add(part[0])
    return {'root_fields':sorted(doc),'partition_fields':fields,'rows':len(rows),'unique_partitions':len(seen),'extra_row_fields':sorted(other_fields),
        'by_tier':{k:{'count':len(v),'earliest':min(v),'latest_ten':sorted(v,reverse=True)[:10]} for k,v in sorted(values.items())},
        'sample_rows':rows[:2],'category_coverage_verified':False}

def collect(s3,dataset):
    url='https://api.finra.org/partitions/group/otcMarket/name/'+dataset;started=prior.now()
    request=urllib.request.Request(url,headers={'Accept':'application/json','User-Agent':'JustHodl-offexchange-source-audit/1.0'})
    opener=urllib.request.build_opener(prior.NoRedirect())
    try:response=opener.open(request,timeout=25)
    except urllib.error.HTTPError as exc:response=exc
    http=response.status;headers={k.lower():v for k,v in response.headers.items() if k.lower() in source.HEADERS};raw=source.read_response(response)
    if raw is None:raise ValueError('Whole partition-discovery response exceeds bound')
    capture={'url':url,'requested_at':started,'received_at':prior.now(),'http_status':http,'headers':headers,'original':source.retain_response(s3,raw)}
    try:capture['inventory']=inspect(raw,dataset)
    except Exception as exc:capture['inventory']={'error_type':type(exc).__name__,'reason':str(exc)}
    key=model.PRIVATE+'requests/'+model.sha((REQUEST+':'+dataset).encode())+'.json'
    source.write_status(s3,key,{'request_id':REQUEST,'dataset':dataset,'status':'complete','capture':capture},True)
    return capture,key

def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_6044_offexchange_available_partitions') as r:
        try:existing=prior.strict(prior.get(s3,STATUS))
        except Exception as exc:
            if not prior.missing(exc):raise
            existing=None
        if existing:
            assert existing['status']=='complete','Never repeat an incomplete discovery request'
            captures=existing['captures'];journals=existing['journals']
        else:
            source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'claimed'},True);captures={};journals={};errors={}
            with ThreadPoolExecutor(max_workers=2) as pool:
                jobs={pool.submit(collect,s3,dataset):dataset for dataset in ('weeklySummary','monthlySummary')}
                for future in as_completed(jobs):
                    name=jobs[future]
                    try:captures[name],journals[name]=future.result()
                    except Exception as exc:errors[name]=type(exc).__name__
                    source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'capturing','captures':captures,'journals':journals,'errors':errors})
            r.kv(capture_errors=errors,captures=captures);assert not errors
            source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'complete','captures':captures,'journals':journals})
        protected={STATUS,*journals.values(),*(v['original']['key'] for v in captures.values())}
        for key in sorted(protected):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        r.kv(captures=captures,protected_artifacts_checked=len(protected),provider_requests_this_run=0 if existing else 2,
            engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
        assert all(v['http_status']==200 and 'error_type' not in v['inventory'] for v in captures.values())
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
