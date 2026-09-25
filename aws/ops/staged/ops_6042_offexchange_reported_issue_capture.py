"""Complete a new June capture with reported-issue identity after 6041 diagnosis.

Adopt five completed 6040 partitions without recollection. The new June query
sorts by symbol, issue name and firm CRD. Different issue descriptions remain
separate; there is no inferred corporate-action or security-master mapping.
"""
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor
from fractions import Fraction
import sys,time,subprocess,urllib.request
import boto3
ROOT=Path(__file__).resolve().parents[3];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged')]
from ops_report import report
import offexchange_measurements as model
import ops_6040_offexchange_complete_partitions as old
from ops_6039_offexchange_retained_measurement_review import independent_ratio,REF
from ops_5975_etf_constituent_source_preflight import runtime
from ops_5998_option_population_retained_acceptance import denied_with_retry
prior=old.prior;source=old.source;BUCKET=old.BUCKET;PRIVATE=old.PRIVATE
REQUEST='chatgpt-offexchange-reported-issue-capture-6042'
STATUS=PRIVATE+'requests/'+prior.sha(REQUEST.encode())+'.json'
PART={'dataset':'monthlySummary','code':'OTC_M_SMBL_FIRM','period':'2026-06-01','tier':'NMS'}
FAILED_NAME='/'.join(PART.values())

def grain(row,part):
    base=old.grain(row,part);issue=model.issue_name(row.get('issueName'))
    return (base[0],issue,*base[1:])

def key_for(label):return PRIVATE+'requests/'+prior.sha((REQUEST+':'+label).encode())+'.json'
def spec(offset):
    request=old.specification(PART,offset);request['body']['sortFields']=['issueSymbolIdentifier','issueName','firmCRDNumber'];return request

def capture(s3,label,request,deadline,opener=None):
    key=key_for(label);source.write_status(s3,key,{'request_id':REQUEST,'source_id':label,'status':'claimed','specification':request},True)
    try:result=source.fetch(s3,request,opener or urllib.request.build_opener(prior.NoRedirect()),deadline)
    except Exception as exc:
        source.write_status(s3,key,{'request_id':REQUEST,'source_id':label,'status':'failed','error_type':type(exc).__name__});raise
    source.write_status(s3,key,{'request_id':REQUEST,'source_id':label,'status':'complete','capture':result})
    return {**result,'journal_key':key}

def collect(s3,deadline,fetcher=capture):
    pages=[];offset=0;expected=None;seen=set();bytes_read=0;state_key=key_for('june-partition')
    source.write_status(s3,state_key,{'request_id':REQUEST,'status':'claimed','partition':PART},True)
    try:
        for index in range(old.MAX_PAGES):
            if time.monotonic()>=deadline:raise TimeoutError('Partition capture deadline')
            request=spec(offset);result=fetcher(s3,'page:'+str(index),request,deadline);pages.append(result)
            source.write_status(s3,state_key,{'request_id':REQUEST,'status':'capturing','partition':PART,'pages':pages})
            bytes_read+=(result.get('original') or {}).get('bytes',0)
            if bytes_read>old.MAX_PARTITION_BYTES:raise ValueError('Whole partition byte bound exceeded')
            rows,count=old.rows_and_count(s3,result,request)
            if expected is None:expected=count['reported_total']
            if count['reported_total']!=expected:raise ValueError('Reported total changed during pagination')
            for row in rows:
                key=grain(row,PART)
                if key in seen:raise ValueError('Duplicate reported issue/firm grain across pages')
                seen.add(key)
            offset=count['next_offset']
            if count['reported_end_reached']:break
        else:raise ValueError('Pagination bound before reported end')
        if len(seen)!=expected:raise ValueError('Unique source grain does not reconcile')
        recheck=fetcher(s3,'first-page-recheck',spec(0),deadline);pages.append(recheck)
        _,count=old.rows_and_count(s3,recheck,spec(0))
        if count['reported_total']!=expected or recheck['original']!=pages[0]['original']:raise ValueError('First page changed')
        result={'partition':PART,'pages':pages,'rows':len(seen),'reported_total':expected,'records_reconciled':True,
            'grain':['issueSymbolIdentifier','issueName','firmCRDNumber'],'snapshot_atomic':False,'first_page_recheck_matched':True,
            'provider_requests':len(pages),'journal_key':state_key,'provider_bytes':sum(v['original']['bytes'] for v in pages)}
        source.write_status(s3,state_key,{'request_id':REQUEST,'status':'complete','result':result});return result
    except Exception as exc:
        source.write_status(s3,state_key,{'request_id':REQUEST,'status':'failed','partition':PART,'pages':pages,'reason':str(exc),'error_type':type(exc).__name__});raise

def qualify(s3,partitions):
    weekly=[];monthly=[];checks={}
    for name,result in partitions.items():
        rows=[];seen=set();part=result['partition']
        for page in result['pages'][:-1]:
            raw=prior.original(s3,page['original']);doc=model.strict(raw)
            count=model.page(raw,page['headers'],page['body']['offset'],page['body']['limit']);assert count['reported_total']==result['reported_total']
            parsed=model.weekly(raw,part['code'],part['period'],part['tier']) if part['dataset']=='weeklySummary' else model.monthly(raw,part['period'],part['tier'])
            for src,row in zip(doc,parsed):
                key=grain(src,part);assert key not in seen;seen.add(key)
                prefix='totalWeekly' if part['dataset']=='weeklySummary' else 'totalMonthly'
                assert Fraction(str(src[prefix+'ShareQuantity']))==Fraction(row['shares'])
                assert Fraction(str(src[prefix+'TradeCount']))==Fraction(row['trades'])
                independent_ratio(row['average_shares_per_reported_trade'],Fraction(row['shares']),Fraction(row['trades']))
                row['source_original']=page['original'];rows.append(row)
        assert len(rows)==len(seen)==result['rows']
        if part['dataset']=='weeklySummary':weekly.extend(rows)
        else:monthly.extend(rows)
        checks[name]={'rows':len(rows),'grain_checked':True,'snapshot_atomic':False}
    joined=model.join_weekly(weekly);concentrations=model.concentration(monthly,records_reconciled=True)
    for row in joined:
        if row['ats'] and row['non_ats']:
            total=Fraction(row['ats']['shares'])+Fraction(row['non_ats']['shares']);assert total==Fraction(row['reported_offexchange_shares'])
            independent_ratio(row['ats_pct_of_reported_offexchange'],Fraction(row['ats']['shares'])*100,total)
    for row in concentrations:
        total=sum((Fraction(v['shares']) for v in row['source_rows']),Fraction(0));assert Fraction(row['reported_non_ats_shares'])==total
        known=sum((Fraction(v['shares'])**2 for v in row['source_rows'] if v['firm_crd']!='0'),Fraction(0))
        hidden=sum((Fraction(v['shares']) for v in row['source_rows'] if v['firm_crd']=='0'),Fraction(0))
        independent_ratio(row['reported_activity_hhi_lower_bound'],known*10000,total**2)
        independent_ratio(row['reported_activity_hhi_upper_bound'],(known+hidden**2)*10000,total**2)
    ambiguous={}
    for row in concentrations:ambiguous.setdefault((row['symbol'],row['month_start'],row['tier']),set()).add(row['reported_issue_name'])
    ambiguous=[{'symbol':k[0],'month_start':k[1],'tier':k[2],'reported_issue_names':sorted(names)} for k,names in ambiguous.items() if len(names)>1]
    return {'partitions':checks,'weekly_rows':len(weekly),'joined_weekly_rows':len(joined),'matched_weekly_legs':sum(bool(v['ats'] and v['non_ats']) for v in joined),
        'monthly_rows':len(monthly),'monthly_reported_issue_period_rows':len(concentrations),'ambiguous_symbol_periods':ambiguous,
        'parser_sha256':prior.sha(Path(model.__file__).read_bytes()),'security_master_identity_verified':False,'signals_qualified':False}

def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6042_offexchange_reported_issue_capture') as r:
        for test in ('test_offexchange_measurements.py','test_offexchange_issue_capture.py'):
            subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
        try:existing=prior.strict(prior.get(s3,STATUS))
        except Exception as exc:
            if not prior.missing(exc):raise
            existing=None
        if existing:
            assert existing['status']=='complete','Never repeat an incomplete request'
            ref=existing['manifest'];manifest=prior.strict(prior.original(s3,ref));qualification=qualify(s3,manifest['partitions'])
            assert qualification==manifest['qualification']
        else:
            old_status=prior.strict(prior.get(s3,old.STATUS));assert set(old_status['errors'])=={FAILED_NAME} and len(old_status['results'])==5
            actual=runtime(lam,s3,events,scheduler,prior.FUNCTION)
            source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'claimed','adopted_request':old.REQUEST,'runtime':actual},True)
            started=time.monotonic();june=collect(s3,started+180);partitions={**old_status['results'],FAILED_NAME:june}
            source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'captured','partitions':partitions})
            qualification=qualify(s3,partitions);assert runtime(lam,s3,events,scheduler,prior.FUNCTION)==actual
            manifest={'contract':'offexchange-reported-issue-partitions.v1','request_id':REQUEST,'generated_at':prior.now(),'inventory':REF,'runtime':actual,
                'partitions':partitions,'qualification':qualification,'adopted_request':old.REQUEST,'diagnosis':'ops_6041_offexchange_partition_diagnosis',
                'elapsed_seconds':round(time.monotonic()-started,3),'market_coverage_complete':False,
                'scope':'Six explicit period/tier/category partitions. Reported issue names stay separate; no security-master, latest-period, ownership or signal qualification.'}
            ref=prior.retain(s3,prior.encoded(manifest));source.write_status(s3,STATUS,{'request_id':REQUEST,'status':'complete','manifest':ref})
        protected={STATUS,ref['key']}
        for part in manifest['partitions'].values():
            protected.add(part['journal_key'])
            for page in part['pages']:protected.update((page['original']['key'],page['journal_key']))
        def deny(key):assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,sorted(protected)):pass
        r.kv(manifest=ref,qualification=qualification,elapsed_seconds=manifest['elapsed_seconds'],protected_artifacts_checked=len(protected),
            provider_requests_this_run=0 if existing else manifest['partitions'][FAILED_NAME]['provider_requests'],
            engine_invocations=0,public_head_writes=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
