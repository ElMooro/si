"""Bounded original option evidence using existing sources, without producers."""
from pathlib import Path
from datetime import datetime, timezone
from decimal import Decimal
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json, subprocess, sys, time
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/shared')]
from ops_report import report
from managed_secret import managed_secret
import option_snapshot_capture as source
import ops_5987_options_dependency_preflight as baseline

BUCKET=baseline.BUCKET
PREFIX=baseline.PREFIX
BASELINE={'key':PREFIX+'b0227bf01533cd334d0cf160a7d491a9a82b13b92d55993e9ad7650ae19ff52e.bin',
    'sha256':'b0227bf01533cd334d0cf160a7d491a9a82b13b92d55993e9ad7650ae19ff52e','bytes':25668}
PROBE=('SPY','NVDA','NET')
CBOE_PROBE=('SPY','NVDA')
ATTEMPT=PREFIX+'ops-5988-originals-attempt.json'


def checked(client,ref):
    assert ref['key']==PREFIX+ref['sha256']+'.bin'
    raw=baseline.evidence.bounded(client.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
    assert len(raw)==ref['bytes'] and source.sha(raw)==ref['sha256']
    return raw


def summarize_chain(client,chain):
    rows=[];expected=source.next_url(source.initial_url(chain['underlying']),chain['underlying'])
    seen=set();stop=None
    for index,page in enumerate(chain['pages']):
        assert stop is None, 'Unexpected source page after terminal response'
        assert page['page']==index+1 and page['request_url']==expected
        assert page['request_sha256']==source.sha(expected.encode()) and expected not in seen
        seen.add(expected)
        if page['status']!='received':
            assert page['status'] in ('time_budget','transport_or_body_failure') and page['original'] is None
            stop=page['status'];continue
        raw=checked(client,page['original'])
        if page['http_status']!=200:stop='provider_http_failure';continue
        try:doc=source.decode(raw)
        except (ValueError,UnicodeDecodeError):doc=None
        if (not isinstance(doc,dict) or doc.get('status') not in ('OK','DELAYED')
                or not isinstance(doc.get('results'),list) or len(doc['results'])>250):
            stop='invalid_provider_envelope';continue
        rows.extend(doc['results'])
        if not doc.get('next_url'):
            stop='complete_returned_pagination';continue
        try:expected=source.next_url(doc['next_url'],chain['underlying'])
        except ValueError:stop='invalid_pagination_address';continue
        if expected in seen:stop='pagination_cycle'
    if stop is None:
        assert len(chain['pages'])==source.MAX_PAGES, 'Unexplained source truncation'
        stop='page_limit'
    assert stop==chain['stop'] and chain['pagination_complete']==(stop=='complete_returned_pagination')
    result=source.summary(rows)
    result.update(pages=len(chain['pages']),stop=chain['stop'],pagination_complete=chain['pagination_complete'],
        capture_is_atomic=False,exchange_chain_completeness_verified=False,
        started_at=chain['started_at'],completed_at=chain['completed_at'])
    return result


def cboe_summary(doc):
    assert isinstance(doc,dict) and isinstance(doc.get('data'),dict)
    data=doc['data'];rows=data.get('options')
    assert isinstance(rows,list)
    valid=[r for r in rows if isinstance(r,dict)]
    def value(v):return str(v) if isinstance(v,Decimal) else v
    identities=[r.get('option') for r in valid if isinstance(r.get('option'),str)]
    return {'root_fields':sorted(doc),'data_fields':sorted(data),
        'root_timestamp_raw':value(doc.get('timestamp')),'data_timestamp_raw':value(data.get('timestamp')),
        'underlying_fields':{k:value(data.get(k)) for k in ('symbol','current_price','last_trade_price','last_trade_time')},
        'rows':len(rows),'object_rows':len(valid),'option_fields':dict(Counter(k for r in valid for k in r)),
        'numeric_states':{key:dict(Counter(source.numeric_state(r,key) for r in valid))
            for key in ('open_interest','volume','iv','gamma','delta','bid','ask')},
        'duplicate_option_id_rows':sum(n-1 for n in Counter(identities).values() if n>1),
        'first_option':identities[0] if identities else None,'last_option':identities[-1] if identities else None,
        'sample_rows':[{k:value(v) for k,v in r.items() if k in ('option','open_interest','volume','iv','gamma','delta','bid','ask','last_trade_time','last_trade_price','timestamp')} for r in valid[:2]],
        'dealer_inventory_observed':False,'contract_multiplier_certified':False,'independent_metric_clocks_certified':False}


def main():
    s3=boto3.client('s3',region_name='us-east-1')
    with report('ops_5988_option_originals_qualification') as r:
        for test in ('test_option_snapshot_capture.py','test_option_originals_qualification.py'):
            subprocess.run([sys.executable,str(ROOT/'tests'/test)],cwd=ROOT,check=True)
        old=json.loads(checked(s3,BASELINE))
        for ref in old['packets'].values():checked(s3,ref)
        secret=managed_secret(('POLYGON_KEY','POLYGON_API_KEY','POLY_KEY'),('/justhodl/polygon/api-key',))
        assert secret, 'Existing runner-managed source configuration unavailable'
        start=time.monotonic();deadline=start+300
        budget={'requests':0,'bytes':0,'max_requests':len(PROBE)*source.MAX_PAGES+len(CBOE_PROBE),'max_bytes':128*1024*1024}
        attempt={'contract':'option-originals-attempt.v1','started_at':source.now(),'status':'collecting',
            'baseline':BASELINE,'option_snapshots':{},'cboe_snapshots':{},'budget':budget}
        try:s3.put_object(Bucket=BUCKET,Key=ATTEMPT,Body=baseline.evidence.encoded(attempt),ContentType='application/json',CacheControl='no-store',IfNoneMatch='*')
        except Exception as exc:
            if baseline.evidence.code(exc) in ('PreconditionFailed','ConditionalRequestConflict','409','412'):
                raise RuntimeError('Original audit already attempted; inspect its retained checkpoint, do not recollect') from None
            raise
        def save():s3.put_object(Bucket=BUCKET,Key=ATTEMPT,Body=baseline.evidence.encoded(attempt),ContentType='application/json',CacheControl='no-store')
        def retain(raw):return baseline.retain(s3,raw)
        for symbol in PROBE:
            def checkpoint(chain):attempt['option_snapshots'][symbol]=chain;save()
            source.collect(symbol,secret,deadline,retain,budget,checkpoint)
        del secret
        for symbol in CBOE_PROBE:
            url='https://cdn.cboe.com/api/global/delayed_quotes/options/'+symbol+'.json'
            meta,raw=source.request(url,'',32*1024*1024,deadline,retain,budget)
            meta['request_url']=url;attempt['cboe_snapshots'][symbol]=meta;save()
        option_summaries={s:summarize_chain(s3,c) for s,c in attempt['option_snapshots'].items()}
        cboe_summaries={}
        for symbol,meta in attempt['cboe_snapshots'].items():
            if meta['status']=='received' and meta['http_status']==200:
                cboe_summaries[symbol]=cboe_summary(source.decode(checked(s3,meta['original'])))
            else:cboe_summaries[symbol]={'status':meta['status'],'http_status':meta['http_status']}
        compiler=retain(Path(source.__file__).read_bytes())
        manifest={'contract':'option-originals-qualification.v1','generated_at':source.now(),'baseline':BASELINE,
            'option_snapshots':attempt['option_snapshots'],'cboe_snapshots':attempt['cboe_snapshots'],
            'option_summary':option_summaries,'cboe_summary':cboe_summaries,'compiler':compiler,
            'provider_requests':budget['requests'],'source_bytes':budget['bytes'],
            'complete_collection_seconds':round(time.monotonic()-start,3),
            'scope':'Unfiltered returned option chains for three explicit probes and two public delayed CBOE chains. No dealer-position, prediction or sizing qualification.'}
        manifest_ref=retain(baseline.evidence.encoded(manifest))
        attempt.update(status='retained',manifest=manifest_ref,completed_at=source.now());save()
        protected={ATTEMPT,manifest_ref['key'],compiler['key'],BASELINE['key']}
        for chain in attempt['option_snapshots'].values():
            protected.update(p['original']['key'] for p in chain['pages'] if p.get('original'))
        protected.update(p['original']['key'] for p in attempt['cboe_snapshots'].values() if p.get('original'))
        def check(key):
            assert baseline.evidence.denied('https://justhodl.ai/'+key)
            assert baseline.evidence.denied('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=6) as pool:list(pool.map(check,sorted(protected)))
        r.kv(retained_manifest=manifest_ref,option_summary=option_summaries,cboe_summary=cboe_summaries,
            provider_requests=budget['requests'],source_bytes=budget['bytes'],complete_collection_seconds=manifest['complete_collection_seconds'],
            protected_artifacts_checked=len(protected),originals_anonymously_denied=True,
            engine_invocations=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
