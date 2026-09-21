"""Preserve complete public composite dependencies; no producer invocation."""
from pathlib import Path
from datetime import datetime,timezone
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
import json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/staged'),str(ROOT/'aws/ops/checks')]
from ops_report import report
import ops_5987_options_dependency_preflight as baseline
from ops_5998_option_population_retained_acceptance import denied_with_retry
from release_package_evidence import check_packages
evidence=baseline.evidence;BUCKET=baseline.BUCKET
PRODUCERS=('justhodl-massive-signals','justhodl-polygon-fx-regime','justhodl-polygon-futures-curves')
CONSUMERS=tuple('justhodl-'+n for n in ('alpha-score','best-setups','convergence-radar','master-ranker','pump-mechanics','theme-rotation'))
PACKETS=('data/massive-signals.json','data/massive-capability.json','data/polygon-options.json','data/polygon-ratios.json',
    'data/polygon-fx-regime.json','data/polygon-futures-curves.json','flow-data.json',
    'data/dealer-gex.json','data/option-population-research.json','data/polygon-options-flow.json','data/option-flow-research.json',
    'data/etf-desk.json','data/etf-desk-research.json','data/provider-fund-flow-research.json','data/etf-holdings-research.json','data/flow-lookthrough.json')
CLOCKS=('observed_at','observation_date','as_of','generated_at','source_capture_completed_at')


def describe(key,doc):
    assert key in PACKETS and isinstance(doc,dict),'Reviewed research packet and object shape required'
    out={'root_fields':sorted(doc),**{k:doc.get(k) for k in ('contract','version',*CLOCKS)},
        'authority':{k:doc.get(k) for k in ('forecast_qualified','calls_eligible','sizing_eligible','execution_eligible')},
        'replay_reference':doc.get('replay'),'replay_verified_by_this_inventory':False}
    if key=='data/massive-signals.json':
        rows=doc.get('tickers') or {};assert isinstance(rows,dict)
        out.update(ticker_names=sorted(rows),ticker_count=len(rows),ranked_count=len(doc.get('top_prepump')or[]),
            row_fields=dict(Counter(k for row in rows.values() if isinstance(row,dict) for k in row)),
            row_clocks={k:sum(isinstance(r,dict) and r.get(k) is not None for r in rows.values()) for k in CLOCKS},
            source_inventory=doc.get('sources'),market_fields=sorted(doc.get('market')or{}))
    if key=='data/polygon-fx-regime.json':
        rows=doc.get('pair_data')or{};assert isinstance(rows,dict)
        out.update(pair_names=sorted(rows),pair_count=len(rows),
            row_fields=dict(Counter(k for row in rows.values() if isinstance(row,dict) for k in row)),
            row_clocks={k:sum(isinstance(r,dict) and r.get(k) is not None for r in rows.values()) for k in CLOCKS})
    if key=='data/polygon-futures-curves.json':
        out.update(status=doc.get('status'),identity_ok=doc.get('identity_ok'),
            declared_products=doc.get('n_products'),products_with_data=doc.get('n_products_with_data'),
            product_names=sorted(doc.get('product_data')or{}),identity_inventory=doc.get('identity'))
    return out


def capture(s3,key):
    assert key in PACKETS,'Public research allowlist only'
    try:obj=s3.get_object(Bucket=BUCKET,Key=key)
    except Exception as exc:
        if evidence.code(exc) not in ('NoSuchKey','404'):raise
        return None,{'status':'missing'}
    raw=evidence.bounded(obj['Body'],16*1024*1024)
    ref=baseline.retain(s3,raw)
    ref.update(source_key=key,acquired_at=datetime.now(timezone.utc).isoformat(),
        last_modified=obj['LastModified'].astimezone(timezone.utc).isoformat(),etag=obj['ETag'],version_id=obj.get('VersionId'))
    return ref,{'status':'retained','bytes':len(raw),**describe(key,json.loads(raw))}


def main():
    s3=boto3.client('s3',region_name='us-east-1');lam=boto3.client('lambda',region_name='us-east-1')
    events=boto3.client('events',region_name='us-east-1');scheduler=boto3.client('scheduler',region_name='us-east-1')
    with report('ops_6001_massive_dependency_preflight') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_massive_dependency_preflight.py')],cwd=ROOT,check=True)
        runtimes={fn:evidence.runtime(lam,s3,events,scheduler,fn) for fn in PRODUCERS};r.kv(producer_runtimes=runtimes)
        packages=check_packages(lam,ROOT,CONSUMERS);r.kv(consumer_packages=packages)
        refs={};inventory={};total=0
        for key in PACKETS:
            ref,info=capture(s3,key);inventory[key]=info
            if ref:refs[key]=ref;total+=ref['bytes']
            assert total<=128*1024*1024,'Complete dependency byte bound exceeded'
        manifest={'contract':'massive-dependency-preflight.v1','generated_at':datetime.now(timezone.utc).isoformat(),
            'producer_runtimes':runtimes,'consumer_packages':packages,'packets':refs,'inventory':inventory,
            'scope':'Complete research bodies and actual source packages. Stored replay references are inventoried, not independently replayed by this baseline.'}
        ref=baseline.retain(s3,evidence.encoded(manifest));protected=[ref['key'],*(v['key'] for v in refs.values())]
        r.kv(retained_manifest=ref,retained_packets=len(refs),retained_bytes=total,packet_inventory=inventory,
            consumer_packages_match=all(p['pass'] for p in packages))
        def deny(key):
            assert denied_with_retry('https://justhodl.ai/'+key) and denied_with_retry('https://'+BUCKET+'.s3.amazonaws.com/'+key)
        with ThreadPoolExecutor(max_workers=4) as pool:
            for _ in pool.map(deny,protected):pass
        r.kv(protected_artifacts_checked=len(protected),originals_anonymously_denied=True,engine_invocations=0,
            provider_requests=0,private_account_reads=0,paid_ai_calls=0,notifications_sent=0,portfolio_writes=0,schedules_changed=0)
        assert all(p['pass'] for p in packages),'Inspect retained package mismatches before changing consumers'


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
