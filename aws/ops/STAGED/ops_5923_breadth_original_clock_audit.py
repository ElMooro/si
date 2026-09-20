"""Read retained native market responses to diagnose the rejected time-of-day rule.

No producer invocation, provider request, source mutation or private account read.
Only aggregate timestamp/shape counts from this engine's protected source archive
are reported; complete licensed price records stay protected.
"""
from pathlib import Path
from collections import Counter
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from zoneinfo import ZoneInfo
import json, sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/lambdas/justhodl-market-internals/source')]
from ops_report import report
import breadth_research_model as model
import breadth_research_store as store
RUN='data/breadth-research/runs/2afad1ac241b022981b96d7433ed33fd418177fe7f88731f32b3ba6158dde65d.json'


def main():
    with report('ops_5923_breadth_original_clock_audit') as r:
        client=boto3.client('s3',region_name='us-east-1');read=store.reader(client,'justhodl-dashboard-live')
        raw=read(RUN);manifest=json.loads(raw);assert RUN==store.PREFIX+'runs/'+model.sha(raw)+'.json'
        inputs=store.checked(manifest['input'],'inputs',read)
        def one(item):
            day,entry=item;ref=entry['evidence'];raw=read(ref['key'])
            assert model.sha(raw)==ref['sha256'] and len(raw)==ref['bytes']
            doc=json.loads(raw);times=Counter();dates=Counter();millis=Counter();rows=doc['results']
            for row in rows:
                instant=datetime.fromtimestamp(row['t']/1000,ZoneInfo('America/New_York'))
                dates[instant.date().isoformat()]+=1;times[instant.strftime('%H:%M:%S')]+=1;millis[instant.microsecond]+=1
            return {'requested':day,'rows':len(rows),'count_fields_match':doc['queryCount']==doc['resultsCount']==len(rows),
                'native_date_mismatches':sum(n for date,n in dates.items() if date!=day),
                'native_et_times':dict(times),'microsecond_counts':dict(millis),
                'nonmidnight_rows':sum(n for clock,n in times.items() if clock!='00:00:00'),
                'adjusted':doc.get('adjusted'),'provider_status':doc.get('status')}
        with ThreadPoolExecutor(max_workers=4) as pool:rows=list(pool.map(one,sorted(inputs['sources'].items())))
        times=Counter();statuses=Counter()
        for row in rows:times.update(row['native_et_times']);statuses[row['provider_status']]+=1
        summary={'sessions':len(rows),'rows':sum(row['rows'] for row in rows),
            'native_date_mismatches':sum(row['native_date_mismatches'] for row in rows),
            'nonmidnight_rows':sum(row['nonmidnight_rows'] for row in rows),'native_et_times':dict(times),
            'all_source_counts_match':all(row['count_fields_match'] for row in rows),
            'all_split_adjusted':all(row['adjusted'] is True for row in rows),'provider_statuses':dict(statuses)}
        r.kv(original_run=RUN,original_bytes_verified=True,temporal_summary=summary,
            representative_sessions=[rows[0],rows[len(rows)//2],rows[-1]],engine_invocations=0,
            provider_requests=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0)
        assert len(rows)==253 and summary['native_date_mismatches']==0 and summary['all_source_counts_match']


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
