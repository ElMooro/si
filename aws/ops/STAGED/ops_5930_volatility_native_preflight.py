"""Audit the volatility runtime and privately retain original FRED research inputs.

No Lambda invocation, private-account read, notification or portfolio write.
Only the already configured FRED credential is resolved in runner memory.
Whole licensed originals stay protected; the report contains schema/coverage
metadata, hashes and compact current observations, never credentials or history.
"""
from pathlib import Path
from datetime import datetime, timezone, timedelta, date
from decimal import Decimal
import ast, base64, csv, hashlib, io, json, os, re, subprocess, sys, time
import urllib.request, urllib.error, urllib.parse, zipfile
import boto3

ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'aws/ops/checks')]
from ops_report import report
from release_package_evidence import shared_imports

BUCKET='justhodl-dashboard-live'
PRIVATE='audit-private/20260909-originals/volatility-research/'
FN='justhodl-vol-surface'
MAX_BYTES=24*1024*1024


def sha(raw): return hashlib.sha256(raw).hexdigest()
def encoded(d): return json.dumps(d,sort_keys=True,separators=(',',':'),allow_nan=False).encode()
def bounded(stream):
    try: raw=stream.read(MAX_BYTES+1)
    finally: stream.close()
    if len(raw)>MAX_BYTES: raise ValueError('Response exceeds reviewed bound')
    return raw


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl): return None


def fetch(url):
    with urllib.request.build_opener(NoRedirect()).open(urllib.request.Request(url,headers={
            'User-Agent':'JustHodl-volatility-research-audit/1.0'}),timeout=30) as response:
        return bounded(response)


def denied(url):
    try:
        with urllib.request.urlopen(urllib.request.Request(url,method='HEAD'),timeout=20): return False
    except urllib.error.HTTPError as exc: return exc.code in (401,403,404)


def retain(s3,raw):
    key=PRIVATE+sha(raw)+'.bin'
    try:
        s3.put_object(Bucket=BUCKET,Key=key,Body=raw,ContentType='application/octet-stream',CacheControl='no-store',IfNoneMatch='*')
    except Exception as exc:
        if str(getattr(exc,'response',{}).get('Error',{}).get('Code')) not in ('412','409','PreconditionFailed','ConditionalRequestConflict'): raise
    assert bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body'])==raw
    return {'key':key,'sha256':sha(raw),'bytes':len(raw)}


def main():
    with report('ops_5930_volatility_native_preflight') as r:
        r.kv(engine_invocations=0,private_account_reads=0,notifications_sent=0,portfolio_writes=0,paid_ai_calls=0)
        lam=boto3.client('lambda',region_name='us-east-1');s3=boto3.client('s3',region_name='us-east-1')
        live=lam.get_function(FunctionName=FN);cfg=live['Configuration']
        archive=fetch(live['Code']['Location'])
        assert base64.b64encode(hashlib.sha256(archive).digest()).decode()==cfg['CodeSha256']
        directory=ROOT/'aws/lambdas'/FN/'source'
        files=[ROOT/p for p in subprocess.check_output(['git','ls-files',str(directory.relative_to(ROOT))],cwd=ROOT,text=True).splitlines()]
        expected={p.relative_to(directory).as_posix():p for p in files}
        expected.update({p.name:p for p in shared_imports(ROOT,files) if not (directory/p.name).exists()})
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            for name,path in expected.items(): assert z.read(name)==path.read_bytes(),'Runtime differs; review '+name
        receipt=json.loads(fetch('https://justhodl.ai/data/ops/releases/'+FN+'.json'))
        assert receipt['code_sha256']==cfg['CodeSha256']
        events=boto3.client('events',region_name='us-east-1');rules=[]
        for page in events.get_paginator('list_rule_names_by_target').paginate(TargetArn=cfg['FunctionArn']):
            for name in page.get('RuleNames',[]):
                rule=events.describe_rule(Name=name)
                rules.append({k:rule.get(k) for k in ('Name','State','ScheduleExpression')})
        r.kv(runtime={'receipt_commit':receipt['commit'],'code_sha256':cfg['CodeSha256'],
            'all_packaged_sources_match':True,'files_checked':len(expected),'source_bytes':len((directory/'lambda_function.py').read_bytes()),
            'memory_mb':cfg['MemorySize'],'timeout_s':cfg['Timeout']},classic_rules=rules)
        retained=[]
        for key in ('data/vol-surface.json','data/vol-surface-history.json'):
            raw=bounded(s3.get_object(Bucket=BUCKET,Key=key)['Body']);old=json.loads(raw)
            assert json.loads(fetch('https://justhodl.ai/'+key))==old
            ref=retain(s3,raw);retained.append(ref)
            r.kv(predecessor={'public_key':key,'protected_original':ref,'keys':sorted(old),'generated_at':old.get('generated_at'),
                'history_first_date':old.get('first_date'),'history_last_date':old.get('last_date'),'history_rows':old.get('n_days')})
        # Inspect literal catalog only. Never import the old handler or its watchlist helpers.
        catalog={}
        for item in ast.parse((directory/'lambda_function.py').read_text(encoding='utf-8')).body:
            if isinstance(item,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='VOL_INDICES' for t in item.targets):
                catalog={v['fred']:v for v in ast.literal_eval(item.value).values()}
        assert len(catalog)==14
        credential=os.environ.get('FRED_KEY') or os.environ.get('FRED_API_KEY')
        if not credential:
            credential=boto3.client('ssm',region_name='us-east-1').get_parameter(Name='/justhodl/fred/api-key',WithDecryption=True)['Parameter']['Value']
        assert credential
        evaluation=datetime.now(timezone.utc).date();sources={};coverage=[]
        for sid in sorted(catalog):
            assert re.fullmatch('[A-Za-z0-9_]+',sid)
            docs={}
            for kind,path in (('definition','series'),('observations','series/observations')):
                q={'series_id':sid,'file_type':'json','realtime_start':str(evaluation),'realtime_end':str(evaluation)}
                if kind=='observations': q.update(observation_start=str(evaluation-timedelta(days=3660)),observation_end=str(evaluation),units='lin',output_type=1,sort_order='asc',limit=100000,offset=0)
                public_url='https://api.stlouisfed.org/fred/'+path+'?'+urllib.parse.urlencode(q)
                time.sleep(0.6)
                try: raw=fetch(public_url+'&api_key='+urllib.parse.quote(credential,safe=''))
                except Exception as exc: raise ValueError('FRED '+kind+' '+sid+' unavailable: '+type(exc).__name__) from None
                if credential.encode() in raw: raise ValueError('Credential reflected; original rejected')
                doc=json.loads(raw);ref=retain(s3,raw);retained.append(ref)
                sources[kind+':'+sid]={**ref,'request_url':public_url,'acquired_at':datetime.now(timezone.utc).isoformat(),'provider':'fred'}
                docs[kind]=doc
            ds=docs['definition']['seriess'];obs=docs['observations'];rows=obs['observations']
            assert len(ds)==1 and ds[0]['id']==sid and obs['count']==len(rows) and obs['offset']==0 and obs['sort_order']=='asc'
            assert ds[0]['units']=='Index' and ds[0]['frequency_short']=='D'
            assert len({o['date'] for o in rows})==len(rows)
            numeric=[o for o in rows if o['value']!='.']
            assert all(Decimal(o['value']).is_finite() for o in numeric)
            notes=ds[0].get('notes','')
            coverage.append({'series_id':sid,'title':ds[0]['title'],'unit':ds[0]['units'],'frequency':ds[0]['frequency_short'],
                'source_updated_at':ds[0].get('last_updated'),'rows':len(rows),'numeric_rows':len(numeric),'null_rows':len(rows)-len(numeric),
                'first_date':rows[0]['date'],'last_date':rows[-1]['date'],'latest_numeric_date':numeric[-1]['date'],
                'latest_numeric_index_points':numeric[-1]['value'],'weekend_rows':sum(date.fromisoformat(o['date']).weekday()>=5 for o in numeric),
                'publisher_notes_present':bool(notes),
                'definition_original':sources['definition:'+sid],'observations_original':sources['observations:'+sid]})
        # Direct publisher files replace neither source nor legacy output during preflight.
        publisher={}
        for symbol in ('VVIX','SKEW'):
            url='https://cdn-api.cboe.com/api/global/us_indices/daily_prices/'+symbol+'_History.csv'
            raw=fetch(url);ref=retain(s3,raw);retained.append(ref)
            rows=list(csv.reader(io.StringIO(raw.decode('utf-8-sig'))))
            assert rows[0]==['DATE',symbol] and 252<len(rows)<15000,'Publisher CSV schema differs'
            parsed=[]
            for row in rows[1:]:
                assert len(row)==2
                day=datetime.strptime(row[0],'%m/%d/%Y').date();value=Decimal(row[1])
                assert day<=evaluation and value.is_finite() and value>=0
                assert not parsed or day>parsed[-1][0],'Duplicate/unordered publisher date'
                parsed.append((day,value))
            publisher[symbol]={**ref,'source_url':url,'provider':'Cboe','acquired_at':datetime.now(timezone.utc).isoformat(),
                'header':rows[0],'rows':len(parsed),'first_date':str(parsed[0][0]),'last_date':str(parsed[-1][0]),
                'latest_index_points':str(parsed[-1][1]),'first_availability_verified':False}
            r.kv(publisher_source=publisher[symbol])
        manifest={'contract':'volatility-source-preflight.v1' ,'generated_at':datetime.now(timezone.utc).isoformat(),
            'evaluation_date':str(evaluation),'sources':sources,'publisher':publisher,'coverage':coverage,'runtime_code_sha256':cfg['CodeSha256']}
        manifest_ref=retain(s3,encoded(manifest));retained.append(manifest_ref)
        # Representative original and whole preflight manifest must remain inaccessible anonymously.
        for ref in (retained[0],sources['observations:VIXCLS'],manifest_ref):
            assert denied('https://'+BUCKET+'.s3.amazonaws.com/'+ref['key']) and denied('https://justhodl.ai/'+ref['key'])
        r.kv(source_manifest=manifest_ref,originals_retained=len(retained),private_archive_anonymous_denied=True)
        for entry in coverage: r.kv(series=entry)
        r.kv(next_work='Native index measurements, same-date tenor/cross-section comparisons, explicit statistical windows and original replay. Fourteen FRED identities plus official VVIX/SKEW retained. No implied default/tail probabilities, unqualified score, portfolio action or producer invocation.')


if __name__=='__main__':
    try: main()
    except Exception:
        print('Credit preflight failed. Inspect the committed report; no producer was invoked.')
        sys.exit(1)
