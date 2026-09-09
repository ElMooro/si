"""Read GitHub release job metadata only; no cloud clients, log bodies or secrets in reports."""
import json, os, re, urllib.request, urllib.error
from pathlib import Path
from datetime import datetime, timezone
repo=os.environ['GITHUB_REPOSITORY']
if repo!='ElMooro/si': raise RuntimeError('Unexpected repository')
def get(path):
    req=urllib.request.Request('https://api.github.com/repos/'+repo+path,headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2026-03-10'})
    with urllib.request.urlopen(req,timeout=30) as response: return json.load(response)
def diagnostics(job_id):
    # Never forward GitHub authorization onto a signed log-download redirect.
    class NoRedirect(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl): return None
    req=urllib.request.Request('https://api.github.com/repos/'+repo+'/actions/jobs/'+str(job_id)+'/logs',headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2026-03-10'})
    try:
        try:
            response=urllib.request.build_opener(NoRedirect).open(req,timeout=30)
        except urllib.error.HTTPError as exc:
            if exc.code!=302: return {'available':False,'http_status':exc.code}
            location=exc.headers.get('Location','')
            if not location.startswith('https://'): return {'available':False}
            response=urllib.request.urlopen(location,timeout=30)
        with response: raw=response.read(20_000_000).decode('utf-8','replace')
        return {'available':True,
                'exception_types':sorted(set(re.findall(r'\b[A-Z][A-Za-z]*(?:Error|Exception)\b',raw))),
                'missing_modules':sorted(set(re.findall(r"No module named ['\"]([A-Za-z0-9_.-]+)['\"]",raw))),
                'traceback_locations':[{'path':m[0],'line':int(m[1])} for m in re.findall(r'File "/home/runner/work/si/si/([A-Za-z0-9_./-]+)", line ([0-9]+)',raw)],
                'failed_test_names':sorted(set(re.findall(r'FAIL(?:ED)?:? (test_[A-Za-z0-9_]+)',raw))),
                'failed_functions':sorted(set(re.findall(r'Deploy failed for ([A-Za-z0-9_-]+)',raw))),
                'deployed_functions':sorted(set(re.findall(r'✅ ([A-Za-z0-9_-]+) deployed',raw)))}
    except Exception as exc: return {'available':False,'error_type':type(exc).__name__}
rows=[]
for run in get('/actions/runs?per_page=20')['workflow_runs']:
    if run['path'] not in {'.github/workflows/run-ops.yml','.github/workflows/deploy-lambdas.yml','.github/workflows/deploy-workers.yml','.github/workflows/pages.yml'}: continue
    row={k:run.get(k) for k in ('id','path','head_sha','status','conclusion','created_at','updated_at')}
    row['jobs']=[]
    for job in get('/actions/runs/'+str(run['id'])+'/jobs')['jobs']:
        item={k:job.get(k) for k in ('id','name','status','conclusion','started_at','completed_at')}
        item['steps']=[{k:step.get(k) for k in ('number','name','status','conclusion','started_at','completed_at')} for step in job.get('steps',[])]
        if job.get('conclusion')=='failure': item['failure_diagnostics']=diagnostics(job['id'])
        row['jobs'].append(item)
    rows.append(row)
p=Path('aws/ops/reports/latest/audit_release_progress.json');p.parent.mkdir(parents=True,exist_ok=True)
p.write_text(json.dumps({'observed_at':datetime.now(timezone.utc).isoformat(),'runs':rows},indent=2)+'\n')
print('Recorded metadata for',len(rows),'release workflow runs; only allowlisted diagnostic codes retained; no application payloads retrieved')
