"""Read GitHub release job metadata only; no cloud clients, log bodies or secrets in reports."""
import json, os, urllib.request
from pathlib import Path
from datetime import datetime, timezone
repo=os.environ['GITHUB_REPOSITORY']
if repo!='ElMooro/si': raise RuntimeError('Unexpected repository')
def get(path):
    req=urllib.request.Request('https://api.github.com/repos/'+repo+path,headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2026-03-10'})
    with urllib.request.urlopen(req,timeout=30) as response: return json.load(response)
rows=[]
for run in get('/actions/runs?per_page=20')['workflow_runs']:
    if run['path'] not in {'.github/workflows/run-ops.yml','.github/workflows/deploy-lambdas.yml','.github/workflows/deploy-workers.yml','.github/workflows/pages.yml'}: continue
    row={k:run.get(k) for k in ('id','path','head_sha','status','conclusion','created_at','updated_at')}
    row['jobs']=[]
    for job in get('/actions/runs/'+str(run['id'])+'/jobs')['jobs']:
        item={k:job.get(k) for k in ('id','name','status','conclusion','started_at','completed_at')}
        item['steps']=[{k:step.get(k) for k in ('number','name','status','conclusion','started_at','completed_at')} for step in job.get('steps',[])]
        row['jobs'].append(item)
    rows.append(row)
p=Path('aws/ops/reports/latest/audit_release_progress.json');p.parent.mkdir(parents=True,exist_ok=True)
p.write_text(json.dumps({'observed_at':datetime.now(timezone.utc).isoformat(),'runs':rows},indent=2)+'\n')
print('Recorded metadata for',len(rows),'release workflow runs; no log or application payloads retrieved')
