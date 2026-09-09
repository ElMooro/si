"""Read GitHub release job metadata only; no cloud clients, log bodies or secrets in reports."""
import json, os, re, time, urllib.request, urllib.error
from pathlib import Path
from datetime import datetime, timezone
repo=os.environ['GITHUB_REPOSITORY']
if repo!='ElMooro/si': raise RuntimeError('Unexpected repository')
def get(path):
    req=urllib.request.Request('https://api.github.com/repos/'+repo+path,headers={'Authorization':'Bearer '+os.environ['GH_TOKEN'],'Accept':'application/vnd.github+json','X-GitHub-Api-Version':'2026-03-10'})
    with urllib.request.urlopen(req,timeout=30) as response: return json.load(response)
def failure_sections(raw):
    rows=[]
    for match in re.finditer(r'──── Deploying ([A-Za-z0-9_-]+) ────(.*?)(?=──── Deploying |\Z)',raw,re.S):
        name,part=match.groups()
        if 'Deploy failed for '+name not in part: continue
        row={'function':name,'aws_errors':[{'code':a,'operation':b} for a,b in re.findall(r'An error occurred \(([A-Za-z0-9_.-]+)\) when calling the ([A-Za-z0-9]+) operation',part)],
             'validation_fields':sorted(set(re.findall(r" at '([A-Za-z0-9_.]+)' failed to satisfy",part))),
             'stages':[label for marker,label in [('Updating existing Lambda','existing_function'),('Built ','package_built'),('Invoking pinned ','candidate_invoked'),('live alias promoted','candidate_promoted'),('Setting up EventBridge rule','classic_schedule'),('Setting up EventBridge Scheduler','scheduler'),('Schedule attached','classic_schedule_done')] if marker in part],
             'configuration_errors':[],
             'transport_flags':[flag for marker,flag in [('Read timeout','READ_TIMEOUT'),('Connection was closed','CONNECTION_CLOSED'),('Could not connect','CONNECT_FAILED'),('timed out','TIMEOUT'),('InvalidParameterValue','INVALID_PARAMETER'),('failed to satisfy constraint','CONSTRAINT_FAILED')] if marker in part]}
        for line in part.splitlines():
            offset=line.find('{')
            if offset<0: continue
            try: doc=json.loads(line[offset:])
            except (ValueError,TypeError): continue
            if isinstance(doc,dict) and doc.get('phase')=='lambda_configuration_failed' and doc.get('function')==name:
                row['configuration_errors'].append({key:doc[key] for key in ('phase','function','operation','error_code','exit_code') if key in doc})
        rows.append(row)
    return rows

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
        return {'available':True, 'function_failures':failure_sections(raw),
                'exception_types':sorted(set(re.findall(r'\b[A-Z][A-Za-z]*(?:Error|Exception)\b',raw))),
                'aws_error_codes':sorted(set(re.findall(r'An error occurred \(([A-Za-z0-9_.-]+)\) when calling',raw))),
                'missing_modules':sorted(set(re.findall(r"No module named ['\"]([A-Za-z0-9_.-]+)['\"]",raw))),
                'traceback_locations':[{'path':m[0],'line':int(m[1])} for m in re.findall(r'File "/home/runner/work/si/si/([A-Za-z0-9_./-]+)", line ([0-9]+)',raw)],
                'failed_test_names':sorted(set(re.findall(r'FAIL(?:ED)?:? (test_[A-Za-z0-9_]+)',raw))),
                'javascript_test_locations':sorted(set(re.findall(r'\btests/([A-Za-z0-9_./-]+\.test\.js:[0-9]+(?::[0-9]+)?)',raw))),
                'failed_javascript_test_numbers':sorted(set(int(value) for value in re.findall(r'\bnot ok ([0-9]+) -',raw))),
                'validation_failures':[{'function':m[0],'category':m[1]} for m in re.findall(r'(justhodl-[A-Za-z0-9_-]+) (invocation did not confirm execution of the pinned version|candidate returned an invalid validation envelope|candidate returned FunctionError)',raw)],
                'failed_functions':sorted(set(re.findall(r'Deploy failed for ([A-Za-z0-9_-]+)',raw))),
                'deployed_functions':sorted(set(re.findall(r'✅ ([A-Za-z0-9_-]+) deployed',raw)))}
    except Exception as exc: return {'available':False,'error_type':type(exc).__name__}
# An observer triggered by Run Ops may start before its dispatched release
# finishes. Follow that bounded pipeline so its final failure/success is retained.
# These reads and waits never touch cloud services or application payloads.
follow_paths={'.github/workflows/run-ops.yml','.github/workflows/deploy-lambdas.yml'}
deadline=time.monotonic()+25*60
while True:
    recent=get('/actions/runs?per_page=60')['workflow_runs']
    active=[run for run in recent if run.get('path') in follow_paths and run.get('status') not in ('completed',)]
    if not active or time.monotonic()>=deadline: break
    print('Awaiting completion of',len(active),'release workflows',flush=True)
    time.sleep(20)
# Retain the requested release even after routine nightly runs fill the recent page.
pinned_ids = (34312891397,)
known_ids = {run['id'] for run in recent}
for pinned_id in pinned_ids:
    if pinned_id not in known_ids:
        recent.append(get('/actions/runs/' + str(pinned_id)))
rows=[]
for run in recent:
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
