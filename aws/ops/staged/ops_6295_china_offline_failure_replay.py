"""Replay only China's retained scheduled sources inside an isolated calculator."""
from pathlib import Path
from datetime import datetime, timezone
from collections import Counter
from types import SimpleNamespace
import importlib.util, json, subprocess, sys, traceback, urllib.parse
import boto3

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT / p) for p in ('aws/ops', 'aws/ops/checks', 'aws/lambdas/justhodl-china-liquidity/source')]
from ops_report import report
from market_runtime_evidence import runtime
import china_store as store

FN = 'justhodl-china-liquidity'
BUCKET = 'justhodl-dashboard-live'
DIAGNOSTIC = ROOT / 'docs/audit/2026-09-28/china-execution-diagnostic.json'


class Offline(store.Session):
    def __init__(self, at, predecessors, attempts, originals):
        super().__init__(None, BUCKET, at, predecessors, opener=None, monthly_code=None)
        self.attempts = attempts
        self.originals = originals
        self.used = 0
        self.last_request = None

    def urlopen(self, req, timeout=None):
        self.ready()
        try:
            request = store.identity(req, self.monthly_code)
        except Exception:
            normalized = store.normalized(req)
            parsed = urllib.parse.urlsplit(normalized.full_url)
            self.last_request = {'host':parsed.hostname,'path':parsed.path,'parameter_names':sorted(dict(urllib.parse.parse_qsl(parsed.query)))}
            self.failure = 'Undeclared provider request'
            self.ready()
        self.last_request = {'source_host':request['source_host'],'endpoint':request['endpoint'],'parameter_names':sorted(request['parameters'])}
        if self.used >= len(self.attempts):
            self.failure = 'Replay requested beyond complete retained attempt population'
            self.ready()
        row = self.attempts[self.used]
        if request != row['request']:
            self.failure = 'Retained acquisition request order differs'
            self.ready()
        self.used += 1
        if row['status'] != 'http_response' or row['http_status'] != 200:
            raise store.EvidenceError('Source unavailable; complete original attempt retained')
        return store.Response(self.originals[row['original']['sha256']], row['headers'])


SAFE_REASONS = {'Undeclared provider request','Undeclared native cache read','Unexpected native cache/public writer',
                'Complete dated native packet required','Invalid native structured publication','Complete native head and history required',
                'Retained acquisition request order differs','Replay requested beyond complete retained attempt population',
                'Every predecessor history row must be preserved'}


def calculate_offline(module, predecessors, attempts, originals):
    attempts = sorted(attempts, key=lambda r: r['requested_at'])
    if not attempts:
        raise ValueError('Complete retained acquisition attempts required')
    at = attempts[0]['requested_at']
    store.validate_predecessors(predecessors, at)
    session = Offline(at, predecessors, attempts, originals)
    # Tests load the actual source with managed clients/secrets stubbed out.
    # calculate swaps transport, S3, clock, environment and notification sinks.
    module.os = SimpleNamespace(environ={})
    outcome = {'status':'failed','production_invocations':0,'provider_requests':0,'public_writes':0,'isolated_calculations':1,
               'replay_clock':at,'clock_basis':'First retained attempt, not a reconstruction of the original handler start'}
    try:
        result = store.calculate(module, session, ('offline-placeholder',))
        output = store.publications(predecessors, session.pending, session.review, at, store.compiler_hashes())
        outcome.update(status='offline_calculation_completed',projected_keys=sorted(output),native_result_type=type(result).__name__)
    except Exception as exc:
        outcome.update(error_type=type(exc).__name__,reason=str(exc) if str(exc) in SAFE_REASONS else 'See source frame; raw exception withheld',
                       source_frames=[{'file':Path(f.filename).name,'function':f.name,'line':f.lineno} for f in traceback.extract_tb(exc.__traceback__)])
    outcome.update(attempts_replayed=session.used,retained_attempts=len(attempts),last_request=session.last_request,
                   staged_keys=sorted(session.pending),session_failure=session.failure if session.failure in SAFE_REASONS else None,
                   notifications_suppressed=session.notifications_suppressed if hasattr(session,'notifications_suppressed') else 0)
    return outcome


def main():
    clients = [boto3.client(n,region_name='us-east-1') for n in ('lambda','s3','events','scheduler')]
    with report('ops_6295_china_offline_failure_replay') as r:
        before = runtime(*clients,FN)
        expected = subprocess.check_output(['git','log','-1','--format=%H','--','aws/lambdas/'+FN],cwd=ROOT,text=True).strip()
        if before['receipt'] != {'status':'matched','commit':expected} or before['source_files_checked'] != 4:
            raise ValueError('Exact four-source native package required')
        evidence = json.loads(DIAGNOSTIC.read_bytes())['diagnostic']
        if before != evidence['actual_runtime']:
            raise ValueError('Producer changed since the diagnosed execution')
        predecessors = {key:store.get(clients[1],BUCKET,key) for key in store.KEYS}
        if store.sha(predecessors[store.HEAD]['raw']) != evidence['public_head']['sha256']:
            raise ValueError('Original public head changed; no stale replay diagnosis')
        attempts=[]; originals={}
        for summary in evidence['own_source_evidence']['attempts']:
            digest=summary['manifest_sha256'];key=store.PRIVATE+digest+'.bin'
            found=store.get(clients[1],BUCKET,key)
            if found is None or store.sha(found['raw']) != digest:
                raise ValueError('Retained whole attempt differs')
            row=store.strict(found['raw'])
            if row['requested_at'] != summary['requested_at'] or row['status'] != summary['status']:
                raise ValueError('Diagnosed acquisition identity differs')
            attempts.append(row)
            if row.get('original'):
                raw=store.retained(clients[1],BUCKET,row['original']);originals[row['original']['sha256']]=raw
        path=ROOT/'aws/lambdas'/FN/'tests/run_tests.py'
        spec=importlib.util.spec_from_file_location('china_isolated_diagnostic',path)
        fixture=importlib.util.module_from_spec(spec);spec.loader.exec_module(fixture);fixture.Tests.setUpClass()
        result=calculate_offline(fixture.Tests.module,predecessors,attempts,originals)
        for key,value in predecessors.items():
            if store.get(clients[1],BUCKET,key) != value:
                raise ValueError('Public predecessor changed during isolated diagnosis')
        if runtime(*clients,FN) != before:
            raise ValueError('Runtime changed during diagnosis')
        r.kv(actual_runtime=before,offline_replay=result,retained_originals=len(originals),retained_attempts=len(attempts),
             production_invocations=0,provider_requests=0,public_writes=0,history_writes=0,account_reads=0,consumer_reads=0,schedule_changes=0,
             scope='Actual source code executed only in an isolated test-loaded calculator with retained provider responses and in-memory staged outputs. No production invocation, provider transport, notification, secret load or source-body disclosure.')


if __name__ == '__main__':
    try:main()
    except Exception:sys.exit(1)
