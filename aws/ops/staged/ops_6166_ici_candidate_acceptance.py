"""Reconstruct both complete retained ICI releases without new source requests."""
from pathlib import Path
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/staged','aws/ops/checks')]
from ops_report import report
from market_runtime_evidence import bounded
from ops_6165_ici_original_baseline import retain,PRIVATE,BUCKET,FUNCTION
import ici_research_candidate as candidate
import verify_ici_research as independent
import retained_access_evidence as access

BASELINE='16f1ae5e41fc156ed4c09c61e2acb5e170c3ebaa2d615103c0bbbe031c0755c2'

def read(s3,ref):
    if ref['key']!=PRIVATE+ref['sha256']+'.bin':raise ValueError('Protected baseline identity required')
    raw=bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('Complete retained original differs')
    return raw

def rule_view(events,name):
    row=events.describe_rule(Name=name)
    return {key:row.get(key) for key in ('Name','Arn','State','ScheduleExpression','Description')}

def main():
    subprocess.run([sys.executable,str(ROOT/'tests/test_ici_research_candidate.py')],cwd=ROOT,check=True)
    s3,lam,events=(boto3.client(name,region_name='us-east-1') for name in ('s3','lambda','events'))
    with report('ops_6166_ici_candidate_acceptance') as r:
        baseline=json.loads(read(s3,{'key':PRIVATE+BASELINE+'.bin','sha256':BASELINE,'bytes':7691}))
        before=lam.get_function_configuration(FunctionName=FUNCTION)
        expected=baseline['predecessor']['runtime']
        if any(before.get(k)!=v for k,v in expected.items()):raise ValueError('ICI predecessor changed')
        rules={row['name']:rule_view(events,row['name']) for row in baseline['predecessor']['schedules'] if row['kind']=='EventBridge rule'}
        if len(rules)!=1 or any(row['State']!='DISABLED' for row in rules.values()):raise ValueError('Preserved disabled rule required')
        sources={};schema={}
        for name in candidate.SOURCES:
            entry=baseline['sources'][name]
            if entry.get('http_status')!=200:raise ValueError('Whole successful original release required')
            raw=read(s3,entry['original']);sources[name]={'raw':raw,'acquired_at':entry['received_at']}
            tables=independent.cells(raw)
            schema[name]=[{'table_index':i,'rows':len(rows),'widths':[len(row) for row in rows],
                'header':rows[0] if rows else [],'row_labels':[row[0] if row else '' for row in rows[1:]]} for i,rows in enumerate(tables)]
        r.kv(source_table_structure=schema)
        view=candidate.compile_releases(sources,baseline['finished_at'])
        proof=independent.verify({k:v['raw'] for k,v in sources.items()},view)
        if proof['reconciliation_issues']:raise ValueError('Source measurements do not reconcile within published rounding')
        ref=retain(s3,json.dumps(view,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
        proof_ref=retain(s3,json.dumps(proof,sort_keys=True,separators=(',',':'),allow_nan=False).encode())
        privacy=access.summarize([access.check(ref['key']),access.check(proof_ref['key'])])
        after=lam.get_function_configuration(FunctionName=FUNCTION)
        if any(after.get(k)!=v for k,v in expected.items()) or any(rule_view(events,name)!=value for name,value in rules.items()):raise ValueError('Predecessor or schedule changed')
        r.kv(baseline_manifest=BASELINE,candidate=ref,independent_proof=proof_ref,checks=proof,
            mmf_latest={k:view['mmf'][k] for k in ('date','total_b','wow_b','govt_b','prime_b','tax_exempt_b')},
            flow_latest={'date':view['long_term']['classes']['total']['date'],
                         'total_w_m':view['long_term']['classes']['total']['latest_w_m'],
                         'equity_4w_m':view['long_term']['equity_sum_4w_m']},
            source_dates={k:{'release_date':v['release_date'],'observation_dates':v['observation_dates'],'unit':v['unit']} for k,v in view['sources'].items()},
            **privacy,provider_requests=0,native_invocations=0,public_writes=0,history_writes=0,account_reads=0,notifications_sent=0,
            schedules_changed=0,candidate_only=True,source_release_vintages_verified=False,forecast_qualified=False,
            scope='Complete retained-release parsing and independent arithmetic. No engine migration or native publication is performed.')
        if not privacy['all_denied']:raise ValueError('Candidate evidence must remain private')

if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
