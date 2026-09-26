"""Complete original credit replay and independent Bond Desk comparison checks."""
from pathlib import Path
from fractions import Fraction
import hashlib,json,subprocess,sys
import boto3
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/ops/staged','aws/shared','aws/lambdas/justhodl-credit-stress/source')]
from ops_report import report
from market_runtime_evidence import runtime,bounded
from ops_6168_bond_desk_baseline import PRIVATE,BUCKET,FUNCTION,retain
import credit_research_store as upstream
import bond_credit_candidate as candidate
import retained_access_evidence as access

BASELINE='36534c93f149632f65e0fcd49566e3396d9ff76f500b52478223f0ca66f82d2e'


def baseline_read(s3,ref):
    if ref['key']!=PRIVATE+ref['sha256']+'.bin':raise ValueError('Exact protected baseline required')
    raw=bounded(s3.get_object(Bucket=BUCKET,Key=ref['key'])['Body'])
    if len(raw)!=ref['bytes'] or hashlib.sha256(raw).hexdigest()!=ref['sha256']:raise ValueError('Whole baseline differs')
    return raw


def original_check(packet,view,read):
    checks=[]
    for name,row in view['comparisons'].items():
        if row['status']!='dated_descriptive_comparison':raise ValueError('Candidate comparison not available for this acceptance fixture')
        values=[]
        for component in row['components']:
            observations=json.loads(read(component['originals']['observations']['key']))['observations']
            definition=json.loads(read(component['originals']['definition']['key']))['seriess'][0]
            index=component['original_row_index'];original=observations[index]
            if definition['id']!=component['series_id'] or definition['units']!='Percent' or original['date']!=component['observation_date'] or index!=len(observations)-1:raise ValueError('Exact latest original coordinate differs')
            value=Fraction(original['value'])
            if value!=Fraction(component['value_percent_decimal']):raise ValueError('Original value differs')
            values.append(value)
        expected=100*(values[0]-values[1])
        if expected!=Fraction(row['value_decimal']) or expected!=Fraction(str(packet['comparisons'][name]['value_bps'])):raise ValueError('Original rational basis-point difference differs')
        checks.append({'comparison':name,'observation_date':row['observation_date'],'original_rows_checked':2,'rational_basis_point_difference':row['value_decimal']})
    return {'contract':'bond-credit-original-check.v1','comparisons_checked':len(checks),'original_rows_checked':sum(r['original_rows_checked'] for r in checks),'checks':checks,'passed':True,
        'historical_point_in_time_verified':False,'forecast_qualified':False,'sizing_eligible':False}


def main():
    s3,lam,events,scheduler=(boto3.client(n,region_name='us-east-1') for n in ('s3','lambda','events','scheduler'))
    with report('ops_6171_bond_credit_candidate') as r:
        subprocess.run([sys.executable,str(ROOT/'tests/test_bond_credit_candidate.py')],cwd=ROOT,check=True)
        before=runtime(lam,s3,events,scheduler,FUNCTION)
        base=json.loads(baseline_read(s3,{'key':PRIVATE+BASELINE+'.bin','sha256':BASELINE,'bytes':62355}))
        source=base['captures']['data/credit-stress.json']['original'];raw=baseline_read(s3,source);packet=json.loads(raw)
        observations={};source_reader=upstream.reader(s3,BUCKET)
        def read(key):
            value=source_reader(key);ref={'sha256':hashlib.sha256(value).hexdigest(),'bytes':len(value)}
            if key in observations and observations[key]!=ref:raise ValueError('Immutable source changed')
            observations[key]=ref;return value
        replayed=upstream.replay(packet['replay'],read)
        if candidate.encode(replayed)!=candidate.encode({k:v for k,v in packet.items() if k!='replay'}):raise ValueError('Typed complete credit replay differs')
        view=candidate.project(raw,base['finished_at']);proof=original_check(packet,view,read)
        evidence={'contract':'bond-credit-candidate-acceptance.v1','baseline':BASELINE,'source':source,'evaluated_at':base['finished_at'],
            'candidate_compiler':retain(s3,Path(candidate.__file__).read_bytes()),'candidate':retain(s3,candidate.encode(view)),
            'independent_proof':retain(s3,candidate.encode(proof)),'complete_original_replay_passed':True,'source_artifacts':observations}
        ref=retain(s3,candidate.encode(evidence));protected=[ref,*[evidence[k] for k in ('candidate_compiler','candidate','independent_proof')]]
        privacy=access.summarize([access.check(v['key']) for v in protected])
        if runtime(lam,s3,events,scheduler,FUNCTION)!=before:raise ValueError('Bond Desk runtime or schedule changed')
        originals={k:v for k,v in observations.items() if k.startswith(upstream.PRIVATE)}
        r.kv(acceptance=ref,checks=proof,source=source,source_run=packet['replay'],source_artifacts=len(observations),
            protected_originals=len(originals),protected_original_bytes=sum(v['bytes'] for v in originals.values()),
            reviewed_credit_series=len(replayed['measurements']),complete_original_replay_passed=True,**privacy,
            native_invocations=0,provider_requests=0,public_writes=0,account_reads=0,notifications_sent=0,schedules_changed=0,
            candidate_only=True,forecast_qualified=False,
            scope='Typed original-bound credit comparisons and independent rational checks; native Bond Desk credit integration remains pending.')
        if not privacy['all_denied']:raise ValueError('Credit candidate must remain private')


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
