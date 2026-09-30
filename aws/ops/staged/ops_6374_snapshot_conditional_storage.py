"""Install the three narrow snapshot storage guards after native code acceptance.

No current/private/history object reads, invokes, schedule changes or artifact
writes. Bucket-policy change preserves every preceding field and statement.
Configuration writes are not CAS; rechecks cannot cancel pre-admitted requests.
"""
from pathlib import Path
import ast,base64,copy,hashlib,importlib.util,json,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops','aws/ops/checks','aws/shared','scripts','aws/lambdas/justhodl-portfolio-snapshot/source')]
from portfolio_snapshot_ordering import guard_statements,storage_guard
BUCKET='justhodl-dashboard-live'
OWNER='857687956942'
REPORT='aws/ops/reports/latest/ops_6371_private_constitution_policy.md'
REPORT_SHA='bf3e2f2ef21023939d7d5e62464984fcca4df850a029ecb20cd06774e6c9c14a'
POLICY_SHA='4d02ce0ca212f86f0692105c062575d888c4d9907831d1d8c966b05fbfb046d1'


class PolicyUnavailable(RuntimeError):pass


def encoded(value):return json.dumps(value,ensure_ascii=False,allow_nan=False,separators=(',',':')).encode('utf-8')


def document(text):
    def pairs(rows):
        out={}
        for key,value in rows:
            if key in out:raise PolicyUnavailable('Duplicate policy field')
            out[key]=value
        return out
    def constant(_):raise PolicyUnavailable('Nonfinite policy field')
    try:
        if type(text) is not str or not 0<len(text.encode())<=20480:raise ValueError('bound')
        value=json.loads(text,object_pairs_hook=pairs,parse_constant=constant)
        if type(value) is not dict or value.get('Version')!='2012-10-17' or type(value.get('Statement')) is not list:raise ValueError('shape')
        encoded(value);return value
    except (ValueError,TypeError,UnicodeError,RecursionError):raise PolicyUnavailable('Complete reviewed bucket policy required') from None


def original_policy(root=ROOT):
    raw=(root/REPORT).read_bytes()
    if hashlib.sha256(raw).hexdigest()!=REPORT_SHA:raise PolicyUnavailable('Exact retained post-repair policy report required')
    rows=[line.strip('|').split('|') for line in raw.decode().splitlines() if line.startswith('|')]
    names=[cell.strip() for cell in rows[0]]
    if any(len(row)!=len(names) for row in rows):raise PolicyUnavailable('Whole report framing required')
    records=[]
    for row in rows[2:]:
        record={}
        for key,cell in zip(names,row):
            cell=cell.strip()
            if not cell:continue
            try:value=ast.literal_eval(cell)
            except (ValueError,SyntaxError):value=cell
            record[key]=value
        records.append(record)
    after=[row for row in records if row.get('phase')=='after']
    if len(after)!=1:raise PolicyUnavailable('Unique complete post-repair phase required')
    row=after[0];body=base64.b64decode(row['whole_policy_responses_base64'],validate=True)
    if len(body)!=row['whole_policy_responses_bytes'] or hashlib.sha256(body).hexdigest()!=row['whole_policy_responses_sha256']:raise PolicyUnavailable('Whole retained policy response mismatch')
    responses=json.loads(body)
    if type(responses) is not list or len(responses)!=2:raise PolicyUnavailable('Both post-repair responses required')
    policies=[document(row['Policy']) for row in responses]
    if policies[0]!=policies[1] or hashlib.sha256(encoded(policies[0])).hexdigest()!=POLICY_SHA:raise PolicyUnavailable('Exact stable predecessor policy required')
    return policies[0]


def prepare(before):
    after=copy.deepcopy(before)
    rows=after.get('Statement')
    if type(rows) is not list or len(rows)!=31 or any(type(row) is not dict for row in rows):raise PolicyUnavailable('Complete 31-statement predecessor required')
    additions=guard_statements()
    if any(row.get('Sid') in {item['Sid'] for item in additions} for row in rows):raise PolicyUnavailable('Storage guard identifier already present')
    rows.extend(additions)
    if len(encoded(after))>20480:raise PolicyUnavailable('S3 policy byte bound exceeded')
    return after


def read_policy(client):
    response=client.get_bucket_policy(Bucket=BUCKET,ExpectedBucketOwner=OWNER)
    if type(response) is not dict or type(response.get('ResponseMetadata')) is not dict or type(response['ResponseMetadata'].get('HTTPStatusCode')) is not int or response['ResponseMetadata']['HTTPStatusCode']!=200:raise PolicyUnavailable('Successful complete policy response required')
    document(response.get('Policy'));return response


def apply(client,baseline,retain):
    target=prepare(baseline);first=read_policy(client);current=document(first['Policy'])
    if current!=baseline and current!=target:raise PolicyUnavailable('Existing policy differs from reviewed predecessor and target')
    second=read_policy(client);retain('before',[first,second])
    if document(second['Policy'])!=current:raise PolicyUnavailable('Policy changed before write; nothing applied')
    changed=current!=target
    if changed:
        try:client.put_bucket_policy(Bucket=BUCKET,ExpectedBucketOwner=OWNER,Policy=encoded(target).decode())
        except Exception:pass
    after=[read_policy(client),read_policy(client)];retain('after',after)
    if any(document(row['Policy'])!=target for row in after):raise PolicyUnavailable('Exact additive storage policy not confirmed')
    # Exercise the same current code guard using configuration calls only.
    digest=storage_guard(client)
    return {'policy_changed':changed,'preserved_statement_count':31,'added_guard_statements':[row['Sid'] for row in guard_statements()],
        'target_policy_bytes':len(encoded(target)),'target_policy_sha256':hashlib.sha256(encoded(target)).hexdigest(),
        'observed_policy_text_sha256':digest,'native_storage_guard_passed':True,
        'private_reads':0,'private_writes':0,'native_invocations':0,'schedule_changes':0,
        'effective_write_authorization_tested':False,'pre_admitted_requests_cancelled':False,'policy_update_atomic_with_other_writers':False}


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    from check_secrets import findings
    path=ROOT/'aws/ops/staged/ops_6373_snapshot_ordering_runtime_acceptance.py'
    spec=importlib.util.spec_from_file_location('snapshot_native_cutover_acceptance',path);native=importlib.util.module_from_spec(spec);spec.loader.exec_module(native)
    with report('ops_6374_snapshot_conditional_storage') as result:
        native.verify_runtime(result)  # exact package/alias/Worker before any policy write
        baseline=original_policy()
        client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=5,read_timeout=15,retries={'total_max_attempts':1}))
        retired=set(json.loads((ROOT/'tests/security/retired-secret-sha256.json').read_bytes())['sha256'])
        def retain(phase,responses):
            raw=encoded(responses)
            if len(raw)>100000 or findings(raw.decode(),retired):raise PolicyUnavailable('Complete policy evidence retention refused')
            result.kv(phase=phase,whole_policy_responses_base64=base64.b64encode(raw).decode(),whole_policy_responses_bytes=len(raw),whole_policy_responses_sha256=hashlib.sha256(raw).hexdigest())
        result.kv(**apply(client,baseline,retain))


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
