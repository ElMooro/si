"""Read-only whole S3 configuration capture before snapshot publication cutover.

No object reads/HEAD/listing, producer calls, policy writes, account content,
provider requests or schedule changes. Preserve complete SDK-decoded settings
and the unmodified policy text in both observations; exclude only transport
ResponseMetadata when checking stability, retaining it in the original capture.
"""
from datetime import date, datetime, timezone
from pathlib import Path
import base64,hashlib,json,math,sys
ROOT=Path(__file__).resolve().parents[3]
sys.path[:0]=[str(ROOT/'aws/ops'),str(ROOT/'scripts')]
BUCKET='justhodl-dashboard-live'
ACCOUNT='857687956942'
METHODS={
 'get_bucket_policy':None,
 'get_bucket_versioning':None,
 'get_public_access_block':'NoSuchPublicAccessBlockConfiguration',
 'get_bucket_replication':'ReplicationConfigurationNotFoundError',
 'get_bucket_ownership_controls':'OwnershipControlsNotFoundError',
 'get_bucket_lifecycle_configuration':'NoSuchLifecycleConfiguration',
 'get_bucket_encryption':'ServerSideEncryptionConfigurationNotFoundError',
}
LIMIT=2_000_000


class ConfigurationUnavailable(RuntimeError):pass


def tagged(value):
    """Lossless JSON encoding of SDK value kinds, including lifecycle dates."""
    if type(value) is dict:
        if any(type(k) is not str for k in value):raise ConfigurationUnavailable('Configuration key type refused')
        return ['object',[[key,tagged(child)] for key,child in value.items()]]
    if type(value) is list:return ['array',[tagged(child) for child in value]]
    if isinstance(value,datetime):
        if value.tzinfo is None:raise ConfigurationUnavailable('Undated configuration timezone refused')
        return ['datetime',value.isoformat()]
    if type(value) is date:return ['date',value.isoformat()]
    if value is None:return ['null']
    if type(value) is bool:return ['boolean',value]
    if type(value) is int:return ['integer',str(value)]
    if type(value) is float:
        if not math.isfinite(value):raise ConfigurationUnavailable('Nonfinite configuration value refused')
        return ['float',value.hex()]
    if type(value) is str:return ['string',value]
    raise ConfigurationUnavailable('Configuration SDK type refused')


def encoded(value):
    try:raw=json.dumps(tagged(value),ensure_ascii=False,allow_nan=False,separators=(',',':')).encode('utf-8')
    except (TypeError,ValueError,UnicodeError,RecursionError):raise ConfigurationUnavailable('Complete configuration encoding unavailable') from None
    if len(raw)>LIMIT:raise ConfigurationUnavailable('Whole configuration exceeds reviewed retention bound')
    return raw


def collect(client):
    captured={}
    for method,absent in METHODS.items():
        try:
            response=getattr(client,method)(Bucket=BUCKET,ExpectedBucketOwner=ACCOUNT)
        except Exception as exc:
            code=getattr(exc,'response',{}).get('Error',{}).get('Code')
            if absent is not None and code==absent:
                # Absence is an explicit result, not an empty invented setting.
                captured[method]={'status':'absent','error_code':absent}
                continue
            raise ConfigurationUnavailable('Required storage configuration unavailable: '+method) from None
        if type(response) is not dict:raise ConfigurationUnavailable('Complete SDK configuration object required')
        metadata=response.get('ResponseMetadata')
        if type(metadata) is not dict or type(metadata.get('HTTPStatusCode')) is not int or metadata['HTTPStatusCode']!=200:raise ConfigurationUnavailable('Successful complete configuration response required')
        encoded(response)
        captured[method]={'status':'present','response':response}
    encoded(captured)
    return captured


def stable_view(capture):
    result={}
    for method,row in capture.items():
        result[method]={**row}
        if row['status']=='present':result[method]['response']={k:v for k,v in row['response'].items() if k!='ResponseMetadata'}
    return result


def assess(first,second):
    if set(first)!=set(METHODS) or set(second)!=set(METHODS):raise ConfigurationUnavailable('Complete storage setting inventory required')
    # Preserve original dictionary order in tagged evidence, but compare decoded
    # settings by type and value independent of SDK mapping iteration order.
    def unordered(value):
        if type(value) is dict:return {key:unordered(value[key]) for key in sorted(value)}
        if type(value) is list:return [unordered(item) for item in value]
        return value
    if encoded(unordered(stable_view(first)))!=encoded(unordered(stable_view(second))):raise ConfigurationUnavailable('Storage configuration changed during capture')
    policy=first['get_bucket_policy']['response'].get('Policy')
    if type(policy) is not str or not policy or len(policy.encode('utf-8'))>20480:raise ConfigurationUnavailable('Complete existing bucket policy required')
    def pairs(rows):
        out={}
        for key,value in rows:
            if key in out:raise ConfigurationUnavailable('Duplicate storage policy field')
            out[key]=value
        return out
    def constant(_):raise ValueError('Nonfinite storage policy value')
    try:document=json.loads(policy,object_pairs_hook=pairs,parse_constant=constant)
    except (ValueError,UnicodeError,RecursionError):raise ConfigurationUnavailable('Invalid complete storage policy') from None
    if type(document) is not dict or type(document.get('Statement')) not in (list,dict):raise ConfigurationUnavailable('Existing storage policy statement inventory required')
    encoded(document)
    statements=document['Statement'] if type(document['Statement']) is list else [document['Statement']]
    if any(type(row) is not dict for row in statements):raise ConfigurationUnavailable('Invalid storage policy statement')
    return {'policy_bytes':len(policy.encode('utf-8')),'policy_sha256':hashlib.sha256(policy.encode('utf-8')).hexdigest(),
            'policy_statement_count':len(statements),'configuration_stable_across_two_reads':True,
            'object_reads':0,'object_head_requests':0,'object_lists':0,'private_reads':0,
            'native_invocations':0,'policy_changes':0,'schedule_changes':0,
            'conditional_write_enforcement_verified':False}


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    from check_secrets import findings
    retired=set(json.loads((ROOT/'tests/security/retired-secret-sha256.json').read_bytes())['sha256'])
    with report('ops_6370_snapshot_storage_configuration') as result:
        client=boto3.client('s3',region_name='us-east-1',config=Config(connect_timeout=5,read_timeout=15,retries={'total_max_attempts':2}))
        first,second=collect(client),collect(client)
        # Raw policy and every SDK-returned field remain in the tagged capture;
        # original request metadata is retained even though it varies by read.
        capture={'contract':'snapshot-storage-configuration.v1','captured_at':datetime.now(timezone.utc).isoformat(),
                 'bucket':BUCKET,'expected_owner':ACCOUNT,'observations':[first,second]}
        raw=encoded(capture)
        if findings(raw.decode('utf-8'),retired):raise ConfigurationUnavailable('Credential pattern prevents configuration retention')
        result.kv(complete_sdk_configuration_base64=base64.b64encode(raw).decode('ascii'),complete_sdk_configuration_sha256=hashlib.sha256(raw).hexdigest(),complete_sdk_configuration_bytes=len(raw))
        result.kv(**assess(first,second))


if __name__=='__main__':
    try:main()
    except Exception:sys.exit(1)
