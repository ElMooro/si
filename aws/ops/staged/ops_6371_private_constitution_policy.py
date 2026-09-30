"""Add four reviewed private-resource ARNs to the existing bucket read deny.

Reads/writes only bucket-policy configuration. Never reads/HEADs/lists an object,
invokes a producer/Worker, writes an account artifact or changes a schedule.
All 31 existing statements, permissions and conditions remain represented.
"""
from pathlib import Path
import base64
import copy
import hashlib
import json
import sys

ROOT = Path(__file__).resolve().parents[3]
sys.path[:0] = [str(ROOT/'aws/ops'), str(ROOT/'aws/ops/checks'), str(ROOT/'aws/shared'), str(ROOT/'scripts')]
from audit_20260909_security import anonymous_deny_statement, policies_equal

BUCKET = 'justhodl-dashboard-live'
OWNER = '857687956942'
SID = 'Audit20260909PrivatePersonalArtifacts'
PATHS = ('brain-constitution.json', 'data/brain-constitution.json',
         'history/archive/feed/brain-constitution.json/*',
         'history/archive/feed/data/brain-constitution.json/*')
REPORT = 'aws/ops/reports/latest/ops_6370_snapshot_storage_configuration.md'
REPORT_SHA = '9375ebe644bd7f546a7d8b56b784c210043e5020b4cbdcb33e0ef50645572291'
CAPTURE_SHA = '1acba787edb8824bbef0091ce0c5327dbc87703fee0dcb855b972dd240bc9ee0'
POLICY_SHA = '4b5f4659e00112153be62da35297e516c600f98c2820e19b1667de4f01242dda'


class PolicyUnavailable(RuntimeError):
    pass


def encoded(value):
    return json.dumps(value, ensure_ascii=False, allow_nan=False, separators=(',', ':')).encode('utf-8')


def document(text):
    def pairs(rows):
        result = {}
        for key, value in rows:
            if key in result:
                raise PolicyUnavailable('Duplicate policy field')
            result[key] = value
        return result
    def constant(_):raise PolicyUnavailable('Nonfinite policy field')
    try:
        if type(text) is not str or not 0 < len(text.encode('utf-8')) <= 20480:
            raise PolicyUnavailable('Complete bounded policy text required')
        result = json.loads(text, object_pairs_hook=pairs, parse_constant=constant)
        encoded(result)
    except (ValueError, TypeError, UnicodeError, RecursionError):
        raise PolicyUnavailable('Invalid complete policy text') from None
    if type(result) is not dict or result.get('Version') != '2012-10-17' or type(result.get('Statement')) is not list:
        raise PolicyUnavailable('Reviewed bucket policy structure required')
    return result


def original_policy(root=ROOT):
    raw = (root/REPORT).read_bytes()
    if hashlib.sha256(raw).hexdigest() != REPORT_SHA:
        raise PolicyUnavailable('Exact retained configuration report required')
    lines = [line.strip('|').split('|') for line in raw.decode('utf-8').splitlines() if line.startswith('|')]
    names = [cell.strip() for cell in lines[0]]
    if any(len(row) != len(names) for row in lines):
        raise PolicyUnavailable('Complete report framing required')
    values = {key:cell.strip() for row in lines[2:] for key,cell in zip(names,row) if cell.strip()}
    body = base64.b64decode(values['complete_sdk_configuration_base64'], validate=True)
    if hashlib.sha256(body).hexdigest() != CAPTURE_SHA:
        raise PolicyUnavailable('Exact whole configuration capture required')
    def obj(value):
        if type(value) is not list or len(value) != 2 or value[0] != 'object':
            raise PolicyUnavailable('Complete typed configuration object required')
        pairs = value[1]
        if any(type(row) is not list or len(row) != 2 or type(row[0]) is not str for row in pairs) or len({k for k,_ in pairs}) != len(pairs):
            raise PolicyUnavailable('Invalid typed configuration fields')
        return dict(pairs)
    capture = obj(json.loads(body))
    if capture['contract'] != ['string', 'snapshot-storage-configuration.v1'] or capture['bucket'] != ['string', BUCKET] or capture['expected_owner'] != ['string', OWNER]:
        raise PolicyUnavailable('Reviewed complete storage identity required')
    observations = capture['observations']
    if observations[0] != 'array' or len(observations[1]) != 2:
        raise PolicyUnavailable('Both complete original observations required')
    texts = []
    for observation in observations[1]:
        setting = obj(obj(observation)['get_bucket_policy'])
        value = obj(setting['response'])['Policy']
        if setting['status'] != ['string', 'present'] or value[0] != 'string':
            raise PolicyUnavailable('Original bucket policy unavailable')
        texts.append(value[1])
    if texts[0] != texts[1] or hashlib.sha256(texts[0].encode()).hexdigest() != POLICY_SHA:
        raise PolicyUnavailable('Stable original bucket policy required')
    return document(texts[0])


def prepare(before):
    """Preserve every existing field; only add the four explicitly named ARNs."""
    after = copy.deepcopy(before)
    statements = after.get('Statement')
    if type(statements) is not list or len(statements) != 31 or any(type(row) is not dict for row in statements):
        raise PolicyUnavailable('Complete reviewed statement inventory required')
    matches = [row for row in statements if row.get('Sid') == SID]
    if len(matches) != 1:
        raise PolicyUnavailable('Unique reviewed private deny required')
    actual = matches[0]
    expected = anonymous_deny_statement(BUCKET, OWNER)
    if {k:v for k,v in actual.items() if k != 'Resource'} != {k:v for k,v in expected.items() if k != 'Resource'}:
        raise PolicyUnavailable('Private deny permissions or condition differs')
    resources = actual.get('Resource')
    if type(resources) is not list or any(type(value) is not str for value in resources) or len(set(resources)) != len(resources):
        raise PolicyUnavailable('Whole unique private resource list required')
    additions = {'arn:aws:s3:::'+BUCKET+'/'+path for path in PATHS}
    if set(expected['Resource']) - set(resources) != additions or set(resources) - set(expected['Resource']):
        raise PolicyUnavailable('Only the four reviewed private resources may be missing')
    # Keep the old list in its exact order; new entries are an additive suffix.
    actual['Resource'].extend(sorted(additions))
    if len(encoded(after)) > 20480:
        raise PolicyUnavailable('S3 policy byte limit exceeded; nothing written')
    return after


def read_policy(client):
    try:
        response = client.get_bucket_policy(Bucket=BUCKET, ExpectedBucketOwner=OWNER)
    except Exception:
        raise PolicyUnavailable('Bucket policy read unavailable') from None
    if (type(response) is not dict or type(response.get('ResponseMetadata')) is not dict or
            type(response['ResponseMetadata'].get('HTTPStatusCode')) is not int or response['ResponseMetadata']['HTTPStatusCode'] != 200):
        raise PolicyUnavailable('Successful complete policy response required')
    document(response.get('Policy'))
    return response


def apply(client, baseline, retain):
    target = prepare(baseline)
    first = read_policy(client)
    current = document(first['Policy'])
    if not policies_equal(current, baseline) and not policies_equal(current, target):
        raise PolicyUnavailable('Existing policy differs from reviewed predecessor and target')
    second = read_policy(client)
    retain('before', [first, second])
    if not policies_equal(document(second['Policy']), current):
        raise PolicyUnavailable('Policy changed before write; nothing applied')
    changed = not policies_equal(current, target)
    if changed:
        try:
            client.put_bucket_policy(Bucket=BUCKET, ExpectedBucketOwner=OWNER, Policy=encoded(target).decode('utf-8'))
        except Exception:
            # A lost acknowledgement is resolved only by complete readback.
            # No blind retry or rollback can remove the added protection.
            pass
    after = [read_policy(client), read_policy(client)]
    retain('after', after)
    if any(not policies_equal(document(row['Policy']), target) for row in after):
        raise PolicyUnavailable('Exact additive policy repair was not confirmed')
    return {'private_definition_matches':True, 'policy_changed':changed,
            'preserved_statement_count':31, 'added_private_resources':list(PATHS),
            'target_policy_bytes':len(encoded(target)), 'target_policy_sha256':hashlib.sha256(encoded(target)).hexdigest(),
            'object_reads':0, 'object_head_requests':0, 'object_lists':0, 'private_reads':0,
            'native_invocations':0, 'schedule_changes':0, 'actual_object_exposure_tested':False,
            'bucket_policy_update_atomic_with_other_writers':False}


def main():
    import boto3
    from botocore.config import Config
    from ops_report import report
    from check_secrets import findings
    with report('ops_6371_private_constitution_policy') as result:
        baseline = original_policy()
        retired = set(json.loads((ROOT/'tests/security/retired-secret-sha256.json').read_bytes())['sha256'])
        client = boto3.client('s3', region_name='us-east-1', config=Config(connect_timeout=5, read_timeout=15, retries={'total_max_attempts':1}))
        def retain(phase, responses):
            raw = encoded(responses)
            if len(raw) > 100000 or findings(raw.decode('utf-8'), retired):
                raise PolicyUnavailable('Complete policy evidence retention refused')
            result.kv(phase=phase, whole_policy_responses_base64=base64.b64encode(raw).decode('ascii'),
                      whole_policy_responses_bytes=len(raw), whole_policy_responses_sha256=hashlib.sha256(raw).hexdigest())
        result.kv(**apply(client, baseline, retain))


if __name__ == '__main__':
    try:
        main()
    except Exception:
        sys.exit(1)
