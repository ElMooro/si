"""Offline official SDK parser/transport contract; no AWS/network access.

Run with the deploy workflow's boto3 test dependency. Complete handler cases
remain in the dependency-free run_tests.py runner.
"""
from urllib.parse import parse_qs, quote, urlsplit
import copy
import importlib.util
from pathlib import Path

import boto3
import botocore
from botocore.awsrequest import AWSResponse
from botocore.config import Config

HERE = Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location('admission_fakes', HERE / 'run_tests.py')
fakes = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fakes)


class Raw:
    def __init__(self, body):
        self.body = body
    def stream(self, **kwargs):
        yield self.body


def packet(prefix, objects=(), groups=(), truncated=False, count=None, corrupt=False):
    # Emulate S3's URL-encoded wire names; botocore performs its existing single
    # transport decode, while our admission code never decodes returned keys.
    parts = ['<ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">',
             f'<Name>{fakes.BUCKET}</Name>', f'<Prefix>{quote(prefix, safe="")}</Prefix>',
             '<Delimiter>%2F</Delimiter>', '<MaxKeys>1</MaxKeys>',
             f'<KeyCount>{len(objects) + len(groups) if count is None else count}</KeyCount>',
             f'<IsTruncated>{str(truncated).lower()}</IsTruncated>', '<EncodingType>url</EncodingType>']
    for key, size in objects:
        parts += ['<Contents>', f'<Key>{quote(key, safe="")}</Key>', f'<Size>{size}</Size>',
                  '<ETag>"invented"</ETag>', '<StorageClass>STANDARD</StorageClass>',
                  '<LastModified>2026-10-02T00:00:00.000Z</LastModified>', '</Contents>']
    for group in groups:
        parts += [f'<CommonPrefixes><Prefix>{quote(group, safe="")}</Prefix></CommonPrefixes>']
    if truncated:
        parts += ['<NextContinuationToken>invented-token</NextContinuationToken>']
    parts += ['</ListBucketResult>']
    return ('<broken' if corrupt else ''.join(parts)).encode()


def client_with_packets(responses):
    client = boto3.client('s3', region_name='us-east-1',
                          endpoint_url='https://invented.invalid',
                          aws_access_key_id='invented', aws_secret_access_key='invented',
                          config=Config(retries={'max_attempts': 0}))
    trace = []
    def send(request, **kwargs):
        query = parse_qs(urlsplit(request.url).query)
        assert query['list-type'] == ['2'] and query['max-keys'] == ['1']
        assert query['delimiter'] == ['/'] and query['encoding-type'] == ['url']
        assert set(query) == {'list-type', 'prefix', 'delimiter', 'max-keys', 'encoding-type'}
        assert urlsplit(request.url).hostname.endswith('invented.invalid')
        prefix = query['prefix'][0]
        trace.append(prefix)
        status, body = responses[prefix]
        return AWSResponse(request.url, status, {'content-type': 'application/xml',
                           'x-amz-request-id': 'invented'}, Raw(body))
    # No network path is reachable: every HTTP send is replaced by this fake.
    client._endpoint.http_session.send = send
    return client, trace


def run_tests():
    series = 'data/providers/eurostat/series/'
    manifest = 'data/providers/eurostat/series-manifest.json'
    empty = {p: (200, packet(p)) for p in (series, manifest)}
    cases = [(empty, True, 2)]
    for prefix, extras, accepted, calls in (
        (series, {'objects': [(series, 0)]}, True, 2),
        (series, {'objects': [(series + 'page-9999.json', 0)]}, False, 1),
        (series, {'groups': [series + 'nested/']}, False, 1),
        (series, {'objects': [(series, 0)], 'truncated': True}, False, 1),
        (manifest, {'objects': [(manifest, 0)]}, False, 2),
        (manifest, {'objects': [(manifest + '.bak', 50)]}, True, 2),
        (manifest, {'objects': [(manifest + '%2Fchild', 0)]}, True, 2),
        (manifest, {'groups': [manifest + '/']}, False, 2),
        (manifest, {'objects': [(manifest + '.bak', 0)], 'truncated': True}, False, 2),
        (series, {'count': 1}, False, 1),
        (manifest, {'corrupt': True}, False, 2),
    ):
        responses = copy.deepcopy(empty)
        responses[prefix] = (200, packet(prefix, **extras))
        cases.append((responses, accepted, calls))
    for prefix, calls in ((series, 1), (manifest, 2)):
        responses = copy.deepcopy(empty)
        responses[prefix] = (403, b'<Error><Code>AccessDenied</Code><Message>invented private denial</Message></Error>')
        cases.append((responses, False, calls))
    for responses, accepted, calls in cases:
        client, trace = client_with_packets(responses)
        module = fakes.load(fakes.SOURCE)
        module.s3 = client
        try:
            module._require_unpopulated_series_namespace('eurostat')
        except RuntimeError as exc:
            assert not accepted and str(exc) == 'missing checkpoint namespace admission refused'
        else:
            assert accepted
        assert trace == [series, manifest][:calls]
    # Validate the actual SDK model and parser, not a Contents-length assumption.
    client, _ = client_with_packets({series: (200, packet(series, groups=[series + 'nested/']))})
    model = client.meta.service_model.operation_model('ListObjectsV2')
    assert model.output_shape.members['KeyCount'].type_name == 'integer'
    assert model.output_shape.members['CommonPrefixes'].type_name == 'list'
    group = client.list_objects_v2(Bucket=fakes.BUCKET, Prefix=series, Delimiter='/', MaxKeys=1)
    assert group['KeyCount'] == 1 and 'Contents' not in group and group['IsTruncated'] is False
    assert group['CommonPrefixes'] == [{'Prefix': series + 'nested/'}]
    print(f'Official SDK listing contract PASS: {len(cases)} offline transport cases; '
          f'grouped parser/model proof; botocore={botocore.__version__}')


if __name__ == '__main__':
    run_tests()
