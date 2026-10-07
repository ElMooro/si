"""Read-only probe scope, redaction and unknown/missing distinction."""
from datetime import datetime, timezone
from pathlib import Path
import json
import sys
import unittest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'aws/ops/staged'))
import ops_6493_network_delivery_diagnostic as probe
NOW = datetime(2026, 10, 7, 22, tzinfo=timezone.utc)


class Missing(Exception):
    response = {'Error': {'Code': '404'}}


class Denied(Exception):
    response = {'Error': {'Code': 'AccessDenied', 'Message': 'SECRET'}}


class ReadOnly:
    def __init__(self): self.calls = []
    def list_rules(self, **kw):
        self.calls.append(('rules', kw))
        return {'Rules': [{'Name': 'coordinator', 'State': 'ENABLED', 'EventPattern': json.dumps({'source':[{'prefix':'justhodl.'}], 'detail': {'secret':'SECRET'}})}]}
    def list_targets_by_rule(self, **kw):
        self.calls.append(('targets', kw))
        return {'Targets':[{'Arn':'arn:aws:lambda:us-east-1:857687956942:function:'+probe.COORDINATOR,
            'Input':'SECRET', 'RoleArn':'SECRET', 'DeadLetterConfig':{'Arn':'SECRET'}}]}
    def get_function_configuration(self, **kw):
        self.calls.append(('config', kw))
        return {'State':'Active', 'Environment':{'Variables':{'KEY':'SECRET'}}, 'Role':'SECRET'}
    def get_metric_statistics(self, **kw):
        self.calls.append(('metric', kw)); return {'Datapoints':[]}
    def head_object(self, **kw):
        self.calls.append(('head', kw)); return {'ContentLength':12, 'LastModified':NOW, 'Metadata':{'secret':'SECRET'}}
    def list_objects_v2(self, **kw):
        self.calls.append(('list', kw))
        return {'Contents':[{'Key':probe.CONTEXT_PREFIX+'a'*64+'.json', 'LastModified':NOW},
                            {'Key':probe.CONTEXT_PREFIX+'PRIVATE', 'LastModified':NOW}], 'IsTruncated':True, 'NextContinuationToken':'SECRET'}
    def filter_log_events(self, **kw):
        self.calls.append(('logs', kw))
        return {'events':[{'timestamp':1,'message':'[coordinator] routed event=regime.changed invokes=3\nSECRET'},
                          {'timestamp':2,'message':'[harvester] scanned=1 engines_with_picks=2 harvested=3 written=4 regime=SECRET'},
                          {'timestamp':3,'message':'PRIVATE SECRET'}], 'nextToken':'SECRET'}


class DeliveryDiagnosticTests(unittest.TestCase):
    def collect(self, client=None):
        client = client or ReadOnly()
        return probe.collect(client, client, client, client, client, NOW), client

    def test_only_read_api_and_exact_resources(self):
        result, client = self.collect()
        self.assertEqual(result['cloud_writes'], 0)
        self.assertEqual(result['object_body_reads'], 0)
        self.assertEqual(result['native_invocations'], 0)
        self.assertEqual({x[1]['Key'] for x in client.calls if x[0]=='head'}, set(probe.KEYS))
        self.assertEqual({x[1]['FunctionName'] for x in client.calls if x[0]=='config'}, set(probe.FUNCTIONS))
        self.assertEqual(len([x for x in client.calls if x[0]=='metric']), 20)
        self.assertEqual([x[1]['Prefix'] for x in client.calls if x[0]=='list'], [probe.CONTEXT_PREFIX])
        self.assertTrue(all(x[1]['limit']==100 for x in client.calls if x[0]=='logs'))

    def test_no_secrets_bodies_target_inputs_or_raw_logs_exported(self):
        result, _ = self.collect()
        encoded = json.dumps(result)
        self.assertNotIn('SECRET', encoded)
        self.assertNotIn('PRIVATE', encoded)
        self.assertNotIn('NextContinuationToken', encoded)
        self.assertEqual(result['event_rules'][0]['input_transform_present'], [True])
        self.assertEqual(result['event_rules'][0]['dead_letter_configured'], [True])

    def test_pagination_and_empty_metrics_remain_explicit(self):
        result, _ = self.collect()
        self.assertEqual(result['functions'][probe.COORDINATOR]['metrics']['Invocations'], [])
        self.assertFalse(result['prospective_context_metadata']['listing_complete'])
        self.assertEqual(result['prospective_context_metadata']['observed_objects'], 1)
        self.assertTrue(result['log_samples'][probe.COORDINATOR]['page_has_continuation'])
        self.assertFalse(result['prospective_context_metadata']['context_contents_verified'])

    def test_missing_and_access_denied_are_different(self):
        class Errors(ReadOnly):
            def head_object(self, **kw):
                if kw['Key']==probe.KEYS[0]: raise Missing()
                raise Denied()
        result, _ = self.collect(Errors())
        self.assertFalse(result['publication_metadata'][probe.KEYS[0]]['exists'])
        self.assertIsNone(result['publication_metadata'][probe.KEYS[1]]['exists'])
        self.assertNotIn('SECRET', json.dumps(result))

    def test_route_and_completion_projection_is_bounded(self):
        result, _ = self.collect()
        item = result['log_samples'][probe.COORDINATOR]
        self.assertEqual(item['routed_event_counts'], {'regime.changed':1})
        self.assertEqual(item['harvester_completion_times_unix_ms'], [2])

    def test_invalid_event_patterns_do_not_leak_values(self):
        for value in ('SECRET', '[]', None):
            self.assertEqual(probe.pattern_projection(value), {'valid_json_object':False})
        self.assertEqual(probe.pattern_projection('{"source":[{"secret":"SECRET"}]}')['source'], ['unsupported_selector'])


if __name__ == '__main__': unittest.main()
