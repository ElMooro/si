"""Actual daily archive handler with in-memory S3, no cloud/provider calls."""
import importlib.util
import io
import json
from pathlib import Path
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

SOURCE = Path(__file__).resolve().parents[1] / 'source/lambda_function.py'
class S3:
    def __init__(self):
        self.objects, self.writes, self.reads = {}, [], []
        self.fail_listing = False
    def get_object(self, **kw):
        self.reads.append(kw['Key'])
        return {'Body': io.BytesIO(self.objects[kw['Key']])}
    def put_object(self, **kw):
        self.writes.append(kw['Key']);self.objects[kw['Key']] = kw['Body']
    def get_paginator(self, name):
        assert name == 'list_objects_v2'
        def pages(**kw):
            objects=[{'Key':key,'Size':len(body),'LastModified':datetime(2026,9,9,12,tzinfo=timezone.utc)} for key,body in sorted(self.objects.items()) if key.startswith(kw['Prefix'])]
            yield {'Contents': objects[:1]}
            if self.fail_listing:raise RuntimeError('incomplete listing')
            yield {'Contents': objects[1:]}
        return types.SimpleNamespace(paginate=pages)

class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.s3=S3();fake=types.ModuleType('boto3');fake.client=lambda *a,**k:self.s3
        private=types.ModuleType('private_artifact');private.is_private_source=lambda key:key=='portfolio/snapshot.json'
        spec=importlib.util.spec_from_file_location('whats_changed_index_test',SOURCE)
        self.mod=importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules,{'boto3':fake,'private_artifact':private}):spec.loader.exec_module(self.mod)
        self.s3.objects['data/yield-curve.json']=b'{"regime":"FLAT","zero":0}'
        self.s3.objects['data/snapshots/data_event-study-2026-09-01.json']=b'{"rows":[null,0]}'

    def test_actual_handler_indexes_only_real_source_owned_dates(self):
        for key in ['data/snapshots/data_yield-curve-2026-02-30.json','data/snapshots/data_unknown-2026-09-02.json','data/snapshots/../private-2026-09-02.json']:
            self.s3.objects[key]=b'{"invalid":true}'
        response=self.mod.lambda_handler({},None)
        self.assertEqual(response['statusCode'],200)
        index=json.loads(self.s3.objects['data/snapshots-index.json'])
        self.assertEqual(index['schema_version'],'daily-snapshot-index.v1');self.assertTrue(index['complete'])
        self.assertEqual(index['n_snapshots'],2)
        self.assertIn('2026-09-01',index['dates']);self.assertEqual(len(index['dates']),2)
        self.assertTrue(all(not row['immutable'] and not row['point_in_time_certified'] for row in index['snapshots']))
        self.assertTrue(all(row['object_last_modified']=='2026-09-09T12:00:00+00:00' for row in index['snapshots']))
        self.assertEqual(json.loads(self.s3.objects['data/whats-changed.json'])['snapshot_index']['n_snapshots'],2)

    def test_private_sources_are_not_read_archived_or_indexed(self):
        self.mod.TRACKED.append(('portfolio/snapshot.json','private_account'))
        privatekey='data/snapshots/portfolio_snapshot-2026-09-02.json';self.s3.objects[privatekey]=b'{"private":true}'
        self.mod.lambda_handler({},None)
        self.assertNotIn('portfolio/snapshot.json',self.s3.reads)
        self.assertNotIn(privatekey,self.s3.writes)
        index=json.loads(self.s3.objects['data/snapshots-index.json'])
        self.assertTrue(all(row['source_key']!='portfolio/snapshot.json' for row in index['snapshots']))
        with self.assertRaisesRegex(ValueError,'private sources'):self.mod.write_snapshot('portfolio/snapshot.json',{},'2026-09-09')

    def test_failed_later_listing_page_does_not_publish_false_complete_index(self):
        previous=b'{"existing":"preserve"}';self.s3.objects['data/snapshots-index.json']=previous
        self.s3.fail_listing=True
        with self.assertRaisesRegex(RuntimeError,'incomplete listing'):self.mod.lambda_handler({},None)
        self.assertEqual(self.s3.objects['data/snapshots-index.json'],previous)
        self.assertNotIn('data/snapshots-index.json',self.s3.writes)
        self.assertNotIn('data/whats-changed.json',self.s3.writes)

if __name__=='__main__':unittest.main(verbosity=2)
