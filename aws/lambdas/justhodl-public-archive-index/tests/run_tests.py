"""Actual index handler: fake metadata pages only; payload reads are forbidden."""
import importlib.util
import json
from pathlib import Path
import sys
import types
import unittest
from datetime import datetime, timezone
from unittest.mock import patch

SOURCE=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
class MetadataS3:
    def __init__(self):self.calls=[];self.writes={};self.routes={}
    def get_object(self, **kw):raise AssertionError('archive payload reads forbidden')
    def list_objects_v2(self, **kw):
        self.calls.append(kw)
        response=self.routes.get((kw['Prefix'],kw.get('ContinuationToken')),{'IsTruncated':False,'Contents':[]})
        if isinstance(response,Exception):raise response
        return response
    def put_object(self, **kw):self.writes[kw['Key']]=json.loads(kw['Body'])

def obj(key,size=0):return {'Key':key,'Size':size,'LastModified':datetime(2026,9,1,14,tzinfo=timezone.utc)}
class HandlerTests(unittest.TestCase):
    def setUp(self):
        self.s3=MetadataS3();boto=types.ModuleType('boto3');boto.client=lambda *a,**kw:self.s3
        private=types.ModuleType('private_artifact');private.is_private_source=lambda key:any(x in key for x in ['..','%','\\','/private/'])
        spec=importlib.util.spec_from_file_location('public_archive_test',SOURCE);self.mod=importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules,{'boto3':boto,'private_artifact':private}):spec.loader.exec_module(self.mod)
        self.engine,self.pattern=self.mod.REGISTRY[0];self.prefix=self.pattern.rsplit('/',1)[0]+'/'
        self.key='data/archive-indexes/'+self.engine+'.json'

    def test_actual_handler_collects_all_pages_zero_bytes_and_exact_metadata(self):
        self.s3.routes[(self.prefix,None)]={'IsTruncated':True,'NextContinuationToken':'next','Contents':[obj(self.prefix+'a.json')]}
        self.s3.routes[(self.prefix,'next')]={'IsTruncated':False,'Contents':[obj(self.prefix+'b.json',123),obj(self.prefix+'a.json'),obj(self.prefix+'nested/hidden.json'),obj(self.prefix+'bad.txt'),obj('data/brain.json'),obj(self.prefix+'../secret.json')]}
        result=self.mod.lambda_handler({'prefix':'backtest/ledger/','bucket':'private','engine':'owner'},None)
        self.assertTrue(result['ok']);doc=self.s3.writes[self.key]
        self.assertEqual([r['key'] for r in doc['snapshots']],[self.prefix+'a.json',self.prefix+'b.json'])
        self.assertEqual(doc['snapshots'][0]['size_bytes'],0)
        self.assertEqual(doc['snapshots'][0]['last_modified'],'2026-09-01T14:00:00+00:00')
        self.assertEqual(doc['engine'],self.engine);self.assertEqual(doc['publisher_engine'],'justhodl-public-archive-index')
        self.assertEqual(doc['listing_pages'],2);self.assertFalse(doc['listing_is_atomic'])
        self.assertTrue(all(r['immutable'] is False and r['content_status']=='NOT_READ' for r in doc['snapshots']))
        self.assertTrue(all(call['Prefix'].startswith('data/archive/') for call in self.s3.calls))
        catalog=self.s3.writes['data/archive-indexes/catalog.json'];self.assertEqual(len(catalog['indexes']),len(self.mod.REGISTRY))

    def test_access_denial_discards_partial_keys_and_publishes_unavailable(self):
        self.s3.routes[(self.prefix,None)]={'IsTruncated':True,'NextContinuationToken':'next','Contents':[obj(self.prefix+'partial.json')]}
        error=RuntimeError('must never publish this arbitrary exception body');error.response={'Error':{'Code':'AccessDenied'}}
        self.s3.routes[(self.prefix,'next')]=error
        result=self.mod.lambda_handler({},None);self.assertFalse(result['ok'])
        doc=self.s3.writes[self.key];self.assertFalse(doc['complete']);self.assertEqual(doc['snapshots'],[])
        self.assertEqual(doc['errors'][0]['code'],'AccessDenied')
        self.assertNotIn('arbitrary exception',json.dumps(self.s3.writes))
        self.assertFalse(self.s3.writes['data/archive-indexes/catalog.json']['complete'])

    def test_repeating_or_missing_pagination_token_cannot_claim_complete(self):
        for value in [None,'repeat']:
            self.s3.calls=[];self.s3.routes={}
            first={'IsTruncated':True,'Contents':[obj(self.prefix+'x.json')]}
            if value:first['NextContinuationToken']=value
            self.s3.routes[(self.prefix,None)]=first
            self.s3.routes[(self.prefix,'repeat')]={'IsTruncated':True,'NextContinuationToken':'repeat','Contents':[]}
            self.mod.lambda_handler({},None)
            self.assertFalse(self.s3.writes[self.key]['complete']);self.assertEqual(self.s3.writes[self.key]['snapshots'],[])
            self.assertLessEqual(len([r for r in self.s3.calls if r['Prefix']==self.prefix]),2)

    def test_validate_only_runs_real_listings_but_zero_publication(self):
        self.s3.routes[(self.prefix,None)]={'IsTruncated':False,'Contents':[obj(self.prefix+'x.json')]}
        result=self.mod.lambda_handler({'mode':'validate_only'},None)
        self.assertTrue(result['ok']);self.assertTrue(result['validation_only'])
        self.assertEqual(result['schema_version'],'public-engine-archive-index.v1');self.assertGreater(result['artifact_size_bytes'],0)
        self.assertTrue(self.s3.calls);self.assertEqual(self.s3.writes,{})

    def test_registry_is_unique_scoped_and_rejects_caller_defined_families(self):
        self.assertEqual(len({e for e,p in self.mod.REGISTRY}),len(self.mod.REGISTRY))
        self.assertEqual(len({p for e,p in self.mod.REGISTRY}),len(self.mod.REGISTRY))
        for engine,pattern in self.mod.REGISTRY:
            self.assertTrue(pattern.startswith('data/archive/'));self.assertEqual(pattern.count('*'),1)
            self.assertNotIn(pattern,('data/archive/*.json','data/archive/auction-crisis/*.json'))
        with self.assertRaises(ValueError):self.mod.build_index('owner','backtest/ledger/*.json')
        # The weekly family's literal prefix also rejects unrelated daily files.
        engine,pattern=next((e,p) for e,p in self.mod.REGISTRY if e=='justhodl-dark-pool')
        prefix=pattern.rsplit('/',1)[0]+'/'
        self.s3.routes[(prefix,None)]={'IsTruncated':False,'Contents':[obj(prefix+'week-2026-09-01.json'),obj(prefix+'day-2026-09-01.json')]}
        doc=self.mod.build_index(engine,pattern);self.assertEqual(len(doc['snapshots']),1)

if __name__=='__main__':unittest.main(verbosity=2)
