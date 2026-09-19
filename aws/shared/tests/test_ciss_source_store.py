from copy import deepcopy
from datetime import datetime
import gzip
import hashlib
import io
import json
import unittest
from unittest.mock import patch

import ciss_source_store as store
import ciss_source_model as model
from evidence_store import capture
from test_ciss_source_model import NOW, packet_inputs


class StorageError(Exception):
    def __init__(self, code):self.response={'Error':{'Code':code}}


class MemoryS3:
    """Conditional fake used only for concurrency and evidence regression tests."""
    def __init__(self):self.objects={};self.metadata={};self.puts=[];self.before_put=None
    def get_object(self, Bucket, Key):
        if Key not in self.objects:raise StorageError('NoSuchKey')
        body=self.objects[Key]
        return {'Body':io.BytesIO(body),'ETag':hashlib.sha256(body).hexdigest(),'Metadata':self.metadata.get(Key,{})}
    def put_object(self, Bucket, Key, Body, **kw):
        if self.before_put:self.before_put(Key)
        if kw.get('IfNoneMatch')=='*' and Key in self.objects:raise StorageError('412')
        if 'IfMatch' in kw and (Key not in self.objects or hashlib.sha256(self.objects[Key]).hexdigest()!=kw['IfMatch']):raise StorageError('412')
        self.objects[Key]=Body;self.metadata[Key]=kw.get('Metadata',{});self.puts.append(Key)


def retained(client):
    discoveries,histories=packet_inputs()
    for items in (discoveries,histories):
        for key,item in items.items():
            url=store.BASE+key.replace('.','/',1)+'?format=csvdata' if '.' in key else store.BASE+key+'?format=csvdata&lastNObservations=1'
            item['request_url']=url
            item['evidence']=capture(client,'test','ecb',url,item['raw'],datetime.fromisoformat(NOW))
    return discoveries,histories


class CissStoreTests(unittest.TestCase):
    def run_fixture(self,s3):
        inputs=retained(s3)
        with patch.object(store,'collect',return_value=(NOW,*inputs,{})),patch.object(store,'datetime') as dt:
            dt.now.return_value=datetime.fromisoformat(NOW)
            return store.run(s3,'test')

    def test_originals_replay_exact_packet_and_archive_previous_bytes(self):
        s3=MemoryS3();old=b'{"generated_at":"2026-09-17T00:00:00Z","version":"legacy"}'
        s3.objects[store.CURRENT]=old
        result=self.run_fixture(s3);self.assertTrue(result['published'])
        out=json.loads(s3.objects[store.CURRENT]);manifest=json.loads(s3.objects[result['replay']['manifest_key']])
        rebuilt=model.build(store.hydrate(s3,'test',manifest['discoveries']),store.hydrate(s3,'test',manifest['histories']),NOW,manifest['errors'])
        rebuilt['collection_started_at']=NOW
        self.assertEqual(model.digest(rebuilt),out['replay']['output_sha256'])
        self.assertIn(old,[v for k,v in s3.objects.items() if 'legacy-unvalidated' in k])
        self.assertEqual(out['headline_reconciliation']['status'],'matched')

    def test_tampered_original_and_request_identity_block_publish(self):
        for kind in ('bytes','request'):
            s3=MemoryS3();disc,hist=retained(s3)
            if kind=='bytes':s3.objects[hist[model.HEAD]['evidence']['key']]=gzip.compress(b'altered')
            else:hist[model.HEAD]['request_url']+='&lastNObservations=1'
            with patch.object(store,'collect',return_value=(NOW,disc,hist,{})),patch.object(store,'datetime') as dt:
                dt.now.return_value=datetime.fromisoformat(NOW)
                with self.assertRaises(ValueError):store.run(s3,'test')
            self.assertNotIn(store.CURRENT,s3.objects)

    def test_cas_conflict_cannot_overwrite_newer_collection(self):
        s3=MemoryS3();self.run_fixture(s3)
        newer=json.loads(s3.objects[store.CURRENT]);newer['collection_started_at']='2026-09-19T00:00:00Z'
        def competing(key):
            if key==store.CURRENT:
                s3.objects[key]=model.encoded(newer);s3.before_put=None
        s3.before_put=competing
        result=self.run_fixture(s3)
        self.assertFalse(result['published']);self.assertEqual(json.loads(s3.objects[store.CURRENT]),newer)

    def test_commentary_is_bound_to_source_output_and_never_calls_ai(self):
        s3=MemoryS3();self.run_fixture(s3)
        with patch.object(store,'datetime') as dt:
            dt.now.return_value=datetime.fromisoformat(NOW)
            result=store.run_commentary(s3,'test')
        out=json.loads(s3.objects['data/ciss-ai.json'])
        self.assertTrue(result['published']);self.assertEqual(len(out['claims']),7)
        manifest=json.loads(s3.objects[out['replay']['manifest_key']])
        source=json.loads(s3.objects[manifest['input']['key']])
        self.assertEqual(model.digest(model.commentary(source,NOW)),manifest['output_sha256'])
        source['ea_composite']=.9;s3.objects[store.CURRENT]=model.encoded(source)
        with self.assertRaisesRegex(ValueError,'source packet differs'):store.run_commentary(s3,'test')
        self.assertEqual(json.loads(s3.objects['data/ciss-ai.json']),out)

    def test_acquisition_cache_reuses_original_clock_without_hiding_expiry(self):
        s3=MemoryS3();disc,hist=retained(s3);source=hist[model.HEAD];url=source['request_url']
        descriptor={k:v for k,v in source.items() if k!='raw'}
        key=model.PREFIX+'cache/'+hashlib.sha256(url.encode()).hexdigest()+'.json'
        s3.objects[key]=model.encoded(descriptor)
        with patch.object(store,'datetime') as dt,patch.object(store,'request',side_effect=TimeoutError) as request:
            dt.now.return_value=datetime.fromisoformat('2026-09-18T22:30:00+00:00')
            got=store.acquire(s3,'test',url,1)
            self.assertEqual(got['acquired_at'],NOW);request.assert_not_called()
            dt.now.return_value=datetime.fromisoformat('2026-09-18T23:30:00+00:00')
            with self.assertRaises(TimeoutError):store.acquire(s3,'test',url,1)
        self.assertEqual(json.loads(s3.objects[key]),descriptor)


if __name__=='__main__':unittest.main()
