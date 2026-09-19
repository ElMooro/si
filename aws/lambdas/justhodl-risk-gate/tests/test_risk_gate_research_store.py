from datetime import datetime,timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(Path(__file__).resolve().parents[1]/'source')]
import risk_gate_research_store as store
from test_risk_gate_research_model import fixture
from report_observations import build as macro_build,encoded,digest
from evidence_store import capture
NOW=datetime(2026,9,18,20,tzinfo=timezone.utc)


class Frozen(datetime):
    @classmethod
    def now(cls,tz=None):return NOW if tz else NOW.replace(tzinfo=None)


class StorageError(Exception):
    def __init__(self,code):self.response={'Error':{'Code':code}}


class Storage:
    def __init__(self):self.objects={};self.race=False
    def get_object(self,Bucket,Key):
        if Key not in self.objects:raise StorageError('NoSuchKey')
        raw,meta=self.objects[Key]
        return {'Body':io.BytesIO(raw),'Metadata':meta,'ETag':hashlib.sha256(raw).hexdigest()}
    def put_object(self,Bucket,Key,Body,**kw):
        if Key==store.CURRENT and self.race:
            self.race=False
            self.objects[Key]=(encoded({'generated_at':'2099-01-01T00:00:00Z'}),{})
            raise StorageError('PreconditionFailed')
        if kw.get('IfNoneMatch')=='*' and Key in self.objects:raise StorageError('PreconditionFailed')
        if 'IfMatch' in kw and (Key not in self.objects or hashlib.sha256(self.objects[Key][0]).hexdigest()!=kw['IfMatch']):
            raise StorageError('PreconditionFailed')
        self.objects[Key]=(Body,kw.get('Metadata',{}))


def prepared():
    client=Storage();source,original=fixture()
    for sid,item in original.items():
        for part in ('definition','observations'):
            url=item['evidence'][part]['source_url']
            item['evidence'][part]=capture(client,'fixture','fred',url,encoded(item[part]),received_at=NOW)
    source=macro_build(source['catalog'],original,NOW.isoformat())
    reviewed=Path(store.report_observations.__file__).read_bytes();sha=hashlib.sha256(reviewed).hexdigest()
    compiler={'key':'data/report-research/compilers/'+sha+'.py','sha256':sha}
    client.put_object(Bucket='fixture',Key=compiler['key'],Body=reviewed)
    manifest={'contract':'report-research-replay.v1','catalog':source['catalog'],'generated_at':NOW.isoformat(),
        'inputs':{sid:{'evidence':item['evidence'],'acquired_at':item['acquired_at']} for sid,item in original.items()},
        'compiler':compiler,'errors':{},'output_sha256':digest(source)}
    key='data/report-research/runs/'+digest(manifest)+'.json'
    client.put_object(Bucket='fixture',Key=key,Body=encoded(manifest))
    source['replay']={'manifest_key':key,'output_sha256':manifest['output_sha256'],'compiler_sha256':sha}
    client.put_object(Bucket='fixture',Key='data/report-measurements.json',Body=encoded(source))
    return client,source,original


class Tests(unittest.TestCase):
    def test_run_preserves_legacy_and_reproduces_exactly_from_original_bytes(self):
        client,source,original=prepared();old=encoded({'generated_at':'2020-01-01T00:00:00Z','target_allocation':['legacy']})
        client.put_object(Bucket='fixture',Key=store.CURRENT,Body=old)
        with patch.object(store,'datetime',Frozen):result=store.run(client,'fixture')
        self.assertTrue(result['published']);packet=json.loads(client.objects[store.CURRENT][0])
        archived=store.PREFIX+'legacy-unvalidated/'+hashlib.sha256(old).hexdigest()+'.json'
        self.assertEqual(client.objects[archived][0],old)
        self.assertEqual(packet['series']['RRPONTSYD']['latest_value'],.576)
        self.assertIsNone(packet['composite'])
        sys.path.insert(0,str(ROOT/'scripts'))
        from replay_risk_gate_research import replay
        def raw(key):
            data=client.objects[key][0]
            return gzip.decompress(data) if key.endswith('.gz') else data
        manifest=json.loads(raw(packet['replay']['manifest_key']))
        self.assertEqual(replay(manifest,raw),{k:v for k,v in packet.items() if k!='replay'})

    def test_corrupt_original_prevents_any_current_write(self):
        client,source,original=prepared()
        ref=original['RRPONTSYD']['evidence']['observations'];client.objects[ref['key']]=(gzip.compress(b'{}'),{})
        with patch.object(store,'datetime',Frozen),self.assertRaises(ValueError):store.run(client,'fixture')
        self.assertNotIn(store.CURRENT,client.objects)

    def test_concurrent_newer_output_wins_without_overwrite(self):
        client,source,original=prepared();client.race=True
        with patch.object(store,'datetime',Frozen):result=store.run(client,'fixture')
        self.assertFalse(result['published'])
        self.assertEqual(json.loads(client.objects[store.CURRENT][0])['generated_at'],'2099-01-01T00:00:00Z')

    def test_immutable_compiler_collision_is_detected_before_publication(self):
        client,source,original=prepared()
        body=Path(store.lce_research_model.__file__).read_bytes()
        key=store.PREFIX+'compilers/'+hashlib.sha256(body).hexdigest()+'.py'
        client.objects[key]=(b'corrupt',{})
        with patch.object(store,'datetime',Frozen),self.assertRaises(ValueError):store.run(client,'fixture')
        self.assertNotIn(store.CURRENT,client.objects)


class ActualHandlerTests(unittest.TestCase):
    def test_active_handler_only_enters_research_store_and_validate_has_no_writes(self):
        import runpy
        namespace=runpy.run_path(str(Path(__file__).parent/'run_tests.py'))
        module=namespace['_load']()
        with patch('risk_gate_research_store.run',return_value={'validation_only':True}) as mocked:
            result=module.lambda_handler({'validate_only':True},None)
        self.assertNotIn('statusCode',result);self.assertTrue(result['ok'])
        mocked.assert_called_once_with(module.s3,module.S3_BUCKET,validation_only=True)
        client,source,original=prepared();before=dict(client.objects)
        with patch.object(store,'datetime',Frozen):out=store.run(client,'fixture',validation_only=True)
        self.assertTrue(out['validation_only']);self.assertEqual(before,client.objects)
        module.s3=client;module.S3_BUCKET='fixture'
        with patch.object(store,'datetime',Frozen):envelope=module.lambda_handler({'mode':'validate_only'},None)
        # Mirror the deployment shell's documented direct-or-HTTP envelope contract.
        body=envelope.get('body') if 'statusCode' in envelope else envelope
        if isinstance(body,str):body=json.loads(body)
        config=json.loads((Path(__file__).parents[1]/'config.json').read_text(encoding='utf-8'))
        self.assertIsInstance(body,dict)
        self.assertIs(body['ok'],True);self.assertIs(body['validation_only'],True)
        self.assertEqual(body['schema_version'],config['release_validation']['schema_version'])
        self.assertTrue(isinstance(body['status'],str) and body['status'])
        self.assertGreater(body['artifact_size_bytes'],0);self.assertEqual(before,client.objects)


if __name__=='__main__':unittest.main()
