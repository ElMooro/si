"""Original-source capture/replay and publication tests without AWS or network."""
from datetime import datetime,timezone
from pathlib import Path
import hashlib
import importlib.util
import io
import json
import sys
import types
import unittest
from unittest.mock import patch
ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
from test_tenor_research_model import DomainTests,fixture,TODAY
from evidence_store import capture

class Store:
    def __init__(self):self.objects={}
    def put_object(self,**kw):
        if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.objects:
            exc=ValueError('exists');exc.response={'Error':{'Code':'PreconditionFailed'}};raise exc
        self.objects[kw['Key']]={'Body':kw['Body'],'Metadata':kw.get('Metadata',{})}
    def get_object(self,**kw):
        if kw['Key'] not in self.objects:
            exc=ValueError('absent');exc.response={'Error':{'Code':'NoSuchKey'}};raise exc
        doc=self.objects[kw['Key']];return {**doc,'Body':io.BytesIO(doc['Body']),'ETag':hashlib.md5(doc['Body']).hexdigest()}

def load():
    store=Store();path=ROOT/'aws/lambdas/justhodl-tenor-signal-interpreter/source/lambda_function.py'
    spec=importlib.util.spec_from_file_location('tenor_test_lambda',path);module=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:store),
                                'managed_secret':types.SimpleNamespace(managed_secret=lambda *a:'fixture')}):spec.loader.exec_module(module)
    return module,store

class RuntimeTests(unittest.TestCase):
    def test_original_responses_replay_and_compilers_are_retained(self):
        module,store=load();pages,ff=fixture()
        refs=[capture(store,module.BUCKET,'treasury_fiscaldata','https://example.org/auctions?page[number]=1',json.dumps(pages[0]).encode())]
        fref=capture(store,module.BUCKET,'fred','https://example.org/fred?series_id=DFF&api_key=secret',json.dumps(ff).encode())
        module.fiscal_pages=lambda _: (pages,refs);module.fred=lambda _:(ff,fref)
        with patch.object(module,'datetime') as clock:
            clock.now.return_value=datetime(2026,9,18,12,tzinfo=timezone.utc)
            result=module.lambda_handler({})
        out=json.loads(store.objects[module.OUTPUT_KEY]['Body']);run=json.loads(store.objects[result['run']]['Body'])
        self.assertEqual(out['quality']['status'],'fresh');self.assertTrue(out['reproducibility']['original_response_replayed'])
        self.assertEqual(hashlib.sha256(module.canonical(run['output'])).hexdigest(),out['reproducibility']['output_sha256'])
        self.assertNotIn('secret',json.dumps(out));self.assertEqual(out['paid_api_calls'],0)
        for ref in run['compilers'].values():self.assertEqual(hashlib.sha256(store.objects[ref['key']]['Body']).hexdigest(),ref['sha256'])
        spec=importlib.util.spec_from_file_location('tenor_cli',ROOT/'scripts/replay_tenor_research.py')
        cli=importlib.util.module_from_spec(spec);spec.loader.exec_module(cli)
        reader=lambda key:store.objects[key]['Body']
        self.assertEqual(cli.replay(result['run'],reader)['status'],'REPRODUCED')
        store.objects[refs[0]['key']]['Body']=b'corrupt'
        with self.assertRaises(Exception):cli.replay(result['run'],reader)

    def test_partial_pagination_is_rejected(self):
        module,_=load();page={'data':[{}],'meta':{'total-pages':1,'total-count':2}}
        module.request=lambda *a:(page,{})
        with self.assertRaisesRegex(ValueError,'row count'):module.fiscal_pages(TODAY)

    def test_archive_corruption_prevents_current_publication(self):
        module,store=load();pages,ff=fixture()
        ref=capture(store,module.BUCKET,'treasury_fiscaldata','https://example.org/auctions',json.dumps(pages[0]).encode())
        store.objects[ref['key']]['Body']=b'corrupt'
        module.fiscal_pages=lambda _:(pages,[ref]);module.fred=lambda _:(None,None)
        with self.assertRaises(Exception):module.lambda_handler({})
        self.assertNotIn(module.OUTPUT_KEY,store.objects)

    def test_missing_original_sources_cannot_be_healthy_or_actionable(self):
        module,store=load()
        def fail(_):raise RuntimeError('source failure')
        module.fiscal_pages=fail;module.fred=fail;module.lambda_handler({})
        out=json.loads(store.objects[module.OUTPUT_KEY]['Body'])
        self.assertEqual(out['quality']['status'],'degraded');self.assertFalse(out['sizing_eligible'])
        self.assertEqual(out['quality']['channels_available'],0);self.assertIsNone(out['any_firing'])
        self.assertFalse(out['reproducibility']['original_response_replayed'])

    def test_older_invocation_cannot_overwrite_current_capture(self):
        module,store=load();new={'generated_at':'2026-09-18T15:00:00+00:00'}
        store.put_object(Key=module.OUTPUT_KEY,Body=json.dumps(new).encode())
        self.assertFalse(module.publish_current({'generated_at':'2026-09-18T14:00:00+00:00'}))
        self.assertEqual(json.loads(store.objects[module.OUTPUT_KEY]['Body']),new)

if __name__=='__main__':unittest.main()
