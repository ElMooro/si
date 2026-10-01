"""Whole native directory builds with complete invented acquisition boundaries."""
from pathlib import Path
from copy import deepcopy
from types import ModuleType,SimpleNamespace
from unittest.mock import patch
import array,base64,gzip,json,pickle,sys,unittest
HERE=Path(__file__).resolve().parent
EXTERNAL=(HERE/'directory_identity.py').exists()
ROOT=HERE.parent/'si-batch-improvements' if EXTERNAL else HERE.parent
SOURCE=HERE if EXTERNAL else ROOT/'aws/lambdas/justhodl-symdir/source'
FIXTURE=ROOT/'tests/fixtures/symbol-directory'
sys.path.insert(0,str(SOURCE))


def build(document=None,use_default=True,predecessor=False):
    evidence=json.loads((FIXTURE/'build-reproduction.json').read_bytes())
    docs=deepcopy(evidence['invented_inputs'])
    if not use_default:docs['data/symbology/master.json']=deepcopy(document)
    before=deepcopy(docs);reads=[];lists=[];requests=[];puts={}
    class Storage:
        def put_object(self,**kw):puts[kw['Key']]=kw
        def list_objects_v2(self,**kw):return {}
    client=Storage();m=ModuleType('whole_directory_test');m.__file__='/invented/symdir/lambda_function.py'
    source=FIXTURE/'pre-integrity-lambda.py.txt' if predecessor else SOURCE/'lambda_function.py'
    with patch.dict(sys.modules,{'boto3':SimpleNamespace(client=lambda *a,**kw:client),'botocore.config':SimpleNamespace(Config=lambda **kw:kw)}):
        exec(compile(source.read_bytes(),m.__file__,'exec'),m.__dict__)
    m.POLYGON_KEY='invented-key';m.FRED_KEY='';m.BLS_KEY='';m.BUCKET='invented-bucket'
    def get_json(key,default=None):
        reads.append(key)
        if key not in evidence['reads']:raise AssertionError('Undeclared fixture source')
        return deepcopy(docs.get(key,default))
    def get_bytes(key,rng=None):
        reads.append(key)
        if key=='data/warm/ofr-fsi/fsi.csv':return b'Date,OFR FSI,Invented component\n2000-01-01,1,2\n'
        raise AssertionError('Undeclared raw fixture source')
    def list_keys(prefix,max_keys=None):
        lists.append(prefix)
        if prefix not in evidence['list_prefixes']:raise AssertionError('Undeclared fixture listing')
        return []
    def http_json(url,**kw):
        requests.append(url)
        if url not in evidence['invented_http_requests']:raise AssertionError('Undeclared fixture request')
        if url.startswith('https://api.polygon.io/v3/reference/tickers?'):
            return {'results':[deepcopy(evidence['invented_polygon_row'])] if 'market=stocks&' in url else []}
        raise ValueError('Invented absent provider fixture')
    m._get_json=get_json;m._get=get_bytes;m._list=list_keys;m._http_json=http_json
    m._http=lambda *a,**kw:(_ for _ in ()).throw(AssertionError('Real HTTP forbidden'))
    m._iso=lambda:'2000-01-01T00:00:00+00:00'
    with patch('time.time',return_value=946684800.0),patch('urllib.request.urlopen',side_effect=AssertionError('Real HTTP forbidden')):
        result=m.lambda_handler({'mode':'build'},SimpleNamespace(get_remaining_time_in_millis=lambda:900000))
    assert reads==evidence['reads'] and lists==evidence['list_prefixes'] and requests==evidence['invented_http_requests']
    assert docs==before
    indexed=pickle.loads(gzip.decompress(puts['data/symdir/docs.pkl.gz']['Body']))
    return m,result,indexed,puts,evidence


class Tests(unittest.TestCase):
    def test_whole_handler_keeps_reference_instrument_without_borrowing_other_issuer_isin(self):
        m,report,indexed,puts,_=build();row=next(r for r in indexed['docs'] if r[0]=='ZZTEST')
        self.assertEqual(row[m.D_TITLE],'Invented replacement issuer');extra=row[m.D_EXTRA]
        self.assertIsNone(extra['isin']);self.assertEqual(extra['identifier_evidence']['status'],'conflicting_issuer')
        self.assertEqual(extra['identifier_evidence']['reported_isin'],'US0000000000')
        self.assertNotIn('isin',m.doc_row(row));self.assertNotIn('instruments',report['errors'])
    def test_malformed_optional_row_cannot_remove_primary_reference_instrument(self):
        for bad in (None,5,True,[]):
            m,report,indexed,_,_=build({'by_ticker':{'ZZTEST':bad}},False)
            row=next(r for r in indexed['docs'] if r[0]=='ZZTEST')
            self.assertEqual(row[m.D_EXTRA]['identifier_evidence']['status'],'invalid_optional_row')
            self.assertNotIn('instruments',report['errors']);self.assertEqual(report['sources']['instruments']['counts'],{'stocks':1})
    def test_missing_optional_population_is_unknown_while_primary_instruments_survive(self):
        for bad in (None,[],{'by_ticker':[]}):
            m,report,indexed,_,_=build(bad,False)
            self.assertTrue(any(r[0]=='ZZTEST' for r in indexed['docs']))
            self.assertIsNone(report['sources']['instruments']['symbology']);self.assertNotIn('instruments',report['errors'])
    def test_full_predecessor_index_outputs_match_retained_complete_artifacts(self):
        m,report,indexed,puts,evidence=build(predecessor=True)
        original=evidence['whole_writes'];self.assertEqual(set(puts),set(original))
        for key in puts:
            expected=base64.b64decode(original[key]['body_base64']);actual=puts[key]['Body']
            if key.endswith('.pkl.gz'):
                self.assertEqual(pickle.loads(gzip.decompress(actual)),pickle.loads(gzip.decompress(expected)))
            elif key.endswith('.json.gz'):
                self.assertEqual(json.loads(gzip.decompress(actual)),json.loads(gzip.decompress(expected)))
            elif not key.endswith('/manifest.json'):
                self.assertEqual(json.loads(actual),json.loads(expected))
            else:
                a,b=json.loads(actual),json.loads(expected)
                # Check each serialization's byte accounting against its own full
                # bodies. Hash-seed dependent set/pickle ordering changes compressed
                # size even when all decoded structures above are equal.
                sizes={'docs_pkl_gz':'data/symdir/docs.pkl.gz',
                       'index_pkl_gz':'data/symdir/index.pkl.gz',
                       'instruments_json_gz':'data/symdir/instruments.json.gz'}
                self.assertEqual(a.pop('bytes'),{name:len(puts[path]['Body']) for name,path in sizes.items()})
                self.assertEqual(b.pop('bytes'),{name:len(base64.b64decode(original[path]['body_base64'])) for name,path in sizes.items()})
                # Compare the remaining whole manifest except environment-specific
                # traceback text. Full original traces remain in the fixture.
                for doc in (a,b):doc['errors']={k:v.split(' | ',1)[0] for k,v in doc['errors'].items()}
                self.assertEqual(a,b)
        row=next(r for r in indexed['docs'] if r[0]=='ZZTEST');self.assertEqual(row[m.D_EXTRA]['isin'],'US0000000000')


if __name__=='__main__':unittest.main()
