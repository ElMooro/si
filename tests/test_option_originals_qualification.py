from pathlib import Path
import copy,io,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/ops/staged'))
import ops_5988_option_originals_qualification as audit

class Store:
    def __init__(self,raw):self.raw=raw
    def get_object(self,**kw):return {'Body':io.BytesIO(self.raw)}

class Tests(unittest.TestCase):
    def fixture(self,doc):
        raw=json.dumps(doc).encode();digest=audit.source.sha(raw)
        url=audit.source.next_url(audit.source.initial_url('SPY'),'SPY')
        return Store(raw),{'underlying':'SPY','started_at':'2026-09-21T12:00:00Z','completed_at':'2026-09-21T12:00:01Z',
            'stop':'complete_returned_pagination','pagination_complete':True,'pages':[
                {'page':1,'request_url':url,'request_sha256':audit.source.sha(url.encode()),'status':'received','http_status':200,
                 'original':{'key':audit.PREFIX+digest+'.bin','sha256':digest,'bytes':len(raw)}}]}
    def test_complete_reconstructed_from_original(self):
        store,chain=self.fixture({'status':'OK','results':[{'open_interest':0}]})
        summary=audit.summarize_chain(store,chain)
        self.assertTrue(summary['pagination_complete']);self.assertEqual(summary['returned_rows'],1)
        self.assertEqual(summary['numeric_states']['open_interest'],{'zero':1})
    def test_false_completeness_rejected(self):
        store,chain=self.fixture({'status':'OK','results':[], 'next_url':'https://api.polygon.io/v3/snapshot/options/SPY?cursor=x'})
        with self.assertRaises(AssertionError):audit.summarize_chain(store,chain)
    def test_tampered_original_rejected(self):
        store,chain=self.fixture({'status':'OK','results':[]});store.raw=b'{}'
        with self.assertRaises(AssertionError):audit.summarize_chain(store,chain)
    def test_source_destination_cannot_be_relabelled(self):
        store,chain=self.fixture({'status':'OK','results':[]});chain['underlying']='NVDA'
        with self.assertRaises(AssertionError):audit.summarize_chain(store,chain)
    def test_page_after_terminal_rejected(self):
        store,chain=self.fixture({'status':'OK','results':[]});chain['pages'].append(copy.deepcopy(chain['pages'][0]))
        with self.assertRaises(AssertionError):audit.summarize_chain(store,chain)
    def test_provider_failure_keeps_original_without_completeness(self):
        store,chain=self.fixture({'status':'NOT_AUTHORIZED'})
        chain.update(stop='provider_http_failure',pagination_complete=False);chain['pages'][0]['http_status']=403
        summary=audit.summarize_chain(store,chain)
        self.assertFalse(summary['pagination_complete']);self.assertEqual(summary['returned_rows'],0)

if __name__=='__main__':unittest.main()
