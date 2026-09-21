from pathlib import Path
from unittest.mock import patch
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'tests')]
import option_flow_research as model
import option_research_rows as codec
from test_option_contract_research import row,page,RECEIVED


def original(doc,blobs,key=None):
    raw=model.encoded(doc);digest=model.sha(raw);path=model.PRIVATE+digest+'.bin';blobs[path]=raw
    return {'key':path,'sha256':digest,'bytes':len(raw),**({'source_key':key} if key else {})}


def fixture():
    blobs={};p=page([row('call'),row('put')]);blobs[p['original']['key']]=p.pop('raw');p.update(status='received',http_status=200)
    source={'underlying':'SPY','started_at':RECEIVED,'completed_at':RECEIVED,'pages':[p],
        'stop':'complete_returned_pagination','pagination_complete':True}
    discovery={key:None for key in model.DISCOVERY}
    inputs={'contract':'option-flow-inputs.v1','generated_at':RECEIVED,'discovery':discovery,
        'universe':model.universe(discovery,blobs.__getitem__),'chains':{'SPY':source},
        'predecessors':{model.LEGACY:original({'all_results':[{'old':True}]},blobs,model.LEGACY),model.CURRENT:None},
        'provider_requests':1,'source_bytes':p['original']['bytes']}
    return blobs,inputs


class ModelTests(unittest.TestCase):
    def setUp(self):self.patch=patch.object(model,'CONTINUITY',('SPY',));self.patch.start()
    def tearDown(self):self.patch.stop()
    def test_public_discovery_preserves_priority_and_deferred_inventory(self):
        blobs={};contexts={k:None for k in model.DISCOVERY}
        contexts[model.DISCOVERY[0]]=original({'alert_tier':['NVDA','SPY','invalid/path','AMD'],'watch_tier':['TSLA']},blobs,model.DISCOVERY[0])
        with patch.object(model,'MAX_UNIVERSE',3):u=model.universe(contexts,blobs.__getitem__)
        self.assertEqual(u['selected'],['SPY','NVDA','AMD']);self.assertEqual(u['deferred'],['TSLA'])
        self.assertEqual(len(u['origins']['SPY']),2);self.assertEqual(u['issues'][0]['reason'],'invalid_symbol')
        self.assertFalse(u['source_contexts'][model.DISCOVERY[0]]['confers_investment_authority'])
    def test_private_source_substitution_rejected(self):
        blobs={};c={k:None for k in model.DISCOVERY};c[model.DISCOVERY[0]]=original({},blobs,'data/trade-tickets.json')
        with self.assertRaises(ValueError):model.universe(c,blobs.__getitem__)
    def test_complete_chain_publication_keeps_exact_zero_and_clocks(self):
        blobs,inputs=fixture();out=model.build(inputs,blobs.__getitem__,blobs.__setitem__)
        chain=model.checked(out['chains']['SPY']['chain'],blobs.__getitem__,'chains')
        block=model.checked(chain['record_blocks'][0]['artifact'],blobs.__getitem__,'rows');rows=codec.unpack(block)
        self.assertEqual(rows[0]['metrics']['open_interest']['value'],'0');self.assertIsNone(rows[0]['open_interest_date'])
        self.assertEqual(out['quality']['returned_rows'],2);self.assertEqual(out['quality']['status'],'descriptive')
        self.assertEqual(len(chain['daily_bar_update_groups']),1);self.assertIsNone(chain['total_session_volume'])
        for k in model.PERMISSIONS:self.assertFalse(out[k])
    def test_body_tampering_rejected_before_build(self):
        blobs,inputs=fixture();p=inputs['chains']['SPY']['pages'][0];blobs[p['original']['key']]+=b' '
        with self.assertRaises(ValueError):model.build(inputs,blobs.__getitem__,blobs.__setitem__)
    def test_universe_cannot_be_replaced_by_caller(self):
        blobs,inputs=fixture();inputs['universe']['selected']=['NVDA']
        with self.assertRaises(ValueError):model.build(inputs,blobs.__getitem__,blobs.__setitem__)
    def test_complete_claim_on_partial_page_rejected(self):
        blobs,inputs=fixture();p=page([row()],next_url='https://api.polygon.io/v3/snapshot/options/SPY?cursor=two')
        blobs[p['original']['key']]=p.pop('raw');p.update(status='received',http_status=200)
        chain=inputs['chains']['SPY'];chain['pages']=[p]
        with self.assertRaises(ValueError):model.reconstruct('SPY',chain,blobs.__getitem__,RECEIVED)
    def test_http_failure_retained_without_zero_oi_inference(self):
        blobs,inputs=fixture();chain=inputs['chains']['SPY'];p=chain['pages'][0]
        p.update(original=original({'status':'NOT_AUTHORIZED'},blobs),http_status=403)
        chain.update(stop='provider_http_failure',pagination_complete=False)
        out=model.reconstruct('SPY',chain,blobs.__getitem__,RECEIVED)
        self.assertEqual(out['research_status'],'source_unavailable');self.assertNotIn('reported_open_interest',out)
        self.assertEqual(len(out['acquisition']['originals']),1)
    def test_partial_prefix_preserved_after_failed_second_page(self):
        blobs,inputs=fixture();url='https://api.polygon.io/v3/snapshot/options/SPY?cursor=two'
        a=page([row()],next_url=url);blobs[a['original']['key']]=a.pop('raw');a.update(status='received',http_status=200)
        b={'page':2,'request_url':url,'request_sha256':model.sha(url.encode()),'acquired_at':RECEIVED,
            'status':'transport_or_body_failure','http_status':None,'original':None}
        chain=inputs['chains']['SPY'];chain.update(pages=[a,b],stop='transport_or_body_failure',pagination_complete=False)
        out=model.reconstruct('SPY',chain,blobs.__getitem__,RECEIVED)
        self.assertEqual(out['coverage']['returned_rows'],1);self.assertEqual(out['research_status'],'partial_returned_snapshot')
        self.assertEqual(out['acquisition']['captured_pages'],2)
    def test_changed_failed_page_request_is_not_unchecked(self):
        blobs,inputs=fixture();chain=inputs['chains']['SPY'];chain['pages'][0].update(status='time_budget',original=None,request_url='https://evil.example')
        chain.update(pagination_complete=False,stop='time_budget')
        with self.assertRaises(ValueError):model.reconstruct('SPY',chain,blobs.__getitem__,RECEIVED)
    def test_capture_clock_outside_packet_rejected(self):
        blobs,inputs=fixture();inputs['chains']['SPY']['completed_at']='2026-09-22T12:00:00Z'
        with self.assertRaises(ValueError):model.build(inputs,blobs.__getitem__,blobs.__setitem__)
    def test_no_cross_date_volume_total(self):
        blobs,inputs=fixture();old=row('call');old['day']['last_updated']=1717099200000000000
        p=page([old,row('put')]);blobs[p['original']['key']]=p.pop('raw');p.update(status='received',http_status=200)
        inputs['chains']['SPY']['pages']=[p];inputs['source_bytes']=p['original']['bytes']
        out=model.build(inputs,blobs.__getitem__,blobs.__setitem__);chain=model.checked(out['chains']['SPY']['chain'],blobs.__getitem__)
        self.assertEqual(len(chain['daily_bar_update_groups']),2);self.assertIsNone(chain['total_session_volume'])
    def test_exact_source_bytes_inventory_required(self):
        blobs,inputs=fixture();inputs['source_bytes']+=1
        with self.assertRaises(ValueError):model.build(inputs,blobs.__getitem__,blobs.__setitem__)


if __name__=='__main__':unittest.main()
