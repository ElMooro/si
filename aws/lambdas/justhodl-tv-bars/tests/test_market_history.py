import base64,gzip,hashlib,importlib.util,io,json,socket,sys,types,unittest
from pathlib import Path
from unittest.mock import Mock,patch
from urllib.error import HTTPError
import market_history_integrity as integrity

SOURCE=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
def load():
    spec=importlib.util.spec_from_file_location('tvbars_offline_test',SOURCE)
    module=importlib.util.module_from_spec(spec)
    with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**kw:Mock())}):spec.loader.exec_module(module)
    return module

class Missing(Exception):response={'Error':{'Code':'NoSuchKey'}}
class Denied(Exception):response={'Error':{'Code':'AccessDenied'}}
ROW=[946684800,1,2,.5,1.5,None]

class HistoryTests(unittest.TestCase):
    def test_index_state_and_mapping_reads_fail_closed(self):
        m=load();m.s3.get_object.side_effect=Denied()
        with self.assertRaises(Denied):m._gj('invented-index',{})
        with self.assertRaises(Denied):m.tv_to_yahoo('NYSE:A')
        m.s3.put_object.assert_not_called()
    def test_denial_stops_the_rest_of_an_on_demand_batch(self):
        m=load();m._session=Mock(return_value=('fixture','fixture'));m._gj=Mock(return_value={'symbols':{}})
        m.bank_symbol=Mock(side_effect=integrity.ProviderDenied('invented denial'))
        with patch.object(m.time,'sleep'):got=m.universe_pull({'tv_symbols':['NYSE:A','NYSE:B']},None)
        self.assertEqual(m.bank_symbol.call_count,1);self.assertFalse(got['results']['NYSE:A']['ok'])
        self.assertNotIn('NYSE:B',got['results'])
    def test_only_authenticated_missing_is_empty(self):
        s=Mock();s.get_object.side_effect=Missing();self.assertEqual(integrity.read_bank(s,'b','k'),{})
        for error in [Denied(),RuntimeError('network')]:
            s.get_object.side_effect=error
            with self.assertRaises(type(error)):integrity.read_bank(s,'b','k')
    def test_corrupt_existing_bank_never_becomes_empty(self):
        for body in [b'bad',gzip.compress(b'{}'),gzip.compress(b'[]')]:
            s=Mock();s.get_object.return_value={'Body':io.BytesIO(body)}
            with self.assertRaises(Exception):integrity.read_bank(s,'b','k')
    def test_denied_bank_stops_before_provider_and_write(self):
        m=load();m.s3.get_object.side_effect=Denied();m.pull=Mock();m.yahoo_bars=Mock()
        with self.assertRaises(Denied):m.bank_symbol('NYSE:A','fixture-token','fixture-cookie')
        m.pull.assert_not_called();m.yahoo_bars.assert_not_called();m.s3.put_object.assert_not_called()
    def test_new_source_never_relabels_legacy_history(self):
        old={'bars':[ROW],'source':'old-attribution','custom':'retained'}
        new=[978307200,2,3,1,2.5,0]
        got=integrity.merged_document(old,[new],'X','X','yahoo-chart:XYZ')
        self.assertEqual(got['bars'],[ROW,new]);self.assertEqual(got['bar_sources'],['legacy:unverified','yahoo-chart:XYZ'])
        self.assertEqual(got['source'],'mixed-bank:unqualified');self.assertEqual(got['custom'],'retained')
        self.assertEqual(old['bars'],[ROW]);self.assertEqual(got['market_history_quality']['legacy_rows'],1)
        for flag in ('provider_identity_verified','cross_provider_equivalence_verified','full_history_verified','raw_upstream_replay_verified','point_in_time_verified'):
            self.assertFalse(got['market_history_quality'][flag])
    def test_corrections_counted_and_other_timestamps_preserved(self):
        first=integrity.merged_document({},[ROW],'X','X','tv')
        corrected=[*ROW];corrected[4]=1.75
        got=integrity.merged_document(first,[corrected],'X','X','yahoo-chart:XYZ')
        self.assertEqual(got['market_history_quality']['revised_rows'],1)
        self.assertEqual(got['bar_sources'],['yahoo-chart:XYZ']);self.assertEqual(first['bars'],[ROW])
    def test_zero_negative_prices_allowed_but_bad_rows_not_invented(self):
        self.assertTrue(integrity.valid_bar([0,-1,0,-2,0,None]))
        for row in [[*ROW[:2],None,*ROW[3:]], [True,*ROW[1:]], [*ROW[:4],float('nan'),None],[*ROW[:5],-1]]:
            self.assertFalse(integrity.valid_bar(row))
            with self.assertRaises(ValueError):integrity.merged_document({},[row],'X','X','tv')
        with self.assertRaises(ValueError):integrity.merged_document({},[ROW,ROW],'X','X','tv')
    def test_tv_explicit_denial_has_no_host_or_yahoo_fallback(self):
        m=load();m.WS=Mock(side_effect=integrity.ProviderDenied('denied'))
        with self.assertRaises(integrity.ProviderDenied):m._connect('fixture')
        self.assertEqual(m.WS.call_count,1)
        m.s3.get_object.side_effect=Missing();m.pull=Mock(side_effect=integrity.ProviderDenied('denied'));m.yahoo_bars=Mock()
        with self.assertRaises(integrity.ProviderDenied):m.bank_symbol('NYSE:A','fixture','fixture')
        m.yahoo_bars.assert_not_called();m.s3.put_object.assert_not_called()
    def test_yahoo_denial_stops_other_candidates(self):
        for status in (401,403,429):
            m=load();m.s3.get_object.side_effect=Missing();m.tv_to_yahoo=Mock(return_value=['ABC','DEF']);m.yahoo_bars=Mock(side_effect=HTTPError('https://example.invalid',status,'denied',{},None))
            with self.assertRaises(integrity.ProviderDenied):m.bank_symbol('NYSE:A',None,None)
            self.assertEqual(m.yahoo_bars.call_count,1);m.s3.put_object.assert_not_called()
    def test_yahoo_missing_extremes_are_not_filled_with_close_or_missing_volume_zeroed(self):
        m=load();payload={'chart':{'result':[{'timestamp':[946684800,978307200], 'indicators':{'quote':[{'open':[1,1],'high':[None,2],'low':[.5,.5],'close':[1.5,1.5],'volume':[None,None]}]}}]}}
        response=Mock();response.__enter__=Mock(return_value=response);response.__exit__=Mock(return_value=False);response.read.return_value=json.dumps(payload).encode()
        with patch('urllib.request.urlopen',return_value=response):got=m.yahoo_bars('ABC')
        self.assertEqual(got,[[978307200,1,2,.5,1.5,None]])
    def test_bank_writes_complete_custody_document(self):
        m=load();m.s3.get_object.side_effect=Missing();m.pull=Mock(return_value=[ROW])
        doc,key=m.bank_symbol('NYSE:A','fixture','fixture')
        written=json.loads(gzip.decompress(m.s3.put_object.call_args.kwargs['Body']))
        self.assertEqual(doc,written);self.assertEqual(doc['bar_sources'],['tradingview-ws (session-auth, server-side)'])
        self.assertFalse(doc['market_history_quality']['raw_upstream_replay_verified'])
    def test_websocket_upgrade_requires_correct_accept(self):
        key='invented-nonce';accept=base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()
        header=('HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: keep-alive, Upgrade\r\nSec-WebSocket-Accept: '+accept).encode()
        integrity.validate_upgrade(header,key)
        with self.assertRaises(ValueError):integrity.validate_upgrade(header,'wrong')
        with self.assertRaises(ValueError):integrity.validate_upgrade(header+b'\r\nSec-WebSocket-Extensions: permessage-deflate',key)
        for status in ('401','403','429','400','1010'):
            with self.assertRaises(integrity.ProviderDenied):integrity.validate_upgrade(('HTTP/1.1 '+status+' refused').encode(),key)

if __name__=='__main__':unittest.main()
