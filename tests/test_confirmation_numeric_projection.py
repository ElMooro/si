from pathlib import Path
import hashlib,json,math,sys,unittest
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/confirmation-numeric';sys.path.insert(0,str(R/'tests'))
from test_confirmation_loader import S3,KEYS,healthy,load

def read(raw,old=False,key=0):
    docs=healthy();docs[KEYS[key]]=raw;client=S3(docs)
    module=load(client,D/'predecessor.py.txt' if old else R/'aws/shared/equity_enrich.py')
    result=module.load_confirmation_feeds(with_availability=True)
    assert client.reads==KEYS and all(stream.closed for stream in client.streams)
    json.dumps(result,allow_nan=False)
    return result

def body(key,token):
    templates=[b'{"contract":"short-interest-tickers.v1","by_ticker":{"TEST":{"short_interest":TOKEN}}}',b'{"aggregate_by_ticker":{"TEST":{"n_funds_holding":TOKEN}}}',b'{"fwd_rev_growth":{"TEST":TOKEN}}',b'{"chains":{"invented":{"expected_catchup_pct":TOKEN,"leader_perf_30d_pct":0,"next_up_tickers":[{"ticker":"TEST","own_30d_pct":0}]}}}']
    return templates[key].replace(b'TOKEN',token.encode())

class Projection(unittest.TestCase):
    def test_predecessor_rounds_an_unrepresentable_significant_digit(self):
        old=read(body(2,'0.100000000000000000001'),True,2)
        self.assertEqual(old[2]['TEST'],0.1)
        self.assertEqual(old[4]['estimate_revisions']['read_status'],'parsed')
    def test_each_real_feed_path_withholds_lossy_values_but_keeps_other_feeds(self):
        for key in range(4):
            for token in ['0.100000000000000000001','9007199254740993.0','1.234567890123456789','1e-1000','1e1000']:
                with self.subTest(key=key,token=token):
                    raw=body(key,token);result=read(raw,key=key);meta=list(result[4].values())[key]
                    self.assertEqual(result[key],{});self.assertEqual(meta['read_status'],'malformed')
                    self.assertEqual(meta['body_sha256'],hashlib.sha256(raw).hexdigest());self.assertEqual(meta['body_bytes'],len(raw))
                    self.assertEqual(sum(m['read_status']=='parsed' for m in result[4].values()),3)
                    for permission in ['original_body_retained','observation_freshness_verified','calls_eligible','sizing_eligible','execution_eligible','forecast_qualified']:self.assertFalse(meta[permission])
    def test_exact_roundtrip_values_including_zero_preserve_whole_results(self):
        for key in range(4):
            for token in ['0.0','-0.0','0e-1000','0.1','1.25','1e2','-12.5','1e-100']:
                with self.subTest(key=key,token=token):
                    raw=body(key,token);self.assertEqual(read(raw,key=key),read(raw,True,key))
    def test_huge_tokens_refuse_whole_projection_without_truncating_hash(self):
        raw=body(2,'0.'+'0'*600+'1');r=read(raw,key=2);m=r[4]['estimate_revisions']
        self.assertEqual(m['read_status'],'malformed');self.assertEqual(m['body_sha256'],hashlib.sha256(raw).hexdigest())
        self.assertEqual(r[2],{})
    def test_unrelated_lossy_member_cannot_hide_behind_valid_measurement(self):
        raw=b'{"fwd_rev_growth":{"TEST":0},"retained_context":0.100000000000000000001}'
        result=read(raw,key=2);self.assertEqual(result[2],{});self.assertEqual(result[4]['estimate_revisions']['read_status'],'malformed')
    def test_unchanged_non_float_and_duplicate_handling(self):
        for raw in [b'{"fwd_rev_growth":{"TEST":100000000000000000000000000000000000000000000000000}}',b'{"fwd_rev_growth":{"TEST":false}}',b'{"fwd_rev_growth":{"TEST":null}}',b'{"fwd_rev_growth":{"TEST":"1e-1000"}}',b'{"x":1,"x":2}',b'{"x":NaN}']:
            self.assertEqual(read(raw,key=2),read(raw,True,2))
    def test_full_predecessor_and_single_exact_edit(self):
        edit=json.loads((D/'edits.json').read_bytes());raw=(D/'predecessor.py.txt').read_bytes()
        self.assertEqual(hashlib.sha256(raw).hexdigest(),edit['predecessor_sha256']);text=raw.decode()
        for old,new in edit['edits']:self.assertEqual(text.count(old),1);text=text.replace(old,new)
        self.assertEqual(text,(R/'aws/shared/equity_enrich.py').read_text(encoding='utf-8'))
        self.assertEqual(hashlib.sha256(text.encode()).hexdigest(),edit['candidate_sha256'])
        prior=json.loads((D/'previous-policy-preservation.json').read_bytes());prior['edits'][edit['target']].extend(edit['edits'])
        self.assertEqual(prior,json.loads((R/'tests/fixtures/no-paid-research/preservation.json').read_bytes()))
        prior=json.loads((D/'previous-financial-preservation.json').read_bytes());prior['edits'].extend(edit['edits']);prior['candidate_sha256']=edit['candidate_sha256']
        self.assertEqual(prior,json.loads((R/'tests/fixtures/financial-missing-zero/edits.json').read_bytes()))

if __name__=='__main__':unittest.main(verbosity=2)
