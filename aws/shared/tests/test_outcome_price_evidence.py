"""Original bytes, request identity, session clocks, units and cache regressions."""
from copy import deepcopy
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
import unittest
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from evidence_store import capture
from outcome_price_evidence import PriceEvidenceVerifier
from outcome_integrity import assess_outcome


class Store:
    def __init__(self): self.rows={}; self.reads=0
    def put_object(self,**kw):
        self.rows[kw['Key']]={**kw,'LastModified':datetime(2026,9,4,5,tzinfo=timezone.utc)}
    def get_object(self,**kw):
        self.reads+=1
        obj=self.rows[kw['Key']]
        return {**obj,'Body':io.BytesIO(obj['Body'])}


def fixture():
    store=Store()
    times=[int(datetime(2026,9,day,4,tzinfo=timezone.utc).timestamp()*1000) for day in (2,3)]
    raw=json.dumps({'ticker':'AAA','status':'OK','adjusted':True,'results':[{'t':t,'c':p} for t,p in zip(times,(100,120))]}).encode()
    receipt=capture(store,'fixture','polygon','https://api.polygon.io/v2/aggs/ticker/AAA/range/1/day/2026-09-02/2026-09-03?adjusted=true&apiKey=synthetic',raw,datetime(2026,9,4,5,tzinfo=timezone.utc))
    marks=[]
    for day,t,price in zip((2,3),times,(100,120)):
        marks.append({'price':price,'observed_at':f'2026-09-{day+1:02}T04:00:00+00:00','as_of':f'2026-09-{day:02}',
            'bar_timestamp_ms':t,'provider':'polygon','instrument_id':'equity:US:AAA','symbol':'AAA','currency':'USD',
            'evidence':deepcopy(receipt),'evidence_sha256':receipt['sha256'],'evidence_parser':'polygon-us-daily-close.v1',
            'observation_time_basis':'aggregate_period_end_not_trade_time',
            'adjustment_basis':'split_adjusted_price','adjustment_vintage':receipt['sha256']})
    row={'signal_type':'fixture','predicted_dir':'UP','prediction_origin':'explicit_direction','logged_at':'2026-09-01T12:00:00+00:00',
        'outcome':{'lineage_contract':'outcome-lineage.v1','return_pct':20,'entry_marks':{'asset':marks[0]},'marks':{'asset':marks[1]}}}
    return store,marks,row


class Evidence(unittest.TestCase):
    def test_actual_parser_recomputes_marks_with_one_cached_read(self):
        store,marks,row=fixture(); verifier=PriceEvidenceVerifier(store,'fixture')
        result=assess_outcome(row,verifier)
        self.assertTrue(result['verified'],result)
        self.assertEqual(verifier.stats['marks_verified'],2); self.assertEqual(store.reads,1)
        self.assertFalse(result['sizing_eligible'])

    def test_hash_shape_and_structural_lineage_are_insufficient(self):
        _,_,row=fixture(); result=assess_outcome(row)
        self.assertTrue(result['lineage_valid']); self.assertFalse(result['verified'])
        self.assertIn('price_archive_not_verified',result['reasons'])

    def test_tampered_missing_archive_never_passes(self):
        store,marks,_=fixture(); key=marks[0]['evidence']['key']
        store.rows[key]['Body']=gzip.compress(b'{}')
        self.assertTrue(PriceEvidenceVerifier(store,'fixture')(marks[0]))
        store.rows={}
        self.assertTrue(PriceEvidenceVerifier(store,'fixture')(marks[0]))

    def test_wrong_claimed_price_time_session_vintage_currency_or_instrument(self):
        for key,bad in [('price',1000),('observed_at','2026-09-03T20:00:00Z'),('as_of','2026-09-01'),
                        ('adjustment_vintage','a'*64),('currency','EUR'),('instrument_id','crypto:BTC/USD'),
                        ('observation_time_basis','trade_time'),('bar_timestamp_ms',0),('evidence_parser','arbitrary')]:
            store,marks,_=fixture(); marks[0][key]=bad
            self.assertTrue(PriceEvidenceVerifier(store,'fixture')(marks[0]),key)

    def test_receipt_timestamp_path_and_metadata_are_checked(self):
        for key,bad in [('first_received_at','2026-01-01T00:00:00Z'),('key','portfolio/risk.json'),('captured',False),('bytes',1)]:
            store,marks,_=fixture(); marks[0]['evidence'][key]=bad
            self.assertTrue(PriceEvidenceVerifier(store,'fixture')(marks[0]),key)
        store,marks,_=fixture(); store.rows[marks[0]['evidence']['key']]['Metadata']['source_url']='https://example.com'
        self.assertTrue(PriceEvidenceVerifier(store,'fixture')(marks[0]))

    def test_cached_receipt_cannot_skip_capture_contract(self):
        store,marks,_=fixture(); verifier=PriceEvidenceVerifier(store,'fixture')
        self.assertFalse(verifier(marks[0])); marks[0]['evidence']['captured']=False
        self.assertTrue(verifier(marks[0]))

    def test_capture_before_completed_daily_period_cannot_validate_final_price(self):
        store,marks,_=fixture(); row=store.rows[marks[1]['evidence']['key']]
        row['Metadata']['received_at']='2026-09-03T18:00:00+00:00'
        row['LastModified']=datetime(2026,9,3,18,tzinfo=timezone.utc)
        marks[1]['evidence']['first_received_at']=row['Metadata']['received_at']
        self.assertEqual(PriceEvidenceVerifier(store,'fixture')(marks[1]),['price_evidence_period_not_complete_at_capture'])


if __name__=='__main__': unittest.main()
