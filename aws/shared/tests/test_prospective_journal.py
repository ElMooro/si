"""A prospective observation cannot be rewritten, backdated or grant capital authority."""
from copy import deepcopy
from datetime import datetime, timezone, timedelta
import io
import json
from pathlib import Path
import sys
import unittest
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from instrument_identity import resolve_instrument
from prospective_journal import projection, ensure_protocol, register, read_record, canonical, digest, validate_record


NOW=datetime(2026,9,18,19,tzinfo=timezone.utc)


class Conflict(Exception):
    response={'Error':{'Code':'PreconditionFailed'}}


class Store:
    def __init__(self): self.rows={}; self.now=NOW
    def put_object(self, **kw):
        if kw.get('IfNoneMatch')=='*' and kw['Key'] in self.rows: raise Conflict()
        self.rows[kw['Key']]={**kw,'LastModified':self.now}
    def get_object(self, **kw):
        obj=self.rows[kw['Key']]
        return {**obj,'Body':io.BytesIO(obj['Body'])}
    def head_object(self, **kw): return self.rows[kw['Key']]


def picks():
    return [{'identity':resolve_instrument('AAA'),'direction':'UP','prediction_origin':'explicit_direction'},
            {'identity':resolve_instrument('BBB'),'direction':'NEUTRAL','prediction_origin':'rank_observation'},
            {'identity':resolve_instrument('BTC','crypto'),'direction':'UP','prediction_origin':'explicit_direction'}]


def source(**changes):
    doc={'generated_at':(NOW-timedelta(minutes=1)).isoformat(),'quality':{'status':'fresh'},
         'private_account':'not allowed in archive','narrative':'must not be copied'}
    doc.update(changes)
    return projection('data/example.json',doc,picks(),'a'*64,NOW.isoformat())


def recorded():
    store=Store(); protocol=ensure_protocol(store,'fixture')
    refs=register(store,'fixture',source(),protocol,'c'*64,NOW)
    return store, protocol, refs


class Journal(unittest.TestCase):
    def test_records_only_explicit_supported_direction_and_fixed_forward_protocol(self):
        store, protocol, refs=recorded()
        self.assertEqual(len(refs),1)
        record=read_record(store,'fixture',refs[0])
        self.assertEqual(record['protocol']['horizons_sessions'],[5,20])
        self.assertEqual(record['registration_date_et'],'2026-09-18')
        self.assertFalse(record['eligibility']['out_of_sample_validation'])
        self.assertNotIn(b'private_account',canonical(record)); self.assertNotIn(b'narrative',canonical(record))
        self.assertNotIn(b'baseline_price',canonical(record))
        self.assertEqual(source()['unsupported_identity_count'],1)

    def test_retry_preserves_first_registration_and_does_not_duplicate(self):
        store, protocol, refs=recorded(); count=len(store.rows)
        again=register(store,'fixture',source(),protocol,'c'*64,NOW+timedelta(minutes=1))
        self.assertFalse(again[0]['created']); self.assertEqual(again[0]['sha256'],refs[0]['sha256'])
        self.assertEqual(len(store.rows),count)

    def test_private_aliases_and_unknown_text_cannot_escape_projection(self):
        for key in ('data/ai-brief.json','data/user-watchlist.json','data/../account.json','portfolio/snapshot.json'):
            with self.assertRaises(ValueError): projection(key,{},picks(),'a'*64,NOW.isoformat())
        record=source(); encoded=canonical(record)
        self.assertNotIn(b'must not be copied',encoded)
        self.assertNotIn(b'not allowed in archive',encoded)

    def test_missing_stale_future_or_error_source_cannot_register(self):
        for doc in ({'generated_at':None},{'generated_at':(NOW-timedelta(days=2)).isoformat()},
                    {'generated_at':(NOW+timedelta(seconds=1)).isoformat()},{'quality':{'status':'error'}}):
            store=Store(); protocol=ensure_protocol(store,'fixture')
            self.assertEqual(register(store,'fixture',source(**doc),protocol,'c'*64,NOW),[])

    def test_protocol_must_exist_before_observation_registration(self):
        store, protocol, _=recorded(); protocol['first_stored_at']=(NOW+timedelta(seconds=1)).isoformat()
        with self.assertRaises(ValueError): register(store,'fixture',source(),protocol,'c'*64,NOW)

    def test_storage_clock_detects_backdating_even_with_valid_json_hash(self):
        store, _, refs=recorded()
        store.rows[refs[0]['key']]['LastModified']=NOW+timedelta(days=1)
        with self.assertRaises(ValueError): read_record(store,'fixture',refs[0])

    def test_tampered_record_protocol_or_authority_never_pass(self):
        for mutation in ('price','protocol','authority','newfield','id','source'):
            store, _, refs=recorded(); rec=json.loads(store.rows[refs[0]['key']]['Body'])
            if mutation=='price': rec['observation']['direction']='DOWN'
            elif mutation=='protocol': rec['protocol']['horizons_sessions']=[1]
            elif mutation=='authority': rec['eligibility']['sizing_eligible']=True
            elif mutation=='newfield': rec['account']='sensitive'
            elif mutation=='id': rec['forecast_id']='f'*64
            else: rec['source']['source_key']='data/ai-brief.json'
            with self.assertRaises(ValueError): validate_record(rec)
        store,protocol,refs=recorded();store.rows[protocol['key']]['Body']=b'{}'
        with self.assertRaises(ValueError): read_record(store,'fixture',refs[0])

    def test_protocol_and_observation_survive_timezone_boundary(self):
        store=Store(); store.now=datetime(2026,9,19,1,tzinfo=timezone.utc)
        doc=projection('data/example.json',{'generated_at':store.now.isoformat()},picks(),'b'*64,store.now.isoformat())
        refs=register(store,'fixture',doc,ensure_protocol(store,'fixture'),'c'*64,store.now)
        self.assertEqual(read_record(store,'fixture',refs[0])['registration_date_et'],'2026-09-18')


if __name__=='__main__': unittest.main()
