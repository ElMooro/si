from pathlib import Path
from datetime import date,datetime,timezone,timedelta
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import futures_research_model as m
C=m.capture;ASOF='2026-09-21';START='2026-06-23';STAMP='2026-09-21T19:00:00+00:00';GENERATED='2026-09-21T19:01:00+00:00'


def fixture():
    originals={};sources={}
    def add(kind,product,rows,ticker=None):
        scope=C.spec(kind,product,START,ASOF,ticker);url=C.initial(scope)
        raw=m.encoded({'status':'OK','results':rows});ref={'key':m.PRIVATE+m.sha(raw)+'.bin','sha256':m.sha(raw),'bytes':len(raw)}
        originals[ref['key']]=raw;name=product+':'+kind+(':'+ticker if ticker else '')
        sources[name]={'contract':C.CONTRACT,'scope':scope,'started_at':STAMP,'completed_at':STAMP,
            'pages':[{'status':'received','page':1,'request_url':url,'request_sha256':m.sha(url.encode()),'acquired_at':STAMP,'http_status':200,'original':ref}],
            'stop':'complete_returned_pagination','pagination_complete':True}
    for product,venue in C.PRODUCTS.items():
        unit,noun=m.UNITS[product]
        add('products',product,[{'product_code':product,'date':ASOF,'trading_venue':venue,'type':'single',
            'price_quotation':'U.S. dollars and cents per '+noun,'unit_of_measure':unit,'unit_of_measure_qty':1000,
            'trade_currency_code':'USD','settlement_currency_code':'USD'}])
        contracts=[]
        for suffix,expiry in [('Z6','2026-12-18'),('H7','2027-03-19')]:
            ticker=product+suffix
            contracts.append({'ticker':ticker,'product_code':product,'trading_venue':venue,'type':'single','active':True,'date':ASOF,
                'first_trade_date':'2025-01-01','last_trade_date':expiry,'settlement_date':expiry})
            bars=[]
            for i in range(23):
                day=date(2026,8,1)+timedelta(days=i*2);px=100+i+(1 if suffix=='H7' else 0)
                ns=int(datetime.combine(day,datetime.min.time(),timezone.utc).timestamp())*1000000000+1
                bars.append({'ticker':ticker,'session_end_date':day.isoformat(),'window_start':ns,'open':px,'high':px+1,
                    'low':px-1,'close':px,'settlement_price':px+1,'volume':10,'dollar_volume':px*10,'transactions':5})
            add('bars',product,bars,ticker)
        add('contracts',product,contracts);add('schedules',product,[])
    return originals,sources


def compile_(blobs,sources):
    emitted={}
    def emit(kind,doc):
        raw=m.encoded(doc);ref=m.ref(raw,kind);emitted[ref['key']]=raw;return ref
    return m.compile_output(sources,GENERATED,blobs.__getitem__,emit),emitted


def change(blobs,sources,name,mutate):
    page=sources[name]['pages'][0];doc=json.loads(blobs[page['original']['key']]);mutate(doc)
    raw=m.encoded(doc);ref={'key':m.PRIVATE+m.sha(raw)+'.bin','sha256':m.sha(raw),'bytes':len(raw)}
    blobs[ref['key']]=raw;page['original']=ref


def add_calendar(blobs,sources,product='ES'):
    names=[name for name in sources if name.startswith(product+':bars:')]
    for name in names:
        change(blobs,sources,name,lambda d:d['results'][-1].update(session_end_date=ASOF,
            window_start=int(datetime(2026,9,21,tzinfo=timezone.utc).timestamp())*1000000000+1))
    page=sources[names[0]]['pages'][0];bars=json.loads(blobs[page['original']['key']])['results'];events=[]
    for row in bars:
        day=date.fromisoformat(row['session_end_date'])
        for event,stamp in [('open',(day-timedelta(days=1)).isoformat()+'T22:00:00Z'),('close',day.isoformat()+'T21:00:00Z')]:
            events.append({'product_code':product,'trading_venue':C.PRODUCTS[product],'session_end_date':day.isoformat(),'event':event,'timestamp':stamp})
    change(blobs,sources,product+':schedules',lambda d:d.update(results=events*3))


class Tests(unittest.TestCase):
    def test_every_original_row_is_retained_and_exact_changes_have_dated_endpoints(self):
        blobs,sources=fixture();out,emitted=compile_(blobs,sources)
        self.assertEqual(out['quality']['selected_contracts'],14);self.assertEqual(out['quality']['complete_datasets'],35)
        row=out['products']['ES']['contracts'][0];v=row['comparisons']['close']['5']
        self.assertEqual(v['elapsed_calendar_days'],10);self.assertEqual(v['absolute_change_decimal'],'5')
        self.assertEqual(v['percent_change_exact'],{'numerator':'500','denominator':'117'})
        self.assertEqual(row['latest_reported_row']['window_start_ns'][-1],'1')
        count=sum(len(json.loads(raw)['rows']) for raw in emitted.values());self.assertEqual(count,343)
        for key in m.FLAGS:self.assertIs(out[key],False)
    def test_matching_curve_uses_same_session_and_separate_price_fields(self):
        blobs,sources=fixture();change(blobs,sources,'ES:bars:ESH7',lambda d:d['results'].pop())
        out,_=compile_(blobs,sources);curve=out['products']['ES']['matched_curves'][0]
        self.assertEqual(curve['near']['session_end_date'],curve['far']['session_end_date'])
        self.assertNotEqual(curve['session_end_date'],curve['latest_near_session'])
        self.assertEqual(curve['far_minus_near_decimal'],'1');self.assertFalse(curve['bar_finality_independently_verified'])
        self.assertEqual(out['products']['ES']['matched_curves'][1]['price_field'],'settlement_price')
    def test_latest_common_missing_settlement_is_not_replaced_with_close_or_older_settlement(self):
        blobs,sources=fixture();change(blobs,sources,'ES:bars:ESH7',lambda d:d['results'][-1].pop('settlement_price'))
        out,_=compile_(blobs,sources);curves=out['products']['ES']['matched_curves']
        self.assertTrue(curves[0]['available']);self.assertFalse(curves[1]['available']);self.assertEqual(curves[1]['reason'],'common_session_price_unavailable')
    def test_negative_and_zero_settlements_remain_valid_absolute_measurements(self):
        blobs,sources=fixture()
        def edit(doc):
            doc['results'][-2]['settlement_price']=0;doc['results'][-1]['settlement_price']=-37
        change(blobs,sources,'CL:bars:CLZ6',edit);out,_=compile_(blobs,sources)
        value=out['products']['CL']['contracts'][0]['comparisons']['settlement_price']['1']
        self.assertTrue(value['available']);self.assertEqual(value['absolute_change_decimal'],'-37')
        self.assertIsNone(value['percent_change_decimal']);self.assertEqual(value['percent_change_reason'],'nonpositive_base_percent_withheld')
    def test_bad_identity_duplicate_dates_or_conflicting_chronology_withhold(self):
        mutations=[lambda r:r[-1].update(ticker='CL'),lambda r:r[-1].update(session_end_date=r[-2]['session_end_date']),
            lambda r:r[-1].update(window_start=r[-2]['window_start']),lambda r:r[-1].update(window_start=10),
            lambda r:r[-1].update(window_start=r[0]['window_start']-1)]
        for edit in mutations:
            blobs,sources=fixture();change(blobs,sources,'ES:bars:ESZ6',lambda d:edit(d['results']))
            out,_=compile_(blobs,sources);row=out['products']['ES']['contracts'][0]
            self.assertFalse(row['chronology_unambiguous']);self.assertFalse(row['comparisons']['close']['1']['available'])
    def test_inconsistent_ohlc_does_not_invalidate_separately_reported_settlement(self):
        blobs,sources=fixture();change(blobs,sources,'ES:bars:ESZ6',lambda d:d['results'][-1].update(high=1))
        out,_=compile_(blobs,sources);r=out['products']['ES']['contracts'][0]
        self.assertFalse(r['comparisons']['close']['1']['available']);self.assertTrue(r['comparisons']['settlement_price']['1']['available'])
    def test_missing_unit_or_wrong_vintage_never_creates_conversion(self):
        for update in ({'price_quotation':None},{'trade_currency_code':'EUR'},{'date':'2025-01-01'},{'unit_of_measure_qty':0}):
            blobs,sources=fixture();change(blobs,sources,'CL:products',lambda d:d['results'][0].update(update))
            out,_=compile_(blobs,sources);p=out['products']['CL'];self.assertFalse(p['specification']['quantity_conversion_qualified'])
            self.assertFalse(p['matched_curves'][0]['available'])
    def test_original_hash_or_reference_tampering_fails(self):
        blobs,sources=fixture();page=sources['ES:products']['pages'][0];blobs[page['original']['key']]=b'{}'
        with self.assertRaises(ValueError):compile_(blobs,sources)
        page['original']['key']='data/trade-tickets.json'
        with self.assertRaises(ValueError):compile_(blobs,sources)
    def test_request_clock_scope_and_completeness_tampering_fails(self):
        for edit in (lambda s:s.update(pagination_complete=False),lambda s:s['pages'][0].update(request_sha256='0'*64),
            lambda s:s['pages'][0].update(acquired_at='2026-09-22T00:00:00Z'),lambda s:s['scope'].update(product='NQ')):
            blobs,sources=fixture();edit(sources['ES:products'])
            with self.assertRaises(ValueError):compile_(blobs,sources)
    def test_absent_or_extra_dataset_cannot_be_silently_ignored(self):
        for name in ('ES:products','ES:bars:ESZ6'):
            blobs,sources=fixture();sources.pop(name)
            with self.assertRaises(ValueError):compile_(blobs,sources)
        blobs,sources=fixture();sources['unreviewed']=sources['ES:products']
        with self.assertRaises(ValueError):compile_(blobs,sources)
    def test_mutated_emitted_records_rejected(self):
        blobs,sources=fixture()
        with self.assertRaises(ValueError):m.compile_output(sources,GENERATED,blobs.__getitem__,lambda kind,doc:{})
    def test_bar_outside_contract_trade_dates_is_not_a_return_endpoint(self):
        blobs,sources=fixture();change(blobs,sources,'ES:contracts',lambda d:d['results'][0].update(first_trade_date='2026-09-01'))
        out,_=compile_(blobs,sources)
        self.assertFalse(out['products']['ES']['contracts'][0]['chronology_unambiguous'])
    def test_sparse_history_keeps_row_count_and_span(self):
        blobs,sources=fixture();change(blobs,sources,'ES:bars:ESZ6',lambda d:d.update(results=d['results'][-2:]))
        out,_=compile_(blobs,sources);r=out['products']['ES']['contracts'][0]
        self.assertEqual(r['coverage']['returned_rows'],2);self.assertTrue(r['comparisons']['close']['1']['available'])
        self.assertFalse(r['comparisons']['close']['5']['available']);self.assertEqual(r['comparisons']['close']['1']['elapsed_calendar_days'],2)
    def test_completed_session_view_excludes_partial_latest_without_dropping_original(self):
        blobs,sources=fixture();add_calendar(blobs,sources);out,_=compile_(blobs,sources)
        p=out['products']['ES'];r=p['contracts'][0]
        self.assertEqual(p['session_calendar']['repeated_event_identity_rows'],92)
        self.assertEqual(r['coverage']['returned_rows'],23);self.assertEqual(r['coverage']['scheduled_ended_rows'],22)
        self.assertEqual(r['coverage']['scheduled_open_rows'],1);self.assertEqual(r['coverage']['unqualified_calendar_rows'],0)
        self.assertEqual(r['comparisons']['close']['1']['to']['ordinal'],22)
        self.assertEqual(r['scheduled_ended_comparisons']['close']['1']['to']['ordinal'],21)
        self.assertIs(r['latest_reported_row']['session_status']['scheduled_session_ended_by_capture'],False)
    def test_calendar_ambiguity_withholds_completed_comparison_but_retains_reported_data(self):
        blobs,sources=fixture();add_calendar(blobs,sources)
        change(blobs,sources,'ES:schedules',lambda d:d['results'][0].update(trading_venue='WRONG'))
        out,_=compile_(blobs,sources);r=out['products']['ES']['contracts'][0]
        self.assertTrue(r['comparisons']['close']['1']['available']);self.assertFalse(r['scheduled_ended_comparisons']['close']['1']['available'])
if __name__=='__main__':unittest.main(verbosity=2)
