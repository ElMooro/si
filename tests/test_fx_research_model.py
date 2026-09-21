from pathlib import Path
from datetime import datetime,timezone,timedelta
from decimal import Decimal,localcontext,ROUND_DOWN
from fractions import Fraction
from unittest.mock import Mock
import copy,json,sys,unittest
ROOT=Path(__file__).resolve().parents[1];sys.path.insert(0,str(ROOT/'aws/shared'))
import fx_quote_capture as capture
import fx_research_model as model
GENERATED='2026-09-21T18:01:00+00:00'


def fixture(rows=None):
    if rows is None:
        first=int(datetime(2026,8,25,tzinfo=timezone.utc).timestamp()*1000)
        rows=[{'t':first+i*86400000,'o':100+i,'c':100+i,'h':101+i,'l':99+i,'v':0,'n':0,'vw':100+i} for i in range(23)]
    originals={};sources={}
    for pair,ticker in capture.PAIRS.items():
        raw=json.dumps({'ticker':ticker,'status':'OK','resultsCount':len(rows),'queryCount':len(rows),'results':rows}).encode()
        ref={'key':model.PRIVATE+model.sha(raw)+'.bin','sha256':model.sha(raw),'bytes':len(raw)};originals[ref['key']]=raw
        url=capture.initial(pair,'2026-06-23','2026-09-21')
        sources[pair]={'contract':capture.CONTRACT,'pair':pair,'provider_ticker':ticker,'from':'2026-06-23','to':'2026-09-21',
            'started_at':'2026-09-21T18:00:00+00:00','completed_at':'2026-09-21T18:00:02+00:00',
            'pagination_complete':True,'stop':'complete_returned_pagination','pages':[{'page':1,'request_url':url,
                'request_sha256':model.sha(url.encode()),'acquired_at':'2026-09-21T18:00:01+00:00',
                'status':'received','http_status':200,'original':ref}]}
    return originals,sources
def change(originals,sources,mutate,pair='EUR_USD'):
    page=sources[pair]['pages'][0];doc=json.loads(originals[page['original']['key']]);mutate(doc)
    raw=json.dumps(doc).encode();ref={'key':model.PRIVATE+model.sha(raw)+'.bin','sha256':model.sha(raw),'bytes':len(raw)}
    originals[ref['key']]=raw;page['original']=ref
def compile_(originals,sources):
    artifacts={}
    def emit(kind,doc):
        raw=model.encoded(doc);ref=model.ref(raw,kind);artifacts[ref['key']]=raw;return ref
    out=model.compile_output(sources,GENERATED,originals.__getitem__,emit)
    return out,artifacts
def bars(out,artifacts,pair='EUR_USD'):
    return [r for block in out['pairs'][pair]['bar_blocks'] for r in json.loads(artifacts[block['key']])['rows']]


class Tests(unittest.TestCase):
    def test_complete_inventory_and_original_row_references(self):
        originals,sources=fixture();out,artifacts=compile_(originals,sources)
        self.assertEqual(out['configured_pairs'],19);self.assertEqual(out['returned_rows'],437)
        for pair in capture.PAIRS:
            records=bars(out,artifacts,pair);self.assertEqual(len(records),23)
            for record in records:
                source=record['source'];doc=json.loads(originals[source['original']['key']]);row=doc['results'][int(source['pointer'].split('/')[-1])]
                self.assertEqual(Decimal(record['values']['c']['decimal']),Decimal(row['c']))
                self.assertEqual(record['values']['v']['decimal'],'0');self.assertFalse(record['bar_finality_verified'])
        self.assertEqual(out['independent_investment_votes'],0)
        self.assertTrue(all(out[k] is False for k in model.FLAGS));self.assertIsNone(out['score'])
    def test_exact_quoted_and_inverse_arithmetic_are_not_negations(self):
        originals,sources=fixture([{'t':1789689600000,'c':100},{'t':1789776000000,'c':150}]);out,_=compile_(originals,sources)
        result=out['pairs']['EUR_USD']['comparisons']['1']
        self.assertEqual(result['quoted_rate_change_decimal'],'50');self.assertEqual(result['inverse_rate_change_exact'],{'numerator':'-100','denominator':'3'})
        self.assertEqual(result['inverse_rate_change_decimal'],'-33.333333333333')
    def test_true_zero_change_and_rounding_are_explicit(self):
        originals,sources=fixture([{'t':1789689600000,'c':100},{'t':1789776000000,'c':100}]);out,_=compile_(originals,sources)
        result=out['pairs']['EUR_USD']['comparisons']['1'];self.assertTrue(result['available']);self.assertEqual(result['quoted_rate_change_decimal'],'0')
        self.assertEqual(model.shown(Fraction(-1,10**20)),'0')
        with localcontext() as context:
            context.rounding=ROUND_DOWN
            self.assertEqual(model.shown(Fraction(2,3)),'0.666666666667')
    def test_missing_latest_close_is_not_replaced_by_previous_row(self):
        originals,sources=fixture();change(originals,sources,lambda d:d['results'][-1].update(c=None))
        out,artifacts=compile_(originals,sources);pair=out['pairs']['EUR_USD']
        self.assertEqual(pair['latest_reported_row']['ordinal'],22);self.assertIsNone(pair['latest_reported_row']['reported_close_decimal'])
        self.assertEqual(len(bars(out,artifacts)),23);self.assertFalse(pair['comparisons']['5']['available'])
    def test_missing_interior_row_never_changes_the_offset(self):
        originals,sources=fixture();change(originals,sources,lambda d:d['results'][17].update(c=None))
        out,_=compile_(originals,sources);pair=out['pairs']['EUR_USD'];result=pair['comparisons']['5']
        self.assertEqual(result['from']['ordinal'],17);self.assertFalse(result['available'])
        result=pair['comparisons']['20'];self.assertTrue(result['available'])
        self.assertEqual(result['from']['ordinal'],2);self.assertEqual(result['returned_rows_in_window'],21);self.assertEqual(result['positive_close_rows_in_window'],20)
    def test_weekend_span_is_not_called_one_trading_day(self):
        originals,sources=fixture([{'t':1789689600000,'c':100},{'t':1789948800000,'c':101}]);out,_=compile_(originals,sources)
        result=out['pairs']['EUR_USD']['comparisons']['1'];self.assertEqual(result['elapsed_calendar_days_decimal'],'3')
        self.assertEqual(result['requested_row_offset'],1)
    def test_out_of_order_response_keeps_source_positions_and_orders_by_timestamp(self):
        originals,sources=fixture();change(originals,sources,lambda d:d['results'].reverse())
        out,artifacts=compile_(originals,sources);pair=out['pairs']['EUR_USD']
        self.assertEqual(pair['chronological_source_ordinals'],list(reversed(range(23))))
        self.assertEqual(pair['comparisons']['1']['to']['ordinal'],0);self.assertEqual(bars(out,artifacts)[0]['source']['pointer'],'/results/0')
    def test_duplicate_or_unknown_timestamps_disable_comparisons_without_dropping_rows(self):
        for stamp in (1787616000000,None,'1789689600000',True):
            originals,sources=fixture()
            def alter(d):d['results'][-1]['t']=d['results'][0]['t'] if stamp==1787616000000 else stamp
            change(originals,sources,alter);out,artifacts=compile_(originals,sources)
            self.assertFalse(out['pairs']['EUR_USD']['chronology_unambiguous']);self.assertEqual(len(bars(out,artifacts)),23)
            self.assertFalse(out['pairs']['EUR_USD']['comparisons']['1']['available'])
    def test_future_window_start_is_unqualified(self):
        originals,sources=fixture();change(originals,sources,lambda d:d['results'][-1].update(t=1893456000000))
        out,artifacts=compile_(originals,sources)
        self.assertIn('future_window_start',bars(out,artifacts)[-1]['issues']);self.assertFalse(out['pairs']['EUR_USD']['comparisons']['1']['available'])
    def test_inconsistent_ohlc_and_nonpositive_endpoint_are_retained_but_not_compared(self):
        for close in (0,-1,999,True,'100'):
            originals,sources=fixture();change(originals,sources,lambda d:d['results'][-1].update(c=close))
            out,artifacts=compile_(originals,sources);self.assertFalse(out['pairs']['EUR_USD']['comparisons']['1']['available'])
            self.assertEqual(len(bars(out,artifacts)),23)
    def test_empty_capture_is_not_zero_activity_or_usable_chronology(self):
        originals,sources=fixture([]);out,_=compile_(originals,sources)
        self.assertEqual(out['returned_rows'],0);self.assertEqual(out['quality']['unambiguous_chronologies'],0)
        self.assertIsNone(out['pairs']['EUR_USD']['latest_reported_row'])
    def test_partial_pagination_cannot_choose_an_apparent_latest_bar(self):
        originals,sources=fixture();change(originals,sources,lambda d:d.update(next_url='https://api.massive.com'+capture.path('EUR_USD','2026-06-23','2026-09-21')+'?cursor=second'))
        sources['EUR_USD'].update(pagination_complete=False,stop='page_limit')
        out,artifacts=compile_(originals,sources);self.assertEqual(len(bars(out,artifacts)),23)
        self.assertFalse(out['pairs']['EUR_USD']['comparisons']['1']['available'])
    def test_false_completeness_and_wrong_request_clock_or_identity_are_rejected(self):
        for mutate in (lambda s:s.update(pagination_complete=False),lambda s:s.update(provider_ticker='C:USDJPY'),
                       lambda s:s.update(completed_at='2026-09-22T00:00:00Z'),lambda s:s['pages'][0].update(request_sha256='a'*64)):
            originals,sources=fixture();mutate(sources['EUR_USD'])
            with self.assertRaises(ValueError):compile_(originals,sources)
    def test_source_byte_tampering_is_rejected(self):
        originals,sources=fixture();key=sources['EUR_USD']['pages'][0]['original']['key'];originals[key]+=b' '
        with self.assertRaises(ValueError):compile_(originals,sources)
    def test_private_path_escape_is_rejected_before_read(self):
        read=Mock()
        with self.assertRaises(ValueError):model.checked_original({'key':'data/trade-tickets.json','sha256':'a'*64,'bytes':1},read)
        read.assert_not_called()
    def test_row_artifact_hash_must_match_its_complete_bytes(self):
        originals,sources=fixture()
        with self.assertRaises(ValueError):model.compile_output(sources,GENERATED,originals.__getitem__,lambda kind,doc:{'key':'wrong'})
    def test_source_precision_and_numeric_bounds(self):
        values=capture.decode(b'{"c":1.123456789012345678901,"v":0,"h":1e1000}')
        self.assertEqual(model.numeric(values,'c')['decimal'],'1.123456789012345678901')
        self.assertEqual(model.numeric(values,'v')['decimal'],'0');self.assertEqual(model.numeric(values,'h')['state'],'outside_numeric_bound')
    def test_missing_identity_is_not_a_reduced_universe(self):
        originals,sources=fixture();sources.pop('USD_TRY')
        with self.assertRaises(ValueError):compile_(originals,sources)
    def test_metal_units_are_not_invented_ounces(self):
        originals,sources=fixture();out,_=compile_(originals,sources)
        self.assertEqual(out['pairs']['EUR_USD']['price_unit'],'USD_per_EUR')
        self.assertEqual(out['pairs']['XAU_USD']['price_unit'],'USD_per_provider_XAU_unit')
        self.assertFalse(out['pairs']['XAU_USD']['metal_base_quantity_unit_verified'])


if __name__=='__main__':unittest.main(verbosity=2)
