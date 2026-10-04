import unittest,base64,gzip,json
from unittest.mock import Mock
import bis_fx_cross as cross
import bis_fx_series as series
from test_bis_fx_series import fixture

def packet(identifier,dates):
    d=series.definition(identifier)
    rows=[[d['freq'],d['geo'],d['currency'],d['collection'],'0',date,str(value) if value is not None else 'NaN','A' if value is not None else 'M','F',''] for date,value in dates]
    return series.fetch(identifier,lambda _:(fixture(rows),{}))

class Tests(unittest.TestCase):
    def test_explicit_base_quote_direction(self):
        d=cross.definition('bisfx:XDR:JPY:D')
        self.assertEqual(d['numerator'],'bis:WS_XRU:D.JP.JPY.A');self.assertEqual(d['denominator'],'bis:WS_XRU:D.XW.XDR.A')
        self.assertEqual(d['unit'],'JPY per XDR')
    def test_ambiguous_currency_country_sets_not_arbitrarily_selected(self):
        for currency in ['XAF','XOF','XCD']:
            with self.assertRaises(ValueError):cross.definition('bisfx:'+currency+':JPY:M')
        self.assertEqual(cross.definition('bisfx:EUR:JPY:D')['denominator'],'bis:WS_XRU:D.XM.EUR.A')
    def test_ratio_is_replayable_and_never_fills_a_missing_leg(self):
        d=cross.definition('bisfx:XDR:JPY:D')
        data={d['numerator']:packet(d['numerator'],[('2026-01-02',150),('2026-01-03',151),('2026-01-04',None)]),
              d['denominator']:packet(d['denominator'],[('2026-01-02',.75),('2026-01-04',.8),('2026-01-05',.9)])}
        got=cross.fetch(d['id'],data.__getitem__)
        self.assertEqual(got['obs'],[['2026-01-02',200],['2026-01-03',None],['2026-01-04',None],['2026-01-05',None]])
        self.assertEqual(got['measurement_evidence']['rows'][0],['2026-01-02',150,.75,200,None])
        self.assertEqual(len(got['source_receipts']),2);self.assertEqual(got['n'],1)
        decoded=json.loads(gzip.decompress(base64.b64decode(got['source_period_evidence']['body'])))
        self.assertEqual(decoded,[data[d['numerator']]['measurement_evidence'],data[d['denominator']]['measurement_evidence']])
        self.assertTrue(cross.cache_valid(got,d['id']));self.assertFalse(cross.cache_valid(got,'bisfx:JPY:XDR:D'))
        self.assertFalse(got['history']['same_time_fix_verified']);self.assertFalse(got['history']['missing_periods_filled'])
        self.assertFalse(got['equivalence_to_watchlist_provider_verified']);self.assertFalse(got['calls_eligible'])
    def test_failed_first_leg_stops_second_and_alternatives(self):
        fetch=Mock(return_value={'id':'wrong','history':{'response_complete':False}})
        with self.assertRaises(ValueError):cross.fetch('bisfx:XDR:JPY:D',fetch)
        self.assertEqual(fetch.call_count,1)
    def test_empty_overlap_is_unavailable_not_observations(self):
        d=cross.definition('bisfx:XDR:JPY:D');data={d['numerator']:packet(d['numerator'],[('2026-01-02',150)]),d['denominator']:packet(d['denominator'],[('2026-01-03',.75)])}
        got=cross.fetch(d['id'],data.__getitem__);self.assertEqual(got['n'],0);self.assertEqual(got['quality']['status'],'unavailable')
    def test_wrong_second_leg_never_joins(self):
        d=cross.definition('bisfx:XDR:JPY:D');left=packet(d['numerator'],[('2026-01-02',150)])
        fetch=Mock(side_effect=[left,left])
        with self.assertRaises(ValueError):cross.fetch(d['id'],fetch)
    def test_frequency_and_collection_are_explicit(self):
        d=cross.definition('bisfx:JPY:VND:M');self.assertEqual(d['numerator'],'bis:WS_XRU:M.VN.VND.E')
        self.assertEqual(d['denominator'],'bis:WS_XRU:M.JP.JPY.E')
        with self.assertRaises(ValueError):cross.definition('bisfx:JPY:VND:D')
        for identifier in ['bisfx:JPY:JPY:D','bisfx:JPY:VND:W','bisfx:JPY:BAD:M','FX_IDC:JPYVND']:
            with self.assertRaises(ValueError):cross.definition(identifier)
if __name__=='__main__':unittest.main()
