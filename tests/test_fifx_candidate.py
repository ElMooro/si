"""Original identities, dates, precision, full history and independent arithmetic."""
from pathlib import Path
from copy import deepcopy
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal, localcontext, ROUND_UP
import hashlib, json, sys, unittest, urllib.parse
ROOT = Path(__file__).resolve().parents[1]
sys.path[:0] = [str(ROOT/'aws/ops/checks'), str(ROOT/'scripts')]
import fifx_candidate as candidate
import fifx_catalog as catalog
import fifx_originals as originals
import verify_fifx_arithmetic as independent

NOW = '2026-09-26T07:00:00+00:00'


def definition(sid):
    # Fixture comes from retained provider evidence, not the catalogue under test.
    row=json.loads((ROOT/'tests/fixtures/fifx-retained-definitions.json').read_bytes())['source_summary'][sid]
    return {'seriess': [{'id': sid, 'units': row['native_unit'], 'frequency_short': row['frequency_short'],
        'frequency': row['frequency'], 'seasonal_adjustment': row['seasonal_adjustment']}]}


def csv(sid, n=550, values=None):
    values = values or [str(Decimal('4')+Decimal((i*i+3*i)%127)/1000) for i in range(n)]
    rows = [(str(date(2026,9,25)-timedelta(days=n-1-i)), v) for i, v in enumerate(values)]
    return ('observation_date,'+sid+'\n'+''.join(d+','+v+'\n' for d,v in rows)).encode()


def quote(sid='^KS11', n=550):
    currency, exchange, zone, names = catalog.QUOTE_IDENTITIES[sid]
    name = names[0] if names else 'Unreviewed'
    times = [int(datetime(2026,9,25,0,tzinfo=timezone.utc).timestamp())-86400*(n-1-i) for i in range(n)]
    return {'chart': {'error': None, 'result': [{'meta': {'symbol': sid, 'currency': currency, 'exchangeName': exchange,
        'exchangeTimezoneName': zone, 'instrumentType': 'INDEX', 'dataGranularity': '1d', 'shortName': name, 'longName': name},
        'timestamp': times, 'indicators': {'quote': [{'close': [100+(i*i%113)/1000 for i in range(n)], 'volume': [0]*n}]}}]}}


def receipt(sid, raw, stamp=NOW):
    url = ('https://fred.stlouisfed.org/graph/fredgraph.csv?'+urllib.parse.urlencode({'id':sid,'cosd':'1988-01-01','coed':stamp[:10]})) if sid in catalog.FRED else (
        'https://query1.finance.yahoo.com/v8/finance/chart/'+urllib.parse.quote(sid,safe='')+'?'+
        ('range=2y&interval=1d' if sid in ('^MOVE','^VHSI') else 'period1=315532800&period2='+str(int(originals.clock(stamp).timestamp()))+'&interval=1d'))
    return {'source_url':url,'http_status':200,'acquired_at':stamp,'sha256':hashlib.sha256(raw).hexdigest(),'bytes':len(raw)}


def build(sid='DGS10', raw=None, stamp=NOW, meta=None):
    if raw is None:raw=csv(sid) if sid in catalog.FRED else json.dumps(quote(sid)).encode()
    proof = receipt(sid,raw)
    meta = meta if meta is not None else definition(sid) if sid in catalog.FRED else None
    result = candidate.build_source(sid,raw,proof,stamp,meta)
    return result, independent.verify(result,raw,proof,meta)


class Tests(unittest.TestCase):
    def test_exact_retained_provider_definition_and_no_friendly_unit_alias(self):
        self.assertEqual(definition('DEXUSUK')['seriess'][0]['units'],'U.S. Dollars to One U.K. Pound Sterling')
        good,_=build('DEXUSUK');self.assertEqual(good['quality']['status'],'within_age_ceiling')
        bad=definition('DEXUSUK');bad['seriess'][0]['units']='U.S. Dollars to One British Pound'
        out,_=build('DEXUSUK',meta=bad);self.assertIsNone(out['current'])

    def test_complete_original_scope_and_every_historical_estimate(self):
        counts = []
        for sid in catalog.SOURCES:
            out, proof = build(sid)
            self.assertEqual(len(out['original_rows']),550)
            self.assertEqual(proof['original_rows'],550)
            self.assertEqual(proof['history_rows'],max(0,550-catalog.SPECS[sid]['window_changes']) if sid!='^VHSI' else 0)
            self.assertTrue(all(out[k] is False for k in catalog.AUTHORITY))
            counts.append(proof['independent_scalar_checks'])
        self.assertGreater(sum(counts),50000)

    def test_yield_zero_negative_and_true_zero_variance_survive(self):
        out, _ = build(raw=csv('DGS10', values=['-1']*550))
        self.assertEqual(out['current']['estimate']['value'],0)
        self.assertEqual(out['current']['baseline']['status'],'flat_baseline')
        self.assertIsNone(out['current']['baseline']['z_score']['value'])
        self.assertEqual(out['current']['baseline']['midrank_percentile']['value'],50)
        out, _ = build(raw=csv('DGS10', values=['0']*550))
        self.assertEqual(len(out['history']),520)

    def test_zero_log_input_invalidates_instead_of_disappearing(self):
        values=['1.2']*550;values[-5]='0'
        out, _ = build('DEXUSEU',csv('DEXUSEU',values=values))
        self.assertEqual(len(out['original_rows']),550);self.assertEqual(len(out['history']),530)
        self.assertEqual(out['last_calculated']['invalid_log_changes'],2)
        self.assertIsNone(out['current']);self.assertIsNone(out['last_calculated']['estimate']['value'])

    def test_missing_latest_never_backfills(self):
        values=['4']*550;values[-1]='.'
        out, _ = build(raw=csv('DGS10',values=values))
        self.assertIsNone(out['current']);self.assertIsNotNone(out['last_calculated'])
        self.assertEqual(out['quality']['status'],'missing_latest_value')

    def test_gap_and_missing_rows_are_disclosed(self):
        values=['4']*550
        values[-12:-2]=['.']*10
        out, _=build(raw=csv('DGS10',values=values))
        self.assertEqual(out['last_calculated']['max_interval_days'],11)
        self.assertEqual(out['last_calculated']['missing_rows_inside_window'],10)
        self.assertIsNone(out['current'])

    def test_prior_excludes_current_spike_and_does_not_clip(self):
        normal,_=build();raw=csv('DGS10');lines=raw.decode().splitlines();lines[-1]=lines[-1].split(',')[0]+',10000'
        spike,_=build(raw=('\n'.join(lines)+'\n').encode())
        self.assertEqual(normal['current']['baseline']['mean'],spike['current']['baseline']['mean'])
        self.assertGreater(spike['current']['baseline']['z_score']['value'],100)

    def test_h10_observation_and_source_ages_are_separate(self):
        raw=csv('DEXJPUS').decode();raw=raw.replace('2026-09-25','2026-09-25')
        rows=raw.splitlines();rows=rows[:1]+rows[1:-7]
        out,_=build('DEXJPUS',('\n'.join(rows)+'\n').encode())
        self.assertEqual(out['quality']['observation_age_days'],8)
        self.assertEqual(out['quality']['status'],'within_age_ceiling')
        self.assertEqual(out['quality']['max_observation_age_days'],12)
        old,_=build('DEXJPUS',('\n'.join(rows)+'\n').encode(),stamp='2026-09-28T07:00:00+00:00')
        self.assertIsNone(old['current']);self.assertEqual(old['quality']['status'],'stale_acquisition')

    def test_future_original_never_becomes_eligible_on_republication(self):
        raw=csv('DGS10')+b'2026-09-28,99\n'
        out,_=build(raw=raw,stamp='2026-09-29T07:00:00+00:00')
        self.assertEqual(len(out['original_rows']),551)
        self.assertEqual(out['last_calculated']['date'],'2026-09-25')
        self.assertEqual(out['quality']['excluded_future_rows'],1);self.assertIsNone(out['current'])

    def test_identity_mismatch_preserves_original_and_forbids_calculation(self):
        doc=quote('^MOVE');doc['chart']['result'][0]['meta']['longName']='Northern Trust iBoxx 5-Year Tar'
        out,_=build('^MOVE',json.dumps(doc).encode())
        self.assertEqual(out['quality']['status'],'identity_mismatch');self.assertEqual(out['history'],[])
        self.assertEqual(len(out['original_rows']),550)
        bad=definition('DGS10');bad['seriess'][0]['units']='Index'
        out,_=build(meta=bad);self.assertIsNone(out['current']);self.assertEqual(out['history'],[])

    def test_quote_session_date_uses_pinned_timezone(self):
        doc=quote('^AXJO',n=1);doc['chart']['result'][0]['timestamp']=[int(datetime(2026,9,25,23,tzinfo=timezone.utc).timestamp())]
        out,_=build('^AXJO',json.dumps(doc).encode())
        self.assertEqual(out['original_rows'][0]['date'],'2026-09-26')
        self.assertEqual(out['quality']['status'],'provisional_session')
        self.assertEqual(out['source_identity']['timezone']['iana_version'],'2026c')

    def test_quote_decimal_lexemes_are_not_binary_floats(self):
        raw=json.dumps(quote(n=40)).replace('100.0,','100.000000000000000000001,',1).encode()
        out,_=build('^KS11',raw)
        self.assertEqual(out['original_rows'][0]['value'],'100.000000000000000000001')

    def test_context_precision_cannot_change_output(self):
        sid='DEXUSUK';raw=csv(sid,n=60);rec=receipt(sid,raw);meta=definition(sid)
        expected=candidate.build_source(sid,raw,rec,NOW,meta)
        with localcontext() as precision:
            precision.prec=6;precision.rounding=ROUND_UP
            self.assertEqual(expected,candidate.build_source(sid,raw,rec,NOW,meta))

    def test_malformed_source_never_authorizes_current(self):
        cases=[b'null',b'{"chart":{}}',b'{"chart":{},"chart":{}}']
        for mutate in (lambda r:r['indicators']['quote'][0]['close'].pop(),
                       lambda r:r['indicators']['quote'][0]['close'].__setitem__(1,'100'),
                       lambda r:r['timestamp'].__setitem__(1,r['timestamp'][0]),
                       lambda r:r['meta'].__setitem__('dataGranularity','1mo')):
            doc=quote(n=40);mutate(doc['chart']['result'][0]);cases.append(json.dumps(doc).encode())
        for raw in cases:
            out,_=build('^KS11',raw);self.assertIsNone(out['current']);self.assertEqual(out['history'],[])

    def test_bound_original_and_request_identity(self):
        raw=csv('DGS10',n=50);rec=receipt('DGS10',raw)
        for invalid in ({**rec,'bytes':1},{**rec,'source_url':rec['source_url'].replace('DGS10','DGS2')},
                        {**rec,'acquired_at':'2026-09-27T07:00:00+00:00'}):
            with self.assertRaises(ValueError):candidate.build_source('DGS10',raw,invalid,NOW,definition('DGS10'))

    def test_error_body_and_missing_source_remain_explicit(self):
        raw=b'{"error":"not found"}';rec={**receipt('^VHSI',raw),'http_status':404}
        out=candidate.build_source('^VHSI',raw,rec,NOW);independent.verify(out,raw,rec)
        self.assertEqual(out['quality']['status'],'http_error');self.assertEqual(out['original_bytes'],len(raw))
        out=candidate.build_source('^VHSI',None,None,NOW);independent.verify(out,None,None)
        self.assertEqual(out['quality']['status'],'unavailable')

    def test_independent_verifier_catches_tampering(self):
        raw=csv('DGS10');rec=receipt('DGS10',raw);meta=definition('DGS10')
        original=candidate.build_source('DGS10',raw,rec,NOW,meta)
        for mutate in (lambda o:o['history'][0]['estimate'].__setitem__('value',123),
                       lambda o:o['history'][-1]['baseline']['z_score'].__setitem__('calculated_decimal','123'),
                       lambda o:o['original_rows'].pop(),lambda o:o.__setitem__('calls_eligible',True),
                       lambda o:o['quality'].__setitem__('observation_age_days',0)):
            out=deepcopy(original);mutate(out)
            with self.assertRaises(AssertionError):independent.verify(out,raw,rec,meta)

if __name__=='__main__':unittest.main(verbosity=2)
