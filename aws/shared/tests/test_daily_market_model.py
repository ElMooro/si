from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
import unittest

from daily_market_model import equity, crypto, build, CONTRACT, ET

NOW = '2026-09-18T22:00:00+00:00'


def source(provider, request, response):
    raw = json.dumps(response).encode()
    sha = hashlib.sha256(raw).hexdigest()
    return {'request': request, 'response': response, 'acquired_at': NOW,
            'evidence': {'contract':'source-evidence.v1', 'provider':provider, 'captured':True,
                'sha256':sha, 'key':'data/evidence/'+provider+'/fixture/'+sha+'.bin.gz',
                'bytes':len(raw), 'first_received_at':NOW}}


def stock():
    start=datetime(2025,8,14,tzinfo=ET);rows=[]
    for n in range(400):
        d=start+timedelta(days=n)
        if d.weekday()>4:continue
        p=100+n/10
        rows.append({'t':int(d.timestamp()*1000),'o':p,'h':p+1,'l':p-1,'c':p,'v':0})
    request={'symbol':'AAA','start':'2025-08-13','end':'2026-09-17','multiplier':1,
             'timespan':'day','adjusted':True,'sort':'desc','limit':50000}
    return source('polygon',request,{'ticker':'AAA','status':'OK','adjusted':True,
                                   'results':list(reversed(rows)),'resultsCount':len(rows)})


def coins():
    request={'vs_currency':'usd','order':'market_cap_desc','per_page':25,'page':1,
             'sparkline':False,'price_change_percentage':'1h,24h,7d,30d'}
    return source('coingecko',request,[{'id':'coin-one','symbol':'one','current_price':3,
                    'last_updated':'2026-09-18T21:59:00Z','total_volume':0,
                    'market_cap':None,'price_change_percentage_24h':0}])


class MarketModelTests(unittest.TestCase):
    def test_original_rows_calendar_baselines_and_units(self):
        inp=stock();before=deepcopy(inp);out=equity(inp,NOW)
        self.assertEqual(inp,before);self.assertEqual(out['date'],'2026-09-17')
        self.assertEqual(out['observed_at'],'2026-09-18T00:00:00-04:00')
        self.assertEqual(out['volume'],0);self.assertEqual(out['volume_unit'],'shares')
        self.assertEqual(out['source_row'],0);self.assertEqual(out['quality']['status'],'fresh')
        self.assertEqual(out['changes']['month']['target_date'],'2026-08-17')
        self.assertEqual(out['changes']['month']['baseline_date'],'2026-08-17')
        self.assertIsNone(out['w52_high']);self.assertFalse(out['observed_window']['all_time'])
        self.assertFalse(out['calls_eligible']);self.assertFalse(out['history_scope']['historical_point_in_time'])
        self.assertAlmostEqual(out['sma20'],sum(r['c'] for r in out['history'][:20])/20)

    def test_reject_forming_dates_identity_units_pagination_and_invalid_bars(self):
        mutations=[lambda s:s['response'].update(ticker='BBB'),lambda s:s['response'].update(adjusted=False),
          lambda s:s['response'].update(next_url='https://next.test'),lambda s:s['response'].update(resultsCount=1),
          lambda s:s['request'].update(end='2026-09-18'),lambda s:s['response']['results'][0].update(c=float('nan')),
          lambda s:s['response']['results'][0].update(l=1000),lambda s:s['response']['results'][0].update(v=-1),
          lambda s:s['response']['results'][0].update(t=s['response']['results'][1]['t']),
          lambda s:s['response']['results'][0].update(t=s['response']['results'][0]['t']+3600000),
          lambda s:s.update(acquired_at='2026-09-19T00:00:00Z')]
        for mutate in mutations:
            s=stock();mutate(s)
            with self.assertRaises(ValueError):equity(s,NOW)

    def test_no_false_baseline_no_missing_volume_zero_and_expiration(self):
        s=stock();s['response']['results']=s['response']['results'][:3];s['response']['resultsCount']=3
        s['response']['results'][0].pop('v');out=equity(s,NOW)
        self.assertIsNone(out['volume']);self.assertIsNone(out['month_pct']);self.assertIsNone(out['sma20'])
        out=equity(stock(),'2026-09-20T22:00:00Z')
        self.assertEqual(out['quality']['status'],'stale');self.assertIsNone(out['price'])
        self.assertIsNotNone(out['last_observed_price']);self.assertIsNone(out['day_pct'])
        self.assertEqual(out['acquired_at'],NOW)

    def test_crypto_stable_identity_collisions_nulls_and_provider_changes(self):
        s=coins();s['response'].append({**s['response'][0],'id':'coin-two'})
        out=crypto(s,NOW)
        self.assertEqual(set(out['by_id']),{'coin-one','coin-two'});self.assertEqual(out['symbol_aliases'],{})
        self.assertEqual(out['symbol_collisions'],{'ONE':['coin-one','coin-two']})
        row=out['by_id']['coin-one'];self.assertEqual(row['volume_24h'],0);self.assertIsNone(row['market_cap'])
        self.assertEqual(row['change_24h'],0);self.assertIsNone(row['change_1h']);self.assertEqual(row['sparkline'],[])
        self.assertFalse(row['provider_reported_changes']['24h']['baseline_verified'])
        stale=crypto(s,'2026-09-19T01:00:00Z')['by_id']['coin-one']
        self.assertIsNone(stale['price']);self.assertEqual(stale['quality']['status'],'stale')
        s['response'][0]['last_updated']='2026-09-19T00:00:00Z'
        with self.assertRaises(ValueError):crypto(s,NOW)

    def test_failed_provider_is_explicit_gap_no_old_quote_substitution(self):
        s=stock();s['response']['adjusted']=False
        out=build({'contract':CONTRACT,'equities':{'AAA':s},'universe':['AAA','BBB'],
                   'errors':{'BBB':'HTTP_403'},'crypto':coins()},NOW)
        self.assertEqual(out['stocks'],{});self.assertEqual(out['quality']['equity_universe'],['AAA','BBB'])
        self.assertEqual(out['quality']['errors'],{'BBB':'HTTP_403','AAA':'INVALID_SOURCE_RESPONSE'})
        self.assertEqual(out['quality']['status'],'degraded');self.assertIn('ONE',out['crypto'])


if __name__=='__main__':unittest.main()
