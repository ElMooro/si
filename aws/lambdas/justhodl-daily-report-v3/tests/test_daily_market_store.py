from copy import deepcopy
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.parse import urlencode

ROOT=Path(__file__).resolve().parents[4]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests'),str(Path(__file__).resolve().parents[1]/'source')]
import daily_market_store as market
import daily_macro_store as store
from daily_market_model import CONTRACT
from evidence_store import capture
from test_daily_market_model import stock, coins, NOW
from test_daily_macro_store import client
from test_daily_macro_model import auxiliary


def retained(s3):
    eq=stock();cr=coins()
    equrl='https://api.polygon.io/v2/aggs/ticker/AAA/range/1/day/2025-08-13/2026-09-17?adjusted=true&sort=desc&limit=50000'
    crurl='https://api.coingecko.com/api/v3/coins/markets?'+urlencode({**cr['request'],'sparkline':'false'})
    for provider,entry,url in [('polygon',eq,equrl),('coingecko',cr,crurl)]:
        raw=json.dumps(entry['response']).encode()
        entry['evidence']=capture(s3,'test',provider,url,raw,datetime.fromisoformat(NOW))
    return {'contract':CONTRACT,'universe':['AAA'],'equities':{'AAA':eq},'crypto':cr,'errors':{}}


class MarketStoreTests(unittest.TestCase):
    def test_real_original_readback_and_tamper_prevent_publication(self):
        s3=client();sources=retained(s3)
        reader=lambda key:gzip.decompress(s3.objects[key])
        self.assertEqual(market.verify_sources(sources,reader),2)
        tampered=deepcopy(sources);tampered['equities']['AAA']['response']['results'][0]['c']=999
        with self.assertRaisesRegex(ValueError,'differs from original'):market.verify_sources(tampered,reader)
        tampered=deepcopy(sources);tampered['equities']['AAA']['request']['symbol']='BBB'
        with self.assertRaisesRegex(ValueError,'request identity'):market.verify_sources(tampered,reader)
        s3.objects[sources['equities']['AAA']['evidence']['key']]=gzip.compress(b'{}')
        aux=auxiliary();aux['market_sources']=sources
        with self.assertRaisesRegex(ValueError,'original bytes'):store.run(s3,'test',lambda:aux)
        self.assertNotIn(store.CURRENT,s3.puts)

    def test_base_includes_exact_market_compiler_and_reproducible_prices(self):
        s3=client();aux=auxiliary();aux['market_sources']=retained(s3)
        # Source fixture clock is 22:00; use same stamp for deterministic output.
        aux['collected_at']=NOW
        with patch.object(store,'datetime') as dt:
            dt.now.return_value=datetime.fromisoformat(NOW)
            result=store.run(s3,'test',lambda:aux)
        out=json.loads(s3.objects[store.CURRENT])
        self.assertEqual(out['stocks']['AAA']['quality']['status'],'fresh')
        self.assertIsNone(out['stocks']['AAA']['score']);self.assertFalse(out['sizing_eligible'])
        self.assertEqual(out['crypto_by_id']['coin-one']['provider_id'],'coin-one')
        manifest=json.loads(s3.objects[result['replay']['manifest_key']])
        ref=manifest['compilers']['daily_market_model'];self.assertEqual(hashlib.sha256(s3.objects[ref['key']]).hexdigest(),ref['sha256'])
        from daily_macro_model import build,digest
        inputs=json.loads(s3.objects[manifest['input']['key']])
        self.assertEqual(digest(build(inputs['macro'],inputs['auxiliary'],NOW)),manifest['output_sha256'])

    def test_collector_preserves_request_originals_and_safe_failure_codes(self):
        s3=client();eq=stock();coin=coins();calls=[]
        def fake_fetch(url,headers,deadline):
            calls.append((url,headers))
            if '/BBB/' in url:raise TimeoutError('credential=must-not-escape')
            return json.dumps(coin['response'] if 'coingecko' in url else eq['response']).encode(),datetime.fromisoformat(NOW)
        with patch.object(market,'fetch',fake_fetch),patch.object(market,'datetime') as dt:
            dt.now.return_value=datetime.fromisoformat(NOW)
            got=market.collect(s3,'test',['AAA','BBB'],{},'fixture-provider-key')
        self.assertEqual(got['errors'],{'BBB':'TimeoutError'})
        self.assertEqual(got['equities']['AAA']['request']['end'],'2026-09-17')
        self.assertTrue(all('apiKey=' not in url for url,_ in calls))
        self.assertNotIn('fixture-provider-key',json.dumps(got))
        self.assertEqual(got['crypto']['response'][0]['last_updated'],'2026-09-18T21:59:00Z')
        self.assertEqual(market.verify_sources(got,lambda k:gzip.decompress(s3.objects[k])),2)


if __name__=='__main__':unittest.main()
