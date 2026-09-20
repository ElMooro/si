import copy
from datetime import datetime, timezone, timedelta
import io
import json
from pathlib import Path
import runpy
import sys
import types
import unittest
from unittest.mock import patch

ROOT=Path(__file__).resolve().parents[4]
SOURCE=Path(__file__).resolve().parents[1]/'source'
sys.path[:0]=[str(SOURCE),str(ROOT/'aws/shared'),str(ROOT/'scripts')]
import price_model as model
from price_io import Collector, refresh_public, reconstruct
from replay_price_observations import verify_vault
from test_fred_levels import Store, fixture as fred_fixture
from evidence_store import capture

NOW=datetime(2026,9,20,6,8,30,tzinfo=timezone.utc)
FIXTURES=Path(__file__).with_name('fixtures')/'market'


def fixture(name):return json.loads((FIXTURES/(name+'.json')).read_bytes())


def source(provider,symbol,kind,request,payload,store=None):
    store=store or Store();raw=model.encoded(payload);url=model.source_url(provider,symbol,kind,request)
    return {'response':payload,'request':request,'evidence':capture(store,'test',provider,url,raw,NOW),'acquired_at':NOW.isoformat()}


def yahoo(name='yahoo-gold',symbol='GC=F',payload=None):
    return {'bars':source('yahoo',symbol,'bars',{'range':'5d','interval':'1d'},payload or fixture(name))}


def fmp():
    return {'quote':source('fmp','AAPL','quote',{'symbols':['AAPL','MSFT','NVDA']},fixture('fmp-quotes')),
            'profile':source('fmp','AAPL','profile',{'symbol':'AAPL'},fixture('fmp-profile'))}


def polygon():
    req={'symbol':'EFS','start':'2026-09-09','end':'2026-09-19','multiplier':1,'timespan':'day','adjusted':True,'sort':'desc','limit':50000}
    return {'bars':source('polygon','EFS','bars',req,fixture('polygon-empty')),
            'previous':source('polygon','EFS','previous',{'adjusted':True},fixture('polygon-previous'))}


def compile(provider,symbol,sources):return model.compile_observation(provider,symbol,sources,NOW.isoformat())


def collector(store,payload=None,fail=False):
    requests=[]
    def open_source(req,timeout):
        requests.append(req.full_url)
        if fail:raise TimeoutError('private credential must not escape')
        return io.BytesIO(model.encoded(payload or fixture('yahoo-gold')))
    return Collector(store,'test','fixture-secret','fixture-secret',NOW,open_source,clock=lambda:NOW),requests


class NativePrices(unittest.TestCase):
    def test_actual_gold_future_keeps_provider_units_and_bar_finality(self):
        out=compile('yahoo','GC=F',yahoo())
        self.assertEqual(out['instrument_type'],'FUTURE');self.assertEqual(out['unit'],'provider_quoted_units')
        self.assertEqual(out['observation_date'],'2026-09-18');self.assertEqual(out['previous_observation_date'],'2026-09-17')
        self.assertIsNone(out['observed_at']);self.assertFalse(out['period_completed_verified'])
        self.assertFalse(out['sizing_eligible']);self.assertIn('finality unverified',out['price_kind'])

    def test_actual_dxy_missing_latest_is_not_dropped(self):
        out=compile('yahoo','DX-Y.NYB',yahoo('yahoo-dxy','DX-Y.NYB'))
        self.assertIsNone(out['value']);self.assertIsNone(out['chg_pct']);self.assertEqual(out['status'],'PENDING_RESOLUTION')
        self.assertEqual(out['observation_date'],'2026-09-20');self.assertEqual(out['unit'],'index_points')

    def test_actual_btc_missing_previous_does_not_skip_back_two_days(self):
        out=compile('yahoo','BTC-USD',yahoo('yahoo-btc','BTC-USD'))
        self.assertEqual(out['value'],80409);self.assertIsNone(out['prev']);self.assertIsNone(out['chg_pct'])
        self.assertEqual(out['previous_observation_date'],'2026-09-19')

    def test_actual_shanghai_currency_and_exchange_timezone_remain_distinct(self):
        out=compile('yahoo','000001.SS',yahoo('yahoo-shanghai','000001.SS'))
        self.assertEqual(out['currency'],'CNY');self.assertEqual(out['unit'],'index_points')
        self.assertEqual(out['provider_exchange_timezone'],'Asia/Shanghai')

    def test_actual_move_metadata_conflict_cannot_vote(self):
        out=compile('yahoo','^MOVE',yahoo('yahoo-move','^MOVE'))
        self.assertEqual(out['status'],'MAPPING_REVIEW');self.assertTrue(out['identity_conflict'])
        self.assertIn('Northern Trust',out['definition'])

    def test_macro_ticker_collision_preserves_raw_security_but_blocks_mapping(self):
        out=compile('yahoo','USCA',yahoo('yahoo-usca','USCA'))
        row={'symbol':'USCA','category':'macro','exchanges':['ECONOMICS'],'value':43.573,'source':'yahoo:USCA'}
        model.merge_observation(row,out,{})
        self.assertEqual(row['status'],'MAPPING_REVIEW');self.assertEqual(row['instrument_type'],'ETF')
        self.assertEqual(row['superseded_legacy_measurement']['value'],43.573)
        proxy={'symbol':'GOLD','category':'commodity'};model.merge_observation(proxy,compile('yahoo','GC=F',yahoo()),{'GOLD':'yahoo:GC=F'})
        self.assertTrue(proxy['mapping']['proxy_possible']);self.assertFalse(proxy['mapping']['semantic_match_verified'])

    def test_actual_fmp_clock_currency_and_undated_previous_close(self):
        out=compile('fmp','AAPL',fmp())
        self.assertEqual(out['observed_at'],'2026-09-18T20:00:02+00:00')
        self.assertEqual(out['unit'],'USD_per_share');self.assertIsNone(out['previous_observation_date'])
        self.assertEqual(out['exact']['change'],'-0.86999');self.assertIn('not supplied',out['comparison_basis'])

    def test_fmp_requires_quote_profile_identity_exchange_and_timestamps(self):
        for kind,field,bad in [('profile','symbol','OTHER'),('profile','currency',None),('profile','exchange','NYSE'),
                              ('quote','timestamp',None),('quote','timestamp',int(NOW.timestamp())+1),('quote','price',True)]:
            sources=fmp();sources[kind]['response'][0][field]=bad
            with self.assertRaises((ValueError,KeyError,TypeError)):compile('fmp','AAPL',sources)
        sources=fmp();sources['quote']['response'].append(sources['quote']['response'][0])
        with self.assertRaises(ValueError):compile('fmp','AAPL',sources)

    def test_actual_polygon_previous_quote_is_2010_not_current_and_open_not_prev(self):
        sources=polygon();sources['previous']['response']['results'][0]['o']=9.96
        sources['previous']['response']['results'][0]['h']=9.96
        out=compile('polygon','EFS',sources)
        self.assertEqual(out['observation_date'],'2010-10-04');self.assertEqual(out['status'],'STALE')
        self.assertEqual(out['value'],9.95);self.assertIsNone(out['prev']);self.assertIsNone(out['chg_pct'])

    def test_polygon_two_completed_bars_compare_close_not_open(self):
        sources=polygon();sources.pop('previous');p=sources['bars']['response']
        p['results']=[{'o':80,'h':120,'l':75,'c':110,'t':1789704000000},
                      {'o':90,'h':110,'l':85,'c':100,'t':1789617600000}]
        p['resultsCount']=2
        out=compile('polygon','EFS',sources)
        self.assertEqual(out['prev'],100);self.assertEqual(out['chg_pct'],10)
        self.assertEqual(out['observation_date'],'2026-09-18');self.assertTrue(out['period_completed_verified'])

    def test_polygon_incomplete_adjustment_and_request_identity_rejected(self):
        for field,bad in [('end','2026-09-20'),('adjusted',False),('symbol','WRONG')]:
            sources=polygon();sources['bars']['request'][field]=bad
            with self.assertRaises(ValueError):compile('polygon','EFS',sources)

    def test_missing_and_negative_prices_are_not_synthetic_zero_or_percentage(self):
        self.assertIsNone(model.pair(1,0)['chg_pct']);self.assertIsNone(model.pair(1,-1)['chg_pct'])
        self.assertEqual(model.pair(-1,-2)['change'],1)
        for value in (True,float('nan'),float('inf'),'3'):
            with self.assertRaises(ValueError):model.pair(value,1)

    def test_yahoo_unaligned_duplicate_future_and_wrong_symbol_rejected(self):
        for bad in ('length','duplicate','future','symbol'):
            payload=fixture('yahoo-gold');r=payload['chart']['result'][0]
            if bad=='length':r['timestamp'].pop()
            elif bad=='duplicate':r['timestamp'][-1]=r['timestamp'][-2]
            elif bad=='future':r['timestamp'][-1]=int(NOW.timestamp())+1
            else:r['meta']['symbol']='WRONG'
            with self.assertRaises(ValueError):compile('yahoo','GC=F',yahoo(payload=payload))

    def test_provider_request_and_clock_cannot_be_swapped(self):
        for field,bad in [('source_url','https://wrong.test/'),('first_received_at','2027-01-01T00:00:00Z'),('key','data/evidence/yahoo/wrong.bin.gz')]:
            sources=yahoo();sources['bars']['evidence'][field]=bad
            with self.assertRaises(ValueError):compile('yahoo','GC=F',sources)

    def test_original_capture_full_replay_and_tampered_cache_value(self):
        store=Store();coll,requests=collector(store);out=coll.get('yahoo','GC=F')
        self.assertIsNotNone(out);self.assertEqual(len(requests),1)
        self.assertEqual(reconstruct(out['replay'],store.read),out)
        out['value']=999
        cached,requests=collector(store);cached.seed([out]);fixed=cached.get('yahoo','GC=F',allowed=False)
        self.assertEqual(fixed['value'],4424.89990234375);self.assertFalse(requests)
        self.assertEqual(cached.reused,{('yahoo','GC=F')})

    def test_mutated_original_and_compiler_fail_replay(self):
        store=Store();coll,_=collector(store);out=coll.get('yahoo','GC=F')
        source_key=out['evidence']['bars']['key'];store.objects[source_key]['Body']=b'bad-gzip'
        with self.assertRaises(Exception):reconstruct(out['replay'],store.read)
        store=Store();coll,_=collector(store);out=coll.get('yahoo','GC=F')
        manifest=json.loads(store.read(out['replay']['key']))
        ref=manifest['compilers']['price_model.py'];store.objects[ref['key']]['Body']=b'changed'
        with self.assertRaises(ValueError):reconstruct(out['replay'],store.read)

    def test_failed_and_budgeted_refresh_keep_prior_clocks_and_values_stale(self):
        store=Store();coll,requests=collector(store,fail=True)
        self.assertIsNone(coll.get('yahoo','GC=F'));self.assertEqual(coll.failures,{'yahoo:GC=F':'TimeoutError'})
        row={'value':100,'asof':'2026-01-01','fetched_at':'2026-01-02T00:00:00Z'};model.mark_unavailable(row,('yahoo','GC=F'))
        self.assertEqual(row['status'],'STALE');self.assertEqual(row['fetched_at'],'2026-01-02T00:00:00Z');self.assertEqual(row['value'],100)
        coll,requests=collector(Store());coll.requests=96
        self.assertIsNone(coll.get('yahoo','GC=F'));self.assertFalse(requests)

    def test_public_refresh_changes_only_selected_rows_and_preserves_complete_predecessor(self):
        store=Store();packet={'generated_at':'2026-09-19T11:00:00Z','symbols':[
            {'symbol':'GOLD','source':'yahoo:GC=F','value':1,'status':'LIVE','category':'commodity'},
            {'symbol':'OTHER','value':2,'status':'LIVE','fetched_at':'2026-01-01T00:00:00Z'}]}
        raw=model.encoded(packet);store.put_object(Key='data/tradingview.json',Body=raw)
        coll,_=collector(store);refresh_public(store,'test','data/tradingview.json',coll,{'GOLD':'yahoo:GC=F'},['yahoo:GC=F'])
        out=json.loads(store.read('data/tradingview.json'))
        self.assertEqual(out['generated_at'],packet['generated_at']);self.assertEqual(out['symbols'][1],packet['symbols'][1])
        self.assertEqual(verify_vault(out,store.read)['alias_rows_checked'],1)
        predecessors=[k for k in store.objects if k.startswith('audit-private/')]
        self.assertEqual(len(predecessors),1);self.assertEqual(store.read(predecessors[0]),raw)
        with self.assertRaises(ValueError):refresh_public(store,'test','data/tradingview.json',coll,{},['yahoo:UNKNOWN'])

    def test_public_price_handler_never_loads_private_registry(self):
        store=Store();store.put_object(Key='data/tradingview.json',Body=model.encoded({'symbols':[
            {'symbol':'GOLD','status':'LIVE','source':'yahoo:GC=F','category':'commodity'}]}))
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:store),
            'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'fixture-secret')}):
            scope=runpy.run_path(str(SOURCE/'lambda_function.py'))
            coll,_=collector(store)
            scope['lambda_handler'].__globals__.update(PriceCollector=lambda *a,**k:coll,
                get_brain=lambda:(_ for _ in ()).throw(AssertionError('private input read')))
            result=scope['lambda_handler']({'public_price_refresh':True,'instruments':['yahoo:GC=F']},None)
        self.assertEqual(result['updated_rows'],1);self.assertNotIn('data/brain.json',store.gets)

    def test_normal_handler_retains_full_price_contract_and_original_cache_clock(self):
        store=Store();coll,_=collector(store);old=coll.get('yahoo','GC=F')
        row={**old,'symbol':'GOLD','status':'LIVE','category':'commodity','n_notes':1,'note_ids':[],'exchanges':[]}
        store.put_object(Key='data/tradingview.json',Body=model.encoded({'generated_at':NOW.isoformat(),'symbols':[row]}))
        cached,requests=collector(store)
        with patch.dict(sys.modules,{'boto3':types.SimpleNamespace(client=lambda *a,**k:store),
            'managed_secret':types.SimpleNamespace(managed_secret=lambda *a,**k:'fixture-secret')}):
            scope=runpy.run_path(str(SOURCE/'lambda_function.py'))
            scope['lambda_handler'].__globals__.update(PriceCollector=lambda *a,**k:cached,
                ALIASES={'GOLD':'yahoo:GC=F'},get_brain=lambda:{},gen_aliases=lambda:{},
                build_registry=lambda brain:{'GOLD':{'symbol':'GOLD','exchanges':[],'n_notes':1,'note_ids':[]}},
                _families=lambda:None,_fleet_prices=lambda:None)
            scope['lambda_handler']({},None)
        output=json.loads(store.read('data/tradingview.json'))['symbols'][0]
        self.assertEqual(output['replay'],old['replay']);self.assertEqual(output['fetched_at'],old['fetched_at'])
        self.assertFalse(requests);self.assertTrue(output['cached']);self.assertEqual(output['unit'],'provider_quoted_units')


if __name__=='__main__':unittest.main()
