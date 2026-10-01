"""Actual Katlin vote, basket and publication paths; all IO remains in memory."""
import copy
import hashlib
import io
import json
import unittest
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import patch
import run_tests as fixture

FEEDS = {'regime':'data/regime-composite.json', 'credit':'data/credit-stress.json', 'vol':'data/vol-regime.json'}
SOURCES = {'regime-composite', 'credit-stress', 'vol-regime'}


def base():
    funding = fixture._funding()
    funding['heartbeat'] = {'score':66, 'regime':'ELEVATED'}
    return {'risk_gate':fixture._gate(), 'khalid_risk':fixture._auth(cap=100,mode='SELECTIVE_RISK_ON'),
            'bond_warroom':funding, 'xasset':{'generated_at':fixture._iso(1),'risk_score':75}}


def withheld():
    return {
        'regime':{'generated_at':fixture._iso(1),'meta_regime':'UNAVAILABLE',
                  'calls_eligible':False,'sizing_eligible':False,'validation_status':'UNVALIDATED_DESCRIPTIVE_HEURISTIC'},
        'credit':{'generated_at':fixture._iso(1),'contract':'credit-native-research.v1',
                  'composite_regime':'RESEARCH_ONLY','calls_eligible':False,'sizing_eligible':False,
                  'measurements':{'example':{'value_pct':0,'observation_date':'2026-09-30'}}},
        'vol':{'as_of':fixture._iso(1),'composite_regime':'CONCERNED','composite_score':46}}


class OptionalVotes(unittest.TestCase):
    def setUp(self):
        self.mod = fixture._load()

    def assert_abstains(self, out):
        self.assertFalse(SOURCES.intersection(row['source'] for row in out['legs']))
        for key in FEEDS:
            c = out['research_context'][key]
            self.assertFalse(c['decision_eligible'])
            self.assertEqual(c['qualified_investment_votes'],0)
            self.assertEqual(c['meaning'],'abstain')
            h = next(row for row in out['source_health'] if row['source']==key)
            self.assertFalse(h['decision_eligible'])

    def test_withheld_votes_cannot_dilute_risk_or_expand_basket(self):
        original=base(); expected=self.mod.war_room(original)
        self.assertEqual(expected['local']['exposure_cap_pct'],65)
        for additions in ({k:v} for k,v in withheld().items()):
            actual=self.mod.war_room({**original,**additions})
            self.assert_abstains(actual)
            for key in ('thermometer','local','exposure_cap_pct','entries_allowed','legs'):
                self.assertEqual(actual[key],expected[key],key)
        actual=self.mod.war_room({**original,**withheld()})
        rows=[{'ticker':str(i),'tier':'READY','asset_class':'stock','learned_excess_126s_pct':9,'composite':80} for i in range(20)]
        self.assertEqual(self.mod.build_basket(rows,actual),self.mod.build_basket(rows,expected))
        self.assertGreater(self.mod.build_basket(rows,actual)['cash_pct'],0)

    def test_missing_malformed_unknown_and_forged_eligibility_never_vote(self):
        bad=[None,[],True,'bad',{}, {'generated_at':fixture._iso(1)}]
        for state in (None,'','UNKNOWN','UNAVAILABLE','RESEARCH_ONLY','NORMAL','CRISIS',True,[],42):
            for score in (None,0,True,'0',[],{},float('nan'),float('inf'),-1,100):
                bad.append({'generated_at':fixture._iso(1),'as_of':fixture._iso(1),'meta_regime':state,
                            'composite_regime':state,'composite_score':score,'calls_eligible':True,'sizing_eligible':True})
        for key in FEEDS:
            for packet in bad:
                with self.subTest(feed=key,packet=packet):
                    out=self.mod.war_room({**base(),key:packet})
                    self.assert_abstains(out)
                    self.assertEqual(out['exposure_cap_pct'],65)
                    json.dumps(out,allow_nan=False)

    def test_every_volatility_state_is_research_pending_mapping(self):
        for state in ('COMPLACENT','NORMAL','CONCERNED','PANIC','UNKNOWN','HIGH','STRESS','EXTREME'):
            p={'as_of':fixture._iso(1),'composite_regime':state,'composite_score':0}
            out=self.mod.war_room({**base(),'vol':p})
            self.assert_abstains(out)
            self.assertEqual(out['research_context']['vol']['received_packet'],p)
            self.assertIn('mapping unresolved',out['research_context']['vol']['reason'])

    def test_context_preserves_packet_units_zero_and_original_clocks(self):
        feeds={**base(),**withheld()};before=copy.deepcopy(feeds)
        out=self.mod.war_room(feeds)
        self.assertEqual(feeds,before)
        for key in FEEDS:
            c=out['research_context'][key]
            self.assertEqual(c['received_packet'],feeds[key])
            raw=json.dumps(feeds[key],sort_keys=True,separators=(',',':'),ensure_ascii=False,allow_nan=False).encode()
            self.assertEqual(c['parsed_input_sha256'],hashlib.sha256(raw).hexdigest())
            self.assertTrue(c['publication_within_36h'])
            self.assertFalse(c['observation_freshness_verified'])
        self.assertEqual(out['research_context']['credit']['received_packet']['measurements']['example']['value_pct'],0)
        for stamp in (None,'invalid',fixture._iso(37),fixture._iso(-1/3600),'2026-10-01T01:00:00',0,[]):
            for key in FEEDS:
                p={**withheld()[key],'generated_at':stamp,'as_of':stamp}
                out=self.mod.war_room({**base(),key:p})
                self.assertFalse(out['research_context'][key]['publication_within_36h'])
                self.assertEqual(next(h for h in out['source_health'] if h['source']==key)['status'],'UNUSABLE')

    def test_funding_and_raw_gate_holds_remain_binding(self):
        for override in ({'bond_warroom':{}},{'risk_gate':{}},{'khalid_risk':{}}):
            out=self.mod.war_room({**base(),**withheld(),**override})
            self.assertEqual(out['posture'],'DATA_HOLD')
            self.assertFalse(out['entries_allowed']);self.assertEqual(out['exposure_cap_pct'],0)
            self.assertEqual(self.mod.build_basket([],out)['cash_pct'],100)

    def test_daily_and_refresh_publish_same_abstention_without_renewing_research(self):
        mod=self.mod;feeds={**base(),**withheld(),'asof':{}}
        today=datetime.now(timezone.utc).date().isoformat();writes=[]
        replacements={
            'load_feeds':lambda:feeds,'build_universe':lambda f:({},{},set()),
            'session_keys':lambda n:list(range(mod.P['min_sessions'])),
            'load_bars':lambda *a:([today],{'SPY':SimpleNamespace(d=[0],c=[100])}),
            'market_context':lambda spy:{},'load_crypto':lambda *a:([],{}),
            'dilution_lane':lambda *a:None,'run_sniper':lambda *a:None,
            's3_json':lambda *a:None,'snapshot_and_base_rates':lambda *a:{},
            's3_put_json':lambda key,out:writes.append(copy.deepcopy(out)) or len(json.dumps(out))}
        with patch.multiple(mod,**replacements):
            result=mod.lambda_handler({})
        self.assertTrue(result['ok']);daily=writes[0];self.assert_abstains(daily['war_room'])
        research=copy.deepcopy(daily);research['research_generated_at']=fixture._iso(2)
        lookup={FEEDS[k]:feeds[k] for k in FEEDS}
        lookup.update({'data/risk-gate.json':feeds['risk_gate'],'data/khalid-risk.json':feeds['khalid_risk'],
                       'data/bond-warroom.json':feeds['bond_warroom']})
        class Memory:
            def get_object(self,**kw):
                data=research if kw['Key']==mod.OUT_KEY else lookup.get(kw['Key'],{})
                return {'Body':io.BytesIO(json.dumps(data).encode()),'ETag':'version'}
            def put_object(self,**kw):
                assert kw['IfMatch']=='version'
                writes.append(json.loads(kw['Body']))
        with patch.object(mod,'s3',Memory()):
            result=mod.lambda_handler({'mode':'permission_refresh'})
        self.assertTrue(result['ok']);refresh=writes[-1];self.assert_abstains(refresh['war_room'])
        self.assertEqual(refresh['research_generated_at'],research['research_generated_at'])
        self.assertEqual(refresh['picks'],daily['picks'])
        self.assertEqual(refresh['war_room']['research_context'],daily['war_room']['research_context'])


if __name__=='__main__':unittest.main()
