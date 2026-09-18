"""Date alignment, exposure reconciliation, missing inputs and private replay."""
import base64
from copy import deepcopy
from datetime import datetime, timedelta, timezone
import hashlib
import json
from pathlib import Path
import statistics
import sys
import unittest

ROOT = Path(__file__).resolve().parents[4]
sys.path[:0] = [str(ROOT/'aws/shared'), str(Path(__file__).resolve().parents[1]/'source')]
import portfolio_risk_model as model

NOW = datetime(2026, 9, 18, 17, tzinfo=timezone.utc)


def packet(symbol, rows):
    doc = {'ticker': symbol, 'adjusted': True, 'status': 'OK', 'results': rows}
    raw = model.canonical(doc)
    doc['_source_evidence'] = {'raw_body_base64': base64.b64encode(raw).decode(), 'body_sha256': hashlib.sha256(raw).hexdigest(),
        'received_at': NOW.isoformat(), 'request': 'https://api.polygon.io/v2/aggs/ticker/'+symbol+'/range/1/day/2026-03-01/2026-09-17?adjusted=true'}
    return doc


def fixture():
    days = [NOW.date()-timedelta(days=i) for i in range(180, 0, -1) if (NOW.date()-timedelta(days=i)).weekday() < 5]
    spy, a, b = [], [], []
    price = 100.0
    for i, day in enumerate(days):
        price *= 1 + ((i%7)-3)*.002
        t = int(datetime.combine(day, datetime.min.time(), timezone.utc).replace(hour=4).timestamp()*1000)
        spy.append({'t':t,'c':price}); a.append({'t':t,'c':price*2}); b.append({'t':t,'c':100.0})
    snapshot = {'generated_at': NOW.isoformat(), 'positions': [{'symbol':'AAA', 'qty':10, 'market_value':1000,
        'current_price':100, 'price_asof_unix_ms':int((NOW-timedelta(hours=20)).timestamp()*1000),
        'valuation_status':'PRICED','sector':'Technology'}]}
    return snapshot, {'AAA': packet('AAA',a), 'BBB':packet('BBB',b), 'SPY':packet('SPY',spy)}


def evaluate(snapshot, packets, scenarios=None):
    return model.build(snapshot, packets, NOW.isoformat(), scenarios or {})


class RiskModel(unittest.TestCase):
    def test_missing_nav_keeps_holdings_model_separate(self):
        s,p=fixture(); out=evaluate(s,p)
        self.assertEqual(out['status'],'AVAILABLE_HOLDINGS_MODEL')
        self.assertIsNone(out['var_1d_99_pct']); self.assertIsNone(out['portfolio_beta_spy'])
        self.assertGreater(out['holdings_risk']['var_1d_99_dollars'],0)
        self.assertEqual(out['holdings_risk']['beta_spy_per_gross'],1)
        self.assertFalse(out['permissions']['sizing_eligible'])

    def test_no_defaults_missing_bars_or_unpriced_holding(self):
        s,p=fixture(); p.pop('AAA'); out=evaluate(s,p)
        self.assertIsNone(out['holdings_risk']['var_1d_99_dollars'])
        self.assertIsNone(out['position_metrics']['AAA']['annual_vol_pct'])
        s,p=fixture(); s['positions'][0]['market_value']=None
        self.assertIsNone(evaluate(s,p)['total_market_value'])

    def test_zero_vol_beta_preserved_correlation_undefined(self):
        s,p=fixture(); s['positions'][0]['symbol']='BBB'
        out=evaluate(s,p)
        self.assertEqual(out['position_metrics']['BBB']['annual_vol_pct'],0)
        self.assertEqual(out['position_metrics']['BBB']['beta_spy'],0)
        self.assertEqual(out['holdings_risk']['var_1d_99_dollars'],0)
        self.assertIsNone(out['correlation_matrix']['BBB']['BBB'])

    def test_duplicate_lots_aggregate_and_signed_short(self):
        s,p=fixture(); baseline=evaluate(s,p)['holdings_risk']['var_1d_99_dollars']
        s['positions'] *= 2
        self.assertAlmostEqual(evaluate(s,p)['holdings_risk']['var_1d_99_dollars'],baseline*2,delta=.02)
        s['positions']=[deepcopy(row) for row in s['positions']]; s['positions'][1]['qty']=-10; s['positions'][1]['market_value']=-1000
        out=evaluate(s,p)
        self.assertEqual(out['gross_market_value'],2000); self.assertEqual(out['total_market_value'],0)
        self.assertEqual(out['holdings_risk']['var_1d_99_dollars'],0)
        # Fully offset holdings should be a modeled zero-risk book, not a division by zero.
        self.assertIsNone(out['var_1d_99_pct'])

    def test_intervals_never_pair_different_dates_or_bridge_missing_day(self):
        s,p=fixture(); rows=p['AAA']['results']; lost=rows[30]['t']
        p['AAA']=packet('AAA',rows[:30]+rows[31:])
        full=evaluate(*fixture()); out=evaluate(s,p)
        self.assertEqual(out['risk_contract']['sample_count'],full['risk_contract']['sample_count']-2)
        self.assertEqual(out['holdings_risk']['beta_spy_per_gross'],1)
        left={('2026-01-01','2026-01-02'):1}; right={('2026-01-02','2026-01-03'):1}
        self.assertEqual(model.pair_values(left,right)[2],[])

    def test_short_undated_duplicate_and_tampered_samples_rejected(self):
        s,p=fixture()
        for rows in (p['AAA']['results'][-20:], [{'c':12}], p['AAA']['results']+[p['AAA']['results'][0]]):
            copy=deepcopy(p); copy['AAA']=packet('AAA',rows)
            self.assertIsNone(evaluate(s,copy)['holdings_risk']['var_1d_99_dollars'])
        p['AAA']['results'][0]['c']+=1
        self.assertEqual(evaluate(s,p)['quality']['data_errors']['AAA'],['ORIGINAL_RESPONSE_EVIDENCE_INVALID'])

    def test_zero_missing_drawdown_and_stale_marks(self):
        s,p=fixture(); s['positions'][0]['price_asof_unix_ms']=int((NOW-timedelta(days=6)).timestamp()*1000)
        self.assertIsNone(evaluate(s,p)['holdings_risk']['var_1d_99_dollars'])
        self.assertIsNone(model.drawdown({'a':1},31))
        self.assertEqual(model.drawdown({str(i):1 for i in range(31)},31),0)

    def test_unknown_sector_is_not_spy_substitution(self):
        s,p=fixture(); s['positions'][0]['sector']='Unknown'
        scenario={'shock':{'name':'Hypothesis','duration_days':1,'spy_return':-.1,'sector_returns':{'Technology':-.2}}}
        row=evaluate(s,p,scenario)['historical_scenarios']['shock']
        self.assertIsNone(row['projected_pnl_dollars']); self.assertFalse(row['historical_replay_verified'])

    def test_private_replay_and_tamper_detection(self):
        s,p=fixture(); bundle,out=model.freeze(s,p,NOW.isoformat(),{})
        self.assertEqual(model.replay(bundle),out)
        bundle['inputs']['snapshot']['positions'][0]['qty']=11
        with self.assertRaises(ValueError): model.replay(bundle)

    def test_reconciled_nav_equation_and_same_book_required(self):
        s,p=fixture(); position={**s['positions'][0], 'mark_age_h':20}
        s['capital_book']={'schema_version':'1.0','status':'READY','allows_new_entries':True,
            'book_id':'fixture','account_id':'fixture','currency':'USD','as_of':NOW.isoformat(),'reconciled_at':NOW.isoformat(),
            'equity_nav':2000,'cash':1000,'liabilities':0,'reserved_order_exposure':0,'gross_exposure':1000,'net_exposure':1000,
            'positions':[position],'unpriced_positions':[],'open_orders':[],
            'nav_history':[{'book_id':'fixture','account_id':'fixture','as_of':(NOW-timedelta(days=i)).isoformat(),'equity_nav':2000} for i in (1,0)]}
        self.assertIsNotNone(evaluate(s,p)['var_1d_99_pct'])
        for bad in (-1,0,9999):
            copy=deepcopy(s); copy['capital_book']['equity_nav']=bad
            self.assertIsNone(evaluate(copy,p)['var_1d_99_pct'])
        s['capital_book']['cash']=0
        self.assertIsNone(evaluate(s,p)['var_1d_99_pct'])


if __name__=='__main__': unittest.main()
