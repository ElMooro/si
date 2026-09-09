"""Actual adapter bookkeeping regressions; supplied events and independent snapshots."""
import copy
import sys
import unittest
from pathlib import Path
from datetime import datetime,timezone
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from research_capital_ledger import replay_research_ledger,digest,SCHEMA
NOW=datetime(2026,9,9,tzinfo=timezone.utc)


def payload(data,observed='2026-08-31T12:00:00Z',available=None,identity='source'):
 return {'payload':data,'sha256':digest(data),'observed_at':observed,'available_at':available or observed,'source_record_id':identity}


def fixture():
 dates=['2026-09-01','2026-09-02','2026-09-03']
 sessions=[{'date':d,'open_at':d+'T14:00:00Z','close_at':d+'T20:00:00Z','valuation_at':d+'T21:00:00Z','open_orders':payload({'orders':[]},d+'T20:00:00Z',d+'T20:05:00Z'),
            'marks':{'AAA':payload({'price':p,'basis':'UNADJUSTED'},d+'T20:00:00Z',d+'T20:05:00Z','mark:'+d)}} for d,p in zip(dates,[100,80,90])]
 policy=payload({'strategy_id':'frozen-v1','constraints':{'gross_limit':1,'absolute_net_limit':1,'name_limit':1,'participation_limit':.2,'minimum_cash':0,'max_risk_mark_age_seconds':180,'max_liquidity_age_hours':96}})
 policy.update(policy_id='p1',fold_id='oos-1',training_label_cutoff_at='2026-08-01T00:00:00Z',training_ended_at='2026-08-02T00:00:00Z',embargo_hours=24,evaluation_start_at=dates[0]+'T14:00:00Z',evaluation_end_at=dates[-1]+'T20:00:00Z')
 decisions=[];fills=[]
 for d,qty,p in [(dates[0],5,100),(dates[2],-5,90)]:
  decisions.append({'decision_id':'decision:'+d,'sequence':1,'decided_at':d+'T14:01:00Z','policy_id':'p1','inputs':[payload({'factor':0})],
                    'open_orders':payload({'orders':[{'order_id':'order:'+d,'symbol':'AAA','side':'BUY' if qty>0 else 'SELL','remaining_quantity':abs(qty),'limit_price':p,'reduce_only':qty<0}]},d+'T14:01:00Z'), 'risk_marks':{'AAA':payload({'price':p,'basis':'UNADJUSTED'},d+'T14:00:30Z')}})
  fills.append({'fill_id':'fill:'+d,'order_id':'order:'+d,'source_record_id':'fill:'+d,'decision_id':'decision:'+d,'symbol':'AAA','executed_at':d+'T14:02:00Z','available_at':d+'T14:03:00Z','sequence':2,'quantity':qty,'price':p,'fees':1,
                'liquidity':payload({'basis':'TRAILING_DOLLAR_ADV','value':10000})})
 return {'schema_version':SCHEMA,'ledger_id':'sim-ledger','book_id':'sim-book','account_id':'sim-account','book_type':'SIMULATED_RESEARCH_BOOK','currency':'USD','accounting_basis':'TRADE_DATE_CASH','account_type':'RESEARCH_MARGIN_ACCOUNT','generated_at':'2026-09-08T00:00:00Z',
         'initial':{'as_of':'2026-09-01T13:00:00Z','available_at':'2026-09-01T13:01:00Z','source_snapshot_id':'independent-initial','cash':1000,'liabilities':0,'positions':{},'receivables':0,'equity_nav':1000},
         'instruments':{'AAA':{'asset_class':'EQUITY','currency':'USD','multiplier':1}},'calendar':payload({'exchange':'XNYS','session_dates':dates}),
         'sessions':sessions,'policies':[policy],'decisions':decisions,'fills':fills,'actions':[],'cash_events':[],
         'financing':[{'session':d,'source_record_id':'funding:'+d,'available_at':d+'T21:00:00Z','cash_interest':0,'margin_interest':0,'borrow_fees':{}} for d in dates],
         'reconciliations':[{'session':d,'source_record_id':'independent:'+d,'available_at':d+'T22:00:00Z','cash':cash,'liabilities':0,'receivables':0,'positions':qty,'equity_nav':nav} for d,cash,qty,nav in [(dates[0],499,{'AAA':5},999),(dates[1],499,{'AAA':5},899),(dates[2],948,{},948)]],
         'coverage':{k:True for k in ('fills','cash','corporate_actions','marks','financing','reconciliations','orders')}}


def align_test_orders(doc):
 # Update the synthetic accepted order to match a deliberately changed test fill.
 for fill in doc['fills']:
  decision=next(d for d in doc['decisions'] if d['decision_id']==fill['decision_id'])
  orders=[{'order_id':fill['order_id'],'symbol':fill['symbol'],'side':'BUY' if fill['quantity']>0 else 'SELL',
           'remaining_quantity':abs(fill['quantity']),'limit_price':fill['price'],'reduce_only':fill is doc['fills'][-1]}]
  decision['open_orders']=payload({'orders':orders},decision['decided_at'])


class LedgerTests(unittest.TestCase):
 def test_real_daily_marks_show_no_trade_day_drawdown_and_fees(self):
  out=replay_research_ledger(fixture(),NOW)
  self.assertEqual(out['status'],'READY',out)
  self.assertEqual([p['equity_nav'] for p in out['nav_curve']],[999,899,948])
  self.assertAlmostEqual(out['max_drawdown_pct'],-10.1)
  self.assertEqual(len(out['daily_returns']),3)
  self.assertFalse(out['publication_eligible']);self.assertIsNone(out['annualized_return_pct'])
 def test_future_feature_or_training_labels_cannot_enter_test_fold(self):
  doc=fixture();doc['decisions'][0]['inputs'][0]['available_at']='2026-09-02T00:00:00Z'
  self.assertIn('unavailable at decision',replay_research_ledger(doc,NOW)['reason'])
  doc=fixture();doc['policies'][0]['training_ended_at']='2026-09-01T14:00:00Z'
  self.assertIn('overlap evaluation',replay_research_ledger(doc,NOW)['reason'])
 def test_overlapping_positions_exceed_cap_and_block_entire_replay(self):
  doc=fixture();doc['instruments']['BBB']=dict(doc['instruments']['AAA'])
  event=copy.deepcopy(doc['fills'][0]);event.update(fill_id='second',order_id='bbb-order',symbol='BBB',quantity=6,sequence=3)
  doc['fills'].append(event)
  data=doc['decisions'][0]['open_orders']['payload'];data['orders'].append({'order_id':'bbb-order','symbol':'BBB','side':'BUY','remaining_quantity':6,'limit_price':100})
  doc['decisions'][0]['open_orders']['sha256']=digest(data)
  doc['decisions'][0]['risk_marks']['BBB']=copy.deepcopy(doc['decisions'][0]['risk_marks']['AAA'])
  out=replay_research_ledger(doc,NOW)
  self.assertEqual(out['status'],'BLOCKED');self.assertIn('gross capital limit',out['reason']);self.assertEqual(out['nav_curve'],[])
 def test_missing_or_wrong_session_marks_never_forward_fill(self):
  doc=fixture();doc['sessions'][1]['marks']={}
  self.assertEqual(replay_research_ledger(doc,NOW)['status'],'BLOCKED')
  doc=fixture();doc['sessions'][1]['marks']['AAA']['observed_at']='2026-09-01T20:00:00Z'
  self.assertIn('aligned actual closing session',replay_research_ledger(doc,NOW)['reason'])
 def test_split_dividend_receivable_and_payment_do_not_create_phantom_pnl(self):
  doc=fixture();day='2026-09-02'
  doc['actions']=[{'action_id':'split','source_record_id':'issuer-split','symbol':'AAA','kind':'SPLIT','ratio':2,'effective_at':day+'T14:00:00Z','available_at':'2026-08-31T12:00:00Z','sequence':0},
                  {'action_id':'div','source_record_id':'issuer-div','symbol':'AAA','kind':'DIVIDEND','cash_per_share':1,'pay_at':'2026-09-03T14:00:00Z','effective_at':day+'T14:00:00Z','available_at':'2026-08-31T12:00:00Z','sequence':1}]
  doc['sessions'][1]['marks']['AAA']=payload({'price':49,'basis':'UNADJUSTED'},day+'T20:00:00Z')
  doc['fills'][1].update(quantity=-10,price=55)
  align_test_orders(doc)
  doc['decisions'][1]['risk_marks']['AAA']=payload({'price':55,'basis':'UNADJUSTED'},'2026-09-03T14:00:30Z')
  doc['reconciliations'][1].update(positions={'AAA':10},receivables=10,equity_nav=999)
  doc['reconciliations'][2].update(cash=1058,equity_nav=1058)
  out=replay_research_ledger(doc,NOW)
  self.assertEqual(out['status'],'READY',out)
  self.assertEqual([p['equity_nav'] for p in out['nav_curve']],[999,999,1058])
 def test_external_capital_deposit_is_not_return(self):
  doc=fixture();day='2026-09-02'
  doc['cash_events']=[{'event_id':'deposit','source_record_id':'bank-deposit','kind':'EXTERNAL_FLOW','at':day+'T14:00:00Z','available_at':day+'T14:00:00Z','amount':100,'sequence':0}]
  doc['sessions'][1]['marks']['AAA']=payload({'price':100,'basis':'UNADJUSTED'},day+'T20:00:00Z')
  doc['sessions'][1]['opening_marks']={'AAA':payload({'price':100,'basis':'UNADJUSTED'},day+'T14:00:00Z')}
  doc['reconciliations'][1].update(cash=599,equity_nav=1099)
  doc['reconciliations'][2].update(cash=1048,equity_nav=1048)
  out=replay_research_ledger(doc,NOW)
  self.assertEqual(out['status'],'READY',out);self.assertEqual(out['daily_returns'][1]['return'],0)
 def test_cash_flow_after_overnight_gap_uses_subperiod_twr(self):
  doc=fixture();day='2026-09-02'
  doc['cash_events']=[{'event_id':'deposit','source_record_id':'bank-deposit','kind':'EXTERNAL_FLOW','at':day+'T14:00:00Z','available_at':day+'T14:00:00Z','amount':100,'sequence':0}]
  doc['sessions'][1]['opening_marks']={'AAA':payload({'price':80,'basis':'UNADJUSTED'},day+'T14:00:00Z')}
  doc['reconciliations'][1].update(cash=599,equity_nav=999)
  doc['reconciliations'][2].update(cash=1048,equity_nav=1048)
  out=replay_research_ledger(doc,NOW)
  self.assertEqual(out['status'],'READY',out)
  self.assertAlmostEqual(out['daily_returns'][1]['return'],899/999-1)
  doc['sessions'][1].pop('opening_marks')
  self.assertIn('opening valuation marks',replay_research_ledger(doc,NOW)['reason'])
 def test_short_locate_and_daily_borrow_costs_required(self):
  doc=fixture();doc['fills'][0].update(quantity=-2,fees=0);doc['fills'][1].update(quantity=2,fees=0)
  align_test_orders(doc)
  self.assertEqual(replay_research_ledger(doc,NOW)['status'],'BLOCKED')
  doc['fills'][0]['locate']=payload({'shares':2,'expires_at':'2026-09-04T00:00:00Z'})
  for row in doc['financing']:row['borrow_fees']={'AAA':2}
  for row,cash,nav,qty in zip(doc['reconciliations'],[1198,1196,1014],[998,1036,1014],[-2,-2,0]):
   row.update(cash=cash,equity_nav=nav,positions={'AAA':qty} if qty else {})
  out=replay_research_ledger(doc,NOW);self.assertEqual(out['status'],'READY',out);self.assertEqual(out['final_nav'],1014)
  doc['financing'][1]['borrow_fees']={}
  self.assertIn('borrowing charge coverage',replay_research_ledger(doc,NOW)['reason'])
 def test_every_session_must_reconcile_and_duplicates_rejected(self):
  doc=fixture();doc['reconciliations'][1]['equity_nav']=900
  self.assertIn('reconciliation mismatch',replay_research_ledger(doc,NOW)['reason'])
  doc=fixture();doc['reconciliations'].pop()
  self.assertIn('every calendar session',replay_research_ledger(doc,NOW)['reason'])
  doc=fixture();doc['fills'].append(copy.deepcopy(doc['fills'][0]))
  self.assertIn('event id missing/duplicate',replay_research_ledger(doc,NOW)['reason'])
 def test_owner_data_and_unsupported_derivatives_are_rejected(self):
  doc=fixture();doc['book_type']='LIVE_BROKER_BOOK'
  self.assertIn('owner broker ledger rejected',replay_research_ledger(doc,NOW)['reason'])
  doc=fixture();doc['instruments']['AAA']['asset_class']='OPTION'
  self.assertIn('unsupported instrument',replay_research_ledger(doc,NOW)['reason'])
 def test_reserved_open_orders_consume_capacity_and_stale_adv_blocks(self):
  doc=fixture();doc['decisions'][0]['open_orders']=payload({'orders':doc['decisions'][0]['open_orders']['payload']['orders']+[{'order_id':'pending','symbol':'AAA','side':'BUY','remaining_quantity':6,'limit_price':100}]},doc['decisions'][0]['decided_at'])
  self.assertIn('gross capital limit',replay_research_ledger(doc,NOW)['reason'])
  doc=fixture();doc['fills'][0]['liquidity']['observed_at']='2025-01-01T00:00:00Z'
  self.assertIn('stale execution liquidity',replay_research_ledger(doc,NOW)['reason'])
 def test_same_order_cannot_fill_beyond_cumulative_remaining_quantity(self):
  doc=fixture();doc['initial'].update(cash=3000,equity_nav=3000)
  first=doc['fills'][0];second=copy.deepcopy(first);second.update(fill_id='duplicate-order-second-fill',source_record_id='second',sequence=3)
  doc['fills']=[first,second]
  order=doc['decisions'][0]['open_orders']['payload']['orders'][0];order['remaining_quantity']=6
  doc['decisions'][0]['open_orders']['sha256']=digest(doc['decisions'][0]['open_orders']['payload'])
  self.assertIn('cumulative fills exceed',replay_research_ledger(doc,NOW)['reason'])
 def test_intraday_dividend_payment_settles_before_close(self):
  doc=fixture();day='2026-09-02'
  doc['actions']=[{'action_id':'div-intraday','source_record_id':'issuer-div','symbol':'AAA','kind':'DIVIDEND','cash_per_share':1,'pay_at':day+'T16:00:00Z','effective_at':day+'T14:00:00Z','available_at':'2026-08-31T12:00:00Z','sequence':0}]
  doc['reconciliations'][1].update(cash=504,equity_nav=904)
  doc['reconciliations'][2].update(cash=953,equity_nav=953)
  out=replay_research_ledger(doc,NOW)
  self.assertEqual(out['status'],'READY',out);self.assertEqual(out['nav_curve'][1]['receivables'],0)
 def test_no_fill_decision_and_closing_orders_still_consume_capital(self):
  doc=fixture();decision=copy.deepcopy(doc['decisions'][0]);decision.update(decision_id='unfilled',decided_at='2026-09-02T14:01:00Z')
  decision['risk_marks']={'AAA':payload({'price':80,'basis':'UNADJUSTED'},'2026-09-02T14:00:30Z')}
  decision['open_orders']=payload({'orders':[{'order_id':'huge','symbol':'AAA','side':'BUY','remaining_quantity':10000,'limit_price':100}]},decision['decided_at'])
  doc['decisions'].append(decision)
  self.assertIn('gross capital limit',replay_research_ledger(doc,NOW)['reason'])
  doc=fixture();doc['sessions'][1]['open_orders']=payload({'orders':[{'order_id':'huge','symbol':'AAA','side':'BUY','remaining_quantity':10000,'limit_price':100}]},'2026-09-02T20:00:00Z')
  self.assertIn('gross capital limit',replay_research_ledger(doc,NOW)['reason'])
 def test_payload_checksum_and_cumulative_participation_guard(self):
  doc=fixture();doc['decisions'][0]['inputs'][0]['payload']['factor']=99
  self.assertIn('checksum mismatch',replay_research_ledger(doc,NOW)['reason'])
  doc=fixture();doc['fills'][0]['liquidity']=payload({'basis':'TRAILING_DOLLAR_ADV','value':1000})
  self.assertIn('participation cap exceeded',replay_research_ledger(doc,NOW)['reason'])

if __name__=='__main__':unittest.main()
