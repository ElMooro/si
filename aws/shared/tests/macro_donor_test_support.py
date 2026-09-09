"""Behavioral tests of production joins/handlers with all I/O replaced in memory."""
import ast
import copy
import io
import json
import sys
import time
import unittest
from datetime import datetime,timezone,timedelta
from pathlib import Path

ROOT=Path(__file__).resolve().parents[3]
sys.path.insert(0,str(ROOT/'aws/shared'))
from macro_donor_inputs import *
NOW=datetime(2026,9,9,tzinfo=timezone.utc)
STAMP=NOW.isoformat()


def repo(score=80):
 return {'generated_at':STAMP,'as_of':'2026-09-08','repo_stress_score':score,'regime':'STRESS',
         'distribution':{'as_of':'2026-09-08','tail_bps':40},'spreads':{'sofr_iorb':{'bps':30}},'facilities':{'srf_usd_bn':0,'srf_as_of':None}}


def ciss():
 return {'generated_at':STAMP,'ea_composite_date':'2026-09-08','ea_composite':0,'ea_regime':'LOW',
         'series':[{'id':'new','area':'FR','latest':0,'latest_date':'2026-09-08'}, {'id':'old','area':'FR','latest':.8,'latest_date':'2024-01-01','discontinued':True}], 'provenance':{'provider':'ECB'}}


def fails():
 return {'generated_at':STAMP,'as_of':'2026-08-26','treasury':{'as_of':'2026-08-26','complete':True,'ftd_bn':0,'ftr_bn':15,'gross_bn':15,'score':90},
         'classes':[{'key':'TIPS','ftd_latest':0,'ftr_latest':3}], 'totals':{'ftd':0,'ftr':15}}


def bond_docs():
 return {REPO:repo(),FAILS:fails(), BOND_KEYS[1]:{'generated_at':'2026-09-04T21:31:58Z','as_of':'2026-08-26',
         'by_tenor_usd_b':{'TREASURY_COUPONS':{'2':-4}}, 'net_positions_usd_b':{'TREASURY_COUPONS':-4},'wow_usd_b':{'TREASURY_COUPONS':0},
         'z_52w':{'TREASURY_COUPONS':0},'financing':{'as_of':'2026-08-26','repo_out_b':4},'transactions':{'TREASURY':{'weekly_b':50,'as_of':'2026-08-26'}}},
         BOND_KEYS[3]:{'generated_at':STAMP,'latest':{'date':'2026-09-08'},'decomposition':{'acm_fitted_10y_pct':4,'risk_neutral_10y_pct':3,'term_premium_10y_pct':1},'source':'NY Fed ACM'},
         BOND_KEYS[4]:{'generated_at':STAMP,'graded_auctions':[{'cusip':'A','auction_date':'2026-09-08','dimensions':{'tail_bp':{'value':0}}}]},
         BOND_KEYS[5]:{'generated_at':STAMP,'total_foreign_holdings':{},'net_purchases':{}}}


def firm():
 return {'generated_at':STAMP,'firm':{'gross_exposure_pct':4,'net_exposure_pct':0},'desk_conflicts':['XYZ'],
         'equity_book':[{'symbol':'XYZ','net_pct':0,'gross_pct':4,'desks':{'long':2,'short':-2},'desk_conflict':True}]}


def extract(engine,names):
 p=ROOT/'aws/lambdas'/engine/'source/lambda_function.py'
 tree=ast.parse(p.read_text());ns={'json':json,'time':time,'datetime':datetime,'timezone':timezone,'timedelta':timedelta}
 for n in tree.body:
  if isinstance(n,(ast.Assign,ast.AnnAssign)):
   try:
    value=ast.literal_eval(n.value)
    targets=n.targets if isinstance(n,ast.Assign) else [n.target]
    for target in targets:
     if isinstance(target,ast.Name):ns[target.id]=value
   except Exception:pass
 nodes=[n for n in tree.body if isinstance(n,(ast.FunctionDef,ast.AsyncFunctionDef)) and n.name in names]
 for n in nodes:n.decorator_list=[]
 exec(compile(ast.Module(body=nodes,type_ignores=[]),str(p),'exec'),ns)
 return ns


class ContractAblations(unittest.TestCase):
 def test_credit_funding_has_bounded_effect_and_stale_no_vote(self):
  got=credit_donors(repo(),ciss(),30,NOW)
  self.assertEqual(got['review_score'],80)
  r=repo();r['distribution']['as_of']='2020-01-01'
  self.assertEqual(credit_donors(r,ciss(),30,NOW)['review_score'],30)
  self.assertEqual(credit_donors(repo(0),ciss(),0,NOW)['review_score'],0)
  self.assertEqual(got['ciss_stress']['score_contribution'],0)
 def test_fails_weekly_zero_ftd_valid_but_incomplete_blocked(self):
  f=fails();self.assertTrue(fails_context(f,NOW)['contract']['usable'])
  f['treasury']['complete']=False
  self.assertFalse(fails_context(f,NOW)['contract']['usable'])
 def test_bond_funding_ablation_changes_review_and_wi_never_fabricated(self):
  docs=bond_docs();out=bond_donors(docs,NOW)
  self.assertEqual(out['funding_review'],'FUNDING_AND_SETTLEMENT_REVIEW')
  docs[REPO]=repo(0)
  self.assertEqual(bond_donors(docs,NOW)['funding_review'],'SETTLEMENT_REVIEW')
  self.assertTrue(out['dealer_inventory']['contract']['usable'])
  self.assertTrue(out['term_premium']['contract']['usable'])
  self.assertIsNone(out['auction_quality']['graded_auctions'][0]['tail_validation']['tail_bps'])
  self.assertEqual(out['foreign_demand']['transaction_status'],'BLOCKED_MISSING_SLT_TRANSACTIONS')
 def test_acm_reconciliation_and_tic_holdings_are_not_transactions(self):
  docs=bond_docs();docs[BOND_KEYS[3]]['decomposition']['risk_neutral_10y_pct']=1
  docs[BOND_KEYS[5]].update(total_foreign_holdings={'change_usd_bn':-20},net_purchases={'change_usd_bn':-20})
  out=bond_donors(docs,NOW)
  self.assertFalse(out['term_premium']['contract']['usable'])
  self.assertIsNone(out['foreign_demand']['net_transactions_usd_bn'])
 def test_bis_cbs_is_exposure_not_usd_lbs(self):
  d={'generated_at':STAMP,'ok':True,'source':'BIS CBS','total':{'period':'2026-Q1','latest_bn':0},'by_counterparty':[{'code':'CN','latest_bn':0}]}
  out=bis_context(d,NOW)
  self.assertTrue(out['contract']['usable']);self.assertEqual(out['lbs_usd_funding']['status'],'BLOCKED_MISSING_LBS')
  d['source']='BIS LBS';self.assertFalse(bis_context(d,NOW)['contract']['usable'])
 def test_ciss_legacy_excluded_and_dates_not_collapsed(self):
  out=fragmentation_context(ciss(),{'FR':{'name':'France','spread_vs_bund_bp':50,'spread_as_of':'2026-08-31'}},NOW)
  self.assertEqual(len(out['active_series']),1);self.assertEqual(len(out['legacy_discontinued_series']),1)
  self.assertEqual(out['country_spread_context'][0]['join_status'],'CONTEXT_DIFFERENT_DATES')
 def test_missing_vintage_is_explicit_and_no_revised_fallback(self):
  out=vintage_net_liquidity({'WALCL':{'updated':STAMP,'vintages':[{'date':'2026-09-01','known_on':'2026-09-02','value':1000}]}},NOW)
  self.assertEqual(out['status'],'BLOCKED');self.assertEqual(out['series'],{})
  self.assertIn('WTREGEN',out['missing_series'])
 def test_vintage_release_delay_zero_units_and_revision_not_backdated(self):
  docs={s:{'updated':STAMP,'vintages':[{'date':'2026-09-01','known_on':'2026-09-01','value':v}]} for s,v in [('WALCL',10000),('WTREGEN',1000),('RRPONTSYD',2)]}
  docs['WALCL']['vintages'].append({'date':'2026-09-01','available_at':'2026-09-04T15:00:00Z','value':11000})
  out=vintage_net_liquidity(docs,NOW)
  self.assertNotIn('2026-09-01',out['series'])
  self.assertEqual(out['series']['2026-09-02'],7000)
  self.assertEqual(out['series']['2026-09-04'],7000)
  self.assertEqual(out['series']['2026-09-07'],8000)
  self.assertEqual(out['status'],'BLOCKED') # warmup unavailable, don't overclaim
 def test_conflicting_vintage_identity_blocks(self):
  docs={s:{'updated':STAMP,'vintages':[{'date':'2026-09-01','known_on':'2026-09-01','value':0}]} for s in ('WALCL','WTREGEN','RRPONTSYD')}
  docs['WALCL']['vintages'].append({'date':'2026-09-01','known_on':'2026-09-01','value':2})
  self.assertIn('conflicting',vintage_net_liquidity(docs,NOW)['reason'])
 def test_capacity_missing_gross_fails_closed(self):
  f=firm();del f['equity_book'][0]['gross_pct']
  self.assertFalse(capacity_donors(f,repo(),NOW)['firm_book']['contract']['usable'])


class ReceiverBuilders(unittest.TestCase):
 def test_actual_receiver_builders_read_guarded_donors(self):
  specs={'justhodl-liquidity-credit-engine':'credit','justhodl-bond-warroom':'bond','justhodl-repo':'fails','justhodl-eurodollar-plumbing':'bis','justhodl-euro-fragmentation':'ciss','justhodl-liquidity-inflection':'pit','justhodl-liquidity-capacity':'capacity'}
  for engine,kind in specs.items():
   with self.subTest(engine=engine):
    ns=extract(engine,['build_audit_donor_context'])
    docs=bond_docs();docs[CISS]=ciss();reads=[]
    def read(k):reads.append(k);return docs.get(k)
    class S3:
     def get_object(self,**kw):return {'Body':io.BytesIO(json.dumps(read(kw['Key'])).encode())}
    ns.update(S3=S3(),BUCKET='fixture',_s3_json=read,sread=read,gj=read,read_existing=read,s3_json=read,get_json=read)
    if kind=='credit':out=ns['build_audit_donor_context'](30,NOW);self.assertEqual(out['review_score'],80)
    elif kind=='ciss':out=ns['build_audit_donor_context']({},NOW);self.assertEqual(out['ea_composite'],0)
    elif kind=='capacity':out=ns['build_audit_donor_context'](firm(),NOW);self.assertEqual(out['repo_market']['score'],80)
    else:out=ns['build_audit_donor_context'](NOW)
    self.assertTrue(reads);self.assertIsInstance(out,dict)
 def test_actual_capacity_handler_retains_offset_gross_and_never_inflates_volume(self):
  ns=extract('justhodl-liquidity-capacity',['lambda_handler','build_audit_donor_context','num','tier_of'])
  f=firm();f['generated_at']=datetime.now(timezone.utc).isoformat()
  docs={'data/firm-book.json':f,REPO:repo()};written=[]
  class S3:
   def put_object(self,**kw):written.append(json.loads(kw['Body']))
  ns.update(get_json=docs.get,fetch_volume_universe=lambda:{'XYZ':{'dollar_vol':1000,'price':1}},s3=S3())
  ns['lambda_handler']({},None);out=written[-1];p=out['positions'][0]
  self.assertEqual(p['gross_pct'],4);self.assertEqual(p['net_position_usd'],0)
  self.assertEqual(p['position_usd'],2000000);self.assertEqual(p['days_to_liquidate'],10000)
  self.assertFalse(out['execution_eligible'])
  ns['fetch_volume_universe']=lambda:{};ns['lambda_handler']({},None)
  self.assertIsNone(written[-1]['positions'][0]['comfortable_position_usd'])
  self.assertEqual(written[-1]['firm']['pct_liquidatable_1d'],0)
 def test_actual_matched_spread_history_rejects_mismatched_latest(self):
  ns=extract('justhodl-euro-fragmentation',['matched_spread_history'])
  out=ns['matched_spread_history']([('2026-09-08',5),('2026-08-31',4)],[('2026-09-07',2),('2026-08-31',3)])
  self.assertEqual(out,[('2026-08-31',100)])
 def test_actual_inflection_gate_removes_revised_performance(self):
  ns=extract('justhodl-liquidity-inflection',['gate_historical_outputs'])
  out={'usd':{},'backtest':{'cagr':1000},'event_study_after_flips':{'SPX':{'mean':99}},'forward_expectation':{'assets':{'SPX':99}}}
  ns['gate_historical_outputs'](out,{'status':'BLOCKED','series':{},'missing_series':['WTREGEN']})
  self.assertEqual(out['backtest']['status'],'BLOCKED');self.assertEqual(out['event_study_after_flips'],{})
  self.assertFalse(out['publication_eligible'])


def run():
 unittest.main(module=__name__)
if __name__=='__main__':run()
