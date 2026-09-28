"""Production adapters with synthetic storage only. Never invoke consumers or send messages."""
from pathlib import Path
from copy import deepcopy
from unittest.mock import patch
import ast,json,sys,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/'aws/shared'),str(ROOT/'aws/shared/tests')]
import momentum_research_boundary as boundary


def extract(name,names,env=None):
    ns={'momentum_exclusion':boundary.exclusion,'MOMENTUM_SOURCE':boundary.DIRECT,
        'MOMENTUM_BASIS':boundary.BASIS,'momentum_guard':boundary.guard,
        'momentum_transition_state':boundary.transition_state,'json':json,'Optional':__import__('typing').Optional,
        'List':list,'Dict':dict,**(env or {})}
    tree=ast.parse((ROOT/'aws/lambdas'/('justhodl-'+name)/'source/lambda_function.py').read_bytes())
    nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name in names]
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated consumer functions>','exec'),ns);return ns


class Forbidden:
    def __getattr__(self,key):raise AssertionError('No external storage, messages or service requests')


class Tests(unittest.TestCase):
    def test_all_seven_consumers_abstain_before_storage(self):
        for name,fn in [('compound-aggregator','load_packet'),('master-ranker','fetch_json'),
                        ('momentum-leaders','load_s3_json'),('velocity-acceleration','load_s3_json'),('opportunity-screener','read_json')]:
            ns=extract(name,[fn],{'S3':Forbidden(),'s3':Forbidden(),'_FEED_HEALTH':[]})
            self.assertEqual(ns[fn](boundary.DIRECT),boundary.exclusion())
        ns=extract('best-ideas',['harvest'],{'s3':Forbidden()})
        self.assertEqual(ns['harvest'](('momentum','label',boundary.DIRECT,'all_qualifying','symbol','score','technical','phrase',25))[0],{})
        ns=extract('convergence-radar',['fetch_engine_raw','extract_ticker_signals_from_engine'],{'s3':Forbidden()})
        spec={'key':boundary.DIRECT}
        self.assertEqual(ns['fetch_engine_raw']('momentum-breakout',spec),('momentum-breakout',[],None))
        self.assertEqual(ns['extract_ticker_signals_from_engine']('momentum-breakout',spec,[{'symbol':'FAKE','score':999}]),{})
    def test_compound_no_membership_count_or_preboundary_percentile(self):
        from test_holdings_derived_boundary import load,Storage
        from holdings_derived_boundary import BASIS
        m=load('justhodl-compound-aggregator')
        history={'d':'2026-01-01','scores':{'KO':999999},'score_basis':BASIS,
                 'activist_boundary':'ownership-feed-abstention.v1','volatility_boundary':'price-compression-abstention.v1'}
        db=Storage({boundary.DIRECT:{'calls_eligible':True,'summary':{'top_25_overall':[{'symbol':'FAKE','score':999999}]}},
            'data/nobrainers.json':{'summary':{'top_25_overall':[{'ticker':'KO','score':40}]}},
            'data/insider-clusters.json':{'clusters':[{'ticker':'KO','score':60}]},'data/compound-history.json':{'days':[history]}})
        with patch.object(m,'S3',db),patch.object(m,'emit_alerts',side_effect=AssertionError('No message')):
            m.lambda_handler({'suppress_alerts':True},None)
        p=db.writes[m.S3_KEY];self.assertEqual([r['symbol'] for r in p['compound']],['KO'])
        self.assertEqual(p['compound'][0]['n_systems'],2);self.assertEqual(p['feed_stats']['momentum'],0)
        self.assertNotIn('pctile_90d_self',p['compound'][0]);self.assertNotIn(boundary.DIRECT,db.reads)
        self.assertEqual(p['momentum_research_exclusion'],boundary.exclusion())
        self.assertEqual(db.writes['data/compound-history.json']['days'][0],history)
        self.assertEqual(db.writes['data/compound-history.json']['days'][-1]['momentum_boundary'],boundary.BASIS)
    def test_abstained_engine_not_counted_as_loaded(self):
        from concurrent.futures import ThreadPoolExecutor,as_completed
        calls=[]
        def fetch(name,spec):calls.append(name);return name,[],None
        ns=extract('convergence-radar',['fetch_all_engines'],{'ENGINE_EXTRACTORS':{'momentum-breakout':{'key':boundary.DIRECT},'other':{'key':'synthetic'}},
            'ThreadPoolExecutor':ThreadPoolExecutor,'as_completed':as_completed,'fetch_engine_raw':fetch})
        records,ages=ns['fetch_all_engines']();self.assertEqual(calls,['other']);self.assertEqual(ages,{'other':None});self.assertEqual(records,{})
    def test_stored_compound_momentum_cannot_enter_master_index(self):
        from test_holdings_derived_boundary import load
        from holdings_derived_boundary import BASIS
        m=load('justhodl-master-ranker');m.engine_trust=None
        feeds={'data/compound-signals.json':{'holdings_exclusions':{'basis':BASIS},'compound':[
            {'symbol':'FAKE','compound_score':150,'n_systems':2,'systems':['momentum','insiders'],'scores':{'momentum':60,'insiders':40}}]},
            'data/insider-clusters.json':{'clusters':[{'ticker':'KO','n_insiders':3,'total_value':100}]}}
        with patch.object(m,'fetch_json',side_effect=lambda key,**kw:feeds.get(key)):
            idx,_=m.build_ticker_index()
        self.assertEqual(set(idx),{'KO'})
    def test_indirect_old_composites_cannot_seed_leaders_or_velocity(self):
        from test_holdings_derived_boundary import Storage
        forged={'calls_eligible':True,'momentum_research_exclusion':{'basis':boundary.BASIS},
                'pump_candidates':[{'ticker':'FAKE','n_engines':99}],'leaders':[{'ticker':'FAKE'}]}
        for name in ('momentum-leaders','velocity-acceleration'):
            db=Storage({key:forged for key in boundary.COMPOSITES})
            ns=extract(name,['load_s3_json'],{'s3':db,'S3_BUCKET':'fixture'})
            for key in boundary.COMPOSITES:self.assertEqual(ns['load_s3_json'](key),boundary.exclusion())
        p={'momentum_research_exclusion':boundary.exclusion(),'tickers':[{'ticker':'KO'}]}
        self.assertIs(boundary.guard(boundary.COMPOSITES[0],p),p)
    def test_old_pending_state_is_preserved_but_cannot_promote(self):
        state={'pending':{'FAKE':{'confirmations':['momentum-breakout'],'status':'confirmed'}},'last_trading_date':'2026-09-25','unknown':'keep'}
        before=deepcopy(state);out=boundary.transition_state(state)
        self.assertEqual(state,before);self.assertEqual(out['momentum_preboundary_state'],before);self.assertEqual(out['pending'],{})
        self.assertEqual(out['unknown'],'keep');self.assertIs(boundary.transition_state(out),out)
        ns=extract('velocity-acceleration',['load_state'],{'STATE_KEY':'synthetic-state','load_s3_json':lambda key:state})
        self.assertEqual(ns['load_state']()['pending'],{})
    def test_incomparable_count_history_has_no_fabricated_acceleration_or_message(self):
        records=[{'ticker':'FAKE','n_engines':99}];result=boundary.transition_abstention(records)
        self.assertFalse(result['sent']);self.assertIsNone(records[0]['prior_n_engines']);self.assertFalse(records[0]['is_accelerating'])
        text=(ROOT/'aws/lambdas/justhodl-convergence-radar/source/lambda_function.py').read_text(encoding='utf-8')
        self.assertIn('save_state(records[:100], prior_state)',text)
        self.assertIn('if not history_comparable:',text)
        self.assertIn('momentum_preboundary_state',text)


if __name__=='__main__':unittest.main(verbosity=2)
