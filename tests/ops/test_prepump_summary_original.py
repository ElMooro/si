"""Preserved consumer counterexamples, package-only scope and no live data."""
from pathlib import Path
from datetime import datetime,timezone
from types import SimpleNamespace
import ast,json,sys,time,unittest
ROOT=Path(__file__).resolve().parents[2]
sys.path[:0]=[str(ROOT/p) for p in ('aws/ops/staged','aws/ops','aws/ops/checks')]
import ops_6271_prepump_summary_original_baseline as op
RAW=(ROOT/'tests/fixtures/pre-prepump-summary-evidence.py.txt').read_bytes();TREE=ast.parse(RAW)


def original(raw):
    outputs={};nodes=[n for n in TREE.body if isinstance(n,ast.FunctionDef) and n.name in ('lambda_handler','freshness_seconds')]
    scope={'time':time,'datetime':datetime,'timezone':timezone,'Optional':__import__('typing').Optional,
        'INPUTS':dict.fromkeys(('brief','positioning','catalysts','clusters','early')),'json':json,
        'load_s3_json':lambda key:raw.get(key),'S3_BUCKET':'synthetic','OUTPUT_KEY':'data/pump-radar-summary.json',
        'put_gzipped':lambda *a,**k:0,'s3':SimpleNamespace(put_object=lambda **kw:outputs.update({kw['Key']:json.loads(kw['Body'])})),
        'print':lambda *a,**k:None}
    scope['INPUTS']={k:k for k in scope['INPUTS']}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'<isolated summary predecessor>','exec'),scope)
    scope['lambda_handler']({},None);return outputs['data/pump-radar-summary.json'],scope


class Tests(unittest.TestCase):
    def test_abstention_reopens_portfolio_position_fallback(self):
        p,_=original({'brief':{'call':'WAIT','sizing_eligible':False,'top_3_long_ideas':[]},
            'positioning':{'aggressive_basket':{'positions':[{'ticker':'SYN_A','position_pct':99,'pump_confirmed':True}]}}})
        self.assertEqual(p['top_picks'][0]['position_pct'],99);self.assertIs(p['top_picks'][0]['pump_confirmed'],True)
        self.assertNotIn('sizing_eligible',p)
    def test_missing_all_sources_still_publishes_a_new_empty_scorecard(self):
        p,_=original({});self.assertEqual(p['catalysts']['n_a_grade'],0);self.assertEqual(p['early']['n_actionable'],0)
        self.assertEqual(p['top_picks'],[]);self.assertTrue(p['generated_at']);self.assertTrue(all(v is False for v in p['sources_loaded'].values()))
    def test_unvalidated_model_size_text_is_parsed_as_real_position(self):
        p,_=original({'brief':{'top_3_long_ideas':[{'ticker':'SYN_A','sized_position':'Not approved: 250% position'}]}})
        self.assertEqual(p['top_picks'][0]['position_pct'],250)
    def test_future_source_clock_becomes_negative_age(self):
        p,ns=original({'brief':{'generated_at':'2099-01-01T00:00:00Z'}})
        self.assertLess(p['sources_freshness']['brief_seconds'],0)
    def test_wrong_source_container_can_crash_the_summary(self):
        with self.assertRaises(AttributeError):original({'positioning':{'aggressive_basket':'not a basket'}})
    def test_capture_is_pinned_and_has_no_consumer_packet_read_or_invocation(self):
        self.assertEqual(op.source_check(RAW)['bytes'],len(RAW))
        with self.assertRaises(ValueError):op.source_check(RAW+b'\n')
        tree=ast.parse(Path(op.__file__).read_bytes());calls={n.func.attr for n in ast.walk(tree) if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute)}
        self.assertFalse(calls & {'get_object','invoke','put_object','update_schedule','put_rule','send_message','publish'})
        self.assertIn('exit',calls);self.assertNotIn('data/pump-radar-summary.json',Path(op.__file__).read_text(encoding='utf-8'))


if __name__=='__main__':unittest.main(verbosity=2)
