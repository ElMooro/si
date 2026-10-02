from pathlib import Path
import ast,copy,hashlib,json,unittest
R=Path(__file__).resolve().parents[1];D=R/'tests/fixtures/ranker-final-audit'
before=(D/'lambda-before.py.txt').read_text(encoding='utf-8');after=(R/'aws/lambdas/justhodl-master-ranker/source/lambda_function.py').read_text(encoding='utf-8')
def handler(text):return next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
def blocks(text):
 return [n for n in handler(text).body if isinstance(n,ast.For) and isinstance(n.target,ast.Name) and (n.target.id=='_tt' and any(isinstance(v,ast.Constant) and v.value=='audit_trail' for v in ast.walk(n)) or n.target.id=='t' and any(isinstance(v,ast.Subscript) and isinstance(v.value,ast.Name) and v.value.id=='t' and isinstance(v.slice,ast.Constant) and v.slice.value=='rationale' and isinstance(v.ctx,ast.Store) for v in ast.walk(n)))]
def apply(text,rows,structural=None,capture=None,high=None):
 rows=copy.deepcopy(rows);scope={'top_tickers':rows,'_structural':structural or {},'_hiconv':high or set(),'n_structural':0,'_cap_rows':capture or {},'n_capture':0}
 nodes=blocks(text);assert len(nodes)==3
 for node in nodes:exec(compile(ast.Module(body=[node],type_ignores=[]),'<reviewed exact annotation block>','exec'),scope)
 return rows
class AuditTiming(unittest.TestCase):
 def test_predecessor_retains_stale_rationale_candidate_matches_final(self):
  rows=[{'ticker':'QAONLY','score':0,'rationale':'Invented rationale'}];overlay={'QAONLY':{'criticality':1}}
  old=apply(before,rows,overlay)[0];new=apply(after,rows,overlay)[0]
  self.assertNotEqual(old['rationale'],old['audit_trail']['rationale']);self.assertEqual(new['rationale'],new['audit_trail']['rationale']);self.assertEqual(new['audit_trail']['final_score'],0)
 def test_both_annotations_and_catchup_are_reflected_without_score_changes(self):
  row={'ticker':'QAONLY','score':-2,'n_systems':0,'systems':[],'contributions':{'invented':0},'capital_flow_mult':0,'risk_regime_mult':None,'t360_coverage':0,'t360_domains':[],'red_flags':['invented']}
  arguments=([row],{'QAONLY':{'criticality':0}},{'QAONLY':{'capture_gap':4,'tier':'STRUCTURALLY_UNDERVALUED','catchup_pct':0,'catchup_basis':'invented'}},{'QAONLY'})
  old=apply(before,*arguments)[0];new=apply(after,*arguments)[0]
  self.assertEqual({k:v for k,v in old.items() if k!='audit_trail'},{k:v for k,v in new.items() if k!='audit_trail'})
  audit=new['audit_trail'];self.assertEqual(audit['rationale'],new['rationale']);self.assertEqual(audit['final_score'],-2);self.assertEqual(audit['multipliers_applied'],{'capital_flow_mult':0});self.assertEqual(audit['score_breakdown']['n_systems'],0)
 def test_no_annotations_preserve_entire_row(self):
  rows=[{'ticker':'QAONLY','score':1,'rationale':None},{'ticker':'QANEXT','score':0,'rationale':''}]
  self.assertEqual(apply(before,rows),apply(after,rows))
 def test_multiple_rows_do_not_share_rationale(self):
  rows=[{'ticker':'QAONLY','score':1,'rationale':'one'},{'ticker':'QANEXT','score':2,'rationale':'two'}];out=apply(after,rows,{'QAONLY':{'criticality':1}})
  self.assertEqual(out[1]['rationale'],'two');self.assertEqual(out[1]['audit_trail']['rationale'],'two');self.assertNotEqual(out[0]['rationale'],out[1]['rationale'])
 def test_whole_source_changes_only_unchanged_block_position(self):
  t=json.loads((D/'transition.json').read_bytes());self.assertEqual(hashlib.sha256(before.encode()).hexdigest(),t['before_sha256']);self.assertEqual(hashlib.sha256(after.encode()).hexdigest(),t['after_sha256'])
  self.assertEqual(before.count(t['moved_block']),1);self.assertEqual(after.count(t['moved_block']),1);self.assertEqual(before.replace(t['moved_block'],''),after.replace(t['moved_block'],''))
if __name__=='__main__':unittest.main(verbosity=2)
