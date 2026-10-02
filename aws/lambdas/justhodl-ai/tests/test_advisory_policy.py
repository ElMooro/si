"""Exact source policy gate, with invented policy values and no operational I/O."""
from pathlib import Path
import ast,hashlib,json,os,types,unittest
R=Path(__file__).resolve().parents[4]
SOURCE=R/'aws/lambdas/justhodl-ai/source/lambda_function.py'
PRIOR=R/'tests/fixtures/ai-pre-advisory-policy-20261002.py'
def policy_function(path,environment):
 text=path.read_text(encoding='utf-8');node=next(n for n in ast.parse(text).body if isinstance(n,ast.FunctionDef) and n.name=='_ledger_while_advisory')
 scope={'os':types.SimpleNamespace(environ={} if environment is None else {'AI_ENVIRONMENT':environment})}
 exec(compile(ast.Module(body=[node],type_ignores=[]),str(path),'exec'),scope)
 return scope['_ledger_while_advisory']
class Policy(unittest.TestCase):
 def test_retained_exact_predecessor_reproduces_truthy_strings_and_lists(self):
  proof=json.loads((R/'docs/audit/2026-10-02/ai-advisory-policy-reproduction.json').read_bytes());self.assertEqual(hashlib.sha256(PRIOR.read_bytes()).hexdigest(),proof['source_sha256']);fn=policy_function(PRIOR,'production')
  for row in proof['cases']:self.assertIs(fn({'ledger_calls_when_advisory':row['input']}),row['opens_advisory_ledger'])
 def test_only_actual_true_opens_an_explicit_advisory_override(self):
  for mode in (None,'production','test','review',' REVIEW '):
   fn=policy_function(SOURCE,mode)
   for value in [False,'false','true','0',0,1,1.0,None,[],['false'],{}, {'enabled':True}]:
    with self.subTest(mode=mode,value=value):self.assertIs(fn({'ledger_calls_when_advisory':value}),False)
   self.assertIs(fn({'ledger_calls_when_advisory':True}),True)
 def test_explicit_false_still_overrides_review_default(self):
  self.assertIs(policy_function(SOURCE,'review')({'ledger_calls_when_advisory':False}),False)
 def test_absent_policy_key_preserves_each_existing_environment_rule(self):
  for mode in (None,'production','test','review',' REVIEW ','preview',''):
   for value in ({},{'unrelated':True},None):self.assertIs(policy_function(SOURCE,mode)(value),policy_function(PRIOR,mode)(value))
 def test_policy_values_are_never_rewritten(self):
  value={'ledger_calls_when_advisory':'false','unrelated':{'original':[1,2]}};before=json.dumps(value);policy_function(SOURCE,'production')(value);self.assertEqual(json.dumps(value),before)
 def test_whole_handler_has_exactly_the_one_reviewed_gate_replacement(self):
  before=PRIOR.read_text(encoding='utf-8');needle='        return bool(policy["ledger_calls_when_advisory"])\n';self.assertEqual(before.count(needle),1);self.assertEqual(SOURCE.read_text(encoding='utf-8'),before.replace(needle,'        return policy["ledger_calls_when_advisory"] is True\n'))
 def test_both_existing_read_paths_use_the_shared_gate(self):
  nodes=ast.parse(SOURCE.read_text(encoding='utf-8')).body
  for name in ('action_market_read','settle_owned_read'):
   fn=next(n for n in nodes if isinstance(n,ast.FunctionDef) and n.name==name);calls=[n for n in ast.walk(fn) if isinstance(n,ast.Call) and isinstance(n.func,ast.Name) and n.func.id=='_ledger_while_advisory'];self.assertEqual(len(calls),1)
 def test_no_paid_regression_retains_every_assertion_through_one_exact_hook(self):
  prior=R/'tests/fixtures/ai-advisory-policy/no-paid-callers.before.py.txt';self.assertEqual(hashlib.sha256(prior.read_bytes()).hexdigest(),'8cfd5dd4b0acd3689ad5d28e4351dbfd20bcacf79bd355b33f96acf61ef72bae')
  text=prior.read_text(encoding='utf-8');before=' return text\n\ndef method';after=' from helpers.ai_advisory_policy_preservation import apply\n return apply(path,text)\n\ndef method';self.assertEqual(text.count(before),1);self.assertEqual((R/'tests/test_no_paid_callers.py').read_text(encoding='utf-8'),text.replace(before,after))
if __name__=='__main__':unittest.main()
