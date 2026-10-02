from pathlib import Path
import hashlib,json
R=Path(__file__).resolve().parents[2]
def apply(path,text):
 if path!='aws/lambdas/justhodl-ai/source/lambda_function.py':return text
 proof=json.loads((R/'docs/audit/2026-10-02/ai-advisory-policy-reproduction.json').read_bytes())
 assert hashlib.sha256(text.encode()).hexdigest()==proof['source_sha256']
 assert text==(R/'tests/fixtures/ai-pre-advisory-policy-20261002.py').read_text(encoding='utf-8')
 before='        return bool(policy["ledger_calls_when_advisory"])\n';after='        return policy["ledger_calls_when_advisory"] is True\n'
 assert text.count(before)==1
 return text.replace(before,after)
