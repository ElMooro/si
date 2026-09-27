"""Preserve the frozen collectors and the explicitly reviewed schedule-only repair."""
from pathlib import Path
import ast
import hashlib

ROOT = Path(__file__).resolve().parents[1]


def reviewed_collector_digest(name):
    raw = (ROOT / 'aws/ops/checks' / (name + '.py')).read_bytes()
    digest = hashlib.sha256(raw).hexdigest()
    if name != 'market_runtime_evidence':
        return digest
    # 7c4f12c071464f5beb57cb0d3b04b1316049529e fixed schedule discovery.
    # Never permit another implementation to inherit this one reviewed exception.
    if digest != 'b5221bc3351790e87e34775058883369da31e9184ea346be5b3b4653dd2cf0ca':
        raise AssertionError('Runtime verifier differs from reviewed schedule repair')
    original = (ROOT / 'tests/fixtures/pre-complete-schedule-runtime.py.txt').read_bytes()
    predecessor = hashlib.sha256(original).hexdigest()
    if predecessor != 'ec1064992267e4dfef5375a4b3481fa54d7192515ee931683b55c74489e2918e':
        raise AssertionError('Whole original runtime verifier must remain preserved')
    def unrelated(body):
        tree = ast.parse(body)
        tree.body = [node for node in tree.body if not (isinstance(node, ast.FunctionDef) and node.name == 'schedule_evidence')]
        return ast.dump(tree, include_attributes=False)
    if unrelated(original) != unrelated(raw):
        raise AssertionError('Only schedule discovery was reviewed; all other verification must match')
    return predecessor
