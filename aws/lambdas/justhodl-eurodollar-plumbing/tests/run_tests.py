"""Network-free production macro donor builders and capacity handler regressions."""
import sys
from pathlib import Path
sys.path.insert(0,str(Path(__file__).resolve().parents[3]/"shared/tests"))
from macro_donor_test_support import run
def test_actual_settlement_assignment():
    import ast
    from datetime import datetime, timezone
    from pd_fails_context import project
    source=(Path(__file__).resolve().parents[1]/'source/lambda_function.py').read_text(encoding='utf-8')
    tree=ast.parse(source);handler=next(n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='lambda_handler')
    nodes=[n for n in handler.body if isinstance(n,ast.Assign) and 'payload["pd_settlement_fails"]' in (ast.get_source_segment(source,n) or '')]
    assert len(nodes)==1
    assert not any(isinstance(n,ast.Name) and n.id=='out' for n in ast.walk(handler))
    stamp=datetime.now(timezone.utc);doc={'generated_at':stamp.isoformat(),'treasury':{'scope_id':'treasury_incl_tips','as_of':stamp.date().isoformat(),'ftd_bn':0,'ftr_bn':2,'gross_bn':2,'quality':{'status':'fresh'}},'headline':{'scope':'ust_ex_tips','as_of':stamp.date().isoformat(),'ftd_bn':99,'ftr_bn':99,'combined_bn':198,'quality':{'status':'fresh'}}}
    payload={};scope={'payload':payload,'S3':object(),'BUCKET':'fixture','load_pd_fails':lambda *args:project(doc,stamp)}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'settlement-assignment','exec'),scope)
    assert payload['pd_settlement_fails']['ftd_bn']==0
    assert payload['pd_settlement_fails']['combined_bn']==2
    assert payload['pd_settlement_fails']['ust_ex_tips']['combined_bn']==198

if __name__=="__main__":
    test_actual_settlement_assignment()
    run()
