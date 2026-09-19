"""Execute the actual overlay consumer against synthetic qualification failures."""
import ast
from pathlib import Path
source=Path(__file__).resolve().parents[1]/'source/lambda_function.py'
tree=ast.parse(source.read_text(encoding='utf-8'))
nodes=[n for n in tree.body if isinstance(n,ast.FunctionDef) and n.name=='build_overlay']
assert len(nodes)==1
def dig(obj,path):
    for part in path.split('.'):
        obj=obj.get(part) if isinstance(obj,dict) else None
    return obj
for carry in ({'unwind_overlay':{'cohort_fragility':99}},
              {'calls_eligible':False,'quality':{'status':'fresh'},'unwind_overlay':{'cohort_fragility':99}},
              {'calls_eligible':True,'quality':{'status':'stale'},'decision_qualification':{'status':'qualified'},'unwind_overlay':{'cohort_fragility':99}},
              {'calls_eligible':True,'quality':{'status':'fresh'},'decision_qualification':{'status':'research_only'},'unwind_overlay':{'cohort_fragility':99}}):
    scope={'OVERLAY':[('data/carry-surface.json','unwind_overlay.cohort_fragility','Carry','id'),('other.json','score','Other','id')],
      '_read_s3_json':lambda k:carry if k.startswith('data/carry') else {'score':25},'_dig':dig,
      '_mean':lambda values:sum(values)/len(values),'build_plumbing_signals':lambda:[]}
    exec(compile(ast.Module(body=nodes,type_ignores=[]),'actual-stress-overlay','exec'),scope)
    rows,score,count=scope['build_overlay']()
    assert rows[0]['status']=='unqualified_carry' and rows[0]['stress'] is None
    assert score==25 and count==1
print('Stress-index carry consumer boundary: 4 qualification cases passed')
