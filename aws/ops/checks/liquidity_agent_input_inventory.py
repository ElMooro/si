"""Inventory the predecessor catalog AND literal requests without executing it."""
import ast, hashlib, re

FRED = {'fetch_fred', '_fred_live', 'get_latest', 'get_series_history'}
FORWARDING = {('fetch_fred','_fred_live','series_id'), ('get_latest','fetch_fred','series_id'),
              ('get_series_history','fetch_fred','series_id'), ('get_series_history','_fred_live','series_id'),
              ('lambda_handler','get_series_history','sid')}


def inventory(raw):
    tree=ast.parse(raw.decode('utf-8'));constants={}
    for node in tree.body:
        if isinstance(node,ast.Assign) and len(node.targets)==1 and isinstance(node.targets[0],ast.Name):
            try:constants[node.targets[0].id]=ast.literal_eval(node.value)
            except (ValueError,TypeError):pass
    catalog=constants.get('FRED_SERIES')
    if not isinstance(catalog,list) or not catalog or any(not isinstance(row,tuple) or len(row) not in (4,5) for row in catalog):
        raise ValueError('Whole declared predecessor catalog required')
    series=[row[0] for row in catalog]
    if len(series)!=len(set(series)) or any(not isinstance(s,str) or not re.fullmatch('[A-Z0-9]+',s) for s in series):
        raise ValueError('Unique explicit original series required')
    calls=[];forwarded=[];unresolved=[];packets=set()
    class Visitor(ast.NodeVisitor):
        function='module'
        def visit_FunctionDef(self,node):
            old=self.function;self.function=node.name;self.generic_visit(node);self.function=old
        def visit_Call(self,node):
            name=node.func.id if isinstance(node.func,ast.Name) else None
            if name in FRED and node.args:
                arg=node.args[0];coord={'function':self.function,'reader':name,'line':node.lineno}
                if isinstance(arg,ast.Constant) and isinstance(arg.value,str) and re.fullmatch('[A-Z0-9]+',arg.value):
                    calls.append({**coord,'series_id':arg.value})
                elif isinstance(arg,ast.Name) and (self.function,name,arg.id) in FORWARDING:
                    forwarded.append({**coord,'argument':arg.id,'basis':'reviewed predecessor forwarding or catalog loop'})
                else:unresolved.append({**coord,'argument_expression':ast.unparse(arg)})
            if name=='_sfeed' and node.args:
                arg=node.args[0];value=arg.value if isinstance(arg,ast.Constant) else constants.get(arg.id) if isinstance(arg,ast.Name) else None
                if isinstance(value,str):packets.add(value)
                else:unresolved.append({'function':self.function,'reader':name,'line':node.lineno,'argument_expression':ast.unparse(arg)})
            if isinstance(node.func,ast.Attribute) and node.func.attr=='get_object':
                for kw in node.keywords:
                    if kw.arg!='Key':continue
                    if isinstance(kw.value,ast.Constant) and isinstance(kw.value.value,str):packets.add(kw.value.value)
                    elif not (self.function=='_sfeed' and isinstance(kw.value,ast.Name) and kw.value.id=='key'):
                        unresolved.append({'function':self.function,'reader':'get_object','line':node.lineno,'argument_expression':ast.unparse(kw.value)})
            self.generic_visit(node)
    Visitor().visit(tree)
    extra=sorted({call['series_id'] for call in calls}-set(series))
    return {'contract':'liquidity-agent-input-inventory.v1','source_sha256':hashlib.sha256(raw).hexdigest(),
        'catalog_rows':[list(row) for row in catalog],'catalog_series':series,'additional_literal_series':extra,
        'all_series':series+extra,'literal_requests':calls,'reviewed_forwarding':forwarded,
        's3_inputs':sorted(packets),'unresolved_requests':unresolved,
        'legacy_labels_units_and_categories_qualified':False,'provider_requests':0,'producer_invocations':0}
