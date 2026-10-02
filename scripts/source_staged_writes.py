"""Candidates for returned write bundles; never certify their reachability.

A literal Key must travel through an append-only local list, an explicit return
field, a direct no-argument local builder call and a matching **loop-item write.
Unknown mutations, rebinding, imports and arbitrary returns remain unresolved.
"""
import ast


def local_nodes(node):
    for child in ast.iter_child_nodes(node):
        yield child
        if not isinstance(child,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef,ast.Lambda)):
            yield from local_nodes(child)


def assignments(nodes,name):
    return [node for node in nodes if isinstance(node,ast.Name) and isinstance(node.ctx,(ast.Store,ast.Del)) and node.id==name]


def binding_names(node):
    if isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef,ast.ClassDef)):
        return {node.name}
    if isinstance(node,(ast.Import,ast.ImportFrom)):
        return {name.asname or name.name.split('.')[0] for name in node.names}
    if isinstance(node,(ast.Assign,ast.AnnAssign)):
        targets=node.targets if isinstance(node,ast.Assign) else [node.target]
        return {item.id for target in targets for item in ast.walk(target) if isinstance(item,ast.Name)}
    return set()


def candidates(scan):
    functions={n.name:n for n in scan.tree.body if isinstance(n,ast.FunctionDef) and not n.decorator_list}
    module_bindings={}
    for node in scan.tree.body:
        for name in binding_names(node):module_bindings[name]=node
    result=[]
    for consumer in functions.values():
        nodes=list(local_nodes(consumer))
        for loop in nodes:
            if not isinstance(loop,ast.For) or not isinstance(loop.target,ast.Name):continue
            it=loop.iter
            if not (isinstance(it,ast.Subscript) and isinstance(it.value,ast.Name) and isinstance(it.slice,ast.Constant) and isinstance(it.slice.value,str)):continue
            if len(assignments(nodes,it.value.id))!=1:continue
            assignments_to=[n for n in consumer.body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name) and n.targets[0].id==it.value.id]
            if len(assignments_to)!=1 or assignments_to[0].lineno>=loop.lineno:continue
            call=assignments_to[0].value
            if not isinstance(call,ast.Call) or call.args or call.keywords or not isinstance(call.func,ast.Name):continue
            producer=functions.get(call.func.id)
            if producer is None or any((producer.args.posonlyargs,producer.args.args,producer.args.kwonlyargs,producer.args.vararg,producer.args.kwarg)):continue
            if assignments(nodes,call.func.id) or any(call.func.id in binding_names(n) for n in nodes):continue
            if module_bindings.get(call.func.id) is not producer:continue
            module_shadows=[n for n in scan.tree.body if isinstance(n,(ast.Assign,ast.AnnAssign,ast.Import,ast.ImportFrom)) and any(isinstance(x,ast.Name) and isinstance(x.ctx,ast.Store) and x.id==call.func.id for x in ast.walk(n))]
            if module_shadows:continue
            returns=[n for n in producer.body if isinstance(n,ast.Return)]
            if len(returns)!=1 or not isinstance(returns[0].value,ast.Dict):continue
            fields=[v for k,v in zip(returns[0].value.keys,returns[0].value.values) if isinstance(k,ast.Constant) and k.value==it.slice.value]
            if len(fields)!=1 or not isinstance(fields[0],ast.Name):continue
            name=fields[0].id;pnodes=list(local_nodes(producer))
            initializers=[n for n in producer.body if isinstance(n,ast.Assign) and len(n.targets)==1 and isinstance(n.targets[0],ast.Name) and n.targets[0].id==name and isinstance(n.value,ast.List) and not n.value.elts]
            if len(initializers)!=1 or len(assignments(pnodes,name))!=1:continue
            # Only append calls and this explicit return may reference the list.
            allowed={id(fields[0])};appends=[]
            for n in pnodes:
                if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and isinstance(n.func.value,ast.Name) and n.func.value.id==name and n.func.attr=='append' and len(n.args)==1 and not n.keywords:
                    if n.lineno<initializers[0].lineno or n.lineno>returns[0].lineno:continue
                    appends.append(n);allowed.add(id(n.func.value))
            if any(isinstance(n,ast.Name) and isinstance(n.ctx,ast.Load) and n.id==name and id(n) not in allowed for n in pnodes):continue
            loopnodes=list(local_nodes(loop))
            if assignments(loopnodes,loop.target.id)!=[loop.target]:continue
            writes=[n for n in loopnodes if isinstance(n,ast.Call) and isinstance(n.func,ast.Attribute) and n.func.attr=='put_object' and not n.args and len(n.keywords)==1 and n.keywords[0].arg is None and isinstance(n.keywords[0].value,ast.Name) and n.keywords[0].value.id==loop.target.id]
            if not writes:continue
            parents={child:parent for parent in ast.walk(producer) for child in ast.iter_child_nodes(parent)}
            for append in appends:
                value=append.args[0];key=None
                if isinstance(value,ast.Call) and isinstance(value.func,ast.Name) and value.func.id=='dict' and not value.args and all(kw.arg is not None for kw in value.keywords):
                    if 'dict' in module_bindings or any('dict' in binding_names(n) for n in pnodes):continue
                    keys=[kw.value for kw in value.keywords if kw.arg=='Key'];key=keys[0] if len(keys)==1 else None
                elif isinstance(value,ast.Dict) and all(k is not None for k in value.keys):
                    keys=[v for k,v in zip(value.keys,value.values) if isinstance(k,ast.Constant) and k.value=='Key'];key=keys[0] if len(keys)==1 else None
                if key is None:continue
                envs=[dict(scan.globals)];parent=parents.get(append)
                while parent is not None and parent is not producer:
                    if isinstance(parent,ast.For) and isinstance(parent.target,(ast.Tuple,ast.List)) and isinstance(parent.iter,(ast.Tuple,ast.List)):
                        names=parent.target.elts;rows=parent.iter.elts
                        if all(isinstance(n,ast.Name) and assignments(list(local_nodes(parent)),n.id)==[n] for n in names) and all(isinstance(row,(ast.Tuple,ast.List)) and len(row.elts)==len(names) for row in rows):
                            envs=[{**env,**{n.id:scan.resolve(v,env) for n,v in zip(names,row.elts)}} for env in envs for row in rows]
                            if len(envs)>256:envs=[];break
                    parent=parents.get(parent)
                for env in envs:
                    resolved=scan.resolve(key,env)
                    if not isinstance(resolved,str) or not resolved.endswith('.json') or '*' in resolved:continue
                    for write in writes:result.append({'key':resolved,'line':write.lineno,'builder_line':append.lineno,'builder_function':producer.name,'returned_field':it.slice.value,'candidate_basis':'returned_literal_write_bundle','runtime_verified':False})
    return result
