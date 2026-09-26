"""Conservative AST-only ownership for reachable checked-in shared writers.

This never imports application modules or evaluates their Python. Argument
values use the existing string/key lattice. Uncalled sibling writers, dynamic
dispatch, decorators and return values are not guessed into ownership.
"""
import ast
from pathlib import Path


class Callable:
    def __init__(self, scanner, node):
        self.scanner, self.node = scanner, node


class SharedWriteGraph:
    MAX_CALLS = 2000
    MAX_DEPTH = 24

    def __init__(self, root, source, environment, scan_type, import_constants):
        self.root, self.source = Path(root).resolve(), Path(source).resolve()
        self.roots = [self.source, self.root/'aws/shared']
        self.environment, self.scan_type, self.import_constants = environment, scan_type, import_constants
        self.modules, self.loading, self.calls = {}, set(), 0
        self.constant_cache = {}
        self.proofs, self.reads, self.unresolved = {}, set(), []
        self.trail, self.seen = [], set()

    def relative(self, path):
        return Path(path).resolve().relative_to(self.root).as_posix()

    def locate(self, source, module, level=0):
        parts = module.split('.') if module else []
        bases = [source.parents[level-1]] if level else [source.parent, *self.roots]
        for base in bases:
            for candidate in (base.joinpath(*parts).with_suffix('.py'), base.joinpath(*parts)/'__init__.py'):
                path = candidate.resolve()
                if path.is_file() and any(path.is_relative_to(root) for root in self.roots):return path
        return None

    def imports(self, source, node):
        result = {}
        if isinstance(node, ast.ImportFrom):
            path = self.locate(source, node.module, node.level)
            values = self.load(path)[1] if path else {}
            for name in node.names:
                if name.name != '*' and name.name in values:result[name.asname or name.name] = values[name.name]
        else:
            for name in node.names:
                if '.' in name.name and not name.asname:continue
                path = self.locate(source, name.name)
                if path:result[name.asname or name.name] = self.load(path)[1]
        return result

    def load(self, path):
        if path in self.modules:return self.modules[path]
        if path in self.loading:return None, {}
        self.loading.add(path)
        try:
            text = path.read_text(encoding='utf-8')
            constants = self.import_constants(path,self.roots,self.environment,cache=self.constant_cache)
            scan = self.scan_type(text,self.environment,constants,self,path)
            # Store the module before following imports, to bound cycles.
            exports = dict(scan.globals)
            for name, node in scan.functions.items():exports[name] = Callable(scan,node)
            self.modules[path] = scan, exports
            for node in scan.tree.body:
                if isinstance(node,(ast.Import,ast.ImportFrom)):scan.globals.update(self.imports(path,node))
                elif isinstance(node,(ast.FunctionDef,ast.AsyncFunctionDef)):scan.globals[node.name]=Callable(scan,node)
                elif isinstance(node,(ast.Assign,ast.AnnAssign)):scan.assign(node,scan.globals)
            # Later assignments can shadow an imported callable. Local callable
            # definitions/aliases retain the scanner's established binding.
            exports.update(scan.globals)
            return scan,exports
        finally:self.loading.discard(path)

    def issue(self, scanner, node, reason):
        self.unresolved.append({'file':self.relative(scanner.source_path),'line':getattr(node,'lineno',0),
                                'operation':'delegated_call','reason':reason})

    def call(self, caller, node, env, stack):
        value = caller.resolve(node.func,env)
        if not isinstance(value,Callable):return False
        target,fn = value.scanner,value.node
        identity = self.relative(target.source_path)+':'+fn.name
        if fn.decorator_list:
            self.issue(caller,node,'decorated shared callable requires explicit analysis: '+identity);return True
        if identity in stack or len(stack)>=self.MAX_DEPTH or self.calls>=self.MAX_CALLS:
            self.issue(caller,node,'shared call graph recursion or analysis bound: '+identity);return True
        args=list(fn.args.posonlyargs)+list(fn.args.args)
        bound=dict(target.globals)
        for arg in args+list(fn.args.kwonlyargs):bound[arg.arg]=None
        for arg,default in zip(args[-len(fn.args.defaults):],fn.args.defaults) if fn.args.defaults else []:
            bound[arg.arg]=target.resolve(default,target.globals)
        for arg,default in zip(fn.args.kwonlyargs,fn.args.kw_defaults):
            if default is not None:bound[arg.arg]=target.resolve(default,target.globals)
        if any(isinstance(passed,ast.Starred) for passed in node.args):
            for arg in args:bound[arg.arg]=None
        else:
            for arg,passed in zip(args,node.args):bound[arg.arg]=caller.resolve(passed,env)
        for kw in node.keywords:
            if kw.arg:bound[kw.arg]=caller.resolve(kw.value,env)
            else:
                mapping=caller.resolve(kw.value,env)
                if isinstance(mapping,dict):bound.update(mapping)
                else:
                    for arg in args+list(fn.args.kwonlyargs):bound[arg.arg]=None
        def fingerprint(value,depth=0):
            if isinstance(value,str) or value is None:return value
            if isinstance(value,Callable):return (self.relative(value.scanner.source_path),value.node.lineno)
            if isinstance(value,dict) and depth<3:return tuple((key,fingerprint(val,depth+1)) for key,val in sorted(value.items()))
            return None
        signature=(identity,fn.lineno,tuple((arg.arg,fingerprint(bound.get(arg.arg))) for arg in args+list(fn.args.kwonlyargs)))
        if signature in self.seen:return True
        self.seen.add(signature)
        self.calls += 1
        edge={'file':self.relative(caller.source_path),'line':getattr(node,'lineno',0),'function':identity}
        self.trail.append(edge)
        # Isolate each invocation's evidence: a concrete call must not erase a
        # separate dynamic call to the same shared write site.
        previous=(target.writes,target.reads,target.proofs,target.unresolved,target.other_writes)
        target.writes,target.reads,target.proofs,target.unresolved,target.other_writes=set(),set(),{},[],[]
        try:
            target.block(fn.body,bound,stack+(identity,))
            if target.source_path.is_relative_to(self.root/'aws/shared'):
                for key,lines in target.proofs.items():
                    for line in sorted(lines):self.proofs.setdefault(key,[]).append({
                        'file':target.source_path.name,'repository_path':self.relative(target.source_path),
                        'line':line,'via':list(self.trail),'basis':'reachable_shared_write_argument'})
                self.reads.update(target.reads)
                self.unresolved.extend(dict(row,file=self.relative(target.source_path)) for row in target.unresolved)
        finally:
            target.writes,target.reads,target.proofs,target.unresolved,target.other_writes=previous
            self.trail.pop()
        return True

    def analyze(self, entry, function):
        scan,exports=self.load(Path(entry).resolve())
        target=exports.get(function)
        if isinstance(target,Callable):
            # The synthetic external invocation binds no invented event values.
            node=ast.Call(func=ast.Name(id=function,ctx=ast.Load()),args=[],keywords=[])
            node.lineno=target.node.lineno
            self.call(scan,node,{function:target},())
        return {'keys':set(self.proofs),'reads':self.reads,'proofs':self.proofs,'unresolved':self.unresolved}
