#!/usr/bin/env python3
"""Static map of how JustHodl Lambdas trigger each other, and the loops in it.

Built after the 2026-10-12 incident (docs/ops/POSTMORTEM_2026-10_EVENT_LOOP.md): justhodl-miss-calibrator
published `near_miss.extreme`, the event coordinator's route for that event invoked justhodl-miss-calibrator,
and every run started three more until the account's Lambda pool was full for 4.4 days ($2,300).

Edges (caller -> callee):
  event   F publishes event E (system_events.publish / publish_many / Bus.publish / put_events DetailType,
          directly or through a one-level wrapper) and the coordinator's ROUTES[E]["invoke"] contains G
  invoke  F calls lambda.invoke(FunctionName=G) with G resolvable to a fleet function
  self    F re-invokes itself (FunctionName=context.function_name / AWS_LAMBDA_FUNCTION_NAME / its own name)
  rule    an EventBridge rule on the system bus (config/eventbridge-bus-rules.json, exported from AWS) sends
          detail-type E straight to G, and F publishes E

A cycle containing an event or rule edge is an EVENT LOOP: always a defect (the 2026-10-12 class).
A cycle of only invoke/self edges is a deliberate chain (self-continuation, dispatcher fan-out) and must be
listed with its stop condition in config/loop-allowlist.json; a new one fails the deploy gate.

The analysis over-approximates on purpose: a name that MAY be published counts as published. A false loop
is resolved by fixing the route or allowlisting a documented invoke chain, never by weakening this file.

CLI:  python3 scripts/event_graph.py            # summary + loops, exit 1 on an unallowed loop
      python3 scripts/event_graph.py --json     # full graph as JSON
"""
from __future__ import annotations

import ast
import json
import re
import sys
from pathlib import Path
from typing import Dict, Iterable, List, Optional, Set, Tuple

REPO = Path(__file__).resolve().parents[1]
COORDINATOR = "justhodl-event-coordinator"
EVENT_NAME_RE = re.compile(r"^[a-z][a-z0-9_]*(\.[a-z0-9_]+)+$")
PUBLISH_FUNCS = {"publish", "publish_many", "publish_event", "emit_event"}
SELF_MARKERS = ("function_name", "AWS_LAMBDA_FUNCTION_NAME")


# --------------------------------------------------------------------------------------------- parsing helpers --
TRIGGER_RE = re.compile(r"\b(publish|publish_many|publish_event|emit_event|put_events|invoke|invoke_async)\s*\(")
IMPORT_RE = re.compile(r"^\s*(?:from\s+([A-Za-z_][\w]*)|import\s+([A-Za-z_][\w]*))", re.M)


def _parse(path: Path, text: Optional[str] = None) -> Optional[ast.AST]:
    import warnings
    try:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            return ast.parse(text if text is not None else path.read_text(encoding="utf-8", errors="replace"),
                             filename=str(path))
    except SyntaxError:
        return None


def _str_constants(tree: ast.AST) -> Dict[str, str]:
    """NAME = "literal" (module or class level, also inside functions) -> {NAME: literal}."""
    out: Dict[str, str] = {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and isinstance(node.value, ast.Constant) and isinstance(node.value.value, str):
            for t in node.targets:
                if isinstance(t, ast.Name):
                    out[t.id] = node.value.value
                elif isinstance(t, ast.Attribute):
                    out[t.attr] = node.value.value
        elif isinstance(node, ast.AnnAssign) and isinstance(node.value, ast.Constant) \
                and isinstance(node.value.value, str) and isinstance(node.target, ast.Name):
            out[node.target.id] = node.value.value
    return out


def _env_default(node: ast.AST) -> Optional[str]:
    """os.environ.get("X", "default") / os.getenv("X", "default") -> "default"."""
    if isinstance(node, ast.Call) and len(node.args) >= 2 and isinstance(node.args[1], ast.Constant) \
            and isinstance(node.args[1].value, str):
        f = node.func
        name = f.attr if isinstance(f, ast.Attribute) else getattr(f, "id", "")
        if name in ("get", "getenv"):
            return node.args[1].value
    return None


def _resolve(node: Optional[ast.AST], consts: Dict[str, str]) -> Optional[str]:
    if node is None:
        return None
    if isinstance(node, ast.Constant) and isinstance(node.value, str):
        return node.value
    if isinstance(node, ast.Name):
        return consts.get(node.id)
    if isinstance(node, ast.Attribute):
        return consts.get(node.attr)
    d = _env_default(node)
    if d is not None:
        return d
    if isinstance(node, ast.BoolOp):  # X or "default"
        for v in node.values:
            r = _resolve(v, consts)
            if r:
                return r
    if isinstance(node, ast.IfExp):
        return _resolve(node.body, consts) or _resolve(node.orelse, consts)
    return None


def _func_name(call: ast.Call) -> str:
    f = call.func
    if isinstance(f, ast.Name):
        return f.id
    if isinstance(f, ast.Attribute):
        return f.attr
    return ""


def _is_self_ref(node: Optional[ast.AST]) -> bool:
    if node is None:
        return False
    src = ast.dump(node)
    return any(m in src for m in SELF_MARKERS)


def _call_arg(call: ast.Call, pos: int, *kw: str) -> Optional[ast.AST]:
    for k in call.keywords:
        if k.arg in kw:
            return k.value
    return call.args[pos] if len(call.args) > pos else None


# ------------------------------------------------------------------------------------------- per-file analysis --
class FileFacts:
    def __init__(self) -> None:
        self.published: Set[str] = set()
        self.dynamic_publish = 0
        self.invokes: Set[str] = set()
        self.self_invoke = False
        self.dynamic_invoke = 0
        self.wrappers: Set[str] = set()      # functions whose event-name parameter flows into publish()
        self.imports: Set[str] = set()


def _tuple_heads(node: ast.AST, consts: Dict[str, str]) -> Iterable[str]:
    """First element of every tuple literal inside node (publish_many([(name, detail), ...]))."""
    for sub in ast.walk(node):
        if isinstance(sub, ast.Tuple) and sub.elts:
            v = _resolve(sub.elts[0], consts)
            if v:
                yield v


class _Scan(ast.NodeVisitor):
    """One pass: imports, every call with the parameters of its enclosing function, all tuple heads."""

    def __init__(self) -> None:
        self.imports: Set[str] = set()
        self.calls: List[Tuple[ast.Call, Set[str], str]] = []
        self.tuples: List[ast.Tuple] = []
        self._fn: List[Tuple[str, Set[str]]] = []

    def visit_Import(self, node):
        self.imports.update(a.name.split(".")[0] for a in node.names)

    def visit_ImportFrom(self, node):
        if node.module:
            self.imports.add(node.module.split(".")[0])

    def _visit_fn(self, node):
        a = node.args
        self._fn.append((node.name, {x.arg for x in a.posonlyargs + a.args + a.kwonlyargs}))
        self.generic_visit(node)
        self._fn.pop()

    visit_FunctionDef = visit_AsyncFunctionDef = _visit_fn

    def visit_Call(self, node):
        name, params = self._fn[-1] if self._fn else ("", set())
        self.calls.append((node, params, name))
        self.generic_visit(node)

    def visit_Tuple(self, node):
        if node.elts:
            self.tuples.append(node)
        self.generic_visit(node)


def analyze_file(tree: ast.AST, consts: Dict[str, str], wrappers: Set[str], fleet: Set[str],
                 own_name: str) -> FileFacts:
    facts = FileFacts()
    scan = _Scan()
    scan.visit(tree)
    facts.imports = scan.imports
    # one level of wrappers: def emit(name, detail): publish(name, detail)
    for call, params, enclosing in scan.calls:
        if enclosing and _func_name(call) in PUBLISH_FUNCS:
            arg = _call_arg(call, 0, "event_name", "name", "detail_type")
            if isinstance(arg, ast.Name) and arg.id in params:
                facts.wrappers.add(enclosing)
    file_heads = None

    pub_funcs = PUBLISH_FUNCS | wrappers | facts.wrappers
    for call, _params, _enclosing in scan.calls:
        name = _func_name(call)
        if name in pub_funcs:
            arg = _call_arg(call, 0, "event_name", "name", "detail_type", "events")
            if name == "publish_many" or (arg is not None and isinstance(arg, (ast.List, ast.Tuple, ast.ListComp))):
                heads = list(_tuple_heads(arg, consts)) if arg is not None else []
                # publish_many(events): events built with .append((NAME, {...})) elsewhere in the file
                if not heads:
                    if file_heads is None:
                        file_heads = [h for h in (_resolve(t.elts[0], consts) for t in scan.tuples)
                                      if h and EVENT_NAME_RE.match(h)]
                    heads = file_heads
                facts.published.update(h for h in heads if EVENT_NAME_RE.match(h))
                if not heads:
                    facts.dynamic_publish += 1
            else:
                v = _resolve(arg, consts)
                if v and EVENT_NAME_RE.match(v):
                    facts.published.add(v)
                elif arg is not None and not (isinstance(arg, ast.Name) and name in facts.wrappers):
                    facts.dynamic_publish += 1
        elif name == "put_events":
            for sub in ast.walk(call):
                if isinstance(sub, ast.Dict):
                    for k, v in zip(sub.keys, sub.values):
                        if isinstance(k, ast.Constant) and k.value == "DetailType":
                            r = _resolve(v, consts)
                            if r and EVENT_NAME_RE.match(r):
                                facts.published.add(r)
                            else:
                                facts.dynamic_publish += 1
        elif name in ("invoke", "invoke_async"):
            target = _call_arg(call, 99, "FunctionName")
            if target is None:
                continue
            if _is_self_ref(target):
                facts.self_invoke = True
                continue
            v = _resolve(target, consts)
            if v:
                v = v.split(":function:")[-1].split(":")[0]
                if v == own_name:
                    facts.self_invoke = True
                elif v in fleet:
                    facts.invokes.add(v)
                continue
            facts.dynamic_invoke += 1
    return facts


# ------------------------------------------------------------------------------------------------ graph build --
def load_routes(repo: Path) -> Dict[str, List[str]]:
    src = repo / "aws" / "lambdas" / COORDINATOR / "source" / "lambda_function.py"
    tree = _parse(src) if src.exists() else None
    if tree is None:
        return {}
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(getattr(t, "id", None) == "ROUTES" for t in node.targets):
            routes = ast.literal_eval(node.value)
            return {k: list((v or {}).get("invoke") or []) for k, v in routes.items()}
    return {}


def load_bus_rules(repo: Path) -> Dict[str, List[str]]:
    """config/eventbridge-bus-rules.json: {"rules": [{"detail_types": [...], "targets": [fn, ...]}]}."""
    p = repo / "config" / "eventbridge-bus-rules.json"
    if not p.exists():
        return {}
    doc = json.loads(p.read_text())
    out: Dict[str, List[str]] = {}
    for rule in doc.get("rules", []):
        for dt in rule.get("detail_types") or []:
            out.setdefault(dt, []).extend(t for t in rule.get("targets", []) if t != COORDINATOR)
    return out


def build_graph(repo: Path = REPO) -> dict:
    lambdas_dir, shared_dir = repo / "aws" / "lambdas", repo / "aws" / "shared"
    fleet = {p.name for p in lambdas_dir.iterdir() if (p / "source").is_dir()} if lambdas_dir.exists() else set()
    shared_trees = {p.stem: _parse(p) for p in shared_dir.glob("*.py")} if shared_dir.exists() else {}
    shared_trees = {k: v for k, v in shared_trees.items() if v is not None}
    shared_consts: Dict[str, str] = {}
    for t in shared_trees.values():
        shared_consts.update(_str_constants(t))
    shared_wrappers: Set[str] = set()
    shared_facts: Dict[str, FileFacts] = {}
    for name, t in shared_trees.items():
        shared_facts[name] = analyze_file(t, shared_consts, set(), fleet, "")
        shared_wrappers |= shared_facts[name].wrappers
    # system_events defines the publish helpers and the EVT_* names; it publishes nothing by itself
    for helper in ("system_events",):
        if helper in shared_facts:
            shared_facts[helper].published.clear()
            shared_facts[helper].dynamic_publish = 0

    def shared_closure(mods: Set[str]) -> Set[str]:
        seen, todo = set(), [m for m in mods if m in shared_facts]
        while todo:
            m = todo.pop()
            if m in seen:
                continue
            seen.add(m)
            todo += [x for x in shared_facts[m].imports if x in shared_facts and x not in seen]
        return seen

    routes = load_routes(repo)
    rules = load_bus_rules(repo)
    nodes: Dict[str, dict] = {}
    for fn in sorted(fleet):
        if fn == COORDINATOR:
            continue
        texts = {p: p.read_text(encoding="utf-8", errors="replace")
                 for p in sorted((lambdas_dir / fn / "source").glob("*.py"))}
        imports = {a or b for txt in texts.values() for a, b in IMPORT_RE.findall(txt)}
        # Only engines whose own code can publish or invoke need a full parse (fast path for ~900 others);
        # everything they inherit from shared modules is already analysed above.
        if any(TRIGGER_RE.search(txt) for txt in texts.values()):
            trees = [t for t in (_parse(p, txt) for p, txt in texts.items()) if t is not None]
        else:
            trees = []
        consts = dict(shared_consts)
        for t in trees:
            consts.update(_str_constants(t))
        local = [analyze_file(t, consts, shared_wrappers, fleet, fn) for t in trees] or [FileFacts()]
        used_shared = shared_closure(imports)
        published = set().union(*(f.published for f in local), *(shared_facts[m].published for m in used_shared)) \
            if local else set()
        nodes[fn] = {
            "published": sorted(published),
            "invokes": sorted(set().union(*(f.invokes for f in local)) if local else set()),
            "self_invoke": any(f.self_invoke for f in local),
            "dynamic_publish": sum(f.dynamic_publish for f in local),
            "dynamic_invoke": sum(f.dynamic_invoke for f in local),
            "shared": sorted(used_shared),
        }
    edges: List[Tuple[str, str, str, str]] = []  # (src, dst, kind, label)
    for fn, n in nodes.items():
        for ev in n["published"]:
            for tgt in routes.get(ev, []):
                edges.append((fn, tgt, "event", ev))
            for tgt in rules.get(ev, []):
                edges.append((fn, tgt, "rule", ev))
        for tgt in n["invokes"]:
            edges.append((fn, tgt, "invoke", ""))
        if n["self_invoke"]:
            edges.append((fn, fn, "self", ""))
    return {"routes": routes, "bus_rules": rules, "nodes": nodes, "edges": edges,
            "routed_events_published": sorted({e for n in nodes.values() for e in n["published"] if e in routes})}


# ----------------------------------------------------------------------------------------------- cycle finding --
def find_cycles(edges: List[Tuple[str, str, str, str]]) -> List[dict]:
    """Strongly connected components with a cycle, each with one concrete path and its edge kinds."""
    adj: Dict[str, List[Tuple[str, str, str]]] = {}
    for s, d, k, lbl in edges:
        adj.setdefault(s, []).append((d, k, lbl))
        adj.setdefault(d, [])
    index, low, stack, on, comps, counter = {}, {}, [], set(), [], [0]

    def strong(v: str) -> None:  # iterative Tarjan
        work = [(v, 0)]
        while work:
            node, i = work.pop()
            if i == 0:
                index[node] = low[node] = counter[0]
                counter[0] += 1
                stack.append(node)
                on.add(node)
            succ = adj[node]
            if i < len(succ):
                work.append((node, i + 1))
                w = succ[i][0]
                if w not in index:
                    work.append((w, 0))
                elif w in on:
                    low[node] = min(low[node], index[w])
                continue
            if low[node] == index[node]:
                comp = []
                while True:
                    w = stack.pop()
                    on.discard(w)
                    comp.append(w)
                    if w == node:
                        break
                comps.append(comp)
            if work:
                parent = work[-1][0]
                low[parent] = min(low[parent], low[node])

    for v in list(adj):
        if v not in index:
            strong(v)
    cycles = []
    for comp in comps:
        members = set(comp)
        inner = [(s, d, k, lbl) for s, d, k, lbl in edges if s in members and d in members]
        if len(comp) == 1 and not any(s == d for s, d, _, _ in inner):
            continue
        kinds = sorted({k for _, _, k, _ in inner})
        cycles.append({"nodes": sorted(members), "kinds": kinds,
                       "event_loop": any(k in ("event", "rule") for k in kinds),
                       "path": _one_path(sorted(members)[0], inner),
                       "edges": sorted({"%s -[%s%s]-> %s" % (s, k, (":" + lbl) if lbl else "", d)
                                        for s, d, k, lbl in inner})})
    return cycles


def _one_path(start: str, inner: List[Tuple[str, str, str, str]]) -> List[str]:
    adj: Dict[str, List[Tuple[str, str, str]]] = {}
    for s, d, k, lbl in inner:
        adj.setdefault(s, []).append((d, k, lbl))
    path, seen = [start], {start}
    node = start
    for _ in range(len(inner) + 1):
        nxt = adj.get(node, [])
        back = [x for x in nxt if x[0] == start]
        if back:
            d, k, lbl = back[0]
            path.append("-[%s%s]-> %s" % (k, (":" + lbl) if lbl else "", d))
            return path
        fresh = [x for x in nxt if x[0] not in seen]
        if not fresh:
            break
        d, k, lbl = fresh[0]
        path.append("-[%s%s]-> %s" % (k, (":" + lbl) if lbl else "", d))
        seen.add(d)
        node = d
    return path


def load_allowlist(repo: Path = REPO) -> List[dict]:
    p = repo / "config" / "loop-allowlist.json"
    return json.loads(p.read_text()).get("invoke_cycles", []) if p.exists() else []


def verdict(repo: Path = REPO) -> dict:
    g = build_graph(repo)
    cycles = find_cycles(g["edges"])
    allowed = {tuple(sorted(a["nodes"])) for a in load_allowlist(repo)}
    event_loops = [c for c in cycles if c["event_loop"]]
    unallowed = [c for c in cycles if not c["event_loop"] and tuple(c["nodes"]) not in allowed]
    stale = sorted(allowed - {tuple(c["nodes"]) for c in cycles})
    return {"graph": g, "cycles": cycles, "event_loops": event_loops, "unallowed_invoke_cycles": unallowed,
            "stale_allowlist": [list(s) for s in stale]}


def main(argv: List[str]) -> int:
    v = verdict(REPO)
    g = v["graph"]
    if "--json" in argv:
        print(json.dumps({k: v[k] for k in v if k != "graph"} | {"nodes": g["nodes"], "edges": g["edges"]},
                         indent=1, default=list))
    else:
        n_pub = sum(1 for n in g["nodes"].values() if n["published"])
        print("functions %d | publishers %d | routes %d (%d invoke) | edges %d | cycles %d" % (
            len(g["nodes"]), n_pub, len(g["routes"]), sum(1 for t in g["routes"].values() if t), len(g["edges"]),
            len(v["cycles"])))
        for c in v["cycles"]:
            tag = "EVENT LOOP" if c["event_loop"] else (
                "invoke chain (allowlisted)" if c not in v["unallowed_invoke_cycles"] else "UNALLOWED invoke chain")
            print("  %s: %s" % (tag, " ".join(c["path"])))
        if v["stale_allowlist"]:
            print("  stale allowlist entries (no longer a cycle): %s" % v["stale_allowlist"])
    return 1 if (v["event_loops"] or v["unallowed_invoke_cycles"]) else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
