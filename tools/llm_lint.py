#!/usr/bin/env python3
"""tools/llm_lint.py — SPEC C5/C-lint (ops 4435).

Repo-wide LLM hygiene linter (the CI check Perplexity asked where to put —
.github/ is denylisted, so it lives here and runs from ops/local/pre-push):

  trailing_dot_model   claude-haiku-4-5.   (the known typo class)
  bad_model_id         model ids failing the canonical regex
  direct_sdk_bypass    api.anthropic.com called outside aws/shared/llm_router
  hardcoded_model      model ids inline in engine code instead of env/router

Exit 1 on findings (CI-able). --json for machine output.
"""
import json, os, re, sys

MODEL_OK = re.compile(r'^(claude-(?:opus|sonnet|haiku|fable)-[\d]+(?:-[\d]+)?'
                      r'(?:-\d{8})?|glm-[\w.\-]+|sonar[\w\-]*|gpt-[\w.\-]+)$')
MODEL_LIT = re.compile(r'["\'](claude-[\w.\-]+|glm-[\w.\-]+|gpt-[\w.\-]+)["\']')
DIRECT = re.compile(r'api\.anthropic\.com')

def lint(root="."):
    finds = []
    for dirpath, _, files in os.walk(root):
        if any(s in dirpath for s in (".git", "node_modules", "ops/history",
                                      "ops/reports", "ran")):
            continue
        for fn in files:
            if not fn.endswith(".py"):
                continue
            path = os.path.join(dirpath, fn)
            rel = os.path.relpath(path, root)
            try:
                src = open(path, encoding="utf-8", errors="replace").read()
            except Exception:
                continue
            in_router = rel.replace("\\", "/").endswith(
                ("aws/shared/llm_router.py", "aws/shared/llm_cost.py",
                 "aws/shared/claude_compat.py", "aws/shared/anthropic_shim.py",
                 "tools/llm_lint.py"))
            for i, line in enumerate(src.split("\n"), 1):
                for m in MODEL_LIT.finditer(line):
                    mid = m.group(1)
                    if mid.endswith("."):
                        finds.append({"kind": "trailing_dot_model",
                                      "path": rel, "line": i, "id": mid})
                    elif not MODEL_OK.match(mid):
                        finds.append({"kind": "bad_model_id", "path": rel,
                                      "line": i, "id": mid})
                    elif "aws/lambdas/" in rel.replace("\\", "/") \
                            and "config" not in rel:
                        finds.append({"kind": "hardcoded_model", "path": rel,
                                      "line": i, "id": mid})
                if DIRECT.search(line) and not in_router:
                    finds.append({"kind": "direct_sdk_bypass", "path": rel,
                                  "line": i})
    return finds

if __name__ == "__main__":
    f = lint(".")
    by = {}
    for x in f:
        by[x["kind"]] = by.get(x["kind"], 0) + 1
    if "--json" in sys.argv:
        print(json.dumps({"n": len(f), "by_kind": by, "findings": f[:300]},
                         indent=1))
    else:
        print(f"llm_lint: {len(f)} findings {by}")
        for x in f[:40]:
            print(f"  {x['kind']:20s} {x['path']}:{x['line']} "
                  f"{x.get('id','')}")
    sys.exit(1 if f else 0)
