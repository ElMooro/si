"""
Comprehensive Lambda static audit.

Categorizes each of the 110 Lambdas in aws/lambdas/ into:
  P0 — broken/corrupted: syntax errors, empty handlers, wrong handler name
  P1 — high-risk: hardcoded secrets, dead endpoints, deprecated runtimes
  P2 — minor: missing error handling, no logging, large files
  OK — looks fine

Cannot test runtime; static-only. The point is to find KNOWN-BAD code
that ships every push, not to second-guess working code.
"""
from __future__ import annotations
import ast, json, os, re, sys
from pathlib import Path
from collections import defaultdict

LAMBDAS = Path("aws/lambdas")
results = []  # list of dicts


# ------------------------------------------------------------------
# Static checks
# ------------------------------------------------------------------

def load_config(d: Path) -> dict:
    cfg = d / "config.json"
    if not cfg.exists():
        return {}
    try:
        return json.loads(cfg.read_text())
    except Exception:
        return {"_invalid_config": True}


def find_source_file(d: Path, handler: str) -> Path | None:
    """Given handler 'index.handler' or 'lambda_function.lambda_handler',
    return the source file we expect."""
    src_dir = d / "source"
    if not src_dir.is_dir():
        return None
    if "." not in handler:
        return None
    module, _ = handler.rsplit(".", 1)
    # try .py and .js
    for ext in (".py", ".mjs", ".js"):
        p = src_dir / f"{module}{ext}"
        if p.exists():
            return p
    return None


SECRET_PATTERNS = [
    # API key shapes — only flag if NOT pulled from env
    (r'(?i)(api[_-]?key|apikey)\s*=\s*["\'][a-zA-Z0-9_\-]{20,}["\']', "hardcoded API key"),
    (r'(?i)(secret|token|password)\s*=\s*["\'][a-zA-Z0-9_\-]{20,}["\']', "hardcoded secret/token"),
    # AWS keys
    (r'AKIA[0-9A-Z]{16}', "AWS access key id (long-lived!)"),
    # GitHub PATs
    (r'(?:ghp_|github_pat_)[a-zA-Z0-9_]{30,}', "GitHub PAT in source"),
]

# Endpoints known to block AWS us-east-1 IPs or to be retired
BLOCKED_ENDPOINTS = {
    "api.binance.com": "Binance Spot — HTTP 451 from AWS us-east-1 (verified 2026-04)",
    "fapi.binance.com": "Binance Futures — HTTP 451 from AWS us-east-1",
    "api1.binance.com": "Binance mirror — HTTP 451",
    "api2.binance.com": "Binance mirror — HTTP 451",
    "api3.binance.com": "Binance mirror — HTTP 451",
    "data-api.binance.vision": "Binance vision mirror — HTTP 451",
}

DEPRECATED_RUNTIMES = {
    "python3.6", "python3.7", "python3.8",
    "nodejs12.x", "nodejs14.x", "nodejs16.x",
    "go1.x",
}

# Modules that are NOT in the AWS Lambda default Python runtime layer.
# (boto3, urllib, json, datetime, os, re, etc are always available.
#  requests, pandas, numpy, etc are NOT — they need a Layer or to be in the zip)
EXTRA_MODULES = {
    "requests", "pandas", "numpy", "scipy", "sklearn", "matplotlib",
    "tabulate", "yaml", "lxml", "bs4", "feedparser", "httpx",
    "anthropic", "openai", "ujson", "orjson", "redis", "psycopg2",
    "pymongo", "stripe", "tweepy", "selenium", "playwright",
}


def py_imports(tree: ast.AST) -> set[str]:
    """Top-level package names imported."""
    out = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for n in node.names:
                out.add(n.name.split(".")[0])
        elif isinstance(node, ast.ImportFrom) and node.module:
            out.add(node.module.split(".")[0])
    return out


def find_handler_func(tree: ast.AST, handler_name: str) -> ast.FunctionDef | None:
    for node in tree.body:
        if isinstance(node, ast.FunctionDef) and node.name == handler_name:
            return node
        if isinstance(node, ast.AsyncFunctionDef) and node.name == handler_name:
            return node
    return None


def audit_python(src_path: Path, handler_func: str) -> list[tuple[str, str]]:
    """Return list of (severity, message) for findings on a python source."""
    findings = []
    text = src_path.read_text(errors="ignore")
    size = len(text)

    # 1. size sanity
    if size < 100:
        findings.append(("P0", f"source extremely small ({size}B) — likely corrupted/empty"))
        if size < 50:
            return findings  # don't bother parsing

    # 2. syntax
    try:
        tree = ast.parse(text)
    except SyntaxError as e:
        findings.append(("P0", f"SyntaxError line {e.lineno}: {e.msg}"))
        return findings

    # 3. handler exists
    if not find_handler_func(tree, handler_func):
        findings.append(("P0", f"handler function '{handler_func}' not defined in module"))

    # 4. imports — flag deps not in default runtime if the Lambda has no layers
    imports = py_imports(tree)
    extras_used = imports & EXTRA_MODULES
    # We'll cross-reference with config.layers below; for now record
    if extras_used:
        findings.append(("INFO", f"imports non-default modules: {sorted(extras_used)}"))

    # 5. blocked endpoints
    for ep, why in BLOCKED_ENDPOINTS.items():
        if ep in text:
            # Skip mention in comments only
            line_with = next((l for l in text.splitlines() if ep in l), "")
            if line_with.strip().startswith("#") or line_with.strip().startswith("'''"):
                continue
            findings.append(("P1", f"references blocked endpoint {ep}: {why}"))

    # 6. hardcoded secrets — skip example/demo placeholders
    for pat, label in SECRET_PATTERNS:
        for m in re.finditer(pat, text):
            snippet = m.group(0)
            # Skip if it looks like a placeholder
            if any(x in snippet.lower() for x in ("xxx", "example", "your_", "<", "fake", "demo", "todo")):
                continue
            # Skip lines that pull from os.environ, ssm, secrets manager
            line_start = text.rfind("\n", 0, m.start()) + 1
            line_end = text.find("\n", m.end())
            line = text[line_start:line_end].lower()
            if any(safe in line for safe in (".environ", "getenv", "get_parameter", "get_secret_value", "ssm", "secrets_manager")):
                continue
            findings.append(("P1", f"{label}: {snippet[:30]}..."))
            break  # one finding per pattern is enough

    # 7. unhandled returns (bad pattern: return None on error means client gets 502)
    # Skip — too many false positives

    # 8. extremely long file — split candidate
    if size > 80_000:
        findings.append(("P2", f"large source ({size//1024} KB) — may exceed Lambda inline size limit; consider splitting"))

    # 9. except: pass (silent failures)
    bare_pass = re.findall(r'except[^:]*:\s*\n\s*pass\b', text)
    if len(bare_pass) > 3:
        findings.append(("P2", f"silent except-pass blocks: {len(bare_pass)} (errors swallowed)"))

    # 10. print() debugging — Lambda environment uses print to ship to CloudWatch,
    # so print is fine. Skip.

    return findings


def audit_node(src_path: Path) -> list[tuple[str, str]]:
    findings = []
    text = src_path.read_text(errors="ignore")
    size = len(text)
    if size < 100:
        findings.append(("P0", f"source extremely small ({size}B) — likely corrupted/empty"))
        return findings
    if not re.search(r'(exports\.(?:handler|main)|export\s+(?:async\s+)?(?:const|function)\s+handler)', text):
        findings.append(("P1", "no exports.handler / export handler found"))
    for ep, why in BLOCKED_ENDPOINTS.items():
        if ep in text:
            findings.append(("P1", f"references blocked endpoint {ep}"))
    for pat, label in SECRET_PATTERNS:
        if re.search(pat, text):
            # Just flag; node patterns vary
            findings.append(("P1", f"possible {label}"))
            break
    return findings


# ------------------------------------------------------------------
# Run
# ------------------------------------------------------------------

for d in sorted(LAMBDAS.iterdir()):
    if not d.is_dir():
        continue
    name = d.name
    cfg = load_config(d)
    runtime = cfg.get("runtime", "?")
    handler = cfg.get("handler", "?")

    findings: list[tuple[str, str]] = []

    if cfg.get("_invalid_config"):
        findings.append(("P0", "config.json is malformed"))

    if runtime in DEPRECATED_RUNTIMES:
        findings.append(("P1", f"deprecated runtime {runtime} (AWS will block invocations)"))

    src = find_source_file(d, handler) if "." in handler else None
    if not src:
        findings.append(("P0", f"source file for handler '{handler}' not found"))
    else:
        if src.suffix == ".py":
            handler_func = handler.rsplit(".", 1)[1]
            findings += audit_python(src, handler_func)
        elif src.suffix in (".js", ".mjs"):
            findings += audit_node(src)

    # Severity rollup
    severities = [s for s, _ in findings]
    if "P0" in severities:
        cls = "P0"
    elif "P1" in severities:
        cls = "P1"
    elif "P2" in severities:
        cls = "P2"
    else:
        cls = "OK"
    results.append({"name": name, "runtime": runtime, "class": cls, "findings": findings})


# ------------------------------------------------------------------
# Report
# ------------------------------------------------------------------
counts = defaultdict(int)
for r in results:
    counts[r["class"]] += 1

print(f"\n=== AUDIT SUMMARY (110 Lambdas) ===")
for c in ("P0", "P1", "P2", "OK"):
    print(f"  {c}: {counts[c]}")

for cls in ("P0", "P1", "P2"):
    same = [r for r in results if r["class"] == cls]
    if not same:
        continue
    print(f"\n=== {cls} ({len(same)}) ===")
    for r in same:
        print(f"\n  {r['name']}  [{r['runtime']}]")
        for sev, msg in r["findings"]:
            if sev == "INFO":
                continue
            print(f"    [{sev}] {msg}")
        # also show INFO if relevant
        infos = [m for s, m in r["findings"] if s == "INFO"]
        if infos and cls in ("P0", "P1"):
            for i in infos[:1]:
                print(f"    [info] {i}")

# Save full report to a file for follow-up commits
report_path = Path("aws/ops/reports/latest/lambda-static-audit-2026-04-27.md")
report_path.parent.mkdir(parents=True, exist_ok=True)
with report_path.open("w") as f:
    f.write(f"# Lambda static audit — 2026-04-27\n\n")
    f.write(f"Static-only audit of all 110 Lambda sources in `aws/lambdas/`.\n\n")
    f.write(f"## Summary\n\n| Class | Count |\n|-------|-------|\n")
    for c in ("P0", "P1", "P2", "OK"):
        f.write(f"| {c} | {counts[c]} |\n")
    f.write("\n")
    for cls in ("P0", "P1", "P2"):
        same = [r for r in results if r["class"] == cls]
        if not same:
            continue
        f.write(f"\n## {cls} ({len(same)})\n\n")
        for r in same:
            f.write(f"### `{r['name']}` [{r['runtime']}]\n")
            for sev, msg in r["findings"]:
                f.write(f"- **{sev}** — {msg}\n")
            f.write("\n")

print(f"\nReport written to {report_path}")
