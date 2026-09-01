#!/usr/bin/env python3
"""Content-Security-Policy as code (ops 5094).

The CSP served for justhodl.ai is a Cloudflare response-header transform rule
(ref `a2a-csp-header`, ops 4386). It was hand-written to close an A2A security
critique and never audited against the origins the fleet actually loads, so
from 2026-08-04 it silently blocked jsDelivr on 38 pages, Google Fonts on 65,
Tailwind on 8, Plotly on 5 and every Lambda function URL in connect-src.

This generator DERIVES the policy from the site tree, so the policy can only
ever be as permissive as the code requires and can never drift below it:

  * script-src : <script src>, ES-module imports, importScripts
  * style-src  : <link rel=stylesheet>, @import url()
  * font-src   : fonts.gstatic.com + CDN hosts that serve @font-face files
  * frame-src  : <iframe src>
  * connect-src: every https:// or wss:// host referenced from JavaScript
                 context (fetch/WebSocket/XHR/constants), minus a curated
                 list of navigation-only hosts (plain <a href> targets), with
                 wildcard folding for the Lambda-URL / execute-api / Supabase
                 families so a new function URL never breaks a page again

Fixed directives (object-src 'none', base-uri 'none', frame-ancestors 'self')
are the security value of the original rule and are never widened.

Usage:
  python3 scripts/gen_csp.py            # print the header
  python3 scripts/gen_csp.py --write    # regenerate config/csp-policy.json
  python3 scripts/gen_csp.py --check    # exit 1 if config is out of date
  python3 scripts/gen_csp.py --json     # machine-readable policy
"""
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
POLICY_PATH = ROOT / "config" / "csp-policy.json"

# Same publish surface as .github/workflows/pages.yml (lean artifact + page dirs)
DENY_DIRS = {"aws", ".github", "scripts", "ci", "config", "cloudflare", "ops", "docs",
             "supabase", "tools-src", "chrome-extension", "node_modules", "_partials",
             "_site", ".git"}
STATIC_DIRS = {"assets", "js", "css", "img", "fonts", "tools", "data"}

URL_RE = re.compile(r"((?:https|wss)://[a-zA-Z0-9.\-]+(?::\d+)?)")
SCRIPT_BLOCK_RE = re.compile(r"<script\b[^>]*>(.*?)</script>", re.S | re.I)
SCRIPT_SRC_RE = re.compile(r"<script\b[^>]*\bsrc=[\"'](https://[^\"']+)", re.I)
LINK_RE = re.compile(r"<link\b[^>]*>", re.I)
IMPORT_RE = re.compile(r"(?:\bimport\s*\(|\bfrom\s+|importScripts\()\s*[\"'](https://[^\"']+)")
CSS_IMPORT_RE = re.compile(r"@import\s+(?:url\()?[\"']?(https://[^\"')\s]+)")
CSS_URL_RE = re.compile(r"url\(\s*[\"']?(https://[^\"')\s]+)")
IFRAME_RE = re.compile(r"<iframe\b[^>]*\bsrc=[\"'](https://[^\"']+)", re.I)
HREF_RE = re.compile(r"\bhref=[\"'](https://[^\"']+)", re.I)
FONT_EXT = (".woff", ".woff2", ".ttf", ".otf", ".eot")


VALID_HOST_RE = re.compile(r"^(https|wss)://(\*\.)?[a-z0-9][a-z0-9-]*(\.[a-z0-9][a-z0-9-]*)+(:\d+)?$", re.I)


def _host(url):
    """Origin of a URL, or None when it is not a real host (placeholders such as
    https://...lambda-url... in comments must never reach the header)."""
    m = URL_RE.match(url)
    if not m:
        return None
    h = m.group(1).lower()
    return h if VALID_HOST_RE.match(h) and ".." not in h else None


def site_files():
    """Every file that ships to GitHub Pages (mirrors pages.yml)."""
    out = []
    for p in ROOT.iterdir():
        if p.is_file() and p.suffix in (".html", ".js", ".css"):
            out.append(p)
        elif p.is_dir() and p.name not in DENY_DIRS and not p.name.startswith("."):
            if p.name in STATIC_DIRS or any(p.rglob("*.html")):
                out.extend(q for q in p.rglob("*") if q.is_file()
                           and q.suffix in (".html", ".js", ".css")
                           and "node_modules" not in q.parts)
    return sorted(out)


def scan(files=None):
    """Return {directive: {host: set(files)}} plus the navigation-only host set."""
    found = {"script-src": {}, "style-src": {}, "font-src": {}, "frame-src": {},
             "connect-src": {}, "img-src": {}}
    nav_only = {}

    def add(directive, url, f):
        h = _host(url)
        if h:
            found[directive].setdefault(h, set()).add(f)

    for path in files or site_files():
        rel = path.relative_to(ROOT).as_posix()
        try:
            text = path.read_text(encoding="utf-8", errors="replace")
        except Exception:
            continue
        js_context = []
        if path.suffix == ".html":
            for m in SCRIPT_SRC_RE.finditer(text):
                add("script-src", m.group(1), rel)
            for m in LINK_RE.finditer(text):
                tag = m.group(0)
                href = re.search(r"\bhref=[\"'](https://[^\"']+)", tag, re.I)
                if not href:
                    continue
                rel_attr = (re.search(r"\brel=[\"']([^\"']+)", tag, re.I) or [None, ""])[1].lower()
                if "stylesheet" in rel_attr:
                    add("style-src", href.group(1), rel)
                elif "preload" in rel_attr or "modulepreload" in rel_attr:
                    as_attr = (re.search(r"\bas=[\"']([^\"']+)", tag, re.I) or [None, ""])[1].lower()
                    if as_attr == "font":
                        add("font-src", href.group(1), rel)
                    elif as_attr == "script":
                        add("script-src", href.group(1), rel)
                    elif as_attr == "style":
                        add("style-src", href.group(1), rel)
                # preconnect / dns-prefetch / canonical / icon: no CSP directive
            for m in IFRAME_RE.finditer(text):
                add("frame-src", m.group(1), rel)
            for m in re.finditer(r"<img\b[^>]*\bsrc=[\"'](https://[^\"']+)", text, re.I):
                add("img-src", m.group(1), rel)
            for m in HREF_RE.finditer(text):
                h = _host(m.group(1))
                if h:
                    nav_only.setdefault(h, set()).add(rel)
            js_context = [b.group(1) for b in SCRIPT_BLOCK_RE.finditer(text)]
            # inline <style> blocks
            for sb in re.finditer(r"<style\b[^>]*>(.*?)</style>", text, re.S | re.I):
                for m in CSS_IMPORT_RE.finditer(sb.group(1)):
                    add("style-src", m.group(1), rel)
                for m in CSS_URL_RE.finditer(sb.group(1)):
                    u = m.group(1)
                    add("font-src" if u.lower().split("?")[0].endswith(FONT_EXT) else "img-src", u, rel)
        elif path.suffix == ".css":
            for m in CSS_IMPORT_RE.finditer(text):
                add("style-src", m.group(1), rel)
            for m in CSS_URL_RE.finditer(text):
                u = m.group(1)
                add("font-src" if u.lower().split("?")[0].endswith(FONT_EXT) else "img-src", u, rel)
            continue
        else:
            js_context = [text]
        for block in js_context:
            for m in IMPORT_RE.finditer(block):
                add("script-src", m.group(1), rel)
            for m in URL_RE.finditer(block):
                add("connect-src", m.group(1), rel)
    return found, nav_only


def _matches_wildcard(host, wildcard):
    """CSP host-source wildcard: scheme://*.example.com matches any subdomain."""
    ws, wh = wildcard.split("://", 1)
    hs, hh = host.split("://", 1)
    if ws != hs or not wh.startswith("*."):
        return host == wildcard
    return hh.endswith(wh[1:]) and hh != wh[2:]


def build(policy_cfg=None, files=None):
    cfg = policy_cfg if policy_cfg is not None else json.loads(POLICY_PATH.read_text())
    found, nav_only = scan(files)
    link_only = set(cfg.get("link_only_hosts") or [])
    deny = set(cfg.get("deny_hosts") or [])
    wild = cfg.get("wildcards") or {}
    extra = cfg.get("extra") or {}
    generated, evidence = {}, {}
    for d in ("script-src", "style-src", "font-src", "frame-src", "connect-src"):
        hosts = set(found[d])
        if d == "connect-src":
            # a host that is ONLY ever an <a href> target is navigation, not a connection
            hosts = {h for h in hosts if not (h in link_only)}
        hosts -= deny
        wilds = list(wild.get(d) or [])
        explicit = sorted(h for h in hosts if not any(_matches_wildcard(h, w) for w in wilds))
        generated[d] = sorted(set(explicit) | set(extra.get(d) or []) | set(wilds))
        evidence[d] = {h: sorted(found[d].get(h, []))[:6] for h in sorted(hosts)}
    # fonts: any CDN that serves stylesheets may also serve @font-face files
    fixed = cfg.get("fixed") or {}
    directives = []
    for d, vals in fixed.items():
        if d in generated:
            continue
        directives.append((d, list(vals)))
    order = ["default-src", "script-src", "style-src", "font-src", "img-src", "media-src",
             "connect-src", "frame-src", "worker-src", "manifest-src", "object-src",
             "base-uri", "frame-ancestors"]
    parts = {}
    for d, vals in directives:
        parts[d] = vals
    parts["script-src"] = ["'self'", "'unsafe-inline'"] + generated["script-src"]
    parts["style-src"] = ["'self'", "'unsafe-inline'"] + generated["style-src"]
    parts["font-src"] = ["'self'", "data:"] + generated["font-src"]
    parts["connect-src"] = ["'self'"] + generated["connect-src"]
    parts["frame-src"] = ["'self'"] + generated["frame-src"]
    header = "; ".join(f"{d} {' '.join(parts[d])}" for d in order if d in parts)
    if cfg.get("upgrade_insecure_requests", True):
        header += "; upgrade-insecure-requests"
    return {"header": header, "generated": generated, "evidence": evidence,
            "nav_only_hosts": sorted(nav_only), "n_files": len(files or site_files())}


def main(argv):
    cfg = json.loads(POLICY_PATH.read_text())
    res = build(cfg)
    if "--json" in argv:
        print(json.dumps({k: res[k] for k in ("header", "generated", "nav_only_hosts", "n_files")}, indent=1))
        return 0
    if "--write" in argv:
        cfg["generated"] = res["generated"]
        cfg["evidence"] = res["evidence"]
        cfg["header"] = res["header"]
        cfg["generated_at"] = datetime.now(timezone.utc).isoformat(timespec="seconds")
        cfg["n_files_scanned"] = res["n_files"]
        POLICY_PATH.write_text(json.dumps(cfg, indent=1, ensure_ascii=False) + "\n")
        print(f"wrote {POLICY_PATH.relative_to(ROOT)} ({len(res['header'])} chars, {res['n_files']} files scanned)")
        return 0
    if "--check" in argv:
        if cfg.get("header") != res["header"]:
            print("STALE: config/csp-policy.json header differs from the site tree — run scripts/gen_csp.py --write")
            print("have:", cfg.get("header"))
            print("want:", res["header"])
            return 1
        print("csp-policy.json is current")
        return 0
    print(res["header"])
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
