#!/usr/bin/env python3
"""build_workspaces.py -- standalone engines for the two chart-pro workspaces.

Khalid (2026-09-03): "make Universe Heatmap and Macro & Economic Data their own
separate engines (same design, same capabilities, everything) but keep them on
chart-pro too -- I want to be able to pull them by themselves when needed."

chart-pro.html stays the single source of truth. This script EXTRACTS from it:
  * the two classes (MacroData, Heatmap) plus the helpers they lean on
    (UI.esc/fmtPct + the signal finders, jhToast, JHF.load/badges),
  * every CSS rule that styles them (selectors touching heatmap/hm-/macro-/
    editor/modal-close/wl-*), together with the :root palette and the light
    theme variables,
  * the modal + editor markup,
and writes:
  assets/jh-workspaces.js   shim (PROXY, State, feed loader, chart handoff) + classes
  assets/jh-workspaces.css  extracted rules + inline-page layout
  heatmap.html              standalone Universe Heatmap
  macro-data.html           standalone Macro & Economic Data

Run locally to commit the outputs, and in pages.yml right after the site
artifact is assembled so the deployed engines are always regenerated from the
chart-pro that ships with them.

Usage: python3 scripts/build_workspaces.py [site_root=.]
"""
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SITE = Path(sys.argv[1]) if len(sys.argv) > 1 else ROOT
SRC = ROOT / "chart-pro.html"

CSS_TOKENS = ("heatmap", ".hm-", "#hm-", "macro", ".modal-close", ".wl-loading", ".wl-empty",
              ".new-wl-modal", ".mc-flag", ".ws-", "series-drop-target", "drag-before", "drag-after")


def between(text, start_marker, end_marker, start_from=0):
    s = text.index(start_marker, start_from)
    e = text.index(end_marker, s + len(start_marker))
    return text[s:e], s, e


def css_rules(style_text):
    """Split a stylesheet into top-level rules (selector, body) handling nesting."""
    rules, i, n = [], 0, len(style_text)
    while i < n:
        j = style_text.find("{", i)
        if j < 0:
            break
        selector = style_text[i:j].strip()
        depth, k = 1, j + 1
        while k < n and depth:
            c = style_text[k]
            if c == "{":
                depth += 1
            elif c == "}":
                depth -= 1
            k += 1
        body = style_text[j + 1:k - 1]
        rules.append((selector, body))
        i = k
    return rules


def wanted(selector, body):
    hay = selector.lower()
    if selector.startswith("@media"):
        inner = css_rules(body)
        keep = [r for r in inner if any(t in r[0].lower() for t in CSS_TOKENS)]
        return keep
    if selector.startswith("@keyframes") and "macro" in hay:
        return True
    return any(t in hay for t in CSS_TOKENS)


def extract_css(html):
    out = []
    for style in re.findall(r"<style[^>]*>(.*?)</style>", html, flags=re.S):
        for selector, body in css_rules(style):
            if selector.startswith("@media"):
                keep = wanted(selector, body)
                if keep:
                    out.append("%s {\n%s\n}" % (selector, "\n".join("  %s {%s}" % (s, b) for s, b in keep)))
            elif wanted(selector, body):
                out.append("%s {%s}" % (selector, body))
    return "\n".join(out)


def extract_vars(html):
    root_block, _, _ = between(html, ":root {", "}")
    light_block, _, _ = between(html, 'body[data-chart-theme="light"] {', "}")
    return root_block + "}\n" + light_block + "}\n"


def extract_method(text, header, next_header_pattern=r"\n  static |\n}\n"):
    s = text.index(header)
    m = re.search(next_header_pattern, text[s + len(header):])
    e = s + len(header) + (m.start() if m else 0)
    return text[s:e].rstrip() + "\n"


def main():
    html = SRC.read_text(encoding="utf-8")
    # ---- JS pieces ----------------------------------------------------------
    macro_js, _, _ = between(html, "class MacroData {", "\n// ─── NATIVE CHART")
    heat_js, _, _ = between(html, "class Heatmap {", "\n// ─── INIT ──")
    ui_class, _, _ = between(html, "class UI {", "\n}\n")
    ui_class += "\n}\n"
    ui_esc = extract_method(ui_class, "  static esc(s) {")
    ui_fmtpct = extract_method(ui_class, "  static fmtPct(n, digits = 2) {")
    finders = ""
    for name in ("findCascadeMatch", "findOptionsMatch", "findInsiderMatch", "findRetailMatch", "findTradeTicket", "findPrediction"):
        finders += extract_method(ui_class, "  static %s(ticker) {" % name)
    toast, _, _ = between(html, "function jhToast(msg) {", "\n}\n")
    toast += "\n}\n"
    jhf_class, _, _ = between(html, "class JHF {", "\n(function jhfWire()")
    jhf_load = extract_method(jhf_class, "  static async load() {", r"\n  static badges\(")
    jhf_badges = extract_method(jhf_class, "  static badges(t) {")
    for piece, label in ((macro_js, "MacroData"), (heat_js, "Heatmap"), (jhf_load, "JHF.load"), (jhf_badges, "JHF.badges")):
        if piece.count("{") != piece.count("}"):
            raise SystemExit("unbalanced braces extracting %s" % label)

    shim = f'''/* jh-workspaces.js -- generated by scripts/build_workspaces.py from chart-pro.html.
 * DO NOT EDIT: edit chart-pro.html and rebuild. The Universe Heatmap and the
 * Macro & Economic Data workspaces run here exactly as inside Chart Pro; the
 * host shim below supplies what Chart Pro's page normally provides. */
(function () {{
  if (typeof window.PROXY === 'undefined') window.PROXY = "https://justhodl-data-proxy.raafouis.workers.dev";
}})();
const PROXY = window.PROXY;
window.State = window.State || {{
  cascade: null, insider: null, options: null, retail: null, trade_tickets: null, predictions: null,
  fusion: null, fusionOn: true, chartEngine: 'native', activeTicker: null,
}};
const State = window.State;
class UI {{
{ui_esc}{ui_fmtpct}{finders}}}
{toast}
class JHF {{
{jhf_load}{jhf_badges}}}
// Chart handoff: the standalone engines open Chart Pro in a new tab for charting and comparison.
class ChartController {{
  static loadTicker(symbol) {{
    if (!symbol) return;
    const url = '/chart-pro.html?s=' + encodeURIComponent(symbol);
    const w = window.open(url, '_blank', 'noopener');
    if (!w) location.href = url;
  }}
}}
class CompareController {{
  static add(symbol) {{
    if (!symbol) return;
    const base = (State.activeTicker && State.activeTicker !== symbol) ? State.activeTicker : symbol;
    const url = '/chart-pro.html?s=' + encodeURIComponent(base) + '&cmp=' + encodeURIComponent(symbol);
    const w = window.open(url, '_blank', 'noopener');
    if (!w) location.href = url;
  }}
}}
const JH_WS = {{
  FEEDS: {{
    cascade: 'https://justhodl-data-proxy.raafouis.workers.dev/data/theme-cascade.json',
    insider: 'https://justhodl-data-proxy.raafouis.workers.dev/data/insider-clusters.json',
    options: 'https://justhodl-data-proxy.raafouis.workers.dev/data/polygon-options-flow.json',
    retail: 'https://justhodl-data-proxy.raafouis.workers.dev/data/retail-sentiment.json',
    trade_tickets: 'https://justhodl-data-proxy.raafouis.workers.dev/data/trade-tickets.json',
    predictions: 'https://justhodl-data-proxy.raafouis.workers.dev/data/predictions-snapshots/latest.json',
  }},
  async loadFeeds() {{
    const get = async (url) => {{
      try {{ const r = await fetch(url + '?t=' + Math.floor(Date.now() / 3e5)); return r.ok ? await r.json() : null; }}
      catch (e) {{ return null; }}
    }};
    const keys = Object.keys(this.FEEDS);
    const values = await Promise.all(keys.map(k => get(this.FEEDS[k])));
    keys.forEach((k, i) => {{ if (values[i]) State[k] = values[i]; }});
    try {{ await JHF.load(); }} catch (e) {{ console.warn('[jh-ws] fusion load', e); }}
    return State;
  }},
  theme() {{
    let t = null;
    try {{ t = localStorage.getItem('jh_chart_theme'); }} catch (e) {{}}
    document.body.dataset.chartTheme = t === 'dark' ? 'dark' : 'light';
  }},
  boot(which) {{
    this.theme();
    document.body.classList.add('jh-ws-page');
    const cls = which === 'heatmap' ? Heatmap : MacroData;
    cls.inline = true;
    cls.init();
    cls.open();
    if (which === 'macro') {{
      let timer = null;
      const input = document.getElementById('macro-search-input');
      if (input) input.addEventListener('input', e => {{ clearTimeout(timer); const v = e.target.value; timer = setTimeout(() => MacroData.search(v), 300); }});
    }}
    this.loadFeeds().then(() => {{ if (which === 'heatmap') {{ Heatmap.render(); }} }});
    return cls;
  }},
}};
window.JH_WS = JH_WS;
'''
    bundle = shim + "\n" + macro_js.rstrip() + "\n\n" + heat_js.rstrip() + "\n"
    bundle += "\nwindow.MacroData = MacroData; window.Heatmap = Heatmap; window.UI = UI; window.JHF = JHF;\n"

    # ---- CSS ----------------------------------------------------------------
    css = "/* jh-workspaces.css -- generated by scripts/build_workspaces.py from chart-pro.html. DO NOT EDIT. */\n"
    css += extract_vars(html)
    css += extract_css(html)
    css += """
/* standalone engine layout */
body.jh-ws-page { margin: 0; background: var(--bg); color: var(--fg-1); font-family: 'Inter', -apple-system, system-ui, sans-serif; }
body.jh-ws-page .ws-header { height: 52px; display: flex; align-items: center; gap: 14px; padding: 0 16px 0 60px; border-bottom: 1px solid var(--border); background: var(--bg-1); font-family: var(--font-mono, ui-monospace, monospace); }
body.jh-ws-page .ws-brand { font-weight: 800; color: var(--fg-0); letter-spacing: .3px; text-decoration: none; }
body.jh-ws-page .ws-brand span { color: var(--cyan); }
body.jh-ws-page .ws-crumb { color: var(--fg-3); font-size: 11px; }
body.jh-ws-page .ws-links { margin-left: auto; display: flex; gap: 8px; align-items: center; }
body.jh-ws-page .ws-links a { color: var(--fg-2); font-size: 11px; font-weight: 700; text-decoration: none; border: 1px solid var(--border); border-radius: 5px; padding: 6px 10px; }
body.jh-ws-page .ws-links a:hover { color: var(--cyan); border-color: var(--cyan); }
body.jh-ws-page .heatmap-modal { position: static; display: block; background: transparent; backdrop-filter: none; inset: auto; }
body.jh-ws-page .heatmap-content, body.jh-ws-page .hm-content { width: 100%; max-width: none; height: calc(100vh - 52px); border: 0; border-radius: 0; }
body.jh-ws-page .macro-open-page, body.jh-ws-page .hm-open-page { display: none; }
body.jh-ws-page .new-wl-modal { position: fixed; }
"""

    # ---- markup ---------------------------------------------------------------
    heat_markup, _, _ = between(html, "<!-- Heatmap Modal (Universe Heatmap workspace) -->", "<!-- Macro Data Catalog Modal -->")
    macro_markup, _, _ = between(html, '<div class="heatmap-modal" id="macro-modal"', "<!-- New Watchlist Modal -->")
    for markup, label in ((heat_markup, "heatmap"), (macro_markup, "macro")):
        if markup.count("<div") != markup.count("</div>"):
            raise SystemExit("unbalanced markup extracting %s" % label)

    def page(which, title, crumb, markup, open_class):
        markup = markup.replace('class="heatmap-modal" id="heatmap-modal"', 'class="heatmap-modal open" id="heatmap-modal"') if which == "heatmap" else \
            markup.replace('class="heatmap-modal" id="macro-modal"', 'class="heatmap-modal open" id="macro-modal"')
        other = ("/macro-data.html", "Macro &amp; Economic Data") if which == "heatmap" else ("/heatmap.html", "Universe Heatmap")
        return f'''<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8"><meta name="viewport" content="width=device-width,initial-scale=1.0">
<title>{title} · JustHodl AI</title>
<meta name="description" content="{crumb} — the same engine that lives inside Chart Pro, as its own page.">
<link rel="stylesheet" href="/assets/jh-workspaces.css">
<!-- generated by scripts/build_workspaces.py from chart-pro.html -- do not edit by hand -->
</head>
<body class="jh-ws-page">
<header class="ws-header">
  <a class="ws-brand" href="/">JustHodl<span>.AI</span></a>
  <span class="ws-crumb">{crumb}</span>
  <div class="ws-links">
    <a href="{other[0]}">{other[1]}</a>
    <a href="/chart-pro.html" title="Open the full terminal">Chart Pro ↗</a>
    <span data-auth-slot></span>
  </div>
</header>
<main>
{markup}
</main>
<script src="/auth-config.js"></script>
<script src="https://cdn.jsdelivr.net/npm/@supabase/supabase-js@2"></script>
<script src="/auth.js"></script>
<script src="/assets/jh-workspaces.js"></script>
<script>
  document.addEventListener('DOMContentLoaded', function () {{
    try {{ if (window.JustHodlAuth) JustHodlAuth.init(); }} catch (e) {{}}
    JH_WS.boot('{which}');
  }});
</script>
<script src="/jh-nav-drawer.js" defer></script>
</body>
</html>
'''

    (SITE / "assets").mkdir(parents=True, exist_ok=True)
    (SITE / "assets" / "jh-workspaces.js").write_text(bundle, encoding="utf-8")
    (SITE / "assets" / "jh-workspaces.css").write_text(css, encoding="utf-8")
    (SITE / "heatmap.html").write_text(page("heatmap", "Universe Heatmap", "Universe Heatmap · configurable performance / cascade / signal heatmap", heat_markup, "open"), encoding="utf-8")
    (SITE / "macro-data.html").write_text(page("macro", "Macro & Economic Data", "Macro &amp; Economic Data · every indexed series, your categories, danger rules", macro_markup, "open"), encoding="utf-8")
    print("built: assets/jh-workspaces.js (%d bytes), assets/jh-workspaces.css (%d bytes), heatmap.html, macro-data.html"
          % (len(bundle), len(css)))


if __name__ == "__main__":
    main()
