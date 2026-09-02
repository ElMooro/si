#!/usr/bin/env python3
"""Bake the provider inventory into data.html at Pages build time (ops 5129).

Why: an external audit of https://justhodl.ai/data.html read the page without
JavaScript, saw only the ECB/FRED retrieval playbook text and concluded the
data plane is two providers. The 57-provider grid is client-rendered from
data/provider-catalog.json, so every non-JS reader (scrapers, auditors, AI
agents, search engines) gets the wrong answer. The playbook's own rule is
"verify after write"; a page that cannot be verified without a browser fails
it. This bake injects, between markers, a static table of every provider
(name, API, datasets, series, size, freshest, coverage) plus a JSON blob
(<script type="application/json" id="data-plane-inventory">) with the same
numbers, and a machine-readable pointer to the catalog. The client grid still
renders live on top. NEVER fails the deploy: any error leaves the file
untouched and exits 0.
"""
import html
import json
import sys
import time
import urllib.request

START = "<!-- inventory-static:begin -->"
END = "<!-- inventory-static:end -->"


def get(url, timeout=12):
    req = urllib.request.Request(f"{url}?t={int(time.time())}", headers={"User-Agent": "jh-bake/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def fmt_n(x):
    try:
        return f"{int(x):,}"
    except Exception:  # noqa: BLE001
        return "—"


def main():
    site = sys.argv[1] if len(sys.argv) > 1 else "_site"
    path = f"{site}/data.html"
    try:
        src = open(path, encoding="utf-8").read()
    except FileNotFoundError:
        print("bake_data_inventory: no data.html — skipping")
        return 0
    try:
        hub = get("https://justhodl.ai/data/provider-catalog.json")
    except Exception as e:  # noqa: BLE001
        print(f"bake_data_inventory: hub unreadable ({e}) — skipping")
        return 0
    provs = hub.get("providers") or []
    if not provs:
        print("bake_data_inventory: empty hub — skipping")
        return 0
    tot = hub.get("totals") or {}
    provs = sorted(provs, key=lambda p: -(p.get("total_mb") or 0))
    rows = []
    inv = []
    for p in provs:
        fh = p.get("freshest_h")
        cov = p.get("coverage_pct")
        rows.append(
            "<tr><td><a href=\"/provider.html?p=%s\">%s</a></td><td>%s</td><td style=\"text-align:right\">%s</td>"
            "<td style=\"text-align:right\">%s</td><td style=\"text-align:right\">%s</td><td style=\"text-align:right\">%s</td>"
            "<td style=\"text-align:right\">%s</td><td>%s</td></tr>" % (
                html.escape(str(p.get("slug", ""))), html.escape(str(p.get("name", ""))), html.escape(str(p.get("api", ""))),
                fmt_n(p.get("datasets")), fmt_n(p.get("series_count")) if p.get("series_count") else "—",
                html.escape(str(p.get("total_mb", "—"))), "—" if fh is None else f"{fh}h",
                "—" if cov is None else f"{cov}%", html.escape(str(p.get("catalog_note") or "")[:120])))
        inv.append({"slug": p.get("slug"), "name": p.get("name"), "api": p.get("api"), "datasets": p.get("datasets"), "series": p.get("series_count"),
                    "mb": p.get("total_mb"), "freshest_h": fh, "coverage_pct": cov, "engines": (p.get("engines") or [])[:12]})
    as_of = hub.get("as_of") or ""
    block = "\n".join([
        START,
        '<section id="inventory-static" style="margin-top:22px;background:#0d141c;border:1px solid #1c2733;border-radius:10px;padding:12px 16px">',
        '<div style="font-family:monospace;font-size:11px;color:#8fd0ff;font-weight:700">DATA PLANE INVENTORY — %s providers · %s datasets/files · %s S3 keys · %s GB · inventoried %s</div>' % (
            fmt_n(tot.get("providers")), fmt_n(hub.get("datasets_total") or tot.get("datasets") or tot.get("keys")), fmt_n(tot.get("keys")), html.escape(str(tot.get("gb", "—"))), html.escape(as_of[:19])),
        '<div style="font-size:10px;color:#5d7285;margin:4px 0 8px">Static snapshot baked at build time from <a href="/data/provider-catalog.json" style="color:#7fb0d0">/data/provider-catalog.json</a> (the machine-readable truth; every provider has <code>/data/providers/{slug}.json</code>). The live grid above refreshes client-side; this table exists so readers without JavaScript — scrapers, auditors, AI agents — see the real data plane.</div>',
        '<div style="overflow-x:auto"><table style="border-collapse:collapse;font-size:10.5px;width:100%">',
        '<thead><tr style="color:#5d7285;text-align:left"><th>provider</th><th>source API</th><th style="text-align:right">datasets</th><th style="text-align:right">series</th><th style="text-align:right">MB</th><th style="text-align:right">freshest</th><th style="text-align:right">coverage</th><th>note</th></tr></thead>',
        "<tbody>" + "\n".join(rows) + "</tbody></table></div>",
        '<script type="application/json" id="data-plane-inventory">' + json.dumps({"as_of": as_of, "totals": tot, "datasets_total": hub.get("datasets_total"), "providers": inv}, separators=(",", ":")).replace("</", "<\\/") + "</script>",
        "</section>",
        END,
    ])
    if START in src and END in src:
        a, b = src.index(START), src.index(END) + len(END)
        out = src[:a] + block + src[b:]
    else:
        anchor = '<details id="ai-playbook"'
        if anchor not in src:
            print("bake_data_inventory: anchor missing — skipping")
            return 0
        out = src.replace(anchor, block + "\n" + anchor, 1)
    open(path, "w", encoding="utf-8").write(out)
    print(f"bake_data_inventory: injected {len(provs)} providers ({len(block)} bytes)")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as e:  # noqa: BLE001
        print(f"bake_data_inventory: error {e} — left untouched")
        sys.exit(0)
