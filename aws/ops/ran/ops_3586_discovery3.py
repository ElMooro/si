"""ops 3586 — DISCOVERY #3: (B) walk PBoC EN pages to the actual monthly
Aggregate Financing table/attachment (build the scrape recipe), (A) corrected
data.gov.tw API variants + harvest export-order hrefs from eng.stat.gov.tw.
Findings-only."""
import json, re, ssl, sys, time, urllib.parse, urllib.request
from pathlib import Path
from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (JustHodl research contact@justhodl.ai)"}
CTX = ssl.create_default_context(); CTX.check_hostname = False; CTX.verify_mode = ssl.CERT_NONE

def fetch(url, timeout=18, insecure=False, limit=500_000):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                   timeout=timeout, context=(CTX if insecure else None))
        return r.status, r.read(limit)
    except Exception as e:
        return None, str(e)[:140].encode()

def links(html, base):
    out = []
    for m in re.finditer(r'<a[^>]+href="([^"]+)"[^>]*>(.*?)</a>', html, re.I | re.S):
        href, txt = m.group(1), re.sub(r"<[^>]+>|\s+", " ", m.group(2)).strip()
        out.append((urllib.parse.urljoin(base, href), txt[:90]))
    return out

with report("3586_discovery3") as rep:
    rep.heading("ops 3586 — discovery #3: PBoC AFRE recipe + TW retargets")
    out = {"probes": {}}

    def note(name, payload):
        out["probes"][name] = payload
        line = f"{name}: {json.dumps(payload, ensure_ascii=False, default=str)[:460]}"
        print(line); rep.log(line)

    # ── B) PBoC AFRE walk ──────────────────────────────────────────────
    st, raw = fetch("http://www.pbc.gov.cn/en/3688006/index.html")
    afre_links = []
    if st == 200:
        html = raw.decode("utf-8", "replace")
        afre_links = [(u, t) for u, t in links(html, "http://www.pbc.gov.cn/en/3688006/index.html")
                      if "aggregate financing" in t.lower() or "social financing" in t.lower()]
    note("B1_stats_index", {"status": st, "afre_links": afre_links[:6]})
    item_links, table_probe = [], None
    if afre_links:
        st2, raw2 = fetch(afre_links[0][0])
        if st2 == 200:
            html2 = raw2.decode("utf-8", "replace")
            item_links = [(u, t) for u, t in links(html2, afre_links[0][0])
                          if re.search(r"20\d\d", t) or "aggregate financing" in t.lower()][:10]
        note("B2_afre_listing", {"status": st2, "items": item_links[:8]})
        if item_links:
            time.sleep(0.5)
            st3, raw3 = fetch(item_links[0][0])
            if st3 == 200:
                html3 = raw3.decode("utf-8", "replace")
                atts = [(u, t) for u, t in links(html3, item_links[0][0])
                        if re.search(r"\.(xlsx?|htm l?|csv)$", u, re.I) or "annex" in t.lower()]
                # table sniff: strip tags of first table
                tm = re.search(r"<table[\s\S]{0,20000}?</table>", html3, re.I)
                rows = []
                if tm:
                    for tr in re.findall(r"<tr[\s\S]*?</tr>", tm.group(0), re.I)[:6]:
                        cells = [re.sub(r"<[^>]+>|&nbsp;|\s+", " ", c).strip()[:24]
                                 for c in re.findall(r"<t[dh][\s\S]*?</t[dh]>", tr, re.I)][:8]
                        if any(cells):
                            rows.append(cells)
                table_probe = {"status": st3, "url": item_links[0][0],
                               "attachments": atts[:6], "table_rows": rows}
            else:
                table_probe = {"status": st3, "err": raw3.decode()[:120]}
        note("B3_afre_item", table_probe or {"skipped": "no items"})

    # ── A) Taiwan retargets ────────────────────────────────────────────
    q = urllib.parse.quote("外銷訂單")
    for tag, url in (("A1_dgtw_v2_singular", f"https://data.gov.tw/api/v2/rest/dataset?query={q}&page=1&size=6"),
                     ("A2_dgtw_front_list", f"https://data.gov.tw/api/front/dataset/list?query={q}&page=1&size=6"),
                     ("A3_dgtw_front_search", f"https://data.gov.tw/api/front/dataset/search?query={q}")):
        st, raw = fetch(url, insecure=True)
        body = raw.decode("utf-8", "replace")[:300]
        is_json = body.strip()[:1] in "[{"
        hits = []
        if st == 200 and is_json:
            try:
                j = json.loads(raw)
                recs = (j.get("records") or (j.get("result") or {}).get("records")
                        or (j.get("payload") or {}).get("records") or (j if isinstance(j, list) else []))
                hits = [{k: str(r0.get(k))[:80] for k in ("id", "title", "name", "資料集名稱") if isinstance(r0, dict) and r0.get(k)}
                        for r0 in (recs or [])[:5]]
            except Exception:
                pass
        note(tag, {"status": st, "json": is_json, "hits": hits,
                   "body_head": (None if hits else body[:140])})
    st, raw = fetch("https://eng.stat.gov.tw/", insecure=True)
    order_hrefs = []
    if st == 200:
        html = raw.decode("utf-8", "replace")
        order_hrefs = [(u, t) for u, t in links(html, "https://eng.stat.gov.tw/")
                       if "order" in t.lower() or "order" in u.lower()][:8]
    note("A4_engstat_order_links", {"status": st, "links": order_hrefs})

    out["verdict"] = "PROBE_COMPLETE"
    print("\nVERDICT: PROBE_COMPLETE"); rep.log("VERDICT: PROBE_COMPLETE")
    Path("aws/ops/reports/3586.json").write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    sys.exit(0)
