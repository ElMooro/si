"""ops 3585 — DISCOVERY PROBE #2 (gate-on-DATA, no builds): (A) Taiwan MOEA
export-ORDERS endpoint (data.gov.tw open-data API + MOEA/DGBAS hosts + DBnomics
retargets), (B) PBoC monthly TSF — walk NBS data.stats.gov.cn easyquery MONTHLY
tree (dbcode=hgyd) to discover the Aggregate Financing valuecode, plus PBoC
English reachability + DBnomics provider scan. Findings-only; never fails."""
import json, ssl, sys, time, urllib.parse, urllib.request
from pathlib import Path
from ops_report import report

UA = {"User-Agent": "Mozilla/5.0 (JustHodl research contact@justhodl.ai)", "Accept": "application/json,text/html"}
CTX = ssl.create_default_context()
CTX.check_hostname = False
CTX.verify_mode = ssl.CERT_NONE   # research read-only probes of gov hosts with broken chains

def fetch(url, timeout=15, insecure=False):
    try:
        r = urllib.request.urlopen(urllib.request.Request(url, headers=UA),
                                   timeout=timeout, context=(CTX if insecure else None))
        raw = r.read(400_000)
        return r.status, raw
    except Exception as e:
        return None, str(e)[:130].encode()

def jget(url, timeout=15, insecure=False):
    st, raw = fetch(url, timeout, insecure)
    if st != 200:
        return None, raw.decode("utf-8", "replace")[:130]
    try:
        return json.loads(raw), None
    except Exception as e:
        return None, f"non-json ({str(e)[:60]}): " + raw[:120].decode("utf-8", "replace")

with report("3585_discovery2") as rep:
    rep.heading("ops 3585 — discovery #2: TW export orders + PBoC monthly TSF")
    out = {"probes": {}}

    def note(name, payload):
        out["probes"][name] = payload
        line = f"{name}: {json.dumps(payload, ensure_ascii=False, default=str)[:430]}"
        print(line); rep.log(line)

    # ── A) TAIWAN EXPORT ORDERS ────────────────────────────────────────
    q = urllib.parse.quote("外銷訂單")
    j, e = jget(f"https://data.gov.tw/api/v2/rest/datasets?query={q}&page=1&size=8", insecure=True)
    rows = []
    if isinstance(j, dict):
        recs = (((j.get("result") or {}).get("records")) or j.get("records")
                or (j.get("payload") or {}).get("records") or [])
        for r0 in recs[:8]:
            rows.append({k: (str(r0.get(k))[:90]) for k in ("id", "title", "name", "識別碼", "資料集名稱")
                         if r0.get(k)})
    note("A1_datagovtw_search", {"err": e, "top_keys": list(j)[:6] if isinstance(j, dict) else None,
                                 "hits": rows})
    # english query fallback
    j2, e2 = jget("https://data.gov.tw/api/v2/rest/datasets?query=export%20orders&page=1&size=6", insecure=True)
    rows2 = []
    if isinstance(j2, dict):
        recs = (((j2.get("result") or {}).get("records")) or j2.get("records") or [])
        rows2 = [{k: str(r0.get(k))[:80] for k in ("id", "title", "name") if r0.get(k)} for r0 in recs[:6]]
    note("A2_datagovtw_en", {"err": e2, "hits": rows2})
    for tag, url in (("A3_moea_root", "https://www.moea.gov.tw/MNS/dos_e/home/Home.aspx"),
                     ("A4_moea_bulletin", "https://www.moea.gov.tw/Mns/dos_e/bulletin/Bulletin.aspx?kind=8"),
                     ("A5_nstatdb", "https://nstatdb.dgbas.gov.tw/dgbasAll/webMain.aspx?sys=220"),
                     ("A6_stat_eng", "https://eng.stat.gov.tw/")):
        st, raw = fetch(url, 15, insecure=True)
        txt = raw.decode("utf-8", "replace") if st == 200 else ""
        note(tag, {"status": st, "len": (len(raw) if st == 200 else None),
                   "orders_str": ("export order" in txt.lower() or "外銷訂單" in txt),
                   "err": (None if st == 200 else raw.decode()[:120])})
    for tag, q3 in (("A7_dbn_taipei", "chinese taipei orders"),
                    ("A8_dbn_tw_manuf", "taiwan manufacturing new orders")):
        j3, e3 = jget("https://api.db.nomics.world/v22/search?limit=6&q=" + urllib.parse.quote(q3))
        docs = ((j3 or {}).get("results") or {}).get("docs") or []
        note(tag, {"err": e3, "hits": [{"p": d.get("provider_code"), "c": d.get("code"),
                                        "n": (d.get("name") or "")[:70]} for d in docs[:5]]})

    # ── B) PBoC MONTHLY TSF via NBS easyquery MONTHLY (hgyd) tree walk ──
    def tree(node):
        u = ("https://data.stats.gov.cn/english/easyquery.htm?m=getTree&dbcode=hgyd"
             f"&wdcode=zb&id={urllib.parse.quote(node)}")
        j4, e4 = jget(u, timeout=18, insecure=True)
        return (j4 if isinstance(j4, list) else []), e4

    root, err = tree("zb")
    note("B1_nbs_hgyd_root", {"err": err, "n": len(root),
                              "cats": [{"id": r0.get("id"), "name": (r0.get("name") or "")[:46]}
                                       for r0 in root[:14]]})
    finance_hits = []
    if root:
        # walk branches whose name smells financial, else walk all shallowly
        cands = [r0 for r0 in root if any(k in (r0.get("name") or "").lower()
                 for k in ("financ", "money", "credit", "bank"))] or root
        for r0 in cands[:4]:
            time.sleep(0.4)
            kids, _ = tree(r0.get("id") or "")
            for k0 in kids:
                nm = (k0.get("name") or "")
                if any(t in nm.lower() for t in ("aggregate financing", "social financing")):
                    finance_hits.append({"id": k0.get("id"), "name": nm[:80], "parent": r0.get("id")})
                if k0.get("isParent") and any(t in nm.lower() for t in ("financ", "social")):
                    time.sleep(0.4)
                    g, _ = tree(k0.get("id") or "")
                    for g0 in g:
                        if any(t in (g0.get("name") or "").lower()
                               for t in ("aggregate financing", "social financing")):
                            finance_hits.append({"id": g0.get("id"), "name": (g0.get("name") or "")[:80],
                                                 "parent": k0.get("id")})
    note("B2_nbs_financing_nodes", {"hits": finance_hits[:10]})
    sample = None
    if finance_hits:
        vc = finance_hits[0]["id"]
        dfw = urllib.parse.quote(json.dumps([{"wdcode": "zb", "valuecode": vc}]))
        u = ("https://data.stats.gov.cn/english/easyquery.htm?m=QueryData&dbcode=hgyd&rowcode=zb"
             f"&colcode=sj&wds=%5B%5D&dfwds={dfw}&k1={int(time.time()*1000)}")
        j5, e5 = jget(u, timeout=20, insecure=True)
        dn = (((j5 or {}).get("returndata") or {}).get("datanodes") or [])[:6]
        sample = {"valuecode": vc, "err": e5,
                  "nodes": [{"code": d0.get("code"), "v": (d0.get("data") or {}).get("data")}
                            for d0 in dn]}
    note("B3_nbs_sample_query", sample or {"skipped": "no financing node found"})
    st, raw = fetch("http://www.pbc.gov.cn/en/3688006/index.html", 15)
    note("B4_pboc_en_reach", {"status": st,
                              "afre_str": (st == 200 and b"ggregate" in raw),
                              "err": (None if st == 200 else raw.decode()[:120])})
    j6, e6 = jget("https://api.db.nomics.world/v22/providers")
    provs = [p.get("code") for p in ((j6 or {}).get("providers") or {}).get("docs", [])
             if any(k in (p.get("name") or "").lower() for k in ("china", "people's bank", "pboc"))]
    note("B5_dbn_cn_providers", {"err": e6, "matches": provs[:8]})

    out["verdict"] = "PROBE_COMPLETE"
    print("\nVERDICT: PROBE_COMPLETE"); rep.log("VERDICT: PROBE_COMPLETE")
    Path("aws/ops/reports/3585.json").write_text(json.dumps(out, indent=2, ensure_ascii=False, default=str))
    sys.exit(0)
