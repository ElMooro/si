"""ops 3582 — SOURCE PROBE (no engine yet, gate-on-DATA): the MacroMicro gap
analysis resolved to 4 candidate additions, all free-primary. Probe each from
the runner and record endpoint + latest value so 3583+ builds only on proven
feeds: (A) Taiwan export orders (DBnomics/FRED search), (B) Korea exports incl.
20-day flash reachability, (C) China TSF / aggregate financing, (D) FRED
releases/dates calendar. Never fails the workflow — findings in the report."""
import json, sys, time, urllib.parse, urllib.request
from pathlib import Path
from ops_report import report

FRED = "2f057499936072679d8843d7fce99989"
UA = {"User-Agent": "JustHodl research contact@justhodl.ai", "Accept": "application/json"}

def gj(url, timeout=25):
    try:
        raw = urllib.request.urlopen(urllib.request.Request(url, headers=UA), timeout=timeout).read()
        return json.loads(raw), None
    except Exception as e:
        return None, str(e)[:140]

def dbn_search(q, limit=6):
    j, e = gj("https://api.db.nomics.world/v22/search?limit=%d&q=%s" % (limit, urllib.parse.quote(q)))
    if not j:
        return [], e
    docs = ((j.get("results") or {}).get("docs")) or j.get("docs") or []
    out = []
    for d in docs[:limit]:
        out.append({"provider": d.get("provider_code"), "dataset": d.get("code") or d.get("dataset_code"),
                    "name": (d.get("name") or "")[:90], "nb": d.get("nb_series")})
    return out, None

def dbn_series(provider, dataset, q, limit=3):
    j, e = gj("https://api.db.nomics.world/v22/series/%s/%s?limit=%d&observations=1&q=%s"
              % (provider, dataset, limit, urllib.parse.quote(q)))
    if not j:
        return [], e
    out = []
    for s0 in ((j.get("series") or {}).get("docs")) or []:
        per, val = (s0.get("period") or [])[-1:], (s0.get("value") or [])[-1:]
        out.append({"id": s0.get("series_code"), "name": (s0.get("series_name") or "")[:80],
                    "last": (per[0] if per else None, val[0] if val else None)})
    return out, None

def fred_search(text, limit=5):
    j, e = gj("https://api.stlouisfed.org/fred/series/search?search_text=%s&api_key=%s&file_type=json&limit=%d&order_by=popularity&sort_order=desc"
              % (urllib.parse.quote(text), FRED, limit))
    if not j:
        return [], e
    return [{"id": s0.get("id"), "title": (s0.get("title") or "")[:80], "freq": s0.get("frequency_short"),
             "last_updated": (s0.get("last_updated") or "")[:10], "end": s0.get("observation_end")}
            for s0 in j.get("seriess") or []], None

with report("3582_asia_leads_probe") as rep:
    rep.heading("ops 3582 — Asia leads + calendar source probe (MacroMicro gap analysis)")
    out = {"probes": {}}

    def note(name, payload):
        out["probes"][name] = payload
        line = f"{name}: {json.dumps(payload, default=str)[:420]}"
        print(line); rep.log(line)

    # A) Taiwan export orders
    hits, e = dbn_search("taiwan export orders")
    note("A1_dbnomics_search_tw_export_orders", {"err": e, "hits": hits})
    fr, e = fred_search("taiwan export orders")
    note("A2_fred_search_tw_export_orders", {"err": e, "hits": fr})
    fr2, e = fred_search("taiwan exports")
    note("A3_fred_search_tw_exports", {"err": e, "hits": fr2})
    # MOEA direct English endpoint reachability (statistics portal)
    j, e = gj("https://dmz26.moea.gov.tw/gmweb/investigate/InvestigateEA.aspx", timeout=20)
    note("A4_moea_direct_reach", {"reachable_json": bool(j), "err": e})

    # B) Korea exports + 20-day flash
    hits, e = dbn_search("korea customs exports")
    note("B1_dbnomics_search_kr_customs", {"err": e, "hits": hits})
    fr, e = fred_search("south korea exports")
    note("B2_fred_search_kr_exports", {"err": e, "hits": fr})
    j, e = gj("https://unipass.customs.go.kr/ets/index.do", timeout=20)
    note("B3_kr_customs_reach", {"reachable": j is not None, "err": e})

    # C) China TSF / aggregate financing / credit impulse
    hits, e = dbn_search("china aggregate financing")
    note("C1_dbnomics_search_cn_tsf", {"err": e, "hits": hits})
    hits2, e = dbn_search("china social financing")
    note("C2_dbnomics_search_cn_social", {"err": e, "hits": hits2})
    fr, e = fred_search("china total social financing")
    note("C3_fred_search_cn_tsf", {"err": e, "hits": fr})
    fr2, e = fred_search("china loans")
    note("C4_fred_search_cn_loans", {"err": e, "hits": fr2})

    # D) FRED releases/dates calendar (upcoming prints)
    j, e = gj("https://api.stlouisfed.org/fred/releases/dates?api_key=%s&file_type=json&include_release_dates_with_no_data=true&sort_order=asc&realtime_start=2026-07-20&realtime_end=2026-08-05&limit=60" % FRED)
    upcoming = [{"date": r.get("date"), "release": (r.get("release_name") or "")[:60]}
                for r in (j or {}).get("release_dates") or []][:25]
    note("D1_fred_releases_dates", {"err": e, "n": len(upcoming), "sample": upcoming[:12]})

    # E) drill the best Taiwan/Korea/China dataset if search surfaced one
    for tag, srch in (("E1_tw_drill", out["probes"]["A1_dbnomics_search_tw_export_orders"]["hits"]),
                      ("E2_kr_drill", out["probes"]["B1_dbnomics_search_kr_customs"]["hits"]),
                      ("E3_cn_drill", (out["probes"]["C1_dbnomics_search_cn_tsf"]["hits"]
                                       or out["probes"]["C2_dbnomics_search_cn_social"]["hits"]))):
        if srch:
            h0 = srch[0]
            ser, e = dbn_series(h0["provider"], h0["dataset"],
                                "export orders" if tag == "E1_tw_drill" else
                                "exports" if tag == "E2_kr_drill" else "financing")
            note(tag, {"drilled": f"{h0['provider']}/{h0['dataset']}", "err": e, "series": ser})
        else:
            note(tag, {"skipped": "no search hits"})

    out["verdict"] = "PROBE_COMPLETE"
    print("\nVERDICT: PROBE_COMPLETE"); rep.log("VERDICT: PROBE_COMPLETE")
    Path("aws/ops/reports/3582.json").write_text(json.dumps(out, indent=2, default=str))
    sys.exit(0)
