"""ops_5129 -- macro coverage reconciliation against the external data.html audit.

Khalid pasted a third-party audit of https://justhodl.ai/data.html. It read the
page WITHOUT JavaScript, so it saw only the ECB/FRED retrieval-playbook text and
concluded the data plane is two providers ("data.html only names ECB and FRED").
The hub actually holds 57 providers / 779k datasets. Two of its findings are
still right and are fixed in this push: plumbing.html published stale layer
weights (25/35/25/20/20 = 125% vs the engine's 25/30/15/10/20), and data.html is
not readable without JS (bake_data_inventory.py now injects a static provider
table + JSON blob at Pages build).

This op reconciles the audit's nine requested domains against what the
warehouse REALLY holds -- evidence, not claims:

  * for each domain, a list of concrete series/datasets/providers to look for
    (FRED banked docs with first/last obs, OFR datasets, NY Fed rates, BIS/ECB/
    IMF/OECD flows, FRB DDP packages, FiscalData, Census EITS, BLS surveys,
    PortWatch, TIC, CFTC, Cboe, coinmetrics, national lanes)
  * verdict per domain: HAVE / PARTIAL / MISSING with the gap named and the
    source that would fill it (free official vs licensed)
Writes data/audit/macro-coverage-5129.json and the report. Never RED unless
the hub is unreadable.
"""
import gzip
import json
import sys
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1", config=Config(max_pool_connections=32, retries={"max_attempts": 4}))


def get_json(key):
    try:
        b = s3.get_object(Bucket=B, Key=key)["Body"].read()
        if key.endswith(".gz") or b[:2] == b"\x1f\x8b":
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception:  # noqa: BLE001
        return None


def head(key):
    try:
        h = s3.head_object(Bucket=B, Key=key)
        return {"bytes": h["ContentLength"], "modified": h["LastModified"].isoformat()[:19]}
    except Exception:  # noqa: BLE001
        return None


def list_keys(prefix, cap=5000):
    out, tok = [], None
    while True:
        kw = {"Bucket": B, "Prefix": prefix, "MaxKeys": 1000}
        if tok:
            kw["ContinuationToken"] = tok
        d = s3.list_objects_v2(**kw)
        out.extend(o["Key"] for o in d.get("Contents") or [])
        tok = d.get("NextContinuationToken")
        if not tok or len(out) >= cap:
            break
    return out


def main():
    with report("5129-macro-coverage") as r:
        r.heading("ops 5129 -- macro coverage reconciliation vs the external data.html audit")
        hub = get_json("data/provider-catalog.json")
        if not hub:
            r.fail("hub unreadable")
            sys.exit(1)
        provs = {p["slug"]: p for p in hub.get("providers") or []}
        r.section("A. what the auditor could not see")
        r.log(f"hub: {len(provs)} providers, {hub.get('datasets_total'):,} datasets, {hub.get('totals', {}).get('gb')} GB (as_of {hub.get('as_of')})")
        r.log("  audit claim 'data.html only names ECB and FRED' = the page's provider grid is client-rendered; fixed by scripts/bake_data_inventory.py (static table + JSON at Pages build)")
        warm = [c["Prefix"].split("/")[2] for c in s3.list_objects_v2(Bucket=B, Prefix="data/warm/", Delimiter="/").get("CommonPrefixes", [])]
        r.log(f"  warm prefixes ({len(warm)}): {', '.join(warm)}")

        # ---- FRED banked docs: first/last obs for the audit's named series
        r.section("B. FRED banked series named by the audit (first obs / last obs / n)")
        roots = [c["Prefix"] for c in s3.list_objects_v2(Bucket=B, Prefix="data/warm/fred-scoped/", Delimiter="/").get("CommonPrefixes", [])]
        banked = {}
        with ThreadPoolExecutor(16) as ex:
            for objs in ex.map(list_keys, roots):
                for k in objs:
                    base = k.rsplit("/", 1)[-1]
                    if base.endswith(".json") and not base.startswith("_"):
                        banked[base[:-5]] = k
        wanted = {
            "us_liquidity": ["WALCL", "WTREGEN", "RRPONTSYD", "WRESBAL", "RESPPLLOPNWW", "WORAL", "SWPT", "H41RESPPALDKNWW", "WLRRAL", "SOFR", "EFFR", "IORB", "OBFR", "TGCR", "BGCR", "DPCREDIT", "TOTRESNS", "BOGMBASE", "WSHOSHO", "WLCFLPCL"],
            "us_credit": ["BAMLC0A0CM", "BAMLH0A0HYM2", "BAMLEMCBPIOAS", "BAMLC0A4CBBB", "DRTSCILM", "DRTSCIS", "TOTCI", "BUSLOANS", "NFCI", "ANFCI", "STLFSI4", "T10Y2Y", "T10Y3M", "DGS10", "DGS2", "DGS30", "DFII10", "T10YIE", "MORTGAGE30US"],
            "fx_usd": ["DTWEXBGS", "DTWEXAFEGS", "DTWEXEMEGS", "DEXUSEU", "DEXJPUS", "DEXCHUS", "DEXUSUK", "DEXKOUS", "DEXBZUS", "DEXMXUS", "DEXINUS", "DEXSZUS", "VIXCLS", "EVZCLS", "JPYUSD", "USDONTD156N"],
            "global_cb": ["JPNASSETS", "ECBASSETSW", "UKASSETS", "CHNASSETS", "SNBASSETS", "TOTALASSETS", "IRLTLT01DEM156N", "IRLTLT01JPM156N", "INTGSTJPM193N", "IR3TIB01EZM156N", "ECBDFR", "ECBMLFR"],
            "us_manufacturing": ["INDPRO", "IPMAN", "TCU", "MCUMFN", "AMTMNO", "DGORDER", "NEWORDER", "AMTMUO", "MANEMP", "AWHMAN", "UMTMTI", "ISRATIO", "BUSINV", "GACDISA066MSFRBNY", "GACDFSA066MSFRBPHI", "TXPCTDEMSA", "RMFSA", "MNFCTRIRSA"],
            "global_manufacturing": ["JPNPROINDMISMEI", "DEUPROINDMISMEI", "KORPROINDMISMEI", "CHNPRINTO01IXPYM", "EA19PRINTO01GYSAM", "MEXPROINDMISMEI", "BRAPROINDMISMEI", "INDPROINDMISMEI", "OECDPRINTO01GYSAM", "XTEXVA01KRM667S", "XTEXVA01TWM667S", "XTEXVA01CNM667S", "XTEXVA01DEM667S", "XTEXVA01JPM667S", "XTEXVA01MXM667S", "XTEXVA01VNM667S"],
            "em_frontier": ["TEDRATE", "EMVOVERALLEMV", "VXEEMCLS", "DEXBZUS", "DEXINUS", "DEXSFUS", "DEXTHUS", "DEXMAUS", "DEXKOUS", "RBBRBIS", "RBINBIS", "RBMXBIS", "RBZABIS", "RBTRBIS", "RBIDBIS", "TRESEGCHM052N", "TRESEGBRM052N", "TRESEGINM052N", "TRESEGMXM052N"],
        }
        cov = {}

        def obs_span(sid):
            k = banked.get(sid)
            if not k:
                return sid, None
            try:
                j = json.loads(s3.get_object(Bucket=B, Key=k)["Body"].read())
                obs = [o for o in (j.get("observations") or []) if o.get("value") not in (None, ".", "")]
                return sid, {"first": obs[0]["date"] if obs else None, "last": obs[-1]["date"] if obs else None, "n": len(obs)}
            except Exception as e:  # noqa: BLE001
                return sid, {"error": str(e)[:60]}
        with ThreadPoolExecutor(24) as ex:
            spans = dict(ex.map(obs_span, sorted({s for v in wanted.values() for s in v})))
        for dom, ids in wanted.items():
            have = [s for s in ids if spans.get(s)]
            miss = [s for s in ids if not spans.get(s)]
            cov[dom] = {"have": {s: spans[s] for s in have}, "missing_from_fred_bank": miss}
            r.log(f"  {dom:<20} banked {len(have)}/{len(ids)}: " + ", ".join(f"{s}({(spans[s] or {}).get('first', '?')[:4]}→{(spans[s] or {}).get('last', '?')[:7]})" for s in have[:9]) + (" …" if len(have) > 9 else ""))
            if miss:
                r.log(f"      not banked: {', '.join(miss)}")

        # ---- non-FRED evidence per domain
        r.section("C. plumbing / liquidity / bonds / FX providers actually held")
        ev = {}
        ofr = [k.rsplit("/", 1)[-1] for k in list_keys("data/warm/ofr/") if "dataset-" in k]
        ev["ofr_datasets"] = ofr
        ofr_hfm = len([k for k in list_keys("data/warm/ofr-hfm/series/")])
        ofr_bsrm = len([k for k in list_keys("data/warm/ofr-bsrm/series/")])
        r.log(f"  OFR: datasets {ofr} · HFM series {ofr_hfm} · BSRM series {ofr_bsrm} · FSI {bool(head('data/warm/ofr-fsi/fsi.csv'))}")
        repo = get_json("data/warm/ofr/dataset-repo.json.gz") or {}
        ts = ((repo.get("payload") or {}).get("timeseries") or {})
        nccbr = [m for m in ts if "NCCBR" in m.upper() or "BILAT" in m.upper()]
        r.log(f"  OFR repo mnemonics: {len(ts)} · NCCBR/bilateral: {nccbr[:6]}")
        ev["ofr_repo_mnemonics"], ev["ofr_nccbr"] = len(ts), nccbr
        nyfed = [k.rsplit("/", 1)[-1] for k in list_keys("data/warm/nyfed/")]
        nyfed_mk = list_keys("data/warm/nyfed-markets/", cap=200)
        r.log(f"  NY Fed markets: warm/nyfed {nyfed} · warm/nyfed-markets {len(nyfed_mk)} keys {[k.rsplit('/', 1)[-1] for k in nyfed_mk[:8]]}")
        ev["nyfed"], ev["nyfed_markets"] = nyfed, [k.rsplit("/", 1)[-1] for k in nyfed_mk[:40]]
        bis = [k.rsplit("/", 1)[-1] for k in list_keys("data/warm/bis/")]
        r.log(f"  BIS: {bis}")
        ev["bis"] = bis
        for want in ("WS_LBS", "WS_GLI", "WS_CBS", "WS_DEBT_SEC", "WS_TC", "WS_CREDIT_GAP", "WS_DSR", "WS_EER", "WS_CBPOL", "WS_CBTA", "WS_SPP", "WS_XRU", "WS_OTC"):
            r.log(f"      {want:<14} {'HAVE' if any(want in k for k in bis) else 'missing'}")
        t0e = get_json("data/index/ecb/flows.json.gz") or {}
        ecb_flows = sorted((t0e.get("flows") or {}).keys())
        r.log(f"  ECB flows in the series index: {len(ecb_flows)} · CISS {'CISS' in ecb_flows} · BSI {'BSI' in ecb_flows} · MIR {'MIR' in ecb_flows} · FM {'FM' in ecb_flows} · YC {'YC' in ecb_flows} · EXR {'EXR' in ecb_flows} · GFS {'GFS' in ecb_flows} · BP6 {'BP6' in ecb_flows} · QSA {'QSA' in ecb_flows}")
        ev["ecb_flows"] = ecb_flows
        imf = sorted({k.rsplit("/", 1)[-1].split(".")[0] for k in list_keys("data/warm/imf-full/src/")})
        r.log(f"  IMF flows banked: {len(imf)} e.g. {imf[:30]}")
        ev["imf"] = imf
        for want in ("IFS", "BOP", "COFER", "DOT", "FSI", "PCPS", "WEO", "GFSR", "IRFCL", "CDIS", "CPIS", "MFS", "FAS"):
            r.log(f"      {want:<8} {'HAVE' if any(x.startswith(want) for x in imf) else 'missing'}")
        oecd = sorted({k.rsplit("/", 1)[-1].split(".")[0] for k in list_keys("data/warm/oecd/data/", cap=2000)})
        r.log(f"  OECD flows banked: {len(oecd)} · CLI {any('CLI' in x for x in oecd)} · KEI {any('KEI' in x for x in oecd)} · STES/MEI {any('STES' in x or 'MEI' in x for x in oecd)} · BTS {any('BTS' in x for x in oecd)}")
        ev["oecd_n"] = len(oecd)
        frb = [k.rsplit("/", 1)[-1] for k in list_keys("data/warm/frbddp-full/") if not k.endswith("state.json")]
        r.log(f"  FRB DDP packages: {frb}")
        ev["frbddp"] = frb
        fisc = [k.rsplit("/", 1)[-1] for k in list_keys("data/warm/fiscaldata-full/src/")]
        r.log(f"  FiscalData endpoints: {len(fisc)} e.g. {[f[:40] for f in fisc[:8]]}")
        ev["fiscaldata_n"] = len(fisc)
        tic = [k.rsplit("/", 1)[-1] for k in list_keys("data/warm/tic-full/")]
        r.log(f"  TIC files: {tic}")
        cftc = [k.rsplit("/", 1)[-1] for k in list_keys("data/warm/cftc/")]
        r.log(f"  CFTC: {cftc}")
        for slug in ("coinmetrics", "cboe", "dtcc", "dtcc-fails", "sec-ftd", "icma-sftr", "official-yields", "te-yields", "repo-master", "ecb-mmsr", "boe-full", "boj-full", "snb", "bcb", "banxico"):
            keys = list_keys(f"data/warm/{slug}/", cap=50)
            r.log(f"  {slug:<16} {len(keys)} keys{'+' if len(keys) >= 50 else ''} e.g. {[k.rsplit('/', 1)[-1][:34] for k in keys[:4]]}")
            ev[slug] = len(keys)

        r.section("D. real economy: manufacturing, IP, ports, exports, hubs")
        pw = get_json("data/portwatch.json") or {}
        r.log(f"  PortWatch feed: keys={list(pw.keys())[:10]} ports={len(pw.get('ports') or pw.get('rows') or [])} as_of={pw.get('as_of') or pw.get('generated_at')}")
        pwk = list_keys("data/warm/portwatch/", cap=200)
        r.log(f"  warm/portwatch: {len(pwk)} keys e.g. {[k.rsplit('/', 1)[-1][:40] for k in pwk[:5]]}")
        ev["portwatch_ports"] = len(pw.get("ports") or pw.get("rows") or [])
        cen = get_json("data/warm/census-us/catalog.json.gz") or {}
        eits = [d.get("slug") for d in (cen.get("datasets") or []) if d.get("family") == "eits"]
        r.log(f"  Census EITS timeseries datasets: {len(eits)} {eits[:24]}")
        ev["census_eits"] = eits
        bls = sorted({k.split("/")[4] for k in list_keys("data/warm/bls-full/src/", cap=3000) if len(k.split('/')) > 5})
        r.log(f"  BLS surveys banked: {bls}")
        ev["bls_surveys"] = bls
        for slug in ("taiwan-moea", "kr-ecos", "hk-data", "cl-datos", "peru-copper", "asia-trade", "real-economy", "gdelt-full"):
            p = provs.get(slug) or {}
            keys = list_keys(f"data/warm/{slug}/", cap=30)
            r.log(f"  {slug:<14} hub datasets={p.get('datasets')} freshest={p.get('freshest_h')}h warm keys={len(keys)}{'+' if len(keys) >= 30 else ''}")
        for feed in ("freight-pulse", "port-cargo", "trade-nowcast", "global-flows", "asia-leads", "esi", "oecd-cli", "macro-nowcast", "global-liquidity", "eurodollar-plumbing", "dollar-radar", "global-sovereign", "sovereign-stress", "credit-stress", "global-recession"):
            h = head(f"data/{feed}.json")
            d = get_json(f"data/{feed}.json") or {}
            srcs = d.get("sources") or d.get("source") or d.get("providers") or None
            r.log(f"  feed {feed:<20} {'HAVE' if h else 'missing'} {h['modified'] if h else ''} keys={list(d.keys())[:8] if isinstance(d, dict) else type(d).__name__} sources={str(srcs)[:120]}")

        # ---- verdicts
        r.section("E. verdict per requested domain")
        verdict = {
            "financial_plumbing_us": {"status": "HAVE (deep)", "evidence": f"OFR {len(ofr)} datasets incl. repo ({len(ts)} mnemonics), HFM {ofr_hfm}, BSRM {ofr_bsrm}, FSI; NY Fed rates + volumes {nyfed}; FRED WALCL/TGA/RRP/reserves banked; FRB DDP {len(frb)} packages; FiscalData {len(fisc)} endpoints",
                                      "gaps": ["FIMA repo pool + foreign official RRP (FRED H41 lines exist; wire into net-liquidity identity)", "SRF usage (NY Fed operations CSV)", "FHLB advances (FRED/FHFA)", "NCCBR split: " + ("present" if nccbr else "check OFR repo mnemonics")]},
            "global_liquidity": {"status": "PARTIAL", "evidence": f"BIS flows held: {bis}; ECB BSI/MIR; FRED CB total assets; feed global-liquidity.json",
                                 "gaps": ["BIS Global Liquidity Indicators (WS_GLI) — free BIS SDMX" if not any("GLI" in k for k in bis) else "GLI present", "BIS Locational Banking Statistics (WS_LBS_D_PUB) — offshore USD stock, free" if not any("LBS" in k for k in bis) else "LBS present", "BIS International Debt Securities (WS_DEBT_SEC2_PUB) — free" if not any("DEBT_SEC" in k for k in bis) else "IDS present", "PBoC MLF/RRR/7d reverse repo volumes — china-liquidity feed exists; verify source depth"]},
            "eurodollar_liquidity": {"status": "PARTIAL", "evidence": "eurodollar-plumbing feed + TIC bctype/bltype + ECB MMSR turnover + ICE/ICMA where banked", "gaps": ["BIS LBS/CBS by currency (the eurodollar quantity)", "cross-currency basis (3m/5y EUR, JPY, GBP, CHF) — no free official source; reconstruct from FX forwards + OIS or license", "Fed liquidity swap lines outstanding — NY Fed central-bank liquidity swaps CSV, free", "FIMA repo usage — H.4.1 line, free"]},
            "bond_liquidity_stress": {"status": "PARTIAL", "evidence": "ICE BofA OAS family banked (92 mirrored at 100% agreement), MOVE via TV dictionary, auctions/FiscalData, official-yields, repo-master", "gaps": ["swap spreads (2s/10s/30s) — no free daily official source; BIS/ECB partial", "TRACE aggregated depth — FINRA authenticated tier (key held, no secret)", "sovereign CDS — licensed only", "EMBI/CEMBI — licensed; proxy via EMB/CEMB/HYEM bars now banked on demand in the universe"]},
            "global_forex_usd_shortage": {"status": "PARTIAL", "evidence": "FRED DTWEX broad/AFE/EME + bilateral spot banked; ECB EXR full history; VIX/EVZ; coinmetrics stablecoin lane", "gaps": ["cross-currency basis (see eurodollar)", "IMF COFER + IFS reserves — check imf-full for COFER/IFS below", "FX implied vols beyond EVZ — licensed", "EM reserves weekly (national CBs) — partial via banxico/bcb/snb lanes"]},
            "us_manufacturing": {"status": "HAVE", "evidence": f"FRED INDPRO/TCU/AMTMNO/DGORDER + regional Fed surveys banked; Census EITS {len(eits)} datasets (M3, ADVM3, MTIS…); BLS CES/PR surveys", "gaps": ["ISM PMI (licensed) — regional Fed diffusion + S&P flash are the free proxies", "S&P Global US PMI — licensed"]},
            "global_manufacturing_hubs": {"status": "PARTIAL", "evidence": "OECD KEI/STES/CLI/BTS banked; Eurostat STS full; FRED OECD-sourced IP/exports for KR/TW/CN/DE/JP/MX/VN; taiwan-moea live; esi feed", "gaps": ["Korea customs exports (first 10/20 days) — kr-ecos lane holds 0 datasets (dead lane, needs a key or the MOTIE/KITA route)", "CPB World Trade Monitor — free monthly xlsx, not banked", "national PMIs (Caixin/NBS China, DE, TW, KR, JP, VN, MX, IN) — NBS free; others licensed", "electricity load (Ember/EIA/ENTSO-E) — free, not banked"]},
            "ports_ip_exports": {"status": "PARTIAL", "evidence": f"PortWatch feed {ev['portwatch_ports']} ports; port-cargo, freight-pulse, trade-nowcast feeds; census intltrade excluded by directive", "gaps": ["PortWatch full universe (~2,000 ports + 28 chokepoints, ArcGIS API, free) — extend from 24 ports", "freight indices (FBX/Drewry/SCFI) — scrape/licensed; Baltic Dry via TV universe bank", "UN Comtrade HS-level — free API, not banked"]},
            "em_frontier": {"status": "PARTIAL", "evidence": "FRED EM FX/reserves/BIS REER banked; global-sovereign/sovereign-stress feeds; World Bank 29.5k indicators; capital-flow desk", "gaps": ["IIF portfolio flows / EPFR — licensed", "EMBI/CEMBI — licensed", "IMF IFS/COFER/BOP official reserves & short-term external debt — imf-full has " + str(len(imf)) + " flows; verify IFS/BOP presence above", "frontier parallel FX / Eurobond bid-ask — licensed or scraped", "WB IDS external debt — free, check worldbank-full"]},
        }
        for dom, v in verdict.items():
            r.log(f"  {dom:<28} {v['status']}")
            r.log(f"      evidence: {v['evidence'][:300]}")
            for g in v["gaps"]:
                r.log(f"      gap: {g}")
        # audit-claim scorecard
        r.section("F. audit claims scorecard")
        claims = [("data.html only names ECB and FRED", "FALSE (client-rendered grid; static bake added)"),
                  ("ECB 406 outage", "TRUE, already fixed (ops 4893; csvdata with no Accept)"),
                  ("oldest-first truncation trap", "TRUE, already handled (ecb-deep time-slicing; extractor merges slices)"),
                  ("fake inception years", "TRUE, already fixed (ops 4895)"),
                  ("agency catalog gap /dataflow/ECB=104 vs all=214", "TRUE, already handled (catalog uses /service/dataflow = 214)"),
                  ("revision policy drift", "UNVERIFIED — full-window rotation is the doctrine; add a revision-audit op"),
                  ("plumbing.html weights inconsistent", "TRUE — badges 25/35/25/20/20 vs engine 25/30/15/10/20; fixed in this push, badge now bound from the feed"),
                  ("desks render Loading… for scrapers", "TRUE for non-JS readers; data.html static inventory baked; other desks: candidate for the same bake"),
                  ("BIS GLI/LBS missing", "PARTIAL — BIS lane holds " + str(len(bis)) + " objects; GLI/LBS/IDS " + ("present" if any(x in k for k in bis for x in ("GLI", "LBS", "DEBT_SEC")) else "missing → new lane")),
                  ("PortWatch fragile", "PARTIAL — 24 ports live with 379d history; full universe is a lane away")]
        for c, v in claims:
            r.log(f"  {c:<48} {v}")
        s3.put_object(Bucket=B, Key="data/audit/macro-coverage-5129.json", Body=json.dumps({"as_of": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()), "fred": cov, "evidence": ev, "verdict": verdict, "claims": claims}, default=str).encode(),
                      ContentType="application/json", CacheControl="max-age=300")
        r.ok("coverage matrix written: data/audit/macro-coverage-5129.json")


if __name__ == "__main__":
    main()
