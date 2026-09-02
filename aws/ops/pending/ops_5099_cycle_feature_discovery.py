"""ops_5099 -- global-cycle v3 feature discovery: what the warehouses actually
hold per country, profiled from the files (read-only).

ops 5098 established the fleet map; its OECD flow matching compared catalog
ids against file names WITH the .dat.gz extension, so it reported 0 walked
cycle flows and mistook DF_CLIM (climate) for DF_CLI. This op profiles the
real candidates the way the v3 engine will read them:

  S1  OECD warehouse: every walked flow id (+size, age, truncated ledger);
      profile DSD_STES@DF_CLI (measures LI/BCICP/CCICP), production
      (DF_PRODUCTION / DSD_STES@DF_PROD*), labour (DSD_STES@DF_STLABOUR /
      STLABOUR), finance (DSD_STES@DF_FIN / MEI_FIN), trade (DF_TRADE),
      key economic indicators (DSD_KEI@DF_KEI) -> dimensions, areas, latest
      period per area, rows
  S2  BIS: WS_TC, WS_CREDIT_GAP, WS_SPP, WS_DPP, WS_CBPOL, WS_EER, WS_DSR,
      WS_LONG_CPI -> dimensions, areas, latest periods
  S3  IMF banked flows (state.have) and which cycle flows exist (IFS, DOT,
      PCPS, CPI, WEO, FSI, MFS, BOP); World Bank cycle indicators presence
  S4  Eurostat: cycle flow ids present in Tier-0 (EI_BSSI/EI_BSCO/EI_BSIN/
      EI_ISIN/STS_INPR/EI_LMHR/UNE_RT/EI_ISRT/EI_ETEU27/EXT_ST) + series
      counts; raw file presence under data/warm/eurostat/data
  S5  TE mirror symbols (PMIs?), FRED scoped: country PMI/IP/unemployment
      series present; national lanes (statcan, boj-full, boe-full,
      banxico, bcb, snb, hk-data, cl-datos, asia-trade, census-econ)
  S6  existing per-country feeds: global-macro (15), oecd-cli (as_of!),
      global-recession, global-sovereign, portwatch/port-cargo per-country
      coverage, asia-leads
  S7  MATRIX: 34 countries x candidate features -> data/audit/cycle-feature-
      matrix-5099.json (the v3 spec)
Gate: RED only if the OECD data prefix is unreadable.
"""
import csv
import gzip
import io
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name="us-east-1", config=Config(max_pool_connections=16))
NOW = datetime.now(timezone.utc)
ISO3 = ["USA", "CHN", "JPN", "DEU", "IND", "GBR", "FRA", "ITA", "CAN", "BRA", "KOR", "AUS", "ESP", "MEX", "IDN", "NLD",
        "TUR", "CHE", "POL", "BEL", "SWE", "IRL", "AUT", "NOR", "ZAF", "DNK", "FIN", "CZE", "HUN", "CHL", "PRT", "GRC",
        "NZL", "ISR"]
ISO2 = {"USA": "US", "CHN": "CN", "JPN": "JP", "DEU": "DE", "IND": "IN", "GBR": "GB", "FRA": "FR", "ITA": "IT", "CAN": "CA",
        "BRA": "BR", "KOR": "KR", "AUS": "AU", "ESP": "ES", "MEX": "MX", "IDN": "ID", "NLD": "NL", "TUR": "TR", "CHE": "CH",
        "POL": "PL", "BEL": "BE", "SWE": "SE", "IRL": "IE", "AUT": "AT", "NOR": "NO", "ZAF": "ZA", "DNK": "DK", "FIN": "FI",
        "CZE": "CZ", "HUN": "HU", "CHL": "CL", "PRT": "PT", "GRC": "EL", "NZL": "NZ", "ISR": "IL"}
MATRIX = {iso: {} for iso in ISO3}
OUT = {"generated_at": NOW.isoformat(timespec="seconds"), "ops": 5099}


def age_h(lm):
    return round((NOW - lm).total_seconds() / 3600, 1) if lm else None


def get_bytes(key, max_bytes=None):
    o = s3.get_object(Bucket=B, Key=key)
    body = o["Body"].read()
    if key.endswith(".gz") or body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return body, o["LastModified"]


def get_json(key):
    try:
        b, lm = get_bytes(key)
        return json.loads(b), lm, None
    except Exception as e:  # noqa: BLE001
        return None, None, str(e)[:100]


def list_prefix(prefix, delimiter=None):
    pag = s3.get_paginator("list_objects_v2")
    kw = {"Bucket": B, "Prefix": prefix}
    if delimiter:
        kw["Delimiter"] = delimiter
    objs, pre = [], []
    for page in pag.paginate(**kw):
        objs.extend(page.get("Contents") or [])
        pre.extend(p["Prefix"] for p in page.get("CommonPrefixes") or [])
    return objs, pre


def profile_sdmx_csv(body, area_col_candidates=("REF_AREA", "BORROWERS_CTY", "LOCATION", "REF_AREA_ISO3", "COUNTRY", "geo", "GEO"),
                     max_rows=3_000_000):
    """Generic SDMX-CSV profiler: dimensions (distinct values), areas, latest
    TIME_PERIOD per (area, measure-ish key), row count."""
    text = body.decode("utf-8", "replace")
    if text.startswith("\ufeff"):
        text = text[1:]
    rd = csv.reader(io.StringIO(text))
    try:
        hdr = next(rd)
    except StopIteration:
        return {"rows": 0, "error": "empty"}
    hdr = [h.strip() for h in hdr]
    col = {h: i for i, h in enumerate(hdr)}
    tp = col.get("TIME_PERIOD", col.get("TIME"))
    ov = col.get("OBS_VALUE", col.get("Value"))
    area_col = next((col[c] for c in area_col_candidates if c in col), None)
    # dimension-ish columns: everything before TIME_PERIOD except DATAFLOW/STRUCTURE*/ACTION and long text columns
    dim_cols = [h for h in hdr if h not in ("DATAFLOW", "STRUCTURE", "STRUCTURE_ID", "STRUCTURE_NAME", "ACTION", "TIME_PERIOD",
                                            "OBS_VALUE", "OBS_STATUS", "UNIT_MULT", "DECIMALS", "OBS_CONF", "TIME_FORMAT",
                                            "Time period", "Observation value") and not h.startswith("Observation")]
    distinct = {h: Counter() for h in dim_cols[:16]}
    latest = {}
    n = 0
    areas = Counter()
    measure_col = next((col[c] for c in ("MEASURE", "INDICATOR", "SUBJECT", "TC_BORROWERS", "DSR_BORROWERS", "CG_DTYPE", "VALUE", "EER_TYPE", "na_item", "indic", "indic_bt", "s_adj") if c in col), None)
    freq_col = col.get("FREQ", col.get("FREQUENCY", col.get("freq")))
    for row in rd:
        n += 1
        if n > max_rows:
            break
        if len(row) <= max(tp or 0, ov or 0):
            continue
        a = row[area_col] if area_col is not None and area_col < len(row) else "_"
        areas[a] += 1
        for h in distinct:
            ci = col[h]
            if ci < len(row) and len(distinct[h]) < 60:
                distinct[h][row[ci]] += 1
        if tp is not None:
            t = row[tp]
            m = row[measure_col] if measure_col is not None and measure_col < len(row) else "_"
            f = row[freq_col] if freq_col is not None and freq_col < len(row) else "_"
            k = (a, f"{m}/{f}")
            if t > latest.get(k, ""):
                latest[k] = t
    per_area_latest = defaultdict(dict)
    for (a, m), t in latest.items():
        per_area_latest[a][m] = t
    return {"rows": n, "header": hdr[:40], "n_areas": len(areas), "areas": sorted(areas)[:120],
            "dims": {h: [k for k, _ in c.most_common(40)] for h, c in distinct.items()},
            "latest_by_area": {a: v for a, v in list(per_area_latest.items())[:200]}}


def area_to_iso3(area):
    """OECD/BIS use ISO3 (or ISO2 for BIS); Eurostat uses ISO2 with EL/UK quirks."""
    a = (area or "").upper()
    if a in ISO3:
        return a
    for iso3, iso2 in ISO2.items():
        if a == iso2:
            return iso3
    if a == "UK":
        return "GBR"
    if a == "EL":
        return "GRC"
    return None


def mark(feature, iso3, latest, source, extra=None):
    if iso3 in MATRIX:
        MATRIX[iso3][feature] = {"latest": latest, "source": source, **(extra or {})}


def main():
    with report("5099-cycle-feature-discovery") as r:
        r.heading("ops 5099 -- global-cycle v3 feature discovery (warehouse-profiled)")
        r.log(f"started {NOW.isoformat(timespec='seconds')}")

        # ── S1 OECD ───────────────────────────────────────────────────────
        r.section("S1 OECD walked flows")
        objs, _ = list_prefix("data/warm/oecd/data/")
        if not objs:
            r.fail("OECD data prefix empty/unreadable")
            sys.exit(1)
        byid = {o["Key"].split("/")[-1].replace(".dat.gz", ""): o for o in objs}
        st, _, _ = get_json("data/warm/oecd/_state/state.json")
        trunc = set((st or {}).get("truncated") or [])
        fails = (st or {}).get("failures") or {}
        r.log(f"{len(byid)} walked flows; truncated ledger {len(trunc)}; failures {len(fails)}; state as_of {(st or {}).get('as_of')}")
        want = [k for k in byid if re.search(r"STES|CLI|BTS|KEI|MEI|PROD|STLABOUR|LABOUR|FIN|TRADE|QNA|PRICES|MONEY|IRS|IR_|"
                                             r"HOUSE|ORDER|RETAIL|CONF|SENT|EO_|EO\b|CAPACITY|ECOUT", k, re.I)]
        want.sort()
        r.log(f"cycle-candidate walked flows ({len(want)}): " + "; ".join(f"{k}({round(byid[k]['Size']/1e6,1)}MB{'/TRUNC' if k in trunc else ''})" for k in want[:120]))
        prof = {}
        targets = ["DSD_STES@DF_CLI", "DSD_KEI@DF_KEI", "DSD_STES@DF_BTS", "DSD_STES@DF_PROD", "DSD_STES@DF_PRODUCTION",
                   "DSD_STES@DF_STLABOUR", "DSD_STES@DF_FIN", "DSD_STES@DF_TRADE", "DSD_STES@DF_RETAIL",
                   "DSD_STES@DF_ORDERS", "DSD_LFS@DF_IALFS_UNE_M", "DSD_STES@DF_STES", "DSD_MEI@DF_MEI",
                   "DSD_PRICES@DF_PRICES_ALL", "DSD_EO@DF_EO", "DSD_STES@DF_MONEY", "DSD_KEI@DF_KEI_M"]
        # add any walked flow whose id contains these families
        fam = [k for k in byid if any(t in k for t in ("STES", "KEI", "STLABOUR", "IALFS", "MEI"))]
        for k in fam:
            if k not in targets:
                targets.append(k)
        for fid in targets:
            o = byid.get(fid)
            if not o:
                continue
            try:
                body, lm = get_bytes(o["Key"])
                p = profile_sdmx_csv(body)
                prof[fid] = {"size_mb": round(o["Size"] / 1e6, 2), "age_h": age_h(lm), "truncated": fid in trunc,
                             "rows": p.get("rows"), "n_areas": p.get("n_areas"), "dims": p.get("dims"), "header": p.get("header")}
                iso_latest = {}
                for a, mv in (p.get("latest_by_area") or {}).items():
                    i3 = area_to_iso3(a)
                    if i3:
                        iso_latest[i3] = mv
                prof[fid]["iso_latest"] = iso_latest
                r.log(f"  {fid}: {prof[fid]['size_mb']}MB rows={p.get('rows')} areas={p.get('n_areas')} trunc={fid in trunc} "
                      f"dims={json.dumps({k: v[:12] for k, v in (p.get('dims') or {}).items()})[:900]}")
                r.log(f"    our countries covered: {len(iso_latest)}/34 · sample latest: {json.dumps(dict(list(iso_latest.items())[:6]))[:600]}")
                for i3, mv in iso_latest.items():
                    for m, t in mv.items():
                        mark(f"oecd:{fid.split('@')[-1]}:{m}", i3, t, f"data/warm/oecd/data/{fid}.dat.gz")
            except Exception as e:  # noqa: BLE001
                r.warn(f"  {fid}: profile failed {str(e)[:140]}")
        OUT["oecd"] = {"walked": len(byid), "candidates": want, "profiles": prof}
        wst, _, e = get_json("data/_state/sdmx-walk-oecd.json")
        if wst:
            fl = wst.get("failures") or {}
            cli_f = {k: str(v)[:160] for k, v in fl.items() if re.search(r"STES|CLI|MEI|BTS", k, re.I)}
            done_stes = [d for d in (wst.get("done") or []) if re.search(r"STES|CLI", str(d), re.I)]
            r.log(f"walker state as_of={wst.get('as_of')} done={len(wst.get('done') or [])} failures={len(fl)} truncated={len(wst.get('truncated') or [])} "
                  f"lease_until={wst.get('lease_until')} · STES/CLI/MEI failures: {json.dumps(cli_f)[:1500]} · done STES: {done_stes[:10]}")
            OUT["oecd"]["walker_state"] = {"as_of": wst.get("as_of"), "n_done": len(wst.get("done") or []), "n_failures": len(fl), "stes_failures": cli_f}
        else:
            r.warn(f"walker state unreadable: {e}")
        # live probe of the CLI dataflow from the runner (small window) -- the source of truth for v3's survey pillar
        import urllib.request
        import urllib.error
        for label, url in (("DF_CLI 2015+", "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,4.1/.M.LI...AA...H?startPeriod=2015-01&format=csvfilewithlabels"),
                           ("DF_CLI all 2024+", "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,/all?startPeriod=2024-01&format=csvfile"),
                           ("DF_CLI 4.0 all 2024+", "https://sdmx.oecd.org/public/rest/data/OECD.SDD.STES,DSD_STES@DF_CLI,4.0/all?startPeriod=2024-01&format=csvfile")):
            try:
                req = urllib.request.Request(url, headers={"User-Agent": "JustHodl ops5099 (+https://justhodl.ai)", "Accept": "text/csv, application/vnd.sdmx.data+csv"})
                with urllib.request.urlopen(req, timeout=60) as resp:
                    body = resp.read()
                p = profile_sdmx_csv(body)
                iso_latest = {}
                for a, mv in (p.get("latest_by_area") or {}).items():
                    i3 = area_to_iso3(a)
                    if i3:
                        iso_latest[i3] = mv
                r.log(f"LIVE OECD {label}: HTTP 200 {len(body)} B rows={p.get('rows')} areas={p.get('n_areas')} ours={len(iso_latest)}/34 header={p.get('header')[:16]} "
                      f"dims={json.dumps({k: v[:12] for k, v in (p.get('dims') or {}).items()})[:900]}")
                r.log(f"  sample latest: {json.dumps(dict(list(iso_latest.items())[:8]))[:700]}")
                OUT["oecd"].setdefault("live_probe", {})[label] = {"bytes": len(body), "rows": p.get("rows"), "n_areas": p.get("n_areas"), "ours": len(iso_latest),
                                                                    "dims": p.get("dims"), "header": p.get("header"), "iso_latest": iso_latest}
                for i3, mv in iso_latest.items():
                    for m, tt in mv.items():
                        mark(f"oecd-live:DF_CLI:{m}", i3, tt, url)
                break
            except urllib.error.HTTPError as he:
                r.warn(f"LIVE OECD {label}: HTTP {he.code} {str(he.read()[:200])}")
            except Exception as ex:  # noqa: BLE001
                r.warn(f"LIVE OECD {label}: {str(ex)[:160]}")

        # ── S2 BIS ────────────────────────────────────────────────────────
        r.section("S2 BIS flows")
        bprof = {}
        for fid in ("WS_TC", "WS_CREDIT_GAP", "WS_SPP", "WS_DPP", "WS_CBPOL", "WS_EER", "WS_DSR", "WS_LONG_CPI", "WS_GLI", "WS_XRU"):
            key = f"data/warm/bis/data/{fid}.dat.gz"
            try:
                body, lm = get_bytes(key)
                p = profile_sdmx_csv(body)
                iso_latest = {}
                for a, mv in (p.get("latest_by_area") or {}).items():
                    i3 = area_to_iso3(a)
                    if i3:
                        iso_latest[i3] = mv
                bprof[fid] = {"size_mb": round(len(body) / 1e6, 2), "age_h": age_h(lm), "rows": p.get("rows"), "n_areas": p.get("n_areas"),
                              "dims": p.get("dims"), "header": p.get("header"), "iso_latest": iso_latest}
                r.log(f"  {fid}: rows={p.get('rows')} areas={p.get('n_areas')} ours={len(iso_latest)}/34 header={p.get('header')[:14]} "
                      f"dims={json.dumps({k: v[:10] for k, v in (p.get('dims') or {}).items()})[:700]}")
                r.log(f"    sample latest: {json.dumps(dict(list(iso_latest.items())[:5]))[:500]}")
                for i3, mv in iso_latest.items():
                    lat = max(mv.values()) if mv else None
                    mark(f"bis:{fid}", i3, lat, key, {"measures": len(mv)})
            except Exception as e:  # noqa: BLE001
                r.warn(f"  {fid}: {str(e)[:120]}")
        OUT["bis"] = bprof

        # ── S3 IMF + World Bank ───────────────────────────────────────────
        r.section("S3 IMF + World Bank")
        ist, _, e = get_json("data/warm/imf-full/_state/state.json")
        if ist:
            have = ist.get("have") or {}
            uni = ist.get("universe") or {}
            hk = list(have.keys()) if isinstance(have, dict) else list(have)
            r.log(f"IMF state phase={ist.get('phase')} n_banked={ist.get('n_banked')} have={len(hk)} universe={len(uni)} failures={len(ist.get('failures') or {})} as_of={ist.get('as_of')}")
            cyc = [k for k in hk if re.match(r"^(IFS|DOT|PCPS|CPI|WEO|FSI|MFS|BOP|IIP|GFS|FAS|COFER|QNEA|DIP|PCTOT|MFS_|IFS_|EER|ITS|WHDREO|APDREO|AFRREO|MCDREO|EUROREO)", str(k), re.I)]
            r.log(f"IMF cycle-relevant banked: {cyc[:60]}")
            src_objs, _ = list_prefix("data/warm/imf-full/src/", delimiter="/")
            sizes = {}
            for k in cyc[:12]:
                so, _ = list_prefix(f"data/warm/imf-full/src/{k}/")
                if so:
                    sizes[k] = {"objects": len(so), "mb": round(sum(o['Size'] for o in so) / 1e6, 1), "newest_age_h": age_h(max(o['LastModified'] for o in so)),
                                "sample": [o["Key"].split("/")[-1] for o in so[:3]]}
            r.log(f"IMF src sizes: {json.dumps(sizes)[:1500]}")
            OUT["imf"] = {"have": hk[:600], "cycle": cyc, "sizes": sizes, "state": {k: v for k, v in ist.items() if not isinstance(v, (dict, list))}}
        else:
            r.warn(f"IMF state: {e}")
        wb_objs, wb_pre = list_prefix("data/warm/worldbank-full/", delimiter="/")
        r.log(f"World Bank prefixes: {wb_pre[:10]}")
        wbcat = None
        for k in ("data/warm/worldbank-full/catalog.json", "data/warm/worldbank-full/catalog.json.gz", "data/warm/worldbank-full/indicators.json.gz",
                  "data/warm/worldbank-full/_state/catalog.json.gz"):
            wbcat, _, e = get_json(k)
            if wbcat:
                r.log(f"  WB catalog {k}: type={type(wbcat).__name__} n={len(wbcat)}")
                items = wbcat.get("indicators") if isinstance(wbcat, dict) else wbcat
                if isinstance(items, dict):
                    items = [{"id": a, "name": (b.get("name") if isinstance(b, dict) else b)} for a, b in items.items()]
                hits = [it for it in (items or []) if isinstance(it, dict) and re.search(r"(industrial production|manufactur|unemploy|exports|imports|gdp growth|credit|broad money|consumer price|business)", str(it.get("name") or ""), re.I)]
                r.log(f"  WB cycle-ish indicators: {len(hits)}; sample {[(h.get('id'), str(h.get('name'))[:50]) for h in hits[:15]]}")
                OUT["worldbank"] = {"n": len(items or []), "cycle_hits": [(h.get("id"), str(h.get("name"))[:80]) for h in hits[:200]]}
                break

        # ── S4 Eurostat ───────────────────────────────────────────────────
        r.section("S4 Eurostat cycle flows")
        t0, _, e = get_json("data/index/eurostat/flows.json.gz")
        efl = (t0 or {}).get("flows") or {}
        if isinstance(efl, list):
            efl = {(x.get("flow") or x.get("id")): x for x in efl}
        pats = r"^(EI_BSSI_M_R2|EI_BSCO_M|EI_BSIN_M_R2|EI_BSRT_M_R2|EI_BSSE_M_R2|EI_BSBU_M_R2|EI_BSFS_M|EI_ISIN_M|EI_ISRT_M|EI_ISBU_M|STS_INPR_M|STS_INTV_M|STS_TRTU_M|STS_COPR_M|STS_INPP_M|EI_LMHR_M|UNE_RT_M|EI_ETEU27_2020_M|EXT_ST_EU27_2020SITC|EI_BSPM_M|TEIBS010|TEIBS020|TEIIS010|TEILM020|EI_MFIR_M|EI_MFLT_M|IRT_ST_M|IRT_LT_MCBY_M|EI_LMHU_M|EI_NAMA_Q|NAMQ_10_GDP|STS_SEPR_M|EI_ISBR_M)"
        ehits = {k: v for k, v in efl.items() if re.match(pats, str(k), re.I)}
        r.log(f"Eurostat Tier-0 {len(efl)} flows; cycle flows present: {len(ehits)}: {json.dumps({k: (v.get('n_series') or v.get('series') or v.get('count')) for k, v in ehits.items()})[:1200]}")
        raw = {}
        for k in ehits:
            ro, _ = list_prefix(f"data/warm/eurostat/data/{k}")
            ro = [o for o in ro if o["Key"].split("/")[-1].lower().startswith(k.lower())]
            if ro:
                raw[k] = {"objects": len(ro), "mb": round(sum(o["Size"] for o in ro) / 1e6, 2), "age_h": age_h(max(o["LastModified"] for o in ro)),
                          "keys": [o["Key"].split("/")[-1] for o in ro[:3]]}
        r.log(f"Eurostat raw files for cycle flows: {json.dumps(raw)[:1500]}")
        # profile one small survey flow to learn the TSV layout
        for k in ("EI_BSCO_M", "EI_BSSI_M_R2", "EI_LMHR_M", "STS_INPR_M"):
            if k in raw:
                try:
                    key = f"data/warm/eurostat/data/{raw[k]['keys'][0]}"
                    body, lm = get_bytes(key)
                    text = body.decode("utf-8", "replace")
                    lines = text.split("\n")
                    r.log(f"  {k} layout ({len(body)} B, {len(lines)} lines): header={lines[0][:300]!r} row1={lines[1][:200]!r}")
                    hdr = lines[0].split("\t")
                    last_cols = hdr[-3:]
                    n_geo = len({ln.split("\t")[0].split(",")[-1] for ln in lines[1:] if ln})
                    r.log(f"    latest period columns: {last_cols} · distinct geo in first key col: {n_geo}")
                    OUT.setdefault("eurostat_layout", {})[k] = {"header_head": lines[0][:200], "latest_cols": last_cols, "n_geo": n_geo}
                    for ln in lines[1:]:
                        if not ln:
                            continue
                        keyf = ln.split("\t")[0]
                        geo = keyf.split(",")[-1]
                        i3 = area_to_iso3(geo)
                        if i3:
                            vals = ln.split("\t")[1:]
                            # latest non-missing
                            latest = None
                            for c, v in zip(reversed(hdr[1:]), reversed(vals)):
                                if v.strip() not in ("", ":", ": "):
                                    latest = c.strip()
                                    break
                            mark(f"eurostat:{k}", i3, latest, key, {"key": keyf[:60]})
                except Exception as e:  # noqa: BLE001
                    r.warn(f"  {k}: layout probe failed {str(e)[:120]}")
        OUT["eurostat"] = {"hits": {k: (v.get("n_series") or v.get("series") or v.get("count")) for k, v in ehits.items()}, "raw": raw}

        # ── S5 TE mirror / FRED scoped / national lanes ───────────────────
        r.section("S5 TE mirror, FRED scoped, national lanes")
        te, _, e = get_json("data/warm/te-mirror/_index.json")
        if te:
            syms = te.get("symbols") or {}
            names = list(syms.keys()) if isinstance(syms, dict) else [s.get("symbol") if isinstance(s, dict) else s for s in syms]
            pmi = [s for s in names if re.search(r"PMI|CONF|IP$|INDPRO|UNEMP|EXPORT|IMPORT|RETAIL|GDP|CPI", str(s), re.I)]
            r.log(f"TE mirror: {len(names)} symbols; cycle-like: {pmi[:80]}")
            OUT["te_mirror"] = {"n": len(names), "cycle_like": pmi[:200], "sample": names[:40]}
        fs_objs, fs_pre = list_prefix("data/warm/fred-scoped/", delimiter="/")
        r.log(f"fred-scoped prefixes: {fs_pre[:12]} top objects: {[o['Key'].split('/')[-1] for o in fs_objs[:8]]}")
        # try to find a series index for fred-scoped
        for k in ("data/providers/fred-scoped/manifest.json", "data/warm/fred-scoped/_index.json", "data/warm/fred-scoped/series-index.json.gz"):
            d, _, e = get_json(k)
            if d:
                r.log(f"  {k}: keys={list(d.keys())[:12]}")
                cats = d.get("categories") or {}
                if isinstance(cats, dict):
                    cyc_cats = [c for c in cats if re.search(r"(business|production|international|trade|employment|survey|leading|PMI)", str(c), re.I)]
                    r.log(f"  fred-scoped cycle categories: {cyc_cats[:30]}")
                    OUT["fred_scoped"] = {"n_categories": len(cats), "cycle_categories": cyc_cats[:100]}
                break
        # FRED country PMI/IP series presence (query the fred pages store? use the series index if present)
        nat = {}
        for lane in ("statcan", "boj-full", "boe-full", "banxico", "bcb", "snb", "hk-data", "cl-datos", "asia-trade", "census-econ", "bls-full", "dol-full", "kr-ecos"):
            o, pre = list_prefix(f"data/warm/{lane}/", delimiter="/")
            nat[lane] = {"top_objects": len(o), "prefixes": pre[:12], "sample": [x["Key"].split("/")[-1] for x in o[:6]]}
        r.log(f"national lanes: {json.dumps(nat)[:2500]}")
        OUT["national"] = nat
        # StatCan cube catalog: cycle cubes
        sc, _, e = get_json("data/warm/statcan/cube-catalog.json.gz")
        if sc:
            cubes = sc.get("cubes") or sc
            if isinstance(cubes, dict):
                cubes = list(cubes.values())
            hits = [c for c in cubes if isinstance(c, dict) and re.search(r"(industrial product|manufactur|labour force|employment|unemploy|merchandise trade|exports|retail trade|business outlook|gdp)", str(c.get("cubeTitleEn") or c.get("title") or ""), re.I)]
            r.log(f"StatCan cubes {len(cubes)}; cycle cubes {len(hits)}: {[(c.get('productId'), str(c.get('cubeTitleEn') or c.get('title'))[:50]) for c in hits[:20]]}")
            OUT["statcan_cycle_cubes"] = [(c.get("productId"), str(c.get("cubeTitleEn") or c.get("title"))[:80], c.get("frequencyCode")) for c in hits[:200]]

        # ── S6 existing per-country feeds ─────────────────────────────────
        r.section("S6 existing per-country feeds")
        gm, _, e = get_json("data/global-macro.json")
        if gm:
            cs = gm.get("countries") or []
            r.log(f"global-macro: method={str(gm.get('method'))[:300]} n={len(cs)} sample={json.dumps(cs[0] if isinstance(cs, list) else list(cs.values())[0])[:500]}")
            for c in (cs if isinstance(cs, list) else cs.values()):
                i3 = area_to_iso3(c.get("code") or c.get("iso3") or "")
                if i3:
                    mark("feed:global-macro", i3, gm.get("as_of"), "data/global-macro.json", {"ip_yoy": c.get("ip_yoy"), "unemployment": c.get("unemployment")})
            OUT["global_macro"] = {"method": str(gm.get("method"))[:600], "n": len(cs)}
        oc, _, e = get_json("data/oecd-cli.json")
        if oc:
            r.log(f"oecd-cli feed: as_of_period={oc.get('as_of_period')} generated={oc.get('generated_at')} n={len(oc.get('by_country') or {})} fetch_errors={str(oc.get('fetch_errors'))[:200]}")
            OUT["oecd_cli_feed"] = {"as_of_period": oc.get("as_of_period"), "n": len(oc.get("by_country") or {})}
        gr, _, e = get_json("data/global-recession.json")
        if gr:
            cs = gr.get("countries") or []
            sample = cs[0] if isinstance(cs, list) and cs else None
            r.log(f"global-recession: prob={gr.get('global_recession_prob_pct')} band={gr.get('band')} n={len(cs)} confirmation={json.dumps(gr.get('confirmation'))[:300]} sample={json.dumps(sample)[:400]}")
        pc, _, e = get_json("data/port-cargo.json")
        if pc:
            r.log(f"port-cargo keys: {list(pc.keys())[:30]}; complete_through={pc.get('complete_through')} true_latest={pc.get('true_latest_date')}")
            for k in ("by_country", "countries", "country_rollup", "ports"):
                v = pc.get(k)
                if isinstance(v, (dict, list)) and len(v):
                    r.log(f"  port-cargo.{k}: n={len(v)} sample={json.dumps((list(v.values())[0] if isinstance(v, dict) else v[0]))[:400]}")
        al, _, e = get_json("data/asia-leads.json")
        if al:
            r.log(f"asia-leads: korea_exports={json.dumps(al.get('korea_exports'))[:300]} taiwan_orders={json.dumps(al.get('taiwan_orders'))[:200]}")
        gs, _, e = get_json("data/global-sovereign.json")
        if gs:
            cs = gs.get("countries") or []
            r.log(f"global-sovereign: n={len(cs)} sample={json.dumps(cs[0] if isinstance(cs, list) and cs else None)[:400]}")
            for c in (cs if isinstance(cs, list) else []):
                i3 = area_to_iso3(c.get("iso3") or c.get("code") or "") or next((k for k, v in {"United States": "USA", "China": "CHN", "Japan": "JPN", "Germany": "DEU"}.items() if k == c.get("country")), None)
                if i3:
                    mark("feed:global-sovereign", i3, gs.get("generated_at"), "data/global-sovereign.json", {"yield_10y": c.get("yield_10y_pct"), "cds_bp": c.get("cds_bp")})

        # ── S7 matrix ─────────────────────────────────────────────────────
        r.section("S7 country x feature matrix")
        feats = sorted({f for iso in MATRIX for f in MATRIX[iso]})
        r.log(f"{len(feats)} features discovered: {feats}")
        for iso in ISO3:
            row = MATRIX[iso]
            r.log(f"  {iso}: {len(row)} features · " + ", ".join(f"{f.split(':', 1)[1][:28]}@{(v.get('latest') or '?') if isinstance(v.get('latest'), str) else json.dumps(v.get('latest'))[:24]}" for f, v in sorted(row.items())[:14]))
            r.kv(iso=iso, n_features=len(row), oecd=sum(1 for f in row if f.startswith("oecd")), bis=sum(1 for f in row if f.startswith("bis")),
                 eurostat=sum(1 for f in row if f.startswith("eurostat")), feeds=sum(1 for f in row if f.startswith("feed")))
        OUT["matrix"] = MATRIX
        OUT["features"] = feats
        s3.put_object(Bucket=B, Key="data/audit/cycle-feature-matrix-5099.json", Body=json.dumps(OUT, default=str).encode(),
                      ContentType="application/json", CacheControl="max-age=300")
        r.ok("matrix written to data/audit/cycle-feature-matrix-5099.json")
        r.section("verdict")
        r.ok(f"OECD walked={len(byid)} profiled={len(prof)} · BIS profiled={len(bprof)} · Eurostat cycle flows={len(ehits)} · features={len(feats)}")


if __name__ == "__main__":
    main()
