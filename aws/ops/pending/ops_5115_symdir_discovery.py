"""ops_5115 -- symbol-directory discovery (read-only).

Khalid: "every single ticker and data (every single one) with its entire
history should be searchable on chart-pro search bar and watchlist ...
exactly as in tradingview".

Before building the directory, learn the true shape of every catalog and
warehouse it must cover -- the 5098 audit gave counts, not record shapes:

  * hub data/provider-catalog.json + data/providers/{slug}.json per provider:
    keys, dataset entry shape, series fields (what a search row can show)
  * data/warm/{prefix}/ layout per provider: sub-prefixes, key patterns,
    ONE sample object head (decompressed) so the series-fetch service can be
    written against the real file grammar, not a guess
  * serving index: data/index/manifest.json, t1 states, a Tier-1 block via
    Range, page-0000 rows for eurostat/ecb (entry + record schema)
  * FRED: series-meta pages (title/units/freq/popularity) and the
    fred-scoped per-series data keys -> is every banked series addressable
  * instrument universes: finviz-universe, symbol-dictionary, tv-watchlists,
    symbology master, indicator-bus, polygon reference counts (live API)

Writes data/audit/symdir-discovery-5115.json (full) and the report.
Never RED unless the hub is unreadable.
"""
import gzip
import io
import json
import os
import sys
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION, config=Config(retries={"max_attempts": 6}, max_pool_connections=32))
lam = boto3.client("lambda", region_name=REGION, config=Config(retries={"max_attempts": 4}))
POLY = "zvEY_KYYMHoAN0JqY7n2Ze6q0kBuJX_d"

WARM_SAMPLE = ["fred-scoped", "fred", "fred-catalog", "eurostat", "ecb", "oecd", "imf", "bis", "statcan", "boj", "boj-full",
               "worldbank", "bls", "nyfed", "ofr", "ofr-bsrm", "ofr-hfm", "ofr-fsi", "bea", "te-mirror", "treasury",
               "fiscaldata-full", "boe", "boe-full", "fed-board", "frbddp-full", "dol", "census-us", "census-econ",
               "indicator-bus", "polygon", "polygon-full", "tradingview-vault-live", "yahoo", "cftc", "snb", "bcb",
               "eiopa", "dbnomics", "banxico", "coinmetrics", "cboe", "tic", "tic-full", "hk-data", "cl-datos",
               "nyfed-research", "finra", "gleif", "asia-trade", "kr-ecos", "taiwan-moea", "peru-copper",
               "chicagofed", "clevelandfed", "atlantafed", "nasa", "occ", "te-feed", "archived-fred"]


def get_bytes(key, rng=None):
    kw = {"Bucket": B, "Key": key}
    if rng:
        kw["Range"] = rng
    return s3.get_object(**kw)["Body"].read()


def get_json(key):
    try:
        b = get_bytes(key)
        if key.endswith(".gz"):
            b = gzip.decompress(b)
        return json.loads(b)
    except Exception as e:  # noqa: BLE001
        return {"__error__": str(e)[:160]}


def head_text(key, n=900):
    """First n chars of an object, decompressing gzip/zip-ish heads where possible."""
    try:
        raw = get_bytes(key, rng="bytes=0-%d" % (max(n * 40, 65536)))
        if key.endswith(".gz") or raw[:2] == b"\x1f\x8b":
            try:
                raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read(n * 4)
            except Exception:  # noqa: BLE001
                try:
                    raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read(n)
                except Exception:  # noqa: BLE001
                    pass
        return raw[:n].decode("utf-8", "replace")
    except Exception as e:  # noqa: BLE001
        return "__error__ " + str(e)[:120]


def list_layout(prefix, pages=1, maxkeys=1000):
    """Sub-prefixes (Delimiter) + first keys + approximate count."""
    out = {"prefix": prefix, "subprefixes": [], "sample_keys": [], "count_sampled": 0, "bytes_sampled": 0, "truncated": False}
    try:
        d = s3.list_objects_v2(Bucket=B, Prefix=prefix, Delimiter="/", MaxKeys=maxkeys)
        out["subprefixes"] = [c["Prefix"] for c in d.get("CommonPrefixes", [])][:60]
        out["sample_keys"] = [(o["Key"], o["Size"]) for o in d.get("Contents", [])][:25]
        tok, n, bts = None, 0, 0
        for _ in range(pages):
            kw = {"Bucket": B, "Prefix": prefix, "MaxKeys": maxkeys}
            if tok:
                kw["ContinuationToken"] = tok
            d2 = s3.list_objects_v2(**kw)
            for o in d2.get("Contents", []):
                n += 1
                bts += o["Size"]
                if len(out["sample_keys"]) < 40:
                    out["sample_keys"].append((o["Key"], o["Size"]))
            tok = d2.get("NextContinuationToken")
            if not tok:
                break
        out["count_sampled"], out["bytes_sampled"], out["truncated"] = n, bts, bool(tok)
        # one level deeper for the first 3 subprefixes
        deeper = {}
        for sp in out["subprefixes"][:3]:
            d3 = s3.list_objects_v2(Bucket=B, Prefix=sp, Delimiter="/", MaxKeys=20)
            deeper[sp] = {"sub": [c["Prefix"] for c in d3.get("CommonPrefixes", [])][:15],
                          "keys": [(o["Key"], o["Size"]) for o in d3.get("Contents", [])][:10]}
        out["deeper"] = deeper
    except Exception as e:  # noqa: BLE001
        out["error"] = str(e)[:160]
    return out


def trunc(o, n=700):
    s = json.dumps(o, default=str)
    return s if len(s) <= n else s[:n] + "…"


def http_json(url, timeout=25):
    req = urllib.request.Request(url, headers={"Accept": "application/json", "User-Agent": "justhodl-ops"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return json.loads(r.read().decode("utf-8", "replace"))


def main():
    out = {"generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())}
    with report("5115-symdir-discovery") as r:
        r.heading("ops 5115 -- symbol directory discovery: every catalog, warehouse grammar and index the search must cover")

        # ---------------- A. hub + per-provider catalogs ----------------
        r.section("A. provider hub and per-provider catalogs")
        hub = get_json("data/provider-catalog.json")
        if "__error__" in hub:
            r.fail("hub unreadable: " + hub["__error__"])
            sys.exit(1)
        r.log("hub keys: " + ", ".join(list(hub.keys())[:30]))
        provs = hub.get("providers") or hub.get("items") or []
        if isinstance(provs, dict):
            provs = list(provs.values())
        r.log(f"hub providers={len(provs)} as_of={hub.get('as_of')}")
        if provs:
            r.log("hub provider entry example: " + trunc(provs[0], 1200))
        out["hub_keys"] = list(hub.keys())
        out["hub_provider_example"] = provs[0] if provs else None
        slugs = [p.get("slug") or p.get("id") for p in provs if isinstance(p, dict)]
        slugs = [s for s in slugs if s]
        out["slugs"] = slugs

        def one_provider(slug):
            doc = get_json(f"data/providers/{slug}.json")
            rec = {"slug": slug}
            if "__error__" in doc:
                rec["error"] = doc["__error__"]
                return rec
            rec["keys"] = list(doc.keys())[:40]
            for k, v in doc.items():
                if isinstance(v, list) and v and isinstance(v[0], dict):
                    rec.setdefault("lists", {})[k] = {"n": len(v), "fields": sorted({kk for x in v[:50] for kk in x.keys()})[:40],
                                                      "sample": v[:2]}
                elif isinstance(v, dict) and len(v) > 5 and k in ("datasets", "series", "flows", "files", "keys"):
                    ks = list(v.keys())[:2]
                    rec.setdefault("dicts", {})[k] = {"n": len(v), "sample": {kk: v[kk] for kk in ks}}
                elif not isinstance(v, (list, dict)):
                    rec.setdefault("scalars", {})[k] = v
            for k in ("series_count", "series", "n_series", "series_manifest", "count_from"):
                if k in doc and not isinstance(doc[k], (list, dict)):
                    rec.setdefault("scalars", {})[k] = doc[k]
            sm = get_json(f"data/providers/{slug}/series-manifest.json")
            if "__error__" not in sm:
                rec["series_manifest"] = {k: sm[k] for k in sm if not isinstance(sm[k], (list, dict))}
                rec["series_manifest_keys"] = list(sm.keys())[:30]
            return rec

        with ThreadPoolExecutor(16) as ex:
            precs = list(ex.map(one_provider, slugs))
        out["providers"] = precs
        for rec in precs:
            if rec.get("error"):
                r.log(f"  {rec['slug']:<26} catalog ERROR {rec['error']}")
                continue
            lists = rec.get("lists", {})
            lsum = "; ".join(f"{k}[{v['n']}] fields={','.join(v['fields'][:12])}" for k, v in lists.items())
            dsum = "; ".join(f"{k}{{{v['n']}}}" for k, v in rec.get("dicts", {}).items())
            r.log(f"  {rec['slug']:<26} keys={','.join(rec.get('keys', [])[:14])} | {lsum} {dsum}")
            for k, v in lists.items():
                r.log(f"      sample {k}: " + trunc(v["sample"][0], 520))
            if rec.get("series_manifest"):
                r.log("      series-manifest: " + trunc(rec["series_manifest"], 400))

        # ---------------- B. warm layouts + sample object heads ----------------
        r.section("B. data/warm/{prefix}/ layouts and one sample object head each")
        d = s3.list_objects_v2(Bucket=B, Prefix="data/warm/", Delimiter="/")
        warm_prefixes = [c["Prefix"].split("/")[2] for c in d.get("CommonPrefixes", [])]
        r.log(f"warm prefixes ({len(warm_prefixes)}): " + ", ".join(warm_prefixes))
        out["warm_prefixes"] = warm_prefixes
        targets = [p for p in WARM_SAMPLE if p in warm_prefixes] + [p for p in warm_prefixes if p not in WARM_SAMPLE][:12]

        def one_warm(p):
            lay = list_layout(f"data/warm/{p}/", pages=2)
            # choose a data-looking sample key: prefer json/csv/tsv/gz not _state/manifest
            cand = [k for k, sz in lay.get("sample_keys", []) if "_state" not in k and "manifest" not in k and sz > 200]
            for sp, dd in (lay.get("deeper") or {}).items():
                cand += [k for k, sz in dd.get("keys", []) if "_state" not in k and "manifest" not in k and sz > 200]
                for sp2 in dd.get("sub", [])[:2]:
                    try:
                        d4 = s3.list_objects_v2(Bucket=B, Prefix=sp2, MaxKeys=5)
                        cand += [o["Key"] for o in d4.get("Contents", []) if o["Size"] > 200]
                    except Exception:  # noqa: BLE001
                        pass
            lay["samples"] = []
            seen_ext = set()
            for k in cand:
                ext = k.rsplit(".", 1)[-1] if "." in k.rsplit("/", 1)[-1] else "noext"
                if ext in seen_ext and len(lay["samples"]) >= 2:
                    continue
                seen_ext.add(ext)
                lay["samples"].append({"key": k, "head": head_text(k, 700)})
                if len(lay["samples"]) >= 3:
                    break
            return p, lay

        with ThreadPoolExecutor(12) as ex:
            warm = dict(ex.map(one_warm, targets))
        out["warm"] = warm
        for p in targets:
            lay = warm[p]
            r.log(f"  warm/{p}: sub={lay.get('subprefixes', [])[:12]} sampled={lay.get('count_sampled')}{'+' if lay.get('truncated') else ''} objs / {lay.get('bytes_sampled', 0) / 1e6:.1f}MB")
            for k, sz in lay.get("sample_keys", [])[:6]:
                r.log(f"      key {k} ({sz}B)")
            for smp in lay.get("samples", []):
                r.log(f"      HEAD {smp['key']}: " + smp["head"][:420].replace("\n", "⏎"))

        # ---------------- C. serving index ----------------
        r.section("C. serving index (Tier 0 / Tier 1) state and record schemas")
        man = get_json("data/index/manifest.json")
        r.log("index manifest: " + trunc(man, 1500))
        out["index_manifest"] = man
        for prov in ("eurostat", "ecb"):
            st = get_json(f"data/_state/t1-{prov}.json")
            st.pop("flows_done", None)
            r.log(f"t1-{prov} state: " + trunc(st, 600))
            out[f"t1_{prov}"] = st
            flows = get_json(f"data/index/{prov}/flows.json.gz")
            fl = flows.get("flows") or {}
            r.log(f"tier0 {prov}: schema={flows.get('schema')} flows={len(fl)} keys={list(flows.keys())[:12]}")
            ex1 = list(fl.items())[:2]
            r.log(f"   tier0 entries: {trunc(ex1, 500)}")
            out[f"tier0_{prov}"] = {"keys": list(flows.keys()), "n_flows": len(fl), "examples": ex1}
            # biggest flow with t1: sample blocks + first rows via Range
            big = sorted(fl.items(), key=lambda kv: -(kv[1].get("series") or 0))[:1]
            if big:
                f, rec = big[0]
                bm = get_json(f"data/index/{prov}/t1/{f}.blocks.json")
                if "__error__" not in bm:
                    b0 = bm["blocks"][0]
                    txt = get_bytes(f"data/index/{prov}/t1/{f}.jsonl", rng=f"bytes={b0['o']}-{b0['o'] + min(b0['c'], 4000) - 1}").decode("utf-8", "replace")
                    rows = [json.loads(l) for l in txt.split("\n")[:3] if l.strip().startswith("{") and l.strip().endswith("}")]
                    r.log(f"   t1 {f}: n={bm.get('n')} blocks={len(bm['blocks'])} schema={bm.get('entry_schema')} first rows: {trunc(rows, 700)}")
                    out[f"t1_sample_{prov}"] = {"flow": f, "n": bm.get("n"), "blocks": len(bm["blocks"]), "rows": rows}
                else:
                    r.log(f"   t1 {f}: blocks missing ({bm['__error__']})")
            pg = get_json(f"data/providers/{prov}/series/page-0000.json")
            rows = (pg.get("rows") or [])[:2]
            r.log(f"   page-0000 keys={list(pg.keys())[:10]} rows={len(pg.get('rows') or [])} sample: {trunc(rows, 900)}")
            out[f"page0_{prov}"] = {"keys": list(pg.keys()), "n": len(pg.get("rows") or []), "rows": rows}

        # ---------------- D. FRED ----------------
        r.section("D. FRED catalog + banked series addressability")
        fm = get_json("data/_state/fred-series-meta.json")
        r.log("fred-series-meta state: " + trunc(fm, 700))
        out["fred_meta_state"] = fm
        lay = list_layout("data/warm/fred-catalog/series-meta/", pages=3)
        r.log(f"series-meta pages sampled={lay['count_sampled']}{'+' if lay['truncated'] else ''}")
        pg = get_json("data/warm/fred-catalog/series-meta/page-0000.json")
        r.log(f"series-meta page-0000: keys={list(pg.keys())[:8]} count={pg.get('count')} sample={trunc((pg.get('rows') or [])[:2], 800)}")
        out["fred_meta_page0"] = {"keys": list(pg.keys()), "count": pg.get("count"), "rows": (pg.get("rows") or [])[:2]}
        fsm = get_json("data/providers/fred-scoped/manifest.json")
        r.log("fred-scoped manifest: " + trunc({k: v for k, v in fsm.items() if not isinstance(v, (list, dict))}, 900))
        out["fred_scoped_manifest"] = {k: v for k, v in fsm.items() if not isinstance(v, (list, dict))}
        # count fred-scoped series objects (bounded listing)
        tok, n, roots = None, 0, {}
        for i in range(400):
            kw = {"Bucket": B, "Prefix": "data/warm/fred-scoped/", "MaxKeys": 1000}
            if tok:
                kw["ContinuationToken"] = tok
            d2 = s3.list_objects_v2(**kw)
            for o in d2.get("Contents", []):
                n += 1
                parts = o["Key"].split("/")
                if len(parts) >= 5:
                    roots[parts[3]] = roots.get(parts[3], 0) + 1
            tok = d2.get("NextContinuationToken")
            if not tok:
                break
        r.log(f"fred-scoped objects: {n}{'+' if tok else ''} across {len(roots)} roots: {trunc(sorted(roots.items(), key=lambda x: -x[1])[:25], 900)}")
        out["fred_scoped_count"] = n
        out["fred_scoped_roots"] = roots
        lay2 = list_layout("data/warm/fred/", pages=2)
        r.log(f"warm/fred (unscoped) sub={lay2.get('subprefixes', [])[:10]} sampled={lay2.get('count_sampled')}{'+' if lay2.get('truncated') else ''}")
        for k, sz in lay2.get("sample_keys", [])[:5]:
            r.log(f"      key {k} ({sz}B)")
        if lay2.get("sample_keys"):
            k = [k for k, sz in lay2["sample_keys"] if sz > 200]
            if k:
                r.log("      HEAD " + k[0] + ": " + head_text(k[0], 500).replace("\n", "⏎"))
        out["fred_unscoped_layout"] = lay2

        # ---------------- E. instrument universes ----------------
        r.section("E. instrument universes on S3")
        for key in ("data/finviz-universe.json", "data/symbol-dictionary.json", "data/tv-watchlists.json", "data/symbology/master.json",
                    "data/symbol-feed.json", "data/symbol-aliases.json", "data/tv-descriptions.json", "data/indicator-bus.json",
                    "data/indicator-bus/registry.json", "data/thesis-state-v2.json.gz", "data/sp500.json", "data/etf-universe.json"):
            try:
                h = s3.head_object(Bucket=B, Key=key)
                doc = get_json(key)
                info = {"bytes": h["ContentLength"], "modified": str(h["LastModified"])[:19]}
                if isinstance(doc, dict):
                    info["keys"] = list(doc.keys())[:15]
                    for k, v in doc.items():
                        if isinstance(v, list) and v:
                            info[f"list:{k}"] = {"n": len(v), "sample": v[0] if not isinstance(v[0], dict) else {kk: v[0][kk] for kk in list(v[0].keys())[:14]}}
                            break
                        if isinstance(v, dict) and len(v) > 20:
                            kk = list(v.keys())[:1]
                            info[f"dict:{k}"] = {"n": len(v), "sample": {x: v[x] for x in kk}}
                            break
                elif isinstance(doc, list):
                    info["n"] = len(doc)
                    info["sample"] = doc[0] if doc else None
                r.log(f"  {key}: " + trunc(info, 900))
                out.setdefault("universes", {})[key] = info
            except Exception as e:  # noqa: BLE001
                r.log(f"  {key}: missing ({str(e)[:80]})")
        # indicator-bus location
        lay3 = list_layout("data/indicator-bus", pages=1)
        r.log(f"indicator-bus keys: {lay3.get('sample_keys', [])[:8]} sub={lay3.get('subprefixes', [])[:8]}")
        out["indicator_bus_layout"] = lay3

        # ---------------- F. polygon reference counts (live) ----------------
        r.section("F. Polygon reference universe (live API, first page per market)")
        pref = {}
        for mkt in ("stocks", "otc", "indices", "crypto", "fx"):
            try:
                d1 = http_json(f"https://api.polygon.io/v3/reference/tickers?market={mkt}&active=true&limit=1000&apiKey={POLY}")
                res = d1.get("results") or []
                pref[mkt] = {"first_page": len(res), "has_next": bool(d1.get("next_url")), "count_field": d1.get("count"),
                             "sample": {k: res[0].get(k) for k in ("ticker", "name", "market", "locale", "primary_exchange", "type", "currency_name", "cik", "composite_figi")} if res else None}
                r.log(f"  {mkt}: page1={len(res)} next={bool(d1.get('next_url'))} sample={trunc(pref[mkt]['sample'], 300)}")
            except Exception as e:  # noqa: BLE001
                pref[mkt] = {"error": str(e)[:120]}
                r.log(f"  {mkt}: ERROR {str(e)[:120]}")
            time.sleep(0.3)
        out["polygon_reference"] = pref

        # ---------------- G. existing serving lambdas ----------------
        r.section("G. existing serving functions (URLs) the page already depends on")
        for fn in ("justhodl-chart-data", "justhodl-fred-proxy", "justhodl-wl-series-api", "justhodl-symbol-search", "justhodl-series-fetch"):
            try:
                c = lam.get_function_configuration(FunctionName=fn)
                try:
                    u = lam.get_function_url_config(FunctionName=fn)["FunctionUrl"]
                except Exception:  # noqa: BLE001
                    u = None
                r.log(f"  {fn}: mem={c['MemorySize']} to={c['Timeout']} mod={c['LastModified'][:19]} url={u}")
                out.setdefault("functions", {})[fn] = {"mem": c["MemorySize"], "timeout": c["Timeout"], "url": u, "runtime": c.get("Runtime")}
            except Exception as e:  # noqa: BLE001
                r.log(f"  {fn}: absent ({str(e)[:60]})")
                out.setdefault("functions", {})[fn] = None

        s3.put_object(Bucket=B, Key="data/audit/symdir-discovery-5115.json", Body=json.dumps(out, default=str).encode(),
                      ContentType="application/json", CacheControl="max-age=300")
        # also drop a compact copy into the repo so the session can read it after git pull
        p = ROOT / "aws" / "ops" / "reports" / "latest" / "5115-symdir-discovery.json"
        p.write_text(json.dumps(out, default=str)[:3_500_000], encoding="utf-8")
        r.ok(f"discovery written: {len(precs)} provider catalogs, {len(warm)} warm layouts, index + fred + universes + polygon reference")


if __name__ == "__main__":
    main()
