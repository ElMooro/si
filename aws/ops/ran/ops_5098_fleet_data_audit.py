"""ops_5098 -- fleet audit: every provider, every engine, every feed -- read for
the global-cycle v3 build.

Khalid (2026-09-02): "audit all my data and data providers and all my
different engines and improve the global cycle exponentially". The v2.1 engine
is honest but ~90% equity momentum; the fleet holds 55+ providers and 849
engines, many of which already carry the real-economy series a cycle model
should be built on. This op establishes ground truth (read-only):

  S1  providers: data/provider-catalog.json hub + data/warm/ prefixes not in
      the hub + import-sentinel dead-lane chip -> datasets, coverage, MB,
      freshness per provider
  S2  engines: every Lambda (list_functions) x schedule-manifest x 7-day
      CloudWatch Invocations/Errors -> scheduled-but-silent, erroring,
      orphan (no schedule, no invocations), orchestrated
  S3  feeds: every data/*.json top-level feed -> age distribution, stale list
  S4  business-cycle series discovery across the macro warehouses: OECD
      (catalog + walked flows, CLI/BTS/KEI/MEI/STLABOUR), IMF, World Bank,
      Eurostat + ECB Tier-0 flow index, BIS, FRED scoped, BOJ/BoE/StatCan/
      Banxico/asia-trade, GDELT, PortWatch/port-cargo, polygon-full
  S5  the cycle-adjacent engine feeds (oecd-cli, us-cycle, cycle-clock,
      global-recession, macro-leads, trade-nowcast, activity-nowcast,
      labor-leading, consumer-pulse, esi, canary-grid, apac-leadlag,
      asia-leads, leading-markets, bis-crossborder, freight-pulse, air-cargo,
      port-cargo, boom-stage, china-liquidity, global-liquidity, singapore-
      nodx, taiwan-moea, peru-copper, import-canary, global-macro,
      geopolitical-risk, credit-composite, gdelt-news, macro-nowcast,
      nowcast-desk, us-cycle...) -> alive? per-country? what can be fused

Writes data/audit/fleet-data-audit-5098.json (machine-readable) for the v3
design. Gate: only the hub being unreadable is RED (sys.exit(1)).
"""
import csv
import gzip
import io
import json
import re
import sys
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from ops_report import report  # noqa: E402

REGION = "us-east-1"
B = "justhodl-dashboard-live"
s3 = boto3.client("s3", region_name=REGION, config=Config(max_pool_connections=32))
lam = boto3.client("lambda", region_name=REGION)
cw = boto3.client("cloudwatch", region_name=REGION)
NOW = datetime.now(timezone.utc)
AUDIT = {"generated_at": NOW.isoformat(timespec="seconds"), "ops": 5098}

CYCLE_KW = re.compile(r"(leading|composite leading|\bcli\b|business confidence|consumer confidence|tendency|"
                      r"\bpmi\b|industrial production|production index|manufactur|retail (sales|trade)|"
                      r"unemploy|employment|jobless|claims|exports?|imports?|trade balance|new orders|"
                      r"orders|capacity util|credit (gap|to|impulse)|money supply|\bm[123]\b|yield|"
                      r"house price|property price|\bgdp\b|economic sentiment|esi|confidence|"
                      r"building permit|housing start|car regist|electricity|freight|cargo|shipping|"
                      r"container|steel|copper|semiconductor|chip|inventor)", re.I)

CYCLE_FEEDS = ["oecd-cli", "us-cycle", "cycle-clock", "global-recession", "macro-leads", "trade-nowcast",
               "activity-nowcast", "macro-nowcast", "labor-leading", "consumer-pulse", "esi", "esi-history",
               "canary-grid", "canary-macro", "apac-leadlag", "apac-leadlag-series", "asia-leads",
               "leading-markets", "leading-markets-history", "bis-crossborder", "freight-pulse", "air-cargo",
               "port-cargo", "portwatch", "boom-stage", "china-liquidity", "china-liquidity-history",
               "global-liquidity", "global-liquidity-history", "singapore-nodx", "taiwan-moea", "peru-copper",
               "import-canary", "import-canary-history", "global-macro", "geopolitical-risk",
               "credit-composite", "credit-stress", "gdelt-news", "nowcast-desk", "bea-economic",
               "bls-labor", "census-economic", "econ-calendar", "macro-surprise", "global-sovereign",
               "sovereign-stress", "global-flows", "global-flow-desk", "eurodollar-plumbing",
               "commodity-curves", "dollar-radar", "fx-intelligence", "indicator-bus", "regime-composite",
               "cross-asset-regime", "macro-regime", "global-business-cycle", "global-business-cycle-history",
               "hiring-velocity", "construction-housing", "grid-queue", "ppi-acceleration",
               "divergence-engine-v2", "labor-leading", "market-extremes", "seasonality"]


def get_bytes(key):
    o = s3.get_object(Bucket=B, Key=key)
    body = o["Body"].read()
    if key.endswith(".gz") or body[:2] == b"\x1f\x8b":
        body = gzip.decompress(body)
    return body, o["LastModified"]


def get_json(key):
    try:
        body, lm = get_bytes(key)
        return json.loads(body), lm, None
    except Exception as e:  # noqa: BLE001
        return None, None, str(e)[:100]


def list_prefix(prefix, delimiter=None, max_keys=None):
    pag = s3.get_paginator("list_objects_v2")
    kw = {"Bucket": B, "Prefix": prefix}
    if delimiter:
        kw["Delimiter"] = delimiter
    objs, prefixes = [], []
    for page in pag.paginate(**kw):
        objs.extend(page.get("Contents") or [])
        prefixes.extend(p["Prefix"] for p in (page.get("CommonPrefixes") or []))
        if max_keys and len(objs) >= max_keys:
            break
    return objs, prefixes


def age_h(lm):
    if lm is None:
        return None
    if isinstance(lm, str):
        try:
            lm = datetime.fromisoformat(lm.replace("Z", "+00:00"))
        except Exception:  # noqa: BLE001
            return None
    if lm.tzinfo is None:
        lm = lm.replace(tzinfo=timezone.utc)
    return round((NOW - lm).total_seconds() / 3600, 1)


def summarize_feed(doc):
    """Cheap structural summary of a feed doc."""
    if not isinstance(doc, dict):
        return {"type": type(doc).__name__, "len": len(doc) if hasattr(doc, "__len__") else None}
    keys = list(doc.keys())
    gen = doc.get("generated_at") or doc.get("as_of") or doc.get("updated_at") or doc.get("timestamp")
    per_country = None
    for k in ("by_country", "countries", "country", "by_region", "regions", "economies"):
        v = doc.get(k)
        if isinstance(v, (dict, list)) and len(v) >= 3:
            per_country = {"key": k, "n": len(v)}
            sample = (list(v.values())[0] if isinstance(v, dict) else v[0])
            if isinstance(sample, dict):
                per_country["fields"] = list(sample.keys())[:14]
            break
    n_series = None
    for k in ("series", "indicators", "signals", "items", "rows", "metrics", "components", "leads"):
        v = doc.get(k)
        if isinstance(v, (dict, list)):
            n_series = {"key": k, "n": len(v)}
            break
    return {"generated_at": gen, "age_h": age_h(gen) if gen else None, "keys": keys[:24], "per_country": per_country,
            "n_series": n_series}


def main():
    with report("5098-fleet-data-audit") as r:
        r.heading("ops 5098 -- fleet audit: providers, engines, feeds, and the business-cycle series they hold")
        r.log(f"started {NOW.isoformat(timespec='seconds')}")

        # ── S1 providers ──────────────────────────────────────────────────
        r.section("S1 providers / warehouses")
        hub, hub_lm, err = get_json("data/provider-catalog.json")
        if not hub:
            r.fail(f"hub unreadable: {err}")
            sys.exit(1)
        provs = hub.get("providers") or []
        r.log(f"hub as_of={hub.get('as_of')} providers={len(provs)} breakdown={json.dumps(hub.get('breakdown'))[:200]}")
        prov_rows = []
        for p in provs:
            row = {k: p.get(k) for k in ("slug", "name", "datasets", "datasets_target", "coverage_pct", "n_keys",
                                         "total_mb", "series_count", "freshest_h", "denied_source_side", "hot_feeds")}
            prov_rows.append(row)
            r.log(f"  {str(p.get('slug')):<18} {str(p.get('name'))[:34]:<34} datasets={p.get('datasets')}/{p.get('datasets_target')} "
                  f"cov={p.get('coverage_pct')} keys={p.get('n_keys')} MB={p.get('total_mb')} series={p.get('series_count')} "
                  f"freshest={p.get('freshest_h')}h denied={p.get('denied_source_side')}")
        AUDIT["providers_hub"] = prov_rows
        _, warm_prefixes = list_prefix("data/warm/", delimiter="/")
        warm = [w.split("/")[2] for w in warm_prefixes]
        hub_slugs = {p.get("slug") for p in provs}
        not_in_hub = [w for w in warm if w not in hub_slugs and not any(w.startswith(h or "~") for h in hub_slugs if h)]
        r.log(f"data/warm/ has {len(warm)} provider prefixes; not represented in the hub by slug: {not_in_hub}")
        AUDIT["warm_prefixes"] = warm
        # state-doc freshness per warm prefix (what import-sentinel keys on)
        state_fresh = {}
        for w in warm:
            objs, _ = list_prefix(f"data/warm/{w}/_state/")
            if objs:
                newest = max(o["LastModified"] for o in objs)
                state_fresh[w] = {"n_state_docs": len(objs), "newest_age_h": age_h(newest)}
        stale = {w: v for w, v in state_fresh.items() if (v["newest_age_h"] or 0) > 48}
        r.log(f"state docs: {len(state_fresh)} prefixes carry _state/; >48h silent: {json.dumps(stale)[:600]}")
        AUDIT["warm_state_freshness"] = state_fresh
        sent, sent_lm, err = get_json("data/import-sentinel.json")
        if sent:
            chips = sent.get("pipeline") or sent.get("chips") or []
            dead = [c for c in chips if isinstance(c, dict) and c.get("name") == "dead-lanes"]
            r.log(f"import-sentinel as_of={sent.get('generated_at') or sent.get('as_of')} dead-lanes chip: {json.dumps(dead)[:500]}")
            AUDIT["import_sentinel_dead_lanes"] = dead
        else:
            r.warn(f"import-sentinel feed: {err}")

        # ── S2 engines ────────────────────────────────────────────────────
        r.section("S2 engines: inventory x schedules x 7d invocations/errors")
        fns = []
        for page in lam.get_paginator("list_functions").paginate():
            fns.extend(page["Functions"])
        r.log(f"{len(fns)} Lambda functions")
        man = json.loads((ROOT / "config" / "schedule-manifest.json").read_text())
        scheduled = defaultdict(list)
        for kind in ("rules", "schedules"):
            for it in man.get(kind) or []:
                if it.get("state") not in (None, "ENABLED"):
                    continue
                for t in it.get("targets") or []:
                    arn = t.get("arn") or ""
                    if ":function:" in arn:
                        scheduled[arn.split(":function:")[1].split(":")[0]].append(f"{kind}:{it.get('name')}:{it.get('expr') or it.get('schedule') or ''}")
        r.log(f"schedule manifest: {len(scheduled)} functions have an ENABLED rule/schedule")
        # CloudWatch 7d sums, batched
        names = [f["FunctionName"] for f in fns]
        inv, errs = {}, {}
        start, end = NOW - timedelta(days=7), NOW
        for i in range(0, len(names), 250):
            chunk = names[i:i + 250]
            q = []
            for j, n in enumerate(chunk):
                for met, tag in (("Invocations", "i"), ("Errors", "e")):
                    q.append({"Id": f"{tag}{j}", "MetricStat": {"Metric": {"Namespace": "AWS/Lambda", "MetricName": met,
                                                                            "Dimensions": [{"Name": "FunctionName", "Value": n}]},
                                                                 "Period": 604800, "Stat": "Sum"}, "ReturnData": True})
            nxt = None
            while True:
                kw = {"MetricDataQueries": q, "StartTime": start, "EndTime": end}
                if nxt:
                    kw["NextToken"] = nxt
                res = cw.get_metric_data(**kw)
                for m in res.get("MetricDataResults") or []:
                    j = int(m["Id"][1:])
                    val = sum(m.get("Values") or [0])
                    (inv if m["Id"][0] == "i" else errs)[chunk[j]] = val
                nxt = res.get("NextToken")
                if not nxt:
                    break
        classes = Counter()
        rows = []
        for f in fns:
            n = f["FunctionName"]
            i7, e7 = inv.get(n, 0), errs.get(n, 0)
            sched = scheduled.get(n)
            if sched and i7 == 0:
                cls = "SCHEDULED_SILENT"
            elif e7 > 0 and i7 > 0 and e7 / max(i7, 1) >= 0.5:
                cls = "ERRORING"
            elif e7 > 0:
                cls = "SOME_ERRORS"
            elif not sched and i7 == 0:
                cls = "ORPHAN_IDLE"
            elif not sched and i7 > 0:
                cls = "ORCHESTRATED"
            else:
                cls = "OK"
            classes[cls] += 1
            rows.append({"fn": n, "cls": cls, "inv7d": i7, "err7d": e7, "sched": sched[:2] if sched else None,
                         "timeout": f.get("Timeout"), "mem": f.get("MemorySize"), "modified": f.get("LastModified"),
                         "desc": (f.get("Description") or "")[:120]})
        r.log(f"classes: {dict(classes)}")
        for cls in ("SCHEDULED_SILENT", "ERRORING", "SOME_ERRORS"):
            sel = [x for x in rows if x["cls"] == cls]
            sel.sort(key=lambda x: -x["err7d"])
            r.log(f"  {cls} ({len(sel)}): " + "; ".join(f"{x['fn']} inv={x['inv7d']:.0f} err={x['err7d']:.0f}" for x in sel[:40]))
        orphans = [x["fn"] for x in rows if x["cls"] == "ORPHAN_IDLE"]
        r.log(f"  ORPHAN_IDLE ({len(orphans)}): {', '.join(orphans[:80])}{' …' if len(orphans) > 80 else ''}")
        AUDIT["engines"] = rows
        AUDIT["engine_classes"] = dict(classes)

        # ── S3 feeds ──────────────────────────────────────────────────────
        r.section("S3 feeds: data/*.json freshness")
        objs, _ = list_prefix("data/", delimiter="/")
        feeds = [o for o in objs if o["Key"].endswith(".json")]
        buckets = Counter()
        stale_feeds = []
        for o in feeds:
            a = age_h(o["LastModified"])
            b = "<1d" if a < 24 else "<3d" if a < 72 else "<7d" if a < 168 else "<30d" if a < 720 else ">30d"
            buckets[b] += 1
            if a >= 168:
                stale_feeds.append((o["Key"].split("/")[-1], round(a / 24), o["Size"]))
        stale_feeds.sort(key=lambda x: -x[1])
        r.log(f"{len(feeds)} top-level feeds; age buckets {dict(buckets)}")
        r.log(f"stale >7d ({len(stale_feeds)}): " + "; ".join(f"{k} {d}d" for k, d, _ in stale_feeds[:120]))
        AUDIT["feeds"] = {"n": len(feeds), "buckets": dict(buckets), "stale_7d": stale_feeds}

        # ── S4 business-cycle series discovery ───────────────────────────
        r.section("S4 business-cycle series discovery across the macro warehouses")
        disc = {}

        # OECD
        oecd_cat, _, err = get_json("data/warm/oecd/catalog.json.gz")
        if oecd_cat is None:
            oecd_cat, _, err = get_json("data/warm/oecd/catalog.json")
        flows = (oecd_cat or {}).get("dataflows") or []
        r.log(f"OECD catalog: {len(flows)} dataflows ({err or 'ok'})")
        walked, _ = list_prefix("data/warm/oecd/data/")
        walked_ids = defaultdict(list)
        for o in walked:
            fid = o["Key"].split("/")[4] if o["Key"].count("/") >= 4 else o["Key"]
            walked_ids[fid].append((o["Size"], o["LastModified"]))
        r.log(f"OECD walked: {len(walked)} objects across {len(walked_ids)} flow ids")
        oecd_hits = []
        for fl in flows:
            fid = fl.get("id") if isinstance(fl, dict) else str(fl)
            name = (fl.get("name") or fl.get("title") or "") if isinstance(fl, dict) else ""
            text = f"{fid} {name}"
            if CYCLE_KW.search(text) or any(k in text.upper() for k in ("CLI", "STES", "KEI", "MEI", "BTS", "QNA", "STLABOUR", "TRADE")):
                w = walked_ids.get(fid) or walked_ids.get(str(fid).replace("@", "_")) or []
                oecd_hits.append({"id": fid, "name": name[:90], "walked_objects": len(w),
                                  "walked_mb": round(sum(s for s, _ in w) / 1e6, 1), "walked_age_h": age_h(max(lm for _, lm in w)) if w else None})
        oecd_hits.sort(key=lambda x: (-(x["walked_objects"] > 0), x["id"]))
        r.log(f"OECD cycle-relevant dataflows: {len(oecd_hits)} (walked: {sum(1 for h in oecd_hits if h['walked_objects'])})")
        for h in oecd_hits[:60]:
            r.log(f"  {'WALKED' if h['walked_objects'] else '  --  '} {h['id']:<40} {h['name'][:60]} objs={h['walked_objects']} MB={h['walked_mb']} age={h['walked_age_h']}h")
        disc["oecd"] = {"n_dataflows": len(flows), "walked_flow_ids": len(walked_ids), "cycle_hits": oecd_hits}
        # peek at the CLI flow content if walked
        cli_keys = [o for o in walked if "CLI" in o["Key"].upper()]
        r.log(f"OECD CLI objects: {[(o['Key'].split('/')[-1], o['Size']) for o in cli_keys[:8]]}")
        if cli_keys:
            k = max(cli_keys, key=lambda o: o["Size"])
            try:
                body, lm = get_bytes(k["Key"])
                head = body[:1500].decode("utf-8", "replace")
                r.log(f"  {k['Key']} ({k['Size']} B, {age_h(lm)}h): head={head[:600]!r}")
                if head.lstrip().startswith(("REF_AREA", "STRUCTURE", "DATAFLOW", "\"")) or "," in head[:200]:
                    rd = csv.DictReader(io.StringIO(body.decode("utf-8", "replace")))
                    latest, areas, n = {}, set(), 0
                    for row in rd:
                        n += 1
                        ra = row.get("REF_AREA") or row.get("LOCATION") or row.get("Reference area") or ""
                        tp = row.get("TIME_PERIOD") or row.get("TIME") or ""
                        areas.add(ra)
                        if tp > latest.get(ra, ""):
                            latest[ra] = tp
                    top = sorted(latest.items(), key=lambda kv: kv[1], reverse=True)[:40]
                    r.log(f"  CLI rows={n} areas={len(areas)} latest periods: {top}")
                    disc["oecd_cli"] = {"key": k["Key"], "rows": n, "areas": sorted(areas), "latest_by_area": latest}
                elif head.lstrip().startswith("<"):
                    m = re.findall(r'TIME_PERIOD[^>]*value="([^"]+)"', body.decode("utf-8", "replace")[:5_000_000])
                    r.log(f"  CLI xml: {len(m)} TIME_PERIOD attrs, max={max(m) if m else None}")
                    disc["oecd_cli"] = {"key": k["Key"], "xml_periods": len(m), "max_period": max(m) if m else None}
            except Exception as e:  # noqa: BLE001
                r.warn(f"  CLI peek failed: {str(e)[:120]}")

        # IMF
        imf_objs, imf_prefixes = list_prefix("data/warm/imf-full/", delimiter="/")
        r.log(f"IMF: prefixes {imf_prefixes[:20]}")
        imf_hits = []
        for pfx in ("data/warm/imf-full/", "data/warm/imf/"):
            for cat_key in (pfx + "catalog.json", pfx + "catalog.json.gz", pfx + "_state/state.json", pfx + "dataflows.json"):
                d, _, e = get_json(cat_key)
                if d:
                    r.log(f"  {cat_key}: keys={list(d.keys())[:12]}")
                    flows_i = d.get("dataflows") or d.get("flows") or d.get("catalog") or []
                    if isinstance(flows_i, dict):
                        flows_i = [{"id": k, **(v if isinstance(v, dict) else {"name": str(v)})} for k, v in flows_i.items()]
                    for fl in flows_i:
                        fid = fl.get("id") or fl.get("dataflow") or ""
                        nm = fl.get("name") or fl.get("title") or ""
                        if CYCLE_KW.search(f"{fid} {nm}") or fid in ("IFS", "DOT", "PCPS", "WEO", "MFS", "CPI", "FSI", "GFS", "BOP", "IIP"):
                            imf_hits.append({"id": fid, "name": str(nm)[:80], "done": fl.get("done") or fl.get("status") or fl.get("objects")})
                    break
        r.log(f"IMF cycle-relevant dataflows: {len(imf_hits)}: {json.dumps(imf_hits[:40])[:1500]}")
        disc["imf"] = {"prefixes": imf_prefixes[:30], "cycle_hits": imf_hits}

        # World Bank
        wb, _, e = get_json("data/warm/worldbank-full/_state/state.json")
        if wb:
            r.log(f"World Bank state: {json.dumps({k: (v if not isinstance(v, (list, dict)) else len(v)) for k, v in wb.items()})[:500]}")
            disc["worldbank"] = {k: (v if not isinstance(v, (list, dict)) else len(v)) for k, v in wb.items()}
        else:
            r.warn(f"World Bank state: {e}")

        # Eurostat + ECB Tier-0
        for prov in ("eurostat", "ecb"):
            t0, _, e = get_json(f"data/index/{prov}/flows.json.gz")
            if not t0:
                r.warn(f"{prov} Tier-0: {e}")
                continue
            fl = t0.get("flows") or t0
            items = fl.items() if isinstance(fl, dict) else [(x.get("flow") or x.get("id"), x) for x in fl]
            hits = []
            for fid, info in items:
                title = (info.get("title") or info.get("name") or "") if isinstance(info, dict) else ""
                if CYCLE_KW.search(f"{fid} {title}"):
                    hits.append({"flow": fid, "title": str(title)[:80], "series": (info.get("n_series") or info.get("series") or info.get("count")) if isinstance(info, dict) else None})
            r.log(f"{prov} Tier-0: {len(list(items))} flows, cycle-relevant {len(hits)}; sample: {json.dumps(hits[:25])[:1400]}")
            disc[prov] = {"n_flows": len(list(items)), "cycle_hits": hits[:400]}

        # BIS
        bis_cat, _, e = get_json("data/warm/bis/catalog.json")
        if bis_cat is None:
            bis_cat, _, e = get_json("data/warm/bis/catalog.json.gz")
        bflows = (bis_cat or {}).get("dataflows") or []
        bis_objs, _ = list_prefix("data/warm/bis/data/")
        r.log(f"BIS catalog {len(bflows)} dataflows; walked objects {len(bis_objs)}: {[o['Key'].split('/')[-1][:40] for o in bis_objs[:30]]}")
        disc["bis"] = {"n_dataflows": len(bflows), "walked": [o["Key"].split("/")[-1] for o in bis_objs[:200]],
                       "flows": [(f.get("id"), (f.get("name") or "")[:60]) if isinstance(f, dict) else str(f) for f in bflows[:80]]}

        # FRED scoped
        fm, _, e = get_json("data/providers/fred-scoped/manifest.json")
        if fm:
            r.log(f"FRED scoped manifest keys={list(fm.keys())[:12]} n={fm.get('n_series') or fm.get('count') or fm.get('total')}")
            disc["fred_scoped"] = {k: (v if not isinstance(v, (list, dict)) else len(v)) for k, v in fm.items()}
        te, _, e = get_json("data/warm/te-mirror/_index.json")
        if te:
            n = len(te.get("series") or te.get("items") or te) if isinstance(te, (dict, list)) else None
            r.log(f"te-mirror index: {n} entries keys={list(te.keys())[:10] if isinstance(te, dict) else 'list'}")
            disc["te_mirror"] = {"n": n}

        # other national/warm lanes: object counts + newest
        others = {}
        for w in warm:
            if w in ("oecd", "eurostat", "ecb", "bis", "imf-full", "worldbank-full", "polygon-full", "gdelt-full"):
                continue
            objs_w, _ = list_prefix(f"data/warm/{w}/", max_keys=4000)
            if objs_w:
                others[w] = {"objects_sampled": len(objs_w), "mb_sampled": round(sum(o["Size"] for o in objs_w) / 1e6, 1),
                             "newest_age_h": age_h(max(o["LastModified"] for o in objs_w))}
        r.log(f"other warm lanes (sampled up to 4000 objects each): {json.dumps(others)[:2500]}")
        disc["other_warm"] = others
        # polygon-full + gdelt-full quick counts
        pg, _ = list_prefix(f"data/warm/polygon-full/grouped/{NOW.year}/")
        r.log(f"polygon-full grouped sessions this year: {len(pg)}, newest {max(o['Key'] for o in pg)[-18:] if pg else None}")
        gd, gpre = list_prefix("data/warm/gdelt-full/", delimiter="/")
        r.log(f"gdelt-full prefixes: {gpre[:12]}")
        disc["polygon_full_sessions_ytd"] = len(pg)
        AUDIT["discovery"] = disc

        # ── S5 cycle-adjacent feeds ───────────────────────────────────────
        r.section("S5 cycle-adjacent engine feeds")
        feed_summ = {}
        for f in sorted(set(CYCLE_FEEDS)):
            doc, lm, e = get_json(f"data/{f}.json")
            if doc is None:
                feed_summ[f] = {"missing": e}
                r.log(f"  {f:<32} MISSING ({e})")
                continue
            sm = summarize_feed(doc)
            sm["s3_age_h"] = age_h(lm)
            feed_summ[f] = sm
            r.log(f"  {f:<32} s3_age={sm['s3_age_h']}h gen_age={sm['age_h']}h per_country={json.dumps(sm['per_country'])[:120]} "
                  f"n_series={json.dumps(sm['n_series'])} keys={sm['keys'][:12]}")
            r.kv(feed=f, s3_age_h=sm["s3_age_h"], per_country=(sm["per_country"] or {}).get("n"), keys=len(sm["keys"]))
        AUDIT["cycle_feeds"] = feed_summ

        # persist
        s3.put_object(Bucket=B, Key="data/audit/fleet-data-audit-5098.json", Body=json.dumps(AUDIT, default=str).encode(),
                      ContentType="application/json", CacheControl="max-age=300")
        r.ok("audit written to data/audit/fleet-data-audit-5098.json")
        r.section("verdict")
        r.ok(f"providers={len(provs)} warm_prefixes={len(warm)} engines={len(fns)} classes={dict(classes)} feeds={len(feeds)} "
             f"stale7d={len(stale_feeds)} oecd_cycle_flows={len(oecd_hits)} (walked {sum(1 for h in oecd_hits if h['walked_objects'])})")


if __name__ == "__main__":
    main()
