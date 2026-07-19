"""ops 3523 — census #6 close: 7 proven-config pages patched (discovery
3522), corrected feed map for the 12 misses re-derived with an EXTENDED
deriver (dict-of-numbers -> bars, depth 4), composer n_raw probe for
the 0-positions question, W2 re-asserted with honest expectations.

  X1 composer live: input_hygiene.n_raw_positions printed — resolves
     whether compose() itself is empty today (regime throttle) or the
     doc is broken; hygiene feeds >= 8
  X2 second-wave discovery on the TRUE feeds (page-grep mapping) —
     >= 8 of 12 derive
  X3 the 7 newly-enhanced pages serve the jh-enhance line AND each
     configured path resolves on its live feed (hard, per page)
"""
import json, re, sys, time, urllib.request
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")

WAVE2 = [
    ("political.html", "data/lobbying-intel.json"),
    ("primary-dealers.html", "data/nyfed-primary-dealer.json"),
    ("us-data-desk.html", "data/census-economic.json"),
    ("macro-leads.html", "data/macro-leads.json"),
    ("bls.html", "data/bls-employment.json"),
    ("heatmap.html", "data/stock-valuations.json"),
    ("valuations.html", "data/stock-valuations.json"),
    ("ofr.html", "data/settlement-fails.json"),
    ("credit-desk.html", "data/cds-proxy.json"),
    ("dollar.html", "data/dollar-radar.json"),
    ("eurodollar.html", "data/eurodollar-plumbing.json"),
    ("tv-notes.html", "data/tradingview-notes.json"),
]
ENHANCED = [
    ("proven-alpha.html", "data/proven-alpha.json", "families",
     ["family", "graded"]),
    ("liquidity.html", "data/liquidity-inflection.json",
     "usd.impulse_tail_180d", None),
    ("industry-rotation.html", "data/industry-rotation.json", "ladder",
     ["name", "price"]),
    ("capital-flow.html", "data/capital-flow.json", "dollar_flow_in",
     ["ticker", "flow_score"]),
    ("share-flows.html", "data/share-flows.json", "boards.top_buybacks",
     ["ticker", "price"]),
    ("opportunities.html", "data/opportunities.json", "top_opportunities",
     ["ticker", "price"]),
    ("panels.html", "data/wl-engines.json", "engines",
     ["name", "coverage"]),
]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3523"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def resolve(doc, path):
    cur = doc
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


def derive2(doc, depth=0, path=""):
    if depth > 4:
        return None
    if isinstance(doc, dict):
        for k, v in doc.items():
            p2 = f"{path}.{k}" if path else k
            if isinstance(v, list) and len(v) >= 5:
                if all(isinstance(r, dict) for r in v[:5]):
                    keys = v[0].keys()
                    lab = next((c for c in ("ticker", "symbol", "name",
                                            "t", "family", "sector",
                                            "series_id", "id", "key",
                                            "label", "class")
                                if c in keys), None)
                    num = next((c for c in keys
                                if isinstance(v[0].get(c), (int, float))
                                and c != lab), None)
                    if lab and num:
                        return {"mode": "bars", "path": p2,
                                "label": lab, "val": num, "n": len(v)}
                if all(isinstance(r, list) and len(r) >= 2
                       for r in v[:5]):
                    return {"mode": "line", "path": p2, "n": len(v)}
            if isinstance(v, dict) and len(v) >= 5 and all(
                    isinstance(x, (int, float)) for x in
                    list(v.values())[:8]):
                return {"mode": "bars-dict", "path": p2, "n": len(v)}
        for k, v in doc.items():
            if isinstance(v, (dict, list)):
                r = derive2(v, depth + 1, f"{path}.{k}" if path else k)
                if r:
                    return r
    if isinstance(doc, list) and doc and isinstance(doc[0], dict):
        return derive2(doc[0], depth + 1, path + ".[0]")
    return None


with report("3523_census6_close") as rep:
    fails = []

    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:560]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3523 — census #6 wave-2 + composer probe")
    cfg = lam.get_function_configuration(
        FunctionName="justhodl-proven-portfolio")
    deploy_lambda(report=rep, function_name="justhodl-proven-portfolio",
                  source_dir=REPO / "aws" / "lambdas" /
                  "justhodl-proven-portfolio" / "source",
                  env_vars=(cfg.get("Environment") or {})
                  .get("Variables") or {},
                  timeout=cfg["Timeout"], memory=cfg["MemorySize"],
                  description="composer v1.2.4 raw probe (ops 3523)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c2 = lam.get_function_configuration(
            FunctionName="justhodl-proven-portfolio")
        if c2.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    lam.invoke(FunctionName="justhodl-proven-portfolio", Payload=b"{}")
    time.sleep(2)
    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/proven-portfolio.json")["Body"].read())
        hyg = doc.get("input_hygiene") or {}
        gate("X1_composer", len(hyg.get("feeds") or {}) >= 8
             and "n_raw_positions" in hyg,
             {"n_raw_positions": hyg.get("n_raw_positions"),
              "n_positions": len(doc.get("positions") or []),
              "guard": hyg.get("output_guard"),
              "n_stale": hyg.get("n_stale"),
              "n_missing": hyg.get("n_missing"),
              "missing": [k for k, v in (hyg.get("feeds") or {}).items()
                          if not v.get("present")]})
    except Exception as e:
        gate("X1_composer", False, str(e)[:300])

    table, misses = [], []
    for page, key in WAVE2:
        try:
            d = json.loads(s3c.get_object(Bucket=BUCKET, Key=key)
                           ["Body"].read())
            c3 = derive2(d)
            (table if c3 else misses).append(
                {"page": page, "feed": key,
                 **(c3 or {"top_keys": sorted(d.keys())[:10]})})
        except Exception as e:
            misses.append({"page": page, "feed": key,
                           "err": str(e)[:60]})
    print("WAVE2_TABLE " + json.dumps(table))
    if misses:
        print("WAVE2_MISSES " + json.dumps(misses))
    rep.log("WAVE2_TABLE " + json.dumps(table)[:1600])
    gate("X2_wave2", len(table) >= 8,
         {"derived": len(table), "misses": len(misses)})

    okp, det = 0, []
    for page, key, path, lv in ENHANCED:
        try:
            served = fetch(f"https://justhodl.ai/{page}?cb={int(time.time())}")
            has = b"jh-enhance.js" in served
            d = json.loads(s3c.get_object(Bucket=BUCKET, Key=key)
                           ["Body"].read())
            node = resolve(d, path)
            if lv:
                ok = (has and isinstance(node, list) and len(node) >= 5
                      and lv[0] in node[0]
                      and isinstance(node[0].get(lv[1]), (int, float)))
            else:
                ok = (has and isinstance(node, list) and len(node) >= 5)
            okp += 1 if ok else 0
            det.append((page, has, bool(ok)))
        except Exception as e:
            det.append((page, "err", str(e)[:40]))
    gate("X3_enhanced_pages", okp == len(ENHANCED), det)

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO / "aws" / "ops" / "reports" / "3523.json").write_text(
        json.dumps({"ops": 3523, "fails": fails, "wave2": table,
                    "misses": misses}, indent=1))
sys.exit(0)
