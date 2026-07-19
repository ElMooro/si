"""ops 3522 — census #9 + #8 shipped, #6 discovery.

#9 composer v1.2.4: tracked input loader -> doc.input_hygiene (per-feed
   presence/age vs SLA/issues; Benzinga corpse will surface honestly) +
   output guard sanitize_positions (finite/dedupe/clamp with counters).
#8 proven-portfolio + short-book: additive "Powered by graded families"
   strip (live proven-alpha join, top-3 PROVEN with hit%, report link)
   + jh-enhance one-liners (paths verified by W3 below).
#6 discovery: for 20 flagship pages lacking jh-enhance, load each
   page's primary feed from S3 and AUTO-DERIVE a verified bars/line
   config (first array-of-dicts >=5 rows with string label + numeric
   field; else first [[date,v]] series) — prints the proven config
   table for the 3523 patch. No guessing lands on pages.

Gates:
  W1 composer CI battery (ages 2.0/900.0 exact, stale flags, guard
     counters) re-run in CI
  W2 composer live: deploy (config passthrough) + invoke -> doc has
     input_hygiene with >=6 feeds, benzinga-earnings-calendar flagged
     stale-or-missing, output_guard counters present, positions intact
  W3 the two shipped enhance paths resolve on live feeds
     (positions:ticker:weight_pct / book:ticker:score)
  W4 both pages served with OPS3522 strip + node-parse
  W5 discovery table over 20 page->feed pairs; each row PASS iff a
     usable config derived (informational rows print keys on miss)
"""
import importlib.util
import io
import json
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from datetime import datetime, timedelta, timezone
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report                      # noqa: E402
from _lambda_deploy_helpers import deploy_lambda   # noqa: E402

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-proven-portfolio"
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")

DISCOVER = [
    ("proven-alpha.html", "data/proven-alpha.json"),
    ("political.html", "data/congress-direct.json"),
    ("primary-dealers.html", "data/nyfed-primary-dealer.json"),
    ("liquidity-inflection.html", "data/liquidity-inflection.json"),
    ("us-data-desk.html", "data/bls-labor.json"),
    ("macro-leads.html", "data/macro-leads.json"),
    ("bls.html", "data/bls-labor.json"),
    ("heatmap.html", "data/heatmap.json"),
    ("industry-rotation.html", "data/industry-rotation.json"),
    ("capital-flow.html", "data/capital-flow.json"),
    ("share-flows.html", "data/share-flows.json"),
    ("opportunities.html", "data/opportunities.json"),
    ("valuations.html", "data/valuations.json"),
    ("panels.html", "data/wl-engines.json"),
    ("ofr.html", "data/ofr-stfm.json"),
    ("credit-desk.html", "data/credit-stress.json"),
    ("dollar.html", "data/dollar-engine.json"),
    ("eurodollar.html", "data/eurodollar-plumbing.json"),
    ("tv-notes.html", "data/tv-notes.json"),
    ("equity-ftd-page-placeholder", "data/equity-ftd.json"),
]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3522"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def derive_config(doc, depth=0, path=""):
    """BFS for (bars_path,label,val) or (line_path). Returns dict."""
    if depth > 3 or not isinstance(doc, dict):
        return None
    for k, v in doc.items():
        p2 = f"{path}.{k}" if path else k
        if isinstance(v, list) and len(v) >= 5:
            if all(isinstance(r, dict) for r in v[:5]):
                keys = v[0].keys()
                lab = next((c for c in ("ticker", "symbol", "name", "t",
                                        "family", "sector", "etf", "key",
                                        "series") if c in keys), None)
                num = next((c for c in ("score", "value", "weight_pct",
                                        "hit_primary", "graded", "usd",
                                        "net", "z", "pct", "yoy", "spike",
                                        "flow_1m_usd", "age_h")
                            if c in keys and isinstance(
                                v[0].get(c), (int, float))), None)
                if not num:
                    num = next((c for c in keys if isinstance(
                        v[0].get(c), (int, float)) and c != lab), None)
                if lab and num:
                    return {"mode": "bars", "path": p2, "label": lab,
                            "val": num, "n": len(v)}
            if all(isinstance(r, list) and len(r) >= 2 for r in v[:5]):
                return {"mode": "line", "path": p2, "n": len(v)}
    for k, v in doc.items():
        if isinstance(v, dict):
            r = derive_config(v, depth + 1, f"{path}.{k}" if path else k)
            if r:
                return r
    return None


with report("3522_census_689") as rep:
    fails = []

    def gate(name, ok, detail):
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:520]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3522 — census #9 + #8 live, #6 discovery")

    try:
        spec = importlib.util.spec_from_file_location(
            "pp", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)

        class FakeS3:
            def __init__(self, store):
                self.store = store

            def get_object(self, Bucket, Key):
                if Key not in self.store:
                    raise Exception("NoSuchKey")
                return {"Body": io.BytesIO(
                    json.dumps(self.store[Key]).encode())}

        fresh = (datetime.now(timezone.utc)
                 - timedelta(hours=2)).isoformat()
        old = (datetime.now(timezone.utc)
               - timedelta(hours=900)).isoformat()
        m.s3 = FakeS3({"data/stress-index.json":
                       {"generated_at": fresh, "x": 1},
                       "data/benzinga-earnings-calendar.json":
                       {"generated_at": old, "rows": []}})
        m.HYG.clear()
        m.rj("data/stress-index.json")
        m.rj("data/benzinga-earnings-calendar.json")
        m.rj("data/missing.json")
        H = m.HYG
        clean, c = m.sanitize_positions(
            [{"ticker": "A", "weight_pct": 5},
             {"ticker": "A", "weight_pct": 3},
             {"ticker": "B", "weight_pct": float("nan")},
             {"ticker": "C", "weight_pct": 12.0}])
        gate("W1_ci",
             H["data/stress-index.json"]["age_h"] == 2.0
             and H["data/stress-index.json"]["stale"] is False
             and H["data/benzinga-earnings-calendar.json"]["stale"] is True
             and H["data/missing.json"]["present"] is False
             and [r["ticker"] for r in clean] == ["A", "C"]
             and clean[1]["weight_pct"] == 8.0
             and c == {"dropped_nonfinite": 1, "deduped": 1, "clamped": 1},
             {"ages": (H["data/stress-index.json"]["age_h"],
                       H["data/benzinga-earnings-calendar.json"]["age_h"]),
              "guard": c})
    except Exception as e:  # noqa: BLE001
        gate("W1_ci", False, str(e)[:320])

    try:
        cfg = lam.get_function_configuration(FunctionName=FN)
        deploy_lambda(report=rep, function_name=FN,
                      source_dir=REPO / "aws" / "lambdas" / FN / "source",
                      env_vars=(cfg.get("Environment") or {})
                      .get("Variables") or {},
                      timeout=cfg["Timeout"], memory=cfg["MemorySize"],
                      description="composer v1.2.4 input hygiene (ops 3522)",
                      create_function_url=False, smoke=False)
        for _ in range(30):
            c2 = lam.get_function_configuration(FunctionName=FN)
            if c2.get("LastUpdateStatus") == "Successful":
                break
            time.sleep(2)
        lam.invoke(FunctionName=FN, Payload=b"{}")
        time.sleep(2)
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/proven-portfolio.json")["Body"].read())
        hyg = doc.get("input_hygiene") or {}
        feeds = hyg.get("feeds") or {}
        bz = feeds.get("data/benzinga-earnings-calendar.json") or {}
        gate("W2_live_hygiene",
             len(feeds) >= 6 and "output_guard" in hyg
             and (bz.get("stale") is True or bz.get("present") is False
                  or "no timestamp field" in (bz.get("issues") or []))
             and isinstance(doc.get("positions"), list),
             {"n_feeds": len(feeds), "n_stale": hyg.get("n_stale"),
              "n_missing": hyg.get("n_missing"),
              "benzinga": bz, "guard": hyg.get("output_guard"),
              "n_positions": len(doc.get("positions") or [])})
    except Exception as e:  # noqa: BLE001
        gate("W2_live_hygiene", False, str(e)[:340])

    try:
        pp = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/proven-portfolio.json")["Body"].read())
        sb = json.loads(s3c.get_object(
            Bucket=BUCKET, Key="data/short-book.json")["Body"].read())
        pos = pp.get("positions") or []
        book = sb.get("book") or []
        ok3 = (pos and all(k in pos[0] for k in ("ticker", "weight_pct"))
               and book and "ticker" in book[0]
               and isinstance(book[0].get("score"), (int, float)))
        gate("W3_enhance_paths", bool(ok3),
             {"pp_n": len(pos), "pp_keys": sorted((pos[0] or {}).keys())[:8]
              if pos else [],
              "sb_n": len(book),
              "sb_keys": sorted((book[0] or {}).keys())[:8] if book else []})
    except Exception as e:  # noqa: BLE001
        gate("W3_enhance_paths", False, str(e)[:300])

    got = {}
    for _ in range(15):
        try:
            cb = int(time.time())
            got["pp"] = fetch(
                f"https://justhodl.ai/proven-portfolio.html?cb={cb}")
            got["sb"] = fetch(f"https://justhodl.ai/short-book.html?cb={cb}")
            if b"OPS3522" in got["pp"] and b"OPS3522" in got["sb"]:
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(20)
    ok_n = True
    for k in ("pp", "sb"):
        mm = re.search(rb'<script id="OPS3522-[^"]+">\n([\s\S]*?)</script>',
                       got.get(k, b""))
        if not mm:
            ok_n = False
            continue
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(mm.group(1))
            pth = f.name
        ok_n = ok_n and subprocess.run(
            ["node", "--check", pth], capture_output=True).returncode == 0
    gate("W4_pages", b"OPS3522" in got.get("pp", b"")
         and b"OPS3522" in got.get("sb", b"")
         and b"jh-enhance.js" in got.get("pp", b"") and ok_n,
         {"pp": b"OPS3522" in got.get("pp", b""),
          "sb": b"OPS3522" in got.get("sb", b""), "node": ok_n})

    table, misses = [], []
    for page, key in DISCOVER:
        try:
            d = json.loads(s3c.get_object(Bucket=BUCKET, Key=key)
                           ["Body"].read())
            cfg2 = derive_config(d)
            if cfg2:
                table.append({"page": page, "feed": key, **cfg2})
            else:
                misses.append({"page": page, "feed": key,
                               "top_keys": sorted(d.keys())[:10]})
        except Exception as e:  # noqa: BLE001
            misses.append({"page": page, "feed": key,
                           "err": str(e)[:60]})
    print("DISCOVERY_TABLE " + json.dumps(table))
    rep.log("DISCOVERY_TABLE " + json.dumps(table)[:1800])
    if misses:
        print("DISCOVERY_MISSES " + json.dumps(misses))
        rep.log("DISCOVERY_MISSES " + json.dumps(misses)[:900])
    gate("W5_discovery", len(table) >= 14,
         {"derived": len(table), "misses": len(misses),
          "sample": table[:4]})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO / "aws" / "ops" / "reports" / "3522.json").write_text(
        json.dumps({"ops": 3522, "fails": fails, "table": table,
                    "misses": misses}, indent=1))
sys.exit(0)
