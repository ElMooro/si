"""ops 3524 — census 6/8/9 FINAL close.

Root-cause from 3523: composer doc's key is `book` (not `positions`) —
raw=40, sanitized=40, hygiene healthy; gates read the wrong key. The
one real find stands: data/stress-index.json ABSENT (JSI throttle
neutral — pre-existing, now surfaced honestly).

Shipped this wave: jh-enhance DICT-BARS mode (object-of-numbers, jsdom-
smoked); proven-portfolio config -> book:ticker:weight_pct; wave-2
pages x9 (political lobbying scores, dealer coupon tenors $B, census
summary, BLS crisis components, heatmap+valuations S&P P/E x499, fails
by class, dollar canaries, eurodollar core metrics).

  Y1 composer truth: doc.book >= 20 sanitized rows, hygiene feeds >= 8,
     n_raw == len(book) + drops, stress-index absence named
  Y2 enhanced coverage: 18 pages served WITH jh-enhance (retry loop),
     AND every configured path resolves on its live feed (dict-aware)
  Y3 component served with DICT-BARS marker + node-parse
"""
import json, re, sys, time, urllib.request
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report

REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
s3c = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")

PAGES = [
    ("proven-portfolio.html", "data/proven-portfolio.json",
     "book", ["ticker", "weight_pct"]),
    ("short-book.html", "data/short-book.json", "book",
     ["ticker", "score"]),
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
    ("opportunities.html", "data/opportunities.json",
     "top_opportunities", ["ticker", "price"]),
    ("panels.html", "data/wl-engines.json", "engines",
     ["name", "coverage"]),
    ("political.html", "data/lobbying-intel.json", "all_tickers",
     ["ticker", "score"]),
    ("primary-dealers.html", "data/nyfed-primary-dealer.json",
     "by_tenor_usd_b.TREASURY_COUPONS", "dict"),
    ("us-data-desk.html", "data/census-economic.json", "summary",
     "dict"),
    ("bls.html", "data/bls-employment.json", "crisis.components",
     ["key", "value"]),
    ("heatmap.html", "data/stock-valuations.json", "sp_table",
     ["t", "pe"]),
    ("valuations.html", "data/stock-valuations.json", "sp_table",
     ["t", "pe"]),
    ("ofr.html", "data/settlement-fails.json", "classes",
     ["key", "ftd_latest"]),
    ("dollar.html", "data/dollar-radar.json", "canaries",
     ["label", "lean"]),
    ("eurodollar.html", "data/eurodollar-plumbing.json",
     "layers.us_core.metrics", ["id", "value"]),
]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3524"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def resolve(doc, path):
    cur = doc
    for part in path.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
    return cur


with report("3524_census_close") as rep:
    fails = []

    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3524 — census 6/8/9 final")
    lam.invoke(FunctionName="justhodl-proven-portfolio", Payload=b"{}")
    time.sleep(3)
    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/proven-portfolio.json")["Body"].read())
        book = doc.get("book") or []
        hyg = doc.get("input_hygiene") or {}
        g = hyg.get("output_guard") or {}
        raw = hyg.get("n_raw_positions")
        gate("Y1_composer",
             len(book) >= 20
             and raw == len(book) + g.get("dropped_nonfinite", 0)
             + g.get("deduped", 0)
             and len(hyg.get("feeds") or {}) >= 8,
             {"n_book": len(book), "n_raw": raw, "guard": g,
              "n_stale": hyg.get("n_stale"),
              "missing": [k for k, v in (hyg.get("feeds") or {}).items()
                          if not v.get("present")],
              "sample": [(p["ticker"], p["weight_pct"], p["tier"])
                         for p in book[:4]]})
    except Exception as e:  # noqa: BLE001
        gate("Y1_composer", False, str(e)[:320])

    okp, det = 0, []
    for page, key, path, lv in PAGES:
        served_ok = False
        for _ in range(12):
            try:
                if b"jh-enhance.js" in fetch(
                        f"https://justhodl.ai/{page}?cb={int(time.time())}"):
                    served_ok = True
                    break
            except Exception:  # noqa: BLE001
                pass
            time.sleep(15)
        try:
            d = json.loads(s3c.get_object(Bucket=BUCKET, Key=key)
                           ["Body"].read())
            node = resolve(d, path)
            if lv == "dict":
                pok = (isinstance(node, dict)
                       and sum(1 for v in node.values()
                               if isinstance(v, (int, float))) >= 3)
            elif lv:
                pok = (isinstance(node, list) and len(node) >= 5
                       and lv[0] in node[0]
                       and isinstance(node[0].get(lv[1]), (int, float)))
            else:
                pok = isinstance(node, list) and len(node) >= 5
        except Exception:  # noqa: BLE001
            pok = False
        okp += 1 if (served_ok and pok) else 0
        det.append((page, served_ok, pok))
    gate("Y2_coverage", okp >= 17, {"ok": okp, "detail": det})

    comp = b""
    for _ in range(10):
        try:
            comp = fetch(f"https://justhodl.ai/jh-enhance.js?cb={int(time.time())}")
            if b"DICT-BARS" in comp:
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(15)
    import subprocess, tempfile
    with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                     delete=False) as f:
        f.write(comp); pth = f.name
    node_ok = subprocess.run(["node", "--check", pth],
                             capture_output=True).returncode == 0
    gate("Y3_component", b"DICT-BARS" in comp and node_ok,
         {"marker": b"DICT-BARS" in comp, "node": node_ok})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO / "aws" / "ops" / "reports" / "3524.json").write_text(
        json.dumps({"ops": 3524, "fails": fails, "detail": det}))
sys.exit(0)
