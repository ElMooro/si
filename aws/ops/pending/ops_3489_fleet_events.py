"""ops 3489 — Tier-2c: congress + insider markers on the tape (fleet fusion).

Engine v1.4.0 (cache v14) EVENTS_ENGINE_OPS3489: pure parsers over three
existing fleet feeds — data/congress-direct.json (senate.transactions:
filer/tx_date/type/amount, MM/DD/YYYY normalized, Purchase/Sale side),
data/insider-trades.json (big_buys >$1M + buy clusters), and
data/insider-sell-cluster.json (WINDOW-based: n_distinct_sellers /
total_sale_value_usd, no per-cluster dates -> marked at feed
generated_at as a "selling now" flag; schema audited from the writer).
Core v6: events rail — congress ◆ (purple-stroked, side-colored) +
insider ▲▼ triangles with full tooltips; micro-proven (2/2/4). Both
surfaces gain an Events toggle (single-symbol, ?ev=1 on flagship).

Gates:
  V1 CI parser units (date-normalize, side-classify, sell-window rule)
  V2 self-selecting REAL-DATA gate: read live congress feed, take its
     first tickered symbol, warm it, assert doc.events.congress >= 1
     with matching filer name
  V3 served-JS 4-surface node-check + core OPS3489 + events strings
  V4 surfaces live: flagship evtbtn/ops3489 + module jhfgEvt; all prior
     markers intact (whales/flags/marks/rtbtn/ops3475)
"""
import importlib.util
import json
import os
import re
import subprocess
import sys
import tempfile
import time
import urllib.request
from pathlib import Path

import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report                      # noqa: E402
from _lambda_deploy_helpers import deploy_lambda   # noqa: E402

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3489"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


with report("3489_fleet_events") as rep:
    out = {"ops": 3489, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:380]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3489 — congress + insider events on the tape")

    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        cg = [{"ticker": "AAPL", "tx_date": "03/15/2026", "filer": "Jane Doe",
               "type": "Purchase", "amount": "$15,001 - $50,000"},
              {"ticker": "AAPL", "tx_date": "2026-04-02", "filer": "John Roe",
               "type": "Sale (Full)", "amount": "$1,001 - $15,000"},
              {"ticker": "MSFT", "tx_date": "03/15/2026", "filer": "X",
               "type": "Purchase", "amount": "$1"}]
        r1 = m.parse_congress_rows(cg, "AAPL")
        ib = {"big_buys": [{"ticker": "AAPL", "filed_at": "2026-05-01T12:00:00",
                            "insider": "Tim C", "role": "CEO",
                            "value": 2500000}]}
        isl = {"generated_at": "2026-06-01T09:00:00Z",
               "clusters": [{"ticker": "AAPL", "n_distinct_sellers": 4,
                             "total_sale_value_usd": 9000000}]}
        r2 = m.parse_insider_feeds(ib, isl, "AAPL")
        gate("V1_parser_units",
             r1[0] == ["2026-03-15", "Jane Doe", "B", "$15,001 - $50,000"]
             and r1[1][2] == "S" and len(r1) == 2
             and r2[-1] == ["2026-06-01", "SELL CLUSTER 4 insiders (30d)",
                            "S", 9000000],
             {"congress": r1, "insiders": r2})
    except Exception as e:  # noqa: BLE001
        gate("V1_parser_units", False, str(e)[:260])

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs v1.4.0 fleet events (ops 3489)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)

    try:
        feed = json.loads(s3c.get_object(
            Bucket=BUCKET, Key="data/congress-direct.json")["Body"].read())
        txs = ((feed.get("senate") or {}).get("transactions")) or []
        pick = next((t for t in txs if t.get("ticker")), None)
        if not pick:
            gate("V2_realdata_selfselect", False,
                 {"reason": "no tickered congress tx in live feed"})
        else:
            sym = pick["ticker"].upper()
            lam.invoke(FunctionName=FN, Payload=json.dumps(
                {"warm": [sym], "periods": ["quarter"],
                 "refresh": True}).encode())
            doc = json.loads(s3c.get_object(
                Bucket=BUCKET,
                Key=f"data/fundgraph/cache/{sym}_quarter_v14.json")["Body"].read())
            ev = (doc.get("events") or {})
            names = [e[1] for e in ev.get("congress") or []]
            gate("V2_realdata_selfselect",
                 len(ev.get("congress") or []) >= 1
                 and any(str(pick.get("filer") or "")[:20] in n
                         for n in names),
                 {"symbol": sym, "congress_n": len(ev.get("congress") or []),
                  "insiders_n": len(ev.get("insiders") or []),
                  "sample": (ev.get("congress") or [])[:2],
                  "picked_filer": pick.get("filer")})
    except Exception as e:  # noqa: BLE001
        gate("V2_realdata_selfselect", False, str(e)[:260])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["core"] = fetch(f"https://justhodl.ai/fg-chart.js?cb={cb}")
            got["cat"] = fetch(f"https://justhodl.ai/fg-catalog.js?cb={cb}")
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"OPS3489" in got["core"] and b"ops3489" in got["flag"] \
               and b"jhfgEvt" in got["why"]:
                break
        except Exception as e:  # noqa: BLE001
            got["err"] = str(e)[:120]
        time.sleep(20)
    checks = [node_ok(got.get("core", b"x=")), node_ok(got.get("cat", b"x="))]
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>",
                   got.get("flag", b""))
    checks.append(node_ok(m1.group(1) if m1 else b"x="))
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>',
                   got.get("why", b""))
    checks.append(node_ok(m2.group(1) if m2 else b"x="))
    gate("V3_served_js", all(checks) and b"OPS3489" in got.get("core", b"")
         and b"CONGRESS" in got.get("core", b""), {"node_ok": checks})
    f = got.get("flag", b"")
    y = got.get("why", b"")
    d4 = {"flag_ops3489": b"ops3489" in f, "evtbtn": b"evtbtn" in f,
          "rt_intact": b"rtbtn" in f, "whales_intact": b"whales_q" in f,
          "flags_intact": b"fgFlags" in f, "marks_intact": b"mkclr" in f,
          "why_evt": b"jhfgEvt" in y, "tdz_intact": b"ops3475" in y}
    gate("V4_surfaces", all(d4.values()), d4)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3489.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
