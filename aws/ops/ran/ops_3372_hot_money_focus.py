"""ops 3372 — hot-money drilldowns: fix + Asia/Europe standing focus, E2E gates.

Khalid: hot-money.html "Inside the hot countries" unpopulated; wants HK,
Taiwan, South Korea, China, other Asia hubs and Europe always covered.
Root causes patched in v1.4.0 (this push): (1) FMP /stable/etf/* calls had
no rename resilience — non-list responses silently zeroed sectors/holdings
(the 2026 rename class); now endpoint LADDERS + %-tolerant weight parsing +
field-name fallbacks. (2) Drill only ran for top-6 inflow rank — Asia/EU
absent whenever LatAm/MEA led. Now a STANDING FOCUS set (7 Asia hubs + 8
Europe) drills every run, focus:true, shared _drill_one code path, batch
FMP quotes for foreign holdings. Page (additive): 🎯 focus badge + legend.

Gates:
  G1  deploy settled: LastUpdateStatus==Successful AND deployed zip carries
      "1.4.0" (3342 race doctrine)
  G2  fresh run: Event invoke → S3 data/hot-money.json version 1.4.0,
      generated_at > invoke time (poll ≤840s)
  G3  FOCUS coverage: Hong Kong, Taiwan, South Korea, China ALL present in
      drilldowns with ≥3 sectors AND ≥8 holdings each
  G4  ≥5 Europe focus countries present & populated; ≥5 Asia hubs total
  G5  momentum wired: ≥60% of focus drills have some day_chg_pct
  G6  live page carries 🎯 focus markers (pages deploy poll)
"""

import base64
import io
import json
import sys
import time
import urllib.error
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

FN = "justhodl-hot-money"
FEED = "https://justhodl.ai/data/hot-money.json"
PAGE = "https://justhodl.ai/hot-money.html"
UA = {"User-Agent": "Mozilla/5.0 (X11; Linux x86_64) ops-3372"}
ASIA = ["Hong Kong", "Taiwan", "South Korea", "China", "Japan", "Singapore", "India"]
EUR = ["Germany", "UK", "France", "Switzerland", "Netherlands", "Italy", "Spain", "Sweden"]
LAM = boto3.client("lambda", "us-east-1")


def req(url, timeout=25):
    r = urllib.request.Request(url + ("&" if "?" in url else "?") + f"t={int(time.time())}",
                               headers=UA)
    try:
        with urllib.request.urlopen(r, timeout=timeout) as resp:
            return resp.status, resp.read()
    except urllib.error.HTTPError as e:
        return e.code, e.read()
    except Exception as e:  # noqa: BLE001
        return -1, str(e).encode()[:200]


def zip_has_marker(marker):
    try:
        info = LAM.get_function(FunctionName=FN)
        st, body = req(info["Code"]["Location"])
        if st != 200:
            return False
        zf = zipfile.ZipFile(io.BytesIO(body))
        src = zf.read("lambda_function.py").decode("utf-8", "replace")
        return marker in src
    except Exception as e:  # noqa: BLE001
        print("[zip]", str(e)[:80])
        return False


def main(rep):
    out = {"gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:320]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:260]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    # G1 — deploy settled + marker (parallel deploy-lambdas race)
    ok1, status = False, "?"
    deadline = time.time() + 300
    while time.time() < deadline:
        cfg = LAM.get_function_configuration(FunctionName=FN)
        status = cfg.get("LastUpdateStatus")
        if status == "Successful" and zip_has_marker('VERSION = "1.4.0"'):
            ok1 = True
            break
        time.sleep(12)
    gate("G1_deploy_settled_v140", ok1, f"LastUpdateStatus={status}")

    # G2 — fresh run
    t_inv = datetime.now(timezone.utc).isoformat()
    LAM.invoke(FunctionName=FN, InvocationType="Event", Payload=b"{}")
    feed, ok2 = None, False
    deadline = time.time() + 840
    while time.time() < deadline:
        st, body = req(FEED)
        if st == 200:
            try:
                j = json.loads(body)
                if j.get("version") == "1.4.0" and (j.get("generated_at") or "") > t_inv:
                    feed, ok2 = j, True
                    break
            except Exception:  # noqa: BLE001
                pass
        time.sleep(20)
    gate("G2_fresh_v140_feed", ok2,
         f"version={feed.get('version') if feed else None} at={feed.get('generated_at', '')[:19] if feed else None}")
    if not feed:
        out["verdict"] = "GAPS: " + ",".join(fails)
        Path("aws/ops/reports/3372.json").write_text(json.dumps(out, indent=2))
        rep.log("VERDICT: " + out["verdict"])
        sys.exit(0)

    dd = feed.get("drilldowns") or {}
    out["drill_countries"] = sorted(dd.keys())

    def populated(c):
        v = dd.get(c) or {}
        return len(v.get("top_sectors") or []) >= 3 and len(v.get("top_holdings") or []) >= 8

    core = ["Hong Kong", "Taiwan", "South Korea", "China"]
    core_state = {c: populated(c) for c in core}
    gate("G3_core_asia_populated", all(core_state.values()), json.dumps(core_state))

    eu_pop = [c for c in EUR if populated(c)]
    asia_pop = [c for c in ASIA if populated(c)]
    gate("G4_focus_breadth", len(eu_pop) >= 5 and len(asia_pop) >= 5,
         f"europe={len(eu_pop)}{eu_pop[:6]} asia={len(asia_pop)}")

    foc = [c for c in (ASIA + EUR) if c in dd]
    with_mom = [c for c in foc
                if any(h.get("day_chg_pct") is not None for h in (dd[c].get("top_holdings") or []))]
    gate("G5_momentum_wired", foc and len(with_mom) >= 0.6 * len(foc),
         f"{len(with_mom)}/{len(foc)} focus drills carry day_chg_pct")

    ok6 = False
    deadline = time.time() + 240
    while time.time() < deadline:
        st, body = req(PAGE)
        if st == 200 and "\U0001f3af focus".encode() in body and b"standing focus" in body:
            ok6 = True
            break
        time.sleep(15)
    gate("G6_page_focus_markers", ok6, f"http {st}")

    ex = dd.get("Hong Kong") or {}
    out["hk_sample"] = {"sectors": (ex.get("top_sectors") or [])[:3],
                       "holdings": [h.get("ticker") for h in (ex.get("top_holdings") or [])[:6]]}
    out["verdict"] = "PASS_ALL" if not fails else "GAPS: " + ",".join(fails)
    print("\nVERDICT:", out["verdict"], "| HK sample:", out["hk_sample"])
    rep.log("VERDICT: " + out["verdict"])
    Path("aws/ops/reports/3372.json").write_text(json.dumps(out, indent=2))
    sys.exit(0)


with report("3372_hot_money_focus") as _rep:
    _rep.heading("ops 3372 — hot-money Asia/Europe focus drilldowns")
    main(_rep)
