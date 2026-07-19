"""ops 3505 — the verdict layer now judges EVERYTHING: fundamentals,
sector position, trends, AND the technical state; why.html renders the
identical three-card block (greens w/ elite count, reds, red-flag
digest) the flagship has.

Engine v1.9.0 (cache v19):
- NEW fundamental rules: ROE (elite >=40), ROA (fin-suppressed, elite
  >=20), Inventory-days (DIO) 3y trend red on +20d build.
- TECH-BASIS VERDICTS (basis 'tech', never fin-suppressed, never
  elite): price vs 200-DMA (G/R with %), bull MA stack, 50/200 regime
  with the cross date (golden = G sev1, death = R sev2), confirmed
  double top (R sev2) / bottom (G sev2) with neckline, RSI >=80
  overbought warn, RVOL >=3x unusual-activity warn.
- greens list cap 12 -> 14 (headline counts stay exact totals).
Module: old inline chips + 3-chip flags strip replaced by the flagship
cards — green header "TICKER green flags (n · ⭐e elite)", red header,
and the FULL red-flag digest card (● lines, click-to-chart evidence);
duplicate stale renderer excised (single data-vk emitter proven).
Harness v12 = 33 behaviors.

Gates:
  F1 CI battery: tech-up fixture yields 4 tech greens incl regime-with-
     date; tech-down yields 5 tech reds (below-200, DC regime sev2
     dated, dbl-top, RSI 83, RVOL 4.2x); tech verdicts fire under a
     FIN sector; None tech -> zero tech chips; ROE/ROA elites; DIO
     trend red; ROA fin-suppressed
  F2 NVDA live v19: tech-basis verdicts >=2, digest >=2 lines,
     n_elite >=8, headline numbers printed (expect the 15-green/11-
     elite neighbourhood Khalid pasted, now + tech state)
  F3 AAPL live v19: tech-basis >=2 (above-200 + regime expected),
     totals printed
  F4 surfaces: module has 'green flags (' + 'red-flag digest' +
     exactly ONE data-vk emitter in the module script + priors; node x2
"""
import importlib.util
import json
import re
import os
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3505"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b)
        p = f.name
    return subprocess.run(["node", "--check", p],
                          capture_output=True).returncode == 0


def flat(v, n=14):
    return [["2024-%02d-01" % (i % 12 + 1), v] for i in range(n)]


with report("3505_full_spectrum_verdicts") as rep:
    out = {"ops": 3505, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:500]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:460]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3505 — tech verdicts + module digest parity")

    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        TUP = {"status": {"last_close": 210, "ma200": 180, "ma50": 195,
                          "pct_vs_200": 16.7, "above_200": True,
                          "bull_stack": True, "rsi14": 62,
                          "patterns": [{"type": "DBL_BOTTOM",
                                        "confirmed": True,
                                        "d": "2026-05-02", "neck": 170.0,
                                        "level": 160.0}],
                          "volume": {"state": "ok", "rvol": 1.2}},
               "events": [["2025-09-01", "GC_50_200", "gc"]]}
        r = m.derive_verdicts({"roic_pct": flat(20)}, 4, "Technology",
                              {}, {}, TUP)
        tk = {x["k"]: x for x in r["greens"] + r["reds"]}
        t1 = (tk["px_vs_200"]["side"] == "G"
              and "since 2025-09-01" in tk["ma_regime"]["why"]
              and tk["dbl_bottom"]["basis"] == "tech"
              and tk["ma_stack"]["side"] == "G"
              and "rsi_hot" not in tk)
        TDN = {"status": {"last_close": 90, "ma200": 120, "ma50": 100,
                          "pct_vs_200": -25.0, "above_200": False,
                          "bull_stack": False, "rsi14": 83,
                          "patterns": [{"type": "DBL_TOP",
                                        "confirmed": True,
                                        "d": "2026-06-10", "neck": 95.0,
                                        "level": 110.0}],
                          "volume": {"state": "ok", "rvol": 4.2}},
               "events": [["2025-03-03", "DC_50_200", "dc"]]}
        r2 = m.derive_verdicts({"roic_pct": flat(20)}, 4,
                               "Financial Services", {}, {}, TDN)
        tk2 = {x["k"]: x for x in r2["greens"] + r2["reds"]}
        t2 = (tk2["px_vs_200"]["side"] == "R"
              and tk2["ma_regime"]["sev"] == 2
              and "since 2025-03-03" in tk2["ma_regime"]["why"]
              and tk2["dbl_top"]["side"] == "R"
              and "83" in tk2["rsi_hot"]["why"]
              and "4.2x" in tk2["rvol_hot"]["why"]
              and not any(x.get("elite") for x in r2["greens"]
                          if x["basis"] == "tech"))
        r3 = m.derive_verdicts({"roic_pct": flat(20)}, 4, "Technology",
                               {}, {}, None)
        t3 = not any(x["basis"] == "tech"
                     for x in r3["greens"] + r3["reds"])
        r4 = m.derive_verdicts(
            {"roe_pct": flat(45), "roa_pct": flat(22),
             "dio_days": [["2021-%02d-01" % (i % 12 + 1), 50]
                          for i in range(6)] + flat(80, 8)},
            4, "Technology", {}, {})
        k4 = {x["k"]: x for x in r4["greens"] + r4["reds"]}
        t4 = (k4["roe_pct"].get("elite") and k4["roa_pct"].get("elite")
              and k4["dio_days"]["side"] == "R")
        r5 = m.derive_verdicts({"roa_pct": flat(1.2)}, 4, "Banks", {}, {})
        t5 = not any(x["k"] == "roa_pct" for x in r5["reds"])
        gate("F1_ci_battery", all([t1, t2, t3, t4, t5]),
             {"tech_up": t1, "tech_down": t2, "no_tech_silent": t3,
              "roe_roa_dio": t4, "roa_fin_suppressed": t5})
    except Exception as e:  # noqa: BLE001
        gate("F1_ci_battery", False, str(e)[:320])

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs v1.9.0 tech verdicts (ops 3505)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if (c.get("LastUpdateStatus") == "Successful"
                and c.get("State") == "Active"):
            break
        time.sleep(2)
    lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["NVDA", "AAPL"], "periods": ["quarter"],
         "refresh": True}).encode())

    for sym, gname, min_elite in (("NVDA", "F2_nvda_live", 8),
                                  ("AAPL", "F3_aapl_live", 3)):
        try:
            doc = json.loads(s3c.get_object(
                Bucket=BUCKET,
                Key=f"data/fundgraph/cache/{sym}_quarter_v19.json"
            )["Body"].read())
            V = doc.get("verdicts") or {}
            allv = (V.get("greens") or []) + (V.get("reds") or [])
            techs = [x for x in allv if x.get("basis") == "tech"]
            fl = doc.get("flags") or []
            gate(gname,
                 len(techs) >= 2
                 and V.get("summary", {}).get("n_elite", 0) >= min_elite
                 and (sym != "NVDA" or len(fl) >= 2),
                 {"summary": V.get("summary"),
                  "tech_verdicts": [t["why"][:64] for t in techs][:6],
                  "n_digest": len(fl),
                  "digest": [f["msg"][:70] for f in fl][:3]})
        except Exception as e:  # noqa: BLE001
            gate(gname, False, str(e)[:300])

    got = {}
    for _ in range(18):
        try:
            cb = int(time.time())
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            got["flag"] = fetch(
                f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            if b"red-flag digest" in got["why"] \
               and b"green flags (" in got["why"]:
                break
        except Exception as e:  # noqa: BLE001
            got["err"] = str(e)[:120]
        time.sleep(20)
    y = got.get("why", b"")
    f = got.get("flag", b"")
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', y)
    mod = m2.group(1) if m2 else b""
    d4 = {"node": node_ok(mod)
          and node_ok((re.search(
              rb"<script>\n('use strict'[\s\S]*?)</script>", f)
              or [None, b"x="])[1]),
          "cards": b"green flags (" in mod
          and b"red-flag digest" in mod and b"\\u25cf" in mod,
          "single_emitter": mod.count(b'data-vk="') == 1,
          "priors": all(k in mod for k in
                        [b"jhfgTbl2", b"jhfgMxSel", b"jhfgTa",
                         b"volume_w", b"data-eye"])
          and b"fgverd" in f}
    gate("F4_surfaces", all(d4.values()), d4)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3505.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
