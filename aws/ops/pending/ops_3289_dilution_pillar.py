"""ops 3289 — DILUTION PILLAR 6 on why.html (Khalid: total outstanding
shares must be one of the MAIN pillars of stock analysis — BMNR dumped
nonstop in 2025 because its share count went ballistic). Extends
justhodl-equity-research to schema 2.3 with a dilution module computed
for ANY symbol from the already-fetched statements (annual+quarterly
diluted shares → 1y/3y/5y CAGR, SBC %rev, net buyback yield, verdict
SHRINKING→DEATH_SPIRAL, share-flows flags join), and renders it as
Pillar 6 inside the Financial Health section on why.html (additive).
Truth bands on AAPL (SHRINKING/STABLE, shares 13–18B, 1y CAGR −6..+2)
and a known-diluter sanity check (BMNR → HEAVY_DILUTION or
DEATH_SPIRAL when FMP covers it; tolerated-absent, warned)."""
import json
import sys
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path

import boto3

from ops_report import report

sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "shared"))
from _lambda_deploy_helpers import deploy_lambda  # noqa: E402

REGION = "us-east-1"
FN = "justhodl-equity-research"
LAM = boto3.client("lambda", region_name=REGION)
AWS_DIR = Path(__file__).resolve().parents[2]


def get(url):
    req = urllib.request.Request(url, headers={
        "User-Agent": "Mozilla/5.0 (jh-ops-3289)"})
    return urllib.request.urlopen(req, timeout=60).read().decode(
        "utf-8", "ignore")


def research(sym):
    """Sync-invoke the research fn the way why.html does, force fresh."""
    for pay in ({"queryStringParameters": {"ticker": sym,
                                           "refresh": "1"}},
                {"ticker": sym, "refresh": True},
                {"queryStringParameters": {"ticker": sym}}):
        try:
            r = LAM.invoke(FunctionName=FN,
                           Payload=json.dumps(pay).encode())
            body = json.loads(r["Payload"].read())
            if isinstance(body, dict) and "body" in body:
                body = json.loads(body["body"])
            if isinstance(body, dict) and (
                    body.get("dilution") is not None
                    or body.get("schema_version")):
                return body
        except Exception as e:
            print("  invoke %s: %s" % (sym, str(e)[:90]))
    return None


with report("3289_dilution_pillar") as rep:
    fails, warns = [], []
    live = LAM.get_function_configuration(FunctionName=FN)
    env = (live.get("Environment") or {}).get("Variables") or {}
    deploy_lambda(report=rep, function_name=FN,
                  source_dir=AWS_DIR / "lambdas" / FN / "source",
                  env_vars=env, eb_rule_name=None, eb_schedule=None,
                  timeout=int(live.get("Timeout") or 300),
                  memory=int(live.get("MemorySize") or 1024),
                  description=str(live.get("Description") or "")[:250],
                  smoke=False)
    time.sleep(8)

    rep.section("2. AAPL truth bands (dilution module)")
    d = research("AAPL")
    dil = (d or {}).get("dilution") or {}
    rep.kv(schema=(d or {}).get("schema_version"),
           verdict=dil.get("verdict"),
           sh_1y=dil.get("sh_1y_cagr_pct"),
           latest=dil.get("latest_shares"),
           bb_yield=dil.get("net_buyback_yield_pct"))
    if str((d or {}).get("schema_version")) != "2.3":
        fails.append("schema not 2.3: %s"
                     % (d or {}).get("schema_version"))
    if dil.get("verdict") not in ("SHRINKING", "STABLE"):
        fails.append("AAPL verdict off: %s" % dil.get("verdict"))
    ls = dil.get("latest_shares") or 0
    if not (13e9 <= ls <= 18e9):
        fails.append("AAPL shares outside band: %s" % ls)
    c1 = dil.get("sh_1y_cagr_pct")
    if c1 is None or not (-6.0 <= c1 <= 2.0):
        fails.append("AAPL 1y share CAGR outside band: %s" % c1)
    if len(dil.get("annual_series") or []) < 5:
        fails.append("AAPL annual share series thin")

    rep.section("3. Known-diluter sanity (BMNR)")
    d2 = research("BMNR")
    dl2 = (d2 or {}).get("dilution") or {}
    rep.kv(bmnr_verdict=dl2.get("verdict"),
           bmnr_1y=dl2.get("sh_1y_cagr_pct"))
    if dl2:
        if dl2.get("verdict") in ("HEAVY_DILUTION", "DEATH_SPIRAL"):
            rep.log("  BMNR correctly graded %s" % dl2["verdict"])
        elif dl2.get("verdict") in ("DILUTING",):
            warns.append("BMNR graded only DILUTING (data window)")
        elif dl2.get("verdict") == "UNKNOWN":
            warns.append("BMNR share history unavailable via FMP")
        else:
            warns.append("BMNR unexpected verdict %s"
                         % dl2.get("verdict"))
    else:
        warns.append("BMNR research doc unavailable")

    rep.section("4. why.html Pillar 6 live")
    ok = False
    for i in range(20):
        try:
            pg = get("https://justhodl.ai/why.html?cb=%d" % time.time())
            if ("renderDilutionPillar" in pg
                    and "SHARE COUNT" in pg.upper()):
                ok = True
                break
        except Exception:
            pass
        time.sleep(18)
    if not ok:
        fails.append("why.html Pillar 6 not live")
    else:
        rep.log("  Pillar 6 markers live")

    rep.kv(warns=warns, fails=fails)
    if fails:
        raise SystemExit("FAILS: %s" % "; ".join(fails))
    rep.log("OPS 3289 PASS — share count is now a main pillar on "
            "why.html for ANY symbol.")
sys.exit(0)
