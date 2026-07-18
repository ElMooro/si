"""ops 3463 — close G4 from ops 3462 (Fundamental Graphs URL CORS + gzip).

Root-cause instrumentation: ops-3462's probe read headers from dict(r.headers)
with capitalized names; Lambda URLs emit lowercase. This ops (a) prints the
ACTUAL Function-URL Cors config and repairs it if absent, (b) redeploys
engine v1.0.1 (case-insensitive header lookup + ?gz=1 force), (c) probes with
case-insensitive reads and gates hard.
"""
import gzip
import io
import json
import sys
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
MARKER = "FUNDGRAPH_V1_OPS3462"
CORS = {"AllowCredentials": False, "AllowHeaders": ["content-type"],
        "AllowMethods": ["*"], "AllowOrigins": ["*"], "MaxAge": 86400}

lam = boto3.client("lambda", region_name="us-east-1")


def probe(url, hdrs):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3463", **hdrs})
    with urllib.request.urlopen(req, timeout=45) as r:
        h = r.headers                                   # case-insensitive get()
        enc = (h.get("Content-Encoding") or "").lower()
        raw = r.read()
        body = gzip.GzipFile(fileobj=io.BytesIO(raw)).read() if enc == "gzip" else raw
        return r.status, {"acao": h.get("Access-Control-Allow-Origin"),
                          "enc": enc, "wire": len(raw), "plain": len(body)}, body


with report("3463_fundgraph_g4_close") as rep:
    out = {"ops": 3463, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:380]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:340]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3463 — Fundamental Graphs G4 close (CORS + gzip)")

    # 1. inspect + repair URL-level CORS
    cfg = lam.get_function_url_config(FunctionName=FN)
    url = cfg["FunctionUrl"].rstrip("/")
    cur = cfg.get("Cors") or {}
    rep.log(f"current Cors: {json.dumps(cur)}")
    print("current Cors:", json.dumps(cur))
    if (cur.get("AllowOrigins") or []) != ["*"]:
        lam.update_function_url_config(FunctionName=FN, AuthType="NONE", Cors=CORS)
        rep.log("Cors repaired -> AllowOrigins ['*']")
        time.sleep(2)
    gate("H1_url_cors_config", True, {"url": url, "had": cur.get("AllowOrigins")})

    # 2. redeploy v1.0.1
    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=120, memory=512,
        description="Fundamental Graphs API v1.0.1 (ops 3463 G4 close)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)

    # 3. probes (case-insensitive header reads)
    st1, h1, b1 = probe(f"{url}/?symbol=AAPL&period=quarter",
                        {"Origin": "https://justhodl.ai", "Accept-Encoding": "gzip"})
    d1 = json.loads(b1)
    gate("H2_cors_star", st1 == 200 and h1["acao"] == "*", h1)
    gate("H3_gzip_header_path",
         h1["enc"] == "gzip" and h1["wire"] < h1["plain"] * 0.5
         and d1.get("ok") and d1.get("marker") == MARKER,
         {**h1, "version": d1.get("version")})

    st2, h2, b2 = probe(f"{url}/?symbol=CHTR&period=quarter&gz=1", {})
    d2 = json.loads(b2)
    gate("H4_gz_query_force", st2 == 200 and h2["enc"] == "gzip" and d2.get("ok"),
         {**h2, "sym": d2.get("symbol")})

    out["function_url"] = url
    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3463.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
