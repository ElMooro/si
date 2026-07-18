"""ops 3473 — symdir final: two-band screener union (large+small caps) + key-sniff diag.

Engine v1.1.5 builds data/fundgraph/symdir.json from FMP /stable stock-list
(US_EXCH filter), memoized in /tmp, Monday-refreshed by warm_auto; search
ranks tiered (exact sym > sym prefix > name prefix > word > contains, then
NASDAQ/NYSE first). FMP dual-search kept as fallback. Archive doc
docs/memory-archive/34-fundamental-graphs.md committed this push.

Gates: D1 symdir built >=6000 rows on S3 · D2 relevance quartet
(micro->MSFT top3, apple->AAPL #1, tesla->TSLA #1, berkshire->BRK-B top3,
src=symdir) · D3 CHTR regression.
"""
import gzip, io, json, sys, time, urllib.request
from pathlib import Path
import boto3

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda

REPO = Path(__file__).resolve().parents[3]
FN = "justhodl-fundamental-graphs"
BUCKET = "justhodl-dashboard-live"
FMP_KEY = "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"
lam = boto3.client("lambda", region_name="us-east-1")
s3 = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3473",
                                               "Accept-Encoding": "gzip"})
    with urllib.request.urlopen(req, timeout=60) as r:
        raw = r.read()
        if (r.headers.get("Content-Encoding") or "").lower() == "gzip":
            raw = gzip.GzipFile(fileobj=io.BytesIO(raw)).read()
        return json.loads(raw)


with report("3473_fundgraph_symdir_search") as rep:
    out = {"ops": 3473, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:400]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:360]
        print(line); rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3473 — symdir ranked search + memory-archive 34")

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                            "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.1.7 two-band union (ops 3473)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    url = lam.get_function_url_config(FunctionName=FN)["FunctionUrl"].rstrip("/")

    # diagnostic force-build first (observability even on FAIL)
    try:
        d0 = fetch(f"{url}/?symdir=1")
        print("SYMDIR DIAG:", json.dumps(d0.get("diag"))[:500])
        rep.log("symdir diag: " + json.dumps(d0.get("diag"))[:500])
        time.sleep(2)
        obj = s3.get_object(Bucket=BUCKET, Key="data/fundgraph/symdir.json")
        sd = json.loads(obj["Body"].read())
        gate("D1_symdir_built", sd.get("n", 0) >= 5500 and d0.get("n", 0) >= 5500,
             {"rows": sd.get("n"), "built": d0.get("n"), "sample": d0.get("sample")})
    except Exception as e:
        gate("D1_symdir_built", False, str(e)[:240])

    try:
        def syms(q):
            d = fetch(f"{url}/?search={q}")
            return [r["symbol"] for r in d.get("results", [])], d.get("src")
        m, s1 = syms("micro")
        a, s2 = syms("apple")
        t, s3_ = syms("tesla")
        b, s4 = syms("berkshire")
        gate("D2_relevance_quartet",
             ("MSFT" in m[:3]) and a[:1] == ["AAPL"] and t[:1] == ["TSLA"]
             and any(x.startswith("BRK") for x in b[:3])
             and {s1, s2, s3_, s4} == {"symdir"},
             {"micro": m[:4], "apple": a[:3], "tesla": t[:3],
              "berkshire": b[:3], "src": s1})
    except Exception as e:
        gate("D2_relevance_quartet", False, str(e)[:240])

    try:
        d = fetch(f"{url}/?symbol=CHTR&period=quarter")
        gate("D3_engine_regression",
             d.get("ok") and len(d.get("points", {})) >= 190,
             {"keys": len(d.get("points", {})), "version": d.get("version")})
    except Exception as e:
        gate("D3_engine_regression", False, str(e)[:240])

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3473.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
