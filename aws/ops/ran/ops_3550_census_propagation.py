"""ops 3550 — CENSUS PROPAGATION (Khalid: "see what engines it can
enhance and do it"). The census matrix becomes a fleet-wide overlay:
a shared census_idx loader (conviction / combo / risk+tier / turn /
patterns / whale $M, tercile-tiered) injected into FIVE engines at
their per-ticker seams:
  · best-setups   -> setup.census_context (beside sector/playbook/
                     gamma context)
  · short-book    -> row.census_context (double_top + HIGH risk =
                     short corroboration)
  · equity-research -> doc.census (SaaS/API surface; why.html rides)
  · comeback-screener -> candidate.census_context (turn + conviction)
  · master-ranker -> top_tickers[].census (beside khalid_note)
why.html Vitals gains an additive census chip strip (self-contained
fetch; silent off-universe).

  V1 deploy x5, EACH zip-marker-proven (b"census_idx" in the live zip)
  V2 best-setups invoke -> >=1 setup carries census_context; sample
  V3 short-book invoke -> >=3 rows carry it; top short w/ context
  V4 equity-research NVDA force -> doc.census.conviction numeric
  V5 comeback invoke -> >=1 candidate carries it
  V6 master-ranker zip-only (scheduled engine; field self-activates
     next run) — marker gate
  V7 why.html served with OPS3550 + node
"""
import io, json, re, subprocess, sys, tempfile, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=600, retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")
ENGINES = ["justhodl-best-setups", "justhodl-short-book",
           "justhodl-equity-research", "justhodl-comeback-screener",
           "justhodl-master-ranker"]


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3550"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def zip_has(fn, marker=b"census_idx"):
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    src = zipfile.ZipFile(io.BytesIO(
        urllib.request.urlopen(loc, timeout=60).read())
        ).read("lambda_function.py")
    return marker in src


with report("3550_census_propagation") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:620]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    rep.heading("ops 3550 — census fleet propagation")
    marks = {}
    for fn in ENGINES:
        cfg = lam.get_function_configuration(FunctionName=fn)
        deploy_lambda(report=rep, function_name=fn,
                      source_dir=REPO/"aws"/"lambdas"/fn/"source",
                      env_vars=(cfg.get("Environment") or {})
                      .get("Variables") or {},
                      timeout=cfg["Timeout"], memory=cfg["MemorySize"],
                      description=(cfg.get("Description") or fn)[:200]
                      + " +census(3550)",
                      create_function_url=False, smoke=False)
        for _ in range(30):
            c = lam.get_function_configuration(FunctionName=fn)
            if c.get("LastUpdateStatus") == "Successful": break
            time.sleep(2)
        marks[fn] = zip_has(fn)
    gate("V1_zip_markers", all(marks.values()), marks)

    try:
        lam.invoke(FunctionName="justhodl-best-setups", Payload=b"{}")
        time.sleep(2)
        d = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/best-setups.json")["Body"].read())
        setups = d.get("setups") or d.get("rows") or []
        hit = [(x.get("ticker"), x["census_context"]["conviction"],
                x["census_context"]["risk_tier"],
                x["census_context"]["patterns"])
               for x in setups if x.get("census_context")][:4]
        gate("V2_best_setups", len(hit) >= 1,
             {"n_setups": len(setups), "with_census": len(
                 [1 for x in setups if x.get("census_context")]),
              "sample": hit})
    except Exception as e:
        gate("V2_best_setups", False, str(e)[:280])

    try:
        lam.invoke(FunctionName="justhodl-short-book", Payload=b"{}")
        time.sleep(2)
        d = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/short-book.json")["Body"].read())
        rows = d.get("book") or d.get("rows") or []
        hit = [(x.get("ticker"), x["census_context"]["risk"],
                x["census_context"]["patterns"])
               for x in rows if x.get("census_context")][:5]
        gate("V3_short_book", len(hit) >= 3,
             {"n": len(rows), "with_census": len(
                 [1 for x in rows if x.get("census_context")]),
              "sample": hit})
    except Exception as e:
        gate("V3_short_book", False, str(e)[:280])

    try:
        rr = lam.invoke(FunctionName="justhodl-equity-research",
                        Payload=json.dumps(
                            {"_internal": "1", "ticker": "NVDA",
                             "force_refresh": True}).encode())
        pay = json.loads(rr["Payload"].read())
        doc = pay.get("body")
        if isinstance(doc, str):
            doc = json.loads(doc)
        cen = (doc or {}).get("census") or (pay.get("census"))
        gate("V4_equity_research",
             isinstance((cen or {}).get("conviction"), (int, float)),
             {"census": cen})
    except Exception as e:
        gate("V4_equity_research", False, str(e)[:280])

    try:
        lam.invoke(FunctionName="justhodl-comeback-screener",
                   Payload=b"{}")
        time.sleep(2)
        d = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/comeback-screener.json")["Body"].read())
        allc = []
        for k, v in d.items():
            if isinstance(v, list):
                allc += [x for x in v if isinstance(x, dict)]
        hit = [(x.get("ticker"), x["census_context"]["turn"],
                x["census_context"]["conviction"])
               for x in allc if x.get("census_context")][:4]
        gate("V5_comeback", len(hit) >= 1,
             {"n_rows": len(allc), "with_census": len(
                 [1 for x in allc if x.get("census_context")]),
              "sample": hit})
    except Exception as e:
        gate("V5_comeback", False, str(e)[:280])

    gate("V6_master_ranker_zip", marks.get("justhodl-master-ranker"),
         "field self-activates on next scheduled run")

    pa = b""
    for _ in range(14):
        try:
            pa = fetch("https://justhodl.ai/why.html?cb=%d"
                       % int(time.time()))
            if b"OPS3550" in pa: break
        except Exception: pass
        time.sleep(20)
    mm = re.search(rb'<script id="OPS3550">\n([\s\S]*?)</script>', pa)
    ok_n = False
    if mm:
        with tempfile.NamedTemporaryFile("wb", suffix=".js",
                                         delete=False) as f:
            f.write(mm.group(1)); pth = f.name
        ok_n = subprocess.run(["node", "--check", pth],
                              capture_output=True).returncode == 0
    gate("V7_why_chips", b"OPS3550" in pa and ok_n, {"node": ok_n})

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3550.json").write_text(
        json.dumps({"ops": 3550, "fails": fails}))
sys.exit(0)
