"""ops 3557 — ETF universe widen (per-ticker-context source) + pins
diagnosis. v1.0.1: flow_records() also unions
etf-flows/per-ticker-context.json (dict-of-dicts, tolerant nesting).

  B1 deploy (zip 1.0.1) + full rerun: n printed with source counts;
     PASS if n>=110, else if per-ticker-context genuinely lacks a
     >50-name dict, PASS at n>=75 with the honest note
  B2 served manifest: print Research & Tools hrefs; PASS if both new
     pages present anywhere in the manifest, else print all
     categories containing 'census' for diagnosis
"""
import io, json, sys, time, urllib.request, zipfile
from pathlib import Path
import boto3
from botocore.config import Config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ops_report import report
from _lambda_deploy_helpers import deploy_lambda
REPO = Path(__file__).resolve().parents[3]
BUCKET = "justhodl-dashboard-live"
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=800,
                                 retries={"max_attempts": 0}))
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url, t=30):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3557"})
    with urllib.request.urlopen(req, timeout=t) as r:
        return r.read()


with report("3557_etf_widen") as rep:
    fails = []
    def gate(n, ok, d):
        line = ("PASS  " if ok else "FAIL  ") + n + " — " + str(d)[:660]
        print(line); rep.log(line)
        if not ok: fails.append(n)

    fn = "justhodl-etf-census"
    deploy_lambda(report=rep, function_name=fn,
                  source_dir=REPO/"aws"/"lambdas"/fn/"source",
                  env_vars={"FMP_API_KEY":
                            "wwVpi37SWHoNAzacFNVCDxEKBTUlS8xb"},
                  timeout=900, memory=1024,
                  description="justhodl-etf-census v1.0.1 (ops 3557)",
                  create_function_url=False, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=fn)
        if c.get("LastUpdateStatus") == "Successful":
            break
        time.sleep(2)
    loc = lam.get_function(FunctionName=fn)["Code"]["Location"]
    src = zipfile.ZipFile(io.BytesIO(urllib.request.urlopen(
        loc, timeout=60).read())).read("lambda_function.py")
    zok = b'"1.0.1"' in src and b"per-ticker-context" in src

    try:
        ptc = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="etf-flows/per-ticker-context.json")["Body"].read())
        big = None
        for cand in (ptc, ptc.get("tickers"), ptc.get("by_ticker"),
                     ptc.get("context")):
            if isinstance(cand, dict) and len(cand) > 50:
                big = len(cand); break
        r = lam.invoke(FunctionName=fn, Payload=b"{}")
        json.loads(r["Payload"].read())
        MX = json.loads(s3c.get_object(Bucket=BUCKET,
            Key="data/etf-census-matrix.json")["Body"].read())
        C = MX["cols"]
        nn = lambda k: sum(1 for v in C.get(k) or []
                           if isinstance(v, (int, float)))
        tt = sorted([(MX["tickers"][i], v) for i, v in
                     enumerate(C.get("tech_score") or [])
                     if isinstance(v, (int, float))],
                    key=lambda x: -x[1])[:8]
        ok = zok and (MX["n"] >= 110 or (big is None
                                         and MX["n"] >= 75))
        gate("B1_widen", ok,
             {"zip": zok, "n": MX["n"], "ptc_dict_n": big,
              "beta_n": nn("beta_spy"),
              "tech_n": nn("tech_score"), "tech_top": tt,
              "ptc_top_keys": sorted(ptc.keys())[:8]
              if isinstance(ptc, dict) else None})
    except Exception as e:
        gate("B1_widen", False, str(e)[:340])

    try:
        man = json.loads(fetch(
            "https://justhodl.ai/nav-manifest.json?cb=%d"
            % int(time.time())))
        cats = {c["name"]: [p["href"] for p in c.get("pages") or []]
                for c in man.get("categories") or []}
        hits = {name: [h for h in hs if "census" in h]
                for name, hs in cats.items()
                if any("census" in h for h in hs)}
        both = any("etf-census" in h for hs in cats.values()
                   for h in hs) and \
            any("fixed-income-census" in h for hs in cats.values()
                for h in hs)
        gate("B2_pins", both,
             {"census_hits_by_cat": hits,
              "rt_n": len(cats.get("Research & Tools") or [])})
    except Exception as e:
        gate("B2_pins", False, str(e)[:260])

    print("RESULT:", "ALL PASS" if not fails else f"FAILS: {fails}")
    (REPO/"aws/ops/reports/3557.json").write_text(
        json.dumps({"ops": 3557, "fails": fails}))
sys.exit(0)
