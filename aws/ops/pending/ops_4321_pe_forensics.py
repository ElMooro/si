"""ops_4321 -- P/E forensics: fresh TSM doc's full valuation vs live
ratios-ttm; name the live field equal to the stored value; call-site
argument order + the ratios_ttm fetch definition. Then the patch."""
import json, subprocess, sys, urllib.request
import boto3
from ops_report import report
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1")
def sh(cmd):
    try:
        return subprocess.run(cmd, capture_output=True, text=True,
                              timeout=25).stdout[:1300]
    except Exception as e:
        return "sh: %s" % e
with report("4321_pe_forensics") as r:
    r.heading("ops 4321 -- who is 1.10, really")
    d = json.loads(s3.get_object(
        Bucket="justhodl-dashboard-live",
        Key="equity-research/TSM.json")["Body"].read())
    v = d.get("valuation") or {}
    r.log("fresh doc generated_at=%s" % d.get("generated_at"))
    r.ok("valuation: %s" % json.dumps(v))
    kd = lam.get_function_configuration(
        FunctionName="justhodl-commodity-curves")
    env = ((kd.get("Environment") or {}).get("Variables") or {})
    fk = env.get("FMP_KEY") or env.get("FMP_API_KEY")
    rt = json.loads(urllib.request.urlopen(
        "https://financialmodelingprep.com/stable/ratios-ttm"
        "?symbol=TSM&apikey=%s" % fk, timeout=25).read())
    rt = rt[0] if isinstance(rt, list) and rt else rt
    tgt = v.get("pe_ttm")
    culprits = {k: rt[k] for k in rt
                if isinstance(rt.get(k), (int, float))
                and tgt is not None
                and abs(rt[k] - tgt) < 0.06}
    r.ok("stored pe_ttm=%s -> live ratios-ttm fields equal to it: %s"
         % (tgt, culprits))
    km = json.loads(urllib.request.urlopen(
        "https://financialmodelingprep.com/stable/key-metrics-ttm"
        "?symbol=TSM&apikey=%s" % fk, timeout=25).read())
    km = km[0] if isinstance(km, list) and km else km
    culp2 = {k: km[k] for k in km
             if isinstance(km.get(k), (int, float))
             and tgt is not None and abs(km[k] - tgt) < 0.06}
    r.log("key-metrics-ttm fields equal to it: %s" % culp2)
    ar = ("aws/lambdas/justhodl-equity-research/source/"
          "lambda_function.py")
    r.section("call site + fetch definition")
    r.log("compute_valuation call:\n%s"
          % sh(["grep", "-n", "-B3", "-A3",
                "compute_valuation(", ar]))
    r.log("ratios_ttm fetch def:\n%s"
          % sh(["grep", "-n", "-B2", "-A2", "ratios-ttm", ar]))
    r.log("raw dict keys around fetch table:\n%s"
          % sh(["grep", "-n", "-B1", "-A1",
                '"ratios_ttm"', ar]))
    if v.get("pe_ttm") is None:
        r.fail("fresh doc has no pe_ttm at all")
        sys.exit(1)
    r.ok("forensics complete -- patch ships against these lines")
