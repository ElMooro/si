"""ops 3480 — Tier-1d: red-flag digest (auto-forensic analyst).

Engine v1.2.2 FLAGS_ENGINE_OPS3480: 10-rule engine over the 200-series doc
(DSO stretch vs sales, capex/D&A starvation, DPO games, CFO<NI quality,
dilution acceleration, gross-margin roll, Sloan>10%, leverage +1 turn,
Beneish zone, SBC creep) -> doc.flags top-6 by severity with evidence
keys. Flagship v1.8: per-symbol digest cards, click a flag -> charts its
evidence series. why-module: top-3 clickable chips.

Proofs already run pre-push: python rule unit test (correct trips, guarded
non-trips, sev-sorted) + jsdom harness step I (chip click charts
evidence). Gates here:

  R1 CI re-runs the rule unit test from the checkout (authoritative)
  R2 warm v1.2.2 -> docs carry flags:list for all 3; contents soft-logged
  R3 flagship v1.8 live (ops3480 + fgFlags) — inline node-check
  R4 module flags live (jhfgFlags) — inline node-check; ops3475 intact
"""
import importlib.util, json, os, re, subprocess, sys, tempfile, time, urllib.request
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
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3480"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


def node_ok(b):
    with tempfile.NamedTemporaryFile("wb", suffix=".js", delete=False) as f:
        f.write(b); p = f.name
    return subprocess.run(["node", "--check", p], capture_output=True).returncode == 0


with report("3480_redflag_digest") as rep:
    out = {"ops": 3480, "gates": {}}; fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:380]
        print(line); rep.log(line)
        if not ok: fails.append(name)

    rep.heading("ops 3480 — red-flag digest (rules + both surfaces)")

    # R1 rule unit test from the checkout
    try:
        os.environ.setdefault("FMP_KEY", "x")
        spec = importlib.util.spec_from_file_location(
            "lf", REPO / "aws/lambdas" / FN / "source/lambda_function.py")
        m = importlib.util.module_from_spec(spec); spec.loader.exec_module(m)
        mk = lambda vals: [["2024-%02d-01" % (i % 12 + 1), v] for i, v in enumerate(vals)]
        P = {"income_quality": mk([1.1]*8+[0.9, 0.7]),
             "share_count_yoy_pct": mk([1]*8+[2, 4.5]),
             "gross_margin_pct": mk([40]*6+[40, 39, 38, 37.5]),
             "beneish_m": mk([-2.5]*9+[-1.5]),
             "dso_days": mk([50]*10), "revenue_yoy_pct": mk([10]*10)}
        fl = m.derive_flags(P, 4)
        ids = [f["id"] for f in fl]
        gate("R1_rule_unit",
             {"EARNINGS_QUALITY", "DILUTION_ACCEL", "MARGIN_ROLL",
              "BENEISH"} <= set(ids)
             and "DSO_STRETCH" not in ids and fl[0]["sev"] == 3,
             {"tripped": [(f["id"], f["sev"]) for f in fl]})
    except Exception as e:
        gate("R1_rule_unit", False, str(e)[:240])

    deploy_lambda(report=rep, function_name=FN,
                  source_dir=REPO / "aws" / "lambdas" / FN / "source",
                  env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                            "CACHE_TTL_SEC": "72000"},
                  timeout=900, memory=512,
                  description="Fundamental Graphs v1.2.2 red-flag digest (ops 3480)",
                  create_function_url=True, smoke=False)
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)
    wp = json.loads(lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["CHTR", "AAPL", "MSFT"], "periods": ["quarter"],
         "refresh": True}).encode())["Payload"].read() or b"{}")
    ok2, det = wp.get("version") == "1.2.2", {"version": wp.get("version")}
    for sym in ("CHTR", "AAPL", "MSFT"):
        try:
            doc = json.loads(s3.get_object(
                Bucket=BUCKET,
                Key=f"data/fundgraph/cache/{sym}_quarter_v12.json")["Body"].read())
            fl = doc.get("flags")
            det[sym] = [(f["id"], f["sev"]) for f in (fl or [])]
            if not isinstance(fl, list):
                ok2 = False
        except Exception as e:
            ok2 = False; det[sym] = str(e)[:100]
    gate("R2_docs_carry_flags", ok2, det)

    got = {}
    for _ in range(21):
        try:
            cb = int(time.time())
            got["flag"] = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            got["why"] = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"ops3480" in got["flag"] and b"jhfgFlags" in got["why"]:
                break
        except Exception as e:
            got["err"] = str(e)[:120]
        time.sleep(20)
    m1 = re.search(rb"<script>\n('use strict'[\s\S]*?)</script>", got.get("flag", b""))
    gate("R3_flagship_v18",
         b"ops3480" in got.get("flag", b"") and b"fgFlags" in got.get("flag", b"")
         and m1 and node_ok(m1.group(1)), {})
    m2 = re.search(rb'<script id="fgwhy-3478">([\s\S]*?)</script>', got.get("why", b""))
    gate("R4_module_flags",
         b"jhfgFlags" in got.get("why", b"") and b"ops3475" in got.get("why", b"")
         and m2 and node_ok(m2.group(1)), {})

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3480.json").write_text(json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"]); print("RESULT:", out["status"])
sys.exit(0)
