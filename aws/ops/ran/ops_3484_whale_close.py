"""ops 3484 — clean close of 3482 Z3/Z5 (13F whale fusion + surface chips).

3483 sed-clone crashed pre-report (banked: never sed-clone ops files).
This is purpose-built: engine v1.3.1 reads the REAL compact 13F schema
(root "t", fields n/wn/nf/tv per ticker); why.html chip hotfix-seated
inside the header literal (span/div split). Gates report per-condition
booleans so nothing fails silently again.

  W1 deploy v1.3.1 + GOOGL refresh -> whales_q.net_usd in 8-15e9
     (known-good ~+$11.5B from the 13F arc) + n_funds > 500
  W2 flagship live: ops3482 marker + whales_q chip template
  W3 why.html live: ops3482 whale chip + ops3475 + flags + marks intact
"""
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
lam = boto3.client("lambda", region_name="us-east-1")
s3c = boto3.client("s3", region_name="us-east-1")


def fetch(url):
    req = urllib.request.Request(url, headers={"User-Agent": "ops-3484"})
    with urllib.request.urlopen(req, timeout=30) as r:
        return r.read()


with report("3484_whale_close") as rep:
    out = {"ops": 3484, "gates": {}}
    fails = []

    def gate(name, ok, detail):
        out["gates"][name] = {"ok": bool(ok), "detail": str(detail)[:420]}
        line = ("PASS  " if ok else "FAIL  ") + name + " — " + str(detail)[:380]
        print(line)
        rep.log(line)
        if not ok:
            fails.append(name)

    rep.heading("ops 3484 — whale fusion close (schema t/n/wn/nf/tv)")

    deploy_lambda(
        report=rep, function_name=FN,
        source_dir=REPO / "aws" / "lambdas" / FN / "source",
        env_vars={"FMP_KEY": FMP_KEY, "S3_BUCKET": BUCKET,
                  "CACHE_TTL_SEC": "72000"},
        timeout=900, memory=512,
        description="Fundamental Graphs v1.3.1 whale schema fix (ops 3484)",
        create_function_url=True, smoke=False,
    )
    for _ in range(30):
        c = lam.get_function_configuration(FunctionName=FN)
        if c.get("LastUpdateStatus") == "Successful" and c.get("State") == "Active":
            break
        time.sleep(2)

    wp = json.loads(lam.invoke(FunctionName=FN, Payload=json.dumps(
        {"warm": ["GOOGL"], "periods": ["quarter"],
         "refresh": True}).encode())["Payload"].read() or b"{}")
    try:
        doc = json.loads(s3c.get_object(
            Bucket=BUCKET,
            Key="data/fundgraph/cache/GOOGL_quarter_v13.json")["Body"].read())
        wq = doc.get("whales_q") or {}
        net = wq.get("net_usd")
        gate("W1_whale_join",
             wp.get("version") == "1.3.1" and net is not None
             and 8e9 <= net <= 15e9 and (wq.get("n_funds") or 0) > 500,
             {"version": wp.get("version"), "net_usd": net,
              "whale_net_usd": wq.get("whale_net_usd"),
              "n_funds": wq.get("n_funds"), "held_usd": wq.get("held_usd")})
    except Exception as e:  # noqa: BLE001
        gate("W1_whale_join", False, str(e)[:260])

    fl = wy = b""
    for _ in range(18):
        try:
            cb = int(time.time())
            fl = fetch(f"https://justhodl.ai/fundamental-graphs.html?cb={cb}")
            wy = fetch(f"https://justhodl.ai/why.html?cb={cb}")
            if b"whales_q" in fl and b"ops3482" in wy:
                break
        except Exception:  # noqa: BLE001
            pass
        time.sleep(20)
    d2 = {"flag_ops3482": b"ops3482" in fl, "flag_whales": b"whales_q" in fl}
    gate("W2_flagship_chip", all(d2.values()), d2)
    d3 = {"why_whale_chip": b"ops3482" in wy and b"whales_q" in wy,
          "tdz_intact": b"ops3475" in wy,
          "flags_intact": b"jhfgFlags" in wy,
          "marks_intact": b"jh_fgwhy_marks" in wy}
    gate("W3_why_chip", all(d3.values()), d3)

    out["status"] = "ALL PASS" if not fails else f"FAILS: {fails}"
    (REPO / "aws" / "ops" / "reports" / "3484.json").write_text(
        json.dumps(out, indent=2))
    rep.heading("RESULT: " + out["status"])
    print("RESULT:", out["status"])

sys.exit(0)
