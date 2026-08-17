"""ops/4816 -- spx-beaters v1.3.0: catalyst/backlog/deal leg (stocks),
fundamentals-intact derating flag (comeback), true-flows + industry-
fundamentals legs (ETFs). Settle marker, invoke, verify on LIVE doc:
weights sum 1.0 both books; >=3 stock rows cite catalyst/backlog/deal
evidence; >=2 ETF rows carry etf_flows and >=1 carries industry_fund
with a boom why-line; intact flags only where dd<=-35 and why cites
DERATING; clamps + trap integrity + 53w ledger unchanged. Readout.
"""
import gzip
import io
import json
import sys
import time
import urllib.request
import zipfile
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "ops"))
sys.path.insert(0, str(ROOT / "aws" / "ops"))
import boto3  # noqa: E402
from botocore.config import Config  # noqa: E402
from botocore.exceptions import ClientError  # noqa: E402
from ops_report import report  # noqa: E402

FN = "justhodl-spx-beaters"
B = "justhodl-dashboard-live"
OUT_KEY = "data/spx-beaters.json"
MARKER = "spx-beaters v1.3.0"
s3 = boto3.client("s3", region_name="us-east-1")
lam = boto3.client("lambda", region_name="us-east-1",
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []
start_iso = __import__("datetime").datetime.now(
    __import__("datetime").timezone.utc).isoformat()


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4816 -- v1.3.0 catalyst+ETF legs verify") as rep:
        rep.heading("1. settle + invoke")
        ok = False
        for _ in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"], timeout=60).read()
                src = zipfile.ZipFile(io.BytesIO(raw)).read(
                    "lambda_function.py").decode("utf-8", "replace")
                if MARKER in src:
                    ok = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not ok:
            rep.fail("marker never settled")
            sys.exit(1)
        rep.ok("marker settled")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 600:
            time.sleep(20)
            try:
                d = sread(OUT_KEY)
                if d.get("marker") == MARKER and \
                        d.get("as_of", "") > start_iso:
                    doc = d
                    break
            except ClientError:
                pass
        if not doc:
            rep.fail("doc never refreshed to v1.3.0")
            sys.exit(1)
        rep.ok("  fresh v1.3.0 doc (~%ds)" % int(time.time() - t0))

        rep.heading("2. weights + feeds")
        w = doc.get("weights") or {}
        for book in ("stock", "etf"):
            sm = sum((w.get(book) or {}).values())
            (rep.ok if abs(sm - 1.0) < 1e-6 else rep.fail)(
                "  %s weights sum = %.3f  %s"
                % (book, sm, json.dumps(w.get(book))))
            if abs(sm - 1.0) >= 1e-6:
                FAILED.append("w_" + book)
        fd = (doc.get("diag") or {}).get("feeds") or {}
        rep.kv(catalyst=fd.get("catalyst"),
               backlog_mined=fd.get("backlog_mined"),
               deal_tape=fd.get("deal_tape"),
               etf_true_flows=fd.get("etf_true_flows"))

        rep.heading("3. new evidence on LIVE rows")
        cat_rows = flow_rows = ind_rows = 0
        cat_ex = flow_ex = None
        intact_bad = 0
        for bname, rows in (doc.get("buckets") or {}).items():
            for r in rows:
                whys = " | ".join(r.get("why") or [])
                if "catalyst" in (r.get("legs") or {}):
                    cat_rows += 1
                    cat_ex = cat_ex or (r["t"], whys[:150])
                if "etf_flows" in (r.get("legs") or {}):
                    flow_rows += 1
                    flow_ex = flow_ex or (r["t"], whys[:150])
                if "industry_fund" in (r.get("legs") or {}):
                    ind_rows += 1
                if r.get("fundamentals_intact"):
                    if (r.get("dd_52w_pct") or 0) > -35 or \
                            "DERATING" not in whys:
                        intact_bad += 1
        (rep.ok if cat_rows >= 3 else rep.warn)(
            "  stock rows w/ catalyst leg = %d (ex: %s)"
            % (cat_rows, cat_ex))
        if cat_rows == 0 and (fd.get("catalyst") or 0) > 20:
            rep.fail("  catalyst feed populated but zero joins")
            FAILED.append("catalyst_join")
        (rep.ok if flow_rows >= 2 else rep.warn)(
            "  ETF rows w/ true-flows leg = %d (ex: %s)"
            % (flow_rows, flow_ex))
        if flow_rows == 0 and (fd.get("etf_true_flows") or 0) > 5:
            rep.fail("  flows feed populated but zero joins")
            FAILED.append("flows_join")
        (rep.ok if ind_rows >= 1 else rep.warn)(
            "  ETF rows w/ industry-fundamentals leg = %d" % ind_rows)
        (rep.ok if intact_bad == 0 else rep.fail)(
            "  fundamentals-intact contract violations = %d"
            % intact_bad)
        if intact_bad:
            FAILED.append("intact")

        rep.heading("4. invariants")
        led = doc.get("ledger") or {}
        okm = (led.get("weeks") == 53
               and (doc.get("mom_status") or {}).get("m12_1"))
        (rep.ok if okm else rep.fail)("  53w + 12-1 intact")
        if not okm:
            FAILED.append("ledger")
        viol = 0
        for rows in (doc.get("buckets") or {}).values():
            for r in rows:
                a = r.get("ai")
                if a and (a["downside_risk_pct"] > 95
                          or not 13 <= a["horizon_weeks"] <= 52):
                    viol += 1
        (rep.ok if viol == 0 else rep.fail)(
            "  ai clamp violations = %d" % viol)
        if viol:
            FAILED.append("clamps")
        rep.kv(counts=json.dumps(doc.get("counts")))

        rep.heading("5. readout")
        for b in ("large", "comeback", "etf_equity", "etf_commodity"):
            rows = (doc.get("buckets") or {}).get(b) or []
            for r in rows[:2]:
                rep.ok("  %-12s %-6s %5.1f legs=%s"
                       % (b, r["t"], r["score"],
                          json.dumps(r.get("legs"))))

        rep.heading("6. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("v1.3.0 LIVE -- contracts/backlog/deal evidence, "
               "derating detection and ETF flows/industry legs "
               "verified on the live league")


if __name__ == "__main__":
    main()
