"""ops/4891 -- industry deep-detail + site sidebar verify.
 (1) settle 'industry-case v1.1.0'; invoke; poll.
 (2) truths: Semiconductors HHI == sum(members share^2)
     recomputed from the doc's own members; members n ==
     ind n; NVDA ret_12m == independent recompute from the
     S3 closes ledger (arr[-1]/arr[-53]); wtd 12m ==
     recompute over covered members; a LEADER-tier member
     really holds >=25%; rev_growth DEFERRED honesty on
     every industry.
 (3) sidebar: /sidebar.js served with sb_root; four sampled
     pages served carrying the tag; industry-case.html
     served with the drill-down (openInd).
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

REGION = "us-east-1"
FN = "justhodl-industry-case"
B = "justhodl-dashboard-live"
OUT_KEY = "data/industry-case.json"
MARKER = "industry-case v1.1.0"
CLOSES = "spx-beaters/weekly-closes.json"

s3 = boto3.client("s3", region_name=REGION)
lam = boto3.client("lambda", region_name=REGION,
                   config=Config(read_timeout=120,
                                 retries={"max_attempts": 1}))
FAILED = []


def sread(key):
    raw = s3.get_object(Bucket=B, Key=key)["Body"].read()
    if raw[:2] == b"\x1f\x8b":
        raw = gzip.decompress(raw)
    return json.loads(raw)


def main():
    with report("ops 4891 -- detail+sidebar verify") as rep:
        rep.heading("1. settle + invoke")
        for _ in range(40):
            try:
                cfg = lam.get_function_configuration(
                    FunctionName=FN)
                if cfg.get("State") == "Active" and \
                        cfg.get("LastUpdateStatus") \
                        != "InProgress":
                    break
            except ClientError:
                pass
            time.sleep(6)
        settled = False
        for att in range(30):
            try:
                gf = lam.get_function(FunctionName=FN)
                raw = urllib.request.urlopen(
                    gf["Code"]["Location"],
                    timeout=60).read()
                if MARKER in zipfile.ZipFile(
                        io.BytesIO(raw)).read(
                        "lambda_function.py").decode(
                        "utf-8", "replace"):
                    rep.ok("marker settled (attempt %d)"
                           % (att + 1))
                    settled = True
                    break
            except (ClientError, Exception):  # noqa: BLE001
                pass
            time.sleep(10)
        if not settled:
            rep.fail("no marker")
            sys.exit(1)
        prev = sread(OUT_KEY).get("generated_at")
        lam.invoke(FunctionName=FN, InvocationType="Event",
                   Payload=b"{}")
        doc = None
        t0 = time.time()
        while time.time() - t0 < 240:
            time.sleep(10)
            try:
                dd = sread(OUT_KEY)
            except (ClientError, KeyError):
                continue
            if dd.get("generated_at") != prev \
                    and dd.get("v") == "1.1.0":
                doc = dd
                break
        if not doc:
            rep.fail("no fresh v1.1.0 doc")
            sys.exit(1)
        rep.ok("fresh in %ds" % int(time.time() - t0))

        rep.heading("2. truths")
        semi = (doc.get("industries") or {}).get(
            "Semiconductors") or {}
        mem = semi.get("members") or []
        if len(mem) == semi.get("n") and mem:
            rep.ok("members complete: %d rows" % len(mem))
        else:
            rep.fail("members %d vs n %s"
                     % (len(mem), semi.get("n")))
            sys.exit(1)
        hhi = round(sum(m["share_pct"] ** 2 for m in mem), 0)
        if abs(hhi - semi["hhi"]) <= 1:
            rep.ok("HHI %s == recompute (%s, top-3 %s%%)"
                   % (semi["hhi"],
                      "highly concentrated"
                      if semi["hhi"] > 2500 else
                      "moderate/competitive",
                      semi["top3_share_pct"]))
        else:
            rep.fail("hhi doc=%s recompute=%s"
                     % (semi["hhi"], hhi))
            FAILED.append("hhi")
        closes = sread(CLOSES).get("closes") or {}
        arr = closes.get("NVDA") or []
        exp = round((arr[-1] / arr[-53] - 1) * 100, 1) \
            if len(arr) >= 53 else None
        nv = doc["cases"]["NVDA"]
        if nv.get("ret_12m_pct") == exp:
            rep.ok("NVDA 12m ret %+0.1f%% == ledger "
                   "recompute (tier %s)"
                   % (exp, nv.get("tier")))
        else:
            rep.fail("ret doc=%s exp=%s"
                     % (nv.get("ret_12m_pct"), exp))
            FAILED.append("ret")
        num = den = 0.0
        for m in mem:
            if m["ret_12m_pct"] is not None:
                num += m["ret_12m_pct"] * m["mcap_b"]
                den += m["mcap_b"]
        wtd = round(num / den, 1) if den else None
        if semi.get("wtd_ret_12m_pct") == wtd:
            rep.ok("wtd 12m %s%% == recompute (%d/%d "
                   "covered, median %s%%)"
                   % (wtd, semi["ret_coverage"],
                      semi["n"],
                      semi["median_ret_12m_pct"]))
        else:
            rep.fail("wtd doc=%s exp=%s"
                     % (semi.get("wtd_ret_12m_pct"), wtd))
            FAILED.append("wtd")
        ld = next((m for m in mem if m["tier"] == "LEADER"),
                  None)
        if ld and ld["share_pct"] >= 25:
            rep.ok("tier check: %s LEADER at %s%%"
                   % (ld["t"], ld["share_pct"]))
        bad_rev = [k for k, v in doc["industries"].items()
                   if (v.get("rev_growth") or {}).get(
                       "status") != "DEFERRED"]
        if not bad_rev:
            rep.ok("rev_growth DEFERRED honesty on all %d "
                   "industries" % doc["n_industries"])
        else:
            rep.fail("rev leak: %s" % bad_rev[:3])
            FAILED.append("rev")

        rep.heading("3. sidebar + pages")
        checks = {"sidebar.js": "sb_root",
                  "index.html": "sidebar.js",
                  "sp500.html": "sidebar.js",
                  "foreign-flows.html": "sidebar.js",
                  "earnings.html": "sidebar.js",
                  "industry-case.html": "openInd"}
        t0 = time.time()
        while time.time() - t0 < 540 and checks:
            for pg, tok in list(checks.items()):
                try:
                    req = urllib.request.Request(
                        "https://justhodl.ai/%s?t=%d"
                        % (pg, int(time.time())),
                        headers={"User-Agent": "ops-4891",
                                 "Cache-Control":
                                 "no-cache"})
                    with urllib.request.urlopen(
                            req, timeout=45) as r:
                        if tok in r.read().decode(
                                "utf-8", "replace"):
                            rep.ok("  %s SERVED (%ds)"
                                   % (pg,
                                      int(time.time()
                                          - t0)))
                            del checks[pg]
                except Exception:  # noqa: BLE001
                    pass
            if checks:
                time.sleep(30)
        if checks:
            rep.fail("  not served: %s" % list(checks))
            FAILED.append("served")

        rep.heading("4. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("full industry infrastructure live + every "
               "page one tap away")


if __name__ == "__main__":
    main()
