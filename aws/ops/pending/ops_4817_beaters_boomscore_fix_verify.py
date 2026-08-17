"""ops/4817 -- spx-beaters v1.3.1: industry-boom silent-join fix.
Root cause (found post-4816): producer league rows carry "boom_score";
engine bound "score" -> boom_by_ind EMPTY since ops 4811, killing the
stock `industry` leg AND the ETF `industry_fund` leg silently (weights
renormalized, nothing crashed). Doctrine reinforcement: bind the
producer's OUTPUT construction and G0 the FIELD, not just the container.
 G0  field-level: live league[0] must contain "boom_score" (numeric).
 (1) settle 'spx-beaters v1.3.1'; invoke; poll fresh doc.
 (2) parse proof: doc.diag.feeds.industry_boom >= 100 (was 0).
 (3) legs live: stock rows with `industry` leg >= 10; ETF rows with
     `industry_fund` >= 1; sample why-lines cite boom score (+20d
     delta when present).
 (4) invariants: weights sum 1.0, 53w + 12-1, ai downside <= 95 &
     horizon 13-52, live trap-set overlap with comeback == NONE.
 (5) readout.
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
MARKER = "spx-beaters v1.3.1"
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


def tick(r):
    for k in ("ticker", "symbol", "t"):
        if r.get(k):
            return str(r[k]).upper()
    return None


def main():
    with report("ops 4817 -- v1.3.1 boom_score fix verify") as rep:
        rep.heading("G0. field-level league contract")
        try:
            lg = (sread("data/industry-boom.json").get("league")
                  or [])
        except ClientError:
            rep.fail("  industry-boom.json missing")
            sys.exit(1)
        if not lg or not isinstance(lg[0].get("boom_score"),
                                    (int, float)):
            rep.fail("  league[0] lacks numeric boom_score: keys=%s"
                     % sorted((lg[0] if lg else {}).keys())[:10])
            sys.exit(1)
        names = [str(r.get("industry"))[:30] for r in lg[:5]]
        rep.ok("  league[0].boom_score = %.1f; sample industries: %s"
               % (lg[0]["boom_score"], names))

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
            rep.fail("doc never refreshed to v1.3.1")
            sys.exit(1)
        rep.ok("  fresh v1.3.1 doc (~%ds)" % int(time.time() - t0))

        rep.heading("2. parse proof")
        ib = ((doc.get("diag") or {}).get("feeds")
              or {}).get("industry_boom")
        (rep.ok if (ib or 0) >= 100 else rep.fail)(
            "  diag.feeds.industry_boom = %s (was 0 in 4811-4816)"
            % ib)
        if (ib or 0) < 100:
            FAILED.append("parse")

        rep.heading("3. industry legs live")
        st_ind = etf_ind = 0
        st_ex = etf_ex = None
        for bname, rows in (doc.get("buckets") or {}).items():
            for r in rows:
                lgs = r.get("legs") or {}
                whys = " | ".join(r.get("why") or [])
                if "industry" in lgs:
                    st_ind += 1
                    if st_ex is None and "boom score" in whys:
                        st_ex = (r["t"],
                                 [w for w in r["why"]
                                  if "boom score" in w][0][:130])
                if "industry_fund" in lgs:
                    etf_ind += 1
                    if etf_ex is None and "boom" in whys:
                        etf_ex = (r["t"],
                                  [w for w in r["why"]
                                   if "boom" in w][0][:130])
        (rep.ok if st_ind >= 10 else rep.fail)(
            "  stock rows w/ industry leg = %d" % st_ind)
        if st_ind < 10:
            FAILED.append("stock_ind")
        (rep.ok if etf_ind >= 1 else rep.fail)(
            "  ETF rows w/ industry_fund leg = %d" % etf_ind)
        if etf_ind < 1:
            FAILED.append("etf_ind")
        if st_ex:
            rep.log("    stock ex %s: %s" % st_ex)
        if etf_ex:
            rep.log("    etf ex %s: %s" % etf_ex)

        rep.heading("4. invariants")
        w = doc.get("weights") or {}
        for book in ("stock", "etf"):
            sm = sum((w.get(book) or {}).values())
            if abs(sm - 1.0) >= 1e-6:
                rep.fail("  %s weights sum %.3f" % (book, sm))
                FAILED.append("w_" + book)
        led = doc.get("ledger") or {}
        okm = (led.get("weeks") == 53
               and (doc.get("mom_status") or {}).get("m12_1"))
        (rep.ok if okm else rep.fail)("  53w + 12-1 intact = %s" % okm)
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
        traps = set()
        for key, cont in (("data/beneish.json", "red_flags"),
                          ("data/earnings-quality.json",
                           "top_10_low_quality_avoid"),
                          ("data/insider-sell-cluster.json",
                           "top_clusters")):
            try:
                for r in (sread(key).get(cont) or []):
                    t = tick(r)
                    if t:
                        traps.add(t)
            except ClientError:
                pass
        cb = (doc.get("buckets") or {}).get("comeback") or []
        overlap = sorted({r["t"] for r in cb} & traps)
        (rep.ok if not overlap else rep.fail)(
            "  comeback trap overlap = %s" % (overlap or "NONE"))
        if overlap:
            FAILED.append("traps")
        rep.kv(counts=json.dumps(doc.get("counts")))

        rep.heading("5. readout")
        for b in ("large", "mid", "comeback", "etf_equity"):
            rows = (doc.get("buckets") or {}).get(b) or []
            for r in rows[:2]:
                rep.ok("  %-11s %-6s %5.1f legs=%s"
                       % (b, r["t"], r["score"],
                          json.dumps(r.get("legs"))))

        rep.heading("6. verdict")
        if FAILED:
            rep.fail("HARD FAILS: %s" % sorted(set(FAILED)))
            sys.exit(1)
        rep.ok("v1.3.1 LIVE -- industry legs restored on both books; "
               "field-level G0 now guards the boom contract")


if __name__ == "__main__":
    main()
